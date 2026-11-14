import hashlib
from bisect import bisect_left
from dataclasses import dataclass, field
from typing import Iterable

from tourillon.core.gossip.topology import Topology
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.storageschema import Pid
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.spaces.ring import Ring

VIEW_BYTES: int = 16


@dataclass(frozen=True)
class ReplicaView:
    node_id: str
    serial: str
    replicas: tuple[tuple[str, ...], ...]
    shared: dict[str, tuple[int, ...]] = field(default_factory=dict)
    sources: tuple[tuple[str, ...], ...] = ()
    owns: bool = False

    def is_replica(self, pid: int, node_id: str | None = None) -> bool:
        return (node_id or self.node_id) in self.replicas[pid]

    def sources_of(self, pid: int) -> tuple[str, ...]:
        return self.sources[pid] if self.sources else self.replicas[pid]

    def primary_source(self, pid: int, node_id: str | None = None) -> str | None:
        """
        Pick the single node ``node_id`` copies ``pid`` from while joining.

        Entering and leaving replicas are paired in sorted order, so joiners
        replacing as many nodes each copy from a distinct one. A joiner left
        unpaired picks a current source by hash, spreading the load.
        """
        node = node_id or self.node_id
        current = self.sources_of(pid)
        candidates = [n for n in current if n != node]
        if not candidates:
            return None

        target = self.replicas[pid]
        entering = sorted(n for n in target if n not in current)
        leaving = sorted(n for n in candidates if n not in target)

        if node in entering and entering.index(node) < len(leaving):
            return leaving[entering.index(node)]

        return candidates[_spread(pid, node) % len(candidates)]

    def covers(self, pids: Iterable[int]) -> bool:
        total = len(self.replicas)
        return all(0 <= pid < total for pid in pids)

    def hash(self, pids: Iterable[int]) -> bytes:
        """
        Hash the replica sets of ``pids``; both sides of a request compare it.

        Two nodes produce the same hash exactly when they agree on the
        ordered replicas of every listed pid, whatever the rest of their
        rings looks like.
        """
        hasher = hashlib.blake2b(digest_size=VIEW_BYTES)

        for pid in pids:
            hasher.update(Pid(pid).to_bytes())

            for node_id in self.replicas[pid]:
                encoded = node_id.encode("utf-8")
                hasher.update(len(encoded).to_bytes(2, "big"))
                hasher.update(encoded)

            hasher.update(b"\xff\xff")

        return hasher.digest()


def build_view(
    node_id: str,
    topology: Topology,
    partitioner: Partitioner,
    replication_factor: int,
) -> ReplicaView:
    ring = topology.ring
    serial = ring.serial()
    total = partitioner.total_partitions

    if not len(ring):
        return ReplicaView(
            node_id=node_id,
            serial=serial,
            replicas=((),) * total,
            sources=((),) * total,
        )

    tokens = [vnode.token for vnode in ring]
    phases = _phases(topology)
    targets = {n for n, phase in phases.items() if phase is not MemberPhase.DRAINING}
    serving = {n for n, phase in phases.items() if phase is MemberPhase.JOINING}
    memo: dict[int, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    replicas: list[tuple[str, ...]] = []
    sources: list[tuple[str, ...]] = []
    shared: dict[str, list[int]] = {}
    owns = False

    for pid in range(total):
        idx = bisect_left(tokens, partitioner.partition_for(pid).end) % len(tokens)
        walked = memo.get(idx)

        if walked is None:
            walked = memo[idx] = (
                _walk(topology.ring, idx, targets, replication_factor),
                _walk(topology.ring, idx, serving, replication_factor),
            )

        owners = walked[0]
        replicas.append(owners)
        sources.append(walked[1])

        if node_id in owners:
            owns = True
            for peer in owners:
                if peer != node_id:
                    shared.setdefault(peer, []).append(pid)

    return ReplicaView(
        node_id=node_id,
        serial=serial,
        replicas=tuple(replicas),
        shared={peer: tuple(pids) for peer, pids in shared.items()},
        sources=tuple(sources),
        owns=owns,
    )


def _phases(topology) -> dict[str, MemberPhase]:
    phases = {}

    for vnode in topology.ring:
        member = topology.registry.get(vnode.node_id)
        if member is not None:
            phases[vnode.node_id] = member.phase

    return phases


def _spread(pid: int, node_id: str) -> int:
    seed = Pid(pid).to_bytes() + node_id.encode("utf-8")
    return int.from_bytes(hashlib.blake2b(seed, digest_size=8).digest(), "big")


def _walk(ring: Ring, start: int, known: set[str], rf: int) -> tuple[str, ...]:
    owners: list[str] = []
    size = len(ring)

    for step in range(size):
        candidate = ring[(start + step) % size].node_id
        if candidate in known and candidate not in owners:
            owners.append(candidate)
            if len(owners) == rf:
                break

    return tuple(owners)
