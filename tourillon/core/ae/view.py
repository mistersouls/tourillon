# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Replica map of every partition, derived once per ring.

Anti-entropy reasons per partition: "who are the replicas of pid p, and do
we both agree on it". Recomputing a preference list per pid and per request
would cost a ring walk each time, so the map is built once per ring and
shared by the initiator and the responder until the ring changes.

The map expresses the *target* ownership of every pid, the state
anti-entropy converges the data towards:

- ``replicas[pid]`` are the first ``replication_factor`` distinct nodes met
  walking clockwise from the successor of the partition's end token, the
  rule of the write path (see ``SimplePlacementStrategy.preference_list``),
  except that DRAINING members are skipped: they are leaving, so their data
  must converge onto the nodes that replace them. JOINING members are
  included: they are the owners being filled.
- ``sources[pid]`` are the nodes currently serving the pid: the same walk
  skipping JOINING members instead. A joining node pulls from them until it
  has caught up.

Handoff substitutes are a per-request availability concern and not part of
ownership.
"""

import asyncio
import hashlib
from bisect import bisect_left
from collections.abc import Iterable
from dataclasses import dataclass, field

from tourillon.core.gossip.topology import Topology
from tourillon.core.models.antientropy import VIEW_BYTES
from tourillon.core.models.member import MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.spaces.ring import Ring

# Pids processed between two event-loop yields while building the map. The
# full build at 1 << 17 partitions takes a few hundred milliseconds; slicing
# it keeps client requests flowing meanwhile.
_YIELD_EVERY = 4096


@dataclass(frozen=True)
class ReplicaView:
    """Immutable replica map of one ring, as seen by ``node_id``.

    - ``replicas[pid]`` is the ordered target replica tuple of every partition;
    - ``sources[pid]`` is the ordered tuple of nodes serving it today;
    - ``owns`` tells whether ``node_id`` is a target replica of any pid;
    - ``shared[peer]`` lists, in ascending order, the pids this node and
      ``peer`` both replicate: the work list of replica-mode sessions.
    """

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
        """Pick the single node ``node_id`` copies ``pid`` from while joining.

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

    def view_hash(self, pids: Iterable[int]) -> bytes:
        """Hash the replica sets of ``pids``; both sides of a request compare it.

        Two nodes produce the same hash exactly when they agree on the
        ordered replicas of every listed pid, whatever the rest of their
        rings looks like.
        """
        hasher = hashlib.blake2b(digest_size=VIEW_BYTES)

        for pid in pids:
            hasher.update(pid.to_bytes(4, "big"))

            for node_id in self.replicas[pid]:
                encoded = node_id.encode("utf-8")
                hasher.update(len(encoded).to_bytes(2, "big"))
                hasher.update(encoded)

            hasher.update(b"\xff\xff")

        return hasher.digest()


async def build_view(
    topology: Topology,
    serial: str,
    partitioner: Partitioner,
    replication_factor: int,
    node_id: str,
) -> ReplicaView:
    """Build the replica map of ``topology.ring``, yielding to the event loop.

    Successor lookups bisect a plain token list (C speed), and both walks
    are memoised per successor vnode: neighbouring partitions often share
    them. Vnodes whose member is unknown to the registry are skipped, exactly
    like the write path does.
    """
    ring = topology.ring
    total = partitioner.total_partitions

    if not len(ring):
        return ReplicaView(
            node_id=node_id,
            serial=serial,
            replicas=((),) * total,
            sources=((),) * total,
        )

    tokens = [vnode.token for vnode in ring]
    phases = _phases(topology, ring)
    targets = {n for n, phase in phases.items() if phase is not MemberPhase.DRAINING}
    serving = {n for n, phase in phases.items() if phase is not MemberPhase.JOINING}
    memo: dict[int, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    replicas: list[tuple[str, ...]] = []
    sources: list[tuple[str, ...]] = []
    shared: dict[str, list[int]] = {}
    owns = False

    for pid in range(total):
        if pid % _YIELD_EVERY == 0:
            await asyncio.sleep(0)

        idx = bisect_left(tokens, partitioner.partition_for(pid).end) % len(tokens)
        walked = memo.get(idx)

        if walked is None:
            walked = memo[idx] = (
                _walk(ring, idx, targets, replication_factor),
                _walk(ring, idx, serving, replication_factor),
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


def _spread(pid: int, node_id: str) -> int:
    seed = pid.to_bytes(4, "big") + node_id.encode("utf-8")
    return int.from_bytes(hashlib.blake2b(seed, digest_size=8).digest(), "big")


def _phases(topology: Topology, ring: Ring) -> dict[str, MemberPhase]:
    phases: dict[str, MemberPhase] = {}

    for vnode in ring:
        member = topology.registry.get(vnode.node_id)
        if member is not None:
            phases[vnode.node_id] = member.phase

    return phases


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
