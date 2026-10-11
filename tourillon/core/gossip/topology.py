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

"""Topology snapshot and TopologyManager for the cluster ring and registry."""

import asyncio
import hashlib
from collections.abc import Iterable
from dataclasses import dataclass

from tourillon.core.gossip.registry import MemberRegistry
from tourillon.core.log import logger
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.spaces.ring import Ring

# Phases whose vnodes are part of the active ring: members that own data.
RING_PHASES = frozenset({MemberPhase.READY, MemberPhase.PAUSED, MemberPhase.DRAINING})


@dataclass(frozen=True)
class Topology:
    """Immutable point-in-time snapshot of the cluster topology.

    Safe to hold across await points. Obtained via TopologyManager.snapshot().
    The active ring contains the vnodes of READY, PAUSED and DRAINING nodes
    (see RING_PHASES). JOINING, IDLE and FAILED vnodes are absent from the ring.

    registry holds a shallow copy of MemberRegistry produced at the time of
    the TopologyManager lock acquisition. It is safe to iterate and query
    without holding any lock.
    """
    registry: MemberRegistry
    ring: Ring


class TopologyManager:
    """Stateful manager for the active ring and MemberRegistry.

    All mutations go through apply_member or merge_registry. Both methods
    compare the incoming Member against the current registry entry using
    member.supersedes() and, when the record is newer, update the registry
    and derive the member's vnodes in the ring from its latest phase
    (RING_PHASES), entirely internally. Callers never manipulate the ring
    directly.

    Snapshot contract: snapshot() acquires the lock, builds an immutable
    Topology from MemberRegistry.snapshot() and the current Ring, releases
    the lock, then returns the frozen Topology.

    member_fingerprint() is always read live from TopologyManager under its
    lock; it is never stored on a Topology snapshot, which would become
    immediately stale.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._registry = MemberRegistry()
        self._ring = Ring.empty()
        self._ring_changed = asyncio.Condition()

        # Fingerprint cache; None means the cache is dirty.
        self._fingerprint: str | None = None

    async def snapshot(self) -> Topology:
        """Return an immutable Topology snapshot acquired under the lock."""
        async with self._lock:
            return Topology(
                registry=self._registry.snapshot(),
                ring=self._ring,
            )

    async def apply_member(self, member: Member) -> bool:
        """Apply a single gossip record to the registry and ring.

        Return True if the registry or ring was modified. Return False if the
        record is equal to or older than the current entry (idempotent no-op).
        """
        async with self._lock:
            changed = self._apply(member)

        if changed:
            async with self._ring_changed:
                self._ring_changed.notify_all()

        return changed

    async def merge_registry(self, members: Iterable[Member]) -> int:
        """Apply each Member via _apply() and return the total count accepted.

        Performed under a single lock acquisition so that snapshots always
        reflect a fully merged state and never observe a partial merge.
        Return value mirrors MemberRegistry.upsert() semantics: only records
        that supersede the current version are counted. Used by the bootstrap
        path and the gossip.delta handler.
        """
        accepted = 0
        async with self._lock:
            for member in members:
                if self._apply(member):
                    accepted += 1

        if accepted:
            async with self._ring_changed:
                self._ring_changed.notify_all()

        return accepted

    async def member_fingerprint(self) -> str:
        """Return a SHA-256 fingerprint of the member registry.

        The fingerprint is computed lazily and cached. It is invalidated on
        every accepted mutation. GossipEngine always calls this method
        immediately before building a gossip.ping payload, never reading it
        from a Topology snapshot.

        The algorithm is: for each member sorted alphabetically by node_id,
        hash node_id_utf8 + uint64_be(epoch).
        epoch is intentionally excluded to avoid spurious full-member syncs
        on ring mutations where no member record changed.
        """
        async with self._lock:
            if self._fingerprint is None:
                self._fingerprint = self._compute_fingerprint()
            return self._fingerprint  # type: ignore[return-value]

    async def wait_for_ring_change(self, serial: str) -> Ring:
        async with self._ring_changed:
            if serial != self._ring.serial():
                return self._ring
            await self._ring_changed.wait()
        return self._ring

    def _apply(self, member: Member) -> bool:
        """Apply member under the already-held lock; return True if modified."""
        old = self._registry.get(member.node_id)
        if not self._registry.upsert(member):
            current_epoch = -1 if old is None else old.epoch
            logger.debug(
                "topology.apply stale member ignored node_id=%s incoming_epoch=%s current_epoch=%s",
                member.node_id,
                member.epoch,
                current_epoch,
            )
            return False
        self._fingerprint = None  # invalidate cache on any accepted mutation
        return self._update_ring(old, member)

    def _update_ring(self, old: Member | None, new: Member) -> bool:
        """Derive the ring from the latest member state, not from transitions.

        Gossip only guarantees that every node ends up with the latest version
        of each member; intermediate phases may never be observed. The member's
        vnodes are therefore always dropped, then added back when its phase owns
        data.
        """
        old_phase = old.phase if old is not None else MemberPhase.IDLE
        previous_ring = self._ring

        # todo(souls): O(n) per accepted update (~0.5s at 10k nodes x 32 vnodes:
        #  drop_nodes ~50ms, add_vnodes ~95ms, serial() ~180ms twice), blocking
        #  the event loop; merge_registry at bootstrap is O(n^2). Options: skip
        #  when ring membership and tokens are unchanged, bisect-based add/drop,
        #  incremental serial, single ring rebuild in merge_registry.
        ring = previous_ring.drop_nodes({new.node_id})
        if new.phase in RING_PHASES:
            ring = ring.add_vnodes(new.vnodes)
        self._ring = ring

        if ring.serial() == previous_ring.serial():
            return False

        logger.info(
            "topology.ring updated node_id=%s from=%s to=%s size=%s",
            new.node_id,
            old_phase.value,
            new.phase.value,
            len(ring),
        )
        return True

    def _compute_fingerprint(self) -> str:
        """Compute the SHA-256 fingerprint of the member registry under the lock."""
        digest = hashlib.sha256()
        for member in sorted(self._registry, key=lambda m: m.node_id):
            digest.update(member.node_id.encode("utf-8"))
            digest.update(member.epoch.to_bytes(8, byteorder="big", signed=False))
        return digest.hexdigest()

