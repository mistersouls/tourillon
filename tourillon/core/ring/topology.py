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

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.registry import MemberRegistry
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode

# Phases whose vnodes are present in the active routing ring once entered.
# JOINING and IDLE nodes are never in the ring.
_RING_ENTRY_SOURCES = frozenset({MemberPhase.IDLE, MemberPhase.JOINING})

# Transitions that do NOT advance the topology epoch.
_NO_EPOCH = frozenset({(MemberPhase.IDLE, MemberPhase.JOINING)})


@dataclass(frozen=True)
class Topology:
    """Immutable point-in-time snapshot of the cluster topology.

    Safe to hold across await points. Obtained via TopologyManager.snapshot().
    The active ring contains the vnodes of READY, DRAINING, PAUSED, and FAILED
    nodes. JOINING and IDLE vnodes are absent from the ring.

    registry holds a shallow copy of MemberRegistry produced at the time of
    the TopologyManager lock acquisition. It is safe to iterate and query
    without holding any lock.
    """

    epoch: int
    registry: MemberRegistry
    ring: Ring

    def members_in_phase(self, *phases: MemberPhase) -> dict[str, Member]:
        """Return a node_id → Member mapping for members matching any phase."""
        return self.registry.members_in_phase(*phases)

    @property
    def active_node_ids(self) -> frozenset[str]:
        """Return node_ids of READY, DRAINING, and PAUSED members."""
        active = (MemberPhase.READY, MemberPhase.DRAINING, MemberPhase.PAUSED)
        return frozenset(m.node_id for m in self.registry if m.phase in active)


class TopologyManager:
    """Stateful manager for the active ring and MemberRegistry.

    All mutations go through apply_member or merge_registry. Both methods
    compare the incoming Member against the current registry entry using
    member.supersedes() and, when the record is newer, update the registry
    and derive the necessary ring mutation from the observed phase transition
    entirely internally. Callers never manipulate the ring directly.

    Ring mutation rules applied automatically by apply_member:
      * → READY (from IDLE/JOINING)  : ring.add_vnodes; epoch++.
      DRAINING → IDLE                : ring.drop_nodes; epoch++.
      IDLE → JOINING                 : no ring change; no epoch change.
      Any other transition           : no ring change; epoch++.

    Snapshot contract: snapshot() acquires the lock, builds an immutable
    Topology from MemberRegistry.snapshot() and the current Ring, releases
    the lock, then returns the frozen Topology.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._registry = MemberRegistry()
        self._ring = Ring.empty()
        self._epoch = 0

    async def snapshot(self) -> Topology:
        """Return an immutable Topology snapshot acquired under the lock."""
        async with self._lock:
            return Topology(
                epoch=self._epoch,
                registry=self._registry.snapshot(),
                ring=self._ring,
            )

    async def apply_member(self, member: Member) -> bool:
        """Apply a single gossip record to the registry and ring.

        Return True if the registry or ring was modified. Return False if the
        record is equal to or older than the current entry (idempotent no-op).
        """
        async with self._lock:
            return self._apply(member)

    async def merge_registry(self, registry: MemberRegistry) -> None:
        """Merge an externally received MemberRegistry atomically.

        Equivalent to calling apply_member for every entry in registry, but
        performed under a single lock acquisition so that snapshots always
        reflect a fully merged state and never observe a partial merge.
        """
        async with self._lock:
            for member in registry:
                self._apply(member)

    def _apply(self, member: Member) -> bool:
        """Apply member under the already-held lock; return True if modified."""
        old = self._registry.get(member.node_id)
        if not self._registry.upsert(member):
            return False
        self._update_ring_and_epoch(old, member)
        return True

    def _update_ring_and_epoch(self, old: Member | None, new: Member) -> None:
        """Derive ring mutation and epoch increment from the phase transition."""
        old_phase = old.phase if old is not None else MemberPhase.IDLE
        new_phase = new.phase

        if (old_phase, new_phase) in _NO_EPOCH:
            return

        if new_phase == MemberPhase.READY and old_phase in _RING_ENTRY_SOURCES:
            vnodes = [VNode(new.node_id, t) for t in new.tokens]
            self._ring = self._ring.add_vnodes(vnodes)
            self._epoch += 1
        elif new_phase == MemberPhase.IDLE and old_phase == MemberPhase.DRAINING:
            self._ring = self._ring.drop_nodes({new.node_id})
            self._epoch += 1
        else:
            self._epoch += 1
