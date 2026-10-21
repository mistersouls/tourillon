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

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

from tourillon.core.gossip.topology import Topology, TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.types import PartitionPlacement
from tourillon.core.peer.pool import PeerPool
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode
from tourlib.models.version import Address

# Phases ineligible to appear in any preference list (primary or handoff).
_EXCLUDED_PHASES = frozenset(
    {MemberPhase.IDLE, MemberPhase.JOINING, MemberPhase.FAILED}
)

# Phases that always require a handoff target regardless of fd state.
_ALWAYS_HANDOFF_PHASES = frozenset({MemberPhase.DRAINING, MemberPhase.PAUSED})


@dataclass(frozen=True)
class Preference:
    """Owner capabilities and an optional substitute, from the local view.

    Reads query the readable owner and any handoff. Writes prefer the handoff,
    otherwise use the owner only when writable. A missing handoff does not
    imply writability. Failure suspicion requests a handoff but does not change
    the owner's phase-based capabilities; receivers still enforce admission.
    """

    node_id: str
    readable: bool
    handoff: str | None
    writable: bool


@dataclass(frozen=True)
class PreferenceList:
    placement: PartitionPlacement
    preferences: tuple[Preference, ...]


class PlacementStrategy(Protocol):
    async def placement_for(self, addr: Address) -> PartitionPlacement:
        """Return the placement for a given token."""

    async def preference_list(self, addr: Address) -> PreferenceList:
        """Return the preference list for a given token, including handoff candidates."""


class SimplePlacementStrategy:
    def __init__(
        self,
        replication_factor: int,
        topologymgr: TopologyManager,
        partitioner: Partitioner,
        peer_pool: PeerPool,
    ) -> None:
        self._rf = replication_factor
        self._topologymgr = topologymgr
        self._partitioner = partitioner
        self._peer_pool = peer_pool

    async def placement_for(self, addr: Address) -> PartitionPlacement:
        topology = await self._topologymgr.snapshot()
        return self._placement_addr(addr, topology.ring)

    async def preference_list(self, addr: Address) -> PreferenceList:
        topology = await self._topologymgr.snapshot()
        ring = topology.ring
        placement = self._placement_addr(addr, ring)

        seen: set[str] = set()
        members: list[Member] = []
        start = placement.vnode
        last_vnode = start

        for vnode in ring.iter_from(start):
            member = topology.registry.get(vnode.node_id)
            if member is None:
                logger.warning(
                    "vnode present in ring but missing from registry",
                    extra={"node_id": vnode.node_id, "token": vnode.token},
                )
                continue
            if member.phase in _EXCLUDED_PHASES:
                continue
            if member.node_id not in seen:
                members.append(member)
                seen.add(member.node_id)
                last_vnode = vnode
            if len(members) == self._rf:
                break

        preferences: list[Preference] = []
        candidates = self._handoff_candidates(last_vnode, topology, seen)

        for member in members:
            fd = self._peer_pool.failure_detector(member.node_id)
            suspect = False if fd is None else not fd.is_alive()
            readable = member.phase in (MemberPhase.READY, MemberPhase.DRAINING)
            needs_handoff = member.phase in _ALWAYS_HANDOFF_PHASES or (
                member.phase == MemberPhase.READY and suspect
            )
            handoff = await anext(candidates, None) if needs_handoff else None
            preferences.append(
                Preference(
                    node_id=member.node_id,
                    readable=readable,
                    handoff=handoff,
                    writable=member.phase == MemberPhase.READY,
                )
            )

        return PreferenceList(placement=placement, preferences=tuple(preferences))

    async def _handoff_candidates(
        self,
        start: VNode,
        topology: Topology,
        used: set[str],
    ) -> AsyncIterator[str]:
        seen = set(used)
        for vnode in topology.ring.iter_from(start):
            member = topology.registry.get(vnode.node_id)
            if member is None:
                logger.warning(
                    "vnode present in ring but missing from registry",
                    extra={"node_id": vnode.node_id, "token": vnode.token},
                )
                continue
            # PAUSED and DRAINING members are under handoff themselves.
            if member.phase != MemberPhase.READY:
                continue
            if member.node_id in seen:
                continue
            if not self._is_alive(member.node_id):
                continue
            seen.add(vnode.node_id)
            yield vnode.node_id

    def _is_alive(self, node_id: str) -> bool:
        """Return True if the node is considered alive by the local failure detector."""
        fd = self._peer_pool.failure_detector(node_id)
        return True if fd is None else fd.is_alive()

    def _placement_addr(self, addr: Address, ring: Ring) -> PartitionPlacement:
        token = self._partitioner.space.hash(addr.to_bytes())
        pid = self._partitioner.pid_for_hash(token)
        group = self._partitioner.group_for(pid)
        partition = self._partitioner.partition_for(pid)
        vnode = ring.successor(partition.end)
        return PartitionPlacement(group=group, vnode=vnode, partition=partition)
