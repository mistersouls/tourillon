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
"""PreferenceEntry, PlacementStrategy Protocol, and SimplePreferenceStrategy."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.ring.partitioner import PartitionPlacement
from tourillon.core.ring.vnode import VNode

if TYPE_CHECKING:
    from tourillon.core.lifecycle.probe import ProbeManager
    from tourillon.core.ring.topology import Topology

logger = logging.getLogger(__name__)

# Phases ineligible to appear in any preference list (primary or handoff).
_EXCLUDED_PHASES = frozenset(
    {MemberPhase.IDLE, MemberPhase.JOINING, MemberPhase.FAILED}
)

# Phases that always require a handoff target regardless of probe state.
_ALWAYS_HANDOFF_PHASES = frozenset({MemberPhase.DRAINING, MemberPhase.PAUSED})


@dataclass(frozen=True)
class PreferenceEntry:
    """One entry in a replication preference list for a partition placement.

    readable is True when reads are permitted (phase READY or DRAINING).
    suspect is True when the local failure detector suspects this node.
    handoff is the node_id of the temporary write target when this node
    cannot safely accept its own writes; None for fully healthy replicas.

    Every node_id across the entire preference list — whether primary or
    handoff — appears at most once. A node already listed as a primary
    replica is not also listed as a handoff target, and vice versa.
    """

    node_id: str
    readable: bool
    suspect: bool
    handoff: str | None


class PlacementStrategy(Protocol):
    """Strategy for computing replication preference lists."""

    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: Topology,
        probe_manager: ProbeManager,
    ) -> list[PreferenceEntry]:
        """Return the ordered preference list for placement."""
        ...


class SimplePreferenceStrategy:
    """Default rf-aware preference list builder.

    Walks the ring clockwise from placement.vnode, collecting up to rf unique
    physical nodes that are eligible (not IDLE, JOINING, or FAILED). For each
    collected node, determines readability and whether a handoff is needed
    based on the node's phase and the local probe state. A second clockwise
    walk from the last primary position finds handoff targets.

    The result is a deterministic function of (placement, topology, probe_manager):
    identical inputs always produce identical outputs.
    """

    def __init__(self, rf: int) -> None:
        """Initialise with replication factor rf."""
        self._rf = rf

    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: Topology,
        probe_manager: ProbeManager,
    ) -> list[PreferenceEntry]:
        """Return the preference list for placement."""
        seen: set[str] = set()
        members: list[Member] = []
        start = placement.vnode
        last_vnode = start

        for vnode in topology.ring.iter_from(start):
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

        preferences: list[PreferenceEntry] = []
        candidates = self._handoff_candidates(last_vnode, topology, probe_manager, seen)

        for member in members:
            suspect = await probe_manager.is_suspect(member.node_id)
            readable = member.phase in (MemberPhase.READY, MemberPhase.DRAINING)
            needs_handoff = member.phase in _ALWAYS_HANDOFF_PHASES or (
                member.phase == MemberPhase.READY and suspect
            )
            handoff = await anext(candidates, None) if needs_handoff else None
            preferences.append(
                PreferenceEntry(
                    node_id=member.node_id,
                    readable=readable,
                    suspect=suspect,
                    handoff=handoff,
                )
            )

        return preferences

    @staticmethod
    async def _handoff_candidates(
        start: VNode,
        topology: Topology,
        probe_manager: ProbeManager,
        used: set[str],
    ) -> AsyncIterator[str]:
        """Yield eligible handoff node_ids not already in used.

        Walks clockwise from start, skipping IDLE, JOINING, FAILED, and SUSPECT
        nodes, as well as any node_id already consumed as a primary or handoff
        target. A SUSPECT node is excluded: forwarding writes to a node that
        the local failure detector already suspects would defeat the purpose of
        hinted handoff.
        """
        seen = set(used)
        for vnode in topology.ring.iter_from(start):
            member = topology.registry.get(vnode.node_id)
            if member is None:
                logger.warning(
                    "vnode present in ring but missing from registry",
                    extra={"node_id": vnode.node_id, "token": vnode.token},
                )
                continue
            if member.phase in _EXCLUDED_PHASES:
                continue
            if member.node_id in seen:
                continue
            if await probe_manager.is_suspect(member.node_id):
                continue
            seen.add(vnode.node_id)
            yield vnode.node_id
