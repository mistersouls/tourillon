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
"""Replica placement strategy implementations."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.ring.partitioner import PartitionPlacement
from tourillon.core.ring.vnode import VNode

if TYPE_CHECKING:
    from tourillon.core.lifecycle.probe import ProbeManager
    from tourillon.core.ring.topology import Topology

_EXCLUDED_PHASES = frozenset(
    {MemberPhase.IDLE, MemberPhase.JOINING, MemberPhase.FAILED}
)
_ALWAYS_HANDOFF_PHASES = frozenset({MemberPhase.DRAINING, MemberPhase.PAUSED})


@dataclass(frozen=True)
class PreferenceEntry:
    node_id: str
    readable: bool
    suspect: bool
    handoff: str | None


class PlacementStrategy(Protocol):
    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: Topology,
        probe_manager: ProbeManager,
    ) -> list[PreferenceEntry]: ...


class SimplePreferenceStrategy:
    def __init__(self, rf: int) -> None:
        self._rf = rf

    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: Topology,
        probe_manager: ProbeManager,
    ) -> list[PreferenceEntry]:
        used: set[str] = set()
        members: list[Member] = []
        start = placement.vnode
        last_vnode = start

        for vnode in topology.ring.iter_from(start):
            member = topology.registry.get(vnode.node_id)
            if member is None or member.phase in _EXCLUDED_PHASES:
                continue
            if member.node_id in used:
                continue
            members.append(member)
            used.add(member.node_id)
            last_vnode = vnode
            if len(members) == self._rf:
                break

        handoff_candidates = self._handoff_candidates(
            start=last_vnode,
            topology=topology,
            probe_manager=probe_manager,
            used=used,
        )

        result: list[PreferenceEntry] = []
        for member in members:
            suspect = await probe_manager.is_suspect(member.node_id)
            readable = member.phase in (MemberPhase.READY, MemberPhase.DRAINING)
            needs_handoff = member.phase in _ALWAYS_HANDOFF_PHASES or (
                member.phase is MemberPhase.READY and suspect
            )
            handoff = await anext(handoff_candidates, None) if needs_handoff else None
            result.append(
                PreferenceEntry(
                    node_id=member.node_id,
                    readable=readable,
                    suspect=suspect,
                    handoff=handoff,
                )
            )
        return result

    @staticmethod
    async def _handoff_candidates(
        start: VNode,
        topology: Topology,
        probe_manager: ProbeManager,
        used: set[str],
    ) -> AsyncIterator[str]:
        consumed = set(used)
        for vnode in topology.ring.iter_from(start):
            member = topology.registry.get(vnode.node_id)
            if member is None or member.phase in _EXCLUDED_PHASES:
                continue
            if member.node_id in consumed:
                continue
            if await probe_manager.is_suspect(member.node_id):
                continue
            consumed.add(member.node_id)
            yield member.node_id
