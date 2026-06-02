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
"""Topology snapshot and stateful topology manager."""

from __future__ import annotations

import asyncio
import hashlib
import struct
from collections.abc import Iterable
from dataclasses import dataclass

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.registry import MemberRegistry
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode

_RING_ENTRY_SOURCES = frozenset({MemberPhase.IDLE, MemberPhase.JOINING})


@dataclass(frozen=True)
class Topology:
    epoch: int
    registry: MemberRegistry
    ring: Ring

    def members_in_phase(self, *phases: MemberPhase) -> dict[str, Member]:
        return self.registry.members_in_phase(*phases)

    @property
    def active_node_ids(self) -> frozenset[str]:
        active = {MemberPhase.READY, MemberPhase.DRAINING, MemberPhase.PAUSED}
        return frozenset(
            member.node_id for member in self.registry if member.phase in active
        )


class TopologyManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._registry = MemberRegistry()
        self._ring = Ring.empty()
        self._epoch = 0
        self._fingerprint: str | None = None

    async def snapshot(self) -> Topology:
        async with self._lock:
            return Topology(
                epoch=self._epoch,
                registry=self._registry.snapshot(),
                ring=self._ring,
            )

    async def apply_member(self, member: Member) -> bool:
        async with self._lock:
            return self._apply(member)

    async def merge_registry(self, members: Iterable[Member]) -> int:
        accepted = 0
        async with self._lock:
            for member in members:
                accepted += int(self._apply(member))
        return accepted

    async def member_fingerprint(self) -> str:
        async with self._lock:
            if self._fingerprint is None:
                self._fingerprint = self._compute_fingerprint()
            return self._fingerprint

    def _apply(self, member: Member) -> bool:
        old = self._registry.get(member.node_id)
        if not self._registry.upsert(member):
            return False

        self._fingerprint = None

        old_phase = old.phase if old is not None else MemberPhase.IDLE
        new_phase = member.phase

        if old_phase in _RING_ENTRY_SOURCES and new_phase is MemberPhase.READY:
            self._ring = self._ring.add_vnodes(
                [VNode(node_id=member.node_id, token=token) for token in member.tokens]
            )
            self._epoch += 1
            return True

        if old_phase is MemberPhase.DRAINING and new_phase is MemberPhase.IDLE:
            self._ring = self._ring.drop_nodes({member.node_id})
            self._epoch += 1
            return True

        if old_phase is MemberPhase.IDLE and new_phase is MemberPhase.JOINING:
            return True

        self._epoch += 1
        return True

    def _compute_fingerprint(self) -> str:
        digest = hashlib.sha256()
        members = sorted(self._registry, key=lambda member: member.node_id)
        for member in members:
            node_bytes = member.node_id.encode("utf-8")
            digest.update(struct.pack(">I", len(node_bytes)))
            digest.update(node_bytes)
            digest.update(struct.pack(">Q", member.generation))
            digest.update(struct.pack(">Q", member.seq))
        return digest.hexdigest()
