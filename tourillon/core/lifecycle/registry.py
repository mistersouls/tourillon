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
"""Pure member registry."""

from __future__ import annotations

from collections.abc import Iterator

from tourillon.core.lifecycle.member import Member, MemberPhase


class MemberRegistry:
    def __init__(self) -> None:
        self._members: dict[str, Member] = {}

    def upsert(self, member: Member) -> bool:
        current = self._members.get(member.node_id)
        if current is not None and not member.supersedes(current):
            return False
        self._members[member.node_id] = member
        return True

    def get(self, node_id: str) -> Member | None:
        return self._members.get(node_id)

    def members_in_phase(self, *phases: MemberPhase) -> dict[str, Member]:
        selected = set(phases)
        return {
            nid: member
            for nid, member in self._members.items()
            if member.phase in selected
        }

    def snapshot(self) -> MemberRegistry:
        snap = MemberRegistry()
        snap._members = dict(self._members)
        return snap

    def __len__(self) -> int:
        return len(self._members)

    def __iter__(self) -> Iterator[Member]:
        return iter(self._members.values())
