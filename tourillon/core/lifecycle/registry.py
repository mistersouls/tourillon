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
"""MemberRegistry — pure node_id → Member key-value store."""

from __future__ import annotations

from collections.abc import Iterator

from tourillon.core.lifecycle.member import Member, MemberPhase


class MemberRegistry:
    """Pure key-value store for Member records indexed by node_id.

    This class has exactly one responsibility: storing and retrieving Member
    values. It carries no gossip knowledge, no propagation counters, and no
    eviction policy. Tombstone eviction is out of scope for this proposal.

    None of the methods here are thread-safe. All callers must hold the
    TopologyManager lock before mutating the registry.
    """

    def __init__(self) -> None:
        self._members: dict[str, Member] = {}

    def upsert(self, member: Member) -> bool:
        """Insert or replace if member supersedes the current entry.

        Return True if the registry was modified, False if the incoming
        record is equal to or older than the one already stored.
        """
        current = self._members.get(member.node_id)
        if current is not None and not member.supersedes(current):
            return False
        self._members[member.node_id] = member
        return True

    def get(self, node_id: str) -> Member | None:
        """Return the Member for node_id, or None if absent."""
        return self._members.get(node_id)

    def members_in_phase(self, *phases: MemberPhase) -> dict[str, Member]:
        """Return a node_id → Member mapping for members matching any phase."""
        phase_set = set(phases)
        return {nid: m for nid, m in self._members.items() if m.phase in phase_set}

    def snapshot(self) -> MemberRegistry:
        """Return a shallow copy safe to read outside the TopologyManager lock.

        The copy captures the registry at this instant. Subsequent mutations
        of the original do not affect the copy.
        """
        copy = MemberRegistry()
        copy._members = dict(self._members)
        return copy

    def __len__(self) -> int:
        return len(self._members)

    def __iter__(self) -> Iterator[Member]:
        return iter(self._members.values())
