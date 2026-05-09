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
"""MemberPhase FSM enumeration and Member gossip record."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class MemberPhase(StrEnum):
    """Lifecycle phase of a cluster member, self-declared and gossiped.

    Phases are totally ordered by the state machine; not every transition is
    legal. Illegal transitions are caught by the phase guards in the bootstrap
    and gossip layers. This enumeration is the single source of truth for
    every phase check in the codebase.
    """

    IDLE = "idle"
    JOINING = "joining"
    READY = "ready"
    PAUSED = "paused"
    DRAINING = "draining"
    FAILED = "failed"


@dataclass(frozen=True)
class Member:
    """Immutable gossip-exchange record describing a single cluster member.

    tokens holds the ordered sequence of hash-space positions claimed by this
    node. It is populated exactly once at the start of the join transition
    (IDLE → READY for the first node; IDLE → JOINING for seeded join) and
    never changes afterwards. An IDLE node that has not yet started carries
    an empty tuple.

    supersedes() is the canonical comparator used by MemberRegistry.upsert()
    and by the gossip merge logic. Two records for the same node_id are
    compared on (generation, seq): a higher generation always wins regardless
    of seq; within the same generation, higher seq wins.
    """

    node_id: str
    peer_address: str
    generation: int
    seq: int
    phase: MemberPhase
    tokens: tuple[int, ...]  # empty for IDLE; populated at join transition

    def supersedes(self, other: Member) -> bool:
        """Return True when self is strictly newer than other.

        Precedence is the lexicographic pair (generation, seq). A self with
        a higher generation always supersedes regardless of seq. Within the
        same generation, higher seq wins.
        """
        return (self.generation, self.seq) > (other.generation, other.seq)
