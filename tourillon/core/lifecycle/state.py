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
"""NodeState dataclass — in-memory view of state.toml."""

from __future__ import annotations

from dataclasses import dataclass

from tourillon.core.lifecycle.member import MemberPhase


@dataclass(frozen=True)
class NodeState:
    """Durable snapshot of a node's lifecycle state, mapped to state.toml.

    Maps directly to the [node] and [topology] sections of state.toml.
    Constructed exclusively by StatePort implementors and the bootstrap
    sequence; never built ad-hoc in domain code.

    tokens is empty for a fresh IDLE node and populated exactly once at the
    start of the join transition (IDLE → READY for the first node;
    IDLE → JOINING for seeded join). epoch is 0 before the first transition
    that advances it.
    """

    phase: MemberPhase
    generation: int
    seq: int
    tokens: tuple[int, ...]  # empty for IDLE; populated at join transition
    epoch: int  # topology version from [topology].epoch
