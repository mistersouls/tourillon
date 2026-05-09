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
"""MemberRegistry tests — scenarios 13, 14."""

from __future__ import annotations

import pytest

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.registry import MemberRegistry

pytestmark = pytest.mark.ring


def _member(node_id: str, phase: MemberPhase, seq: int = 0) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:7701",
        generation=1,
        seq=seq,
        phase=phase,
        tokens=(42,),
    )


@pytest.mark.ring
def test_13_snapshot_is_independent_of_original() -> None:
    """Snapshot is unaffected by mutations of the original registry."""
    registry = MemberRegistry()
    registry.upsert(_member("A", MemberPhase.READY))
    snap = registry.snapshot()

    # mutate original
    registry.upsert(_member("B", MemberPhase.READY))
    registry.upsert(_member("A", MemberPhase.DRAINING, seq=1))

    assert snap.get("B") is None, "Snapshot should not see B added after snapshot"
    assert snap.get("A") is not None
    assert snap.get("A").phase == MemberPhase.READY  # type: ignore[union-attr]


@pytest.mark.ring
def test_14_members_in_phase_filters_correctly() -> None:
    """Returns {"A": member_a, "B": member_b}; JOINING/IDLE/FAILED members absent."""
    registry = MemberRegistry()
    registry.upsert(_member("A", MemberPhase.READY))
    registry.upsert(_member("B", MemberPhase.DRAINING))
    registry.upsert(_member("C", MemberPhase.JOINING))
    registry.upsert(_member("D", MemberPhase.IDLE))
    registry.upsert(_member("E", MemberPhase.FAILED))

    result = registry.members_in_phase(MemberPhase.READY, MemberPhase.DRAINING)

    assert set(result.keys()) == {"A", "B"}
    assert result["A"].phase == MemberPhase.READY
    assert result["B"].phase == MemberPhase.DRAINING
