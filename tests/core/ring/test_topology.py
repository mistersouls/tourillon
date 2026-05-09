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
"""TopologyManager tests — scenarios 7, 8, 15."""

from __future__ import annotations

import pytest

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.registry import MemberRegistry
from tourillon.core.ring.topology import TopologyManager

pytestmark = pytest.mark.ring


def _member(
    node_id: str,
    phase: MemberPhase,
    tokens: tuple[int, ...] = (10, 50),
    generation: int = 1,
    seq: int = 0,
) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:7701",
        generation=generation,
        seq=seq,
        phase=phase,
        tokens=tokens,
    )


@pytest.mark.ring
async def test_7_apply_member_joining_to_ready_adds_vnodes_and_advances_epoch() -> None:
    """Returns True; snapshot ring includes B's vnodes; epoch advanced by 1."""
    mgr = TopologyManager()

    # B starts JOINING
    b_joining = _member("node-b", MemberPhase.JOINING, tokens=(30, 90))
    await mgr.apply_member(b_joining)
    snap_before = await mgr.snapshot()
    assert snap_before.epoch == 0  # JOINING does not advance epoch
    assert len(snap_before.ring) == 0  # not in ring yet

    # B transitions to READY
    b_ready = _member("node-b", MemberPhase.READY, tokens=(30, 90), seq=1)
    modified = await mgr.apply_member(b_ready)

    assert modified is True
    snap = await mgr.snapshot()
    assert snap.epoch == 1
    ring_ids = {v.node_id for v in snap.ring}
    assert "node-b" in ring_ids
    assert len(snap.ring) == 2  # two tokens


@pytest.mark.ring
async def test_8_apply_member_same_record_twice_returns_false() -> None:
    """Returns False; snapshot unchanged."""
    mgr = TopologyManager()
    member = _member("node-b", MemberPhase.READY)
    await mgr.apply_member(member)
    snap_before = await mgr.snapshot()

    modified = await mgr.apply_member(member)

    assert modified is False
    snap_after = await mgr.snapshot()
    assert snap_after.epoch == snap_before.epoch


@pytest.mark.ring
async def test_15_merge_registry_is_atomic() -> None:
    """A snapshot taken immediately after contains all 5 new members; no partial merge."""
    mgr = TopologyManager()

    registry = MemberRegistry()
    for i in range(5):
        m = _member(f"node-{i}", MemberPhase.READY, tokens=(i * 10,))
        registry.upsert(m)

    await mgr.merge_registry(registry)

    snap = await mgr.snapshot()
    assert len(list(snap.registry)) == 5
    for i in range(5):
        assert snap.registry.get(f"node-{i}") is not None
