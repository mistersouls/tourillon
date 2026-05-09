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
"""SimplePreferenceStrategy tests — scenarios 5, 6."""

from __future__ import annotations

import pytest

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.ring.partitioner import LogicalPartition, PartitionPlacement
from tourillon.core.ring.placement import SimplePreferenceStrategy
from tourillon.core.ring.topology import Topology, TopologyManager
from tourillon.core.ring.vnode import VNode

pytestmark = pytest.mark.ring


def _member(node_id: str, phase: MemberPhase, token: int, seq: int = 0) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:7701",
        generation=1,
        seq=seq,
        phase=phase,
        tokens=(token,),
    )


async def _topology_5_nodes() -> tuple[Topology, VNode]:
    """Build 5-node topology: A=READY, B=JOINING, C=READY, D=DRAINING, E=PAUSED.

    D and E must go through READY first so their vnodes are present in the
    routing ring before they transition to their target phases.
    """
    mgr = TopologyManager()

    # A and C enter the ring directly via IDLE → READY.
    await mgr.apply_member(_member("A", MemberPhase.READY, 20, seq=0))
    # B is JOINING: vnodes stored in registry but not in ring.
    await mgr.apply_member(_member("B", MemberPhase.JOINING, 40, seq=0))
    await mgr.apply_member(_member("C", MemberPhase.READY, 80, seq=0))
    # D: READY first (seq=0), then DRAINING (seq=1) — vnodes stay in ring.
    await mgr.apply_member(_member("D", MemberPhase.READY, 120, seq=0))
    await mgr.apply_member(_member("D", MemberPhase.DRAINING, 120, seq=1))
    # E: READY first (seq=0), then PAUSED (seq=1) — vnodes stay in ring.
    await mgr.apply_member(_member("E", MemberPhase.READY, 160, seq=0))
    await mgr.apply_member(_member("E", MemberPhase.PAUSED, 160, seq=1))

    snap = await mgr.snapshot()
    start_vnode = VNode("A", 20)
    return snap, start_vnode


@pytest.mark.ring
async def test_5_preference_list_excludes_joining_includes_ready_draining_paused() -> (
    None
):
    """A, C, D readable; E present in PL with readable=False; B absent."""
    topology, start_vnode = await _topology_5_nodes()
    probe = ProbeManager()
    strategy = SimplePreferenceStrategy(rf=4)

    placement = PartitionPlacement(
        partition=LogicalPartition(pid=0, start=0, end=10),
        vnode=start_vnode,
    )

    result = await strategy.preference_list(placement, topology, probe)

    node_ids = [e.node_id for e in result]
    assert "B" not in node_ids, "JOINING node must not appear in preference list"

    by_id = {e.node_id: e for e in result}

    assert by_id["A"].readable is True
    assert by_id["C"].readable is True
    assert by_id["D"].readable is True
    assert by_id["E"].readable is False


@pytest.mark.ring
async def test_6_preference_list_suspect_node_gets_handoff_no_duplicates() -> None:
    """A and D readable; one PreferenceEntry(handoff=*) for C; no node_id appears > once."""
    topology, start_vnode = await _topology_5_nodes()
    import time

    probe = ProbeManager()
    # Record a real heartbeat first so _intervals is populated, then manipulate
    # _last_arrival to make phi() exceed the threshold (8.0) and mark C as SUSPECT.
    await probe.record_heartbeat("C")
    await probe.record_heartbeat("C")  # second call populates _intervals
    detector = probe._detectors["C"]  # noqa: SLF001
    # Inject an old last_arrival so elapsed time dwarfs the mean interval.
    detector._last_arrival = time.monotonic() - 1000  # noqa: SLF001

    strategy = SimplePreferenceStrategy(rf=4)

    placement = PartitionPlacement(
        partition=LogicalPartition(pid=0, start=0, end=10),
        vnode=start_vnode,
    )

    result = await strategy.preference_list(placement, topology, probe)

    node_ids = [e.node_id for e in result]
    handoff_ids = [e.handoff for e in result if e.handoff is not None]

    # No node_id appears more than once across primary + handoff positions
    all_ids = node_ids + handoff_ids
    assert len(all_ids) == len(set(all_ids)), f"Duplicate node_ids: {all_ids}"

    # C is suspect and should have a handoff target
    by_id = {e.node_id: e for e in result}
    if "C" in by_id:
        assert by_id["C"].suspect is True
