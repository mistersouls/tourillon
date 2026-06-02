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

from __future__ import annotations

import asyncio

import pytest

from tests.helpers.adapters import InMemoryStateAdapter
from tourillon.core.lifecycle.bootstrap import Bootstraper, BootstrapError
from tourillon.core.lifecycle.checks import (
    NodeIdMismatchError,
    check_node_id_consistency,
)
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.phi import FailureDetector
from tourillon.core.lifecycle.probe import MemberState, ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StateError
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import (
    LogicalPartition,
    Partitioner,
    PartitionPlacement,
    PartitionRange,
)
from tourillon.core.ring.placement import SimplePreferenceStrategy
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.ring.vnode import VNode
from tourillon.core.structure.config import (
    NodeSize,
    ServerConfig,
    TlsConfig,
    TourillonConfig,
)
from tourillon.infra.store.state import FileStateAdapter


@pytest.mark.ring
def test_01_hash_in_range() -> None:
    h = HashSpace(bits=8).hash(b"hello")
    assert 0 <= h < 256


@pytest.mark.ring
def test_02_hash_deterministic() -> None:
    hs = HashSpace(bits=8)
    assert hs.hash(b"x") == hs.hash(b"x")


@pytest.mark.ring
def test_03_hashspace_bits_validation() -> None:
    with pytest.raises(ValueError, match="bits must be >= 1"):
        HashSpace(bits=0)


@pytest.mark.ring
def test_04_successor_empty_ring() -> None:
    with pytest.raises(ValueError):
        Ring.empty().successor(0)


@pytest.mark.ring
def test_05_successor_wrap_around() -> None:
    ring = Ring([VNode("a", 10), VNode("a", 200)])
    assert ring.successor(300) == VNode("a", 10)


@pytest.mark.ring
def test_06_add_vnodes_is_immutable() -> None:
    ring = Ring([VNode("a", 10)])
    new_ring = ring.add_vnodes([VNode("b", 5)])
    assert len(ring) == 1
    assert [v.token for v in new_ring] == [5, 10]


@pytest.mark.ring
def test_07_drop_nodes() -> None:
    ring = Ring([VNode("a", 10), VNode("b", 20)])
    assert list(ring.drop_nodes({"a"})) == [VNode("b", 20)]


@pytest.mark.ring
def test_08_pid_for_hash() -> None:
    p = Partitioner(HashSpace(bits=8), 4, 1)
    assert p.pid_for_hash(0xFF) == 15


@pytest.mark.ring
def test_09_partitioner_partition_shift_validation() -> None:
    with pytest.raises(ValueError):
        Partitioner(HashSpace(bits=8), 8, 1)


@pytest.mark.ring
def test_10_partitioner_segment_shift_validation() -> None:
    with pytest.raises(ValueError):
        Partitioner(HashSpace(bits=8), 4, 4)


@pytest.mark.ring
def test_11_partition_for_zero() -> None:
    p = Partitioner(HashSpace(bits=8), 4, 1)
    assert p.partition_for(0) == LogicalPartition(pid=0, start=0, end=16)


@pytest.mark.ring
def test_12_contains_non_wrap() -> None:
    lp = LogicalPartition(pid=0, start=0, end=16)
    assert lp.contains(8) is True
    assert lp.contains(0) is False
    assert lp.contains(16) is True


@pytest.mark.ring
def test_13_contains_wrap() -> None:
    lp = LogicalPartition(pid=15, start=240, end=0)
    assert lp.contains(255) is True
    assert lp.contains(0) is True
    assert lp.contains(128) is False


@pytest.mark.ring
def test_14_ranges_single_vnode() -> None:
    p = Partitioner(HashSpace(bits=8), 4, 1)
    ranges = p.ranges_for("a", Ring([VNode("a", 128)]))
    assert ranges == [
        PartitionRange(owner=VNode("a", 128), start_pid=0, end_pid=15, count=16)
    ]


@pytest.mark.ring
def test_15_partition_range_wraps_true() -> None:
    assert PartitionRange(VNode("a", 0), 10, 3, 9).wraps is True


@pytest.mark.ring
def test_16_partition_range_wraps_false() -> None:
    assert PartitionRange(VNode("a", 0), 0, 9, 10).wraps is False


@pytest.mark.ring
def test_17_partitioner_totals() -> None:
    p = Partitioner(HashSpace(bits=8), 4, 1)
    assert p.total_partitions == 16
    assert p.total_segments == 2


@pytest.mark.ring
def test_18_member_supersedes_higher_generation() -> None:
    newer = Member("n", "a", 2, 0, MemberPhase.READY, (1,), 10)
    older = Member("n", "a", 1, 99, MemberPhase.READY, (1,), 10)
    assert newer.supersedes(older) is True


@pytest.mark.ring
def test_19_member_supersedes_higher_seq() -> None:
    newer = Member("n", "a", 1, 5, MemberPhase.READY, (1,), 10)
    older = Member("n", "a", 1, 3, MemberPhase.READY, (1,), 10)
    assert newer.supersedes(older) is True


@pytest.mark.ring
def test_20_member_equal_not_superseding() -> None:
    member = Member("n", "a", 1, 1, MemberPhase.READY, (1,), 10)
    assert member.supersedes(member) is False


@pytest.mark.ring
def test_21_registry_upsert_new_member() -> None:
    from tourillon.core.lifecycle.registry import MemberRegistry

    registry = MemberRegistry()
    member = Member("n", "a", 1, 0, MemberPhase.READY, (1,), 10)
    assert registry.upsert(member) is True
    assert registry.get("n") == member


@pytest.mark.ring
def test_22_registry_reject_older_member() -> None:
    from tourillon.core.lifecycle.registry import MemberRegistry

    registry = MemberRegistry()
    registry.upsert(Member("n", "a", 1, 5, MemberPhase.READY, (1,), 10))
    assert registry.upsert(Member("n", "a", 1, 3, MemberPhase.READY, (1,), 10)) is False


@pytest.mark.ring
def test_23_registry_members_in_phase() -> None:
    from tourillon.core.lifecycle.registry import MemberRegistry

    registry = MemberRegistry()
    registry.upsert(Member("ready", "a", 1, 0, MemberPhase.READY, (), 10))
    registry.upsert(Member("idle", "a", 1, 0, MemberPhase.IDLE, (), 10))
    assert set(registry.members_in_phase(MemberPhase.READY)) == {"ready"}


@pytest.mark.ring
def test_24_registry_snapshot_independent() -> None:
    from tourillon.core.lifecycle.registry import MemberRegistry

    registry = MemberRegistry()
    old = Member("n", "a", 1, 0, MemberPhase.READY, (), 10)
    new = Member("n", "a", 1, 1, MemberPhase.READY, (), 10)
    registry.upsert(old)
    snap = registry.snapshot()
    registry.upsert(new)
    assert snap.get("n") == old


@pytest.mark.ring
async def test_25_topology_apply_ready_adds_ring() -> None:
    tm = TopologyManager()
    accepted = await tm.apply_member(
        Member("n", "a", 1, 0, MemberPhase.READY, (10, 20), 10)
    )
    snapshot = await tm.snapshot()
    assert accepted is True
    assert snapshot.epoch == 1
    assert len(snapshot.ring) == 2


@pytest.mark.ring
async def test_26_topology_noop_same_member() -> None:
    tm = TopologyManager()
    member = Member("n", "a", 1, 0, MemberPhase.READY, (10,), 10)
    await tm.apply_member(member)
    before = await tm.snapshot()
    accepted = await tm.apply_member(member)
    after = await tm.snapshot()
    assert accepted is False
    assert after.epoch == before.epoch
    assert len(after.ring) == len(before.ring)


@pytest.mark.ring
async def test_27_topology_idle_to_joining_no_epoch() -> None:
    tm = TopologyManager()
    accepted = await tm.apply_member(
        Member("n", "a", 1, 0, MemberPhase.JOINING, (10,), 10)
    )
    snapshot = await tm.snapshot()
    assert accepted is True
    assert snapshot.epoch == 0
    assert len(snapshot.ring) == 0


@pytest.mark.ring
async def test_28_topology_draining_to_idle_drops_ring() -> None:
    tm = TopologyManager()
    await tm.apply_member(Member("n", "a", 1, 0, MemberPhase.READY, (10,), 10))
    after_ready = await tm.snapshot()
    await tm.apply_member(Member("n", "a", 1, 1, MemberPhase.DRAINING, (10,), 10))
    after_draining = await tm.snapshot()
    await tm.apply_member(Member("n", "a", 1, 2, MemberPhase.IDLE, (10,), 10))
    after_idle = await tm.snapshot()
    assert after_ready.epoch == 1
    assert after_draining.epoch == 2
    assert after_idle.epoch == 3
    assert len(after_idle.ring) == 0


@pytest.mark.ring
async def test_29_topology_merge_registry() -> None:
    tm = TopologyManager()
    merged = await tm.merge_registry(
        [
            Member("n1", "a", 1, 0, MemberPhase.JOINING, (), 10),
            Member("n2", "a", 1, 0, MemberPhase.READY, (2,), 10),
        ]
    )
    snapshot = await tm.snapshot()
    assert merged == 2
    assert snapshot.registry.get("n1") is not None
    assert snapshot.registry.get("n2") is not None


@pytest.mark.ring
async def test_30_fingerprint_changes_after_mutation() -> None:
    tm = TopologyManager()
    await tm.apply_member(Member("n", "a", 1, 0, MemberPhase.READY, (1,), 10))
    fp1 = await tm.member_fingerprint()
    await tm.apply_member(Member("n", "a", 1, 1, MemberPhase.READY, (1,), 10))
    fp2 = await tm.member_fingerprint()
    assert fp1 != fp2


@pytest.mark.ring
async def test_31_fingerprint_same_after_noop() -> None:
    tm = TopologyManager()
    member = Member("n", "a", 1, 0, MemberPhase.READY, (1,), 10)
    await tm.apply_member(member)
    fp1 = await tm.member_fingerprint()
    await tm.apply_member(member)
    fp2 = await tm.member_fingerprint()
    assert fp1 == fp2


@pytest.mark.ring
async def test_32_snapshot_empty() -> None:
    snapshot = await TopologyManager().snapshot()
    assert snapshot.epoch == 0
    assert len(snapshot.registry) == 0
    assert len(snapshot.ring) == 0


@pytest.mark.ring
async def test_33_preference_single_ready() -> None:
    tm = TopologyManager()
    await tm.apply_member(Member("a", "a", 1, 0, MemberPhase.READY, (10,), 10))
    topology = await tm.snapshot()
    placement = PartitionPlacement(0, LogicalPartition(0, 0, 1), VNode("a", 10))
    entries = await SimplePreferenceStrategy(rf=1).preference_list(
        placement,
        topology,
        ProbeManager(),
    )
    assert len(entries) == 1
    assert entries[0].node_id == "a"
    assert entries[0].readable is True
    assert entries[0].suspect is False
    assert entries[0].handoff is None


@pytest.mark.ring
async def test_34_preference_excludes_joining() -> None:
    tm = TopologyManager()
    await tm.apply_member(Member("ready", "a", 1, 0, MemberPhase.READY, (10,), 10))
    await tm.apply_member(Member("joining", "a", 1, 0, MemberPhase.JOINING, (20,), 10))
    topology = await tm.snapshot()
    placement = PartitionPlacement(0, LogicalPartition(0, 0, 1), VNode("ready", 10))
    entries = await SimplePreferenceStrategy(rf=2).preference_list(
        placement,
        topology,
        ProbeManager(),
    )
    assert {entry.node_id for entry in entries} == {"ready"}


@pytest.mark.ring
async def test_35_preference_drain_gets_handoff() -> None:
    tm = TopologyManager()
    await tm.apply_member(Member("d", "a", 1, 0, MemberPhase.READY, (10,), 10))
    await tm.apply_member(Member("r", "a", 1, 0, MemberPhase.READY, (20,), 10))
    await tm.apply_member(Member("d", "a", 1, 1, MemberPhase.DRAINING, (10,), 10))
    topology = await tm.snapshot()
    placement = PartitionPlacement(0, LogicalPartition(0, 0, 1), VNode("d", 10))
    entries = await SimplePreferenceStrategy(rf=1).preference_list(
        placement,
        topology,
        ProbeManager(),
    )
    assert entries[0].readable is True
    assert entries[0].handoff == "r"


@pytest.mark.ring
def test_36_failure_detector_phi_without_heartbeats() -> None:
    assert FailureDetector().phi() == 0.0


@pytest.mark.ring
async def test_37_failure_detector_after_heartbeats() -> None:
    fd = FailureDetector()
    fd.record_heartbeat()
    await asyncio.sleep(0.05)
    fd.record_heartbeat()
    assert fd.phi() < 1.0
    assert fd.is_available() is True


@pytest.mark.ring
async def test_38_probe_unknown_without_observations() -> None:
    assert await ProbeManager().state_of("node1") == MemberState.UNKNOWN


@pytest.mark.ring
async def test_39_probe_live_after_first_heartbeat() -> None:
    pm = ProbeManager()
    await pm.record_heartbeat("node1")
    assert await pm.state_of("node1") == MemberState.LIVE


@pytest.mark.ring
async def test_40_probe_all_states_empty() -> None:
    assert await ProbeManager().all_states_with_phi() == []


@pytest.mark.ring
async def test_41_file_state_load_absent_returns_none(tmp_path) -> None:
    assert await FileStateAdapter(tmp_path / "state.toml").load() is None


@pytest.mark.ring
async def test_42_file_state_roundtrip(tmp_path) -> None:
    state = NodeState("n", MemberPhase.READY, 1, 0, (1, 2), 1, (3,), (4,))
    adapter = FileStateAdapter(tmp_path / "state.toml")
    await adapter.save(state)
    assert await adapter.load() == state


@pytest.mark.ring
async def test_43_file_state_save_no_temp_left(tmp_path) -> None:
    adapter = FileStateAdapter(tmp_path / "state.toml")
    await adapter.save(NodeState("n", MemberPhase.READY, 1, 0, (), 1))
    assert not (tmp_path / "state.tmp").exists()


@pytest.mark.ring
async def test_44_file_state_load_malformed_raises(tmp_path) -> None:
    path = tmp_path / "state.toml"
    path.write_text("[node\nphase='ready'", encoding="utf-8")
    with pytest.raises(StateError):
        await FileStateAdapter(path).load()


def _cfg() -> TourillonConfig:
    return TourillonConfig(
        node_id="node-1",
        node_size=NodeSize.M,
        data_dir="./data",
        tls=TlsConfig("", "", ""),
        kv_server=ServerConfig("127.0.0.1:7000"),
        peer_server=ServerConfig("127.0.0.1:7001"),
        partition_shift=10,
    )


@pytest.mark.ring
async def test_45_bootstraper_start_first_node() -> None:
    state_port = InMemoryStateAdapter(None)
    tm = TopologyManager()
    boot = Bootstraper(_cfg(), state_port, tm, HashSpace(), segment_shift=2)
    state = await boot.start_first_node()
    snapshot = await tm.snapshot()
    assert state.phase is MemberPhase.READY
    assert state.generation == 1
    assert state.epoch == 1
    assert len(state.tokens) == 4
    assert len(snapshot.ring) == 4


@pytest.mark.ring
async def test_46_bootstraper_start_ready_node() -> None:
    persisted = NodeState("node-1", MemberPhase.READY, 1, 0, (5, 10, 15, 20), 1)
    state_port = InMemoryStateAdapter(persisted)
    tm = TopologyManager()
    boot = Bootstraper(_cfg(), state_port, tm, HashSpace(), segment_shift=2)
    result = await boot.start_ready_node()
    snapshot = await tm.snapshot()
    assert result == persisted
    assert state_port.save_calls == 0
    assert len(snapshot.ring) == 4


@pytest.mark.ring
async def test_47_bootstraper_start_node_rejects_joining() -> None:
    state_port = InMemoryStateAdapter(
        NodeState("node-1", MemberPhase.JOINING, 1, 0, (1, 2, 3, 4), 0)
    )
    boot = Bootstraper(
        _cfg(), state_port, TopologyManager(), HashSpace(), segment_shift=2
    )
    with pytest.raises(BootstrapError) as exc:
        await boot.start_node()
    assert exc.value.exit_code == 1


@pytest.mark.ring
def test_48_check_node_id_mismatch() -> None:
    with pytest.raises(NodeIdMismatchError):
        check_node_id_consistency("node-a", "node-b")
