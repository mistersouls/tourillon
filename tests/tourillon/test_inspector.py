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

import ssl

import pytest

from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer.pool import PeerPool
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.services.inspector import NodeInspector
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.telemetry import build_noop_telemetry
from tourlib.adapter import MsgPackSerializer
from tourlib.models.inspect import NodeInspectRequest


def _build_inspector(node_id: str = "node-1") -> tuple[NodeInspector, TopologyManager]:
    topologymgr = TopologyManager()
    partitioner = Partitioner(pshift=17, gshift=5, hashspace=HashSpace(bits=128))
    peer_pool = PeerPool(
        tls_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
        topologymgr=topologymgr,
        fd_provider=FailureDetectorProvider(serializer=MsgPackSerializer()),
        telemetry=build_noop_telemetry(),
    )
    inspector = NodeInspector(
        node_id=node_id,
        stats=GossipStats(known_members=3, ae_cycles_total=12),
        peer_pool=peer_pool,
        partitioner=partitioner,
        topologymgr=topologymgr,
    )
    return inspector, topologymgr


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_streams_compact_membership_rows() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-3")
    members = [
        Member("node-1", "peer-1:7701", 2, MemberPhase.READY, ("b",)),
        Member("node-3", "peer-3:7701", 3, MemberPhase.READY, ("21",)),
        Member("node-2", "peer-2:7701", 1, MemberPhase.JOINING, ("16",)),
    ]
    await topologymgr.merge_registry(members)

    request = NodeInspectRequest()
    chunks = [chunk async for chunk in inspector.inspect(request)]

    assert len(chunks) == 1
    chunk = chunks[0]
    assert chunk.node_id == "node-3"
    assert chunk.chunk_index == 0
    assert chunk.has_more is False
    assert chunk.next_cursor is None
    assert [member.node_id for member in chunk.members] == [
        "node-1",
        "node-2",
        "node-3",
    ]
    assert [member.phase for member in chunk.members] == ["READY", "JOINING", "READY"]
    assert [member.status for member in chunk.members] == ["UNKNOWN", "UNKNOWN", "-"]
    assert [member.epoch for member in chunk.members] == [2, 1, 3]
    assert chunk.self_member is not None
    assert chunk.self_member.node_id == "node-3"
    assert chunk.self_member.phase == "READY"
    assert chunk.self_member.epoch == 3
    assert chunk.ring_position is not None
    assert chunk.ring_position.total_partitions == 1 << 17
    assert chunk.gossip_stats["known_members"] == 3
    assert chunk.gossip_stats["ae_cycles_total"] == 12


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_streams_multiple_chunks_and_only_first_has_stats() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-1")
    inspector.INSPECT_CHUNK_SIZE = 2
    members = [
        Member(f"node-{idx}", f"peer-{idx}:7701", idx, MemberPhase.READY, (f"{idx:x}",))
        for idx in range(1, 6)
    ]
    await topologymgr.merge_registry(members)

    request = NodeInspectRequest()
    chunks = [chunk async for chunk in inspector.inspect(request)]

    assert len(chunks) == 3
    assert [chunk.has_more for chunk in chunks] == [True, True, False]
    assert chunks[0].next_cursor == "node-2"
    assert chunks[1].next_cursor == "node-4"
    assert chunks[2].next_cursor is None
    assert chunks[0].ring_position is not None
    assert chunks[1].ring_position is not None
    assert chunks[2].ring_position is not None
    assert chunks[0].gossip_stats
    assert chunks[1].gossip_stats == {}
    assert chunks[2].gossip_stats == {}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_includes_gossip_stats_on_first_chunk() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-1")
    await topologymgr.merge_registry(
        [Member("node-1", "peer-1:7701", 1, MemberPhase.READY, ("1",))]
    )

    chunks = [chunk async for chunk in inspector.inspect(NodeInspectRequest())]

    assert len(chunks) == 1
    assert chunks[0].gossip_stats


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_applies_phase_prefix_cursor_and_probe_filters() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-3")
    members = [
        Member("node-1", "peer-1:7701", 2, MemberPhase.READY, ("b",)),
        Member("node-2", "peer-2:7701", 1, MemberPhase.JOINING, ("16",)),
        Member("node-3", "peer-3:7701", 3, MemberPhase.READY, ("21",)),
    ]
    await topologymgr.merge_registry(members)

    chunks = [
        chunk
        async for chunk in inspector.inspect(
            NodeInspectRequest(
                phases=("READY",),
                probe_statuses=("UNKNOWN",),
                node_id_prefix="node-",
                cursor="node-1",
            )
        )
    ]

    assert len(chunks) == 1
    assert chunks[0].members == ()
    assert chunks[0].self_member.node_id == "node-3"
    assert chunks[0].has_more is False


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_only_anomalies_keeps_non_ready_members() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-3")
    members = [
        Member("node-1", "peer-1:7701", 2, MemberPhase.READY, ("b",)),
        Member("node-2", "peer-2:7701", 1, MemberPhase.JOINING, ("16",)),
        Member("node-3", "peer-3:7701", 3, MemberPhase.READY, ("21",)),
    ]
    await topologymgr.merge_registry(members)

    chunks = [
        chunk
        async for chunk in inspector.inspect(NodeInspectRequest(only_anomalies=True))
    ]

    assert len(chunks) == 1
    assert [member.node_id for member in chunks[0].members] == ["node-2"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_keeps_self_identity_when_filters_exclude_local_node() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-3")
    members = [
        Member("node-2", "peer-2:7701", 1, MemberPhase.JOINING, ("16",)),
        Member("node-3", "peer-3:7701", 3, MemberPhase.READY, ("21",)),
    ]
    await topologymgr.merge_registry(members)

    chunks = [
        chunk
        async for chunk in inspector.inspect(
            NodeInspectRequest(phases=("JOINING",)),
        )
    ]

    assert len(chunks) == 1
    assert [member.node_id for member in chunks[0].members] == ["node-2"]
    assert chunks[0].self_member is not None
    assert chunks[0].self_member.node_id == "node-3"
    assert chunks[0].self_member.phase == "READY"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_inspect_raises_when_self_member_missing_from_registry() -> None:
    inspector, topologymgr = _build_inspector(node_id="node-3")
    members = [
        Member("node-2", "peer-2:7701", 1, MemberPhase.JOINING, ("16",)),
    ]
    await topologymgr.merge_registry(members)

    with pytest.raises(RuntimeError, match="self member 'node-3' not found"):
        _ = [chunk async for chunk in inspector.inspect(NodeInspectRequest())]


@pytest.mark.tourillon
def test_build_ring_position_keeps_wraparound_as_single_range() -> None:
    inspector, _ = _build_inspector(node_id="node-1")

    class _Owner:
        def __init__(self, node_id: str, token: int) -> None:
            self.node_id = node_id
            self.token = token

    class _PartitionRange:
        def __init__(self, owner: object, arc: tuple[int, int] | None) -> None:
            self.owner = owner
            self.arc = arc

    class _PartitionerStub:
        partition_shift = 10
        total_partitions = 1024

        @staticmethod
        def ranges_for(_ring: object) -> list[object]:
            return [_PartitionRange(_Owner("node-1", 0xABC), (900, 120))]

        @staticmethod
        def arc_count(_arc: tuple[int, int] | None) -> int:
            return (1024 - 901) + (120 + 1)

    inspector._partitioner = _PartitionerStub()  # type: ignore[assignment]

    ring_position = inspector._build_ring_position(ring=object())

    assert len(ring_position.vnode_ranges) == 1
    vnode_range = ring_position.vnode_ranges[0]
    assert vnode_range.token == "0xabc"
    assert vnode_range.arc == (900, 120)
    assert vnode_range.partitions == (1024 - 901) + (120 + 1)
