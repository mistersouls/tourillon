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

from importlib.metadata import PackageNotFoundError

import pytest

import tourillon.core.services.inspector as inspector_module
from tests.helpers.inspect import make_antientropy, make_view
from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.heartbeat import HeartbeatTable
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.probe.detector import PeerStatus
from tourillon.core.services.inspector import (
    NodeInspector,
    decode_cursor,
    encode_cursor,
    matches,
    summarise,
)
from tourillon.core.spaces.hashspace import HashSpace
from tourlib.exceptions import ValidationError
from tourlib.models.gc import GcInfo, GcStatus
from tourlib.models.inspect import MembersRequest, MembersSummary, PhaseCount

pytestmark = pytest.mark.tourillon

READY = MemberPhase.READY


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _Detector:
    def __init__(self, status: PeerStatus) -> None:
        self._status = status

    def status(self) -> PeerStatus:
        return self._status


class _PeerPool:
    def __init__(self, detectors: dict[str, _Detector] | None = None) -> None:
        self._detectors = detectors or {}

    def failure_detector(self, node_id: str) -> _Detector | None:
        return self._detectors.get(node_id)


async def _local_member() -> Member:
    return Member("node-9", "local:7701", 3, MemberPhase.IDLE, ())


async def _failing_local_member() -> Member:
    raise OSError("state file unreadable")


def _build(
    clock: _Clock, node_id: str = "node-1", detectors: dict | None = None
) -> tuple[NodeInspector, TopologyManager, HeartbeatTable]:
    topologymgr = TopologyManager(clock=clock)
    heartbeats = HeartbeatTable(node_id, interval=1.0, clock=clock, wall=lambda: 1.0)
    inspector = NodeInspector(
        node_id=node_id,
        stats=GossipStats(known_members=8, ae_cycles_total=12),
        peer_pool=_PeerPool(detectors),  # type: ignore[arg-type]
        partitioner=Partitioner(pshift=10, gshift=2, hashspace=HashSpace(bits=128)),
        topologymgr=topologymgr,
        gc_snapshot=lambda: GcInfo(True, 2, 600, 0, GcStatus()),
        heartbeats=heartbeats,
        antientropy=lambda: make_antientropy(queued=4),
        local_member=_local_member,
        clock=clock,
    )
    return inspector, topologymgr, heartbeats


def _member(index: int, phase: MemberPhase = READY) -> Member:
    return Member(f"node-{index}", f"peer-{index}:7701", 1, phase, (f"{index}a",))


async def _cluster() -> tuple[NodeInspector, _Clock]:
    """Eight members seen from node-1 at t=100 (suspect 6 s, dead 18 s).

    node-2 READY ALIVE, node-3 READY SUSPECT (heard 10 s ago), node-4 READY
    SUSPECT (15 s), node-5 JOINING ALIVE, node-6 READY DEAD (40 s), node-7
    DRAINING ALIVE, node-8 READY ALIVE by heartbeat but SUSPECT for the
    failure detector.
    """
    clock = _Clock()
    detectors = {"node-8": _Detector(PeerStatus.SUSPECT)}
    inspector, topologymgr, heartbeats = _build(clock, detectors=detectors)
    phases = {5: MemberPhase.JOINING, 7: MemberPhase.DRAINING}
    await topologymgr.merge_registry(
        [_member(i, phases.get(i, READY)) for i in range(1, 9)]
    )
    for heard_at, node_ids in (
        (60.0, ("node-6",)),
        (85.0, ("node-4",)),
        (90.0, ("node-3",)),
        (95.0, ("node-2", "node-5", "node-8")),
        (99.0, ("node-7",)),
    ):
        clock.now = heard_at
        heartbeats.merge({node_id: int(heard_at) for node_id in node_ids})
    clock.now = 100.0
    return inspector, clock


def _ids(page) -> list[str]:
    return [item.node_id for item in page.items]


async def test_members_summarises_every_member_and_lists_anomalies() -> None:
    inspector, _ = await _cluster()

    page = await inspector.members(MembersRequest())

    assert page.node_id == "node-1"
    assert page.members == 8
    assert page.summary == (
        PhaseCount("READY", alive=2, suspect=3, dead=1),
        PhaseCount("JOINING", alive=1),
        PhaseCount("DRAINING", alive=1),
    )
    assert page.anomalies_only is True
    assert page.total == 6
    assert page.cursor is None
    assert _ids(page) == ["node-6", "node-4", "node-3", "node-8", "node-5", "node-7"]


async def test_member_views_carry_heard_and_since_ages() -> None:
    inspector, _ = await _cluster()

    page = await inspector.members(MembersRequest(list_all=True))
    views = {item.node_id: item for item in page.items}

    assert views["node-1"].status == "ALIVE"
    assert views["node-1"].last_heard_s == 0.0
    assert views["node-1"].since_s == 100.0
    assert views["node-3"].status == "SUSPECT"
    assert views["node-3"].last_heard_s == 10.0
    assert views["node-3"].since_s == 10.0
    assert views["node-5"].status == "ALIVE"
    assert views["node-5"].since_s == 100.0
    assert views["node-6"].status == "DEAD"
    assert views["node-8"].status == "SUSPECT"
    assert views["node-8"].since_s == 5.0
    assert views["node-2"].address == "peer-2:7701"


async def test_list_all_orders_by_node_id() -> None:
    inspector, _ = await _cluster()

    page = await inspector.members(MembersRequest(list_all=True))

    assert page.anomalies_only is False
    assert page.total == 8
    assert _ids(page) == [f"node-{i}" for i in range(1, 9)]


@pytest.mark.parametrize(
    "request_,expected",
    [
        (
            MembersRequest(phases=("READY",), statuses=("SUSPECT", "DEAD")),
            ["node-3", "node-4", "node-6", "node-8"],
        ),
        (MembersRequest(phases=("JOINING", "DRAINING")), ["node-5", "node-7"]),
        (MembersRequest(phases=("READY",), prefix="node-2"), ["node-2"]),
        (
            MembersRequest(statuses=("ALIVE",), prefix="node-"),
            ["node-1", "node-2"] + ["node-5", "node-7"],
        ),
        (MembersRequest(phases=("JOINING",), statuses=("DEAD",)), []),
    ],
)
async def test_filters_combine_or_within_and_across(request_, expected) -> None:
    inspector, _ = await _cluster()

    page = await inspector.members(request_)

    assert _ids(page) == expected
    assert page.total == len(expected)
    assert page.members == 8
    assert page.cursor is None


@pytest.mark.parametrize(
    "request_,pages",
    [
        (
            MembersRequest(list_all=True, limit=3),
            [
                ["node-1", "node-2", "node-3"],
                ["node-4", "node-5", "node-6"],
                ["node-7", "node-8"],
            ],
        ),
        (
            MembersRequest(limit=2),
            [["node-6", "node-4"], ["node-3", "node-8"], ["node-5", "node-7"]],
        ),
    ],
)
async def test_pagination_walks_every_page_with_the_cursor(request_, pages) -> None:
    inspector, _ = await _cluster()
    seen: list[list[str]] = []
    cursor = None

    for _ in pages:
        page = await inspector.members(
            MembersRequest(
                list_all=request_.list_all, limit=request_.limit, cursor=cursor
            )
        )
        seen.append(_ids(page))
        cursor = page.cursor

    assert seen == pages
    assert cursor is None


async def test_exact_page_boundary_has_no_next_cursor() -> None:
    inspector, _ = await _cluster()

    page = await inspector.members(MembersRequest(list_all=True, limit=8))

    assert len(page.items) == 8
    assert page.cursor is None


@pytest.mark.parametrize(
    "request_",
    [
        MembersRequest(cursor="!!!"),
        MembersRequest(cursor=encode_cursor(())),
        MembersRequest(cursor="eyJhIjoxfQ"),
        MembersRequest(list_all=True, cursor=encode_cursor((1,))),
        MembersRequest(cursor=encode_cursor(("x",))),
    ],
)
async def test_invalid_cursor_is_a_validation_error(request_) -> None:
    inspector, _ = await _cluster()

    with pytest.raises(ValidationError, match="invalid cursor"):
        await inspector.members(request_)


def test_cursor_round_trips_without_padding() -> None:
    cursor = encode_cursor((1, 42.5, "node-3"))

    assert "=" not in cursor
    assert decode_cursor(cursor) == (1, 42.5, "node-3")


async def test_status_describes_this_node() -> None:
    inspector, _ = await _cluster()

    status = await inspector.status()

    assert status.node_id == "node-1"
    assert status.address == "peer-1:7701"
    assert status.phase == "READY"
    assert status.epoch == 1
    assert status.since_s == 100.0
    assert status.version
    assert status.members == MembersSummary(total=8, ready_alive=2)
    assert status.antientropy.queued == 4
    assert status.gc.running is True
    assert status.gossip_stats["ae_cycles_total"] == 12
    assert status.ring_position.total_partitions == 1024
    assert status.ring_position.partition_shift == 10
    assert status.ring_position.vnode_ranges


async def test_status_without_phase_record_has_no_since() -> None:
    clock = _Clock()
    inspector, topologymgr, _ = _build(clock)
    await topologymgr.merge_registry([_member(1)])
    topologymgr._phase_since.clear()

    status = await inspector.status()

    assert status.since_s is None


async def test_status_falls_back_to_local_state_before_first_gossip() -> None:
    inspector, topologymgr, _ = _build(_Clock(), node_id="node-9")
    await topologymgr.merge_registry([_member(2)])

    status = await inspector.status()

    assert (status.node_id, status.address) == ("node-9", "local:7701")
    assert (status.phase, status.epoch, status.since_s) == ("IDLE", 3, None)
    assert status.members.total == 2


async def test_status_propagates_local_state_failure() -> None:
    inspector, _, _ = _build(_Clock(), node_id="node-9")
    inspector._local_member = _failing_local_member

    with pytest.raises(OSError, match="state file unreadable"):
        await inspector.status()


def test_package_version_falls_back_when_not_installed(monkeypatch) -> None:
    def missing(_name: str) -> str:
        raise PackageNotFoundError("tourillon")

    monkeypatch.setattr(inspector_module, "version", missing)

    assert inspector_module._package_version() == "unknown"


class _Owner:
    def __init__(self, node_id: str, token: int) -> None:
        self.node_id = node_id
        self.token = token


class _Range:
    def __init__(self, owner: _Owner, arc: tuple[int, int] | None) -> None:
        self.owner = owner
        self.arc = arc


class _PartitionerStub:
    partition_shift = 10

    def __init__(self, total: int, ranges: list[_Range]) -> None:
        self.total_partitions = total
        self._ranges = ranges

    def ranges_for(self, _ring: object) -> list[_Range]:
        return self._ranges

    @staticmethod
    def arc_count(_arc: tuple[int, int] | None) -> int:
        return (1024 - 901) + (120 + 1)


def test_ring_position_keeps_a_wrapped_arc_as_one_range() -> None:
    inspector, _, _ = _build(_Clock())
    inspector._partitioner = _PartitionerStub(  # type: ignore[assignment]
        1024,
        [_Range(_Owner("node-1", 0xABC), (900, 120)), _Range(_Owner("x", 1), (1, 2))],
    )

    ring = inspector._build_ring_position(ring=object())

    assert len(ring.vnode_ranges) == 1
    assert ring.vnode_ranges[0].token == "0xabc"
    assert ring.vnode_ranges[0].arc == (900, 120)
    assert ring.owned_partitions == 244


def test_ring_position_of_an_empty_grid_owns_nothing() -> None:
    inspector, _, _ = _build(_Clock())
    inspector._partitioner = _PartitionerStub(0, [])  # type: ignore[assignment]

    ring = inspector._build_ring_position(ring=object())

    assert ring.owned_percent == 0.0
    assert ring.vnode_ranges == ()


def test_matches_anomalies_only_and_filters() -> None:
    joining = make_view("node-2", phase="JOINING")

    assert matches(joining, MembersRequest()) is True
    assert matches(make_view("node-1"), MembersRequest()) is False
    assert matches(joining, MembersRequest(statuses=("DEAD",))) is False
    assert matches(joining, MembersRequest(prefix="node-1")) is False
    assert matches(joining, MembersRequest(prefix="node-2")) is True


def test_summarise_orders_known_phases_first() -> None:
    views = [
        make_view("a", phase="IDLE", status="DEAD"),
        make_view("b", phase="CUSTOM"),
        make_view("c"),
    ]

    assert summarise(views) == (
        PhaseCount("READY", alive=1),
        PhaseCount("IDLE", dead=1),
        PhaseCount("CUSTOM", alive=1),
    )
    assert summarise([]) == ()
