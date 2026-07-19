import secrets

import pytest

from tourillon.core.rebalance.planner import RebalancePlanner
from tourillon.core.rebalance.transfer import RangeTransfer
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


@pytest.fixture
def partitioner():
    hs = HashSpace(bits=8)
    return Partitioner(
        hash_space=hs,
        partition_shift=4,   # 16 partitions
        segment_shift=2,
    )


@pytest.fixture
def planner(partitioner):
    return RebalancePlanner(
        partitioner=partitioner,
        node_id="node-x",
        replication_factor=3,
    )


def test_simple(planner):
    old_ring = Ring([VNode("node-1", 0), VNode("node-1", 4)])
    new_ring = old_ring.add_vnodes([VNode("node-x", 2)])

    assert planner.plan(old_ring, new_ring) == [
        RangeTransfer(start_pid=0, end_pid=15, count=16, src='node-1', dst='node-x')
    ]


def test_partial_intersection_nonwrap(planner):
    old_ring = Ring([VNode("A", 0)])
    new_ring = old_ring.add_vnodes([VNode("B", 128)])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=8, end_pid=15, count=8, src="A", dst="B"),
        RangeTransfer(start_pid=0, end_pid=7, count=8, src="A", dst="B"),
    ]


def test_partial_intersection_wrap(planner):
    old_ring = Ring([VNode("A", 250), VNode("A", 20)])
    new_ring = old_ring.add_vnodes([VNode("B", 5)])                    # B owns (1→5]

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=15, end_pid=15, count=1, src="A", dst="B"),
        RangeTransfer(start_pid=0, end_pid=0, count=1, src="A", dst="B"),
        RangeTransfer(start_pid=1, end_pid=14, count=14, src="A", dst="B"),
    ]


def test_difference_nonwrap(planner):
    old_ring = Ring([VNode("A", 0)])
    new_ring = old_ring.add_vnodes([VNode("B", 64)])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=4, end_pid=15, count=12, src="A", dst="B"),
        RangeTransfer(start_pid=0, end_pid=3, count=4, src="A", dst="B"),
    ]


def test_difference_wrap(planner, partitioner):
    old_ring = Ring([VNode("A", 250), VNode("A", 20)])
    new_ring = old_ring.add_vnodes([VNode("B", 15)])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=15, end_pid=15, count=1, src="A", dst="B"),
        RangeTransfer(start_pid=0, end_pid=0, count=1, src="A", dst="B"),
        RangeTransfer(start_pid=1, end_pid=14, count=14, src="A", dst="B"),
    ]


def test_remove_vnode(planner):
    old_ring = Ring([VNode("A", 0), VNode("B", 128)])
    new_ring = old_ring.drop_nodes({"A"})

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == []


def test_replicas_increase(planner):
    old_ring = Ring([VNode("A", 0)])
    new_ring = Ring([VNode("A", 0), VNode("B", 100), VNode("C", 200)])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=12, end_pid=15, count=4, src="A", dst="B"),
        RangeTransfer(start_pid=12, end_pid=15, count=4, src="A", dst="C"),
        RangeTransfer(start_pid=0, end_pid=5, count=6, src="A", dst="B"),
        RangeTransfer(start_pid=0, end_pid=5, count=6, src="A", dst="C"),
        RangeTransfer(start_pid=6, end_pid=11, count=6, src="A", dst="B"),
        RangeTransfer(start_pid=6, end_pid=11, count=6, src="A", dst="C"),
    ]


def test_replicas_decrease(planner):
    old_ring = Ring([VNode("A", 0), VNode("B", 100), VNode("C", 200)])
    new_ring = Ring([VNode("A", 0)])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == []


def test_multi_owner_multi_vnode(planner):
    old_ring = Ring([
        VNode("A", 0), VNode("A", 50),
        VNode("B", 100), VNode("B", 150),
    ])

    new_ring = old_ring.add_vnodes([
        VNode("C", 75), VNode("C", 125)
    ])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=9, end_pid=15, count=7, src="A", dst="C"),
        RangeTransfer(start_pid=0, end_pid=2, count=3, src="A", dst="C"),
        RangeTransfer(start_pid=3, end_pid=3, count=1, src="A", dst="C"),
        RangeTransfer(start_pid=4, end_pid=5, count=2, src="A", dst="C"),
        RangeTransfer(start_pid=6, end_pid=6, count=1, src="A", dst="C"),
        RangeTransfer(start_pid=7, end_pid=8, count=2, src="A", dst="C"),
    ]


def test_empty_to_nonempty(planner, partitioner):
    old_ring = Ring([])
    new_ring = Ring([VNode("A", 0)])

    assert planner.plan(old_ring, new_ring) == []


def test_nonempty_to_empty(planner, partitioner):
    old_ring = Ring([VNode("A", 0)])
    new_ring = Ring([])

    assert planner.plan(old_ring, new_ring) == []


def test_full_wrap(planner, partitioner):
    old_ring = Ring([VNode("A", 0)])
    new_ring = old_ring.add_vnodes([VNode("B", 0)])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=0, end_pid=15, count=16, src='A', dst='B')
    ]


def test_three_nodes_add_fourth(planner):
    old_ring = Ring([
        VNode("A", 0),
        VNode("B", 85),
        VNode("C", 170),
    ])

    new_ring = old_ring.add_vnodes([
        VNode("D", 42),
    ])

    # old: [(0, 4), (B, C, A)], [(5, 9), (C, A, B)], [(10, 15), (A, B, C)]
    # new: [(0, 1), (D, B, C)], [(2, 4), (B, C, A)], [(5, 9), (C, A, D)], [(10, 15), (A, D, B)]
    # diff: [(0, 1), (A, D)], [(5, 9), (B, D)], [(10, 15), (C, D)]
    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=10, end_pid=15, count=6, src="C", dst="D"),
        RangeTransfer(start_pid=0, end_pid=1, count=2, src="A", dst="D"),
        RangeTransfer(start_pid=5, end_pid=9, count=5, src="B", dst="D"),
    ]


def test_drain_one_add_one(planner):
    old_ring = Ring([
        VNode("A", 0),
        VNode("B", 85),
        VNode("C", 170),
    ])

    # drain B, add D
    new_ring = Ring([
        VNode("A", 0),
        VNode("D", 85),
        VNode("C", 170),
    ])

    # old: [(0, 4), (B, C, A)], [(5, 9), (C, A, B)], [(10, 15), (A, B, C)]
    # new: [(0, 4, (D, C, A)], [(5, 9), (C, A, D)], [(10, 15), (A, D, C)]
    transfers = planner.plan(old_ring, new_ring)

    assert transfers == [
        RangeTransfer(start_pid=10, end_pid=15, count=6, src="B", dst="D"),
        RangeTransfer(start_pid=0, end_pid=4, count=5, src="B", dst="D"),
        RangeTransfer(start_pid=5, end_pid=9, count=5, src="B", dst="D"),
    ]


def test_four_nodes_same_partition_remove_one(planner, partitioner):
    old_ring = Ring([
        VNode("A", 1),
        VNode("B", 2),
        VNode("C", 3),
        VNode("D", 4),
    ])

    new_ring = Ring([
        VNode("A", 1),
        VNode("B", 2),
        VNode("C", 3),
    ])

    transfers = planner.plan(old_ring, new_ring)

    assert transfers == []


def test_too_vnodes():
    hashspace = HashSpace(bits=8)
    partitioner = Partitioner(hash_space=hashspace, partition_shift=3, segment_shift=1)
    planner = RebalancePlanner(
        partitioner=partitioner,
        node_id="node-x",
        replication_factor=3,
    )
    old_ring = Ring([
        VNode("A", 7),
        VNode("A", 10),
        VNode("A", 26),
        VNode("A", 84),
        VNode("A", 156),
        VNode("A", 218),
        VNode("A", 236),
        VNode("A", 241),
    ])
    new_ring = old_ring.add_vnodes([
        VNode("x", 12),
        VNode("x", 78),
        VNode("x", 85),
        VNode("x", 106),
        VNode("x", 136),
        VNode("x", 172),
        VNode("x", 185),
        VNode("x", 235)
    ])
    plan = planner.plan(old_ring, new_ring)
    assert plan == [
        RangeTransfer(start_pid=7, end_pid=7, count=1, src="A", dst="x"),
        RangeTransfer(start_pid=0, end_pid=1, count=2, src="A", dst="x"),
        RangeTransfer(start_pid=2, end_pid=2, count=1, src="A", dst="x"),
        RangeTransfer(start_pid=3, end_pid=3, count=1, src="A", dst="x"),
        RangeTransfer(start_pid=4, end_pid=4, count=1, src="A", dst="x"),
        RangeTransfer(start_pid=5, end_pid=5, count=1, src="A", dst="x"),
        RangeTransfer(start_pid=6, end_pid=6, count=1, src="A", dst="x"),
    ]
