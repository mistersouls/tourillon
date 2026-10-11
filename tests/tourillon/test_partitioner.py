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

import pytest

from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.types import LogicalPartition
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode


def _expand_arc(arc: tuple[int, int] | None, total: int) -> set[int]:
    if arc is None:
        return set()
    start, end = arc
    if start == end:
        return set(range(total))
    if start < end:
        return set(range(start + 1, end + 1))
    return set(range(start + 1, total)) | set(range(0, end + 1))


@pytest.mark.tourillon
def test_ranges_for_one_vnode_returns_single_full_ring_arc() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    vnode = VNode(80, "A")

    ranges = list(partitioner.ranges_for(Ring([vnode])))

    assert len(ranges) == 1
    assert ranges[0].owner == vnode
    assert ranges[0].pred == vnode
    assert _expand_arc(ranges[0].arc, partitioner.total_partitions) == set(
        range(partitioner.total_partitions)
    )


@pytest.mark.tourillon
def test_ranges_for_two_vnodes_contains_empty_arc_and_wrap_multi_arc() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    ring = Ring([VNode(10, "A"), VNode(15, "B")])

    ranges = list(partitioner.ranges_for(ring))

    assert any(partition_range.arc is None for partition_range in ranges)
    assert any(
        _expand_arc(partition_range.arc, partitioner.total_partitions) == set(
        range(partitioner.total_partitions)
        )
        for partition_range in ranges
    )


@pytest.mark.tourillon
def test_ranges_for_two_vnodes_contains_single_non_wrap_arc() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    ring = Ring([VNode(10, "A"), VNode(16, "B")])

    ranges = list(partitioner.ranges_for(ring))

    # pid 0 covers [0, 16): routing gives it to ring.successor(16) = B@16.
    assert any(
        _expand_arc(partition_range.arc, partitioner.total_partitions) == {0}
        for partition_range in ranges
    )


@pytest.mark.tourillon
def test_ranges_for_two_vnodes_contains_single_wrap_arc() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    ring = Ring([VNode(250, "A"), VNode(5, "B")])

    ranges = list(partitioner.ranges_for(ring))

    wrap_range = next(pr for pr in ranges if pr.pred.token > pr.owner.token)
    # pid 15 covers [240, 256): routing gives it to ring.successor(0) = B@5.
    assert _expand_arc(wrap_range.arc, partitioner.total_partitions) == {15}


@pytest.mark.tourillon
def test_ranges_for_two_vnodes_can_produce_wrap_empty_arc_with_duplicate_tokens() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    ring = Ring([VNode(10, "A"), VNode(10, "B")])

    ranges = list(partitioner.ranges_for(ring))

    assert len(ranges) == 2
    assert all(pr.arc is None for pr in ranges)
    assert {(pr.pred.node_id, pr.owner.node_id) for pr in ranges} == {
        ("A", "B"),
        ("B", "A"),
    }


@pytest.mark.tourillon
def test_ranges_for_n_vnodes_covers_empty_one_multi_wrap_multi_without_arc_duplicates() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    ring = Ring(
        [
            VNode(40, "A"),
            VNode(47, "B"),
            VNode(48, "C"),
            VNode(80, "D"),
            VNode(250, "E"),
        ]
    )

    ranges = list(partitioner.ranges_for(ring))
    expanded = [
        _expand_arc(partition_range.arc, partitioner.total_partitions)
        for partition_range in ranges
    ]

    assert any(partition_range.arc is None for partition_range in ranges)
    # pid 2 covers [32, 48): routing gives it to ring.successor(48) = C@48.
    assert any(values == {2} for values in expanded)
    assert any(len(values) > 1 for values in expanded if values)
    assert any(
        partition_range.pred.token > partition_range.owner.token and len(values) > 1
        for partition_range, values in zip(ranges, expanded, strict=True)
    )

    concrete_arcs = [partition_range.arc for partition_range in ranges if partition_range.arc is not None]
    assert len(concrete_arcs) == len(set(concrete_arcs))


@pytest.mark.tourillon
def test_ranges_for_empty_ring_yields_no_ranges() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))

    assert list(partitioner.ranges_for(Ring.empty())) == []


@pytest.mark.tourillon
def test_partition_count_for_arc_matches_arc_expansion_size() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))
    total = partitioner.total_partitions

    arcs: list[tuple[int, int] | None] = [
        None,
        (0, 0),
        (2, 3),
        (4, 3),
        (15, 0),
        (0, 15),
    ]

    for arc in arcs:
        assert partitioner.arc_count(arc) == len(_expand_arc(arc, total))


@pytest.mark.tourillon
def test_logical_partition_contains_normal_range() -> None:
    partition = LogicalPartition(pid=1, start=10, end=20)

    assert partition.contains(10) is True
    assert partition.contains(15) is True
    assert partition.contains(19) is True
    assert partition.contains(20) is False
    assert partition.contains(9) is False


@pytest.mark.tourillon
def test_logical_partition_contains_wraparound_range() -> None:
    # start >= end → wraparound: h >= start OR h < end
    partition = LogicalPartition(pid=0, start=250, end=10)

    assert partition.contains(250) is True
    assert partition.contains(255) is True
    assert partition.contains(0) is True
    assert partition.contains(9) is True
    assert partition.contains(10) is False
    assert partition.contains(100) is False


@pytest.mark.tourillon
def test_partitioner_rejects_pshift_gte_bits() -> None:
    with pytest.raises(ValueError, match="partition_shift"):
        Partitioner(pshift=8, gshift=2, hashspace=HashSpace(bits=8))


@pytest.mark.tourillon
def test_partitioner_rejects_gshift_gte_pshift() -> None:
    with pytest.raises(ValueError, match="group_shift"):
        Partitioner(pshift=4, gshift=4, hashspace=HashSpace(bits=8))


@pytest.mark.tourillon
def test_partitioner_properties_return_expected_values() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))

    assert partitioner.total_groups == 4
    assert partitioner.pid_per_group == 4


@pytest.mark.tourillon
def test_partitioner_group_for_returns_correct_group() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))

    assert partitioner.group_for(0) == 0
    assert partitioner.group_for(4) == 1
    assert partitioner.group_for(8) == 2


@pytest.mark.tourillon
def test_partitioner_partition_for_returns_logical_partition() -> None:
    partitioner = Partitioner(pshift=4, gshift=2, hashspace=HashSpace(bits=8))

    partition = partitioner.partition_for(0)

    assert isinstance(partition, LogicalPartition)
    assert partition.pid == 0





