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

from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode


@pytest.mark.tourillon
def test_successor_and_predecessor_raise_on_empty_ring() -> None:
    ring = Ring.empty()

    with pytest.raises(ValueError, match="empty ring"):
        ring.successor(10)

    with pytest.raises(ValueError, match="empty ring"):
        ring.predecessor(10)


@pytest.mark.tourillon
def test_successor_and_predecessor_wrap_around() -> None:
    ring = Ring([VNode(10, "a"), VNode(20, "b"), VNode(30, "c")])

    assert ring.successor(20) == VNode(20, "b")
    assert ring.successor(31) == VNode(10, "a")
    assert ring.predecessor(10) == VNode(30, "c")
    assert ring.predecessor(25) == VNode(20, "b")


@pytest.mark.tourillon
def test_add_vnodes_returns_sorted_new_ring_without_mutating_original() -> None:
    original = Ring([VNode(20, "b")])

    updated = original.add_vnodes([VNode(10, "a"), VNode(30, "c")])

    assert [v.token for v in original] == [20]
    assert [v.token for v in updated] == [10, 20, 30]


@pytest.mark.tourillon
def test_drop_nodes_filters_requested_node_ids() -> None:
    ring = Ring([VNode(10, "a"), VNode(20, "b"), VNode(30, "a")])

    filtered = ring.drop_nodes({"a"})

    assert [v.node_id for v in filtered] == ["b"]
    assert [v.token for v in filtered] == [20]


@pytest.mark.tourillon
def test_iter_from_yields_clockwise_sequence_from_token_position() -> None:
    ring = Ring([VNode(10, "a"), VNode(20, "b"), VNode(30, "c")])

    from_present = list(ring.iter_from(VNode(20, "x")))
    from_absent = list(ring.iter_from(VNode(25, "x")))
    from_after_max = list(ring.iter_from(VNode(99, "x")))

    assert [v.token for v in from_present] == [20, 30, 10]
    assert [v.token for v in from_absent] == [30, 10, 20]
    assert [v.token for v in from_after_max] == [10, 20, 30]
