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
"""Ring immutability and successor lookup tests — scenarios 1, 2, 12."""

from __future__ import annotations

import pytest

from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode

pytestmark = pytest.mark.ring


def _vnodes(node_id: str, tokens: list[int]) -> list[VNode]:
    return [VNode(node_id=node_id, token=t) for t in tokens]


@pytest.mark.ring
def test_1_add_vnodes_returns_new_ring_with_all_vnodes_sorted() -> None:
    """Returns new Ring; original unchanged; result contains all former + new vnodes sorted ascending."""
    initial = (
        _vnodes("node-a", [10, 50])
        + _vnodes("node-b", [30, 80])
        + _vnodes("node-c", [60, 120])
    )
    ring = Ring(initial)

    new_vnodes = _vnodes("node-d", [5, 99])
    ring2 = ring.add_vnodes(new_vnodes)

    # original unchanged
    assert len(ring) == 6
    assert all(v.node_id != "node-d" for v in ring)

    # new ring contains all 8 vnodes
    assert len(ring2) == 8
    tokens = [v.token for v in ring2]
    assert tokens == sorted(tokens)
    assert any(v.node_id == "node-d" for v in ring2)


@pytest.mark.ring
def test_2_drop_nodes_returns_new_ring_without_dropped_node() -> None:
    """New ring without node-3 vnodes; original unchanged."""
    ring = Ring(
        _vnodes("node-1", [10, 80])
        + _vnodes("node-2", [40, 120])
        + _vnodes("node-3", [25, 60])
    )

    ring2 = ring.drop_nodes({"node-3"})

    assert len(ring) == 6  # original unchanged
    assert len(ring2) == 4
    assert all(v.node_id != "node-3" for v in ring2)
    tokens = [v.token for v in ring2]
    assert tokens == sorted(tokens)


@pytest.mark.ring
def test_12_successor_always_valid_after_arbitrary_mutations() -> None:
    """ring.successor(t) always returns a vnode present in the ring; len consistent."""
    base = (
        _vnodes("node-a", [20, 100])
        + _vnodes("node-b", [50, 180])
        + _vnodes("node-c", [80, 220])
    )
    ring = Ring(base)

    # add then drop
    ring = ring.add_vnodes(_vnodes("node-d", [15, 60, 130]))
    ring = ring.drop_nodes({"node-b"})
    ring = ring.add_vnodes(_vnodes("node-e", [200]))

    assert len(ring) == 8  # a:2 + c:2 + d:3 + e:1

    for token in [0, 14, 15, 60, 100, 199, 201, 250, 255]:
        result = ring.successor(token)
        assert result in list(ring), f"successor({token}) not in ring"


@pytest.mark.ring
def test_12b_successor_wraps_around() -> None:
    """successor wraps at the end of the ring."""
    ring = Ring(_vnodes("node-a", [50, 150]))
    # token 200 > 150 → wraps to first vnode (token 50)
    assert ring.successor(200).token == 50
