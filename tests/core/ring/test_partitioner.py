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
"""Partitioner tests — scenarios 3, 4."""

from __future__ import annotations

import pytest

from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode

pytestmark = pytest.mark.ring


def _ring_with_nodes(bits: int = 8) -> Ring:
    max_val = 1 << bits
    step = max_val // 4
    return Ring(
        [
            VNode("node-a", step),
            VNode("node-b", step * 2),
            VNode("node-c", step * 3),
        ]
    )


@pytest.mark.ring
def test_3_placement_for_token_is_deterministic_across_independent_rings() -> None:
    """All 3 return identical PartitionPlacement for every token."""
    hs = HashSpace(bits=8)
    ps = 4

    ring_a = _ring_with_nodes()
    ring_b = _ring_with_nodes()
    ring_c = _ring_with_nodes()
    assert ring_a is not ring_b

    partitioner = Partitioner(hs, ps)

    for token in range(0, 256, 3):
        pa = partitioner.placement_for_token(token, ring_a)
        pb = partitioner.placement_for_token(token, ring_b)
        pc = partitioner.placement_for_token(token, ring_c)
        assert pa == pb == pc, f"Mismatch at token={token}: {pa} vs {pb} vs {pc}"


@pytest.mark.ring
def test_4_partitioner_raises_when_shift_equals_bits() -> None:
    """Raises ValueError (partition_shift must be < bits)."""
    hs = HashSpace(bits=8)
    with pytest.raises(ValueError, match="strictly less than"):
        Partitioner(hs, 8)


@pytest.mark.ring
def test_4b_partitioner_raises_when_shift_exceeds_bits() -> None:
    """Raises ValueError when partition_shift > bits."""
    hs = HashSpace(bits=8)
    with pytest.raises(ValueError):
        Partitioner(hs, 9)


@pytest.mark.ring
def test_partitioner_total_partitions_correct() -> None:
    """total_partitions == 2**partition_shift."""
    hs = HashSpace(bits=8)
    p = Partitioner(hs, 4)
    assert p.total_partitions == 16


@pytest.mark.ring
def test_partitioner_pid_for_hash_in_range() -> None:
    """pid_for_hash always returns a value in [0, total_partitions)."""
    hs = HashSpace(bits=8)
    p = Partitioner(hs, 4)
    for h in range(256):
        pid = p.pid_for_hash(h)
        assert 0 <= pid < p.total_partitions
