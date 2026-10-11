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

from itertools import count

import pytest

from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import SimplePlacementStrategy
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode
from tourlib.models.version import Address


def _address_in_pid(partitioner: Partitioner, pid: int) -> Address:
    """Return a real address whose hash falls in partition `pid`."""
    candidates = (Address(key=f"key-{i}".encode(), namespace=b"ns") for i in count())
    return next(
        addr
        for addr in candidates
        if partitioner.pid_for_hash(partitioner.space.hash(addr.to_bytes())) == pid
    )


@pytest.mark.tourillon
def test_routing_and_segments_agree_on_partition_owner() -> None:
    """Routing and transfer planning must give each partition the same owner.

    Hash space of 16 positions split into 4 partitions of 4 positions:

        pid :   0      1      2       3
        hash:  0-3    4-7    8-11   12-15
                 ^      ^
                A@2    B@6
    """
    partitioner = Partitioner(pshift=2, gshift=1, hashspace=HashSpace(bits=4))
    ring = Ring([VNode(token=2, node_id="A"), VNode(token=6, node_id="B")])
    placement = SimplePlacementStrategy(
        replication_factor=1,
        topologymgr=None,  # unused by _placement_addr
        partitioner=partitioner,
        peer_pool=None,  # unused by _placement_addr
    )

    routing = {
        pid: placement._placement_addr(  # noqa: SLF001
            _address_in_pid(partitioner, pid), ring
        ).vnode.node_id
        for pid in range(partitioner.total_partitions)
    }
    segments = {
        pid: segment.owner.node_id
        for segment in partitioner.segments_for(ring)
        for pid in range(segment.lo, segment.hi + 1)
    }

    # Routing (successor(partition.end)) is the reference rule.
    assert segments == routing, f"routing={routing} segments={segments}"
