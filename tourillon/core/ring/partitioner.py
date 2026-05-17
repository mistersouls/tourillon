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
"""Partitioner, LogicalPartition, and PartitionPlacement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode

if TYPE_CHECKING:
    from tourillon.core.structure.record import StoreKey


@dataclass(frozen=True)
class LogicalPartition:
    """Contiguous half-open arc (start, end] of the circular hash space.

    pid is a stable storage-key prefix that survives ring mutations. When a
    node leaves and another takes over a partition, the underlying keys do
    not need to be renamed; only ownership changes.

    The arc wraps around the zero boundary when start >= end.
    """

    pid: int
    start: int
    end: int  # half-open (start, end]

    def contains(self, h: int) -> bool:
        """Return True if h falls within this arc, handling wrap-around."""
        if self.start < self.end:
            return self.start < h <= self.end
        return h > self.start or h <= self.end


@dataclass(frozen=True)
class PartitionPlacement:
    """Ephemeral binding between a LogicalPartition and its current owner VNode.

    Never persisted. Always recomputed after any ring mutation. address
    returns str(pid) — a stable storage-key prefix independent of the owner.
    """

    partition: LogicalPartition
    vnode: VNode

    @property
    def address(self) -> str:
        """Return str(pid) — stable storage-key prefix independent of owner."""
        return str(self.partition.pid)


class Partitioner:
    """Imposes a static grid of 2**partition_shift logical partitions over the ring.

    The grid is independent of physical nodes and never changes when topology
    changes. partition_shift is fixed for the lifetime of the cluster;
    partition_shift < bits is enforced at construction time.

    pid_for_hash() is O(1). placement_for_token() is O(1) + O(log n)
    for the subsequent ring successor lookup.
    """

    def __init__(self, hash_space: HashSpace, partition_shift: int) -> None:
        """Raise ValueError if partition_shift >= hash_space.bits."""
        if partition_shift >= hash_space.bits:
            raise ValueError(
                f"partition_shift ({partition_shift}) must be "
                f"strictly less than bits ({hash_space.bits})"
            )
        self._hs = hash_space
        self._shift = partition_shift
        self._total = 1 << partition_shift
        self._step = hash_space.max >> partition_shift

    @property
    def total_partitions(self) -> int:
        """Return the total number of logical partitions (2**partition_shift)."""
        return self._total

    @property
    def partition_shift(self) -> int:
        """Return the partition shift value (log₂ of total_partitions)."""
        return self._shift

    def pid_for_hash(self, h: int) -> int:
        """Return the partition ID for hash h in O(1)."""
        return h >> (self._hs.bits - self._shift)

    def pid_for_addr(self, addr: StoreKey) -> int:
        """Return the partition ID for a StoreKey in O(1).

        Hashes ``addr`` via ``HashSpace.hash(addr.to_routing_bytes())``.
        Prefer this over calling ``pid_for_hash(_hash_key(addr))`` directly so
        the hash function and encoding remain in one place.
        """
        return self.pid_for_hash(self._hs.hash(addr.to_routing_bytes()))

    def segment_for_pid(self, pid: int) -> LogicalPartition:
        """Return the LogicalPartition arc for the given partition ID."""
        start = pid * self._step
        end = ((pid + 1) * self._step) % self._hs.max
        return LogicalPartition(pid=pid, start=start, end=end)

    def placement_for_token(self, token: int, ring: Ring) -> PartitionPlacement:
        """Return the PartitionPlacement for token on ring.

        O(1) partition lookup followed by O(log n) ring successor lookup.
        Raise ValueError when ring is empty.
        """
        pid = self.pid_for_hash(token)
        partition = self.segment_for_pid(pid)
        vnode = ring.successor(token)
        return PartitionPlacement(partition=partition, vnode=vnode)

    def placement_for_addr(self, addr: StoreKey, ring: Ring) -> PartitionPlacement:
        """Return the PartitionPlacement for a StoreKey on ring.

        Hashes ``addr`` via ``HashSpace.hash(addr.to_routing_bytes())`` then
        delegates to ``placement_for_token``.  This is the canonical entry
        point for coordinator code that routes by key; callers should prefer
        this method over calling ``_hash_key`` + ``placement_for_token``
        directly so that the hash function and encoding are defined in one
        place.

        Raise ValueError when ring is empty.
        """
        token = self._hs.hash(addr.to_routing_bytes())
        return self.placement_for_token(token, ring)
