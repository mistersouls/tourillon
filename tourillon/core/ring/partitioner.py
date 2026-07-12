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
"""Partitioner, LogicalPartition, PartitionPlacement, and PartitionRange."""

from collections.abc import Iterator
from dataclasses import dataclass

from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


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

    segment: int
    partition: LogicalPartition
    vnode: VNode


@dataclass(frozen=True)
class PartitionRange:
    """Contiguous range of partition IDs owned by a single VNode.

    start_pid and end_pid are inclusive. When start_pid > end_pid the range
    wraps around the zero boundary (the wraps property reflects this).

    PartitionRange is a display and payload-compaction primitive only. It is
    never used for routing or storage decisions. The ring and individual pids
    remain the authoritative units for those operations.

    Never persisted. Always recomputed after any ring mutation.
    """

    owner: VNode
    pred: VNode
    start_pid: int
    end_pid: int
    count: int

    @property
    def wraps(self) -> bool:
        """Return True if this range crosses the zero boundary."""
        return self.start_pid > self.end_pid

    def contains(self, pid: int) -> bool:
        if self.wraps:
            return pid >= self.start_pid or pid <= self.end_pid
        return self.start_pid <= pid <= self.end_pid

    def intersection(self, other: "PartitionRange") -> tuple[int, int] | None:
        if self.contains(other.start_pid):
            start = other.start_pid
        elif other.contains(self.start_pid):
            start = self.start_pid
        else:
            return None

        end = other.end_pid if self.contains(other.end_pid) else self.end_pid
        return start, end

    def iter(self, total_partitions: int) -> Iterator[int]:
        if self.wraps:
            yield from range(self.start_pid, total_partitions)
            yield from range(0, self.end_pid + 1)
        else:
            yield from range(self.start_pid, self.end_pid + 1)


class Partitioner:
    """Imposes a static grid of 2**partition_shift logical partitions over the ring.

    The grid is independent of physical nodes and never changes when topology
    changes. partition_shift is fixed for the lifetime of the cluster;
    partition_shift < bits is enforced at construction time.

    Partitions are grouped into 2**segment_shift coarser segments. Each segment
    covers a contiguous arc of 2**(partition_shift - segment_shift) partitions
    within the hash space, providing a coarser granularity over the ring.

    pid_for_hash() is O(1). placement_for_token() is O(1) + O(log n)
    for the subsequent ring successor lookup.
    """

    def __init__(
        self,
        hash_space: HashSpace,
        partition_shift: int,
        segment_shift: int,
    ) -> None:
        """Raise ValueError if partition_shift >= hash_space.bits or segment_shift >= partition_shift."""
        if partition_shift >= hash_space.bits:
            raise ValueError(
                f"partition_shift ({partition_shift}) must be "
                f"strictly less than bits ({hash_space.bits})"
            )
        if segment_shift >= partition_shift:
            raise ValueError(
                f"segment_shift ({segment_shift}) must be strictly "
                f"less than partition_shift ({partition_shift})"
            )
        self._hs = hash_space
        self._partition_shift = partition_shift
        self._segment_shift = segment_shift
        self._total_partitions = 1 << partition_shift
        self._total_segments = 1 << segment_shift
        self._partition_step = hash_space.max >> partition_shift
        self._pids_per_segment = 1 << (partition_shift - segment_shift)

    @property
    def total_partitions(self) -> int:
        """Return the total number of logical partitions (2**partition_shift)."""
        return self._total_partitions

    @property
    def space(self) -> HashSpace:
        return self._hs

    @property
    def partition_shift(self) -> int:
        """Return the partition shift value (log₂ of total_partitions)."""
        return self._partition_shift

    @property
    def segment_shift(self) -> int:
        """Return the segment shift value (log₂ of total_segments)."""
        return self._segment_shift

    @property
    def total_segments(self) -> int:
        """Return the total number of segments (2**segment_shift)."""
        return self._total_segments

    @property
    def pids_per_segment(self) -> int:
        """Return the number of partitions per segment (2**(partition_shift - segment_shift))."""
        return self._pids_per_segment

    def pid_for_hash(self, h: int) -> int:
        """Return the partition ID for hash h in O(1)."""
        return h >> (self._hs.bits - self._partition_shift)

    def segment_for(self, pid: int) -> int:
        """Return the segment ID for a given partition ID in O(1).

        Segments are coarser groups of contiguous partitions. The segment ID
        is the upper segment_shift bits of the pid:
        pid >> (partition_shift - segment_shift).
        """
        return pid >> (self._partition_shift - self._segment_shift)

    def partition_for(self, pid: int) -> LogicalPartition:
        """Return the LogicalPartition arc for the given partition ID."""
        start = pid * self._partition_step
        end = ((pid + 1) * self._partition_step) % self._hs.max
        return LogicalPartition(pid=pid, start=start, end=end)

    def range_size(self, start: int, end: int) -> int:
        return (
            (self._total_partitions - start) + end + 1
            if start > end
            else end - start + 1
        )

    def placement_for_token(self, token: int, ring: Ring) -> PartitionPlacement:
        """Return the PartitionPlacement for token on ring.

        O(1) partition lookup followed by O(log n) ring successor lookup.
        Raise ValueError when ring is empty.
        """
        pid = self.pid_for_hash(token)
        partition = self.partition_for(pid)
        segment = self.segment_for(pid)
        vnode = ring.successor(partition.end)
        return PartitionPlacement(segment=segment, partition=partition, vnode=vnode)

    def ranges_for(self, ring: Ring) -> Iterator[PartitionRange]:
        """Return partition ranges owned by node_id on ring, in ascending start_pid order.

        For each vnode belonging to node_id, the owned range spans from
        predecessor vnode up to vnode.token (inclusive),
        expressed as pid boundaries. The ring iterates in ascending token order
        and pid_for_hash is monotone, so the result is naturally sorted.
        Runs in O(n log n).
        """
        prev: VNode | None = None
        total = 0

        for vnode in ring:
            if prev is None:
                prev = ring.predecessor(vnode.token)

            assert prev is not None
            start_pid = self.pid_for_hash(prev.token)
            end_pid = self._pid_successor(vnode.token)
            count = self.range_size(start_pid, end_pid)
            total += count

            yield PartitionRange(
                owner=vnode,
                pred=prev,
                start_pid=start_pid,
                end_pid=end_pid,
                count=count,
            )

            prev = vnode
            if total == self._total_partitions:
                break

    def _pid_successor(self, token: int) -> int:
        pid = self.pid_for_hash(token)
        partition = self.partition_for(pid)
        if token == partition.end:
            return pid
        return (pid - 1) % self._total_partitions
