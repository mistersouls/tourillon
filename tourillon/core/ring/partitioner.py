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
"""Partition grid and placement helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


@dataclass(frozen=True)
class LogicalPartition:
    pid: int
    start: int
    end: int

    def contains(self, h: int) -> bool:
        if self.start < self.end:
            return self.start < h <= self.end
        return h > self.start or h <= self.end


@dataclass(frozen=True)
class PartitionPlacement:
    segment: int
    partition: LogicalPartition
    vnode: VNode


@dataclass(frozen=True)
class PartitionRange:
    owner: VNode
    start_pid: int
    end_pid: int
    count: int

    @property
    def wraps(self) -> bool:
        return self.start_pid > self.end_pid


class Partitioner:
    def __init__(
        self,
        hash_space: HashSpace,
        partition_shift: int,
        segment_shift: int,
    ) -> None:
        if partition_shift >= hash_space.bits:
            raise ValueError(
                f"partition_shift ({partition_shift}) must be strictly less than bits ({hash_space.bits})"
            )
        if segment_shift >= partition_shift:
            raise ValueError(
                f"segment_shift ({segment_shift}) must be strictly less than partition_shift ({partition_shift})"
            )
        self._hash_space = hash_space
        self._partition_shift = partition_shift
        self._segment_shift = segment_shift
        self._total_partitions = 1 << partition_shift
        self._total_segments = 1 << segment_shift
        self._partition_step = hash_space.max >> partition_shift
        self._pids_per_segment = 1 << (partition_shift - segment_shift)

    @property
    def total_partitions(self) -> int:
        return self._total_partitions

    @property
    def total_segments(self) -> int:
        return self._total_segments

    @property
    def partition_shift(self) -> int:
        return self._partition_shift

    @property
    def segment_shift(self) -> int:
        return self._segment_shift

    @property
    def pids_per_segment(self) -> int:
        return self._pids_per_segment

    def pid_for_hash(self, h: int) -> int:
        return h >> (self._hash_space.bits - self._partition_shift)

    def segment_for(self, pid: int) -> int:
        return pid >> (self._partition_shift - self._segment_shift)

    def partition_for(self, pid: int) -> LogicalPartition:
        start = pid * self._partition_step
        end = ((pid + 1) * self._partition_step) % self._hash_space.max
        return LogicalPartition(pid=pid, start=start, end=end)

    def placement_for_token(self, token: int, ring: Ring) -> PartitionPlacement:
        pid = self.pid_for_hash(token)
        partition = self.partition_for(pid)
        vnode = ring.successor(partition.end)
        return PartitionPlacement(
            segment=self.segment_for(pid),
            partition=partition,
            vnode=vnode,
        )

    def ranges_for(self, node_id: str, ring: Ring) -> list[PartitionRange]:
        """Return partition ranges owned by node_id on ring, in ascending start_pid order.

        For each vnode belonging to node_id, the owned range spans from
        predecessor(vnode.token) + 1 up to vnode.token (inclusive),
        expressed as pid boundaries. The ring iterates in ascending token order
        and pid_for_hash is monotone, so the result is naturally sorted.
        For a single-vnode ring, normalize the wrapped range to unwrapped.
        Runs in O(n log n) where n = len(ring).
        """
        if len(ring) == 0:
            return []

        is_single = len(ring) == 1
        ranges: list[PartitionRange] = []

        for vnode in ring:
            if vnode.node_id != node_id:
                continue

            # Single vnode owns full unwrapped circle; multi-vnode uses predecessor.
            if is_single:
                start_pid = 0
                end_pid = self._total_partitions - 1
                count = self._total_partitions
            else:
                pred = ring.predecessor(vnode.token)
                start_pid = self.pid_for_hash(pred.token) + 1
                end_pid = self.pid_for_hash(vnode.token)
                count = (
                    (self._total_partitions - start_pid) + end_pid + 1
                    if start_pid > end_pid
                    else end_pid - start_pid + 1
                )

            ranges.append(
                PartitionRange(
                    owner=vnode,
                    start_pid=start_pid,
                    end_pid=end_pid,
                    count=count,
                )
            )
        return ranges
