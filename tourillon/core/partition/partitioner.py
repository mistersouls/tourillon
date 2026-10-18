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

from collections.abc import Iterator
from dataclasses import dataclass

from tourillon.core.partition.types import LogicalPartition
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode


@dataclass(frozen=True)
class OwnerSegment:
    # [lo, hi] inclusive range
    lo: int
    hi: int
    owner: VNode


@dataclass
class PartitionRange:
    owner: VNode
    pred: VNode
    # None for empty arc,
    # (i, i] full ring arc,
    # (i, i+1] for single-item arc,
    # (i, j] for multi-item arc
    arc: tuple[int, int] | None

    def linear(self, total: int) -> list[OwnerSegment]:
        if self.arc is None:
            return []

        start, end = self.arc

        if start == end:
            return [OwnerSegment(lo=0, hi=total - 1, owner=self.owner)]
        if start < end:
            return [OwnerSegment(lo=start + 1, hi=end, owner=self.owner)]

        segments = [OwnerSegment(lo=0, hi=end, owner=self.owner)]
        if start + 1 < total:
            segments.append(OwnerSegment(lo=start + 1, hi=total - 1, owner=self.owner))
        return segments


class Partitioner:
    def __init__(self, pshift: int, gshift: int, hashspace: HashSpace) -> None:
        if pshift >= hashspace.bits:
            raise ValueError(
                f"partition_shift ({pshift}) must be "
                f"strictly less than bits ({hashspace.bits})"
            )
        if gshift >= pshift:
            raise ValueError(
                f"group_shift ({gshift}) must be strictly "
                f"less than partition_shift ({pshift})"
            )

        self._pshift = pshift
        self._gshift = gshift
        self._hashspace = hashspace

        self._total_partitions = 1 << pshift
        self._total_groups = 1 << gshift
        self._partition_step = hashspace.max >> pshift
        self._pids_per_group = 1 << (pshift - gshift)

    @property
    def space(self) -> HashSpace:
        return self._hashspace

    @property
    def partition_shift(self) -> int:
        return self._pshift

    @property
    def total_partitions(self) -> int:
        return self._total_partitions

    @property
    def total_groups(self) -> int:
        return self._total_groups

    @property
    def pid_per_group(self) -> int:
        return self._pids_per_group

    def pid_for_hash(self, h: int) -> int:
        return h >> (self._hashspace.bits - self._pshift)

    def group_for(self, pid: int) -> int:
        return pid >> (self._pshift - self._gshift)

    def group_bounds(self, gid: int) -> tuple[int, int]:
        if gid < 0 or gid >= self._total_groups:
            raise ValueError(f"invalid group_id {gid}")

        size = self._pids_per_group
        start = gid * size
        end = start + size - 1
        return start, end

    def groups_for_arc(self, arc: tuple[int, int]) -> set[int]:
        """Return the set of group IDs touched by a partition segment."""
        lo, hi = arc
        max_pid = self._total_partitions - 1

        if lo < 0 or hi < 0 or lo > max_pid or hi > max_pid:
            raise ValueError(f"invalid segment bounds: [{lo}, {hi}]")

        # Standard linear segment: [lo, hi]
        if lo <= hi:
            start_gid = self.group_for(lo)
            end_gid = self.group_for(hi)
            return set(range(start_gid, end_gid + 1))

        # Defensive wrap-around support: [lo, max_pid] U [0, hi]
        g1_start = self.group_for(lo)
        g1_end = self.group_for(max_pid)
        g2_start = self.group_for(0)
        g2_end = self.group_for(hi)

        groups = set(range(g1_start, g1_end + 1))
        groups.update(range(g2_start, g2_end + 1))
        return groups

    def partition_for(self, pid: int) -> LogicalPartition:
        start = pid * self._partition_step
        end = ((pid + 1) * self._partition_step) % self._hashspace.max
        return LogicalPartition(pid=pid, start=start, end=end)

    def ranges_for(self, ring: Ring) -> Iterator[PartitionRange]:
        n = len(ring)
        if n == 0:
            return

        if n == 1:
            vnode = ring[0]
            pid = (self.pid_for_hash(vnode.token) - 1) % self._total_partitions
            yield PartitionRange(owner=vnode, pred=vnode, arc=(pid, pid))
            return

        for i in range(n):
            pred = ring[i - 1]
            succ = ring[i]
            yield PartitionRange(owner=succ, pred=pred, arc=self._arc_for(pred, succ))

    def segments_for(self, ring: Ring) -> Iterator[OwnerSegment]:
        """Yield owner segments sorted by ascending pid.

        Only the arc that wraps around the ring can own the last partition
        without starting at pid 0: that tail segment is emitted last.
        """
        last_pid = self._total_partitions - 1
        tail: OwnerSegment | None = None
        for pr in self.ranges_for(ring):
            for segment in pr.linear(self._total_partitions):
                if segment.lo > 0 and segment.hi == last_pid:
                    tail = segment
                else:
                    yield segment
        if tail is not None:
            yield tail

    def arc_count(self, arc: tuple[int, int] | None) -> int:
        if arc is None:
            return 0

        start, end = arc
        if start == end:
            return self._total_partitions
        if start < end:
            return end - start
        return (self._total_partitions - start) + end

    def _arc_for(self, pred: VNode, succ: VNode) -> tuple[int, int] | None:
        """Return the (start, end] pid arc owned by succ.

        Routing assigns a partition to ring.successor(partition.end), so succ
        owns every partition whose end boundary falls in (pred.token, succ.token].
        Those are the pids [pid(pred), pid(succ) - 1], i.e. the arc
        (pid(pred) - 1, pid(succ) - 1].
        """
        pred_pid = self.pid_for_hash(pred.token)
        succ_pid = self.pid_for_hash(succ.token)

        if pred.token == succ.token:
            return None

        # Empty arc means no partition boundary is crossed in (pred.token, succ.token].
        # With the (start, end] pid convention, equal bounds encode full-ring, not empty.
        if pred.token < succ.token and pred_pid == succ_pid:
            return None

        total = self._total_partitions
        return (pred_pid - 1) % total, (succ_pid - 1) % total
