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
"""Ring — immutable sorted sequence of VNode instances."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterator

from tourillon.core.ring.vnode import VNode


class Ring:
    """Immutable sorted sequence of VNode instances ordered by ascending token.

    Mutations (add_vnodes, drop_nodes) return new Ring instances; the
    original is never modified. A coroutine holding a ring reference across
    an await point can never observe this ring changing underneath it,
    eliminating race conditions without extra locks.

    All operations that mutate the ring by returning a new instance run in
    O(n log n) or O(n) time. Successor lookup is O(log n) via bisect_right.
    """

    def __init__(self, vnodes: list[VNode] | None = None) -> None:
        self._vnodes: list[VNode] = sorted(vnodes or [], key=lambda v: v.token)

    @classmethod
    def empty(cls) -> Ring:
        """Return an empty Ring."""
        return cls([])

    @classmethod
    def _from_sorted(cls, vnodes: list[VNode]) -> Ring:
        """Return a Ring from an already-sorted VNode list, skipping the sort step.

        Callers are responsible for guaranteeing that vnodes is sorted by
        ascending token. This is a private constructor used only by add_vnodes,
        drop_nodes, and _merge_sorted to avoid redundant work.
        """
        instance = cls.__new__(cls)
        instance._vnodes = vnodes  # noqa: SLF001
        return instance

    def successor(self, token: int) -> VNode:
        """Return the first VNode clockwise at or after token.

        Wraps around to the first vnode when token exceeds the highest token
        in the ring. Raise ValueError when the ring is empty.
        """
        if not self._vnodes:
            raise ValueError("Cannot find successor in an empty ring")
        idx = bisect_right(self._vnodes, token, key=lambda v: v.token)
        if idx == len(self._vnodes):
            idx = 0  # Wrap around to the first vnode.
        return self._vnodes[idx]

    def add_vnodes(self, vnodes: list[VNode]) -> Ring:
        """Return a new Ring with all existing and new vnodes, sorted ascending."""
        if not vnodes:
            return self
        new_sorted = sorted(vnodes, key=lambda v: v.token)
        return Ring._from_sorted(self._merge_sorted(self._vnodes, new_sorted))

    def drop_nodes(self, node_ids: set[str]) -> Ring:
        """Return a new Ring without any vnode belonging to node_ids."""
        return Ring._from_sorted([v for v in self._vnodes if v.node_id not in node_ids])

    def iter_from(self, vnode: VNode) -> Iterator[VNode]:
        """Yield all vnodes in clockwise order starting at vnode's token position.

        Uses bisect_left to find the starting index. If vnode is not in the
        ring, iteration begins at the first vnode whose token >= vnode.token.
        Wraps around and yields every vnode exactly once per call.
        """
        if not self._vnodes:
            return
        n = len(self._vnodes)
        idx = bisect_left(self._vnodes, vnode.token, key=lambda v: v.token)
        for i in range(n):
            yield self._vnodes[(idx + i) % n]

    def __len__(self) -> int:
        return len(self._vnodes)

    def __iter__(self) -> Iterator[VNode]:
        return iter(self._vnodes)

    @staticmethod
    def _merge_sorted(a: list[VNode], b: list[VNode]) -> list[VNode]:
        """Merge two sorted VNode lists into a single sorted list.

        Equivalent to the merge step in merge sort; runs in O(n + k).
        """
        i = j = 0
        merged: list[VNode] = []
        len_a, len_b = len(a), len(b)

        while i < len_a and j < len_b:
            if a[i].token <= b[j].token:
                merged.append(a[i])
                i += 1
            else:
                merged.append(b[j])
                j += 1

        if i < len_a:
            merged.extend(a[i:])
        if j < len_b:
            merged.extend(b[j:])

        return merged
