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
"""Immutable sorted vnode ring."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterator

from tourillon.core.ring.vnode import VNode


class Ring:
    def __init__(self, vnodes: list[VNode] | None = None) -> None:
        self._vnodes: list[VNode] = sorted(vnodes or [], key=lambda vnode: vnode.token)

    @classmethod
    def empty(cls) -> Ring:
        return cls([])

    @classmethod
    def _from_sorted(cls, vnodes: list[VNode]) -> Ring:
        ring = cls.__new__(cls)
        ring._vnodes = vnodes  # noqa: SLF001
        return ring

    def successor(self, token: int) -> VNode:
        if not self._vnodes:
            raise ValueError("Cannot find successor in an empty ring")
        idx = bisect_right(self._vnodes, token, key=lambda vnode: vnode.token)
        if idx == len(self._vnodes):
            idx = 0
        return self._vnodes[idx]

    def predecessor(self, token: int) -> VNode:
        if not self._vnodes:
            raise ValueError("Cannot find predecessor in an empty ring")
        idx = bisect_left(self._vnodes, token, key=lambda vnode: vnode.token)
        return self._vnodes[(idx - 1) % len(self._vnodes)]

    def add_vnodes(self, vnodes: list[VNode]) -> Ring:
        if not vnodes:
            return self
        new_sorted = sorted(vnodes, key=lambda vnode: vnode.token)
        return Ring._from_sorted(self._merge_sorted(self._vnodes, new_sorted))

    def drop_nodes(self, node_ids: set[str]) -> Ring:
        return Ring._from_sorted([v for v in self._vnodes if v.node_id not in node_ids])

    def iter_from(self, vnode: VNode) -> Iterator[VNode]:
        if not self._vnodes:
            return
        idx = bisect_left(
            self._vnodes, vnode.token, key=lambda candidate: candidate.token
        )
        for offset in range(len(self._vnodes)):
            yield self._vnodes[(idx + offset) % len(self._vnodes)]

    def __len__(self) -> int:
        return len(self._vnodes)

    def __iter__(self) -> Iterator[VNode]:
        return iter(self._vnodes)

    @staticmethod
    def _merge_sorted(left: list[VNode], right: list[VNode]) -> list[VNode]:
        i = j = 0
        merged: list[VNode] = []
        while i < len(left) and j < len(right):
            if left[i].token <= right[j].token:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                j += 1
        merged.extend(left[i:])
        merged.extend(right[j:])
        return merged
