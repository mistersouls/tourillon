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

from tourillon.core.models.transfer import RangeTransfer, TransferPlan
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode


class TransferPlanner:
    def __init__(
        self,
        node_id: str,
        partitioner: Partitioner,
        replication_factor: int,
    ) -> None:
        self._node_id = node_id
        self._partitioner = partitioner
        self._replication_factor = replication_factor

    def plan(self, current: Ring, target: Ring) -> TransferPlan:
        transfers: list[RangeTransfer] = []
        current_iter = self._partitioner.segments_for(current)
        target_iter = self._partitioner.segments_for(target)
        current_segment = next(current_iter, None)
        target_segment = next(target_iter, None)

        while True:
            if current_segment is None or target_segment is None:
                break

            current_replicas = self._replicaset(current_segment.owner, current)
            new_replicas = self._replicaset(target_segment.owner, target)
            lo = max(current_segment.lo, target_segment.lo)
            hi = min(current_segment.hi, target_segment.hi)

            if lo <= hi and current_replicas != new_replicas:
                transfers.extend(self._pair_segments((lo, hi), current_replicas, new_replicas))

            if current_segment.hi < target_segment.hi:
                current_segment = next(current_iter, None)
            elif target_segment.hi < current_segment.hi:
                target_segment = next(target_iter, None)
            else:
                current_segment = next(current_iter, None)
                target_segment = next(target_iter, None)

        return TransferPlan(ranges=transfers, serial=current.serial())

    def _pair_segments(
        self,
        segment: tuple[int, int],
        old_replicas: frozenset[str],
        new_replicas: frozenset[str],
    ) -> list[RangeTransfer]:
        leaving = sorted(old_replicas - new_replicas)
        entering = sorted(new_replicas - old_replicas)
        if not entering:
            return []

        if not (self._node_id in old_replicas or self._node_id in new_replicas):
            return []

        result: list[RangeTransfer] = []
        for src, dst in zip(leaving, entering, strict=False):
            result.append(RangeTransfer(segment=segment, src=src, dst=dst))

        unpaired = entering[len(leaving):]
        if not unpaired:
            return result

        src = None if not old_replicas else min(old_replicas)
        if src is None:
            return result
        for dst in unpaired:
            result.append(RangeTransfer(segment=segment, src=src, dst=dst))

        return result

    def _replicaset(self, owner: VNode, ring: Ring) -> frozenset[str]:
        replicas: set[str] = set()
        for vnode in ring.iter_from(owner):
            if vnode.node_id in replicas:
                continue
            replicas.add(vnode.node_id)
            if len(replicas) == self._replication_factor:
                break
        return frozenset(replicas)

