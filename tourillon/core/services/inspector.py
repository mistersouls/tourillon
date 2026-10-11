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

import asyncio
from collections.abc import AsyncIterator, Iterable, Iterator

from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.registry import MemberRegistry
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer.pool import PeerPool
from tourlib.models.inspect import (
    MemberInspect,
    NodeInfo,
    NodeInspectRequest,
    RingPositionInspect,
    VnodeRangeInspect,
)


class NodeInspector:
    INSPECT_CHUNK_SIZE = 1024

    def __init__(
        self,
        node_id: str,
        stats: GossipStats,
        peer_pool: PeerPool,
        partitioner: Partitioner,
        topologymgr: TopologyManager,
    ) -> None:
        self._node_id = node_id
        self._stats = stats
        self._peer_pool = peer_pool
        self._partitioner = partitioner
        self._topologymgr = topologymgr

    async def inspect(self, request: NodeInspectRequest) -> AsyncIterator[NodeInfo]:
        topology = await self._topologymgr.snapshot()
        snapshot_at = NodeInfo.now_iso()
        self_member = self._build_self_member(topology.registry)
        filters = self._build_filters(request)
        member_by_id = self._build_node_map(
            topology.registry,
            prefix=request.node_id_prefix,
            phase_filter=filters["phases"],
            probe_filter=filters["probe_statuses"],
        )
        ring_position = self._build_ring_position(topology.ring)
        node_ids = self._collect_node_ids(member_by_id, request)
        chunk_size = min(self.INSPECT_CHUNK_SIZE, request.limit)
        gossip_stats = self._stats.to_dict()

        if not node_ids:
            yield self._build_chunk(
                snapshot_at=snapshot_at,
                chunk_index=0,
                chunk_ids=(),
                member_by_id=member_by_id,
                total_nodes=0,
                chunk_size=chunk_size,
                gossip_stats=gossip_stats,
                ring_position=ring_position,
                self_member=self_member,
            )
            return

        for chunk_index, chunk_ids in enumerate(self._iter_chunks(node_ids, chunk_size)):
            yield self._build_chunk(
                snapshot_at=snapshot_at,
                chunk_index=chunk_index,
                chunk_ids=chunk_ids,
                member_by_id=member_by_id,
                total_nodes=len(node_ids),
                chunk_size=chunk_size,
                gossip_stats=gossip_stats,
                ring_position=ring_position,
                self_member=self_member,
            )
            # yield loop
            await asyncio.sleep(0)

    @staticmethod
    def _build_filters(request: NodeInspectRequest) -> dict[str, set[str]]:
        return {
            "phases": set(request.phases),
            "probe_statuses": set(request.probe_statuses),
        }

    def _build_node_map(
        self,
        registry: Iterable,
        *,
        prefix: str | None,
        phase_filter: set[str],
        probe_filter: set[str],
    ) -> dict[str, MemberInspect]:
        members: dict[str, MemberInspect] = {}
        for member in sorted(registry, key=lambda item: item.node_id):
            node_id = member.node_id
            phase = member.phase.value.upper()
            status = self._status_for(node_id)
            if prefix is not None and not node_id.startswith(prefix):
                continue
            if phase_filter and phase not in phase_filter:
                continue
            if probe_filter and status not in probe_filter:
                continue
            members[node_id] = MemberInspect(
                node_id=node_id,
                phase=phase,
                status=status,
                epoch=member.epoch,
            )
        return members

    def _build_self_member(self, registry: MemberRegistry) -> MemberInspect:
        member = registry.get(self._node_id)
        if member is None:
            raise RuntimeError(f"self member '{self._node_id}' not found in topology registry")
        return MemberInspect(
            node_id=member.node_id,
            phase=member.phase.value.upper(),
            status=self._status_for(member.node_id),
            epoch=member.epoch,
        )

    def _status_for(self, node_id: str) -> str:
        if node_id == self._node_id:
            return "-"

        fd = self._peer_pool.failure_detector(node_id)
        if fd is not None:
            status = fd.status()
            return status.value.upper()

        return "UNKNOWN"

    @staticmethod
    def _collect_node_ids(
        member_by_id: dict[str, MemberInspect],
        request: NodeInspectRequest,
    ) -> list[str]:
        node_ids: list[str] = []
        for node_id in sorted(member_by_id):
            if request.cursor is not None and node_id <= request.cursor:
                continue
            if request.only_anomalies and not NodeInspector._is_anomalous(
                member_by_id.get(node_id),
            ):
                continue
            node_ids.append(node_id)
        return node_ids

    @staticmethod
    def _is_anomalous(member: MemberInspect | None) -> bool:
        if member is None:
            return False
        return member.phase != "READY" or member.status in {"SUSPECT", "DEAD"}

    @staticmethod
    def _iter_chunks(node_ids: list[str], chunk_size: int) -> Iterator[tuple[str, ...]]:
        for start in range(0, len(node_ids), chunk_size):
            yield tuple(node_ids[start:start + chunk_size])

    def _build_ring_position(self, ring) -> RingPositionInspect:
        vnode_ranges: list[VnodeRangeInspect] = []
        owned_partitions = 0
        total = self._partitioner.total_partitions

        for partition_range in self._partitioner.ranges_for(ring):
            if partition_range.owner.node_id != self._node_id:
                continue

            partitions = self._partitioner.arc_count(partition_range.arc)
            owned_partitions += partitions
            token = self._format_token(partition_range.owner.token)
            arc = partition_range.arc
            vnode_ranges.append(
                VnodeRangeInspect(
                    token=token,
                    arc=arc,
                    partitions=partitions
                )
            )

        owned_percent = 0.0 if total == 0 else (owned_partitions / total) * 100
        return RingPositionInspect(
            partition_shift=self._partitioner.partition_shift,
            total_partitions=total,
            owned_partitions=owned_partitions,
            owned_percent=owned_percent,
            vnode_ranges=tuple(vnode_ranges),
        )

    @staticmethod
    def _format_token(token: int) -> str:
        return f"0x{token:x}"

    def _build_chunk(
        self,
        *,
        snapshot_at: str,
        chunk_index: int,
        chunk_ids: tuple[str, ...],
        member_by_id: dict[str, MemberInspect],
        total_nodes: int,
        chunk_size: int,
        gossip_stats: dict[str, int | str | None],
        ring_position: RingPositionInspect,
        self_member: MemberInspect,
    ) -> NodeInfo:
        end = (chunk_index + 1) * chunk_size
        has_more = end < total_nodes
        next_cursor = chunk_ids[-1] if has_more else None
        return NodeInfo(
            node_id=self._node_id,
            snapshot_at=snapshot_at,
            chunk_index=chunk_index,
            has_more=has_more,
            next_cursor=next_cursor,
            self_member=self_member,
            members=tuple(member_by_id[node_id] for node_id in chunk_ids if node_id in member_by_id),
            ring_position=ring_position,
            gossip_stats=gossip_stats if chunk_index == 0 else {},
        )
