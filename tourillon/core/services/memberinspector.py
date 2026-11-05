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

"""Bounded operator report of a member's placement and anti-entropy debt."""

import asyncio
import time
from collections.abc import Mapping

from tourillon.core.exceptions import MemberNotFoundError
from tourillon.core.gossip.topology import Topology, TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner, PartitionRange
from tourillon.core.peer.pool import PeerPool
from tourillon.core.services.inspector import NodeInspector
from tourillon.core.spaces.vnode import VNode
from tourlib.envelope import build_envelope
from tourlib.exceptions import ProtocolError, ResponseError
from tourlib.models.ae import AeStatus, AeStatusRequest
from tourlib.models.member import (
    MemberDebt,
    MemberReport,
    StandIn,
    UnansweredNeighbor,
)
from tourlib.port import Serializer

_FANOUT = 16
_TIMEOUT = 2.0


class MemberInspector:
    def __init__(
        self,
        node_id: str,
        replication_factor: int,
        partitioner: Partitioner,
        topologymgr: TopologyManager,
        members: NodeInspector,
        peer_pool: PeerPool,
        serializer: Serializer,
    ) -> None:
        self._node_id = node_id
        self._rf = replication_factor
        self._partitioner = partitioner
        self._topologymgr = topologymgr
        self._members = members
        self._peer_pool = peer_pool
        self._serializer = serializer

    async def inspect(self, node_id: str) -> MemberReport:
        topology = await self._topologymgr.snapshot()
        member = topology.registry.get(node_id)
        if member is None:
            raise MemberNotFoundError(node_id, self._node_id)

        view = await self._members.member_view(node_id)
        if view is None:
            raise MemberNotFoundError(node_id, self._node_id)

        partitions, vnodes, neighbors, stand_ins = self._ring_info(
            member, view.status, topology
        )
        debts, unanswered = await self._debts(node_id, neighbors, topology)
        total = self._partitioner.total_partitions
        return MemberReport(
            node_id=member.node_id,
            view_from=self._node_id,
            address=member.peer_address,
            phase=member.phase.value.upper(),
            epoch=member.epoch,
            status=view.status,
            since_s=view.since_s,
            last_heard_s=view.last_heard_s,
            ring_share_percent=0.0 if total == 0 else partitions * 100 / total,
            partitions=partitions,
            total_partitions=total,
            vnodes=vnodes,
            answered=len(neighbors) - len(unanswered),
            neighbors=len(neighbors),
            owed_by=debts,
            unanswered=unanswered,
            stand_ins=stand_ins,
        )

    def _ring_info(
        self, member: Member, status: str, topology: Topology
    ) -> tuple[int, int, tuple[str, ...], tuple[StandIn, ...]]:
        ring_nodes = list(topology.ring)
        owner_ranges: list[PartitionRange] = []
        partitions = 0
        for item in self._partitioner.ranges_for(topology.ring):
            if item.owner.node_id == member.node_id:
                count = self._partitioner.arc_count(item.arc)
                partitions += count
                if count:
                    owner_ranges.append(item)

        vnodes = sum(1 for vnode in ring_nodes if vnode.node_id == member.node_id)
        neighbors = self._neighbors(member.node_id, ring_nodes)
        stand_ins = self._stand_ins(member, status, owner_ranges, topology)
        return partitions, vnodes, neighbors, stand_ins

    def _neighbors(self, node_id: str, ring_nodes: list[VNode]) -> tuple[str, ...]:
        if self._rf <= 1:
            return ()

        target_indexes = [
            index for index, vnode in enumerate(ring_nodes) if vnode.node_id == node_id
        ]
        neighbors: set[str] = set()
        for index in target_indexes:
            for direction in (-1, 1):
                found = 0
                for step in range(1, len(ring_nodes) + 1):
                    peer = ring_nodes[(index + direction * step) % len(ring_nodes)]
                    if peer.node_id != node_id and peer.node_id not in neighbors:
                        neighbors.add(peer.node_id)
                        found += 1
                    if found >= max(0, self._rf - 1):
                        break
        return tuple(sorted(neighbors))

    def _stand_ins(
        self,
        target: Member,
        status: str,
        ranges: list[PartitionRange],
        topology: Topology,
    ) -> tuple[StandIn, ...]:
        if status == "ALIVE" and target.phase is MemberPhase.READY:
            return ()

        allocations: dict[str, int] = {}
        for item in ranges:
            stand_in = self._stand_in(item, topology)
            if stand_in is not None:
                allocations[stand_in] = allocations.get(stand_in, 0) + (
                    self._partitioner.arc_count(item.arc)
                )
        return tuple(
            StandIn(node_id=node_id, partitions=count)
            for node_id, count in sorted(allocations.items())
        )

    def _stand_in(self, item: PartitionRange, topology: Topology) -> str | None:
        seen, last_owner = self._natural_replicas(item, topology)
        now = time.monotonic()
        count = len(topology.registry)

        for vnode in topology.ring.iter_from(last_owner):
            member = topology.registry.get(vnode.node_id)
            if member is None or member.phase is not MemberPhase.READY:
                continue
            if member.node_id in seen:
                continue
            if self._members.status_of(member.node_id, count, now) == "ALIVE":
                return member.node_id

        return None

    def _natural_replicas(
        self, item: PartitionRange, topology: Topology
    ) -> tuple[set[str], VNode]:
        seen: set[str] = set()
        last_owner = item.owner
        for vnode in topology.ring.iter_from(item.owner):
            member = topology.registry.get(vnode.node_id)
            if member is None or member.node_id in seen:
                continue
            if member.phase in (MemberPhase.IDLE, MemberPhase.FAILED):
                continue
            seen.add(member.node_id)
            last_owner = vnode
            if len(seen) == self._rf:
                break

        return seen, last_owner

    async def _debts(
        self,
        target: str,
        neighbors: tuple[str, ...],
        topology: Topology,
    ) -> tuple[tuple[MemberDebt, ...], tuple[UnansweredNeighbor, ...]]:
        semaphore = asyncio.Semaphore(_FANOUT)
        results = await asyncio.gather(
            *(
                self._query_neighbor(peer, target, semaphore, topology)
                for peer in neighbors
            )
        )
        debts = tuple(
            sorted(
                (debt for debt, _ in results if debt is not None),
                key=lambda debt: debt.holder,
            )
        )
        unanswered = tuple(
            sorted(
                (failure for _, failure in results if failure is not None),
                key=lambda failure: failure.node_id,
            )
        )
        return debts, unanswered

    async def _query_neighbor(
        self,
        peer: str,
        target: str,
        semaphore: asyncio.Semaphore,
        topology: Topology,
    ) -> tuple[MemberDebt | None, UnansweredNeighbor | None]:
        try:
            async with semaphore:
                status = await asyncio.wait_for(
                    self._request_status(peer, target),
                    timeout=_TIMEOUT,
                )
        except Exception as exc:
            logger.warning(
                "member.inspect neighbour query failed target=%s peer=%s error=%s",
                target,
                peer,
                exc,
            )
            member_count = len(topology.registry)
            status = self._members.status_of(peer, member_count, time.monotonic())
            return None, UnansweredNeighbor(node_id=peer, status=status)

        if not status.items:
            return None, None
        item = status.items[0]
        return (
            MemberDebt(
                holder=peer,
                versions=item.versions,
                oldest_wall_ms=item.oldest_wall_ms,
                phase=item.phase,
                status=item.status,
                retry_in_s=item.retry_in_s,
                last_error=item.last_error,
                stuck=item.stuck,
            ),
            None,
        )

    async def _request_status(self, peer: str, target: str) -> AeStatus:
        request = AeStatusRequest(limit=1, target=target)
        envelope = build_envelope(
            kind="ae.status",
            payload=self._serializer.encode(request.to_document()),
            schema_id=self._serializer.schema_id,
        )
        async with self._peer_pool.lease(peer) as client:
            response = await client.request(envelope, timeout=_TIMEOUT)
        if response.kind == "ae.status.reject":
            data = self._serializer.decode(response.payload)
            if not isinstance(data, Mapping):
                raise ResponseError("peer returned an invalid ae.status rejection")
            message = data.get("message", "peer rejected ae.status")
            raise ResponseError(str(message))
        if response.kind != "ae.status.accept":
            raise ProtocolError(f"unexpected response kind {response.kind!r}")
        status = AeStatus.from_document(self._serializer.decode(response.payload))
        if status.node_id != peer or any(
            item.target != target for item in status.items
        ):
            raise ProtocolError("ae.status response does not match the requested peer")
        if len(status.items) > 1:
            raise ProtocolError("target-filtered ae.status returned multiple targets")

        return status
