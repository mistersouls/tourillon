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
"""Operator views of this node and of the cluster as this node sees it.

Nothing here contacts another node: the member list is the local gossip view
(no fan-out), statuses combine the local failure detector with the age of the
member's gossip heartbeat, and every answer has a bounded size whatever the
cluster size.
"""

import base64
import bisect
import json
import time
from collections.abc import Awaitable, Callable, Iterable
from importlib.metadata import PackageNotFoundError, version

from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.heartbeat import HeartbeatTable
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer.pool import PeerPool
from tourillon.core.probe.detector import PeerStatus
from tourlib.exceptions import ValidationError
from tourlib.models.gc import GcInfo
from tourlib.models.inspect import (
    PHASES,
    AntiEntropyInfo,
    MembersPage,
    MembersRequest,
    MembersSummary,
    MemberView,
    NodeStatus,
    PhaseCount,
    RingPositionInspect,
    VnodeRangeInspect,
    now_iso,
)

_SEVERITY = {"DEAD": 0, "SUSPECT": 1}
_ANOMALY = 2

type SortKey = tuple


def _package_version() -> str:
    try:
        return version("tourillon")
    except PackageNotFoundError:
        return "unknown"


def encode_cursor(key: SortKey) -> str:
    raw = json.dumps(list(key), separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> SortKey:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        key = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except (ValueError, TypeError) as exc:
        raise ValidationError("invalid cursor") from exc
    if not isinstance(key, list) or not key:
        raise ValidationError("invalid cursor")
    return tuple(key)


class NodeInspector:
    def __init__(
        self,
        node_id: str,
        stats: GossipStats,
        peer_pool: PeerPool,
        partitioner: Partitioner,
        topologymgr: TopologyManager,
        gc_snapshot: Callable[[], GcInfo],
        heartbeats: HeartbeatTable,
        antientropy: Callable[[], AntiEntropyInfo],
        local_member: Callable[[], Awaitable[Member]],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._node_id = node_id
        self._stats = stats
        self._peer_pool = peer_pool
        self._partitioner = partitioner
        self._topologymgr = topologymgr
        self._gc_snapshot = gc_snapshot
        self._heartbeats = heartbeats
        self._antientropy = antientropy
        self._local_member = local_member
        self._clock = clock
        self._version = _package_version()

    async def status(self) -> NodeStatus:
        topology = await self._topologymgr.snapshot()
        me = topology.registry.get(self._node_id)
        count = len(topology.registry)
        if me is None:
            # An IDLE node waiting for "node join" has not gossiped itself yet.
            me = await self._local_member()
            count += 1

        now = self._clock()
        ready_alive = sum(
            1
            for member in topology.registry
            if member.phase is MemberPhase.READY
            and self.status_of(member.node_id, count, now) == "ALIVE"
        )
        return NodeStatus(
            node_id=self._node_id,
            address=me.peer_address,
            version=self._version,
            snapshot_at=now_iso(),
            phase=me.phase.value.upper(),
            epoch=me.epoch,
            since_s=self._phase_age(self._node_id, now),
            ring_position=self._build_ring_position(topology.ring),
            members=MembersSummary(total=count, ready_alive=ready_alive),
            antientropy=self._antientropy(),
            gc=self._gc_snapshot(),
            gossip_stats=self._stats.to_dict(),
        )

    async def members(self, request: MembersRequest) -> MembersPage:
        topology = await self._topologymgr.snapshot()
        now = self._clock()
        count = len(topology.registry)
        views = [self._view(member, count, now) for member in topology.registry]
        summary = summarise(views)

        selected = [
            (self._sort_key(view, request, now), view)
            for view in views
            if matches(view, request)
        ]
        selected.sort(key=lambda pair: pair[0])
        keys = [key for key, _ in selected]
        start = 0
        if request.cursor is not None:
            try:
                start = bisect.bisect_right(keys, decode_cursor(request.cursor))
            except TypeError as exc:
                raise ValidationError("invalid cursor") from exc

        end = start + request.limit
        page = selected[start:end]
        cursor = encode_cursor(page[-1][0]) if page and end < len(selected) else None
        return MembersPage(
            node_id=self._node_id,
            snapshot_at=now_iso(),
            members=count,
            summary=summary,
            items=tuple(view for _, view in page),
            total=len(selected),
            cursor=cursor,
            anomalies_only=request.anomalies_only,
        )

    async def member_view(self, node_id: str) -> MemberView | None:
        """This node's view of one member; None when it is not a member."""
        topology = await self._topologymgr.snapshot()
        member = topology.registry.get(node_id)
        if member is None:
            return None
        return self._view(member, len(topology.registry), self._clock())

    def status_of(self, node_id: str, count: int, now: float) -> str:
        """Return ALIVE, SUSPECT or DEAD as this node sees the member.

        The worst of two signals wins: the heartbeat age, known for every
        member, and the failure detector, known only for connected peers.
        """
        if node_id == self._node_id:
            return "ALIVE"
        status = self._heartbeats.status(node_id, count, now)
        detector = self._peer_pool.failure_detector(node_id)
        if detector is not None and status is PeerStatus.ALIVE:
            status = detector.status()
        return status.value.upper()

    def _phase_age(self, node_id: str, now: float) -> float | None:
        since = self._topologymgr.phase_since.get(node_id)
        return None if since is None else max(0.0, now - since)

    def _view(self, member: Member, count: int, now: float) -> MemberView:
        status = self.status_of(member.node_id, count, now)
        heard = self._heartbeats.age(member.node_id, now)
        since = self._phase_age(member.node_id, now)
        if status != "ALIVE" and heard is not None:
            since = heard if since is None else min(since, heard)
        return MemberView(
            node_id=member.node_id,
            address=member.peer_address,
            phase=member.phase.value.upper(),
            status=status,
            epoch=member.epoch,
            last_heard_s=heard,
            since_s=since,
        )

    @staticmethod
    def _sort_key(view: MemberView, request: MembersRequest, now: float) -> SortKey:
        if not request.anomalies_only:
            return (view.node_id,)
        severity = _SEVERITY.get(view.status, _ANOMALY)
        changed_at = now - (view.since_s or 0.0)
        return (severity, changed_at, view.node_id)

    def _build_ring_position(self, ring) -> RingPositionInspect:
        vnode_ranges: list[VnodeRangeInspect] = []
        owned_partitions = 0
        total = self._partitioner.total_partitions

        for partition_range in self._partitioner.ranges_for(ring):
            if partition_range.owner.node_id != self._node_id:
                continue

            partitions = self._partitioner.arc_count(partition_range.arc)
            owned_partitions += partitions
            vnode_ranges.append(
                VnodeRangeInspect(
                    token=f"0x{partition_range.owner.token:x}",
                    arc=partition_range.arc,
                    partitions=partitions,
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


def matches(view: MemberView, request: MembersRequest) -> bool:
    if request.anomalies_only:
        return view.anomalous
    if request.phases and view.phase not in request.phases:
        return False
    if request.statuses and view.status not in request.statuses:
        return False
    return request.prefix is None or view.node_id.startswith(request.prefix)


def summarise(views: Iterable[MemberView]) -> tuple[PhaseCount, ...]:
    """Count members per phase and status; only phases present appear."""
    counts: dict[str, dict[str, int]] = {}
    for view in views:
        row = counts.setdefault(view.phase, {"ALIVE": 0, "SUSPECT": 0, "DEAD": 0})
        row[view.status] = row.get(view.status, 0) + 1

    order = {phase: index for index, phase in enumerate(PHASES)}
    return tuple(
        PhaseCount(
            phase=phase,
            alive=counts[phase]["ALIVE"],
            suspect=counts[phase]["SUSPECT"],
            dead=counts[phase]["DEAD"],
        )
        for phase in sorted(counts, key=lambda p: (order.get(p, len(order)), p))
    )
