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

"""Operator views of anti-entropy: pending delivery per target, obligations.

Everything is read locally: the obligations persisted in META, the backoff
state of the delivery loop and the gossip view. Scans stop at
``SCAN_LIMIT`` obligations so an answer stays bounded on a node that has
accumulated a large debt; counts are then lower bounds.
"""

import bisect
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, replace

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.storageschema import ObligationKey
from tourillon.core.services.antientropy import AntiEntropyService
from tourillon.core.services.inspector import (
    NodeInspector,
    decode_cursor,
    encode_cursor,
)
from tourlib.exceptions import ValidationError
from tourlib.models.ae import (
    SCAN_LIMIT,
    AeCounters,
    AeStatus,
    AeStatusRequest,
    AeTarget,
    ObligationsPage,
    ObligationsRequest,
    ObligationView,
)

# Phases in which a target takes obligation deliveries.
_DELIVERY_PHASES = frozenset({"JOINING", "READY", "DRAINING"})
_PAGE = 1024


@dataclass
class _Debt:
    versions: int
    oldest_wall_ms: int


@dataclass(frozen=True)
class _Moment:
    wall_ms: int
    monotonic: float
    members: int


class AntiEntropyInspector:
    def __init__(
        self,
        node_id: str,
        antientropy: AntiEntropyService,
        total_groups: int,
        topologymgr: TopologyManager,
        members: NodeInspector,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._node_id = node_id
        self._ae = antientropy
        self._total_groups = total_groups
        self._topologymgr = topologymgr
        self._members = members
        self._clock = clock
        self._monotonic = monotonic

    async def _scan(
        self, target: str | None
    ) -> AsyncIterator[tuple[int, ObligationKey]]:
        local = self._ae.local
        for group in range(self._total_groups):
            after: bytes | None = None
            while True:
                page, after = await local.obligation_page(group, after, _PAGE, target)
                for obligation in page:
                    yield group, obligation
                if after is None:
                    break

    async def _debts(self, target: str | None = None) -> tuple[dict[str, _Debt], bool]:
        debts: dict[str, _Debt] = {}
        seen = 0
        async for _, obligation in self._scan(target):
            if seen >= SCAN_LIMIT:
                return debts, True
            seen += 1
            wall_ms = obligation.required_ts.wall_ms
            debt = debts.get(obligation.target)
            if debt is None:
                debts[obligation.target] = _Debt(1, wall_ms)
            else:
                debt.versions += 1
                debt.oldest_wall_ms = min(debt.oldest_wall_ms, wall_ms)
        return debts, False

    def _counters(self) -> AeCounters:
        stats = self._ae.stats
        return AeCounters(
            delivered=stats.obligations_delivered,
            delivery_errors=stats.delivery_errors,
            sweeps=stats.sweeps,
            diverged=stats.pids_diverged,
            pulled=stats.pulled,
            pushed=stats.pushed + stats.handed_off,
            sweep_errors=max(0, stats.errors - stats.delivery_errors),
        )

    def _uptime(self, now: _Moment) -> float | None:
        started = self._ae.started_at
        return None if started is None else max(0.0, now.wall_ms / 1000 - started)

    def _stuck(self, item: AeTarget, ago: float | None, now: _Moment) -> bool:
        """ALIVE, deliverable, not deferred, yet nothing delivered for too long."""
        after = self._ae.stuck_after
        uptime = self._uptime(now)
        return (
            uptime is not None
            and uptime > after
            and item.status == "ALIVE"
            and item.phase in _DELIVERY_PHASES
            and item.retry_in_s == 0
            and (now.wall_ms - item.oldest_wall_ms) / 1000 > after
            and (ago is None or ago > after)
        )

    def _target(self, target: str, debt: _Debt, registry, now: _Moment) -> AeTarget:
        member = registry.get(target)
        phase = None if member is None else member.phase.value.upper()
        status = (
            "DEAD"
            if member is None
            else self._members.status_of(target, now.members, now.monotonic)
        )
        retry_in, error, ago = self._ae.delivery_state(target)
        item = AeTarget(
            target=target,
            versions=debt.versions,
            oldest_wall_ms=debt.oldest_wall_ms,
            phase=phase,
            status=status,
            retry_in_s=round(retry_in, 3),
            last_error=error,
        )
        if self._stuck(item, ago, now):
            item = replace(item, stuck=True)
        return item

    async def status(self, request: AeStatusRequest) -> AeStatus:
        topology = await self._topologymgr.snapshot()
        registry = topology.registry
        me = registry.get(self._node_id)
        debts, truncated = await self._debts(request.target)
        now = _Moment(int(self._clock() * 1000), self._monotonic(), len(registry))
        targets = sorted(
            (self._target(t, debt, registry, now) for t, debt in debts.items()),
            key=lambda item: (item.oldest_wall_ms, item.target),
        )
        page, cursor = _page(
            targets, [(t.oldest_wall_ms, t.target) for t in targets], request
        )
        started = self._ae.started_at
        return AeStatus(
            node_id=self._node_id,
            phase="UNKNOWN" if me is None else me.phase.value.upper(),
            running=self._ae.running,
            started_at_ms=None if started is None else int(started * 1000),
            snapshot_at_ms=now.wall_ms,
            stuck_after_s=self._ae.stuck_after,
            counters=self._counters(),
            items=tuple(page),
            targets=len(targets),
            versions=sum(item.versions for item in targets),
            stuck=sum(1 for item in targets if item.stuck),
            cursor=cursor,
            truncated=truncated,
        )

    async def obligations(self, request: ObligationsRequest) -> ObligationsPage:
        start = _obligation_cursor(request.cursor)
        items: list[ObligationView] = []
        last: tuple[int, bytes] | None = None
        total = 0
        more = truncated = False
        async for group, obligation in self._scan(request.target):
            if total >= SCAN_LIMIT:
                truncated = True
                break
            total += 1
            position = (group, obligation.to_bytes())
            if start is not None and position <= start:
                continue
            if len(items) < request.limit:
                items.append(_view(obligation))
                last = position
            else:
                more = True
        more = more or (truncated and len(items) == request.limit)
        cursor = None
        if more and last is not None:
            cursor = encode_cursor((last[0], last[1].hex()))
        return ObligationsPage(
            node_id=self._node_id,
            snapshot_at_ms=int(self._clock() * 1000),
            target=request.target,
            items=tuple(items),
            total=total,
            cursor=cursor,
            truncated=truncated,
        )


def _page(
    items: list[AeTarget], keys: list[tuple[int, str]], request: AeStatusRequest
) -> tuple[list[AeTarget], str | None]:
    start = 0
    if request.cursor is not None:
        key = decode_cursor(request.cursor)
        if len(key) != 2 or not isinstance(key[0], int) or not isinstance(key[1], str):
            raise ValidationError("invalid cursor")
        start = bisect.bisect_right(keys, (key[0], key[1]))
    end = start + request.limit
    page = items[start:end]
    cursor = None
    if page and end < len(items):
        cursor = encode_cursor(keys[end - 1])
    return page, cursor


def _obligation_cursor(cursor: str | None) -> tuple[int, bytes] | None:
    if cursor is None:
        return None
    key = decode_cursor(cursor)
    if len(key) != 2 or not isinstance(key[0], int) or not isinstance(key[1], str):
        raise ValidationError("invalid cursor")
    try:
        return key[0], bytes.fromhex(key[1])
    except ValueError as exc:
        raise ValidationError("invalid cursor") from exc


def _view(obligation: ObligationKey) -> ObligationView:
    ts = obligation.required_ts
    address = obligation.key.address
    return ObligationView(
        target=obligation.target,
        partition=obligation.key.pid.value,
        namespace=address.namespace,
        key=address.key,
        wall_ms=ts.wall_ms,
        counter=ts.counter,
        node_id=ts.node_id,
    )
