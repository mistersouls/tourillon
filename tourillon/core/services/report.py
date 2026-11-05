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
"""The full status of this node and its health verdict.

Adds to the base node status what costs a storage read or a scan of the
obligations: storage usage, pending delivery, join or drain progress, and the
TLS state. The health verdict is computed here so a supervision probe
receives a status and a few reasons only. Nothing contacts another node.
"""

import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime

from tourillon.core.ports.storage import Storage
from tourillon.core.services.aeinspector import AntiEntropyInspector
from tourillon.core.services.antientropy import AntiEntropyService
from tourillon.core.services.inspector import NodeInspector
from tourillon.core.transports.handshakes import HandshakeMonitor
from tourlib.models.ae import AeStatus, AeStatusRequest
from tourlib.models.health import (
    CERTIFICATE_WARNING_S,
    PENDING_WARNING_S,
    STORAGE_CRITICAL,
    STORAGE_WARNING,
    HealthReason,
    NodeHealth,
)
from tourlib.models.inspect import (
    AntiEntropyInfo,
    MembersPage,
    MembersRequest,
    NodeStatus,
    ProgressInfo,
    StorageInfo,
    TlsInfo,
    WaitingOn,
)

# Peers listed under a join or drain; the others are only counted.
WAITING_LIMIT = 10
_MOVING_PHASES = ("JOINING", "DRAINING", "PAUSED")


class NodeReport:
    def __init__(
        self,
        *,
        inspector: NodeInspector,
        ae_inspector: AntiEntropyInspector,
        antientropy: AntiEntropyService,
        storage: Storage,
        total_groups: int,
        handshakes: HandshakeMonitor,
        certificate_expires_at: datetime,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._inspector = inspector
        self._ae_inspector = ae_inspector
        self._ae = antientropy
        self._storage = storage
        self._total_groups = total_groups
        self._handshakes = handshakes
        self._expires_at = certificate_expires_at
        self._clock = clock

    async def status(self) -> NodeStatus:
        base = await self._inspector.status()
        pending = await self._ae_inspector.status(AeStatusRequest(limit=1))
        usage = await self._storage.usage(self._total_groups)
        return replace(
            base,
            antientropy=with_pending(base.antientropy, pending),
            storage=StorageInfo(
                map_size=usage.map_size,
                used=usage.used,
                versions=usage.versions,
                retention=base.gc.versions,
            ),
            tls=self._tls(),
            progress=await self._progress(),
        )

    async def health(self) -> NodeHealth:
        status = await self.status()
        down = await self._inspector.members(
            MembersRequest(statuses=("DEAD", "SUSPECT"), limit=1)
        )
        now = self._clock()
        reasons = critical_reasons(status, now) + warning_reasons(status, down, now)
        return NodeHealth.from_reasons(status.node_id, reasons)

    def _tls(self) -> TlsInfo:
        rejections = self._handshakes.snapshot()
        return TlsInfo(
            expires_at=self._expires_at.isoformat(),
            rejected=rejections.count,
            last_rejection=rejections.last_reason,
            last_rejection_ago_s=rejections.last_ago_s,
        )

    async def _progress(self) -> ProgressInfo | None:
        goal = self._ae.goal
        if goal is None:
            return None
        ranked = sorted(goal.waiting.items(), key=lambda item: (-item[1], item[0]))
        waiting = [await self._waiting(peer, n) for peer, n in ranked[:WAITING_LIMIT]]
        return ProgressInfo(
            goal=goal.goal,
            total=goal.total,
            done=goal.done,
            waiting=tuple(waiting),
            waiting_more=max(0, len(ranked) - WAITING_LIMIT),
        )

    async def _waiting(self, peer: str, partitions: int) -> WaitingOn:
        view = await self._inspector.member_view(peer)
        retry_in, error, _ = self._ae.delivery_state(peer)
        return WaitingOn(
            node_id=peer,
            partitions=partitions,
            phase=None if view is None else view.phase,
            status="DEAD" if view is None else view.status,
            since_s=None if view is None else view.since_s,
            retry_in_s=round(retry_in, 3),
            last_error=error,
        )


def with_pending(info: AntiEntropyInfo, pending: AeStatus) -> AntiEntropyInfo:
    oldest = pending.items[0].oldest_wall_ms if pending.items else None
    return replace(
        info,
        running=pending.running,
        pending_versions=pending.versions,
        pending_targets=pending.targets,
        oldest_wall_ms=oldest,
        stuck=pending.stuck,
        pending_truncated=pending.truncated,
    )


def critical_reasons(status: NodeStatus, now: float) -> list[HealthReason]:
    reasons = _storage_reasons(status, critical=True)
    reasons += _certificate_reasons(status, now, critical=True)
    info = status.antientropy
    if info.stuck:
        reasons.append(
            _critical(
                "delivery_stuck",
                f"Delivery to {_nodes(info.stuck)} is stuck; "
                f"see: tourctl ae status {status.address}",
            )
        )
    if not info.running and status.phase != "IDLE":
        reasons.append(_critical("ae_stopped", "The anti-entropy worker is stopped"))
    if not status.gc.running:
        reasons.append(_critical("gc_stopped", "The GC worker is stopped"))
    return reasons


def warning_reasons(
    status: NodeStatus, down: MembersPage, now: float
) -> list[HealthReason]:
    reasons = []
    if status.phase in _MOVING_PHASES:
        reasons.append(HealthReason("phase", _phase_message(status)))
    if down.total:
        reasons.append(HealthReason("members_down", _members_message(down)))
    pending = _pending_message(status, now)
    if pending is not None:
        reasons.append(HealthReason("pending_old", pending))
    reasons += _storage_reasons(status, critical=False)
    reasons += _certificate_reasons(status, now, critical=False)
    return reasons


def _critical(code: str, message: str) -> HealthReason:
    return HealthReason(code, message, critical=True)


def _storage_reasons(status: NodeStatus, *, critical: bool) -> list[HealthReason]:
    storage = status.storage
    if storage is None:
        return []
    ratio = storage.used_ratio
    full = ratio >= STORAGE_CRITICAL
    if full != critical or ratio < STORAGE_WARNING:
        return []
    code = "storage_full" if critical else "storage_high"
    return [HealthReason(code, f"Storage is {ratio:.0%} full", critical)]


def _certificate_reasons(
    status: NodeStatus, now: float, *, critical: bool
) -> list[HealthReason]:
    if status.tls is None:
        return []
    expires_at = datetime.fromisoformat(status.tls.expires_at)
    left = expires_at.timestamp() - now
    day = expires_at.date().isoformat()
    if critical and left <= 0:
        return [_critical("certificate_expired", f"Certificate expired on {day}")]
    if not critical and 0 < left < CERTIFICATE_WARNING_S:
        return [
            HealthReason(
                "certificate_expiring",
                f"Certificate expires in {duration(left)} ({day})",
            )
        ]
    return []


def _phase_message(status: NodeStatus) -> str:
    progress = status.progress
    if progress is None or not progress.total:
        return f"Node is {status.phase}"
    return (
        f"Node is {status.phase}: {progress.done:,} of {progress.total:,} "
        "partitions done"
    )


def _members_message(down: MembersPage) -> str:
    dead = sum(count.dead for count in down.summary)
    suspect = sum(count.suspect for count in down.summary)
    parts = [f"{n:,} {label}" for n, label in ((dead, "DEAD"), (suspect, "SUSPECT"))]
    counts = ", ".join(part for part in parts if not part.startswith("0 "))
    noun = "member" if down.total == 1 else "members"
    text = f"{down.total:,} {noun} not ALIVE ({counts})"
    if not down.items:
        return text
    first = down.items[0]
    since = "" if first.since_s is None else f" for {duration(first.since_s)}"
    return f"{text}: {first.node_id} {first.status}{since}"


def _pending_message(status: NodeStatus, now: float) -> str | None:
    info = status.antientropy
    if info.oldest_wall_ms is None:
        return None
    age = now - info.oldest_wall_ms / 1000
    if age <= PENDING_WARNING_S:
        return None
    return (
        f"{info.pending_versions:,} versions pending for "
        f"{_nodes(info.pending_targets)}, oldest {duration(age)}"
    )


def _nodes(count: int) -> str:
    return f"{count:,} node" if count == 1 else f"{count:,} nodes"


def duration(seconds: float) -> str:
    """Coarse human duration: 45s, 14m, 3h, 2d."""
    seconds = max(0.0, seconds)
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{int(seconds // size)}{unit}"
    return f"{int(seconds)}s"
