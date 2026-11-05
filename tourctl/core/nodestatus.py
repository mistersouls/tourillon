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
"""``tourctl node status``: one node, summarised.

The member list moved to ``tourctl cluster status``; this view keeps only
counts so it stays the same size whatever the cluster size (E3).
"""

import time
from datetime import datetime
from pathlib import Path

from tourclient import InspectService, client
from tourclient.exception import UnknownServiceError
from tourctl.core.cluster import capped
from tourctl.core.error import INSPECT_ERROR_PROFILE, cli_error
from tourctl.core.render import (
    block,
    fields,
    format_duration,
    format_since,
    format_size,
    messages,
    table,
    to_json,
)
from tourlib.models.gc import GcInfo, GcStatus
from tourlib.models.health import CERTIFICATE_WARNING_S, STORAGE_WARNING
from tourlib.models.inspect import (
    AntiEntropyInfo,
    NodeStatus,
    ProgressInfo,
    RingPositionInspect,
    StorageInfo,
    TlsInfo,
    VnodeRangeInspect,
    WaitingOn,
)

_PHASE_MESSAGES = {
    "JOINING": "This node is JOINING; anti-entropy is filling its partitions.",
    "DRAINING": "This node is DRAINING; anti-entropy is moving its partitions away.",
    "PAUSED": "This node is PAUSED; stand-ins serve its partitions.",
    "IDLE": "This node is IDLE; it owns no partitions. Add it with: tourctl node join",
    "FAILED": "This node is FAILED; it owns no partitions.",
}


async def _fetch_status(
    address: str, tourconf: str | Path | None, context_name: str | None
) -> NodeStatus:
    async with client(
        "inspect",
        endpoint="peer",
        bind=address,
        tourconf=tourconf,
        context_name=context_name,
    ) as service:
        if not isinstance(service, InspectService):
            raise UnknownServiceError("Unknown service: inspect")
        return await service.status()


async def node_status(
    address: str,
    *,
    vnodes: bool = False,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
    now_ms: int | None = None,
) -> str:
    """Fetch ``node.inspect`` from ADDRESS and render it as text or JSON."""
    try:
        status = await _fetch_status(address, tourconf, context_name)
    except Exception as exc:
        raise cli_error(exc, context_name, INSPECT_ERROR_PROFILE) from None

    if as_json:
        document = status.to_document()
        document["messages"] = status_messages(status, now_ms)
        return to_json(document)
    return render_status(status, vnodes=vnodes, now_ms=now_ms)


def render_status(
    status: NodeStatus, *, vnodes: bool = False, now_ms: int | None = None
) -> str:
    parts = [
        fields(
            (
                ("Name", status.node_id),
                ("Address", status.address),
                ("Phase", _phase(status)),
                ("Version", f"tourillon {status.version}"),
            )
        ),
    ]
    if status.progress is not None:
        parts += _progress(status.progress)
    parts.append(block("Ring", _ring(status.ring_position)))
    if vnodes:
        parts.append(_vnode_table(status.ring_position))
    parts += [
        block("Members (view from this node)", _members(status)),
        block("Anti-entropy", _antientropy(status.antientropy, now_ms)),
        block("Garbage collection", _gc(status.gc, now_ms, status.storage is None)),
    ]
    if status.storage is not None:
        parts.append(block("Storage", _storage(status.storage)))
    if status.tls is not None:
        parts.append(block("TLS", _tls(status.tls, now_ms)))
    parts.append(messages(capped(status_messages(status, now_ms))))
    return "\n\n".join(parts)


def _phase(status: NodeStatus) -> str:
    text = f"{status.phase} (epoch {status.epoch})"
    if status.since_s is None:
        return text
    return f"{text}, since {format_duration(status.since_s)}"


def _ring(ring: RingPositionInspect) -> str:
    share = f"{ring.owned_percent:.3g}%"
    return fields(
        (
            (
                "Partitions",
                f"{ring.owned_partitions:,} of {ring.total_partitions:,} ({share})",
            ),
            ("Vnodes", f"{len(ring.vnode_ranges):,}"),
        )
    )


def _vnode_table(ring: RingPositionInspect) -> str:
    rows = [
        (item.token, _partition_label(item, ring.total_partitions), item.partitions)
        for item in ring.vnode_ranges
    ]
    return table(("TOKEN", "PARTITIONS", "COUNT"), rows)


def _partition_label(item: VnodeRangeInspect, total: int) -> str | None:
    """Partition ids owned by the vnode; ``(w)`` marks a wrap past the end."""
    if item.arc is None or item.partitions == 0:
        return None
    start, end = item.arc
    if total <= 0:
        return f"{start}-{end}"
    first = (start + 1) % total
    suffix = " (w)" if start > end else ""
    return f"{first}-{end}{suffix}"


def _members(status: NodeStatus) -> str:
    total = status.members.total
    ready = status.members.ready_alive
    others = total - ready
    noun = "member" if total == 1 else "members"
    return (
        f"{total:,} {noun}: {ready:,} READY ALIVE, {others:,} others\n"
        "See them with: tourctl cluster status"
    )


def _antientropy(info: AntiEntropyInfo, now_ms: int | None) -> str:
    rows = [("Pending", _pending(info, now_ms))]
    if info.queued or info.inflight:
        rows.append(
            (
                "Queue",
                f"{info.queued:,} obligations queued, {info.inflight:,} in flight",
            )
        )
    rows += [
        ("Last sweep", _last_sweep(info, now_ms)),
        ("Delivered", f"{info.delivered:,}"),
    ]
    if info.errors:
        rows.append(("Errors", f"{info.errors:,}"))
    if not info.running:
        rows.append(("Worker", "stopped"))
    return fields(rows)


def _pending(info: AntiEntropyInfo, now_ms: int | None) -> str:
    if not info.pending_versions:
        return "none"
    bound = "at least " if info.pending_truncated else ""
    text = (
        f"{bound}{info.pending_versions:,} versions to {_nodes(info.pending_targets)}"
    )
    if info.oldest_wall_ms is None:
        return text
    age = format_since(info.oldest_wall_ms, now_ms).removesuffix(" ago")
    return f"{text}, oldest {age}"


def _nodes(count: int) -> str:
    return f"{count:,} node" if count == 1 else f"{count:,} nodes"


def _last_sweep(info: AntiEntropyInfo, now_ms: int | None) -> str:
    if info.last_sweep_at is None:
        return "none yet"
    ago = format_since(_iso_ms(info.last_sweep_at), now_ms)
    return f"{ago}, {info.last_sweep_diverged:,} diverged"


def _gc(info: GcInfo, now_ms: int | None, retention: bool = True) -> str:
    rows = [
        ("Last pass", _last_pass(info.status, now_ms)),
        ("Next pass", _next_pass(info, now_ms)),
    ]
    if retention:
        rows.append(("Retention", f"{info.versions} versions per key"))
    return fields(rows)


def _last_pass(status: GcStatus, now_ms: int | None) -> str:
    if status.state in ("queued", "running"):
        return f"{status.state} ({status.job_id}), {status.scanned:,} versions scanned"
    if status.finished_at is None:
        return "none yet"
    ago = format_since(_iso_ms(status.finished_at), now_ms)
    if status.state == "failed":
        return f"{ago}, failed: {status.error or 'unknown error'}"
    if status.state == "cancelled":
        return f"{ago}, cancelled"
    verb = "collectable (dry run)" if status.dry_run else "collected"
    return f"{ago}, {status.collected:,} versions {verb}"


def _next_pass(info: GcInfo, now_ms: int | None) -> str:
    if not info.running:
        return "worker stopped"
    if info.interval <= 0:
        return "on demand only"
    finished = info.status.finished_at
    if finished is None:
        return f"within {format_duration(info.interval)}"
    due_ms = _iso_ms(finished) + int(info.interval * 1000)
    text = format_since(due_ms, now_ms)
    return "due now" if text.endswith("ago") else text


def _storage(info: StorageInfo) -> str:
    used = f"{format_size(info.used)} used ({info.used_ratio:.0%})"
    return fields(
        (
            ("Map size", f"{format_size(info.map_size)}, {used}"),
            (
                "Versions",
                f"{format_count(info.versions)} (retention {info.retention} per key)",
            ),
        )
    )


def format_count(count: int) -> str:
    """``912``, ``45.3 k``, ``1.2 M``: a count read at a glance."""
    for size, unit in ((1_000_000_000, "G"), (1_000_000, "M"), (1_000, "k")):
        if count >= size:
            return f"{count / size:.1f} {unit}"
    return str(count)


def _tls(info: TlsInfo, now_ms: int | None) -> str:
    return fields(
        (
            ("Certificate", _certificate(info, now_ms)),
            ("Rejected handshakes", _rejections(info)),
        )
    )


def _certificate(info: TlsInfo, now_ms: int | None) -> str:
    day = info.expires_at[:10]
    left = _seconds_left(info, now_ms)
    if left <= 0:
        return f"EXPIRED {_coarse(-left)} ago ({day})"
    return f"expires in {_coarse(left)} ({day})"


def _coarse(seconds: float) -> str:
    """Whole days past one day: a certificate date needs no hours."""
    if seconds >= 86400:
        return f"{int(seconds // 86400)}d"
    return format_duration(seconds)


def _seconds_left(info: TlsInfo, now_ms: int | None) -> float:
    now = time.time() if now_ms is None else now_ms / 1000
    return _iso_ms(info.expires_at) / 1000 - now


def _rejections(info: TlsInfo) -> str:
    if not info.rejected:
        return "none in the last hour"
    text = f"{info.rejected:,} in the last hour"
    if info.last_rejection is None:
        return text
    ago = ""
    if info.last_rejection_ago_s is not None:
        ago = f", {format_duration(info.last_rejection_ago_s)} ago"
    return f"{text} (last: {info.last_rejection}{ago})"


_GOAL_VERBS = {"join": "copied", "drain": "handed off"}


def _progress(progress: ProgressInfo) -> list[str]:
    if not progress.total:
        line = "first pass in progress"
    else:
        verb = _GOAL_VERBS.get(progress.goal, "done")
        line = f"{progress.done:,} of {progress.total:,} partitions {verb}"
    parts = [block("Progress", line)]
    if progress.waiting:
        rows = [(w.node_id, w.partitions, waiting_reason(w)) for w in progress.waiting]
        if progress.waiting_more:
            rows.append((f"... {progress.waiting_more:,} more", None, None))
        parts.append(table(("WAITING ON", "PARTITIONS", "REASON"), rows))
    return parts


def waiting_reason(item: WaitingOn) -> str:
    """Why a peer still holds up the goal, most blocking cause first."""
    if item.phase is None:
        return "not a member"
    if item.status != "ALIVE":
        since = "" if item.since_s is None else f" for {format_duration(item.since_s)}"
        return f"{item.status}{since}"
    error = f": {item.last_error}" if item.last_error else ""
    if item.retry_in_s > 0:
        return f"retry in {format_duration(item.retry_in_s)}{error}"
    if item.last_error:
        return f"failed{error}"
    return "in progress"


def _iso_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def status_messages(status: NodeStatus, now_ms: int | None = None) -> list[str]:
    lines = []
    phase_message = _PHASE_MESSAGES.get(status.phase)
    if phase_message:
        lines.append(phase_message)
    others = status.members.total - status.members.ready_alive
    if others:
        verb = "member is" if others == 1 else "members are"
        lines.append(
            f"{others:,} {verb} not READY and ALIVE; see: tourctl cluster status"
        )
    lines += _progress_messages(status)
    lines += _work_messages(status)
    lines += _resource_messages(status, now_ms)
    return lines or ["Nothing to report."]


def _progress_messages(status: NodeStatus) -> list[str]:
    progress = status.progress
    if progress is None:
        return []
    blocked = [w.node_id for w in progress.waiting if w.status != "ALIVE"]
    return [
        f"The {progress.goal} resumes when {node} is ALIVE again." for node in blocked
    ]


def _resource_messages(status: NodeStatus, now_ms: int | None) -> list[str]:
    lines = []
    storage = status.storage
    if storage is not None and storage.used_ratio >= STORAGE_WARNING:
        lines.append(
            f"Storage is {storage.used_ratio:.0%} full. Collected versions free "
            "space for reuse; the file does not shrink."
        )
    tls = status.tls
    if tls is not None:
        lines += _tls_messages(tls, now_ms)
    return lines


def _tls_messages(tls: TlsInfo, now_ms: int | None) -> list[str]:
    lines = []
    left = _seconds_left(tls, now_ms)
    if left <= 0:
        lines.append("The node certificate has EXPIRED; renew it and restart the node.")
    elif left < CERTIFICATE_WARNING_S:
        lines.append(f"The node certificate expires in {_coarse(left)}; renew it.")
    if tls.rejected:
        noun = "handshake was" if tls.rejected == 1 else "handshakes were"
        lines.append(
            f"{tls.rejected:,} TLS {noun} rejected in the last hour; "
            "check peer and client certificates."
        )
    return lines


def _work_messages(status: NodeStatus) -> list[str]:
    lines = []
    info = status.antientropy
    if info.pending_versions:
        lines.append(
            f"{info.pending_versions:,} versions wait for {_nodes(info.pending_targets)}; "
            f"see: tourctl ae status {status.address}"
        )
    if info.stuck:
        lines.append(
            f"Delivery to {_nodes(info.stuck)} is stuck although they are ALIVE; "
            f"see: tourctl ae status {status.address}"
        )
    if info.backlog and info.running:
        lines.append(
            "Obligations are backing up; delivery is slower than new work arrives."
        )
    if not info.running and status.phase != "IDLE":
        lines.append("The anti-entropy worker is stopped; nothing is delivered.")
    if not status.gc.running:
        lines.append("The GC worker is stopped; old versions are not collected.")
    if status.gc.status.state == "failed":
        lines.append(
            "The last GC pass failed; run it again with: "
            f"tourctl gc run {status.address}"
        )
    return lines
