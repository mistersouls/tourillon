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

import json
from dataclasses import asdict
from pathlib import Path
from typing import NoReturn

from tourclient import InspectService, client
from tourclient.exception import UnknownServiceError
from tourctl.core.error import INSPECT_ERROR_PROFILE, handle_error
from tourlib.models.inspect import MemberInspect, NodeInfo, NodeInspectRequest


def _raise_user_error(message: str) -> NoReturn:
    raise SystemExit(message)


def _inspect_error_message(exc: Exception, context_name: str | None) -> str:
    if isinstance(exc, UnknownServiceError):
        return str(exc)
    return handle_error(exc, context_name, INSPECT_ERROR_PROFILE)


async def _run_node_inspect(
    address: str,
    *,
    request: NodeInspectRequest,
    tourconf: str | Path | None,
    context_name: str | None,
) -> list[NodeInfo]:
    async with client(
        "inspect",
        endpoint="peer",
        bind=address,
        tourconf=tourconf,
        context_name=context_name,
    ) as inspect_service:
        if not isinstance(inspect_service, InspectService):
            raise UnknownServiceError("Unknown service: inspect")
        return await inspect_service.inspect(request)


async def inspect_node(
    address: str,
    *,
    as_json: bool = False,
    phases: tuple[str, ...] = (),
    probe_statuses: tuple[str, ...] = (),
    node_id_prefix: str | None = None,
    only_anomalies: bool = False,
    limit: int = 500,
    cursor: str | None = None,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    """Run node.inspect through tourclient and render either JSON or human-readable text."""
    request = NodeInspectRequest(
        phases=phases,
        probe_statuses=probe_statuses,
        node_id_prefix=node_id_prefix,
        only_anomalies=only_anomalies,
        limit=limit,
        cursor=cursor,
    )
    try:
        chunks = await _run_node_inspect(
            address=address,
            request=request,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        _raise_user_error(_inspect_error_message(exc, context_name))

    if as_json:
        return json.dumps(
            [asdict(chunk) for chunk in chunks],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    return _render_text(address, chunks)


def _render_text(address: str, chunks: list[NodeInfo]) -> str:
    if not chunks:
        return f"Inspecting {address} ...\nNo data received from node.inspect."

    first = chunks[0]
    node_id = first.node_id
    all_members = _merge_members(chunks)
    phase = first.self_member.phase
    epoch = str(first.self_member.epoch)
    ring = first.ring_position
    gossip = first.gossip_stats
    gossip_state = _gossip_state(gossip)

    lines = [
        f"Inspecting {address} ({node_id}) ...",
        f"Snapshot at: {first.snapshot_at}  (source: live RPC)",
        "",
        "  Status",
        f"    Summary:      phase={phase}, gossip={gossip_state}",
        "",
        "  Identity",
        f"    Node ID:      {node_id}",
        f"    Phase:        {phase}",
        f"    Epoch:        {epoch}",
        "",
        f"  Ring position  (partition_shift={ring.partition_shift}, {ring.total_partitions:,} total partitions)",
        f"    Owned partitions:  {ring.owned_partitions:,}  ({ring.owned_percent:.1f} % of total)",
        f"    Vnode ranges:      {len(ring.vnode_ranges)}",
    ]

    for vnode_range in ring.vnode_ranges:
        pid_label = _format_pid_label(vnode_range.arc, ring.total_partitions)
        lines.append(
            "      token "
            f"{_short_token(vnode_range.token):<14} ->  "
            f"pids {pid_label:<16} "
            f"({vnode_range.partitions} partitions)"
        )

    lines.extend(
        [
            "",
            f"  Cluster membership  (as seen by {node_id})",
            f"    Members:  {len(all_members)} total",
        ]
    )

    for member in sorted(all_members.values(), key=lambda item: item.node_id):
        marker = "  <- this node" if member.node_id == node_id else ""
        lines.append(
            f"      {member.node_id:<8} {member.phase:<8} {member.status:<8} "
            f"epoch={member.epoch}{marker}"
        )

    lines.extend(
        [
            "",
            "  Gossip health  (local engine)",
            f"    Known members:      {_display(gossip.get('known_members'))}",
            f"    AE cycles total:    {_display(gossip.get('ae_cycles_total'))}",
            f"    AE diverged:        {_display(gossip.get('ae_diverged'))}",
            f"    Last AE peer:       {_display(gossip.get('last_ae_peer'))}",
            f"    Last AE at:         {_display(gossip.get('last_ae_at'))}",
            f"    Bootstrap:          ok={_display(gossip.get('bootstrap_ok'))} err={_display(gossip.get('bootstrap_err'))}",
            f"    Last hot failure:   {_display(gossip.get('last_hot_failure'))}",
            f"    Last ae failure:    {_display(gossip.get('last_ae_failure'))}",
            f"    Last stop failure:  {_display(gossip.get('last_stop_failure'))}",
            "",
            "  Notes",
            f"    - View is local to {node_id} and may lag briefly behind cluster-wide state.",
        ]
    )
    return "\n".join(lines)


def _merge_members(chunks: list[NodeInfo]) -> dict[str, MemberInspect]:
    members: dict[str, MemberInspect] = {}
    for chunk in chunks:
        for member in chunk.members:
            members[member.node_id] = member
    return members


def _display(value: object | None) -> str:
    return "-" if value is None else str(value)


def _short_token(token: str) -> str:
    raw = token
    return raw if len(raw) <= 12 else f"{raw[:10]}..."


def _format_pid_label(arc: tuple[int, int] | None, total_partitions: int) -> str:
    if arc is None:
        return "-"

    start, end = arc
    if total_partitions <= 0:
        return f"{start}-{end}"

    pid_start = (start + 1) % total_partitions
    suffix = "(w)" if start > end else ""
    return f"{pid_start}-{end}{suffix}"


def _gossip_state(stats: dict[str, object]) -> str:
    diverged = stats.get("ae_diverged")
    hot_failure = stats.get("last_hot_failure")
    ae_failure = stats.get("last_ae_failure")
    if hot_failure or ae_failure:
        return "degraded"
    if isinstance(diverged, int) and diverged > 0:
        return "active"
    return "stable"
