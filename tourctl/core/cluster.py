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
"""``tourctl cluster status``: the cluster as one node sees it through gossip.

One request returns a phase x status summary of every member and one page of
members. Nothing fans out to the members themselves, so the cost of a call,
and of each ``--watch`` refresh, does not grow with the cluster.
"""

import asyncio
import json
import ssl
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from tourclient import InspectService, client
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.error import (
    EXIT_INVALID,
    EXIT_NOT_FOUND,
    INSPECT_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourctl.core.render import (
    block,
    fields,
    format_duration,
    format_since,
    messages,
    table,
    to_json,
)
from tourlib.exceptions import ProtocolError, ResponseError, ValidationError
from tourlib.models.inspect import DEFAULT_LIMIT, MembersPage, MembersRequest
from tourlib.models.member import MemberDebt, MemberReport, MemberRequest

MAX_MESSAGES = 5
MIN_INTERVAL = 1.0


@dataclass(frozen=True)
class Target:
    """Where to ask, and the options to repeat in the next-page command."""

    address: str | None
    tourconf: str | Path | None
    context_name: str | None


def build_request(
    *,
    list_all: bool,
    phases: Iterable[str],
    statuses: Iterable[str],
    prefix: str | None,
    limit: int,
    cursor: str | None,
) -> MembersRequest:
    try:
        return MembersRequest(
            phases=tuple(phases),
            statuses=tuple(statuses),
            prefix=prefix,
            list_all=list_all,
            limit=limit,
            cursor=cursor,
        )
    except ValidationError as exc:
        raise CliError(str(exc), EXIT_INVALID, error="invalid_option") from None


def _cluster_error(exc: Exception, context_name: str | None) -> CliError:
    if isinstance(exc, RequestRejectedError) and exc.code == "invalid_request":
        return CliError(str(exc), EXIT_INVALID, error="invalid_request")
    return cli_error(exc, context_name, INSPECT_ERROR_PROFILE)


async def _fetch(
    service: InspectService, request: MembersRequest
) -> tuple[MembersPage, float]:
    started = time.perf_counter()
    page = await service.members(request)
    return page, (time.perf_counter() - started) * 1000


def _session(target: Target):
    return client(
        "inspect",
        endpoint="peer",
        bind=target.address,
        tourconf=target.tourconf,
        context_name=target.context_name,
    )


async def cluster_status(
    request: MembersRequest,
    target: Target,
    *,
    as_json: bool = False,
) -> str:
    try:
        async with _session(target) as service:
            if not isinstance(service, InspectService):
                raise UnknownServiceError("Unknown service: inspect")
            page, age_ms = await _fetch(service, request)
    except Exception as exc:
        raise _cluster_error(exc, target.context_name) from None

    if as_json:
        return to_json(page_document(page, age_ms))
    return render_page(page, age_ms, request, target)


async def cluster_member(
    node_id: str,
    target: Target,
    *,
    as_json: bool = False,
) -> str:
    try:
        request = MemberRequest(node_id=node_id)
        async with _session(target) as service:
            if not isinstance(service, InspectService):
                raise UnknownServiceError("Unknown service: inspect")
            report = await service.member(request)
    except (
        RequestRejectedError,
        UnknownServiceError,
        FileNotFoundError,
        KeyError,
        ValidationError,
        ValueError,
        ssl.SSLError,
        OSError,
        ProtocolError,
        ResponseError,
    ) as exc:
        if isinstance(exc, RequestRejectedError) and exc.code == "member_not_found":
            raise CliError(str(exc), EXIT_NOT_FOUND, error="member_not_found") from None
        if isinstance(exc, ValidationError):
            raise CliError(str(exc), EXIT_INVALID, error="invalid_option") from None
        raise _cluster_error(exc, target.context_name) from None

    if as_json:
        document = report.to_document()
        document["messages"] = member_messages(report)
        return to_json(document)
    return render_member(report)


async def watch_cluster_status(
    request: MembersRequest,
    target: Target,
    *,
    interval: float,
    as_json: bool = False,
) -> None:
    """Refresh the summary and the first page until interrupted (E10).

    Every refresh reuses one connection, so behind a load balancer the view
    keeps coming from the same node.
    """
    if request.cursor is not None:
        raise CliError("--watch shows the first page only.", EXIT_INVALID)
    if interval < MIN_INTERVAL:
        raise CliError("--interval must be at least 1 second.", EXIT_INVALID)

    try:
        async with _session(target) as service:
            if not isinstance(service, InspectService):
                raise UnknownServiceError("Unknown service: inspect")
            await _watch_loop(service, request, target, interval, as_json)
    except asyncio.CancelledError:
        return
    except Exception as exc:
        raise _cluster_error(exc, target.context_name) from None


async def _watch_loop(
    service: InspectService,
    request: MembersRequest,
    target: Target,
    interval: float,
    as_json: bool,
) -> None:
    while True:
        page, age_ms = await _fetch(service, request)
        if as_json:
            line = json.dumps(page_document(page, age_ms), sort_keys=True)
            print(line, flush=True)
        else:
            _redraw(render_page(page, age_ms, request, target), interval)
        await asyncio.sleep(interval)


def _redraw(text: str, interval: float) -> None:
    stamp = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    header = f"Every {interval:g}s: tourctl cluster status   {stamp}"
    if sys.stdout.isatty():
        sys.stdout.write("\x1b[H\x1b[2J")
    print(f"{header}\n\n{text}\n", flush=True)


def page_document(page: MembersPage, age_ms: float) -> dict:
    document = page.to_document()
    document["view_age_ms"] = round(age_ms, 3)
    document["messages"] = page_messages(page)
    return document


def render_page(
    page: MembersPage, age_ms: float, request: MembersRequest, target: Target
) -> str:
    parts = [
        f"View from: {page.node_id} (gossip, {_age(age_ms)} old)\n"
        f"Members:   {page.members:,}",
        _summary_table(page),
    ]
    listing = _listing(page, request, target)
    if listing:
        parts.append(listing)
    parts.append(messages(capped(page_messages(page))))
    return "\n\n".join(parts)


def _age(age_ms: float) -> str:
    return "<1ms" if age_ms < 1 else f"{round(age_ms)}ms"


def _summary_table(page: MembersPage) -> str:
    rows = [
        (row.phase, f"{row.alive:,}", f"{row.suspect:,}", f"{row.dead:,}")
        for row in page.summary
    ]
    return table(("PHASE", "ALIVE", "SUSPECT", "DEAD"), rows)


def _listing(page: MembersPage, request: MembersRequest, target: Target) -> str:
    if page.anomalies_only and page.total == 0:
        return ""
    title = "ANOMALIES" if page.anomalies_only else "MEMBERS"
    heading = f"{title} ({page.total:,})"
    if not page.items:
        return f"{heading}\nNo member matches the filters."
    rows = [
        (
            item.node_id,
            item.address,
            item.phase,
            item.status,
            _duration(item.last_heard_s),
            _duration(item.since_s),
        )
        for item in page.items
    ]
    headers = ("NODE", "ADDRESS", "PHASE", "STATUS", "LAST HEARD", "SINCE")
    lines = [heading, table(headers, rows)]
    footer = _footer(page, request, target)
    if footer:
        lines.append(footer)
    return "\n".join(lines)


def _duration(seconds: float | None) -> str | None:
    return None if seconds is None else format_duration(seconds)


def _footer(page: MembersPage, request: MembersRequest, target: Target) -> str:
    if page.cursor is None and request.cursor is None:
        return ""
    shown = f"Showing {len(page.items):,} of {page.total:,}."
    if page.cursor is None:
        return f"{shown} This is the last page."
    return f"{shown} Next page: {next_page_command(request, target, page.cursor)}"


def next_page_command(request: MembersRequest, target: Target, cursor: str) -> str:
    argv = ["tourctl", "cluster", "status"]
    if target.address:
        argv += ["-a", target.address]
    if target.context_name:
        argv += ["--context", target.context_name]
    if request.list_all:
        argv.append("--all")
    for phase in request.phases:
        argv += ["--phase", phase]
    for status in request.statuses:
        argv += ["--status", status]
    if request.prefix:
        argv += ["--prefix", request.prefix]
    if request.limit != DEFAULT_LIMIT:
        argv += ["--limit", str(request.limit)]
    argv += ["--cursor", cursor]
    return " ".join(argv)


def _plural(count: int, singular: str, plural: str) -> str:
    return f"{count:,} {singular if count == 1 else plural}"


def _member_next_try(debt: MemberDebt) -> str:
    if debt.phase not in ("JOINING", "READY", "DRAINING"):
        return "when READY"
    if debt.status != "ALIVE":
        return "when ALIVE"
    if debt.retry_in_s > 0:
        return f"in {format_duration(debt.retry_in_s)}"
    return "now"


def member_messages(report: MemberReport) -> list[str]:
    lines = []
    pending = sum(item.versions for item in report.owed_by)
    if pending:
        lines.append(
            f"{_plural(pending, 'version is', 'versions are')} delivered when "
            f"{report.node_id} is ALIVE."
        )
    else:
        lines.append(
            f"No pending anti-entropy obligation is known for {report.node_id}."
        )
    if report.unanswered:
        peers = ", ".join(
            f"{item.node_id} ({item.status})" for item in report.unanswered
        )
        lines.append(
            f"{_plural(len(report.unanswered), 'neighbour did', 'neighbours did')} "
            f"not answer within 2s: {peers}."
        )
    if report.status == "DEAD":
        lines.append(
            f"If {report.node_id} is lost for good: "
            f"tourctl node remove {report.node_id} --dry-run"
        )

    return lines


def render_member(report: MemberReport) -> str:
    status = report.status
    if report.since_s is not None:
        status += f" for {format_duration(report.since_s)}"

    parts = [
        fields(
            (
                ("Node", report.node_id),
                ("Address", report.address),
                ("Phase", f"{report.phase} (epoch {report.epoch})"),
                ("Status", f"{status} (view from {report.view_from})"),
                (
                    "Last heard",
                    (
                        "-"
                        if report.last_heard_s is None
                        else f"{format_duration(report.last_heard_s)} ago"
                    ),
                ),
                (
                    "Ring share",
                    f"{report.ring_share_percent:.4g}% "
                    f"({report.partitions:,} / {report.total_partitions:,} partitions; "
                    f"{report.vnodes:,} vnodes)",
                ),
            )
        ),
        _member_debts(report),
        _member_stand_ins(report),
        messages(member_messages(report)),
    ]
    return "\n\n".join(part for part in parts if part)


def _member_debts(report: MemberReport) -> str:
    title = (
        f"Owed to it (answered {report.answered:,} of "
        f"{report.neighbors:,} neighbours)"
    )
    if not report.owed_by:
        return f"{title}\nNone."
    rows = [
        (
            item.holder,
            f"{item.versions:,}",
            format_since(item.oldest_wall_ms),
            _member_next_try(item),
            item.last_error or ("stuck" if item.stuck else None),
        )
        for item in report.owed_by
    ]
    rows.append(
        (
            "Total",
            f"{sum(item.versions for item in report.owed_by):,}",
            None,
            None,
            None,
        )
    )

    return (
        title
        + "\n"
        + table(
            ("HOLDER", "VERSIONS", "OLDEST", "NEXT TRY", "LAST ERROR / STUCK"),
            rows,
        )
    )


def _member_stand_ins(report: MemberReport) -> str:
    if not report.stand_ins:
        return ""
    count = sum(item.partitions for item in report.stand_ins)
    owners = _human_list([item.node_id for item in report.stand_ins])
    return block(
        "Stand-ins",
        f"{count:,} partitions are served by {owners}.",
    )


def _human_list(items: list[str]) -> str:
    if len(items) == 1:
        return items[0]
    return f"{', '.join(items[:-1])} and {items[-1]}"


def page_messages(page: MembersPage) -> list[str]:
    """What is abnormal in the summary, and what happens next."""
    totals = {"DEAD": 0, "SUSPECT": 0}
    phases: dict[str, int] = {}
    for row in page.summary:
        totals["DEAD"] += row.dead
        totals["SUSPECT"] += row.suspect
        phases[row.phase] = phases.get(row.phase, 0) + row.total

    lines = [
        text
        for count, text in (
            (totals["DEAD"], "{} DEAD; their replicas are served by stand-ins."),
            (totals["SUSPECT"], "{} SUSPECT; requests fail over while it lasts."),
        )
        if count
        for text in [text.format(_plural(count, "node is", "nodes are"))]
    ]
    lines += _phase_messages(phases)
    return lines or ["All members are READY and ALIVE."]


_PHASE_MESSAGES = (
    ("JOINING", "{} JOINING; anti-entropy is filling their partitions."),
    ("DRAINING", "{} DRAINING; anti-entropy is moving their partitions away."),
    (
        "PAUSED",
        "{} PAUSED; reads and writes for their partitions go to stand-ins.",
    ),
    ("IDLE", "{} IDLE; they own no partitions."),
    ("FAILED", "{} FAILED; they own no partitions."),
)


def _phase_messages(phases: dict[str, int]) -> list[str]:
    return [
        text.format(_plural(phases[phase], "node is", "nodes are"))
        for phase, text in _PHASE_MESSAGES
        if phases.get(phase)
    ]


def capped(lines: list[str], limit: int = MAX_MESSAGES) -> list[str]:
    """At most ``limit`` messages; the rest is counted (E7)."""
    if len(lines) <= limit:
        return lines
    rest = len(lines) - limit
    return [*lines[:limit], f"... and {rest} more (use --json for the full list)"]
