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

"""``tourctl ae status`` and ``tourctl ae obligations``: delivery debt of one node."""

import math
import shlex
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from tourclient import client
from tourclient.core.services.ae import AeService
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.cluster import capped
from tourctl.core.error import AE_ERROR_PROFILE, EXIT_INVALID, CliError, cli_error
from tourctl.core.kvvalue import display, json_safe
from tourctl.core.render import (
    fields,
    format_duration,
    format_since,
    format_wall,
    messages,
    table,
    to_json,
)
from tourlib.adapter import MsgPackSerializer
from tourlib.models.ae import (
    OBLIGATIONS_DEFAULT_LIMIT,
    SCAN_LIMIT,
    STATUS_DEFAULT_LIMIT,
    AeStatus,
    AeStatusRequest,
    AeTarget,
    ObligationsPage,
    ObligationsRequest,
    ObligationView,
)

_WIRE = MsgPackSerializer()
_DELIVERY_PHASES = frozenset({"JOINING", "READY", "DRAINING"})
_NONE_PENDING = "No replication obligations are pending."
ERROR_WIDTH = 60


@asynccontextmanager
async def _service(
    address: str, tourconf: str | Path | None, context_name: str | None
) -> AsyncIterator[AeService]:
    async with client(
        "ae",
        endpoint="peer",
        bind=address,
        tourconf=tourconf,
        context_name=context_name,
    ) as service:
        if not isinstance(service, AeService):
            raise UnknownServiceError("Unknown service: ae")
        yield service


def _invalid(exc: Exception) -> CliError:
    return CliError(str(exc), EXIT_INVALID, error="invalid_input")


def _ae_error(exc: Exception, context_name: str | None) -> CliError:
    if isinstance(exc, RequestRejectedError) and exc.code == "invalid_request":
        return CliError(str(exc), EXIT_INVALID, error="invalid_request")
    return cli_error(exc, context_name, AE_ERROR_PROFILE)


def _next_page(
    verb: str,
    address: str,
    options: list[str],
    context_name: str | None,
    cursor: str,
) -> str:
    argv = ["tourctl", "ae", verb, address, *options]
    if context_name:
        argv += ["--context", context_name]
    argv += ["--cursor", cursor]
    return " ".join(shlex.quote(arg) for arg in argv)


def _footer(shown: int, total: str, cursor: str | None, command: str) -> str:
    text = f"Showing {shown:,} of {total}."
    if cursor is not None:
        text += f" Next page: {command}"
    return text


# ---------------------------------------------------------------- ae status


def next_try(item: AeTarget) -> str:
    if item.status != "ALIVE":
        return "when ALIVE"
    if item.phase is None:
        return "not a member"
    if item.phase not in _DELIVERY_PHASES:
        return "when READY"
    if item.retry_in_s > 0:
        return f"in {format_duration(math.ceil(item.retry_in_s))}"
    return "now"


def short_error(text: str | None) -> str | None:
    """One table cell; ``--json`` keeps the full text."""
    if text is None or len(text) <= ERROR_WIDTH:
        return text
    return text[: ERROR_WIDTH - 3] + "..."


def _age(snapshot_ms: int, wall_ms: int) -> str:
    return format_duration(max(0, snapshot_ms - wall_ms) / 1000)


def _status_header(status: AeStatus) -> str:
    since = None
    if status.started_at_ms is not None:
        since = format_since(status.started_at_ms, status.snapshot_at_ms)
    return fields(
        (
            ("Node", status.node_id),
            ("Phase", status.phase),
            ("Worker", "running" if status.running else "stopped"),
            ("Since", since or "-"),
        )
    )


def _counters(status: AeStatus) -> str:
    c = status.counters
    return table(
        ("AREA", "COMPLETED", "DIVERGED", "IN", "OUT", "ERRORS"),
        (
            (
                "Delivery",
                f"{c.delivered:,}",
                None,
                None,
                None,
                f"{c.delivery_errors:,}",
            ),
            (
                "Sweep",
                f"{c.sweeps:,}",
                f"{c.diverged:,}",
                f"{c.pulled:,}",
                f"{c.pushed:,}",
                f"{c.sweep_errors:,}",
            ),
        ),
    )


def _plural(count: int, singular: str, plural: str) -> str:
    return f"{count:,} {singular if count == 1 else plural}"


def _total(count: int, truncated: bool) -> str:
    return f"{count:,}+" if truncated else f"{count:,}"


def _targets(status: AeStatus, footer_command: Callable[[str], str]) -> str:
    if not status.targets:
        return _NONE_PENDING
    rows = table(
        ("PENDING TO", "VERSIONS", "OLDEST", "NEXT TRY", "LAST ERROR"),
        [
            (
                item.target,
                f"{item.versions:,}",
                _age(status.snapshot_at_ms, item.oldest_wall_ms),
                next_try(item),
                short_error(item.last_error),
            )
            for item in status.items
        ],
    )
    footer = _footer(
        len(status.items),
        f"{status.targets:,}",
        status.cursor,
        footer_command(status.cursor) if status.cursor else "",
    )
    return f"{rows}\n{footer}"


def status_messages(status: AeStatus) -> list[str]:
    """What looks wrong, most specific first; per-target lines are capped."""
    stuck = [
        f"{item.target} is ALIVE and nothing defers it, yet "
        f"{_plural(item.versions, 'version is', 'versions are')} pending after "
        f"{_age(status.snapshot_at_ms, item.oldest_wall_ms)}."
        for item in status.items
        if item.stuck
    ]
    hidden = status.stuck - len(stuck)
    if hidden > 0:
        stuck.append(
            f"{_plural(hidden, 'more target looks', 'more targets look')} "
            "stuck beyond this page."
        )
    lines = capped(stuck)
    if status.stuck:
        lines.append("Delivery may be stuck; check this node's logs for ae.delivery.")
    if not status.running:
        lines.append(
            "The anti-entropy worker is stopped; obligations are not delivered."
        )
    if status.truncated:
        lines.append(
            f"Counting stopped at {SCAN_LIMIT:,} obligations; totals are lower bounds."
        )
    return lines


def render_status(
    status: AeStatus,
    request: AeStatusRequest,
    address: str,
    context_name: str | None = None,
) -> str:
    options = []
    if request.limit != STATUS_DEFAULT_LIMIT:
        options += ["--limit", str(request.limit)]

    def command(cursor: str) -> str:
        return _next_page("status", address, options, context_name, cursor)

    parts = [
        _status_header(status),
        _counters(status),
        _targets(status, command),
    ]
    notes = status_messages(status)
    if notes:
        parts.append(messages(notes))
    return "\n\n".join(parts)


async def ae_status(
    address: str,
    *,
    limit: int = STATUS_DEFAULT_LIMIT,
    cursor: str | None = None,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    """Show the anti-entropy worker of ADDRESS and the targets it owes data."""
    try:
        request = AeStatusRequest(limit=limit, cursor=cursor)
    except ValueError as exc:
        raise _invalid(exc) from None
    try:
        async with _service(address, tourconf, context_name) as service:
            status = await service.status(request)
    except Exception as exc:
        raise _ae_error(exc, context_name) from None
    if as_json:
        document = status.to_document()
        document["messages"] = status_messages(status)
        return to_json(document)
    return render_status(status, request, address, context_name)


# ----------------------------------------------------------- ae obligations


def _decoded(raw: bytes) -> Any:
    try:
        return _WIRE.decode(raw)
    except Exception:
        return raw


def key_text(item: ObligationView) -> str:
    return f"{display(_decoded(item.namespace))}/{display(_decoded(item.key))}"


def version_text(item: ObligationView) -> str:
    return f"{format_wall(item.wall_ms)} c={item.counter} {item.node_id}"


def _obligation_json(item: ObligationView) -> dict[str, Any]:
    return {
        "target": item.target,
        "partition": item.partition,
        "namespace": json_safe(_decoded(item.namespace)),
        "key": json_safe(_decoded(item.key)),
        "version": {
            "wall": format_wall(item.wall_ms),
            "wall_ms": item.wall_ms,
            "counter": item.counter,
            "node_id": item.node_id,
        },
    }


def obligations_document(page: ObligationsPage) -> dict[str, Any]:
    return {
        "node_id": page.node_id,
        "snapshot_at_ms": page.snapshot_at_ms,
        "target": page.target,
        "total": page.total,
        "truncated": page.truncated,
        "cursor": page.cursor,
        "items": [_obligation_json(item) for item in page.items],
    }


def _obligations_header(page: ObligationsPage) -> str:
    pairs = [("Node", page.node_id)]
    if page.target is not None:
        pairs.append(("Target", page.target))
    pairs.append(("Pending", _total(page.total, page.truncated)))
    return fields(pairs)


def render_obligations(
    page: ObligationsPage,
    request: ObligationsRequest,
    address: str,
    context_name: str | None = None,
) -> str:
    header = _obligations_header(page)
    if not page.items:
        if page.total:
            return f"{header}\n\nNo obligation after this cursor."
        suffix = f" to {page.target}." if page.target else "."
        return f"{header}\n\n{_NONE_PENDING[:-1]}{suffix}"
    rows = table(
        ("TARGET", "PARTITION", "KEY", "VERSION"),
        [
            (item.target, str(item.partition), key_text(item), version_text(item))
            for item in page.items
        ],
    )
    options = []
    if request.target is not None:
        options += ["--target", request.target]
    if request.limit != OBLIGATIONS_DEFAULT_LIMIT:
        options += ["--limit", str(request.limit)]
    command = ""
    if page.cursor is not None:
        command = _next_page("obligations", address, options, context_name, page.cursor)
    footer = _footer(
        len(page.items), _total(page.total, page.truncated), page.cursor, command
    )
    return f"{header}\n\n{rows}\n{footer}"


async def ae_obligations(
    address: str,
    *,
    target: str | None = None,
    limit: int = OBLIGATIONS_DEFAULT_LIMIT,
    cursor: str | None = None,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    """List the replication obligations persisted on ADDRESS, oldest group first."""
    try:
        request = ObligationsRequest(target=target, limit=limit, cursor=cursor)
    except ValueError as exc:
        raise _invalid(exc) from None
    try:
        async with _service(address, tourconf, context_name) as service:
            page = await service.obligations(request)
    except Exception as exc:
        raise _ae_error(exc, context_name) from None
    if as_json:
        return to_json(obligations_document(page))
    return render_obligations(page, request, address, context_name)
