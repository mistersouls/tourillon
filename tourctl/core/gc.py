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

"""``tourctl gc run`` and ``tourctl gc status``: local historical GC of one node."""

import asyncio
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from tourclient import client
from tourclient.core.services.gc import GcService
from tourclient.exception import UnknownServiceError
from tourctl.core.error import (
    EXIT_FAILED,
    EXIT_INVALID,
    EXIT_NOT_FOUND,
    EXIT_TIMEOUT,
    GC_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourctl.core.render import (
    fields,
    format_duration,
    format_since,
    format_size,
    messages,
    table,
    to_json,
)
from tourlib.exceptions import ResponseError
from tourlib.models.gc import GcJobs, GcRequest, GcResponse, GcStatus

RECENT_JOBS = 5
_ACTIVE = frozenset({"queued", "running"})
_NOTE = "Record bytes are logical; the data file does not shrink."


@asynccontextmanager
async def _service(
    address: str, tourconf: str | Path | None, context_name: str | None
) -> AsyncIterator[GcService]:
    async with client(
        "gc",
        endpoint="peer",
        bind=address,
        tourconf=tourconf,
        context_name=context_name,
    ) as service:
        if not isinstance(service, GcService):
            raise UnknownServiceError("Unknown service: gc")
        yield service


def short_id(job_id: str) -> str:
    return job_id[:8]


def _wall_ms(iso: str | None) -> int | None:
    if iso is None:
        return None
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def _elapsed(seconds: float) -> str:
    return f"{seconds:.1f}s" if seconds < 60 else format_duration(seconds)


def _duration(status: GcStatus, now_ms: int | None) -> str | None:
    started = _wall_ms(status.started_at)
    if started is None:
        return None
    end = _wall_ms(status.finished_at)
    if end is None:
        end = now_ms if now_ms is not None else int(datetime.now().timestamp() * 1000)
    return _elapsed(max(0, end - started) / 1000)


def _state(status: GcStatus, now_ms: int | None) -> str:
    duration = _duration(status, now_ms)
    if duration is None or status.state == "queued":
        return status.state
    link = {"completed": "in", "running": "for"}.get(status.state, "after")
    return f"{status.state} {link} {duration}"


def _mode(status: GcStatus) -> str:
    return "estimate (read-only)" if status.dry_run else "collect"


def _scope(status: GcStatus) -> str:
    if status.partition is None:
        return "all local partitions"
    return f"partition {status.partition}"


def _counts(status: GcStatus) -> str:
    label = "COLLECTIBLE" if status.dry_run else "COLLECTED"
    return table(
        ("SCANNED", label, "RECORD BYTES"),
        [
            (
                f"{status.scanned:,}",
                f"{status.collected:,}",
                format_size(status.record_bytes),
            )
        ],
    )


def _job_fields(
    node_id: str, status: GcStatus, now_ms: int | None
) -> list[tuple[str, str]]:
    pairs = [
        ("Node", node_id),
        ("Job", short_id(status.job_id)),
        ("Mode", _mode(status)),
        ("Scope", _scope(status)),
        ("State", _state(status, now_ms)),
    ]
    if status.state == "running" and status.current_group is not None:
        pairs.append(("Group", str(status.current_group)))
    if status.error:
        pairs.append(("Error", status.error))
    return pairs


def render_job(
    node_id: str, status: GcStatus, address: str, now_ms: int | None = None
) -> str:
    parts = [fields(_job_fields(node_id, status, now_ms))]
    if status.state != "queued":
        parts += [_counts(status), _NOTE]
    if status.state in _ACTIVE:
        parts.append(
            f"Follow with: tourctl gc status {address} --job {short_id(status.job_id)}"
        )
    return "\n\n".join(parts)


def _check_timeout(timeout: float) -> None:
    if not math.isfinite(timeout) or timeout <= 0:
        raise CliError(
            "--timeout must be positive and finite",
            EXIT_INVALID,
            error="invalid_input",
        )


def _check_ack(result: GcResponse, request: GcRequest) -> None:
    status = result.status
    if (
        status.dry_run != request.dry_run
        or status.partition != request.partition
        or status.state != "queued"
    ):
        raise ResponseError("gc.run acknowledgement does not match the submitted pass")


def _find(jobs: GcJobs, job_id: str) -> GcStatus:
    for job in jobs.jobs:
        if job.job_id == job_id:
            return job
    raise ResponseError(f"GC job {job_id} is no longer retained by the node")


async def _wait(
    service: GcService, initial: GcResponse, timeout: float, address: str
) -> GcResponse:
    job_id = initial.status.job_id
    current = initial.status
    try:
        async with asyncio.timeout(timeout):
            while current.state in _ACTIVE:
                await asyncio.sleep(0.25)
                jobs = await service.status()
                if jobs.node_id != initial.node_id:
                    raise ResponseError("GC status came from a different node")
                current = _find(jobs, job_id)
    except TimeoutError:
        raise CliError(
            f"Timed out waiting for GC job {short_id(job_id)}; "
            f"the pass was not cancelled.\n"
            f"Follow with: tourctl gc status {address} --job {short_id(job_id)}",
            EXIT_TIMEOUT,
            error="wait_timeout",
            details={"job_id": job_id},
        ) from None
    return GcResponse(initial.node_id, current)


def _finish_run(result: GcResponse, address: str, as_json: bool) -> str:
    if result.status.state in {"failed", "cancelled"}:
        raise CliError(
            render_job(result.node_id, result.status, address),
            EXIT_FAILED,
            error=f"gc_{result.status.state}",
            details=result.to_dict(),
            prefix=False,
        )
    if as_json:
        return to_json(result.to_dict())
    return render_job(result.node_id, result.status, address)


async def run_gc(
    address: str,
    *,
    dry_run: bool = False,
    partition: int | None = None,
    wait: bool = False,
    timeout: float = 60.0,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    """Submit one local pass; waiting never changes the worker's lifecycle."""
    _check_timeout(timeout)
    try:
        request = GcRequest(dry_run=dry_run, partition=partition)
        async with _service(address, tourconf, context_name) as service:
            result = await service.run(request)
            _check_ack(result, request)
            if wait:
                result = await _wait(service, result, timeout, address)
    except Exception as exc:
        raise cli_error(exc, context_name, GC_ERROR_PROFILE) from None
    return _finish_run(result, address, as_json)


def _select(jobs: GcJobs, prefix: str) -> GcStatus:
    matches = [job for job in jobs.jobs if job.job_id.startswith(prefix)]
    if not matches:
        raise CliError(
            f"No retained GC job starts with {prefix!r} on {jobs.node_id}",
            EXIT_NOT_FOUND,
            error="job_not_found",
        )
    if len(matches) > 1:
        ids = ", ".join(short_id(job.job_id) for job in matches)
        raise CliError(
            f"GC job prefix {prefix!r} is ambiguous: {ids}",
            EXIT_INVALID,
            error="ambiguous_job",
        )
    return matches[0]


def _interval(jobs: GcJobs, now_ms: int | None) -> str:
    text = f"every {format_duration(jobs.worker.interval)}"
    next_ms = _wall_ms(jobs.next_pass_at)
    if next_ms is None:
        return text
    return f"{text}, next {format_since(next_ms, now_ms)}"


def _header(jobs: GcJobs, now_ms: int | None) -> str:
    worker = jobs.worker
    return fields(
        (
            ("Node", jobs.node_id),
            ("Worker", "running" if worker.running else "stopped"),
            ("Retention", f"{worker.versions} versions per key"),
            ("Interval", _interval(jobs, now_ms)),
            ("Hot queue", f"{worker.hot_queue_size:,} partitions"),
        )
    )


def _job_row(job: GcStatus, now_ms: int | None) -> tuple:
    started = _wall_ms(job.started_at)
    return (
        short_id(job.job_id),
        "estimate" if job.dry_run else "collect",
        "all" if job.partition is None else str(job.partition),
        job.state,
        None if started is None else format_since(started, now_ms),
        _duration(job, now_ms),
        f"{job.scanned:,}",
        f"{job.collected:,}",
    )


def _jobs_table(shown: list[GcStatus], total: int, now_ms: int | None) -> str:
    if not shown:
        return "No GC pass has run since the node started."
    rows = table(
        (
            "JOB",
            "MODE",
            "SCOPE",
            "STATE",
            "STARTED",
            "DURATION",
            "SCANNED",
            "COLLECTED",
        ),
        [_job_row(job, now_ms) for job in shown],
    )
    footer = f"Showing {len(shown)} of {total}."
    if len(shown) < total:
        footer += " Use --all to list every job."
    return f"{rows}\n{footer}"


def status_messages(jobs: GcJobs, address: str) -> list[str]:
    lines = []
    if not jobs.worker.running:
        lines.append("The GC worker is stopped; old versions are not collected.")
    if jobs.jobs and jobs.jobs[0].state == "failed":
        lines.append(
            "The last GC pass failed; run it again with: " f"tourctl gc run {address}"
        )
    return lines


def render_jobs(
    jobs: GcJobs, address: str, *, show_all: bool = False, now_ms: int | None = None
) -> str:
    shown = list(jobs.jobs if show_all else jobs.jobs[:RECENT_JOBS])
    parts = [_header(jobs, now_ms), _jobs_table(shown, len(jobs.jobs), now_ms)]
    notes = status_messages(jobs, address)
    if notes:
        parts.append(messages(notes))
    return "\n\n".join(parts)


def _jobs_json(jobs: GcJobs, show_all: bool, address: str) -> str:
    document = jobs.to_dict()
    if not show_all:
        document["jobs"] = document["jobs"][:RECENT_JOBS]
    document["total"] = len(jobs.jobs)
    document["messages"] = status_messages(jobs, address)
    return to_json(document)


def _render_status(
    jobs: GcJobs,
    address: str,
    job: str | None,
    show_all: bool,
    as_json: bool,
    now_ms: int | None,
) -> str:
    if job is None:
        if as_json:
            return _jobs_json(jobs, show_all, address)
        return render_jobs(jobs, address, show_all=show_all, now_ms=now_ms)
    selected = _select(jobs, job)
    if as_json:
        return to_json({"node_id": jobs.node_id, "job": selected.to_dict()})
    return render_job(jobs.node_id, selected, address, now_ms)


async def gc_status(
    address: str,
    *,
    job: str | None = None,
    show_all: bool = False,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
    now_ms: int | None = None,
) -> str:
    """Show the GC worker of ADDRESS and its recent passes, or one pass."""
    if job is not None and not job:
        raise CliError("--job must not be empty", EXIT_INVALID, error="invalid_input")
    try:
        async with _service(address, tourconf, context_name) as service:
            jobs = await service.status()
    except Exception as exc:
        raise cli_error(exc, context_name, GC_ERROR_PROFILE) from None
    return _render_status(jobs, address, job, show_all, as_json, now_ms)
