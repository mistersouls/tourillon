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
import json
import math
from pathlib import Path

from tourclient import client
from tourclient.core.services.node import NodeService
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.error import NODE_GC_ERROR_PROFILE, handle_error
from tourlib.exceptions import EnvelopeError, ResponseError
from tourlib.models.gc import GcRequest, GcResponse, GcStatus


def format_record_bytes(size: int) -> str:
    """Binary units for human output; JSON keeps the original integer."""
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{size} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable unit")


def render_status(status: GcStatus) -> list[str]:
    label = "Collectible" if status.dry_run else "Collected"
    return [
        f"Scope         {'all local partitions' if status.partition is None else f'partition={status.partition}'}",
        f"Job           {status.job_id or '-'}",
        f"State         {status.state}",
        f"Mode          {'estimate' if status.dry_run else 'collect'}",
        f"Scanned       {status.scanned:,} versions",
        f"{label:<14}{status.collected:,} versions",
        f"Record bytes  {format_record_bytes(status.record_bytes)}",
        f"Current group {status.current_group if status.current_group is not None else '-'}",
        f"Started       {status.started_at or '-'}",
        f"Finished      {status.finished_at or '-'}",
        f"Error         {status.error or '-'}",
    ]


async def _wait(
    node: NodeService, initial: GcResponse, timeout: float, address: str
) -> GcResponse:
    current = initial
    try:
        async with asyncio.timeout(timeout):
            while current.status.state in {"queued", "running"}:
                await asyncio.sleep(0.25)
                current = await node.gc_status(initial.status.job_id)
                if (
                    current.node_id != initial.node_id
                    or current.status.job_id != initial.status.job_id
                    or current.status.partition != initial.status.partition
                    or current.status.dry_run != initial.status.dry_run
                    or current.status.state == "idle"
                ):
                    raise ResponseError(
                        "GC polling returned a different node or job, or an idle pass"
                    )
    except TimeoutError:
        raise SystemExit(
            f"Timed out waiting for GC job {initial.status.job_id}; "
            f"the pass was not cancelled. Follow with: tourctl node inspect {address}"
        ) from None
    return current


def _render(result: GcResponse, address: str, as_json: bool) -> str:
    if result.status.state in {"failed", "cancelled"}:
        if as_json:
            raise SystemExit(
                json.dumps(
                    result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True
                )
            )
        raise SystemExit(
            f"GC {result.status.state} on {result.node_id}\n"
            + "\n".join(render_status(result.status))
        )
    if as_json:
        return json.dumps(
            result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True
        )
    state = (
        "submitted; completion is asynchronous"
        if result.status.state == "queued"
        else result.status.state
    )
    lines = [f"GC {state} on {result.node_id}", *render_status(result.status)]
    lines.append("Record bytes are logical; file size may not decrease.")
    if result.status.state in {"queued", "running"}:
        lines.append(f"Follow with: tourctl node inspect {address}")
    return "\n".join(lines)


async def collect(
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
    if not math.isfinite(timeout) or timeout <= 0:
        raise SystemExit("GC timeout must be positive and finite")
    try:
        request = GcRequest(dry_run=dry_run, partition=partition)
        async with client(
            "node",
            endpoint="peer",
            bind=address,
            tourconf=tourconf,
            context_name=context_name,
        ) as node:
            if not isinstance(node, NodeService):
                raise UnknownServiceError("Unknown service: node")
            result = await node.gc(request)
            if (
                result.status.dry_run != dry_run
                or result.status.partition != partition
                or result.status.state != "queued"
            ):
                raise ResponseError(
                    "GC acknowledgement does not match the submitted pass"
                )
            if wait:
                result = await _wait(node, result, timeout, address)
    except RequestRejectedError as exc:
        raise SystemExit(str(exc)) from None
    except (OSError, KeyError, ValueError, EnvelopeError, UnknownServiceError) as exc:
        raise SystemExit(
            handle_error(exc, context_name, NODE_GC_ERROR_PROFILE)
        ) from None
    return _render(result, address, as_json)
