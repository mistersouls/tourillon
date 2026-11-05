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
import sys
from dataclasses import dataclass

from tourclient.core.services.node import NodeService
from tourctl.core.error import EXIT_FAILED, EXIT_TIMEOUT, CliError
from tourlib.models.inspect import NodeStatus, ProgressInfo

POLL_INTERVAL = 2.0


@dataclass(frozen=True)
class FollowResult:
    status: NodeStatus | None
    updates: tuple[str, ...]
    stopped: bool = False
    streamed: bool = False


async def follow_phase(
    node_service: NodeService,
    *,
    expected_phase: str,
    timeout: float,
    address: str,
    stream: bool = False,
) -> FollowResult:
    deadline = asyncio.get_running_loop().time() + timeout
    status: NodeStatus | None = None
    updates: list[str] = []
    previous: tuple[object, ...] | None = None

    try:
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise _timeout_error(status, timeout, address)

            try:
                async with asyncio.timeout(remaining):
                    status = await node_service.status()
            except TimeoutError:
                raise _timeout_error(status, timeout, address) from None

            if status.phase == expected_phase:
                return FollowResult(status, tuple(updates))

            previous = _record_progress(status, previous, updates, stream)

            if status.phase == "FAILED":
                raise CliError(
                    f"Node operation failed; node phase is {status.phase}.",
                    EXIT_FAILED,
                    error="node_operation_failed",
                    details={"node_id": status.node_id, "phase": status.phase},
                )

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise _timeout_error(status, timeout, address)

            await asyncio.sleep(min(POLL_INTERVAL, remaining))
    except asyncio.CancelledError:
        return FollowResult(status, tuple(updates), stopped=True, streamed=stream)


def render_follow(result: FollowResult, *, as_json: bool) -> str:
    if as_json:
        document = {} if result.status is None else result.status.to_document()
        document["updates"] = list(result.updates)
        document["stopped_following"] = result.stopped
        return json.dumps(
            document,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )

    lines = [] if result.streamed else list(result.updates)
    if result.stopped:
        if result.status is None:
            lines.append("Stopped following; the membership operation continues.")
            return "\n".join(lines)
        lines.append(
            f"Stopped following; {result.status.node_id} is still "
            f"{result.status.phase}."
        )
    else:
        assert result.status is not None
        lines.append(f"{result.status.node_id} is {result.status.phase}.")
    return "\n".join(lines)


def _write_line(line: str) -> None:
    sys.stdout.write(f"{line}\n")
    sys.stdout.flush()


def _timeout_error(status: NodeStatus | None, timeout: float, address: str) -> CliError:
    if status is None:
        return CliError(
            f"Timed out after {timeout:g}s while reading node status.\n"
            f"Follow with: tourctl node status {address}",
            EXIT_TIMEOUT,
            error="node_operation_timeout",
        )
    return CliError(
        f"Timed out after {timeout:g}s; {status.node_id} is still "
        f"{status.phase}.\nFollow with: tourctl node status {address}",
        EXIT_TIMEOUT,
        error="node_operation_timeout",
        details={"node_id": status.node_id, "phase": status.phase},
    )


def _progress_signature(status: NodeStatus) -> tuple[object, ...]:
    progress = status.progress
    if progress is None:
        return (status.phase, None)
    return (
        status.phase,
        progress.goal,
        progress.done,
        progress.total,
        tuple(
            (item.node_id, item.status, item.partitions) for item in progress.waiting
        ),
    )


def _record_progress(
    status: NodeStatus,
    previous: tuple[object, ...] | None,
    updates: list[str],
    stream: bool,
) -> tuple[object, ...]:
    signature = _progress_signature(status)
    if signature == previous:
        return signature

    line = _progress_line(status)
    updates.append(line)
    if stream:
        _write_line(line)
    return signature


def _progress_line(status: NodeStatus) -> str:
    progress = status.progress
    if progress is None or not progress.total:
        return f"{status.node_id}: {status.phase}"
    return _format_progress(status.node_id, status.phase, progress)


def _format_progress(node_id: str, phase: str, progress: ProgressInfo) -> str:
    action = "partitions synced" if progress.goal == "join" else "partitions handed off"
    line = f"{node_id}: {phase}, {progress.done:,} of {progress.total:,} {action}"
    if progress.waiting:
        peer = progress.waiting[0]
        line += (
            f", waiting on {peer.node_id} ({peer.status}) for "
            f"{peer.partitions:,} partitions"
        )
    return line
