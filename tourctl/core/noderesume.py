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
from pathlib import Path

from tourclient import client
from tourclient.core.services.node import NodeService
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.error import (
    EXIT_FAILED,
    EXIT_TIMEOUT,
    NODE_RESUME_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourlib.exceptions import EnvelopeError
from tourlib.models.inspect import NodeStatus

_POLL_INTERVAL = 1.0


async def resume(
    address: str,
    *,
    wait: bool,
    timeout: float,
    as_json: bool,
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    """Start node resume and optionally wait for READY on the same connection."""
    if timeout <= 0:
        raise CliError(
            "Resume timeout must be greater than zero.", error="invalid_timeout"
        )

    try:
        async with client(
            "node",
            endpoint="peer",
            bind=address,
            tourconf=tourconf,
            context_name=context_name,
        ) as node_service:
            if not isinstance(node_service, NodeService):
                raise UnknownServiceError("Unknown service: node")
            response = await node_service.resume()
            status = await _wait_until_ready(node_service, timeout) if wait else None
    except (
        RequestRejectedError,
        OSError,
        KeyError,
        ValueError,
        EnvelopeError,
        UnknownServiceError,
    ) as exc:
        raise cli_error(exc, context_name, NODE_RESUME_ERROR_PROFILE) from None

    if status is not None:
        return _render_status(status, as_json)
    if as_json:
        return json.dumps(
            {"message": response.message},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    return response.message


async def _wait_until_ready(node_service: NodeService, timeout: float) -> NodeStatus:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        status = await node_service.status()
        if status.phase == "READY":
            return status
        if status.phase == "FAILED":
            raise CliError(
                f"Node resume failed; node phase is {status.phase}.",
                EXIT_FAILED,
                error="resume_failed",
            )
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise CliError(
                f"Node resume timed out after {timeout:g}s; node remains "
                f"{status.phase}.",
                EXIT_TIMEOUT,
                error="resume_timeout",
                details={"phase": status.phase, "node_id": status.node_id},
            )
        await asyncio.sleep(min(_POLL_INTERVAL, remaining))


def _render_status(status: NodeStatus, as_json: bool) -> str:
    if as_json:
        return json.dumps(
            status.to_document(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )
    return f"node_id={status.node_id}\nphase={status.phase}"
