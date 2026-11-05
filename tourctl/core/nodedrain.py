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
from pathlib import Path

from tourclient import client
from tourclient.core.services.node import NodeService
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.error import (
    EXIT_INVALID,
    NODE_DRAIN_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourctl.core.nodefollow import follow_phase, render_follow
from tourlib.exceptions import EnvelopeError


async def drain(
    address: str,
    *,
    wait: bool = False,
    timeout: float = 600.0,
    cancel: bool = False,
    as_json: bool = False,
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    """Request draining or cancel it; optionally follow completion."""
    if wait and timeout <= 0:
        raise CliError(
            "Drain timeout must be greater than zero.",
            EXIT_INVALID,
            error="invalid_timeout",
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
            response = (
                await node_service.cancel_drain()
                if cancel
                else await node_service.drain()
            )
            status = None
            if wait:
                status = await follow_phase(
                    node_service,
                    expected_phase="READY" if cancel else "IDLE",
                    timeout=timeout,
                    address=address,
                    stream=not as_json,
                )
    except (
        RequestRejectedError,
        OSError,
        KeyError,
        ValueError,
        EnvelopeError,
        UnknownServiceError,
    ) as exc:
        raise cli_error(exc, context_name, NODE_DRAIN_ERROR_PROFILE) from None

    if status is not None:
        return render_follow(status, as_json=as_json)
    if as_json:
        return json.dumps(
            {"message": response.message},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    return response.message
