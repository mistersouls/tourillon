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
    NODE_JOIN_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourctl.core.nodefollow import follow_phase, render_follow
from tourlib.exceptions import EnvelopeError
from tourlib.models.node import JoinRequest, JoinResponse


async def _run_join(
    address: str,
    *,
    seeds: set[str] | None = None,
    wait: bool = False,
    timeout: float = 600.0,
    cancel: bool = False,
    as_json: bool = False,
    tourconf: str | Path | None,
    context_name: str | None,
) -> JoinResponse:
    async with client(
        "node",
        endpoint="peer",
        bind=address,
        tourconf=tourconf,
        context_name=context_name,
    ) as node_service:
        if not isinstance(node_service, NodeService):
            raise UnknownServiceError("Unknown service: node")

        if cancel:
            response = await node_service.cancel_join()
        else:
            effective_seeds = tuple(sorted(seeds)) if seeds else None
            response = await node_service.join(JoinRequest(seeds=effective_seeds))

        if not wait:
            return JoinResponse(success=True, message=response.message)

        follow = await follow_phase(
            node_service,
            expected_phase="IDLE" if cancel else "READY",
            timeout=timeout,
            address=address,
            stream=not as_json,
        )
        return JoinResponse(
            success=True,
            message=render_follow(follow, as_json=as_json),
        )


async def join(
    address: str,
    *,
    seeds: set[str] | None = None,
    wait: bool = False,
    timeout: float = 600.0,
    cancel: bool = False,
    as_json: bool = False,
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    if cancel and seeds is not None:
        raise CliError(
            "--seeds has no effect with --cancel.",
            EXIT_INVALID,
            error="invalid_options",
        )
    if wait and timeout <= 0:
        raise CliError(
            "Join timeout must be greater than zero.",
            2,
            error="invalid_timeout",
        )

    try:
        response = await _run_join(
            address,
            seeds=seeds,
            wait=wait,
            timeout=timeout,
            cancel=cancel,
            as_json=as_json,
            tourconf=tourconf,
            context_name=context_name,
        )
    except (
        RequestRejectedError,
        OSError,
        KeyError,
        ValueError,
        EnvelopeError,
        UnknownServiceError,
    ) as exc:
        raise cli_error(exc, context_name, NODE_JOIN_ERROR_PROFILE) from None

    if not response.success:
        raise CliError(response.message, error="join_refused")
    if as_json and not wait:
        return json.dumps(
            {"message": response.message},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    return response.message
