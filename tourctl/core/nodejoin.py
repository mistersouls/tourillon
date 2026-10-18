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

from pathlib import Path

from tourclient import client
from tourclient.core.services.node import NodeService
from tourclient.exception import UnknownServiceError
from tourctl.core.error import NODE_JOIN_ERROR_PROFILE, handle_error
from tourlib.models.node import JoinRequest, JoinResponse


async def _run_join(
    address: str,
    *,
    seeds: set[str] | None = None,
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
        effective_seeds = tuple(sorted(seeds)) if seeds else None
        return await node_service.join(JoinRequest(seeds=effective_seeds))


async def join(
    address: str,
    *,
    seeds: set[str] | None = None,
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    try:
        response = await _run_join(
            address, seeds=seeds, tourconf=tourconf, context_name=context_name
        )
    except Exception as exc:
        raise SystemExit(
            handle_error(exc, context_name, NODE_JOIN_ERROR_PROFILE)
        ) from None

    if not response.success:
        raise SystemExit(response.message)
    return response.message
