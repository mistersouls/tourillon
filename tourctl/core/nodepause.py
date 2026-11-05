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
from tourctl.core.error import NODE_PAUSE_ERROR_PROFILE, cli_error
from tourlib.exceptions import EnvelopeError


async def pause(
    address: str,
    *,
    as_json: bool,
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    """Pause a node through its direct peer endpoint."""
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
            response = await node_service.pause()
    except (
        RequestRejectedError,
        OSError,
        KeyError,
        ValueError,
        EnvelopeError,
        UnknownServiceError,
    ) as exc:
        raise cli_error(exc, context_name, NODE_PAUSE_ERROR_PROFILE) from None

    if as_json:
        return json.dumps(
            {"message": response.message},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    return response.message
