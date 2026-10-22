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

from collections.abc import AsyncIterator
from typing import Any

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourillon.core.exceptions import DrainError
from tourlib.envelope import Envelope, replay_envelope
from tourlib.models.inspect import NodeInspectRequest
from tourlib.models.node import DrainRequest, JoinRequest

dispatcher = peer_dispatcher()


@dispatcher.on("node.inspect")
async def inspect(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    data: dict[str, Any] = serializer.decode(request.payload)
    inspect_request = NodeInspectRequest.from_document(data)
    async for info in manager.inspect(inspect_request):
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(info),
        )


@dispatcher.on("node.join")
async def join(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    data: dict[str, Any] = serializer.decode(request.payload)
    join_req = JoinRequest(**data)
    seeds = list(join_req.seeds) if join_req.seeds else []
    await manager.join(seeds)
    yield replay_envelope(
        kind="node.join.accept",
        request=request,
        payload=serializer.encode(
            {"message": "Join operation is successful triggered."}
        ),
    )


@dispatcher.on("node.drain")
async def drain(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    DrainRequest.from_document(serializer.decode(request.payload))
    try:
        await manager.drain()
    except DrainError as exc:
        yield replay_envelope(
            kind="node.drain.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.drain.accept",
        request=request,
        payload=serializer.encode(
            {"message": "Drain initiated; draining continues asynchronously."}
        ),
    )
