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

from tourillon.bootstrap.deps import (
    get_manager,
    peer_dispatcher,
)
from tourillon.core.models.gossip import GossipDigest, GossipPing, GossipPush
from tourlib.envelope import Envelope, replay_envelope

dispatcher = peer_dispatcher()


@dispatcher.on("gossip.push")
async def push(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    data: dict[str, Any] = serializer.decode(request.payload)
    response = await manager.gossip.accept(GossipPush.from_document(data))
    yield replay_envelope(
        kind="gossip.push.accept",
        payload=serializer.encode(response),
        request=request,
    )


@dispatcher.on("gossip.ping")
async def ping(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    data: dict[str, Any] = serializer.decode(request.payload)
    response = await manager.gossip.pong(GossipPing.from_document(data))
    yield replay_envelope(
        kind="gossip.pong",
        payload=serializer.encode(response),
        request=request,
    )


@dispatcher.on("gossip.digest")
async def digest(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    data: dict[str, Any] = serializer.decode(request.payload)
    digest_request = GossipDigest.from_document(data)

    async for response in manager.gossip.delta(digest_request):
        yield replay_envelope(
            kind="gossip.delta",
            payload=serializer.encode(response),
            request=request,
        )
