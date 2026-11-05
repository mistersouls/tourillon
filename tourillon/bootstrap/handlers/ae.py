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

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ValidationError
from tourlib.models.ae import AeStatusRequest, ObligationsRequest

dispatcher = peer_dispatcher()


async def _answer(
    request: Envelope,
    kind: str,
    parse: Callable[[Any], Any],
    query: Callable[[Any], Awaitable[Any]],
) -> Envelope:
    serializer = get_manager().serializer
    try:
        model = parse(serializer.decode(request.payload))
        result = await query(model)
    except (ValidationError, ValueError, TypeError) as exc:
        return replay_envelope(
            kind=f"{kind}.reject",
            request=request,
            payload=serializer.encode({"code": "invalid_request", "message": str(exc)}),
        )
    return replay_envelope(
        kind=f"{kind}.accept",
        request=request,
        payload=serializer.encode(result.to_document()),
    )


@dispatcher.on("ae.status")
async def status(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    yield await _answer(
        request, "ae.status", AeStatusRequest.from_document, manager.ae_status
    )


@dispatcher.on("ae.obligations")
async def obligations(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    yield await _answer(
        request,
        "ae.obligations",
        ObligationsRequest.from_document,
        manager.ae_obligations,
    )
