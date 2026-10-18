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

from collections.abc import AsyncIterator, Callable
from typing import Any

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourillon.core.exceptions import ProcessError, TransferError
from tourillon.core.models.transfer import (
    TransferChunk,
    TransferClaim,
    TransferCommit,
    TransferInit,
    TransferReject,
)
from tourlib import Serializer
from tourlib.envelope import Envelope, replay_envelope

dispatcher = peer_dispatcher()

# Errors answered with a typed `<kind>.reject` instead of a transport error:
# a TransferError is retried by the peer, a ProcessError is fatal.
_REJECTED = (TransferError, ProcessError)


@dispatcher.on("transfer.init")
async def init(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        t_init = _decode(serializer, request, TransferInit.from_dict)
        accept = await manager.transfer.init(t_init)
        yield _reply(serializer, request, "transfer.init.accept", accept.to_dict())
    except _REJECTED as exc:
        yield _reject(serializer, request, exc)


@dispatcher.on("transfer.claim")
async def claim(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        t_claim = _decode(serializer, request, TransferClaim.from_dict)
        async for t_chunk in manager.transfer.claim(t_claim):
            yield _reply(serializer, request, "transfer.chunk", t_chunk.to_dict())
    except _REJECTED as exc:
        yield _reject(serializer, request, exc)


@dispatcher.on("transfer.chunk")
async def chunk(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        t_chunk = _decode(serializer, request, TransferChunk.from_dict)
        await manager.transfer.chunk(t_chunk)
        yield _reply(serializer, request, "transfer.chunk.accept", {"seq": t_chunk.seq})
    except _REJECTED as exc:
        yield _reject(serializer, request, exc)


@dispatcher.on("transfer.commit")
async def commit(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        t_commit = _decode(serializer, request, TransferCommit.from_dict)
        activated = await manager.transfer.commit(t_commit)
        yield _reply(
            serializer, request, "transfer.commit.accept", {"activated": activated}
        )
    except _REJECTED as exc:
        yield _reject(serializer, request, exc)


def _decode[T](
    serializer: Serializer,
    request: Envelope,
    parse: Callable[[dict[str, Any]], T],
) -> T:
    try:
        return parse(serializer.decode(request.payload))
    except (KeyError, TypeError, ValueError) as exc:
        raise ProcessError(f"malformed {request.kind} payload: {exc}") from exc


def _reply(
    serializer: Serializer,
    request: Envelope,
    kind: str,
    payload: dict[str, Any],
) -> Envelope:
    return replay_envelope(
        kind=kind, request=request, payload=serializer.encode(payload)
    )


def _reject(serializer: Serializer, request: Envelope, exc: Exception) -> Envelope:
    reject = TransferReject(reason=str(exc), fatal=isinstance(exc, ProcessError))
    return _reply(serializer, request, f"{request.kind}.reject", reject.to_dict())
