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

"""Peer handlers of the data anti-entropy protocol (``antientropy.*``).

Every request answers ``<kind>.accept`` or ``<kind>.reject`` with an
``AntiEntropyRejectError`` payload. A malformed payload is refused as
``invalid``; admission, view and replica checks are done by the responder.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourillon.core.antientropy.responder import AntiEntropyResponder
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.antientropy import (
    BucketsRequest,
    FetchRequest,
    KeysRequest,
    PushRequest,
    RangeRequest,
    RootsRequest,
)
from tourlib import Serializer
from tourlib.envelope import Envelope, replay_envelope

dispatcher = peer_dispatcher()

type _Serve = Callable[[AntiEntropyResponder, Any], Awaitable[dict[str, Any]]]


async def _roots(responder: AntiEntropyResponder, request: Any) -> dict[str, Any]:
    return (await responder.roots(request)).to_dict()


async def _buckets(responder: AntiEntropyResponder, request: Any) -> dict[str, Any]:
    return (await responder.buckets(request)).to_dict()


async def _range(responder: AntiEntropyResponder, request: Any) -> dict[str, Any]:
    return {"cell": (await responder.range(request)).to_bytes()}


async def _keys(responder: AntiEntropyResponder, request: Any) -> dict[str, Any]:
    return (await responder.keys(request)).to_dict()


async def _fetch(responder: AntiEntropyResponder, request: Any) -> dict[str, Any]:
    return (await responder.fetch(request)).to_dict()


async def _push(responder: AntiEntropyResponder, request: Any) -> dict[str, Any]:
    return {"applied": await responder.push(request)}


@dispatcher.on("antientropy.roots")
async def roots(request: Envelope) -> AsyncIterator[Envelope]:
    yield await _handle(request, RootsRequest.from_dict, _roots)


@dispatcher.on("antientropy.buckets")
async def buckets(request: Envelope) -> AsyncIterator[Envelope]:
    yield await _handle(request, BucketsRequest.from_dict, _buckets)


@dispatcher.on("antientropy.range")
async def range_digest(request: Envelope) -> AsyncIterator[Envelope]:
    yield await _handle(request, RangeRequest.from_dict, _range)


@dispatcher.on("antientropy.keys")
async def keys(request: Envelope) -> AsyncIterator[Envelope]:
    yield await _handle(request, KeysRequest.from_dict, _keys)


@dispatcher.on("antientropy.fetch")
async def fetch(request: Envelope) -> AsyncIterator[Envelope]:
    yield await _handle(request, FetchRequest.from_dict, _fetch)


@dispatcher.on("antientropy.push")
async def push(request: Envelope) -> AsyncIterator[Envelope]:
    yield await _handle(request, PushRequest.from_dict, _push)


async def _handle(
    request: Envelope, parse: Callable[[dict[str, Any]], Any], serve: _Serve
) -> Envelope:
    manager = get_manager()
    serializer = manager.serializer

    try:
        decoded = _decode(serializer, request, parse)
        payload = await serve(manager.antientropy.responder, decoded)
    except AntiEntropyRejectError as exc:
        return _reply(serializer, request, f"{request.kind}.reject", exc.to_dict())

    return _reply(serializer, request, f"{request.kind}.accept", payload)


def _decode(
    serializer: Serializer, request: Envelope, parse: Callable[[dict[str, Any]], Any]
) -> Any:
    try:
        data = serializer.decode(request.payload)

        if not isinstance(data, dict):
            raise TypeError("payload must be a mapping")

        return parse(data)
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise AntiEntropyRejectError(
            AntiEntropyRejectError.INVALID, f"malformed {request.kind} payload: {exc}"
        ) from exc


def _reply(
    serializer: Serializer, request: Envelope, kind: str, payload: dict[str, Any]
) -> Envelope:
    return replay_envelope(
        kind=kind, request=request, payload=serializer.encode(payload)
    )
