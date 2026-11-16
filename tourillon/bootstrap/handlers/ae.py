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

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.ae import BucketsRequest, KeysRequest, RootsRequest, RangeRequest, FetchRequest
from tourlib.envelope import Envelope
from tourlib.handle import DictReplier, MalformedError

dispatcher = peer_dispatcher()


@dispatcher.on("ae.roots")
async def roots(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    replier = DictReplier(manager.serializer, request)

    try:
        response = await manager.ae.roots(replier.decode(RootsRequest))
        yield replier.reply("ae.roots.accept", response)
    except MalformedError:
        ...
    except AntiEntropyRejectError as exc:
        yield replier.reply("ae.roots.reject", exc)


@dispatcher.on("ae.buckets")
async def buckets(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    replier = DictReplier(manager.serializer, request)

    try:
        response = await manager.ae.buckets(replier.decode(BucketsRequest))
        yield replier.reply("ae.buckets.accept", response)
    except MalformedError:
        ...
    except AntiEntropyRejectError as exc:
        yield replier.reply("ae.buckets.reject", exc)


@dispatcher.on("ae.range")
async def range_(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    replier = DictReplier(manager.serializer, request)

    try:
        response = await manager.ae.range(replier.decode(RangeRequest))
        yield replier.reply("ae.range.accept", response)
    except MalformedError:
        ...
    except AntiEntropyRejectError as exc:
        yield replier.reply("ae.range.reject", exc)


@dispatcher.on("ae.keys")
async def keys(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    replier = DictReplier(manager.serializer, request)

    try:
        response = await manager.ae.keys(replier.decode(KeysRequest))
        yield replier.reply("ae.keys.accept", response)
    except MalformedError:
        ...
    except AntiEntropyRejectError as exc:
        yield replier.reply("ae.keys.reject", exc)


@dispatcher.on("ae.fetch")
async def fetch(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    replier = DictReplier(manager.serializer, request)

    try:
        response = await manager.ae.fetch(replier.decode(FetchRequest))
        yield replier.reply("ae.fetch.accept", response)
    except MalformedError:
        ...
    except AntiEntropyRejectError as exc:
        yield replier.reply("ae.fetch.reject", exc)
