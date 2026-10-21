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
from tourillon.core.exceptions import StoreOperationError
from tourillon.core.models.storageschema import Key, PartitionAddress, Record
from tourlib.envelope import Envelope, replay_envelope

dispatcher = peer_dispatcher()


@dispatcher.on("replication.exists")
async def exists(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        key = Key.from_dict(data["key"])
        owned = await manager.store.exists(key)
        yield replay_envelope(
            kind="replication.exists.accept",
            request=request,
            payload=serializer.encode({"owned": owned}),
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind="replication.exists.reject",
            request=request,
            payload=serializer.encode(exc.to_dict()),
        )


@dispatcher.on("replication.read")
async def read(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        address = PartitionAddress.from_dict(data["addr"])
        record = await manager.store.read(address)
        payload = None if record is None else record.to_dict()
        yield replay_envelope(
            kind="replication.read.accept",
            request=request,
            payload=serializer.encode(payload),
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict()),
        )


@dispatcher.on("replication.lookup")
async def lookup(request: Envelope) -> AsyncIterator[Envelope]:
    """Answer whether this node holds one exact version of an address."""
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        key = Key.from_dict(data["key"])
        record = await manager.store.read_at(key)
        payload = None if record is None else record.to_dict()
        yield replay_envelope(
            kind="replication.lookup.accept",
            request=request,
            payload=serializer.encode(payload),
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict()),
        )


@dispatcher.on("replication.write")
async def write(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        record = Record.from_dict(data["record"])
        accepted = await manager.store.write(record)
        yield replay_envelope(
            kind="replication.write.accept",
            request=request,
            payload=serializer.encode(accepted.to_dict()),
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict()),
        )


@dispatcher.on("replication.hint")
async def hint(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        record = Record.from_dict(data["record"])
        accepted = await manager.store.hint(record, intent=data["intent"])
        yield replay_envelope(
            kind="replication.hint.accept",
            request=request,
            payload=serializer.encode(accepted.to_dict()),
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind="replication.hint.reject",
            request=request,
            payload=serializer.encode(exc.to_dict()),
        )
