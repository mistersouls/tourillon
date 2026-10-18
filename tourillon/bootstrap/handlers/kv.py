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

from tourillon.bootstrap.deps import client_dispatcher, get_manager
from tourillon.core.exceptions import StoreOperationError
from tourlib.envelope import Envelope, replay_envelope
from tourlib.models.kv import DeleteRequest, GetRequest, PutRequest, RepairRequest

dispatcher = client_dispatcher()


@dispatcher.on("kv.put")
async def put(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        put_req = PutRequest(
            key=data["key"],
            value=data["value"],
            namespace=data["namespace"],
            quorum_write=data.get("quorum_write"),
        )
        version = await manager.store.put(put_req)
        yield replay_envelope(
            kind="kv.put.accept",
            request=request,
            payload=serializer.encode(None if version is None else version.to_dict())
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict())
        )


@dispatcher.on("kv.get")
async def get(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        get_req = GetRequest(
            key=data["key"],
            namespace=data["namespace"],
            quorum_read=data.get("quorum_read"),
        )
        version = await manager.store.get(get_req)
        yield replay_envelope(
            kind="kv.get.accept",
            request=request,
            payload=serializer.encode(None if version is None else version.to_dict())
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict())
        )


@dispatcher.on("kv.delete")
async def delete(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        delete_req = DeleteRequest(
            key=data["key"],
            namespace=data["namespace"],
            quorum_write=data.get("quorum_write"),
        )
        version = await manager.store.delete(delete_req)
        yield replay_envelope(
            kind="kv.delete.accept",
            request=request,
            payload=serializer.encode(None if version is None else version.to_dict())
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict())
        )


@dispatcher.on("kv.repair")
async def repair(request: Envelope) -> AsyncIterator[Envelope]:
    """Resume a write that was persisted without reaching its quorum."""
    manager = get_manager()
    serializer = manager.serializer
    try:
        data: dict[str, Any] = serializer.decode(request.payload)
        repair_req = RepairRequest(
            token=data["token"],
            quorum_write=data.get("quorum_write"),
        )
        result = await manager.store.repair(repair_req)
        yield replay_envelope(
            kind="kv.repair.accept",
            request=request,
            payload=serializer.encode(result.to_dict()),
        )
    except StoreOperationError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict())
        )
