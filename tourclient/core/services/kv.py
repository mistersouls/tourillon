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

from collections.abc import Mapping
from typing import Any

from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourlib import Serializer
from tourlib.exceptions import ResponseError
from tourlib.models.kv import (
    DeleteRequest,
    GetRequest,
    PutRequest,
    ReadOutcome,
    RepairRequest,
    WriteOutcome,
)


class KvService:
    """Application service for key/value operations."""

    def __init__(
        self,
        transport: Transport,
        serializer: Serializer,
    ) -> None:
        self._transport = transport
        self._serializer = serializer

    async def put(
        self,
        *,
        namespace: Any,
        key: Any,
        value: Any,
        quorum_write: int = 1,
    ) -> WriteOutcome:
        request = PutRequest(
            namespace=self._serializer.encode(namespace),
            key=self._serializer.encode(key),
            value=self._serializer.encode(value),
            quorum_write=quorum_write,
        )
        document = await self._request_document("kv.put", request)
        return self._parse(WriteOutcome, document, "kv.put")

    async def get(
        self,
        *,
        namespace: Any,
        key: Any,
        quorum_read: int = 1,
    ) -> ReadOutcome:
        request = GetRequest(
            namespace=self._serializer.encode(namespace),
            key=self._serializer.encode(key),
            quorum_read=quorum_read,
        )
        document = await self._request_document("kv.get", request)
        return self._parse(ReadOutcome, document, "kv.get")

    async def delete(
        self,
        *,
        namespace: Any,
        key: Any,
        quorum_write: int = 1,
    ) -> WriteOutcome:
        request = DeleteRequest(
            namespace=self._serializer.encode(namespace),
            key=self._serializer.encode(key),
            quorum_write=quorum_write,
        )
        document = await self._request_document("kv.delete", request)
        return self._parse(WriteOutcome, document, "kv.delete")

    async def repair(
        self,
        *,
        token: str,
        quorum_write: int = 1,
    ) -> Mapping[str, Any]:
        """Resume a write that was persisted without reaching its quorum.

        `token` is the handle carried by the quorum failure. It designates the
        version already written, so repair copies it to the replicas that lack
        it instead of writing a new one — retrying is therefore safe.
        """
        request = RepairRequest(token=token, quorum_write=quorum_write)
        response = await self._transport.request(kind="kv.repair", document=request)
        if response.kind == "kv.repair.reject":
            raise self._rejected("kv.repair", response.payload)
        if response.kind != "kv.repair.accept":
            raise ResponseError(f"unexpected kv.repair response kind: {response.kind}")
        document = self._serializer.decode(response.payload)
        if not isinstance(document, Mapping):
            raise ResponseError("invalid kv.repair response payload")
        return document

    async def _request_document(
        self,
        request_kind: str,
        request: PutRequest | DeleteRequest | GetRequest,
    ) -> Any:
        response = await self._transport.request(kind=request_kind, document=request)
        if response.kind == f"{request_kind}.reject":
            raise self._rejected(request_kind, response.payload)
        if response.kind != f"{request_kind}.accept":
            raise ResponseError(
                f"unexpected {request_kind} response kind: {response.kind}"
            )
        return self._serializer.decode(response.payload)

    def _rejected(self, request_kind: str, payload: bytes) -> RequestRejectedError:
        try:
            document = self._serializer.decode(payload)
        except ValueError:
            document = None
        details = document if isinstance(document, Mapping) else {}
        return RequestRejectedError(request_kind, details)

    @staticmethod
    def _parse[T: (WriteOutcome, ReadOutcome)](
        model: type[T], document: Any, request_kind: str
    ) -> T:
        if not isinstance(document, Mapping):
            raise ResponseError(f"invalid {request_kind} response payload")
        try:
            return model.from_dict(document)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError(f"invalid {request_kind} response payload") from exc
