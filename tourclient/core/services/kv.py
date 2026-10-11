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
from tourlib.models.kv import DeleteRequest, GetRequest, PutRequest
from tourlib.models.version import Version


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
        quorum_write: int | None = None,
    ) -> Version | None:
        request = PutRequest(
            namespace=self._serializer.encode(namespace),
            key=self._serializer.encode(key),
            value=self._serializer.encode(value),
            quorum_write=quorum_write,
        )
        return await self._request_and_decode("kv.put", "kv.put.accept", request)

    async def get(
        self,
        *,
        namespace: Any,
        key: Any,
        quorum_read: int | None = None,
    ) -> Version | None:
        request = GetRequest(
            namespace=self._serializer.encode(namespace),
            key=self._serializer.encode(key),
            quorum_read=quorum_read,
        )
        return await self._request_and_decode("kv.get", "kv.get.accept", request)

    async def delete(
        self,
        *,
        namespace: Any,
        key: Any,
        quorum_write: int | None = None,
    ) -> Version | None:
        request = DeleteRequest(
            namespace=self._serializer.encode(namespace),
            key=self._serializer.encode(key),
            quorum_write=quorum_write,
        )
        return await self._request_and_decode("kv.delete", "kv.delete.accept", request)

    async def _request_and_decode(
        self,
        request_kind: str,
        response_kind: str,
        request: PutRequest | DeleteRequest | GetRequest,
    ) -> Version | None:
        response = await self._transport.request(kind=request_kind, document=request)
        if response.kind == f"{request_kind}.reject":
            raise self._rejected(request_kind, response.payload)
        if response.kind != response_kind:
            raise ResponseError(f"unexpected {request_kind} response kind: {response.kind}")
        document = self._serializer.decode(response.payload)
        return self._parse_version(document, request_kind=request_kind)

    def _rejected(self, request_kind: str, payload: bytes) -> RequestRejectedError:
        try:
            document = self._serializer.decode(payload)
        except ValueError:
            document = None
        details = document if isinstance(document, Mapping) else {}
        return RequestRejectedError(request_kind, details)

    def _parse_version(self, document: Any, *, request_kind: str) -> Version | None:
        if document is None:
            return None
        if not isinstance(document, Mapping):
            raise ResponseError(f"invalid {request_kind} response payload")
        try:
            return Version.from_dict(document)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError(f"invalid {request_kind} response payload") from exc
