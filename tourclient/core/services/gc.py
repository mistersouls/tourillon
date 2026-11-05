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

from tourclient.core import Transport
from tourclient.exception import RequestRejectedError
from tourlib import Serializer
from tourlib.exceptions import ResponseError
from tourlib.models.gc import GcJobs, GcRequest, GcResponse


class GcService:
    """Local historical GC of one node: submit a pass, read its jobs."""

    def __init__(self, transport: Transport, serializer: Serializer) -> None:
        self._transport = transport
        self._serializer = serializer

    async def run(self, request: GcRequest) -> GcResponse:
        """Submit a local pass without waiting for its completion."""
        document = await self._request("gc.run", request)
        try:
            return GcResponse.from_document(document)
        except (TypeError, ValueError) as exc:
            raise ResponseError("Invalid gc.run response payload") from exc

    async def status(self) -> GcJobs:
        """Read the worker and its retained passes, newest first."""
        document = await self._request("gc.status", {})
        try:
            return GcJobs.from_document(document)
        except (TypeError, ValueError) as exc:
            raise ResponseError("Invalid gc.status response payload") from exc

    async def _request(self, kind: str, request: Any) -> Any:
        response = await self._transport.request(kind=kind, document=request)
        if response.kind not in {f"{kind}.accept", f"{kind}.reject"}:
            raise ResponseError(f"Unexpected {kind} response kind: {response.kind}")
        document = self._serializer.decode(response.payload)
        if response.kind == f"{kind}.reject":
            if (
                not isinstance(document, Mapping)
                or not isinstance(document.get("message"), str)
                or not document["message"]
            ):
                raise ResponseError(f"Invalid {kind} rejection payload")
            raise RequestRejectedError(kind, dict(document))
        return document
