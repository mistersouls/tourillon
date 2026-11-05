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

from collections.abc import Callable, Mapping
from typing import Any

from tourclient.core import Transport
from tourclient.exception import RequestRejectedError
from tourlib import Serializer
from tourlib.exceptions import ResponseError
from tourlib.models.ae import (
    AeStatus,
    AeStatusRequest,
    ObligationsPage,
    ObligationsRequest,
)


class AeService:
    """Anti-entropy of one node: pending delivery per target, obligations."""

    def __init__(self, transport: Transport, serializer: Serializer) -> None:
        self._transport = transport
        self._serializer = serializer

    async def status(self, request: AeStatusRequest) -> AeStatus:
        """Read the worker counters and one page of targets owed data."""
        return await self._request("ae.status", request, AeStatus.from_document)

    async def obligations(self, request: ObligationsRequest) -> ObligationsPage:
        """Read one page of the obligations persisted on the node."""
        return await self._request(
            "ae.obligations", request, ObligationsPage.from_document
        )

    async def _request(self, kind: str, request: Any, parse: Callable[[Any], Any]):
        response = await self._transport.request(
            kind=kind, document=request.to_document()
        )
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
        try:
            return parse(document)
        except (TypeError, ValueError) as exc:
            raise ResponseError(f"Invalid {kind} response payload") from exc
