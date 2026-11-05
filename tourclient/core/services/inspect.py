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
from tourlib.models.health import NodeHealth
from tourlib.models.inspect import MembersPage, MembersRequest, NodeStatus
from tourlib.models.member import MemberReport, MemberRequest


class InspectService:
    """Operator views of node health, cluster membership and member details."""

    def __init__(
        self,
        transport: Transport,
        serializer: Serializer,
    ) -> None:
        self._transport = transport
        self._serializer = serializer

    async def status(self) -> NodeStatus:
        data = await self._request("node.inspect", {})
        try:
            return NodeStatus.from_document(data)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError("invalid node.inspect response") from exc

    async def health(self) -> NodeHealth:
        data = await self._request("node.health", {})
        try:
            return NodeHealth.from_document(data)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError("invalid node.health response") from exc

    async def members(self, request: MembersRequest | None = None) -> MembersPage:
        request = request or MembersRequest()
        data = await self._request("node.members", request.to_document())
        try:
            return MembersPage.from_document(data)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError("invalid node.members response") from exc

    async def member(self, request: MemberRequest) -> MemberReport:
        data = await self._request("node.member", request.to_document())
        try:
            return MemberReport.from_document(data)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError("invalid node.member response") from exc

    async def _request(self, kind: str, document: dict[str, Any]) -> Mapping:
        response = await self._transport.request(kind=kind, document=document)
        data = self._serializer.decode(response.payload)
        if response.kind == f"{kind}.reject":
            raise RequestRejectedError(kind, data if isinstance(data, Mapping) else {})
        if response.kind != f"{kind}.accept":
            raise ResponseError(f"unexpected {kind} response kind: {response.kind}")
        if not isinstance(data, Mapping):
            raise ResponseError(f"invalid {kind} response payload")
        return data
