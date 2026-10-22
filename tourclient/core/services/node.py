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
from tourlib.models.node import DrainRequest, DrainResponse, JoinRequest, JoinResponse


class NodeService:
    def __init__(
        self,
        transport: Transport,
        serializer: Serializer,
    ) -> None:
        self._transport = transport
        self._serializer = serializer

    async def join(self, req: JoinRequest) -> JoinResponse:
        req = req or JoinRequest()
        response = await self._transport.request(kind="node.join", document=req)
        data: Mapping[str, Any] = self._serializer.decode(response.payload)
        if response.kind == "node.join.accept":
            return JoinResponse(**data, success=True)
        if response.kind == "node.join.reject":
            return JoinResponse(message=data.get("message", ""), success=False)
        raise ResponseError(f"Unexpected node join response kind: {response.kind}")

    async def drain(self) -> DrainResponse:
        """Initiate draining without waiting for the node to finish."""
        response = await self._transport.request(
            kind="node.drain", document=DrainRequest()
        )
        if response.kind not in {"node.drain.accept", "node.drain.reject"}:
            raise ResponseError(f"Unexpected node drain response kind: {response.kind}")
        try:
            document = self._serializer.decode(response.payload)
            result = DrainResponse.from_document(document)
        except (TypeError, ValueError) as exc:
            raise ResponseError("Invalid node drain response payload") from exc
        if response.kind == "node.drain.reject":
            raise RequestRejectedError("node.drain", {"message": result.message})
        return result
