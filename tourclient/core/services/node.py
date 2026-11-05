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
from typing import Any, Protocol, TypeVar

from tourclient.core import Transport
from tourclient.exception import RequestRejectedError
from tourlib import Serializer
from tourlib.exceptions import ResponseError
from tourlib.models.inspect import NodeStatus
from tourlib.models.node import (
    CancelDrainRequest,
    CancelDrainResponse,
    CancelJoinRequest,
    CancelJoinResponse,
    DrainRequest,
    DrainResponse,
    JoinRequest,
    JoinResponse,
    PauseRequest,
    PauseResponse,
    ResumeRequest,
    ResumeResponse,
)


class _MessageResponse(Protocol):
    message: str


_Response = TypeVar("_Response", bound=_MessageResponse)


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

    async def cancel_join(self) -> CancelJoinResponse:
        response = await self._transport.request(
            kind="node.join.cancel", document=CancelJoinRequest()
        )
        return self._message_response(
            response.kind,
            response.payload,
            "node.join.cancel",
            CancelJoinResponse.from_document,
        )

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

    async def cancel_drain(self) -> CancelDrainResponse:
        response = await self._transport.request(
            kind="node.drain.cancel", document=CancelDrainRequest()
        )
        return self._message_response(
            response.kind,
            response.payload,
            "node.drain.cancel",
            CancelDrainResponse.from_document,
        )

    async def pause(self) -> PauseResponse:
        response = await self._transport.request(
            kind="node.pause", document=PauseRequest()
        )
        return self._control_response(response.kind, response.payload, "node.pause")

    async def resume(self) -> ResumeResponse:
        response = await self._transport.request(
            kind="node.resume", document=ResumeRequest()
        )
        return self._control_response(response.kind, response.payload, "node.resume")

    async def status(self) -> NodeStatus:
        response = await self._transport.request(kind="node.inspect", document={})
        if response.kind != "node.inspect.accept":
            raise ResponseError(
                f"Unexpected node inspect response kind: {response.kind}"
            )
        try:
            return NodeStatus.from_document(self._serializer.decode(response.payload))
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseError("Invalid node inspect response") from exc

    def _control_response(
        self, kind: str, payload: bytes, request_kind: str
    ) -> PauseResponse | ResumeResponse:
        response_kind = f"{request_kind}.accept"
        reject_kind = f"{request_kind}.reject"
        if kind not in {response_kind, reject_kind}:
            raise ResponseError(f"Unexpected {request_kind} response kind: {kind}")

        try:
            document = self._serializer.decode(payload)
            result = (
                PauseResponse.from_document(document)
                if request_kind == "node.pause"
                else ResumeResponse.from_document(document)
            )
        except (TypeError, ValueError) as exc:
            raise ResponseError(f"Invalid {request_kind} response payload") from exc
        if kind == reject_kind:
            raise RequestRejectedError(request_kind, {"message": result.message})
        return result

    def _message_response(
        self,
        kind: str,
        payload: bytes,
        request_kind: str,
        parser: Callable[[Any], _Response],
    ) -> _Response:
        response_kind = f"{request_kind}.accept"
        reject_kind = f"{request_kind}.reject"
        if kind not in {response_kind, reject_kind}:
            raise ResponseError(f"Unexpected {request_kind} response kind: {kind}")

        try:
            result = parser(self._serializer.decode(payload))
        except (TypeError, ValueError) as exc:
            raise ResponseError(f"Invalid {request_kind} response payload") from exc
        if kind == reject_kind:
            raise RequestRejectedError(request_kind, {"message": result.message})
        return result
