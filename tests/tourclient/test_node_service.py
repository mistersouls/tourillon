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

import ssl
from collections.abc import AsyncIterator

import pytest

from tests.helpers import start_envelope_server
from tests.helpers.inspect import make_status
from tourclient.core.services.node import NodeService
from tourclient.core.services.registry import get_service, validate_service_endpoint
from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.node import (
    CancelDrainResponse,
    CancelJoinResponse,
    DrainResponse,
    PauseResponse,
    ResumeResponse,
)

pytestmark = pytest.mark.tourctl


@pytest.mark.parametrize(
    ("kind", "document", "error"),
    [
        ("node.drain.accept", {"message": "Draining initiated"}, None),
        ("node.drain.reject", {"message": "Not ready"}, RequestRejectedError),
        ("node.drain.other", None, ResponseError),
        ("node.drain.accept", None, ResponseError),
        ("node.drain.accept", {"message": 1}, ResponseError),
        ("node.drain.accept", {"message": ""}, ResponseError),
        ("node.drain.accept", {"message": "ok", "unknown": True}, ResponseError),
        ("node.drain.reject", [], ResponseError),
    ],
)
async def test_leave_over_real_mtls_transport(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    kind: str,
    document: object,
    error: type[Exception] | None,
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    calls: list[object] = []

    async def handler(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(serializer.decode(request.payload))
        yield replay_envelope(
            kind=kind, request=request, payload=serializer.encode(document)
        )

    server = await start_envelope_server({"node.drain": handler}, server_ssl)
    host, port = server.endpoint
    transport = Transport(
        bind=f"{host}:{port}", ssl_context=client_ssl, serializer=serializer
    )
    try:
        await transport.connect()
        service = get_service(
            "node", "peer", transport=transport, serializer=serializer
        )
        assert isinstance(service, NodeService)
        if error is None:
            assert await service.drain() == DrainResponse("Draining initiated")
        else:
            with pytest.raises(error) as caught:
                await service.drain()
            if isinstance(caught.value, RequestRejectedError):
                assert caught.value.request_kind == "node.drain"
                assert str(caught.value) == "Not ready"
        assert calls == [{}]
    finally:
        await transport.close()
        await server.stop()


@pytest.mark.tourctl
async def test_pause_resume_and_inspect_share_node_service_over_mtls(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    calls: list[str] = []

    async def pause(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(request.kind)
        yield replay_envelope(
            kind="node.pause.accept",
            request=request,
            payload=serializer.encode({"message": "Node is paused."}),
        )

    async def resume(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(request.kind)
        yield replay_envelope(
            kind="node.resume.accept",
            request=request,
            payload=serializer.encode({"message": "Resume initiated."}),
        )

    async def inspect(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(request.kind)
        yield replay_envelope(
            kind="node.inspect.accept",
            request=request,
            payload=serializer.encode(make_status(phase="READY").to_document()),
        )

    server = await start_envelope_server(
        {"node.pause": pause, "node.resume": resume, "node.inspect": inspect},
        server_ssl,
    )
    host, port = server.endpoint
    transport = Transport(
        bind=f"{host}:{port}", ssl_context=client_ssl, serializer=serializer
    )
    try:
        await transport.connect()
        service = get_service(
            "node", "peer", transport=transport, serializer=serializer
        )
        assert isinstance(service, NodeService)
        assert await service.pause() == PauseResponse("Node is paused.")
        assert await service.resume() == ResumeResponse("Resume initiated.")
        assert (await service.status()).phase == "READY"
        assert calls == ["node.pause", "node.resume", "node.inspect"]
    finally:
        await transport.close()
        await server.stop()


@pytest.mark.tourctl
async def test_cancel_join_and_drain_use_their_peer_kinds_over_mtls(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    calls: list[str] = []

    async def cancel_join(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(request.kind)
        yield replay_envelope(
            kind="node.join.cancel.accept",
            request=request,
            payload=serializer.encode({"message": "node-a: JOINING -> IDLE"}),
        )

    async def cancel_drain(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(request.kind)
        yield replay_envelope(
            kind="node.drain.cancel.accept",
            request=request,
            payload=serializer.encode({"message": "node-a: DRAINING -> READY"}),
        )

    server = await start_envelope_server(
        {
            "node.join.cancel": cancel_join,
            "node.drain.cancel": cancel_drain,
        },
        server_ssl,
    )
    host, port = server.endpoint
    transport = Transport(
        bind=f"{host}:{port}", ssl_context=client_ssl, serializer=serializer
    )
    try:
        await transport.connect()
        service = get_service(
            "node", "peer", transport=transport, serializer=serializer
        )
        assert isinstance(service, NodeService)
        assert await service.cancel_join() == CancelJoinResponse(
            "node-a: JOINING -> IDLE"
        )
        assert await service.cancel_drain() == CancelDrainResponse(
            "node-a: DRAINING -> READY"
        )
        assert calls == ["node.join.cancel", "node.drain.cancel"]
    finally:
        await transport.close()
        await server.stop()


@pytest.mark.tourctl
async def test_cancel_drain_maps_rejection_to_request_rejected_error(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def reject(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="node.drain.cancel.reject",
            request=request,
            payload=serializer.encode({"message": "Node is not draining"}),
        )

    server = await start_envelope_server({"node.drain.cancel": reject}, server_ssl)
    host, port = server.endpoint
    transport = Transport(
        bind=f"{host}:{port}", ssl_context=client_ssl, serializer=serializer
    )
    try:
        await transport.connect()
        service = get_service(
            "node", "peer", transport=transport, serializer=serializer
        )
        assert isinstance(service, NodeService)
        with pytest.raises(RequestRejectedError, match="Node is not draining"):
            await service.cancel_drain()
    finally:
        await transport.close()
        await server.stop()


def test_node_drain_service_is_peer_only() -> None:
    validate_service_endpoint("node", "peer")
    with pytest.raises(ValueError, match="not available on endpoint 'client'"):
        validate_service_endpoint("node", "client")
