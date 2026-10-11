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
from tourclient.core import ProbeService, Transport
from tourlib.adapter import MsgPackSerializer
from tourlib.constant import MSGPACK_SCHEMA_ID
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ResponseError


@pytest.mark.asyncio
async def test_probe_service_ping_returns_ok(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    captured: dict[str, Envelope] = {}

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = request
        ping_data = serializer.decode(request.payload)
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "test-server",
                    "timestamp_ms": ping_data["timestamp_ms"],
                    "recv_ms": ping_data["timestamp_ms"] + 1,
                }
            ),
        )

    server = await start_envelope_server({"probe.ping": ping_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        try:
            await transport.connect()
            service = ProbeService(transport, serializer=serializer)
            result = await service.ping()
        finally:
            await transport.close()
    finally:
        await server.stop()

    assert captured["request"].schema_id == MSGPACK_SCHEMA_ID
    ping_data = serializer.decode(captured["request"].payload)
    assert ping_data["node_id"] == "client"
    assert isinstance(ping_data["timestamp_ms"], int)
    assert result.node_id == "test-server"
    assert result.timestamp_ms == ping_data["timestamp_ms"]
    assert result.recv_ms == ping_data["timestamp_ms"] + 1


@pytest.mark.asyncio
async def test_probe_service_ping_requires_transport_connected(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    """ProbeService.ping() surfaces connection errors when endpoint is unreachable."""
    server_ssl, client_ssl = mtls_contexts
    _ = server_ssl

    transport = Transport(
        bind="127.0.0.1:9999",
        ssl_context=client_ssl,
        serializer=MsgPackSerializer(),
    )
    service = ProbeService(transport, serializer=MsgPackSerializer())

    with pytest.raises(OSError):
        await service.ping()


@pytest.mark.asyncio
async def test_probe_service_ping_fails_on_unexpected_response_kind(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts

    async def bad_ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="probe.status",
            request=request,
            payload=b"OK",
        )

    server = await start_envelope_server({"probe.ping": bad_ping_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=MsgPackSerializer(),
        )
        try:
            await transport.connect()
            service = ProbeService(transport, serializer=MsgPackSerializer())
            with pytest.raises(ResponseError, match="unexpected ping response kind"):
                await service.ping()
        finally:
            await transport.close()
    finally:
        await server.stop()
