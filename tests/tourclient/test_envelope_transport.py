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
import asyncio
from collections.abc import AsyncIterator

import pytest

from tests.helpers import start_envelope_server
from tourclient.core import Transport
from tourlib.adapter import MsgPackSerializer
from tourlib.constant import MSGPACK_SCHEMA_ID
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ProtocolError


@pytest.mark.asyncio
async def test_envelope_transport_request_document_uses_default_codec_schema(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    document = {
        "namespace": "default",
        "key": "hello",
        "value": {"count": 1},
    }
    captured: dict[str, Envelope] = {}

    async def echo_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = request
        yield replay_envelope(
            kind="kv.list",
            request=request,
            payload=request.payload,
        )

    server = await start_envelope_server({"kv.list": echo_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            response = await transport.request("kv.list", document)
        finally:
            await transport.close()
    finally:
        await server.stop()

    assert captured["request"].schema_id == MSGPACK_SCHEMA_ID
    assert serializer.decode(captured["request"].payload) == document
    assert response.schema_id == MSGPACK_SCHEMA_ID
    assert serializer.decode(response.payload) == document


@pytest.mark.asyncio
async def test_envelope_transport_surfaces_transport_error_payload(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    transport = None
    server = await start_envelope_server({}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()

        with pytest.raises(ProtocolError, match="transport error: unknown kind: kv.get"):
            await transport.request("kv.get", {})
    finally:
        if transport is not None:
            await transport.close()
        await server.stop()


@pytest.mark.asyncio
async def test_envelope_transport_keeps_other_in_flight_request_alive_after_transport_error(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def slow_list_handler(request: Envelope) -> AsyncIterator[Envelope]:
        await asyncio.sleep(0.1)
        yield replay_envelope(
            kind="kv.list",
            request=request,
            payload=request.payload,
        )

    server = await start_envelope_server({"kv.list": slow_list_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            in_flight_ok = asyncio.create_task(
                transport.request("kv.list", {"namespace": "default", "key": "ok"})
            )
            await asyncio.sleep(0.01)

            in_flight_error = asyncio.create_task(
                transport.request("kv.get", {"namespace": "default", "key": "missing"})
            )

            with pytest.raises(ProtocolError, match="transport error: unknown kind: kv.get"):
                await in_flight_error

            ok_response = await in_flight_ok
        finally:
            await transport.close()
    finally:
        await server.stop()

    assert serializer.decode(ok_response.payload) == {
        "namespace": "default",
        "key": "ok",
    }

