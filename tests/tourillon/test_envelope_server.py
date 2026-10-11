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

import asyncio
import ssl
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from tests.helpers import start_envelope_server
from tourillon.core.telemetry import build_noop_telemetry
from tourillon.core.transports.dispatcher import Dispatcher
from tourillon.core.transports.server import EnvelopeServer
from tourlib.constant import TRANSPORT_ERROR_KIND
from tourlib.envelope import Envelope, read_envelope


class _ServerStopStub:
    def __init__(self, *, first_wait_error: BaseException | None = None) -> None:
        self.first_wait_error = first_wait_error
        self.close_calls = 0
        self.close_clients_calls = 0
        self.abort_clients_calls = 0
        self.wait_closed_calls = 0
        self.sockets = [object()]

    def close(self) -> None:
        self.close_calls += 1

    def close_clients(self) -> None:
        self.close_clients_calls += 1

    def abort_clients(self) -> None:
        self.abort_clients_calls += 1

    async def wait_closed(self) -> None:
        self.wait_closed_calls += 1
        if self.wait_closed_calls == 1 and self.first_wait_error is not None:
            raise self.first_wait_error


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_routes_rpc_and_replies(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts

    async def ping_handler(envelope: Envelope) -> AsyncIterator[Envelope]:
        assert envelope.kind == "gossip.ping"
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="gossip.ping",
            payload=b"pong",
        )

    server = await start_envelope_server({"gossip.ping": ping_handler}, server_ssl)
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    request = Envelope(
        proto_version=1,
        schema_id=1,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="gossip.ping",
        payload=b"hello",
    )
    writer.write(request.to_bytes())
    await writer.drain()

    response = await read_envelope(reader)

    assert response.correlation_id == request.correlation_id
    assert response.kind == "gossip.ping"
    assert response.payload == b"pong"

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_handles_fnf_without_response(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    received = asyncio.Event()

    async def put_handler(envelope: Envelope) -> AsyncIterator[Envelope]:
        received.set()
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="kv.put",
            payload=b"ok",
        )

    server = await start_envelope_server({"kv.put": put_handler}, server_ssl)
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    request = Envelope(
        proto_version=1,
        schema_id=1,
        mode=Envelope.MODE_FNF,
        correlation_id=uuid4(),
        kind="kv.put",
        payload=b"value",
    )
    writer.write(request.to_bytes())
    await writer.drain()

    await asyncio.wait_for(received.wait(), timeout=1)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(reader.readexactly(1), timeout=0.1)

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_returns_error_for_unknown_kind(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    server = await start_envelope_server({}, server_ssl)
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    request = Envelope(
        proto_version=1,
        schema_id=2,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="kv.get",
        payload=b"",
    )
    writer.write(request.to_bytes())
    await writer.drain()

    response = await read_envelope(reader)
    assert response.kind == TRANSPORT_ERROR_KIND
    assert response.correlation_id == request.correlation_id
    assert response.payload.decode("utf-8", errors="replace").startswith("unknown kind")

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_allows_out_of_order_responses(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts

    async def list_handler(envelope: Envelope) -> AsyncIterator[Envelope]:
        delay_ms = int(envelope.payload.decode("utf-8"))
        await asyncio.sleep(delay_ms / 1000)
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="kv.list",
            payload=envelope.payload,
        )

    server = await start_envelope_server({"kv.list": list_handler}, server_ssl, max_in_flight=4)
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    slow = Envelope(
        proto_version=1,
        schema_id=3,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="kv.list",
        payload=b"200",
    )
    fast = Envelope(
        proto_version=1,
        schema_id=3,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="kv.list",
        payload=b"10",
    )

    writer.write(slow.to_bytes() + fast.to_bytes())
    await writer.drain()

    first = await read_envelope(reader)
    second = await read_envelope(reader)

    assert first.correlation_id == fast.correlation_id
    assert second.correlation_id == slow.correlation_id

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_closes_on_invalid_payload_header(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts

    async def get_handler(envelope: Envelope) -> AsyncIterator[Envelope]:
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="kv.get",
            payload=b"value",
        )

    server = await start_envelope_server({"kv.get": get_handler}, server_ssl)
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    kind = b"kv.get"
    frame = Envelope.HEADER_STRUCT.pack(
        1,
        1,
        Envelope.MODE_RPC,
        uuid4().bytes,
        Envelope.MAX_PAYLOAD + 1,
        len(kind),
        b"\x00" * 7,
    ) + kind

    writer.write(frame)
    await writer.drain()

    data = await asyncio.wait_for(reader.read(1), timeout=1)
    assert data == b""

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.tourillon
def test_envelope_server_requires_non_null_ssl_context() -> None:
    with pytest.raises(ValueError, match="ssl_context is required"):
        EnvelopeServer(Dispatcher({}), None, telemetry=build_noop_telemetry())  # type: ignore[arg-type]


@pytest.mark.tourillon
def test_envelope_server_requires_mtls_verify_mode() -> None:
    server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    with pytest.raises(ValueError, match="mTLS requires"):
        EnvelopeServer(Dispatcher({}), server_ssl, telemetry=build_noop_telemetry())


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_stop_closes_active_clients_before_waiting() -> None:
    server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ssl.verify_mode = ssl.CERT_REQUIRED
    server = EnvelopeServer(Dispatcher({}), server_ssl, telemetry=build_noop_telemetry())
    inner = _ServerStopStub()
    server._server = inner  # type: ignore[assignment]  # noqa: SLF001

    await server.stop()

    assert inner.close_calls == 1
    assert inner.close_clients_calls == 1
    assert inner.wait_closed_calls == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_envelope_server_stop_aborts_clients_after_grace_timeout() -> None:
    server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ssl.verify_mode = ssl.CERT_REQUIRED
    server = EnvelopeServer(Dispatcher({}), server_ssl, telemetry=build_noop_telemetry())
    inner = _ServerStopStub(first_wait_error=TimeoutError())
    server._server = inner  # type: ignore[assignment]  # noqa: SLF001

    await server.stop()

    assert inner.close_calls == 1
    assert inner.close_clients_calls == 1
    assert inner.abort_clients_calls == 1
    assert inner.wait_closed_calls == 2


