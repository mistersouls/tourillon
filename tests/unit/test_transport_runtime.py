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
"""Runtime transport tests to cover framing, client, server, and pool logic."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from tourillon.core.ports.transport import (
    ConnectionClosedError,
    ProtocolError,
    ResponseTimeoutError,
)
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.framing import read_envelope
from tourillon.core.transport.pool import PeerClientPool
from tourillon.core.transport.server import TcpServer, _ConnectionSession


class _DummyWriter:
    def __init__(self) -> None:
        self.buf = bytearray()
        self.closed = False

    def get_extra_info(self, _name: str, default: object = None) -> object:
        return default

    def write(self, data: bytes) -> None:
        self.buf.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def _reader_with_bytes(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


@pytest.mark.bootstrap
class TestFraming:
    @pytest.mark.asyncio
    async def test_read_envelope_ok_and_protocol_errors(self):
        ok_env = Envelope(kind="kv.put", payload=b"x", schema_id=1)
        decoded = await read_envelope(_reader_with_bytes(ok_env.encode()))
        assert decoded.kind == "kv.put"

        bad_ver = bytearray(ok_env.encode())
        bad_ver[0] = 7
        with pytest.raises(ProtocolError, match="error.proto_version_unsupported"):
            await read_envelope(_reader_with_bytes(bytes(bad_ver)))

        bad_kind = bytearray(ok_env.encode())
        bad_kind[23] = 0
        with pytest.raises(ProtocolError, match="error.kind_len_invalid"):
            await read_envelope(_reader_with_bytes(bytes(bad_kind)))

        payload_too_big = Envelope(kind="kv.put", payload=b"x" * 10)
        with pytest.raises(ProtocolError, match="error.payload_too_large"):
            await read_envelope(
                _reader_with_bytes(payload_too_big.encode()), max_payload=1
            )


@pytest.mark.bootstrap
class TestTcpClient:
    @pytest.mark.asyncio
    async def test_request_stream_send_close(self, monkeypatch):
        writer = _DummyWriter()

        async def fake_open_connection(_host, _port, ssl=None):  # noqa: ANN001
            return asyncio.StreamReader(), writer

        responses: asyncio.Queue[Envelope | None] = asyncio.Queue()

        async def fake_read_envelope(_reader):
            item = await responses.get()
            if item is None:
                raise asyncio.IncompleteReadError(partial=b"", expected=1)
            return item

        monkeypatch.setattr(
            "tourillon.core.transport.client.asyncio.open_connection",
            fake_open_connection,
        )
        monkeypatch.setattr(
            "tourillon.core.transport.client.read_envelope", fake_read_envelope
        )

        client = TcpClient()
        await client.connect("127.0.0.1:7001")

        req = Envelope(kind="test.req", payload=b"req")
        await responses.put(
            Envelope(
                kind="test.res",
                payload=b"ok",
                correlation_id=req.correlation_id,
            )
        )
        reply = await client.request(req, timeout=0.5)
        assert reply.kind == "test.res"

        stream_req = Envelope(kind="test.stream", payload=b"go")
        await responses.put(
            Envelope(
                kind="chunk.1", payload=b"1", correlation_id=stream_req.correlation_id
            )
        )
        await responses.put(
            Envelope(
                kind="chunk.2", payload=b"2", correlation_id=stream_req.correlation_id
            )
        )
        seen = []
        async for item in client.stream(stream_req, timeout=0.5):
            seen.append(item.kind)
            if len(seen) == 2:
                break
        assert seen == ["chunk.1", "chunk.2"]

        await client.send(Envelope(kind="fire", payload=b"and-forget"))
        assert writer.buf

        await responses.put(None)
        await client.close()
        assert not client.is_connected

    @pytest.mark.asyncio
    async def test_request_timeout_and_closed_paths(self, monkeypatch):
        writer = _DummyWriter()

        async def fake_open_connection(_host, _port, ssl=None):  # noqa: ANN001
            return asyncio.StreamReader(), writer

        async def slow_read(_reader):
            await asyncio.sleep(0.2)
            raise asyncio.IncompleteReadError(partial=b"", expected=1)

        monkeypatch.setattr(
            "tourillon.core.transport.client.asyncio.open_connection",
            fake_open_connection,
        )
        monkeypatch.setattr("tourillon.core.transport.client.read_envelope", slow_read)

        client = TcpClient()
        await client.connect("127.0.0.1:7001")

        with pytest.raises(ResponseTimeoutError):
            await client.request(Envelope(kind="timeout", payload=b"x"), timeout=0.01)

        await client.close()
        with pytest.raises(ConnectionClosedError):
            await client.send(Envelope(kind="x", payload=b"y"))


@pytest.mark.bootstrap
class TestServerAndPool:
    @pytest.mark.asyncio
    async def test_connection_session_routes_and_protocol_error(self, monkeypatch):
        dispatcher = Dispatcher()

        @dispatcher.on("echo")
        async def handler(receive, send):
            req = await receive()
            await send(
                Envelope(
                    kind="echo.ok",
                    payload=req.payload,
                    correlation_id=req.correlation_id,
                )
            )

        writer = _DummyWriter()
        cid = uuid.uuid4()
        sequence = [Envelope(kind="echo", payload=b"data", correlation_id=cid)]

        async def fake_read(_reader, _max=0):
            if sequence:
                return sequence.pop(0)
            await asyncio.sleep(0.01)
            raise asyncio.IncompleteReadError(partial=b"", expected=1)

        monkeypatch.setattr("tourillon.core.transport.server.read_envelope", fake_read)
        session = _ConnectionSession(dispatcher, asyncio.StreamReader(), writer, 1024)
        await session.run()
        assert writer.buf

        writer2 = _DummyWriter()

        async def fail_read(_reader, _max=0):
            raise ProtocolError("error.kind_len_invalid", uuid.uuid4())

        monkeypatch.setattr("tourillon.core.transport.server.read_envelope", fail_read)
        session2 = _ConnectionSession(dispatcher, asyncio.StreamReader(), writer2, 1024)
        await session2.run()
        assert b"error.kind_len_invalid" in writer2.buf

    @pytest.mark.asyncio
    async def test_tcpserver_start_stop(self, monkeypatch):
        class _FakeAsyncServer:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

            async def wait_closed(self):
                return None

        fake_server = _FakeAsyncServer()

        async def fake_start_server(handler, host, port, ssl=None):  # noqa: ANN001
            _ = (handler, host, port, ssl)
            return fake_server

        monkeypatch.setattr(
            "tourillon.core.transport.server.asyncio.start_server", fake_start_server
        )

        tcp_server = TcpServer(Dispatcher(), name="peer")
        await tcp_server.start("127.0.0.1", 9000)
        await tcp_server.stop()
        assert fake_server.closed is True

    @pytest.mark.asyncio
    async def test_peer_client_pool_acquire_release(self, monkeypatch):
        class _FakeClient:
            def __init__(self):
                self.is_connected = False
                self.connect_calls = 0
                self.close_calls = 0

            async def connect(self, _address, _tls):
                self.connect_calls += 1
                self.is_connected = True

            async def close(self):
                self.close_calls += 1
                self.is_connected = False

        created: list[_FakeClient] = []

        def _factory():
            client = _FakeClient()
            created.append(client)
            return client

        monkeypatch.setattr("tourillon.core.transport.pool.TcpClient", _factory)

        pool = PeerClientPool()
        c1 = await pool.acquire("n1", "127.0.0.1:7001")
        c2 = await pool.acquire("n1", "127.0.0.1:7001")
        assert c1 is c2

        c1.is_connected = False
        c3 = await pool.acquire("n1", "127.0.0.1:7001")
        assert c3 is not c1
        await pool.release("n1")
        await pool.close_all()


@pytest.mark.bootstrap
def test_transport_error_classes_cover_imported_ports():
    exc = ConnectionClosedError("peer-1")
    assert "peer-1" in str(exc)
    assert issubclass(ResponseTimeoutError, Exception)
