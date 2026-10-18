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
import uuid
from typing import cast

import pytest

from tourlib.constant import PROTOCOL_VERSION, RAW_SCHEMA_ID, TRANSPORT_ERROR_KIND
from tourlib.envelope import Envelope
from tourlib.exceptions import ProtocolError, ResponseTimeoutError
from tourlib.transport import EnvelopeClient


def _rpc_envelope() -> Envelope:
    return Envelope(
        proto_version=PROTOCOL_VERSION,
        schema_id=RAW_SCHEMA_ID,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid.uuid4(),
        kind="kv.get",
        payload=b"ok",
    )


def _fnf_envelope() -> Envelope:
    return Envelope(
        proto_version=PROTOCOL_VERSION,
        schema_id=RAW_SCHEMA_ID,
        mode=Envelope.MODE_FNF,
        correlation_id=uuid.uuid4(),
        kind="kv.put",
        payload=b"ok",
    )


def _make_client() -> EnvelopeClient:
    return EnvelopeClient(
        bind="127.0.0.1:4000", tls_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    )


class _FakeWriter:
    def __init__(self) -> None:
        self.closed = False
        self.writes: list[bytes] = []

    def is_closing(self) -> bool:
        return self.closed

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class _FakeReader:
    pass


@pytest.mark.tourlib
def test_envelope_client_bind_property() -> None:
    client = _make_client()
    assert client.bind == "127.0.0.1:4000"


@pytest.mark.tourlib
def test_envelope_client_inactivity_duration_is_non_negative() -> None:
    client = _make_client()
    assert client.inactivity_duration >= 0.0


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_request_raises_value_error_for_fnf_mode() -> None:
    client = _make_client()
    with pytest.raises(ValueError, match="request envelope mode must be RPC"):
        await client.request(_fnf_envelope())


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_send_raises_value_error_for_rpc_mode() -> None:
    client = _make_client()
    with pytest.raises(ValueError, match="send envelope mode must be FNF"):
        await client.send(_rpc_envelope())


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_stream_raises_value_error_for_fnf_mode() -> None:
    client = _make_client()
    with pytest.raises(ValueError, match="Stream envelope mode must be RPC"):
        async for _ in client.stream(_fnf_envelope()):
            pass


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_send_raises_runtime_error_when_writer_is_none() -> None:
    client = _make_client()
    # _closing=False, _writer=None → RuntimeError
    with pytest.raises(RuntimeError, match="client is not connected"):
        await client._send(_rpc_envelope())


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_send_calls_connect_and_send_on_fnf_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    envelope = _fnf_envelope()
    calls: list[str] = []

    async def fake_connect() -> None:
        calls.append("connect")

    async def fake_send(_: Envelope) -> None:
        calls.append("send")

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(client, "_send", fake_send)

    await client.send(envelope)

    assert calls == ["connect", "send"]


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_connect_raises_runtime_error_when_reader_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    writer = _FakeWriter()

    async def fake_open_connection(host: str, port: int, ssl: ssl.SSLContext):
        _ = host, port, ssl
        return None, writer

    monkeypatch.setattr(
        "tourlib.transport.asyncio.open_connection", fake_open_connection
    )

    with pytest.raises(RuntimeError, match="Failed to create reader"):
        await client.connect()


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_connect_and_close_roundtrip_updates_connection_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    writer = _FakeWriter()
    read_gate = asyncio.Event()

    async def fake_open_connection(host: str, port: int, ssl: ssl.SSLContext):
        _ = host, port, ssl
        return _FakeReader(), writer

    async def fake_read_loop(self: EnvelopeClient) -> None:
        await read_gate.wait()

    monkeypatch.setattr(
        "tourlib.transport.asyncio.open_connection", fake_open_connection
    )
    monkeypatch.setattr(EnvelopeClient, "_read_loop", fake_read_loop)

    await client.connect()
    client._reader = cast(asyncio.StreamReader, cast(object, _FakeReader()))
    client._writer = cast(asyncio.StreamWriter, cast(object, writer))
    assert client.is_connected is True

    await client.close()
    assert client.is_connected is False


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_dequeue_with_timeout_returns_item_before_deadline() -> None:
    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    item = _rpc_envelope()
    await queue.put(item)

    result = await EnvelopeClient._dequeue_with_timeout(
        queue=queue,
        correlation_id=uuid.uuid4(),
        timeout=0.1,
    )

    assert result is item


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_dequeue_with_timeout_raises_response_timeout_error() -> None:
    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    correlation_id = uuid.uuid4()

    with pytest.raises(ResponseTimeoutError, match=f"{correlation_id}"):
        await EnvelopeClient._dequeue_with_timeout(
            queue=queue,
            correlation_id=correlation_id,
            timeout=0.01,
        )


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_request_propagates_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    envelope = _rpc_envelope()

    async def fake_connect() -> None:
        return None

    async def fake_send(_: Envelope) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(client, "_send", fake_send)

    with pytest.raises(asyncio.CancelledError):
        await client.request(envelope, timeout=0.01)


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_request_times_out_and_cleans_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    envelope = _rpc_envelope()

    async def fake_connect() -> None:
        return None

    async def fake_send(_: Envelope) -> None:
        return None

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(client, "_send", fake_send)

    with pytest.raises(ResponseTimeoutError, match=str(envelope.correlation_id)):
        await client.request(envelope, timeout=0.01)

    assert envelope.correlation_id.bytes not in client._pending


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_stream_yields_then_closes_and_cleans_stream_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    envelope = _rpc_envelope()
    streamed = _rpc_envelope()
    calls: list[int] = []

    async def fake_connect() -> None:
        return None

    async def fake_send(_: Envelope) -> None:
        return None

    async def fake_dequeue(
        queue: asyncio.Queue[Envelope | None], correlation_id: uuid.UUID, timeout: float
    ):
        _ = queue, correlation_id, timeout
        calls.append(1)
        return streamed if len(calls) == 1 else None

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(client, "_send", fake_send)
    monkeypatch.setattr(
        EnvelopeClient, "_dequeue_with_timeout", staticmethod(fake_dequeue)
    )

    received: list[Envelope] = []
    with pytest.raises(
        ConnectionError, match=f"Stream closed for {envelope.correlation_id}"
    ):
        async for item in client.stream(envelope, timeout=0.01):
            received.append(item)

    assert received == [streamed]
    assert envelope.correlation_id.bytes not in client._streams


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_stream_opened_on_a_dead_connection_survives_the_reconnection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    envelope = _rpc_envelope()
    streamed = _rpc_envelope()
    dead_writer = _FakeWriter()
    dead_writer.closed = True  # the previous connection died
    client._reader = cast(asyncio.StreamReader, cast(object, _FakeReader()))
    client._writer = cast(asyncio.StreamWriter, cast(object, dead_writer))
    read_gate = asyncio.Event()

    async def fake_open_connection(host: str, port: int, ssl: ssl.SSLContext):
        _ = host, port, ssl
        return _FakeReader(), _FakeWriter()

    async def fake_read_loop(self: EnvelopeClient) -> None:
        await self._streams[envelope.correlation_id.bytes].put(streamed)
        await read_gate.wait()

    monkeypatch.setattr(
        "tourlib.transport.asyncio.open_connection", fake_open_connection
    )
    monkeypatch.setattr(EnvelopeClient, "_read_loop", fake_read_loop)

    stream = client.stream(envelope, timeout=1.0)
    assert await anext(stream) == streamed
    await stream.aclose()
    read_gate.set()

    assert envelope.correlation_id.bytes not in client._streams
    await client.close()


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_read_loop_routes_pending_stream_and_unsolicited_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    pending = _rpc_envelope()
    streamed = _rpc_envelope()
    unsolicited = _rpc_envelope()
    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    future = asyncio.get_running_loop().create_future()
    client._reader = cast(asyncio.StreamReader, cast(object, _FakeReader()))
    client._writer = cast(asyncio.StreamWriter, cast(object, _FakeWriter()))
    client._read_task = asyncio.current_task()
    client._pending[pending.correlation_id.bytes] = future
    client._streams[streamed.correlation_id.bytes] = queue

    sequence = [pending, streamed, unsolicited]

    async def fake_read_envelope(reader: _FakeReader) -> Envelope:
        _ = reader
        if sequence:
            return sequence.pop(0)
        raise EOFError

    monkeypatch.setattr("tourlib.transport.read_envelope", fake_read_envelope)

    with pytest.raises(ConnectionError, match="Connection closed by peer"):
        await client._read_loop()

    assert future.done() and future.result() is pending
    drained: list[Envelope | None] = []
    while not queue.empty():
        drained.append(queue.get_nowait())
    assert drained[0] is streamed
    assert drained[-1] is None
    assert client._pending == {}
    assert streamed.correlation_id.bytes in client._streams


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_read_loop_rejects_only_targeted_request_on_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _make_client()
    failed = _rpc_envelope()
    succeeded = _rpc_envelope()
    queue: asyncio.Queue[Envelope | BaseException | None] = asyncio.Queue()
    stream_cid = _rpc_envelope()

    client._reader = cast(asyncio.StreamReader, cast(object, _FakeReader()))
    client._writer = cast(asyncio.StreamWriter, cast(object, _FakeWriter()))
    client._read_task = asyncio.current_task()

    failed_future = asyncio.get_running_loop().create_future()
    succeeded_future = asyncio.get_running_loop().create_future()
    client._pending[failed.correlation_id.bytes] = failed_future
    client._pending[succeeded.correlation_id.bytes] = succeeded_future
    client._streams[stream_cid.correlation_id.bytes] = queue

    failed_reply = Envelope(
        proto_version=PROTOCOL_VERSION,
        schema_id=RAW_SCHEMA_ID,
        mode=Envelope.MODE_RPC,
        correlation_id=failed.correlation_id,
        kind=TRANSPORT_ERROR_KIND,
        payload=b"internal error: boom",
    )
    success_reply = Envelope(
        proto_version=PROTOCOL_VERSION,
        schema_id=RAW_SCHEMA_ID,
        mode=Envelope.MODE_RPC,
        correlation_id=succeeded.correlation_id,
        kind="kv.get.accept",
        payload=b"ok",
    )
    streamed_reply = Envelope(
        proto_version=PROTOCOL_VERSION,
        schema_id=RAW_SCHEMA_ID,
        mode=Envelope.MODE_RPC,
        correlation_id=stream_cid.correlation_id,
        kind="kv.scan.accept",
        payload=b"chunk",
    )

    sequence = [failed_reply, success_reply, streamed_reply]

    async def fake_read_envelope(reader: _FakeReader) -> Envelope:
        _ = reader
        if sequence:
            return sequence.pop(0)
        raise EOFError

    monkeypatch.setattr("tourlib.transport.read_envelope", fake_read_envelope)

    with pytest.raises(ConnectionError, match="Connection closed by peer"):
        await client._read_loop()

    assert isinstance(failed_future.exception(), ProtocolError)
    assert str(failed_future.exception()) == "transport error: internal error: boom"
    assert succeeded_future.result() is success_reply

    drained: list[Envelope | BaseException | None] = []
    while not queue.empty():
        drained.append(queue.get_nowait())
    assert drained[0] is streamed_reply
    assert drained[-1] is None


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_teardown_reader_closes_stream_queues() -> None:
    client = EnvelopeClient(
        bind="127.0.0.1:4000",
        tls_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
    )
    envelope = _rpc_envelope()
    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    client._streams[envelope.correlation_id.bytes] = queue

    await client._teardown_reader(ConnectionError("closed"))

    assert await queue.get() is None


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_send_rejects_when_client_is_closing() -> None:
    client = EnvelopeClient(
        bind="127.0.0.1:4000",
        tls_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
    )
    client._closing = True

    with pytest.raises(ConnectionError, match="closing"):
        await client._send(_rpc_envelope())
