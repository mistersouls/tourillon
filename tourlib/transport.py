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
import logging
import ssl
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import suppress

from tourlib.constant import (
    ENVELOPE_RESPONSE_TIMEOUT,
)
from tourlib.constant import TRANSPORT_ERROR_KIND
from tourlib.envelope import Envelope, read_envelope
from tourlib.exceptions import ProtocolError, ResponseTimeoutError
from tourlib.helpers import parse_bind

default_logger = logging.getLogger("tourlib.transport")


class EnvelopeClient:
    def __init__(
        self,
        bind: str,
        tls_ctx: ssl.SSLContext,
        logger: logging.Logger | None = None,
    ) -> None:
        self._bind = bind
        self._tls_ctx = tls_ctx
        self._logger = logger or default_logger
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._pending: dict[bytes, asyncio.Future[Envelope]] = {}
        self._streams: dict[bytes, asyncio.Queue[Envelope | BaseException | None]] = {}
        self._closing = False
        self._last_activity = time.monotonic()

    @property
    def bind(self) -> str:
        return self._bind

    @property
    def inactivity_duration(self) -> float:
        return time.monotonic() - self._last_activity

    async def connect(self) -> None:
        def consume_task_exception(task: asyncio.Task[None]) -> None:
            with suppress(asyncio.CancelledError):
                task.exception()

        async with self._connect_lock:
            if self.is_connected:
                return

            await self._close_connection()
            host, port = parse_bind(self._bind)
            self._reader, self._writer = await asyncio.open_connection(host, port, ssl=self._tls_ctx)
            reader = self._reader

            if reader is None:
                raise RuntimeError(f"Failed to create reader after connection {self._bind}")

            loop = asyncio.get_running_loop()
            read_task = loop.create_task(self._read_loop(), name="envelope-client-read-loop")
            read_task.add_done_callback(consume_task_exception)
            self._read_task = read_task
            self._logger.debug("Connected to %s:%d", host, port)

    async def close(self) -> None:
        async with self._connect_lock:
            self._closing = True
            try:
                await self._close_connection()
                self._logger.debug("Connection closed for %s", self._bind)
            finally:
                self._closing = False

    async def request(
        self,
        envelope: Envelope,
        timeout: float = ENVELOPE_RESPONSE_TIMEOUT
    ) -> Envelope:
        if envelope.mode != Envelope.MODE_RPC:
            raise ValueError("request envelope mode must be RPC")

        await self.connect()
        future = asyncio.get_running_loop().create_future()
        key = envelope.correlation_id.bytes
        self._pending[key] = future

        try:
            await self._send(envelope)
            async with asyncio.timeout(timeout):
                return await future
        except TimeoutError:
            raise ResponseTimeoutError(
                f"No response within {timeout}s for {envelope.correlation_id}"
            ) from None
        finally:
            self._pending.pop(key, None)

    async def send(self, envelope: Envelope) -> None:
        if envelope.mode != Envelope.MODE_FNF:
            raise ValueError("send envelope mode must be FNF")

        await self.connect()
        await self._send(envelope)

    async def stream(
        self,
        envelope: Envelope,
        timeout: float = ENVELOPE_RESPONSE_TIMEOUT
    ) -> AsyncIterator[Envelope]:
        if envelope.mode != Envelope.MODE_RPC:
            raise ValueError("Stream envelope mode must be RPC")

        queue: asyncio.Queue[Envelope | BaseException | None] = asyncio.Queue()
        cid = envelope.correlation_id
        key = cid.bytes

        # Connect before registering the stream: reconnecting tears the previous
        # connection down and closes every registered stream, this one included.
        await self.connect()
        self._streams[key] = queue

        try:
            await self._send(envelope)
            while True:
                item = await self._dequeue_with_timeout(queue, cid, timeout)
                if item is None:
                    raise ConnectionError(f"Stream closed for {cid}")
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            self._streams.pop(key, None)

    @property
    def is_connected(self) -> bool:
        return (
            self._reader is not None
            and self._writer is not None
            and not self._writer.is_closing()
            and self._read_task is not None
            and not self._read_task.done()
        )

    async def _close_connection(self) -> None:
        await self._teardown_reader(ConnectionError("connection closed"))

    @staticmethod
    async def _dequeue_with_timeout(
        queue: asyncio.Queue[Envelope | BaseException | None],
        correlation_id: uuid.UUID,
        timeout: float
    ) -> Envelope | BaseException | None:
        try:
            async with asyncio.timeout(timeout):
                return await queue.get()
        except TimeoutError:
            raise ResponseTimeoutError(
                f"Stream timeout after {timeout}s for {correlation_id}"
            ) from None

    def _fail_pending(self, error: BaseException) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    def _close_streams(self) -> None:
        for queue in self._streams.values():
            queue.put_nowait(None)

    def _reject(self, key: bytes, error: BaseException) -> None:
        future = self._pending.pop(key, None)
        if future is not None and not future.done():
            future.set_exception(error)

        queue = self._streams.get(key)
        if queue is not None:
            queue.put_nowait(error)

    async def _read_loop(self) -> None:
        reader = self._reader
        if reader is None:
            return
        failure: BaseException | None = None

        try:
            while True:
                envelope = await read_envelope(reader)
                self._last_activity = time.monotonic()
                self._logger.debug(
                    "Received kind=%s cid=%.8s bind=%s",
                    envelope.kind, envelope.correlation_id.bytes, self._bind
                )

                key = envelope.correlation_id.bytes
                if envelope.kind == TRANSPORT_ERROR_KIND:
                    message = envelope.payload.decode("utf-8", errors="replace")
                    self._reject(key, ProtocolError(f"transport error: {message}"))
                elif key in self._pending:
                    future = self._pending.pop(key)
                    if not future.done():
                        future.set_result(envelope)
                elif key in self._streams:
                    await self._streams[key].put(envelope)
                else:
                    self._logger.warning(
                        "Received unsolicited %s with cid=%.8s bind=%s; ignoring.",
                        envelope.kind, envelope.correlation_id, self._bind
                    )
        except (EOFError, OSError) as exc:
            failure = ConnectionError("Connection closed by peer")
            raise failure from exc
        except Exception as exc:
            self._logger.exception("Read loop error: %s", exc_info=exc)
            failure = exc
            raise exc
        finally:
            await self._teardown_reader(failure or ConnectionError("Connection closed"))

    async def _send(self, envelope: Envelope) -> None:
        try:
            async with self._write_lock:
                if self._closing:
                    raise ConnectionError("client is closing")

                writer = self._writer
                if writer is None or writer.is_closing():
                    raise RuntimeError("client is not connected")

                writer.write(envelope.to_bytes())
                await writer.drain()
                self._last_activity = time.monotonic()
                self._logger.debug(
                    "Sent %s cid=%.8s bind=%s",
                    envelope.kind, envelope.correlation_id.bytes, self._bind
                )
        except Exception as exc:
            self._logger.exception("Send error: %s", exc_info=exc)
            await self.close()
            raise exc

    async def _teardown_reader(self, error: BaseException) -> None:
        writer = self._writer
        current_task = asyncio.current_task()
        read_task = self._read_task

        self._reader = None
        self._writer = None
        self._read_task = None
        self._fail_pending(error)
        self._close_streams()

        if writer is not None and not writer.is_closing():
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

        if read_task is not None and read_task is not current_task:
            read_task.cancel()  # type: ignore[union-attr]
            with suppress(asyncio.CancelledError, ConnectionError, ProtocolError):
                await read_task # type: ignore[union-attr]
