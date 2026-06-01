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
"""Multiplexed mTLS TCP client for all outgoing Envelope traffic.

A single TcpClient wraps one TLS connection and a background read loop that
delivers incoming Envelopes to the correct caller by matching correlation_id.
Multiple in-flight request()/stream() calls on the same connection are fully
supported; each is identified by its unique correlation_id UUID.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
import uuid
from collections.abc import AsyncIterator

from tourillon.core.ports.transport import (
    RESPONSE_TIMEOUT,
    ConnectionClosedError,
    ResponseTimeoutError,
)
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.framing import read_envelope

logger = logging.getLogger(__name__)

# Sentinel placed into a stream queue to signal that the stream is done.
_STREAM_CLOSED: Envelope = Envelope(kind="_stream.closed", payload=b"")


class TcpClient:
    """Multiplexed Envelope client for one mTLS connection.

    Call connect() once before any request() or stream() calls. The
    background read loop runs as a task inside the caller's TaskGroup or
    event loop. Call close() to tear down the connection gracefully; all
    pending callers receive ConnectionClosedError.
    """

    def __init__(self) -> None:
        self._writer: asyncio.StreamWriter | None = None
        self._reader: asyncio.StreamReader | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._addr: str = ""
        # correlation_id bytes → Future[Envelope] for request()
        self._pending: dict[bytes, asyncio.Future[Envelope]] = {}
        # correlation_id bytes → Queue[Envelope] for stream()
        self._streams: dict[bytes, asyncio.Queue[Envelope]] = {}
        self._closed = False

    async def connect(
        self,
        addr: str,
        tls_ctx: ssl.SSLContext | None = None,
    ) -> None:
        """Open a TLS connection to *addr* ("host:port") and start the read loop."""
        host, _, port_str = addr.rpartition(":")
        port = int(port_str)
        self._reader, self._writer = await asyncio.open_connection(
            host, port, ssl=tls_ctx
        )
        self._addr = addr
        self._closed = False
        loop = asyncio.get_running_loop()
        self._read_task = loop.create_task(
            self._read_loop(), name="TcpClient.read_loop"
        )
        logger.debug("connected addr=%s", addr)

    async def request(
        self,
        env: Envelope,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> Envelope:
        """Send *env* and return the single matching response Envelope.

        Raise ResponseTimeoutError if no matching response arrives within
        *timeout* seconds; the connection remains open. Raise
        ConnectionClosedError if the connection is lost before the response.
        """
        if self._closed or self._writer is None:
            raise ConnectionClosedError()

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Envelope] = loop.create_future()
        key = env.correlation_id.bytes
        self._pending[key] = fut

        await self._send(env)

        try:
            async with asyncio.timeout(timeout):
                return await fut
        except TimeoutError:
            self._pending.pop(key, None)
            raise ResponseTimeoutError(
                f"No response within {timeout}s for {env.correlation_id}"
            ) from None
        except asyncio.CancelledError:
            self._pending.pop(key, None)
            raise ConnectionClosedError() from None

    async def stream(
        self,
        env: Envelope,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> AsyncIterator[Envelope]:
        """Send *env* and yield every response Envelope sharing its correlation_id.

        *timeout* applies individually to each Envelope in the sequence. The
        caller detects the terminal Envelope (e.g. kind ending in '.done') and
        breaks the iteration. Raise ResponseTimeoutError or ConnectionClosedError
        on failure.
        """
        if self._closed or self._writer is None:
            raise ConnectionClosedError()

        queue: asyncio.Queue[Envelope] = asyncio.Queue()
        key = env.correlation_id.bytes
        self._streams[key] = queue

        await self._send(env)

        try:
            while True:
                item = await self._dequeue_with_timeout(
                    queue, timeout, env.correlation_id
                )
                if item is _STREAM_CLOSED:
                    raise ConnectionClosedError()
                yield item
        finally:
            self._streams.pop(key, None)

    async def send(self, env: Envelope) -> None:
        """Send *env* without registering any response handler (fire-and-forget).

        Use this to push rebalance.transfer chunks or a rebalance.commit back to
        the peer on a correlation_id already open via a concurrent stream() call.
        The caller is responsible for consuming any response envelopes via the
        active stream() queue on that correlation_id.

        Raise ConnectionClosedError when the connection is already closed.
        """
        if self._closed or self._writer is None:
            raise ConnectionClosedError()
        await self._send(env)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._read_task is not None:
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._read_task
        self._fail_pending()
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()

    @property
    def is_connected(self) -> bool:
        """Return True while the underlying connection is up."""
        return not self._closed and self._writer is not None

    async def _dequeue_with_timeout(
        self,
        queue: asyncio.Queue[Envelope],
        timeout: float,
        correlation_id: uuid.UUID,
    ) -> Envelope:
        """Get the next item from *queue* or raise ResponseTimeoutError."""
        try:
            async with asyncio.timeout(timeout):
                return await queue.get()
        except TimeoutError:
            raise ResponseTimeoutError(
                f"Stream timeout after {timeout}s for {correlation_id}"
            ) from None

    async def _send(self, env: Envelope) -> None:
        assert self._writer is not None
        logger.debug(
            "→ %s  cid=%.8s  addr=%s", env.kind, env.correlation_id, self._addr
        )
        data = env.encode()
        async with self._write_lock:
            self._writer.write(data)
            await self._writer.drain()

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                env = await read_envelope(self._reader)
                logger.debug(
                    "← %s  cid=%.8s  addr=%s",
                    env.kind,
                    env.correlation_id,
                    self._addr,
                )
                key = env.correlation_id.bytes

                if key in self._pending:
                    fut = self._pending.pop(key)
                    if not fut.done():
                        fut.set_result(env)
                elif key in self._streams:
                    await self._streams[key].put(env)
                else:
                    logger.debug(
                        "← unsolicited %s  cid=%.8s  addr=%s",
                        env.kind,
                        env.correlation_id,
                        self._addr,
                    )
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            pass
        except Exception:
            logger.exception("read_loop error")
        finally:
            self._closed = True
            self._fail_pending()

    def _fail_pending(self) -> None:
        exc = ConnectionClosedError()
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()
        for queue in list(self._streams.values()):
            queue.put_nowait(_STREAM_CLOSED)
        self._streams.clear()
