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
"""asyncio TCP server with mTLS and Envelope framing.

Each accepted connection is managed by a _ConnectionSession that reads
Envelopes in a loop, validates them, and dispatches them as concurrent handler
tasks. In-flight tracking enforces MAX_IN_FLIGHT_PER_CONN per connection.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
from collections.abc import Callable

from tourillon.core.ports.transport import (
    MAX_IN_FLIGHT_PER_CONN,
    MAX_PAYLOAD_DEFAULT,
    ProtocolError,
)
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.framing import read_envelope

logger = logging.getLogger(__name__)


class _ConnectionSession:
    """Manage the lifecycle of a single accepted connection.

    Encapsulates the write lock, in-flight set, and handler task set so that
    TcpServer._handle_connection and the dispatch loop remain simple. Call
    run() to start reading; it returns when the connection should be closed.
    """

    def __init__(
        self,
        dispatcher: Dispatcher,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_payload: int,
    ) -> None:
        self._dispatcher = dispatcher
        self._reader = reader
        self._writer = writer
        self._max_payload = max_payload
        self._peer = writer.get_extra_info("peername", default="unknown")
        self._write_lock = asyncio.Lock()
        self._in_flight: set[bytes] = set()
        self._handler_tasks: set[asyncio.Task[None]] = set()

    async def run(self) -> None:
        """Read and dispatch Envelopes until the connection should close."""
        try:
            await self._dispatch_loop()
        except Exception:  # noqa: pragma: no cover
            logger.exception("connection loop error", extra={"peer": str(self._peer)})
        finally:
            await self._close()

    async def send(self, env: Envelope) -> None:
        """Write *env* to the wire, serialised under the write lock."""
        data = env.encode()
        async with self._write_lock:
            self._writer.write(data)
            await self._writer.drain()

    async def _dispatch_loop(self) -> None:
        while True:
            if len(self._in_flight) >= MAX_IN_FLIGHT_PER_CONN:
                logger.warning(
                    "too_many_in_flight",
                    extra={
                        "event": "too_many_in_flight",
                        "count": len(self._in_flight),
                        "peer": str(self._peer),
                    },
                )
                break

            env = await self._read_next()
            if env is None:
                break

            handler = self._dispatcher.lookup(env.kind)
            if handler is None:
                logger.debug(
                    "unknown_kind",
                    extra={"kind": env.kind, "peer": str(self._peer)},
                )
                break

            self._spawn_handler(env, handler)

    async def _read_next(self) -> Envelope | None:
        """Read one Envelope from the stream; return None on any terminal condition."""
        try:
            return await read_envelope(self._reader, self._max_payload)
        except ProtocolError as exc:
            error_env = Envelope(
                kind=exc.error_kind,
                payload=b"",
                correlation_id=exc.correlation_id,
            )
            await self.send(error_env)
            logger.warning(
                exc.error_kind,
                extra={"event": exc.error_kind, "peer": str(self._peer)},
            )
            return None
        except (TimeoutError, asyncio.IncompleteReadError, OSError):
            logger.debug(
                "read_timeout",
                extra={"event": "read_timeout", "peer": str(self._peer)},
            )
            return None

    def _spawn_handler(self, env: Envelope, handler: Callable) -> None:  # noqa: ANN001
        """Create a task for *handler* and track it for clean cancellation."""
        self._in_flight.add(env.correlation_id.bytes)
        task: asyncio.Task[None] = asyncio.get_running_loop().create_task(
            self._run_handler(env, handler)
        )
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)

    async def _run_handler(
        self, env: Envelope, handler: Callable
    ) -> None:  # noqa: ANN001
        """Invoke *handler* then remove *env* from in-flight tracking."""

        async def receive() -> Envelope:
            return env

        try:
            await handler(receive, self.send)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "handler error",
                extra={"kind": env.kind, "peer": str(self._peer)},
            )
        finally:
            self._in_flight.discard(env.correlation_id.bytes)

    async def _close(self) -> None:
        """Cancel all handler tasks and close the writer."""
        for task in list(self._handler_tasks):
            task.cancel()
        if self._handler_tasks:
            await asyncio.gather(*self._handler_tasks, return_exceptions=True)
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()


class TcpServer:
    """mTLS TCP server that accepts connections and dispatches Envelopes.

    Bind the server with start(). Call stop() during graceful shutdown. The
    ssl_context must enforce mutual TLS: require_cert=True with CERT_REQUIRED
    verify_mode must be set on the context before passing it here.

    The KV listener is only started when phase == READY and stopped otherwise;
    that lifecycle is managed by the bootstrap layer, not here.
    """

    def __init__(
        self,
        dispatcher: Dispatcher,
        ssl_context: ssl.SSLContext | None = None,
        max_payload: int = MAX_PAYLOAD_DEFAULT,
        name: str = "server",
    ) -> None:
        self._dispatcher = dispatcher
        self._ssl_context = ssl_context
        self._max_payload = max_payload
        self._name = name
        self._server: asyncio.Server | None = None

    async def start(self, host: str, port: int) -> None:
        """Bind and start accepting connections on *host*:*port*."""
        self._server = await asyncio.start_server(
            self._handle_connection,
            host,
            port,
            ssl=self._ssl_context,
        )
        logger.info("%s server listening on %s:%d.", self._name, host, port)

    async def stop(self) -> None:
        """Stop accepting connections and close the server socket."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("%s server stopped.", self._name)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        session = _ConnectionSession(
            self._dispatcher, reader, writer, self._max_payload
        )
        await session.run()
