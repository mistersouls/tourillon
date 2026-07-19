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

import asyncio
import contextlib
import logging
import ssl
import uuid
from collections.abc import AsyncIterator

from tourillon.core.transport.conn import ConnectionHandler
from tourillon.core.transport.dispatcher import Dispatcher
from tourlib.envelope import Envelope
from tourlib.exceptions import ProtocolError
from tourlib.framing import MAX_IN_FLIGHT_PER_CONN, MAX_PAYLOAD_DEFAULT, read_envelope

logger = logging.getLogger(__name__)


class _ConnectionSession:
    """
    Manage the lifecycle of a single accepted TCP connection.

    This session implements the concurrency and routing model for a framed,
    half‑duplex protocol using correlation IDs (cid). Its responsibilities are:

      • read Envelopes sequentially from the wire,
      • validate and dispatch them to the correct handler,
      • ensure that at most one handler task runs per cid,
      • route additional Envelopes for the same cid to the existing handler
        via a per‑cid queue,
      • serialize all writes under a lock,
      • enforce MAX_IN_FLIGHT_PER_CONN,
      • cancel and clean up all handler tasks on shutdown.

    ## Intention

    The design guarantees that bursts of Envelopes for the same cid are never
    lost: the first Envelope spawns a handler task, and subsequent Envelopes
    for that cid are queued and drained by that same task. When the handler
    finishes, it removes the cid from `_in_flight`, ensuring that the next
    Envelope for that cid spawns a fresh handler.

    Because `_dispatch_loop` checks membership in `_in_flight` before routing,
    it cannot enqueue into a stale queue. This prevents race conditions between
    handler termination and dispatch.

    All writes to the wire are serialized under `_write_lock`, ensuring correct
    framing and preventing interleaving of responses.
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
        # correlation_id bytes → receive queue for a running handler.
        # Presence in this dict also serves as the in-flight membership check.
        self._in_flight: dict[bytes, asyncio.Queue[AsyncIterator[Envelope]]] = {}
        self._handler_tasks: set[asyncio.Task[None]] = set()

    async def run(self) -> None:
        """Read and dispatch Envelopes until the connection should close."""
        logger.debug("Accepted connection from %s.", self._peer)
        try:
            await self._dispatch_loop()
        except Exception as ex:
            logger.exception(
                "Unhandled error on connection from %s.", self._peer, exc_info=ex
            )
        finally:
            await self._close()
            logger.debug("Closed connection from %s.", self._peer)

    async def send(self, env: Envelope) -> None:
        """Write *env* to the wire, serialized under the write lock."""
        logger.debug(
            "→ %s  cid=%.8s  peer=%s", env.kind, env.correlation_id, self._peer
        )
        data = env.encode()
        async with self._write_lock:
            self._writer.write(data)
            await self._writer.drain()

    async def _dispatch_loop(self) -> None:
        while True:
            if len(self._in_flight) >= MAX_IN_FLIGHT_PER_CONN:
                logger.warning(
                    "Too many in-flight requests (%d) on connection from %s; closing.",
                    len(self._in_flight),
                    self._peer,
                )
                break

            env = await self._read_next()
            if env is None:
                break

            logger.debug(
                "← %s  cid=%.8s  peer=%s", env.kind, env.correlation_id, self._peer
            )

            handler = self._dispatcher.lookup(env.kind)
            if handler is None:
                logger.debug(
                    "Unknown envelope kind %r from %s; closing connection.",
                    env.kind,
                    self._peer,
                )
                break

            # Route to an already-running streaming handler when cid matches.
            cid_key = env.correlation_id.bytes
            if cid_key in self._in_flight:
                await self._in_flight[cid_key].put(handler(env))
                continue

            self._spawn_session(env, handler)

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
            logger.warning("Protocol error from %s: %s.", self._peer, exc.error_kind)
            return None
        except (TimeoutError, asyncio.IncompleteReadError, OSError):
            logger.debug(
                "Connection closed from %s (timeout or client disconnect).", self._peer
            )
            return None

    def _spawn_session(self, env: Envelope, handler: ConnectionHandler) -> None:
        cid_key = env.correlation_id.bytes
        queue: asyncio.Queue[AsyncIterator[Envelope]] = asyncio.Queue()
        queue.put_nowait(handler(env))
        self._in_flight[cid_key] = queue
        task: asyncio.Task[None] = asyncio.get_running_loop().create_task(
            self._run_session(env.correlation_id, queue)
        )
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)

    async def _run_session(
        self, cid: uuid.UUID, queue: asyncio.Queue[AsyncIterator[Envelope]]
    ) -> None:  # noqa: ANN001
        """Invoke *handler* then remove *env* from in-flight tracking."""

        try:
            while not queue.empty():
                envelopes = queue.get_nowait()
                async for envelope in envelopes:
                    await self.send(envelope)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logger.exception(
                "cid=%.8s handler raised an unhandled exception (peer %s).",
                cid,
                self._peer,
                exc_info=ex,
            )
        finally:
            self._in_flight.pop(cid.bytes, None)

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
        logger.debug("%s server listening on %s:%d.", self._name, host, port)

    async def stop(self) -> None:
        """Stop accepting connections and close the server socket."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.debug("%s server stopped.", self._name)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        session = _ConnectionSession(
            self._dispatcher, reader, writer, self._max_payload
        )
        await session.run()
