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

from tourillon.core.log import logger as _logger
from tourillon.core.ports import Telemetry
from tourillon.core.transports.dispatcher import Dispatcher
from tourillon.core.transports.session import EnvelopeSession
from tourlib.constant import MAX_IN_FLIGHT

logger = _logger.transport()


class EnvelopeServer:
    """Async envelope-aware TCP server.

    The server reads envelope frames from each client connection and dispatches
    them concurrently to handlers keyed by envelope kind.
    """

    def __init__(
        self,
        dispatcher: Dispatcher,
        ssl_context: ssl.SSLContext,
        *,
        name: str = "server",
        max_in_flight: int = MAX_IN_FLIGHT,
        telemetry: Telemetry,
    ) -> None:
        if dispatcher is None:
            raise ValueError("dispatcher is required")
        if ssl_context is None:
            raise ValueError("ssl_context is required")
        if ssl_context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("mTLS requires ssl_context.verify_mode=ssl.CERT_REQUIRED")
        if not name:
            raise ValueError("name must not be empty")
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be >= 1")

        self._dispatcher = dispatcher
        self._ssl_context = ssl_context
        self._name = name
        self._max_in_flight = max_in_flight
        self._telemetry = telemetry
        self._server: asyncio.base_events.Server | None = None

    @property
    def endpoint(self) -> tuple[str, int]:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("server is not running")

        sock_host, sock_port, *_ = self._server.sockets[0].getsockname()
        return str(sock_host), int(sock_port)

    async def start(self, host: str, port: int) -> None:
        if self._server is not None:
            raise RuntimeError("server is already running")

        logger.info("Starting transport server '%s' on %s:%s", self._name, host, port)

        self._server = await asyncio.start_server(
            self._handle_connection,
            host=host,
            port=port,
            ssl=self._ssl_context,
        )
        endpoint_host, endpoint_port = self.endpoint
        logger.info(
            "Transport server '%s' listening on %s:%s",
            self._name,
            endpoint_host,
            endpoint_port,
        )

    async def stop(self, grace: float = 5.0) -> None:
        if self._server is None:
            return

        server, self._server = self._server, None
        logger.debug("Stopping transport server '%s'", self._name)

        server.close()
        server.close_clients()
        try:
            async with asyncio.timeout(grace):
                await server.wait_closed()
        except TimeoutError:
            server.abort_clients()
            await server.wait_closed()
        logger.debug("Transport server '%s' stopped", self._name)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.debug("Accepted connection on '%s' from %s", self._name, peer)
        endpoint = self._name
        self._telemetry.connections_accepted_total.labels(endpoint=endpoint).inc()
        session = EnvelopeSession(
            dispatcher=self._dispatcher,
            reader=reader,
            writer=writer,
            max_in_flight=self._max_in_flight,
            telemetry=self._telemetry,
            endpoint=endpoint,
        )
        try:
            with self._telemetry.start_span(
                "transport.connection",
                attributes={"endpoint": endpoint, "peer": str(peer)},
            ):
                await session.serve()
        finally:
            self._telemetry.connections_closed_total.labels(endpoint=endpoint).inc()
            logger.debug("Closed connection on '%s' from %s", self._name, peer)
