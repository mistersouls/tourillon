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

from tourillon.core.log import logger as _logger
from tourillon.core.ports import Telemetry
from tourillon.core.transports.dispatcher import Dispatcher
from tourlib.constant import MAX_ENVELOPE_PAYLOAD, MAX_IN_FLIGHT, TRANSPORT_ERROR_KIND
from tourlib.envelope import Envelope, read_envelope
from tourlib.exceptions import ProtocolError

logger = _logger.transport()


class EnvelopeSession:
    """Manage one envelope-aware connection lifecycle."""

    def __init__(
        self,
        dispatcher: Dispatcher,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        telemetry: Telemetry,
        max_payload: int = MAX_ENVELOPE_PAYLOAD,
        max_in_flight: int = MAX_IN_FLIGHT,
        endpoint: str = "server",
    ) -> None:
        if max_payload < 1:
            raise ValueError("max_payload must be >= 1")
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be >= 1")

        self._dispatcher = dispatcher
        self._reader = reader
        self._writer = writer
        self._max_payload = max_payload
        self._max_in_flight = max_in_flight
        self._write_lock = asyncio.Lock()
        self._telemetry = telemetry
        self._endpoint = endpoint
        self._in_flight = 0

    async def serve(self) -> None:
        semaphore = asyncio.Semaphore(self._max_in_flight)
        tasks: set[asyncio.Task[None]] = set()
        peer = self._writer.get_extra_info("peername")
        logger.debug("Session opened from %s", peer)

        try:
            while True:
                try:
                    envelope = await read_envelope(
                        self._reader, max_payload=self._max_payload
                    )
                except EOFError:
                    logger.debug("Session closed by peer %s", peer)
                    break
                except ConnectionResetError:
                    logger.debug("Connection reset by peer %s", peer)
                    break
                except ProtocolError as exc:
                    logger.warning("Protocol error from %s: %s", peer, exc)
                    self._telemetry.protocol_errors_total.labels(
                        endpoint=self._endpoint
                    ).inc()
                    break

                await semaphore.acquire()
                self._update_in_flight(1)
                task = asyncio.create_task(self._process_envelope(envelope, semaphore))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        finally:
            if tasks:
                logger.debug(
                    "Waiting for %s in-flight task(s) from %s", len(tasks), peer
                )
                await asyncio.gather(*tasks, return_exceptions=True)

            if not self._writer.is_closing():
                self._writer.close()
                await self._writer.wait_closed()
            logger.debug("Session fully closed from %s", peer)

    async def _process_envelope(
        self,
        envelope: Envelope,
        semaphore: asyncio.Semaphore,
    ) -> None:
        try:
            with self._telemetry.start_span(
                "transport.envelope",
                attributes={
                    "endpoint": self._endpoint,
                    "kind": envelope.kind,
                    "mode": envelope.mode,
                },
            ):
                logger.info(
                    "Received envelope kind=%s mode=%s cid=%.8s",
                    envelope.kind,
                    envelope.mode,
                    envelope.correlation_id.hex,
                )
                handler = self._dispatcher.lookup(envelope.kind)
                if handler is None:
                    self._telemetry.unknown_kind_total.labels(
                        endpoint=self._endpoint
                    ).inc()
                    logger.warning("No handler registered for kind '%s'", envelope.kind)
                    await self._send_unknown_kind_error(envelope)
                    return

                responses = handler(envelope)
                self._telemetry.dispatched_total.labels(endpoint=self._endpoint).inc()
                if envelope.mode == Envelope.MODE_FNF:
                    async for _ in responses:
                        # FNF forbids responses; drain to execute handler side effects.
                        continue
                    logger.debug("Processed FNF envelope kind=%s", envelope.kind)
                    return

                async for response in responses:
                    await self._write_envelope(response)
        except Exception as exc:
            self._telemetry.protocol_errors_total.labels(endpoint=self._endpoint).inc()
            logger.exception(
                "Handler failure for kind '%s' (mode=%s)",
                envelope.kind,
                envelope.mode,
            )
            with self._telemetry.start_span(
                "transport.envelope.error",
                attributes={
                    "endpoint": self._endpoint,
                    "kind": envelope.kind,
                    "mode": envelope.mode,
                },
            ) as span:
                span.record_exception(exc)
                span.set_status_error(str(exc))
            if envelope.mode == Envelope.MODE_RPC:
                await self._send_internal_error(envelope, str(exc))
        finally:
            self._update_in_flight(-1)
            semaphore.release()

    async def _send_unknown_kind_error(self, envelope: Envelope) -> None:
        if envelope.mode == Envelope.MODE_FNF:
            return

        logger.debug("Sending unknown-kind error for '%s'", envelope.kind)

        payload = f"unknown kind: {envelope.kind}".encode()
        error = Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind=TRANSPORT_ERROR_KIND,
            payload=payload,
        )
        await self._write_envelope(error)

    async def _send_internal_error(self, envelope: Envelope, message: str) -> None:
        logger.warning(
            "Sending internal error for kind '%s': %s", envelope.kind, message
        )
        payload = f"internal error: {message}".encode()
        error = Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind=TRANSPORT_ERROR_KIND,
            payload=payload,
        )
        await self._write_envelope(error)

    async def _write_envelope(self, envelope: Envelope) -> None:
        if self._writer.is_closing():
            logger.debug("Writer is closing; dropping envelope kind=%s", envelope.kind)
            return

        async with self._write_lock:
            self._writer.write(envelope.to_bytes())
            await self._writer.drain()

    def _update_in_flight(self, delta: int) -> None:
        self._in_flight += delta
        self._telemetry.in_flight_current.labels(endpoint=self._endpoint).set(
            self._in_flight
        )
