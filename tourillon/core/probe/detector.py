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
import contextlib
import random
import time
from enum import StrEnum

from tourillon.core.log import describe, logger
from tourillon.core.probe.phi import PhiAccrual
from tourlib import Serializer
from tourlib.envelope import build_envelope
from tourlib.transport import EnvelopeClient


class PeerStatus(StrEnum):
    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"


class PeerFailureDetector:
    def __init__(
        self,
        peer_id: str,
        serializer: Serializer,
        threshold: float = 8.0,
        probe_interval: float = 5.0,
        probe_jitter: float = 0.2,
    ) -> None:
        self._peer_id = peer_id
        self._serializer = serializer
        self._threshold = threshold
        self._probe_interval = probe_interval
        self._probe_jitter = probe_jitter
        self._active: bool = False
        self._phi_accrual = PhiAccrual(threshold=self._threshold)
        self._client: EnvelopeClient | None = None
        self._lock = asyncio.Lock()
        self._probe_task: asyncio.Task | None = None

    @property
    def is_running(self) -> bool:
        return self._probe_task is not None and not self._probe_task.done()

    async def run(self, client: EnvelopeClient) -> None:
        async with self._lock:
            self._client = client

            if not self.is_running:
                self._active = True
                self._probe_task = asyncio.create_task(
                    self._probe_peer(), name=f"probe-{self._peer_id}"
                )

    async def stop(self) -> None:
        async with self._lock:
            task = self._probe_task
            self._probe_task = None
            self._active = False
            self._phi_accrual.reset()

        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def status(self) -> PeerStatus:
        if self._active:
            return PeerStatus.SUSPECT

        phi = self._phi_accrual.phi()
        if phi < 1:
            return PeerStatus.ALIVE
        elif phi < self._threshold:
            return PeerStatus.SUSPECT
        else:
            return PeerStatus.DEAD

    def is_alive(self) -> bool:
        return self.status() == PeerStatus.ALIVE

    def is_suspect(self) -> bool:
        return self.status() == PeerStatus.SUSPECT

    def is_dead(self) -> bool:
        return self.status() == PeerStatus.DEAD

    def reset(self) -> None:
        self._phi_accrual.reset()

    async def _probe_peer(self) -> None:
        if self._client is None:
            raise RuntimeError("Cannot probe peer without an EnvelopeClient")

        failures = 0
        while True:
            failures = await self._probe_once(self._client, failures)

            if self._phi_accrual.has_observations and self._phi_accrual.phi() < 1:
                self._active = False
                self._phi_accrual.reset()
                logger.debug(
                    "probe peer %s confirmed alive, probing stops", self._peer_id
                )
                break

            interval = self._probe_interval + random.uniform(
                -self._probe_jitter, self._probe_jitter
            )
            logger.debug(
                "Probing peer %s, next probe in %.2f seconds", self._peer_id, interval
            )
            await asyncio.sleep(interval)

    async def _probe_once(self, client: EnvelopeClient, failures: int) -> int:
        """Send one ping; return the number of consecutive failed probes.

        Only the first failure of a streak is a warning: a peer that stays down
        would otherwise log one line per probe interval until it comes back.
        """
        payload = self._serializer.encode({"timestamp_ms": int(time.time() * 1000)})
        ping = build_envelope(
            kind="probe.ping", payload=payload, schema_id=self._serializer.schema_id
        )
        try:
            await client.request(ping)
        except Exception as exc:
            failures += 1
            log = logger.warning if failures == 1 else logger.debug
            log(
                "probe peer %s failed (attempt %d): %s",
                self._peer_id,
                failures,
                describe(exc),
            )
            return failures

        self._phi_accrual.record()
        if failures:
            logger.info(
                "probe peer %s answered after %d failed attempt(s)",
                self._peer_id,
                failures,
            )
        return 0


class FailureDetectorProvider:
    def __init__(
        self,
        serializer: Serializer,
        threshold: float = 8.0,
        probe_interval: float = 5.0,
        probe_jitter: float = 0.2,
    ) -> None:
        self._serializer = serializer
        self._threshold = threshold
        self._probe_interval = probe_interval
        self._probe_jitter = probe_jitter

    def create(self, peer_id: str) -> PeerFailureDetector:
        return PeerFailureDetector(
            peer_id=peer_id,
            serializer=self._serializer,
            threshold=self._threshold,
            probe_interval=self._probe_interval,
            probe_jitter=self._probe_jitter,
        )
