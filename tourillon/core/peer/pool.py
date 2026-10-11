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
import ssl
from collections.abc import AsyncIterator
from collections import OrderedDict
from dataclasses import dataclass

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger as _logger
from tourillon.core.ports.telemetry import Telemetry
from tourillon.core.probe.detector import FailureDetectorProvider, PeerFailureDetector
from tourillon.core.transports.client import PeerClient
from tourlib.constant import ENVELOPE_CONNECTION_TIMEOUT
from tourlib.transport import EnvelopeClient

logger = _logger.transport()


@dataclass
class PeerConnection:
    peer_id: str
    client: EnvelopeClient
    fd: PeerFailureDetector
    refcount: int = 0
    ephemeral: bool = False


class PeerPool:
    def __init__(
        self,
        tls_ctx: ssl.SSLContext,
        topologymgr: TopologyManager,
        fd_provider: FailureDetectorProvider,
        telemetry: Telemetry,
        min_size: int = 32,
        max_size: int = 256,
        max_overflow: int = 8,
        idle_ttl: float = 60 * 10,
        connection_timeout: float = ENVELOPE_CONNECTION_TIMEOUT,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if idle_ttl <= 0:
            raise ValueError("idle_ttl must be > 0")

        self._tls_ctx = tls_ctx
        self._topologymgr = topologymgr
        self._fd_provider = fd_provider
        self._min_size = min_size
        self._max_size = max_size
        self._max_overflow = max_overflow
        self._idle_ttl = idle_ttl
        self._connection_timeout = connection_timeout
        self._telemetry = telemetry

        self._permanents: dict[str, PeerConnection] = {}
        self._overflows: dict[str, PeerConnection] = {}

        # LRU tracks permanent peers only;
        # overflow peers are managed separately.
        self._lru: OrderedDict[str, PeerConnection] = OrderedDict()

        self._pool_lock = asyncio.Lock()
        self._reaper_task: asyncio.Task | None = None

    def failure_detector(self, peer_id: str) -> PeerFailureDetector | None:
        if peer_id in self._permanents:
            return self._permanents[peer_id].fd
        if peer_id in self._overflows:
            return self._overflows[peer_id].fd
        return None

    async def start(self) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            loop = asyncio.get_running_loop()
            self._reaper_task = loop.create_task(
                self._idle_reaper(), name="client-pool-idle-reaper"
            )
            logger.info("Started client connect pool")

    async def acquire(
        self,
        peer_id: str,
    ) -> EnvelopeClient:
        with self._telemetry.start_span("pool.acquire", attributes={"peer_id": peer_id}) as span:
            try:
                async with self._pool_lock:
                    return await self._acquire_locked(peer_id)
            except TimeoutError as exc:
                self._telemetry.pool_acquire_errors_total.add(1, attributes={"peer_id": peer_id})
                span.record_exception(exc)
                span.set_status_error("connection timeout")
                logger.warning("pool.acquire timeout peer_id=%s", peer_id)
                raise

    @contextlib.asynccontextmanager
    async def lease(self, peer_id: str) -> AsyncIterator[EnvelopeClient]:
        """Acquire a peer client for a context and always release it on exit."""
        client = await self.acquire(peer_id)
        try:
            yield client
        finally:
            await self.release(peer_id)

    async def _acquire_locked(self, peer_id: str) -> EnvelopeClient:
        bind = await self._bind_for(peer_id)
        if peer_id in self._permanents:
            peer = self._permanents[peer_id]
            await self._may_refresh_peer(peer, bind)
            self._lru.move_to_end(peer_id)
            logger.debug("pool.acquire reuse permanent peer_id=%s", peer_id)
            return peer.client

        if peer_id in self._overflows:
            peer = self._overflows[peer_id]
            await self._may_refresh_peer(peer, bind)
            logger.debug("pool.acquire reuse overflow peer_id=%s", peer_id)
            return peer.client

        if len(self._permanents) < self._max_size:
            peer = await self._create_connected_peer(peer_id, bind)
            peer.refcount += 1
            self._permanents[peer_id] = peer
            self._lru[peer_id] = peer
            self._telemetry.pool_connections_total.add(1, attributes={"peer_id": peer_id, "type": "permanent"})
            self._update_size_gauges()
            logger.info("pool.acquire new permanent peer_id=%s bind=%s", peer_id, bind)
            return peer.client

        if len(self._overflows) < self._max_overflow:
            peer = await self._create_connected_peer(peer_id, bind)
            peer.ephemeral = True
            peer.refcount += 1
            self._overflows[peer_id] = peer
            self._telemetry.pool_connections_total.add(1, attributes={"peer_id": peer_id, "type": "overflow"})
            self._update_size_gauges()
            logger.info("pool.acquire new overflow peer_id=%s bind=%s", peer_id, bind)
            return peer.client

        # max_overflow reached – evict LRU permanent
        victim_id, victim_peer = self._lru.popitem(last=False)
        await victim_peer.client.close()
        await victim_peer.fd.stop()
        del self._permanents[victim_id]
        self._telemetry.pool_evictions_total.add(1, attributes={"peer_id": victim_id, "reason": "lru_evict"})
        logger.info("pool.acquire lru_evict victim_id=%s new_peer_id=%s", victim_id, peer_id)

        peer = await self._create_connected_peer(peer_id, bind)
        peer.refcount += 1
        self._permanents[peer_id] = peer
        self._lru[peer_id] = peer
        self._telemetry.pool_connections_total.add(1, attributes={"peer_id": peer_id, "type": "permanent"})
        self._update_size_gauges()
        return peer.client

    async def release(self, peer_id: str) -> None:
        async with self._pool_lock:
            peer = self._permanents.get(peer_id) or self._overflows.get(peer_id)
            if peer is None:
                return

            peer.refcount -= 1
            logger.debug(
                "pool.release peer_id=%s refcount=%d ephemeral=%s",
                peer_id, peer.refcount, peer.ephemeral,
            )

            # auto-delete overflow
            if peer.ephemeral and peer.refcount == 0:
                await peer.client.close()
                await peer.fd.stop()
                del self._overflows[peer_id]
                self._telemetry.pool_evictions_total.add(
                    1, attributes={"peer_id": peer_id, "reason": "overflow_release"}
                )
                self._update_size_gauges()
                logger.info("pool.release overflow closed peer_id=%s", peer_id)

    async def shutdown(self) -> None:
        if self._reaper_task is not None and not self._reaper_task.done():
            self._reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper_task
            self._reaper_task = None
            logger.info("Stopped client connect pool")

        async with self._pool_lock:
            peers = list(self._permanents.values()) + list(self._overflows.values())

            for peer in peers:
                await peer.client.close()
                await peer.fd.stop()
                self._telemetry.pool_evictions_total.add(
                    1, attributes={"peer_id": peer.peer_id, "reason": "pool_close"}
                )
                logger.info("pool.close evict peer_id=%s", peer.peer_id)

            self._permanents.clear()
            self._overflows.clear()
            self._update_size_gauges()

    async def _create_connected_peer(self, peer_id: str, bind: str) -> PeerConnection:
        fd = self._fd_provider.create(peer_id=peer_id)
        client = PeerClient(bind, self._tls_ctx, fd=fd, logger=logger)
        await asyncio.wait_for(client.connect(), timeout=self._connection_timeout)
        peer = PeerConnection(peer_id=peer_id, client=client, fd=fd)
        return peer

    async def _idle_reaper(self) -> None:
        while True:
            await asyncio.sleep(1)

            async with self._pool_lock:
                await self._remove_expired()
                await self._try_promote_overflow()

    async def _may_refresh_peer(
        self,
        peer: PeerConnection,
        bind: str,
    ) -> None:
        client = peer.client
        if client.bind == bind:
            peer.refcount += 1
            return

        # bind changed – close old and reconnect
        conn_type = "overflow" if peer.ephemeral else "permanent"
        logger.info(
            "pool.refresh peer_id=%s old_bind=%s new_bind=%s",
            peer.peer_id, client.bind, bind,
        )
        await client.close()
        new = PeerClient(bind, self._tls_ctx, peer.fd, logger=logger)
        await asyncio.wait_for(new.connect(), timeout=self._connection_timeout)
        peer.client = new
        peer.refcount = 1
        self._telemetry.pool_evictions_total.add(
            1, attributes={"peer_id": peer.peer_id, "reason": "refresh"}
        )
        self._telemetry.pool_connections_total.add(
            1, attributes={"peer_id": peer.peer_id, "type": conn_type}
        )

    async def _bind_for(self, peer_id: str) -> str:
        topology = await self._topologymgr.snapshot()
        member = topology.registry.get(peer_id)
        if member is None:
            raise RuntimeError(f"unknown peer: {peer_id}")
        return member.peer_address

    async def _remove_expired(self) -> None:
        if len(self._permanents) <= self._min_size:
            expired_permanents: list[str] = []
        else:
            expired_permanents = [
                peer_id
                for peer_id, peer in self._permanents.items()
                if peer.refcount == 0 and peer.client.inactivity_duration >= self._idle_ttl
            ]

        expired_overflows = [
            peer_id
            for peer_id, peer in self._overflows.items()
            if peer.refcount == 0 and peer.client.inactivity_duration >= self._idle_ttl
        ]

        for peer_id in expired_permanents:
            await self._permanents[peer_id].client.close()
            await self._permanents[peer_id].fd.stop()
            del self._permanents[peer_id]
            self._lru.pop(peer_id, None)
            self._telemetry.pool_evictions_total.add(
                1, attributes={"peer_id": peer_id, "reason": "idle_expired"}
            )
            logger.debug("pool.reaper expire permanent peer_id=%s", peer_id)

        for peer_id in expired_overflows:
            await self._overflows[peer_id].client.close()
            await self._overflows[peer_id].fd.stop()
            del self._overflows[peer_id]
            self._telemetry.pool_evictions_total.add(
                1, attributes={"peer_id": peer_id, "reason": "idle_expired"}
            )
            logger.debug("pool.reaper expire overflow peer_id=%s", peer_id)

        if expired_permanents or expired_overflows:
            self._update_size_gauges()

    async def _try_promote_overflow(self) -> None:
        promoted = False
        while len(self._permanents) < self._max_size and len(self._overflows) > 0:
            promote_id = min(
                self._overflows,
                key=lambda pid: self._overflows[pid].client.inactivity_duration
            )
            peer = self._overflows.pop(promote_id)
            peer.ephemeral = False
            self._permanents[promote_id] = peer
            self._lru[promote_id] = peer
            self._telemetry.pool_promotions_total.add(1, attributes={"peer_id": promote_id})
            logger.debug("pool.reaper promote peer_id=%s", promote_id)
            promoted = True
        if promoted:
            self._update_size_gauges()

    def _update_size_gauges(self) -> None:
        self._telemetry.pool_size_permanent.set(len(self._permanents))
        self._telemetry.pool_size_overflow.set(len(self._overflows))

