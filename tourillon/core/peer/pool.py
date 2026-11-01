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
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import describe
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
    retired_clients: dict[int, tuple[EnvelopeClient, int]] = field(default_factory=dict)


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
        priority_size: int = 2,
        idle_ttl: float = 60 * 10,
        connection_timeout: float = ENVELOPE_CONNECTION_TIMEOUT,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if priority_size < 0:
            raise ValueError("priority_size must be >= 0")
        if idle_ttl <= 0:
            raise ValueError("idle_ttl must be > 0")

        self._tls_ctx = tls_ctx
        self._topologymgr = topologymgr
        self._fd_provider = fd_provider
        self._min_size = min_size
        self._max_size = max_size
        self._max_overflow = max_overflow
        self._priority_size = priority_size
        self._idle_ttl = idle_ttl
        self._connection_timeout = connection_timeout
        self._telemetry = telemetry

        self._permanents: dict[str, PeerConnection] = {}
        self._overflows: dict[str, PeerConnection] = {}
        self._priority_reserve: dict[str, PeerConnection] = {}

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
        if peer_id in self._priority_reserve:
            return self._priority_reserve[peer_id].fd
        return None

    async def start(self) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            loop = asyncio.get_running_loop()
            self._reaper_task = loop.create_task(
                self._idle_reaper(), name="client-pool-idle-reaper"
            )
            logger.debug("Started client connect pool")

    async def acquire(
        self,
        peer_id: str,
        *,
        priority: bool = False,
    ) -> EnvelopeClient:
        try:
            async with self._pool_lock:
                return await self._acquire_locked(peer_id, priority=priority)
        except (TimeoutError, OSError) as exc:
            # The caller reports the failure with its own context; the pool
            # only counts it and keeps the connection detail at DEBUG.
            reason = "timeout" if isinstance(exc, TimeoutError) else "connect"
            self._telemetry.pool_acquire_errors_total.add(
                1, attributes={"reason": reason}
            )
            logger.debug(
                "pool.acquire failed peer_id=%s reason=%s error=%s",
                peer_id,
                reason,
                describe(exc),
            )
            raise

    @contextlib.asynccontextmanager
    async def lease(
        self, peer_id: str, *, priority: bool = False
    ) -> AsyncIterator[EnvelopeClient]:
        """Acquire a peer client for a context and always release it on exit."""
        client = await self.acquire(peer_id, priority=priority)
        try:
            yield client
        finally:
            await self.release(peer_id, client)

    async def _acquire_locked(self, peer_id: str, *, priority: bool) -> EnvelopeClient:
        bind = await self._bind_for(peer_id)
        peer = self._find_peer(peer_id)
        if peer is not None:
            await self._may_refresh_peer(peer, bind)
            if peer_id in self._permanents:
                self._lru.move_to_end(peer_id)
            return peer.client

        if len(self._permanents) < self._max_size:
            peer = await self._create_connected_peer(peer_id, bind)
            peer.refcount = 1
            self._permanents[peer_id] = peer
            self._lru[peer_id] = peer
            self._telemetry.pool_connections_total.add(
                1, attributes={"type": "permanent"}
            )
            self._update_size_gauges()
            logger.info("pool.acquire new permanent peer_id=%s bind=%s", peer_id, bind)
            return peer.client

        if len(self._overflows) < self._max_overflow:
            return await self._create_overflow(peer_id, bind)

        victim_id = self._idle_lru_peer()
        if victim_id is not None:
            await self._evict_permanent(victim_id)
            peer = await self._create_connected_peer(peer_id, bind)
            peer.refcount = 1
            self._permanents[peer_id] = peer
            self._lru[peer_id] = peer
            self._telemetry.pool_connections_total.add(
                1, attributes={"type": "permanent"}
            )
            self._update_size_gauges()
            return peer.client

        if priority:
            if len(self._priority_reserve) >= self._priority_size:
                raise TimeoutError(
                    "peer pool priority reserve is full; "
                    f"no priority connection available for {peer_id}"
                )
            return await self._create_priority_overflow(peer_id, bind)

        raise TimeoutError(
            f"peer pool is saturated; no idle connection available for {peer_id}"
        )

    def _find_peer(self, peer_id: str) -> PeerConnection | None:
        return (
            self._permanents.get(peer_id)
            or self._overflows.get(peer_id)
            or self._priority_reserve.get(peer_id)
        )

    async def _create_overflow(self, peer_id: str, bind: str) -> EnvelopeClient:
        peer = await self._create_connected_peer(peer_id, bind)
        peer.ephemeral = True
        peer.refcount = 1
        self._overflows[peer_id] = peer
        self._telemetry.pool_connections_total.add(1, attributes={"type": "overflow"})
        self._update_size_gauges()
        logger.debug("pool.acquire new overflow peer_id=%s bind=%s", peer_id, bind)
        return peer.client

    async def _create_priority_overflow(
        self, peer_id: str, bind: str
    ) -> EnvelopeClient:
        peer = await self._create_connected_peer(peer_id, bind)
        peer.ephemeral = True
        peer.refcount = 1
        self._priority_reserve[peer_id] = peer
        self._telemetry.pool_connections_total.add(1, attributes={"type": "overflow"})
        self._update_size_gauges()
        logger.debug("pool.acquire new priority overflow peer_id=%s", peer_id)
        return peer.client

    def _idle_lru_peer(self) -> str | None:
        return next(
            (
                peer_id
                for peer_id, peer in self._lru.items()
                if peer.refcount == 0 and not peer.retired_clients
            ),
            None,
        )

    async def _evict_permanent(self, peer_id: str) -> None:
        peer = self._permanents.pop(peer_id)
        self._lru.pop(peer_id, None)
        await peer.client.close()
        await peer.fd.stop()
        self._telemetry.pool_evictions_total.add(1, attributes={"reason": "lru_evict"})
        logger.debug("pool.acquire evicted idle permanent peer_id=%s", peer_id)

    async def release(self, peer_id: str, client: EnvelopeClient) -> None:
        async with self._pool_lock:
            peer = self._find_peer(peer_id)
            if peer is None:
                return

            if client is not peer.client:
                await self._release_retired(peer, client)
                return

            peer.refcount -= 1
            logger.debug(
                "pool.release peer_id=%s refcount=%d ephemeral=%s",
                peer_id,
                peer.refcount,
                peer.ephemeral,
            )

            # auto-delete overflow
            if peer.ephemeral and peer.refcount == 0 and not peer.retired_clients:
                await self._remove_ephemeral(peer)

    async def _release_retired(
        self, peer: PeerConnection, client: EnvelopeClient
    ) -> None:
        retired = peer.retired_clients.get(id(client))
        if retired is None or retired[0] is not client:
            return

        remaining = retired[1] - 1
        if remaining:
            peer.retired_clients[id(client)] = (client, remaining)
        else:
            del peer.retired_clients[id(client)]
            await client.close()

        if peer.ephemeral and peer.refcount == 0 and not peer.retired_clients:
            await self._remove_ephemeral(peer)

    async def _remove_ephemeral(self, peer: PeerConnection) -> None:
        self._overflows.pop(peer.peer_id, None)
        self._priority_reserve.pop(peer.peer_id, None)
        await peer.client.close()
        await peer.fd.stop()
        self._telemetry.pool_evictions_total.add(
            1, attributes={"reason": "overflow_release"}
        )
        self._update_size_gauges()
        logger.debug("pool.release overflow closed peer_id=%s", peer.peer_id)

    async def shutdown(self) -> None:
        if self._reaper_task is not None and not self._reaper_task.done():
            self._reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper_task
            self._reaper_task = None
            logger.debug("Stopped client connect pool")

        async with self._pool_lock:
            peers = (
                list(self._permanents.values())
                + list(self._overflows.values())
                + list(self._priority_reserve.values())
            )

            for peer in peers:
                await peer.client.close()
                for retired_client, _ in peer.retired_clients.values():
                    await retired_client.close()
                await peer.fd.stop()
                self._telemetry.pool_evictions_total.add(
                    1, attributes={"reason": "pool_close"}
                )
                logger.debug("pool.close evict peer_id=%s", peer.peer_id)

            self._permanents.clear()
            self._overflows.clear()
            self._priority_reserve.clear()
            self._update_size_gauges()

    async def _create_connected_peer(self, peer_id: str, bind: str) -> PeerConnection:
        fd = self._fd_provider.create(peer_id=peer_id)
        client = PeerClient(
            bind,
            self._tls_ctx,
            fd=fd,
            logger=logger,
            context=self._telemetry.inject_context,
        )
        connected = False
        try:
            await asyncio.wait_for(client.connect(), timeout=self._connection_timeout)
            connected = True
        finally:
            if not connected:
                await client.close()
                await fd.stop()
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

        if peer.retired_clients:
            raise TimeoutError(
                f"peer {peer.peer_id} still has leases on its previous address"
            )

        # Connect first; active callers keep using the old client until release.
        conn_type = "overflow" if peer.ephemeral else "permanent"
        logger.info(
            "pool.refresh peer_id=%s old_bind=%s new_bind=%s",
            peer.peer_id,
            client.bind,
            bind,
        )
        new = PeerClient(
            bind,
            self._tls_ctx,
            peer.fd,
            logger=logger,
            context=self._telemetry.inject_context,
        )
        connected = False
        try:
            await asyncio.wait_for(new.connect(), timeout=self._connection_timeout)
            connected = True
        finally:
            if not connected:
                await new.close()

        if peer.refcount:
            peer.retired_clients[id(client)] = (client, peer.refcount)
        else:
            await client.close()
        peer.client = new
        peer.refcount = 1
        self._telemetry.pool_evictions_total.add(1, attributes={"reason": "refresh"})
        self._telemetry.pool_connections_total.add(1, attributes={"type": conn_type})

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
                if peer.refcount == 0
                and not peer.retired_clients
                and peer.client.inactivity_duration >= self._idle_ttl
            ]

        expired_overflows = [
            peer_id
            for peer_id, peer in self._overflows.items()
            if peer.refcount == 0
            and not peer.retired_clients
            and peer.client.inactivity_duration >= self._idle_ttl
        ]
        expired_priorities = [
            peer_id
            for peer_id, peer in self._priority_reserve.items()
            if peer.refcount == 0
            and not peer.retired_clients
            and peer.client.inactivity_duration >= self._idle_ttl
        ]

        for peer_id in expired_permanents:
            await self._permanents[peer_id].client.close()
            await self._permanents[peer_id].fd.stop()
            del self._permanents[peer_id]
            self._lru.pop(peer_id, None)
            self._telemetry.pool_evictions_total.add(
                1, attributes={"reason": "idle_expired"}
            )
            logger.debug("pool.reaper expire permanent peer_id=%s", peer_id)

        for peer_id in expired_overflows:
            await self._overflows[peer_id].client.close()
            await self._overflows[peer_id].fd.stop()
            del self._overflows[peer_id]
            self._telemetry.pool_evictions_total.add(
                1, attributes={"reason": "idle_expired"}
            )
            logger.debug("pool.reaper expire overflow peer_id=%s", peer_id)

        for peer_id in expired_priorities:
            peer = self._priority_reserve.pop(peer_id)
            await peer.client.close()
            await peer.fd.stop()
            self._telemetry.pool_evictions_total.add(
                1, attributes={"reason": "idle_expired"}
            )
            logger.debug("pool.reaper expire priority reserve peer_id=%s", peer_id)

        if expired_permanents or expired_overflows or expired_priorities:
            self._update_size_gauges()

    async def _try_promote_overflow(self) -> None:
        promoted = False
        while len(self._permanents) < self._max_size and len(self._overflows) > 0:
            promote_id = min(
                self._overflows,
                key=lambda pid: self._overflows[pid].client.inactivity_duration,
            )
            peer = self._overflows.pop(promote_id)
            peer.ephemeral = False
            self._permanents[promote_id] = peer
            self._lru[promote_id] = peer
            self._telemetry.pool_promotions_total.add(1)
            logger.debug("pool.reaper promote peer_id=%s", promote_id)
            promoted = True
        if promoted:
            self._update_size_gauges()

    def _update_size_gauges(self) -> None:
        self._telemetry.pool_size_permanent.set(len(self._permanents))
        self._telemetry.pool_size_overflow.set(
            len(self._overflows) + len(self._priority_reserve)
        )
