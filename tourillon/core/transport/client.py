import asyncio
import logging
import ssl

from tourlib.transport import TcpClient

_DEFAULT_CONNECT_TIMEOUT: float = 10.0

logger = logging.getLogger(__name__)


class PeerClientPool:
    """Persistent TcpClient pool, one client per node_id.

    Shared across gossip, rebalance, and replication. At most one active
    TcpClient exists per node_id: concurrent acquire() callers for the same
    node_id serialise on a per-node asyncio.Lock.

    On unexpected disconnection (is_connected == False) the stale client is
    closed and a fresh one is created at the next acquire(). On connection
    failure the exception propagates to the caller — the pool never masks
    network errors.
    """

    def __init__(
        self,
        ssl_ctx: ssl.SSLContext | None = None,
        connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT,
    ) -> None:
        self._ssl_ctx = ssl_ctx
        self._connect_timeout = connect_timeout
        self._clients: dict[str, TcpClient] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._pool_lock = asyncio.Lock()

    async def acquire(self, node_id: str, address: str) -> TcpClient:
        """Return the existing TcpClient for node_id, or create a new one.

        If the client is present and is_connected, return it immediately.
        Otherwise close the stale client (if any), create a fresh TcpClient,
        connect under asyncio.timeout(), and store it. Concurrency-safe: a
        per-node asyncio.Lock serialises concurrent callers for the same node_id.
        """
        node_lock = await self._get_or_create_lock(node_id)
        async with node_lock:
            client = self._clients.get(node_id)
            if client is not None and client.is_connected:
                return client
            if client is not None:
                await client.close()
            client = TcpClient()
            async with asyncio.timeout(self._connect_timeout):
                await client.connect(address, self._ssl_ctx)
            self._clients[node_id] = client
            logger.debug("Pool: created new TcpClient for %s at %s.", node_id, address)
            return client

    async def release(self, node_id: str) -> None:
        """Close and remove the TcpClient for node_id.

        Called when a node completes DRAINING → IDLE to prevent a stale
        client from persisting in the pool.
        """
        node_lock = await self._get_or_create_lock(node_id)
        async with node_lock:
            client = self._clients.pop(node_id, None)
            if client is not None:
                await client.close()
                logger.debug("Pool: released TcpClient for %s.", node_id)

    async def close_all(self) -> None:
        """Close all TcpClients. Called at daemon shutdown."""
        async with self._pool_lock:
            node_ids = list(self._clients.keys())

        for node_id in node_ids:
            await self.release(node_id)
        logger.debug("Pool: closed all clients.")

    async def _get_or_create_lock(self, node_id: str) -> asyncio.Lock:
        """Return the per-node lock, creating it if absent."""
        async with self._pool_lock:
            if node_id not in self._locks:
                self._locks[node_id] = asyncio.Lock()
            return self._locks[node_id]
