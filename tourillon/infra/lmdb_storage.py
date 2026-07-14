import asyncio

from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.storage.store import PartitionStore
from tourillon.infra.lmdb_backend import LmdbBackendStorage, LmdbConfig


class LMDBStorage:
    def __init__(self, conf: LmdbConfig, partitioner: Partitioner) -> None:
        self._conf = conf
        self._partitioner = partitioner
        self._backends: dict[int, LmdbBackendStorage] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def open_by_pid(self, pid: int) -> PartitionStore:
        """Open a partition store for the given partition ID."""
        backend = await self._backend_for(pid)
        return PartitionStore(pid, backend)

    async def close(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for segment, backend in self._backends.items():
                tg.create_task(
                    backend.close(),
                    name=f"close-backend-segment-{segment}"
                )

        self._backends.clear()

    async def _backend_for(self, pid: int) -> LmdbBackendStorage:
        segment = self._partitioner.segment_for(pid)
        async with self._lock:
            if segment not in self._backends:
                self._backends[segment] = LmdbBackendStorage(self._conf, str(segment))
            return self._backends[segment]
