from contextlib import asynccontextmanager
from itertools import groupby
from typing import AsyncIterator, Mapping, Iterable

from tourillon.core.models.storageschema import PartitionAddress, Record, Pid
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports import Telemetry
from tourillon.core.ports.storage import Storage
from tourillon.core.storage.cell import DigestCell
from tourillon.core.storage.session import Session, SessionProvider
from tourlib.models.ts import HLCTimestamp


class LocalStore:
    def __init__(
        self,
        storage: Storage,
        partitioner: Partitioner,
        telemetry: Telemetry
    ) -> None:
        self._storage = storage
        self._partitioner = partitioner
        self._telemetry = telemetry

    async def timestamp(self) -> HLCTimestamp:
        latest = HLCTimestamp(wall_ms=0, counter=0, node_id="")
        for group in range(self._partitioner.total_groups):
            lo, hi = self._partitioner.group_bounds(group)
            async with self._session(group) as session:
                tags = await session.last_tags_many(Pid(pid) for pid in range(lo, hi))
                ts = max((kt.key.ts for kt in tags), default=latest)
                latest = max(latest, ts)

        return latest

    async def get(self, address: PartitionAddress) -> Record | None:
        with self._telemetry.start_span("storage.transaction"):
            async with self._pid_session(address.pid.value) as session:
                with self._telemetry.start_span("storage.get_record"):
                    return await session.get_last_record(address)

    async def put(self, record: Record) -> bool:
        if not record.tag.owned():
            raise ValueError(f"Cannot put record with non-owned tag: {record.tag.kind}")

        with self._telemetry.start_span("storage.transaction"):
            async with self._pid_session(record.key.pid.value, write=True) as session:
                with self._telemetry.start_span("storage.put_record"):
                    return await session.put_record(record)

    async def roots(self, pids: Iterable[int]) -> Mapping[int, DigestCell]:
        """Get the root digest cells for all partitions."""
        cells: dict[int, DigestCell] = {}

        for group, batch in groupby(sorted(pids), key=self._partitioner.group_for):
            async with self._session(group) as session:
                cells.update(await session.roots(batch))

        return cells

    async def list_roots(self, after: int | None = None, limit: int = 64) -> list[tuple[int, DigestCell]]:
        """List root digest cells for partitions in ascending order."""
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

        if after is not None and after >= self._partitioner.total_partitions - 1:
            return []

        roots: list[tuple[int, DigestCell]] = []
        cursor = 0 if after is None else self._partitioner.group_for(after + 1)

        for group in range(self._partitioner.total_groups):
            while len(roots) < limit:
                async with self._session(group) as session:
                    remaining = limit - len(roots)
                    page = await session.list_roots(after=cursor, limit=remaining)
                    more = len(page) == remaining

                roots.extend(page)

                if page:
                    cursor = page[-1][0]

                if not more:
                    break

            if len(roots) >= limit:
                break

        return roots

    @asynccontextmanager
    async def _pid_session(self, pid: int, write: bool = False) -> AsyncIterator[Session]:
        """Create a session for a given partition ID."""
        group = self._partitioner.group_for(pid)
        async with self._session(group, write=write) as session:
            yield session

    @asynccontextmanager
    async def _session(self, group: int, write: bool = False) -> AsyncIterator[Session]:
        """Create a session for a given group ID."""
        provider = SessionProvider(self._storage)
        async with provider.session(group, write=write) as session:
            yield session
