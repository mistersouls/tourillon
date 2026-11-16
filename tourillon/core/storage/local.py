import asyncio
from contextlib import asynccontextmanager
from itertools import groupby
from typing import AsyncIterator, Mapping, Iterable, Sequence

from tourillon.core.models.ae import MAX_PAGE_LIMIT

from tourillon.core.models.storageschema import (
    PartitionAddress, Record, Pid, Key,
    TagKind, Keyspace, Obligation
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports import Telemetry
from tourillon.core.ports.storage import Storage
from tourillon.core.storage.cell import DigestCell
from tourillon.core.storage.session import Session, SessionProvider, DEFAULT_BUCKET_SHIFT
from tourlib.models.ts import HLCTimestamp
from tourillon.core.log import logger as _logger

logger = _logger.storage()

SCAN_PAGE = 4096

class LocalStore:
    def __init__(
        self,
        storage: Storage,
        partitioner: Partitioner,
        telemetry: Telemetry,
        bucket_shift: int = DEFAULT_BUCKET_SHIFT
    ) -> None:
        self._storage = storage
        self._partitioner = partitioner
        self._telemetry = telemetry
        self._bucket_shift = bucket_shift

    @property
    def bucket_shift(self) -> int:
        return self._bucket_shift

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

    async def apply(self, records: Sequence[Record]) -> list[bool]:
        results: list[bool] = []

        for group, members in groupby(
            records,
            key=lambda r: self._partitioner.group_for(r.key.pid.value)
        ):
            batch = list(members)
            with self._telemetry.start_span("storage.transaction"):
                async with self._session(group, write=True) as session:
                    attrs = {"group": group, "count": len(batch)}
                    with self._telemetry.start_span("storage.apply_records", attributes=attrs):
                        results.extend(await session.put_record_many(batch))

        return results

    async def read_records(self, keys: Sequence[Key], max_bytes: int) -> tuple[list[Record], int]:
        records: list[Record] = []
        size: int = 0
        consumed: int = 0

        for group, members in groupby(
            keys,
            key=lambda k: self._partitioner.group_for(k.pid.value)
        ):
            batch = list(members)
            async with self._session(group) as session:
                found = await session.get_record_many(batch)

            found_by_key = {record.key: record for record in found}

            for key in batch:
                record = found_by_key.get(key)

                if record is not None and record.tag.owned():
                    cost = record.size()

                    if records and size + cost > max_bytes:
                        return records, consumed

                    records.append(record)
                    size += cost

                consumed += 1

        return records, consumed

    async def list_obligations(self, after: bytes | None = None, limit: int = 64) -> list[Obligation]:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

        obligations: list[Obligation] = []
        for group in range(self._partitioner.total_groups):
            async with self._session(group) as session:
                obligations.extend(await session.obligation_keys(after, limit))

        return sorted(obligations, key=lambda o: o.to_bytes())[:limit]

    async def record_from_obligations(self, obligations: Sequence[Obligation]) -> dict[Key, Record]:
        found: dict[Key, Record] = {}
        by_address: dict[PartitionAddress, Record | None] = {}

        for obligation in obligations:
            address = obligation.address
            if address not in by_address:
                async with self._pid_session(address.pid.value) as session:
                    by_address[address] = await session.get_last_record(address)

            record = by_address[address]
            if record is not None and record.key.ts >= obligation.ts:
                found[obligation.key] = record

        return found

    async def remove_obligations(self, obligations: Sequence[Obligation]) -> None:
        ordered = sorted(obligations, key=lambda o: self._partitioner.group_for(o.key.pid.value))
        for group, members in groupby(
            ordered, key=lambda o: self._partitioner.group_for(o.key.pid.value)
        ):
            async with self._session(group, write=True) as session:
                await session.remove_obligation_many(list(members))

    def foreign(self, record: Record) -> bool:
        """Whether a record is filed under a pid its address does not hash to."""
        expected = self._partitioner.pid_for_hash(
            self._partitioner.space.hash(record.key.address.to_bytes())
        )

        if expected == record.key.pid.value:
            return False

        logger.warning(
            "ae.foreign version: pid=%s expected=%s key=%s",
            record.key.pid.value,
            expected,
            record.key.to_bytes(Keyspace.TAGS).hex(),
        )

        return True

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

    async def list_buckets(
        self,
        pid: int,
        after: int | None = None,
        limit: int = 64
    ) -> list[tuple[int, DigestCell]]:
        async with self._pid_session(pid=pid) as session:
            return await session.list_buckets(pid=pid, after=after, limit=limit)

    async def buckets_until(
        self,
        pid: int,
        after: int | None = None,
        upto: int | None = None,
        limit: int = MAX_PAGE_LIMIT
    ) -> dict[int, DigestCell]:
        cells: dict[int, DigestCell] = {}
        cursor = after

        async with self._pid_session(pid=pid) as session:
            while True:
                page = await session.list_buckets(pid, cursor, limit)
                more = len(page) == limit

                for bucket, cell in page:
                    if upto is not None and bucket > upto:
                        return cells

                    cells[bucket] = cell

                if not more:
                    break

                cursor = page[-1][0]

            return cells

    async def keys(
        self,
        start: Key | None = None,
        stop: Key | None = None,
        limit: int = MAX_PAGE_LIMIT,
        include: list[TagKind] | None = None,
    ) -> list[Key]:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

        start_pid = start.pid.value if start is not None else 0
        stop_pid = stop.pid.value if stop is not None else self._partitioner.total_partitions - 1
        if not (0 <= start_pid <= stop_pid < self._partitioner.total_partitions):
            raise ValueError(f"Invalid start/stop PIDs: {start_pid}, {stop_pid}")

        selected: list[Key] = []
        cursor = start.to_bytes(Keyspace.TAGS) if start is not None else None
        upper = stop.to_bytes(Keyspace.TAGS) if stop is not None else None
        kinds = None if include is None else frozenset(include)

        for group, _ in groupby(range(start_pid, stop_pid + 1), key=self._partitioner.group_for):
            async with self._session(group) as session:
                remaining = limit - len(selected)
                selected.extend(
                    await self._keys_in_group(session, cursor, upper, remaining, kinds)
                )

            if len(selected) == limit:
                break

        return selected

    async def range_digest(self, start: Key, stop: Key) -> DigestCell:
        if start.pid != stop.pid:
            raise ValueError(
                f"start and stop keys must have the same PID: {start.pid.value}, {stop.pid.value}"
            )

        cell = DigestCell()
        cursor: bytes | None = start.to_tag_bytes()
        limit = SCAN_PAGE

        async with self._pid_session(start.pid.value) as session:
            while cursor is not None:
                page = await session.list_tags(start=cursor, limit=limit)
                cursor = page[-1].key.to_tag_bytes() if len(page) == limit else None

                for tagged in page:
                    if tagged.key.to_tag_bytes() >= stop.to_tag_bytes():
                        break

                    if tagged.tag.owned():
                        cell = cell + DigestCell.of(tagged.key)

                await asyncio.sleep(0)

            return cell

    @asynccontextmanager
    async def _pid_session(self, pid: int, write: bool = False) -> AsyncIterator[Session]:
        """Create a session for a given partition ID."""
        group = self._partitioner.group_for(pid)
        async with self._session(group, write=write) as session:
            yield session

    @asynccontextmanager
    async def _session(self, group: int, write: bool = False) -> AsyncIterator[Session]:
        """Create a session for a given group ID."""
        provider = SessionProvider(self._storage, self._bucket_shift)
        async with provider.session(group, write=write) as session:
            yield session

    @staticmethod
    async def _keys_in_group(
        session: Session,
        start: bytes | None,
        stop: bytes | None,
        limit: int,
        include: frozenset[TagKind] | None
    ) -> list[Key]:
        if start is not None and stop is not None and start >= stop:
            raise ValueError(f"start must be strictly les than stop")

        selected: list[Key] = []
        cursor = start

        while True:
            remaining = limit - len(selected)
            page = await session.list_tags(start=cursor, limit=remaining)
            if not page:
                return selected

            for item in page:
                cursor = item.key.to_bytes(Keyspace.TAGS)
                if stop is not None and cursor >= stop:
                    return selected

                if include is None or item.tag.kind in include:
                    selected.append(item.key)

            if len(selected) == limit:
                return selected
