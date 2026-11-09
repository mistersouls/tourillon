import asyncio
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from itertools import groupby

from tourillon.core.models.storageschema import (
    PID_SIZE,
    Key,
    Keyspace,
    ObligationKey,
    PartitionAddress,
    RawPrefix,
    Record,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Storage
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import (
    DEFAULT_BUCKET_SHIFT,
    OWNED_KINDS,
    DigestCell,
    SessionProvider,
)

# Records per write transaction. Small on purpose: the group writer is shared
# with client writes and must be released often.
APPLY_BATCH = 64
# Value bytes per write transaction, on top of the record count.
APPLY_BATCH_BYTES = 8 * 1024 * 1024
# Keys read per page when a range digest must be computed by scanning.
SCAN_PAGE = 4096


class Store:
    def __init__(
        self,
        storage: Storage,
        partitioner: Partitioner,
        retention: RetentionPolicy,
        bucket_shift: int = DEFAULT_BUCKET_SHIFT,
    ) -> None:
        self._storage = storage
        self._partitioner = partitioner
        self._retention = retention
        self._bucket_shift = bucket_shift

    @property
    def retention_versions(self):
        return self._retention.versions

    async def roots(self, pids: Sequence[int]) -> Mapping[int, DigestCell]:
        cells: dict[int, DigestCell] = {}

        for group, members in groupby(sorted(pids), key=self._partitioner.group_for):
            batch = list(members)
            async with self._session(group=group) as session:
                values = await session.roots(batch)

            cells.update(values)

        return cells

    async def list_root(
        self, after: int | None = None, limit: int = 64
    ) -> list[tuple[int, DigestCell]]:
        if limit < 1:
            raise ValueError("Limit must be greater than 0")

        roots: list[tuple[int, DigestCell]] = []
        first = 0 if after is None else self._partitioner.group_for(after + 1)
        cursor = after

        for group in range(first, self._partitioner.total_groups):
            while len(roots) < limit:
                async with self._session(group=group) as session:
                    remaining = limit - len(roots)
                    page = await session.list_roots(cursor, remaining)
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
        self, pid: int, after: int | None = None, limit: int = 64
    ) -> list[tuple[int, DigestCell]]:
        async with self._pid_session(pid=pid) as session:
            return await session.list_buckets(pid, after, limit)

    async def buckets_until(
        self,
        pid: int,
        after: int | None = None,
        upto: int | None = None,
        limit: int = 64,
    ) -> Mapping[int, DigestCell]:
        if limit < 1:
            raise ValueError("Limit must be greater than 0")

        cells: dict[int, DigestCell] = {}
        cursor = after

        while True:
            page = await self.list_buckets(pid, cursor, limit)
            more = page[-1][0] if len(page) == limit else None

            for bucket, cell in page:
                if upto is not None and bucket > upto:
                    return cells
                cells[bucket] = cell

            if more is None:
                return cells

            cursor = more
            await asyncio.sleep(0)

    async def range_digest(self, start: bytes, stop: bytes) -> DigestCell:
        """Fold the owned versions of a TAGS range into one digest cell.

        Used for the bucket that straddles the grace cutoff: its stored cell
        also counts versions too recent to compare, so the eligible part is
        recomputed by scanning instead.
        """
        cell = DigestCell()
        cursor: bytes | None = start

        while cursor is not None:
            keys = await self._owned_keys(cursor, stop, SCAN_PAGE)

            for key in keys:
                cell = cell + DigestCell.of(key)

            cursor = (
                keys[-1].to_bytes(Keyspace.TAGS) if len(keys) == SCAN_PAGE else None
            )
            await asyncio.sleep(0)

        return cell

    async def list_obligations(
        self, after: bytes | None = None, limit: int = 64
    ) -> list[ObligationKey]:
        if limit < 1:
            raise ValueError("Limit must be greater than 0")

        obligations: list[ObligationKey] = []
        cursor = after

        for group in range(self._partitioner.total_groups):
            while len(obligations) < limit:
                async with self._session(group=group) as session:
                    remaining = limit - len(obligations)
                    page = await session.obligation_keys(cursor, remaining)
                    more = len(page) == remaining

                obligations.extend(page)

                if page:
                    cursor = page[-1].to_bytes()

                if not more:
                    break

            if len(obligations) >= limit:
                break

        return obligations

    async def record_from_obligations(
        self, obligations: Sequence[ObligationKey]
    ) -> Mapping[Key, Record]:
        """Materialize one latest record per address, mapped to captured debts."""
        found: dict[Key, Record] = {}
        by_address: dict[PartitionAddress, Record | None] = {}

        for obligation in obligations:
            address = obligation.address
            if address not in by_address:
                async with self._pid_session(address.pid.value) as session:
                    by_address[address] = await session.get_last_record(address)

            record = by_address[address]
            if record is not None and record.key.ts >= obligation.required_ts:
                found[obligation.key] = record

        return found

    async def _owned_keys(self, start: bytes, stop: bytes, limit: int) -> list[Key]:
        """List owned version keys in the TAGS range ``(start, stop)``.

        ``start`` is an exclusive cursor and ``stop`` an exclusive upper bound;
        both begin with the 4-byte pid, which bounds the scan to one partition.
        Versions that are not LIVE or TOMBSTONE are skipped: they are not part
        of the digest.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        group = self._group_of(start)
        async with self._session(group=group) as session:
            page = await session.tags(
                prefix=RawPrefix(start[:PID_SIZE]),
                after=start,
                limit=limit,
            )

        keys: list[Key] = []

        for tagged in page:
            key = tagged.key
            if key.to_bytes(Keyspace.TAGS) >= stop:
                return keys

            tag = tagged.tag
            if tag.kind in OWNED_KINDS:
                keys.append(key)

        return keys

    def _group_of(self, tags_key: bytes) -> int:
        return self._partitioner.group_for(int.from_bytes(tags_key[:PID_SIZE], "big"))

    @asynccontextmanager
    async def _pid_session(self, pid: int, write: bool = False):
        group = self._partitioner.group_for(pid)
        async with self._session(group=group, write=write) as session:
            yield session

    @asynccontextmanager
    async def _session(self, group: int, write: bool = False):
        provider = SessionProvider(self._storage)
        async with provider.session(group=group, write=write) as session:
            yield session
