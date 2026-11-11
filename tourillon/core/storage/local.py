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

"""The single transactional entry point for local records and metadata."""

import asyncio
from collections import Counter
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from itertools import groupby

from tourillon.core.log import logger
from tourillon.core.models.repair import VersionLookup
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
from tourillon.core.ports.telemetry import Telemetry
from tourillon.core.storage.retention import RetentionPolicy, WriteDisposition
from tourillon.core.storage.session import (
    DEFAULT_BUCKET_SHIFT,
    OWNED_KINDS,
    DigestCell,
    Session,
    SessionProvider,
)
from tourillon.core.telemetry import build_noop_telemetry
from tourlib.models.ts import HLCTimestamp

# Records per write transaction. Small on purpose: the group writer is shared
# with client writes and must be released often.
APPLY_BATCH = 64
# Value bytes per write transaction, on top of the record count.
APPLY_BATCH_BYTES = 8 * 1024 * 1024
# Keys read per page when a range digest must be computed by scanning.
SCAN_PAGE = 4096


class LocalStore:
    def __init__(
        self,
        storage: Storage,
        partitioner: Partitioner,
        retention: RetentionPolicy,
        bucket_shift: int = DEFAULT_BUCKET_SHIFT,
        *,
        telemetry: Telemetry | None = None,
    ) -> None:
        self._storage = storage
        self._partitioner = partitioner
        self._retention = retention
        self._bucket_shift = bucket_shift
        self._telemetry = telemetry or build_noop_telemetry()

    @property
    def retention_versions(self) -> int:
        return self._retention.versions

    async def get(self, address: PartitionAddress) -> Record | None:
        async with self._pid_session(address.pid.value) as session:
            return await session.get_last_record(address)

    async def read_at(self, key: Key) -> Record | None:
        async with self._pid_session(key.pid.value) as session:
            return await session.read_record(key)

    async def lookup(self, key: Key) -> VersionLookup:
        async with self._pid_session(key.pid.value) as session:
            return await self._retention.lookup(session, key)

    async def put(
        self, record: Record, obligations: Sequence[ObligationKey] = ()
    ) -> bool:
        if record.tag.kind not in OWNED_KINDS:
            raise ValueError("stored records must be LIVE or TOMBSTONE")
        if any(ob.key != record.key for ob in obligations):
            raise ValueError("obligations must refer to the stored version")

        group = self._partitioner.group_for(record.key.pid.value)
        with self._telemetry.start_span(
            "storage.transaction", attributes={"group": group}
        ):
            async with self._session(group, write=True) as session:
                with self._telemetry.start_span("storage.record.put"):
                    written = await session.put_record(record)
                if obligations:
                    with self._telemetry.start_span("storage.obligations.put"):
                        await session.put_obligation_many(obligations)

        return written

    async def latest_hlc(self) -> HLCTimestamp:
        latest = HLCTimestamp(0, 0, "")
        for group in range(self._partitioner.total_groups):
            lo, hi = self._partitioner.group_bounds(group)
            async with self._session(group) as session:
                latest = max(latest, await session.latest_timestamp(range(lo, hi + 1)))

        return latest

    async def collect_page(
        self, group: int, after: bytes | None, limit: int, *, dry_run: bool = False
    ) -> tuple[list[Key], int, int]:
        self._validate_limit(limit)
        async with self._session(group, write=not dry_run) as session:
            keys = await session.keys(after=after, limit=limit)
            count, size = await self._retention.collect(session, keys, dry_run=dry_run)

        return keys, count, size

    async def collect_address(
        self, address: PartitionAddress, limit: int
    ) -> tuple[int, int, int]:
        self._validate_limit(limit)
        async with self._pid_session(address.pid.value, write=True) as session:
            keys = await session.oldest_keys(address, limit)
            count, size = await self._retention.collect(session, keys)

        return len(keys), count, size

    async def apply_result(self, pid: int, records: Sequence[Record]) -> dict[str, int]:
        if any(
            r.key.pid.value != pid or r.tag.kind not in OWNED_KINDS for r in records
        ):
            raise ValueError(
                "records must be owned versions of the requested partition"
            )

        counts = Counter(applied=0, already_present=0, ignored_retention=0)
        chunk: list[Record] = []
        size = 0
        for record in records:
            self.foreign(record)
            if chunk and (
                len(chunk) >= APPLY_BATCH or size + record.size() > APPLY_BATCH_BYTES
            ):
                counts.update(await self._apply_chunk(pid, chunk))
                chunk, size = [], 0
                await asyncio.sleep(0)
            chunk.append(record)
            size += record.size()

        if chunk:
            counts.update(await self._apply_chunk(pid, chunk))

        return dict(counts)

    async def _apply_chunk(
        self, pid: int, records: Sequence[Record]
    ) -> list[WriteDisposition]:
        async with self._pid_session(pid, write=True) as session:
            return await self._retention.apply(session, records)

    def foreign(self, record: Record) -> bool:
        expected = self._partitioner.pid_for_hash(
            self._partitioner.space.hash(record.key.address.to_bytes())
        )
        if expected == record.key.pid.value:
            return False

        logger.warning(
            "ae.foreign pid=%d expected=%d namespace=%s",
            record.key.pid.value,
            expected,
            record.key.address.namespace,
        )
        return True

    async def read(
        self, pid: int, keys: Sequence[Key], max_bytes: int
    ) -> tuple[list[Record], int]:
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if any(key.pid.value != pid for key in keys):
            raise ValueError("keys must belong to the requested partition")

        records: list[Record] = []
        consumed, size = 0, 0
        async with self._pid_session(pid) as session:
            found = await session.read_record_many(keys)

        for record in found:
            if record is not None and records and size + record.size() > max_bytes:
                break
            consumed += 1
            if record is not None and record.tag.kind in OWNED_KINDS:
                records.append(record)
                size += record.size()

        return records, consumed

    async def keys(
        self, start: bytes, stop: bytes, limit: int
    ) -> tuple[list[Key], bytes | None]:
        self._validate_limit(limit)
        async with self._session(self._group_of(start)) as session:
            page = await session.tags(
                prefix=RawPrefix(start[:PID_SIZE]), after=start, limit=limit
            )

        selected = [item for item in page if item.key.to_bytes(Keyspace.TAGS) < stop]
        cursor = (
            page[-1].key.to_bytes(Keyspace.TAGS)
            if len(page) == limit and len(selected) == len(page)
            else None
        )
        return [item.key for item in selected if item.tag.kind in OWNED_KINDS], cursor

    async def remove_obligations(self, obligations: Sequence[ObligationKey]) -> None:
        ordered = sorted(
            obligations, key=lambda ob: self._partitioner.group_for(ob.key.pid.value)
        )
        for group, members in groupby(
            ordered, key=lambda ob: self._partitioner.group_for(ob.key.pid.value)
        ):
            async with self._session(group, write=True) as session:
                await session.remove_obligation_many(list(members))

    async def has_obligations(self) -> bool:
        for group in range(self._partitioner.total_groups):
            async with self._session(group) as session:
                if await session.get_last_obligation() is not None:
                    return True
        return False

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")

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
        if after is not None and after >= self._partitioner.total_partitions - 1:
            return []
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
        self._validate_limit(limit)
        async with self._pid_session(pid=pid) as session:
            return await session.list_buckets(pid, after, limit)

    async def bucket_page(
        self, pid: int, after: int | None, limit: int
    ) -> tuple[list[tuple[int, DigestCell]], int | None]:
        page = await self.list_buckets(pid, after, limit)
        return page, page[-1][0] if len(page) == limit else None

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
            keys, cursor = await self.keys(cursor, stop, SCAN_PAGE)

            for key in keys:
                cell = cell + DigestCell.of(key)

            await asyncio.sleep(0)

        return cell

    async def list_obligations(
        self, after: bytes | None = None, limit: int = 64
    ) -> list[ObligationKey]:
        if limit < 1:
            raise ValueError("Limit must be greater than 0")

        obligations: list[ObligationKey] = []
        for group in range(self._partitioner.total_groups):
            async with self._session(group=group) as session:
                obligations.extend(await session.obligation_keys(after, limit))

        return sorted(obligations, key=lambda ob: ob.to_bytes())[:limit]

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

    def _group_of(self, tags_key: bytes) -> int:
        return self._partitioner.group_for(int.from_bytes(tags_key[:PID_SIZE], "big"))

    @asynccontextmanager
    async def _pid_session(
        self, pid: int, write: bool = False
    ) -> AsyncIterator[Session]:
        group = self._partitioner.group_for(pid)
        async with self._session(group=group, write=write) as session:
            yield session

    @asynccontextmanager
    async def _session(self, group: int, write: bool = False) -> AsyncIterator[Session]:
        provider = SessionProvider(self._storage)
        async with provider.session(
            group=group, write=write, bucket_shift=self._bucket_shift
        ) as session:
            yield session
