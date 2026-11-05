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

"""Local storage operations of anti-entropy.

Everything here is bounded so anti-entropy never competes unfairly with
client traffic:

- reads use read-only sessions, which never wait for the group writer;
- every scan is paged, one short read transaction per page, with an
  event-loop yield in between;
- writes go through ``GroupSession.put_many`` in small batches, one write
  transaction each, so the single writer of a group is held for one batch at
  a time and client writes interleave between batches.

``put_many`` is idempotent and never overwrites a stored version, so
applying a version that is already present, or applying it twice, changes
nothing.

Replication obligations (META) are read and removed here too: they are the
durable work list of the delivery worker.
"""

import asyncio
from collections.abc import Callable, Sequence
from itertools import groupby

from tourillon.core.log import logger
from tourillon.core.models.antientropy import record_size
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    ObligationKey,
    PartitionAddress,
    Record,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Storage
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import (
    OWNED_KINDS,
    DigestCell,
    GroupSessionManager,
)

# Records per write transaction. Small on purpose: the group writer is shared
# with client writes and must be released often.
APPLY_BATCH = 64
# Value bytes per write transaction, on top of the record count.
APPLY_BATCH_BYTES = 8 * 1024 * 1024
# Keys read per page when a range digest must be computed by scanning.
SCAN_PAGE = 4096


class LocalStore:
    """Anti-entropy view of the local partition groups."""

    def __init__(
        self,
        storage: Storage,
        partitioner: Partitioner,
        notify: Callable[[PartitionAddress], None],
        retention: RetentionPolicy,
    ) -> None:
        self._storage = storage
        self._retention = retention
        self._partitioner = partitioner
        if not callable(notify):
            raise ValueError("GC notification callback is required")
        self._notify = notify

    @property
    def retention_versions(self) -> int:
        """Policy identifier carried by AE scopes, not an admission decision."""
        return self._retention.versions

    async def roots(self, pids: Sequence[int]) -> dict[int, DigestCell]:
        """Read the root cell of each pid, one read session per group."""
        cells: dict[int, DigestCell] = {}

        for group, members in groupby(sorted(pids), key=self._partitioner.group_for):
            batch = list(members)

            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                values = await session.roots(batch)

            cells.update(zip(batch, values, strict=True))

        return cells

    async def root_page(
        self, after: int | None, limit: int
    ) -> tuple[list[tuple[int, DigestCell]], int | None]:
        """List non-empty local roots after an exclusive pid, across groups.

        Groups hold contiguous pid ranges, so walking them in order yields
        pids in ascending order. Returns None as cursor at the end.
        """
        roots: list[tuple[int, DigestCell]] = []
        first = 0 if after is None else self._partitioner.group_for(after + 1)
        cursor = after

        for group in range(first, self._partitioner.total_groups):

            while len(roots) < limit:
                provider = await self._storage.session_provider_for(group)
                manager = GroupSessionManager(provider)
                async with manager.session() as session:
                    page, more = await session.root_page(cursor, limit - len(roots))

                roots.extend(page)

                if page:
                    cursor = page[-1][0]

                if more is None:
                    break

            if len(roots) >= limit:
                return roots, roots[-1][0]

        return roots, None

    async def bucket_page(
        self, pid: int, after: int | None, limit: int
    ) -> tuple[list[tuple[int, DigestCell]], int | None]:
        """One page of the bucket cells of ``pid`` after an exclusive cursor."""
        group = self._partitioner.group_for(pid)

        provider = await self._storage.session_provider_for(group)
        manager = GroupSessionManager(provider)
        async with manager.session() as session:
            return await session.bucket_page(pid, after, limit)

    async def buckets_until(
        self, pid: int, after: int | None, upto: int | None, limit: int
    ) -> dict[int, DigestCell]:
        """Every bucket cell of ``pid`` in ``(after, upto]`` (to the end if None)."""
        cells: dict[int, DigestCell] = {}
        cursor = after

        while True:
            page, more = await self.bucket_page(pid, cursor, limit)

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
        group = self._group_of(start)
        cell = DigestCell()
        cursor: bytes | None = start

        while cursor is not None:
            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                keys, cursor = await session.owned_keys(cursor, stop, SCAN_PAGE)

            for key in keys:
                cell = cell + DigestCell.of(key)

            await asyncio.sleep(0)

        return cell

    async def keys(
        self, start: bytes, stop: bytes, limit: int
    ) -> tuple[list[Key], bytes | None]:
        group = self._group_of(start)
        provider = await self._storage.session_provider_for(group)
        manager = GroupSessionManager(provider)
        async with manager.session() as session:
            keys, cursor = await session.owned_keys(start, stop, limit)

        for key in keys:
            self.notify(key)

        return keys, cursor

    def notify(self, key: Key) -> None:
        self._notify(PartitionAddress(key.pid, key.address))

    async def read(
        self, pid: int, keys: Sequence[Key], max_bytes: int
    ) -> tuple[list[Record], int]:
        """Read owned versions of ``keys`` in order, up to ``max_bytes``.

        Returns the records and how many keys were consumed; always consumes
        at least one key so callers progress even past one oversized value.
        Versions no longer present or not owned are consumed silently.
        """
        group = self._partitioner.group_for(pid)
        records: list[Record] = []
        consumed = 0
        size = 0

        for start in range(0, len(keys), APPLY_BATCH):
            chunk = keys[start : start + APPLY_BATCH]

            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                found = await session.get_many(chunk)

            for record in found:
                if record is not None and record.tag.kind in OWNED_KINDS:
                    cost = record_size(record)

                    if records and size + cost > max_bytes:
                        return records, consumed

                    records.append(record)
                    size += cost
                consumed += 1

        return records, consumed

    async def apply(self, pid: int, records: Sequence[Record]) -> int:
        """Write owned versions in small transactions; return how many were new."""
        result = await self.apply_result(pid, records)
        return result["applied"]

    async def apply_result(self, pid: int, records: Sequence[Record]) -> dict[str, int]:
        group = self._partitioner.group_for(pid)
        result = {"applied": 0, "already_present": 0, "ignored_retention": 0}

        for batch in _batches(records):
            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session(write=True) as session:
                outcomes = await self._retention.apply(session, batch)
                for disposition in outcomes:
                    result[disposition] += 1

            for record in batch:
                self.notify(record.key)
            await asyncio.sleep(0)

        return result

    async def latest_records(
        self, obligations: Sequence[ObligationKey]
    ) -> dict[Key, Record]:
        """Materialize one latest record per address, mapped to captured debts."""
        found: dict[Key, Record] = {}
        by_address: dict[PartitionAddress, Record | None] = {}

        for obligation in obligations:
            address = obligation.address
            if address not in by_address:
                group = self._partitioner.group_for(address.pid.value)
                provider = await self._storage.session_provider_for(group)
                manager = GroupSessionManager(provider)
                async with manager.session() as session:
                    by_address[address] = await session.get(address)

            record = by_address[address]
            if record is not None and record.key.ts >= obligation.required_ts:
                found[obligation.key] = record

        return found

    async def records(self, keys: Sequence[Key]) -> dict[Key, Record]:
        """Read the owned versions of ``keys``, any group; absent ones are omitted."""
        found: dict[Key, Record] = {}

        for group, members in groupby(
            sorted(keys, key=_pid_of),
            key=lambda k: self._partitioner.group_for(_pid_of(k)),
        ):
            batch = list(members)

            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                values = await session.get_many(batch)

            for key, record in zip(batch, values, strict=True):
                if record is not None and record.tag.kind in OWNED_KINDS:
                    found[key] = record

        return found

    async def obligation_page(
        self, group: int, after: bytes | None, limit: int, target: str | None = None
    ) -> tuple[list[ObligationKey], bytes | None]:
        """One page of the obligations persisted in ``group``, for one target or all."""
        provider = await self._storage.session_provider_for(group)
        manager = GroupSessionManager(provider)
        async with manager.session() as session:
            return await session.obligation_page(after, limit, target)

    async def remove_obligations(self, obligations: Sequence[ObligationKey]) -> None:
        """Delete obligations, one write transaction per group."""
        for group, members in groupby(
            sorted(obligations, key=lambda ob: _pid_of(ob.key)),
            key=lambda ob: self._partitioner.group_for(_pid_of(ob.key)),
        ):

            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session(write=True) as session:
                await session.remove_obligations(list(members))

    async def has_obligations(self) -> bool:
        """Whether any group still persists a replication obligation."""
        for group in range(self._partitioner.total_groups):

            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                if await session.has_obligations():
                    return True

        return False

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

    def _group_of(self, tags_key: bytes) -> int:
        return self._partitioner.group_for(int.from_bytes(tags_key[:4], "big"))


def _pid_of(key: Key) -> int:
    return key.pid.value


def _batches(records: Sequence[Record]) -> list[list[Record]]:
    batches: list[list[Record]] = []
    current: list[Record] = []
    size = 0

    for record in records:
        cost = record_size(record)

        if current and (len(current) == APPLY_BATCH or size + cost > APPLY_BATCH_BYTES):
            batches.append(current)
            current, size = [], 0

        current.append(record)
        size += cost

    if current:
        batches.append(current)

    return batches
