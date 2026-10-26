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

``put_many`` is idempotent, never downgrades an owned version and promotes a
local HINT or STAGING copy of the same version, so applying a version that
is already present, or applying it twice, changes nothing.
"""

import asyncio
from collections.abc import Sequence
from itertools import groupby

from tourillon.core.log import logger
from tourillon.core.models.antientropy import record_size
from tourillon.core.models.storageschema import Key, Keyspace, Record
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Storage
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

    def __init__(self, storage: Storage, partitioner: Partitioner) -> None:
        self._storage = storage
        self._partitioner = partitioner
        self._managers: dict[int, GroupSessionManager] = {}

    async def roots(self, pids: Sequence[int]) -> dict[int, DigestCell]:
        """Read the root cell of each pid, one read session per group."""
        cells: dict[int, DigestCell] = {}

        for group, members in groupby(sorted(pids), key=self._partitioner.group_for):
            batch = list(members)
            manager = await self._manager(group)

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
            manager = await self._manager(group)

            while len(roots) < limit:
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
        manager = await self._manager(self._partitioner.group_for(pid))

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
        manager = await self._manager(self._group_of(start))
        cell = DigestCell()
        cursor: bytes | None = start

        while cursor is not None:
            async with manager.session() as session:
                keys, cursor = await session.owned_keys(cursor, stop, SCAN_PAGE)

            for key in keys:
                cell = cell + DigestCell.of(key)

            await asyncio.sleep(0)

        return cell

    async def keys(
        self, start: bytes, stop: bytes, limit: int
    ) -> tuple[list[Key], bytes | None]:
        manager = await self._manager(self._group_of(start))
        async with manager.session() as session:
            return await session.owned_keys(start, stop, limit)

    async def read(
        self, pid: int, keys: Sequence[Key], max_bytes: int
    ) -> tuple[list[Record], int]:
        """Read owned versions of ``keys`` in order, up to ``max_bytes``.

        Returns the records and how many keys were consumed; always consumes
        at least one key so callers progress even past one oversized value.
        Versions no longer present or not owned are consumed silently.
        """
        manager = await self._manager(self._partitioner.group_for(pid))
        records: list[Record] = []
        consumed = 0
        size = 0

        for start in range(0, len(keys), APPLY_BATCH):
            chunk = keys[start : start + APPLY_BATCH]

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
        manager = await self._manager(self._partitioner.group_for(pid))
        applied = 0

        for batch in _batches(records):
            async with manager.session(write=True) as session:
                written = await session.put_many(batch)

            applied += sum(written)
            await asyncio.sleep(0)

        return applied

    def foreign(self, record: Record) -> bool:
        """Whether a record is filed under a pid its address does not hash to."""
        expected = self._partitioner.pid_for_hash(
            self._partitioner.space.hash(record.key.address.to_bytes())
        )

        if expected == record.key.pid.value:
            return False

        logger.warning(
            "antientropy.foreign version: pid=%s expected=%s key=%s",
            record.key.pid.value,
            expected,
            record.key.to_bytes(Keyspace.TAGS).hex(),
        )

        return True

    def _group_of(self, tags_key: bytes) -> int:
        return self._partitioner.group_for(int.from_bytes(tags_key[:4], "big"))

    async def _manager(self, group: int) -> GroupSessionManager:
        manager = self._managers.get(group)

        if manager is None:
            provider = await self._storage.session_provider_for(group)
            manager = self._managers[group] = GroupSessionManager(provider)

        return manager


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
