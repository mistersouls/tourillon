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
"""In-memory implementations of Storage, PartitionStore, PartitionStaging, PartitionHint.

These classes stand in for the real storage backend in unit tests. They
implement the same port protocols with full in-memory semantics: staging
records are stored in a list and are invisible to scan() until commit()
promotes them, mirroring the staging-visibility invariant of the real adapter.

No durable transactions or cursor engines are involved; persistence is limited
to the lifetime of the object.
"""

from __future__ import annotations

import struct
from collections.abc import AsyncIterator

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.record import (
    KvMetadata,
    Record,
    StoreKey,
    Tombstone,
    Version,
)


class InMemoryPartitionHint:
    """In-memory PartitionHint for one (pid, for_node_id) pair.

    Hint records are kept in a list alongside a set of stale HLCs
    (hlcs that have been marked STALE after successful replay).
    """

    def __init__(self, pid: int, for_node_id: str) -> None:
        self._pid = pid
        self._for_node_id = for_node_id
        self._hints: list[Record] = []
        self._stale_hlcs: set[HLCTimestamp] = set()

    async def put(self, addr: StoreKey, val: bytes, meta: KvMetadata) -> None:
        """Store a hinted write."""
        rec = Version(
            address=addr, metadata=meta.hlc, value=val, quorum_write=meta.quorum_write
        )
        self._hints.append(rec)

    async def delete(self, addr: StoreKey, meta: KvMetadata) -> None:
        """Store a hinted tombstone."""
        rec = Tombstone(address=addr, metadata=meta.hlc, quorum_write=meta.quorum_write)
        self._hints.append(rec)

    async def mark_stale(self, addr: StoreKey, hlc: HLCTimestamp) -> None:
        """Mark a hint as STALE (replayed successfully)."""
        self._stale_hlcs.add(hlc)

    def pending(self) -> AsyncIterator[Record]:
        """Yield all pending (non-stale) hint records."""
        return self._iter_pending()

    async def _iter_pending(self) -> AsyncIterator[Record]:  # type: ignore[override]
        for rec in self._hints:
            if rec.metadata not in self._stale_hlcs:
                yield rec

    def hint_records(self) -> list[Record]:
        """Return all hint records (test helper)."""
        return list(self._hints)


class InMemoryPartitionStaging:
    """In-memory PartitionStaging for one (pid, epoch) pair.

    Staged records are kept in an append-only list. commit() is a no-op
    beyond clearing the staging list (the real adapter would flip visibility
    tags in a write transaction). cleanup() removes all staged records.
    """

    def __init__(self, pid: int, epoch: int) -> None:
        self._pid = pid
        self._epoch = epoch
        self._staged: list[Record] = []

    async def stage(self, record: Record) -> None:
        """Append record to the in-memory staging list."""
        self._staged.append(record)

    async def commit(self) -> None:
        """Promote all staged records: clear the staging list (no-op here)."""

    async def cleanup(self) -> None:
        """Discard all staged records for this (pid, epoch)."""
        self._staged.clear()

    async def exists(self) -> bool:
        """Return True when at least one staged record is present."""
        return bool(self._staged)

    async def last_staged_log_key(self) -> bytes | None:
        """Return a synthetic DBI_LOG cursor key for the highest-HLC staged record.

        Layout matches the port contract: pid(4B BE) | wall_ms(8B BE) |
        counter(2B BE) | node_id_prefix(2B) | keyspace | key. Callers may
        pass this verbatim as resume_from to scan().

        Returns None when no records are staged.
        """
        if not self._staged:
            return None
        last = self._staged[-1]
        hlc = last.metadata
        node_id_bytes = hlc.node_id.encode("utf-8")[:4].ljust(4, b"\x00")
        hlc_bytes = (
            struct.pack(">Q", hlc.wall_ms)
            + struct.pack(">H", hlc.counter)
            + node_id_bytes[:2]
        )
        return (
            struct.pack(">I", self._pid)
            + hlc_bytes
            + last.address.keyspace
            + last.address.key
        )

    def staged_records(self) -> list[Record]:
        """Return a snapshot copy of all staged records."""
        return list(self._staged)


class InMemoryPartitionStore:
    """In-memory PartitionStore for one pid.

    Committed records (added via add_record() or put/delete) are always
    visible to scan() and get(). Staging contexts are keyed by epoch and
    returned by staging(). Hint contexts are keyed by for_node_id.

    Phantom HLCs are tracked; get() skips records whose HLC is phantom.
    """

    def __init__(self, pid: int) -> None:
        self._pid = pid
        self._records: list[Record] = []
        self._staging: dict[int, InMemoryPartitionStaging] = {}
        self._hints: dict[str, InMemoryPartitionHint] = {}
        self._phantom_hlcs: set[HLCTimestamp] = set()

    def add_record(self, record: Record) -> None:
        """Pre-populate committed records (test helper; no staging involved)."""
        self._records.append(record)

    async def scan(self, resume_from: bytes | None = None) -> AsyncIterator[Record]:
        """Yield committed records in HLC order.

        When resume_from is None, yields all records from the start.
        When resume_from is a cursor key (as returned by last_staged_log_key()),
        positions strictly after the cursor: the cursor record itself is never
        re-yielded. Only records with an HLC strictly greater than the cursor's
        (wall_ms, counter) pair are emitted.
        """
        sorted_records = sorted(self._records, key=_hlc_key)
        if resume_from is None:
            for rec in sorted_records:
                yield rec
            return

        cursor_wall, cursor_counter = _parse_cursor_hlc(resume_from)
        for rec in sorted_records:
            rwall, rcounter, _ = _hlc_key(rec)
            if (rwall, rcounter) > (cursor_wall, cursor_counter):
                yield rec

    def staging(self, epoch: int) -> InMemoryPartitionStaging:
        """Return or create the staging context for the given epoch."""
        if epoch not in self._staging:
            self._staging[epoch] = InMemoryPartitionStaging(self._pid, epoch)
        return self._staging[epoch]

    def hint(self, for_node_id: str) -> InMemoryPartitionHint:
        """Return or create the hint context for for_node_id."""
        if for_node_id not in self._hints:
            self._hints[for_node_id] = InMemoryPartitionHint(self._pid, for_node_id)
        return self._hints[for_node_id]

    async def get(self, addr: StoreKey) -> Record | None:
        """Return the most recent non-phantom committed record for addr, or None."""
        candidates = [
            r
            for r in self._records
            if r.address == addr and r.metadata not in self._phantom_hlcs
        ]
        if not candidates:
            # Also check hints
            for hint_store in self._hints.values():
                for rec in hint_store.hint_records():
                    if rec.address == addr and rec.metadata not in self._phantom_hlcs:
                        candidates.append(rec)
            if not candidates:
                return None
        return max(candidates, key=_hlc_key)

    async def put(self, addr: StoreKey, val: bytes, meta: KvMetadata) -> None:
        """Write a committed version."""
        rec = Version(
            address=addr, metadata=meta.hlc, value=val, quorum_write=meta.quorum_write
        )
        self._records.append(rec)

    async def delete(self, addr: StoreKey, meta: KvMetadata) -> None:
        """Write a committed tombstone."""
        rec = Tombstone(address=addr, metadata=meta.hlc, quorum_write=meta.quorum_write)
        self._records.append(rec)

    async def mark_phantom(self, addr: StoreKey, hlc: HLCTimestamp) -> None:
        """Mark hlc as PHANTOM — get() will skip it."""
        self._phantom_hlcs.add(hlc)

    async def max_hlc(self) -> HLCTimestamp | None:
        """Return the max HLC across all committed and hint records, or None.

        Mirrors DBI_LOG semantics: the last key in DBI_LOG is the maximum HLC
        regardless of record visibility tag. Used once at startup to seed
        HLCClock.restore() without a separate persisted checkpoint.
        """
        all_records: list[Record] = list(self._records)
        for hint_store in self._hints.values():
            all_records.extend(hint_store.hint_records())
        if not all_records:
            return None
        return max(
            (rec.metadata for rec in all_records),
            key=lambda t: (t.wall_ms, t.counter, t.node_id),
        )


class InMemoryStorage:
    """In-memory Storage: factory for per-partition partition stores."""

    def __init__(self) -> None:
        self._partitions: dict[int, InMemoryPartitionStore] = {}

    def open_partition(self, pid: int) -> InMemoryPartitionStore:
        """Return or create the PartitionStore for pid."""
        if pid not in self._partitions:
            self._partitions[pid] = InMemoryPartitionStore(pid)
        return self._partitions[pid]


def _hlc_key(rec: Record) -> tuple[int, int, str]:
    """Return a sortable (wall_ms, counter, node_id) tuple from a record."""
    hlc: HLCTimestamp = rec.metadata
    return hlc.wall_ms, hlc.counter, hlc.node_id


def _parse_cursor_hlc(cursor: bytes) -> tuple[int, int]:
    """Extract (wall_ms, counter) from a cursor bytes key.

    Cursor layout: pid(4B BE) | wall_ms(8B BE) | counter(2B BE) | …
    Returns (wall_ms, counter) for strict-greater-than comparison.
    Records whose (wall_ms, counter) is strictly greater than this value
    are considered to be after the cursor position.
    """
    wall_ms = struct.unpack(">Q", cursor[4:12])[0]
    counter = struct.unpack(">H", cursor[12:14])[0]
    return (wall_ms, counter)
