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
"""Storage, PartitionStore, PartitionStaging, PartitionHint — hexagonal storage port protocols.

The core domain depends only on these Protocol interfaces. Storage-engine
specifics (transaction semantics, cursor positioning, named data spaces) are
confined to tourillon/infra/store/ and never leak into core/.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.record import KvMetadata, Record, StoreKey


class PartitionHint(Protocol):
    """Hinted-handoff context for a specific target node.

    Written by kv.hint handlers; replayed by HintReplayManager.
    All data stored via this context carries tag b"\\x02" + for_node_id in
    DBI_KEYS so that kv.get sees it on the handoff node, and so that
    HintReplayManager can identify and replay pending hints.
    """

    async def put(self, addr: StoreKey, val: bytes, meta: KvMetadata) -> None:
        """Write a hinted value for the target node."""

    async def delete(self, addr: StoreKey, meta: KvMetadata) -> None:
        """Write a hinted tombstone for the target node."""

    async def mark_stale(self, addr: StoreKey, hlc: HLCTimestamp) -> None:
        """Mark a hint as STALE (replayed successfully). Tag \\x02→\\x04."""

    def pending(self) -> AsyncIterator[Record]:
        """Yield all pending (tag b'\\x02+node_id') hint records."""


class PartitionStaging(Protocol):
    """Rebalance staging context scoped to one (pid, epoch) pair.

    stage() writes records to an invisible staging area, tagged with the
    current epoch so they remain hidden from read operations. commit()
    atomically promotes all staged entries to the committed (visible) state
    in a single durable write transaction. cleanup() removes all staging
    entries for this (pid, epoch) without promoting them, used on cancellation
    or when the applicator detects that the staging epoch is stale.

    Callers must ensure the pid appears in state.toml staging_pids BEFORE
    the first stage() call (invariant §2). commit() must be called BEFORE
    state.toml is updated to move the pid to committed_pids (invariant §1).
    """

    async def stage(self, record: Record) -> None:
        """Write one record to the staging area for this (pid, epoch)."""

    async def commit(self) -> None:
        """Atomically promote all staged entries to committed visibility.

        Must be called before updating the node state file (storage-first
        invariant §1): a crash between the storage commit and the state
        update is recoverable on restart; the reverse order is not.
        """

    async def cleanup(self) -> None:
        """Delete all staging entries for this (pid, epoch).

        Called on cancellation (superseded plan) or on startup when the
        stored epoch is older than the gossip epoch (stale staging entries).
        Does not affect any committed records in the partition.
        """

    async def exists(self) -> bool:
        """Return True if staging entries exist for this (pid, epoch).

        Used during crash recovery to distinguish "transfer incomplete —
        restart from cursor" from "storage committed but state file not yet
        updated — auto-heal on the committed path".
        """

    async def last_staged_log_key(self) -> bytes | None:
        """Return the DBI_LOG cursor key of the highest-HLC staging entry.

        Layout: pid(4B BE) | hlc(12B) | keyspace | key  (DBI_LOG key format).
        Pass verbatim as resume_from to scan() to position the cursor
        strictly after the last staged record, yielding only the delta.
        Returns None when no staging entries exist for this (pid, epoch).
        """


class PartitionStore(Protocol):
    """Per-partition handle for scan, staging, and KV read/write access.

    pid is bound once at open_partition(); PartitionStore itself is
    pid-scoped.
    """

    def scan(self, resume_from: bytes | None = None) -> AsyncIterator[Record]:
        """Yield all committed records in HLC order for this partition.

        When resume_from is None, yields all committed entries from the
        beginning of the partition. When resume_from is a cursor key
        (as returned by last_staged_log_key()), positions the read cursor
        strictly after that key, yielding only records not yet seen by the
        caller. Never re-sends the record at the cursor position.
        """

    def staging(self, epoch: int) -> PartitionStaging:
        """Return a PartitionStaging context scoped to (self.pid, epoch)."""

    def hint(self, for_node_id: str) -> PartitionHint:
        """Return a PartitionHint context scoped to for_node_id."""

    async def get(self, addr: StoreKey) -> Record | None:
        """Return the most recent visible record for addr, or None.

        Implements the DBI_KEYS backward seek algorithm:
        COMMITTED (\\x00) and HINT (\\x02) entries are visible;
        STAGING (\\x01), STALE (\\x04), and PHANTOM (\\xff) are skipped.
        Returns None when the key is absent or all entries are invisible.
        """

    async def put(self, addr: StoreKey, val: bytes, meta: KvMetadata) -> None:
        """Write a COMMITTED (\\x00) version for addr.

        Atomically writes both DBI_KEYS (tag \\x00) and DBI_LOG (value)
        in a single write transaction.
        """

    async def delete(self, addr: StoreKey, meta: KvMetadata) -> None:
        """Write a COMMITTED (\\x00) tombstone for addr.

        Atomically writes DBI_KEYS (tag \\x00) and DBI_LOG (empty value)
        in a single write transaction.
        """

    async def mark_phantom(self, addr: StoreKey, hlc: HLCTimestamp) -> None:
        """Atomically mark hlc as PHANTOM (\\xff) and write V* as COMMITTED.

        Both the phantom mark and V* write occur within a single transaction.
        DBI_LOG is unchanged for the phantom entry (value preserved for history).
        Called by read repair when the local version has a higher HLC than V*.
        """

    async def max_hlc(self) -> HLCTimestamp | None:
        """Return the maximum HLC timestamp ever written to this partition, or None.

        Seeks to the last key in DBI_LOG for this partition and decodes the
        embedded HLC. Includes committed records, hints, and staging entries —
        any record that was written to the log. Returns None when the partition
        is empty. Cost: O(log N) I/O (one cursor seek to the end).

        Called once at node startup to seed HLCClock.restore() without
        requiring a separate persisted checkpoint. The wall clock always
        advances during a restart, so restoring from the persisted max HLC is
        sufficient to guarantee monotonicity after recovery.
        """


class Storage(Protocol):
    """Factory for per-partition storage handles.

    The pid is supplied once at open_partition(); all subsequent operations
    on the returned PartitionStore are scoped to that pid.
    """

    def open_partition(self, pid: int) -> PartitionStore:
        """Return the PartitionStore for *pid*."""
