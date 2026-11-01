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

"""Initiator side of one anti-entropy session with one peer.

The descent narrows a disagreement level by level, so the cost follows the
size of the difference, not the size of the data:

1. ``roots``: one request compares up to ``pids_per_cycle`` partition
   roots; the peer answers only the pids that differ;
2. ``buckets``: for a differing pid, compare its time buckets
   (``wall_ms >> 22``, about 70 minutes each);
3. ``range``: the bucket that straddles the grace cutoff also counts
   versions too recent to judge, so both sides recompute the digest of its
   eligible part only;
4. ``keys``: for a differing range, list version keys page by page and
   merge them against the local ones;
5. ``push`` what the peer lacks; ``fetch`` and apply what this node lacks.

The direction of every copy follows target ownership (``ReplicaView``),
never a caller-chosen mode:

- versions are pulled only for pids this node replicates;
- versions are pushed only for pids the peer replicates.

Two nodes that both replicate a pid therefore exchange both ways; a node
holding versions of a pid it does not replicate (a handoff write, a node
draining, a late topology view) only pushes them; a joining node catching
up from a node that is leaving that pid only pulls. Nothing is deleted:
storage reclamation belongs to the GC.

Only versions older than the cutoff are compared; younger ones are still
travelling through the write path and its replication obligations. Join
and drain goals use the current time as cutoff.

The session stops as soon as the replica view goes stale (the ring moved)
or its budget is spent; what is left is found again by a later cycle
because the digests still differ. No progress state needs persisting:
the data and its digests *are* the state.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from itertools import groupby
from typing import Any

from tourillon.core.ae.local import LocalStore
from tourillon.core.ae.view import ReplicaView
from tourillon.core.log import logger
from tourillon.core.models.antientropy import (
    MAX_BATCH_BYTES,
    MAX_PAGE_LIMIT,
    MAX_PIDS_PER_REQUEST,
    BucketsRequest,
    BucketsResponse,
    FetchRequest,
    FetchResponse,
    KeysRequest,
    KeysResponse,
    PushRequest,
    RangeRequest,
    RootsRequest,
    RootsResponse,
    Scope,
)
from tourillon.core.models.storageschema import PID_SIZE, Key, Keyspace, Record
from tourillon.core.storage.session import (
    DEFAULT_BUCKET_SHIFT,
    OWNED_KINDS,
    DigestCell,
    bucket_bounds,
)

# Sends one request and returns the decoded ``.accept`` payload; raises
# AntiEntropyRejectError on ``.reject``.
Call = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]


class ViewChangedError(Exception):
    """The replica view used by the session is no longer current."""


class BudgetSpentError(Exception):
    """The session reached its per-cycle work budget."""


@dataclass
class SessionTally:
    """Work done by one session, merged into the service statistics."""

    roots_compared: int = 0
    pids_diverged: int = 0
    keys_scanned: int = 0
    pushed: int = 0
    pulled: int = 0
    pushed_bytes: int = 0
    pulled_bytes: int = 0

    def copied(self) -> tuple[int, int, int, int]:
        return self.pulled, self.pulled_bytes, self.pushed, self.pushed_bytes


@dataclass(frozen=True)
class SessionLimits:
    keys_per_request: int
    max_batch_bytes: int
    max_repairs: int
    max_keys_scanned: int


@dataclass
class SyncProgress:
    """In-memory budget checkpoints, scoped to one peer and replica view."""

    positions: dict[int, tuple[int, bytes | None]] = field(default_factory=dict)
    next_pid: int = 0


class PeerSync:
    def __init__(
        self,
        local: LocalStore,
        peer: str,
        call: Call,
        view: ReplicaView,
        stale: asyncio.Event,
        limits: SessionLimits,
        cutoff_ms: int,
        goal: str | None = None,
        *,
        progress: SyncProgress,
    ) -> None:
        self._local = local
        self._peer = peer
        self._call = call
        self._view = view
        self._stale = stale
        self._limits = limits

        # Pages and batches are clamped to what one envelope holds; anything
        # beyond is paginated, never refused.
        self._page = min(
            limits.keys_per_request,
            MAX_PAGE_LIMIT,
            max(1, limits.max_keys_scanned // 2),
            max(1, limits.max_repairs),
        )
        self._batch_bytes = min(limits.max_batch_bytes, MAX_BATCH_BYTES)
        self._cutoff_ms = cutoff_ms
        self.tally = SessionTally()

        # Join and drain goals report every page at INFO: they may walk
        # thousands of pids, and the operator waits for them. Sweeps run in
        # the background and report at DEBUG.
        self._goal = goal
        self._level = logging.INFO if goal else logging.DEBUG

        # Differing pids where this node holds versions at or past the cutoff
        # bucket: those versions were not compared yet, so the pid is not
        # fully reconciled even when the session succeeds.
        self.recent: set[int] = set()
        self._progress = progress

    async def sync_roots(self, pids: Sequence[int]) -> None:
        """Compare roots of ``pids`` with the peer, then reconcile those that differ.

        The pids are compared in as many root exchanges as needed to fit an
        envelope. Returns once every differing pid is reconciled; raises
        ViewChangedError, BudgetSpentError or the transport / reject error that
        stopped it.
        """
        pids = sorted(pids, key=lambda pid: (pid < self._progress.next_pid, pid))
        pages = -(-len(pids) // MAX_PIDS_PER_REQUEST)

        for index, start in enumerate(range(0, len(pids), MAX_PIDS_PER_REQUEST)):
            page = pids[start : start + MAX_PIDS_PER_REQUEST]
            before = self.tally.copied()
            started = time.monotonic()
            diverged = await self._sync_root_page(page)
            self._report_page(index + 1, pages, page, diverged, before, started)

    def _report_page(
        self,
        index: int,
        pages: int,
        pids: Sequence[int],
        diverged: int,
        before: tuple[int, int, int, int],
        started: float,
    ) -> None:
        pulled, pulled_bytes, pushed, pushed_bytes = (
            now - then for now, then in zip(self.tally.copied(), before, strict=True)
        )
        logger.get().log(
            self._level,
            "ae.%s page %d/%d peer=%s pids=%d..%d (%d) diverged=%d "
            "pulled=%d (%s) pushed=%d (%s) in %.2fs",
            self._goal or "sweep",
            index,
            pages,
            self._peer,
            pids[0],
            pids[-1],
            len(pids),
            diverged,
            pulled,
            human_bytes(pulled_bytes),
            pushed,
            human_bytes(pushed_bytes),
            time.monotonic() - started,
        )

    async def _sync_root_page(self, pids: Sequence[int]) -> int:
        local = await self._local.roots(pids)

        request = RootsRequest(
            self._scope(pids), tuple(local[pid].to_bytes() for pid in pids)
        )

        response = RootsResponse.from_dict(
            await self._rpc("antientropy.roots", request.to_dict())
        )

        self.tally.roots_compared += len(pids)
        differing = {pid for pid, _ in response.diffs if pid in local}
        self.tally.pids_diverged += len(differing)

        ordered = sorted(
            differing, key=lambda pid: (pid < self._progress.next_pid, pid)
        )
        for pid in ordered:
            self._progress.next_pid = pid
            await self.sync_pid(pid)
            self._progress.positions.pop(pid, None)
            self._progress.next_pid = pid + 1

        return len(differing)

    async def sync_pid(self, pid: int) -> None:
        scope = self._scope((pid,))
        after: int | None = None
        cutoff_bucket = self._cutoff_ms >> DEFAULT_BUCKET_SHIFT

        while True:
            page = BucketsResponse.from_dict(
                await self._rpc(
                    "antientropy.buckets",
                    BucketsRequest(scope, after, self._page).to_dict(),
                )
            )

            theirs = {b: DigestCell.from_bytes(c) for b, c in page.cells}
            mine = await self._local.buckets_until(pid, after, page.cursor, self._page)
            self._note_recent(pid, mine, page.cursor, cutoff_bucket)

            for bucket in sorted(theirs.keys() | mine.keys()):
                if bucket > cutoff_bucket:
                    break
                saved_bucket, _ = self._progress.positions.get(pid, (-1, None))
                if bucket < saved_bucket:
                    continue
                if bucket > saved_bucket:
                    self._progress.positions[pid] = (bucket, None)
                await self._sync_bucket(
                    scope, pid, bucket, theirs.get(bucket), mine.get(bucket)
                )

            if page.cursor is None or page.cursor > cutoff_bucket:
                return

            after = page.cursor

    def _note_recent(
        self, pid: int, mine: dict[int, DigestCell], cursor: int | None, cutoff: int
    ) -> None:
        """Flag ``pid`` when local versions may lie beyond the comparable part.

        The cutoff bucket is only compared up to the cutoff and later buckets
        not at all. When the remote page stops past the cutoff, local buckets
        after its cursor are never read, so the pid is flagged conservatively.
        """
        if any(bucket >= cutoff for bucket in mine) or (
            cursor is not None and cursor > cutoff
        ):
            self.recent.add(pid)

    async def _sync_bucket(
        self,
        scope: Scope,
        pid: int,
        bucket: int,
        theirs: DigestCell | None,
        mine: DigestCell | None,
    ) -> None:
        start, stop = bucket_bounds(pid, bucket)

        if (theirs or DigestCell()) == (mine or DigestCell()):
            # Equal sets have equal subsets: nothing to do even for the
            # bucket that straddles the cutoff.
            return

        horizon = pid.to_bytes(PID_SIZE, "big") + (self._cutoff_ms + 1).to_bytes(
            8, "big"
        )

        if stop <= horizon:
            await self._sync_range(scope, pid, start, stop)
            return

        # The bucket straddles the cutoff: compare its eligible part only.
        response = await self._rpc(
            "antientropy.range", RangeRequest(scope, start, horizon).to_dict()
        )

        remote = DigestCell.from_bytes(response["cell"])

        if remote != await self._local.range_digest(start, horizon):
            await self._sync_range(scope, pid, start, horizon)

    async def _sync_range(
        self, scope: Scope, pid: int, start: bytes, stop: bytes
    ) -> None:
        """Merge-compare the keys of ``(start, stop)`` page by remote page.

        Each remote page covers ``(cursor, page.cursor]``; the local keys of
        the same window are read in pages too, so memory stays bounded by
        ``keys_per_request`` even when one side holds a whole bucket the
        other lacks.
        """
        _, saved = self._progress.positions.get(pid, (-1, None))
        cursor = saved if saved is not None and start <= saved < stop else start

        while cursor is not None:
            remaining = self._limits.max_keys_scanned - self.tally.keys_scanned
            if remaining < 2:
                raise BudgetSpentError("keys scanned")
            limit = min(self._page, remaining // 2)
            page = KeysResponse.from_dict(
                await self._rpc(
                    "antientropy.keys",
                    KeysRequest(scope, cursor, stop, limit).to_dict(),
                )
            )

            local, local_cursor = await self._local.keys(cursor, stop, limit)
            self._spend_scan(len(page.keys) + len(local))
            ends = [c for c in (page.cursor, local_cursor) if c is not None]
            cursor = min(ends) if ends else None
            boundary = stop if cursor is None else cursor + b"\x00"
            bucket = (
                int.from_bytes(start[PID_SIZE : PID_SIZE + 8], "big")
                >> DEFAULT_BUCKET_SHIFT
            )
            await self._merge_page(
                scope,
                pid,
                bucket,
                boundary,
                {k.to_bytes(Keyspace.TAGS) for k in local},
                set(page.keys),
            )
            self._progress.positions[pid] = (
                (bucket + 1, None) if cursor is None else (bucket, cursor)
            )

    async def _merge_page(
        self,
        scope: Scope,
        pid: int,
        bucket: int,
        boundary: bytes,
        mine: set[bytes],
        theirs: set[bytes],
    ) -> None:
        ordered = sorted(k for k in mine | theirs if k < boundary)
        for direction, group in groupby(
            ordered, key=lambda raw: self._direction(raw, pid, mine, theirs)
        ):
            keys = list(group)
            while keys:
                if direction != "skip":
                    self._spend_repairs()
                remaining = (
                    self._limits.max_repairs - self.tally.pushed - self.tally.pulled
                )
                chunk = keys[: min(self._page, max(1, remaining))]
                await self._merge_chunk(scope, pid, direction, chunk)
                # Only completed chunks advance the exclusive key checkpoint.
                self._progress.positions[pid] = (bucket, chunk[-1])
                keys = keys[len(chunk) :]

    def _direction(
        self, raw: bytes, pid: int, mine: set[bytes], theirs: set[bytes]
    ) -> str:
        if raw not in theirs and self._view.is_replica(pid, self._peer):
            return "push"
        if raw not in mine and self._view.is_replica(pid):
            return "pull"
        return "skip"

    async def _merge_chunk(
        self, scope: Scope, pid: int, direction: str, keys: list[bytes]
    ) -> None:
        if direction == "push":
            await self._push(
                scope, pid, [Key.from_bytes(raw, Keyspace.TAGS) for raw in keys]
            )
        elif direction == "pull":
            await self._pull(scope, pid, keys)

    async def _push(self, scope: Scope, pid: int, keys: list[Key]) -> None:
        while keys:
            self._spend_repairs()
            records, consumed = await self._local.read(pid, keys, self._batch_bytes)
            keys = keys[consumed:]

            if not records:
                continue

            response = await self._rpc(
                "antientropy.push", PushRequest(scope, tuple(records)).to_dict()
            )

            if delivery_count(response) != len(records):
                raise ValueError("push did not account for every submitted record")
            self.tally.pushed += response["applied"]
            self.tally.pushed_bytes += _size(records)

    async def _pull(self, scope: Scope, pid: int, keys: list[bytes]) -> None:
        requested = set(keys)
        while keys:
            self._spend_repairs()
            chunk = tuple(keys[: self._page])

            response = FetchResponse.from_dict(
                await self._rpc(
                    "antientropy.fetch",
                    FetchRequest(scope, chunk, self._batch_bytes).to_dict(),
                )
            )

            keys = keys[min(response.consumed, len(chunk)) :]
            records = [r for r in response.records if _wanted(r, pid, requested)]

            for record in records:
                self._local.foreign(record)

            self.tally.pulled += await self._local.apply(pid, records)
            self.tally.pulled_bytes += _size(records)

    def _scope(self, pids: Sequence[int]) -> Scope:
        return Scope(
            tuple(pids), self._view.view_hash(pids), self._local.retention_versions
        )

    async def _rpc(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._stale.is_set():
            raise ViewChangedError(kind)
        return await self._call(kind, payload)

    def _spend_scan(self, count: int) -> None:
        self.tally.keys_scanned += count
        if self.tally.keys_scanned > self._limits.max_keys_scanned:
            raise BudgetSpentError("keys scanned")

    def _spend_repairs(self) -> None:
        if self.tally.pushed + self.tally.pulled >= self._limits.max_repairs:
            raise BudgetSpentError("repairs")


def _size(records: Sequence[Record]) -> int:
    """Value bytes carried by ``records``."""
    return sum(len(record.value.data) for record in records)


def human_bytes(count: int) -> str:
    """Format a byte count for logs, in binary units."""
    if count < 1024:
        return f"{count} B"

    size = count / 1024
    for unit in ("KiB", "MiB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024

    return f"{size:.1f} GiB"


def _wanted(record: Record, pid: int, requested: set[bytes]) -> bool:
    """Accept only owned versions of this partition that were asked for."""
    return (
        record.key.pid.value == pid
        and record.tag.kind in OWNED_KINDS
        and record.key.to_bytes(Keyspace.TAGS) in requested
    )


def delivery_count(payload: dict[str, Any]) -> int:
    total = 0
    for name in ("applied", "already_present", "ignored_retention"):
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"invalid push disposition: {name}")
        total += value
    return total
