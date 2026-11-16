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

import asyncio
import logging
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from itertools import groupby
from typing import Any, Literal

from tourillon.core.ae.view import ReplicaView
from tourillon.core.exceptions import (
    AntiEntropyRejectError,
    BudgetSpentError,
    ViewChangedError,
)
from tourillon.core.log import logger
from tourillon.core.models.ae import (
    MAX_BATCH_BYTES,
    MAX_PAGE_LIMIT,
    MAX_PIDS_PER_REQUEST,
    BucketsRequest,
    BucketsResponse,
    KeysRequest,
    KeysResponse,
    RootsRequest,
    RootsResponse,
    Scope, RangeRequest, RangeResponse, FetchResponse, FetchRequest,
)
from tourillon.core.models.storageschema import Key, Keyspace, PartitionTime, Pid, Record
from tourillon.core.peer import PeerPool
from tourillon.core.storage.cell import Bucket, DigestCell
from tourillon.core.storage.local import LocalStore
from tourlib import Serializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ProtocolError
from tourlib.handle import DictPayload
from tourlib.utils.format import human_bytes


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
    positions: dict[int, tuple[int, Key | None]] = field(default_factory=dict)
    next_pid: int = 0

    def bounds(self, pid: int, bounds: tuple[int, int]) -> tuple[Key, Key]:
        _, saved = self.positions.get(pid, (-1, None))
        start, stop = bounds
        start_key = PartitionTime(Pid(pid), start).key()
        stop_key = PartitionTime(Pid(pid), stop).key()
        start_bytes = start_key.to_bytes(Keyspace.TAGS)
        stop_bytes = stop_key.to_bytes(Keyspace.TAGS)

        if (
            saved is not None
            and start_bytes <= saved.to_bytes(Keyspace.TAGS) < stop_bytes
        ):
            return saved, stop_key

        return start_key, stop_key


Goal = Literal["sweep", "sync_in", "sync_out"]
Direction = Literal["push", "pull", "skip"]


class PeerSync:
    def __init__(
        self,
        peer: str,
        store: LocalStore,
        serializer: Serializer,
        peer_pool: PeerPool,
        view: ReplicaView,
        limits: SessionLimits,
        progress: SyncProgress,
        stale: asyncio.Event,
        cutoff_ms: int,
        *,
        goal: Goal = "sweep",
    ) -> None:
        self._peer = peer
        self._store = store
        self._serializer = serializer
        self._pool = peer_pool
        self._view = view
        self._limits = limits
        self._progress = progress
        self._stale = stale
        self._cutoff_ms = cutoff_ms
        self._goal = goal

        self._page = min(
            limits.keys_per_request,
            MAX_PAGE_LIMIT,
            max(1, limits.max_keys_scanned // 2),
            max(1, limits.max_repairs),
        )
        self._batch_size = min(limits.max_batch_bytes, MAX_BATCH_BYTES)
        self._tally = SessionTally()

        # Differing pids where this node holds versions at or past the cutoff
        # bucket: those versions were not compared yet, so the pid is not
        # fully reconciled even when the session succeeds.
        self.recent: set[int] = set()

    @property
    def tally(self) -> SessionTally:
        return self._tally

    async def sync_pid(self, pid: int) -> None:
        scope = Scope(pids=(pid,), view=self._view.hash((pid,)))
        after: int | None = None
        cutoff_bucket = self._cutoff_ms >> self._store.bucket_shift

        while True:
            page = BucketsResponse.from_dict(
                await self._rpc("ae.buckets", BucketsRequest(scope, after, self._page))
            )

            theirs = {b: DigestCell.from_bytes(c) for b, c in page.cells}
            mine = await self._store.buckets_until(
                pid, after, page.cursor, cutoff_bucket
            )
            self._note_recent(pid, mine, page.cursor, cutoff_bucket)

            for bucket in sorted(theirs.keys() | mine.keys()):
                if bucket > cutoff_bucket:
                    break

                saved, _ = self._progress.positions.get(pid, (-1, None))
                if bucket < saved:
                    continue

                if bucket > saved:
                    self._progress.positions[pid] = (bucket, None)

                await self._sync_bucket(
                    scope, bucket, theirs.get(bucket), mine.get(bucket)
                )

            if page.cursor is None or page.cursor > cutoff_bucket:
                return

            after = page.cursor

    async def sync_roots(self, pids: Iterable[int]) -> None:
        pids = sorted(pids, key=lambda pid: (pid < self._progress.next_pid, pid))
        pages = -(-len(pids) // MAX_PIDS_PER_REQUEST)

        for index, start in enumerate(range(0, len(pids), MAX_PIDS_PER_REQUEST)):
            page = pids[start : start + MAX_PIDS_PER_REQUEST]
            before = self._tally.copied()
            started = time.monotonic()
            diverged = await self._sync_root_page(page)
            self._report_page(index + 1, pages, page, diverged, before, started)

    def _direction(
        self,
        raw: Key,
        mine: set[Key],
        theirs: set[Key]
    ) -> Direction:
        if raw not in theirs and self._view.is_replica(raw.pid.value, self._peer):
            return "push"
        if raw not in mine and self._view.is_replica(raw.pid.value):
            return "pull"
        return "skip"

    async def _merge_chunk(
        self,
        scope: Scope,
        direction: Direction,
        keys: list[Key]
    ) -> None:
        if direction == "push":
            await self._push(scope, keys)
        elif direction == "pull":
            await self._pull(scope, keys)

    async def _merge_page(
        self,
        scope: Scope,
        bucket: int,
        through: Key | None,
        mine: set[Key],
        theirs: set[Key],
    ) -> None:
        ordered = sorted(
            (k for k in mine | theirs if through is None or k.to_tag_bytes() <= through.to_tag_bytes()),
            key=lambda k: k.to_tag_bytes()
        )
        for direction, group in groupby(
            ordered, key=lambda raw: self._direction(raw, mine, theirs)
        ):
            keys = list(group)
            while keys:
                if direction != "skip":
                    self._spend_repairs()

                remaining = self._limits.max_repairs - self._tally.pushed - self._tally.pulled
                chunk = keys[:min(self._page, max(1, remaining))]
                await self._merge_chunk(scope, direction, chunk)
                # Only completed chunks advance the exclusive key checkpoint.
                self._progress.positions[scope.pid] = (bucket, chunk[-1])
                keys = keys[len(chunk):]

    def _note_recent(
        self, pid: int, mine: Mapping[int, DigestCell], cursor: int | None, cutoff: int
    ) -> None:
        if any(bucket >= cutoff for bucket in mine) or (
            cursor is not None and cursor > cutoff
        ):
            self.recent.add(pid)

    async def _pull(self, scope: Scope, keys: list[Key]) -> None:
        requested = list(keys)

        while keys:
            self._spend_repairs()
            chunk = tuple(keys[:self._page])

            response = FetchResponse.from_dict(
                await self._rpc("ae.fetch", FetchRequest(scope, chunk, self._batch_size))
            )

            keys = keys[min(response.consumed, len(chunk)):]
            records = [r for r in response.records if _wanted(r, scope.pid, requested)]

            for record in records:
                self._store.foreign(record)

            self._tally.pulled += await self._store.apply(records)
            self._tally.pulled_bytes += sum(r.size() for r in records)

    async def _push(self, scope: Scope, keys: list[Key]) -> None:
        ...

    async def _rpc(self, kind: str, payload: DictPayload) -> dict[str, Any]:
        if self._stale.is_set():
            raise ViewChangedError(kind)

        request = build_envelope(
            kind=kind,
            payload=self._serializer.encode(payload.to_dict()),
            schema_id=self._serializer.schema_id,
        )
        async with self._pool.lease(self._peer) as client:
            resp = await client.request(request)

        if resp.kind == f"{kind}.accept":
            data = self._serializer.decode(resp.payload)
            return data if isinstance(data, dict) else {}

        if resp.kind == f"{kind}.reject":
            raise AntiEntropyRejectError.from_dict(
                self._serializer.decode(resp.payload)
            )

        raise ProtocolError(
            f"unexpected response kind '{resp.kind}' for {kind!r} request"
        )

    def _report_page(
        self,
        index: int,
        pages: int,
        pids: list[int],
        diverged: int,
        before: tuple[int, int, int, int],
        started: float,
    ) -> None:
        pulled, pulled_bytes, pushed, pushed_bytes = (
            now - then for now, then in zip(self._tally.copied(), before, strict=True)
        )
        logger.log(
            logging.DEBUG if self._goal == "sweep" else logging.DEBUG,
            "ae.%s page %d/%d peer=%s pids=%d..%d (%d) diverged=%d "
            "pulled=%d (%s) pushed=%d (%s) in %.2fs",
            self._goal, index, pages, self._peer, pids[0], pids[-1],
            len(pids), diverged, pulled, human_bytes(pulled_bytes),
            pushed, human_bytes(pushed_bytes), time.monotonic() - started
        )

    def _spend_repairs(self) -> None:
        if self._tally.pushed + self._tally.pulled >= self._limits.max_repairs:
            raise BudgetSpentError("repairs")

    def _spend_scan(self, count: int) -> None:
        self._tally.keys_scanned += count
        if self._tally.keys_scanned > self._limits.max_keys_scanned:
            raise BudgetSpentError("keys scanned")

    async def _sync_bucket(
        self,
        scope: Scope,
        bucket: int,
        theirs: DigestCell | None,
        mine: DigestCell | None,
    ) -> None:
        start, stop = Bucket(pid=scope.pid, id=bucket).bounds(self._store.bucket_shift)

        if (theirs or DigestCell()) == (mine or DigestCell()):
            # Equal sets have equal subsets: nothing to do even for the
            # bucket that straddles the cutoff.
            return

        horizon = self._cutoff_ms + 1

        if stop <= horizon:
            await self._sync_range(scope, start, stop)
            return

        # The bucket straddles the cutoff: compare its eligible part only.
        start_key = PartitionTime(Pid(scope.pid), start).key()
        horizon_key = PartitionTime(Pid(scope.pid), horizon).key()
        response = RangeResponse.from_dict(
            await self._rpc("ae.range", RangeRequest(scope, start_key, horizon_key))
        )

        if response.cell != await self._store.range_digest(start_key, horizon_key):
            await self._sync_range(scope, start, horizon)

    async def _sync_range(self, scope: Scope, start: int, stop: int) -> None:
        """
        Merge-compare the keys of ``(start, stop)`` page by remote page.

        Each remote page covers ``(cursor, page.cursor]``; the local keys of
        the same window are read in pages too, so memory stays bounded by
        ``keys_per_request`` even when one side holds a whole bucket the
        other lacks.
        """
        cursor, end = self._progress.bounds(scope.pid, (start, stop))

        while cursor is not None:
            remaining = self._limits.max_keys_scanned - self._tally.keys_scanned
            if remaining < 2:
                raise BudgetSpentError("keys scanned")

            limit = min(self._page, remaining // 2)
            page = KeysResponse.from_dict(
                await self._rpc("ae.keys", KeysRequest(scope, cursor, end, limit))
            )

            local = await self._store.keys(cursor, end, limit)
            local_cursor = local[-1] if local else None
            self._spend_scan(len(page.keys) + len(local))
            ends = [c for c in (page.cursor, local_cursor) if c is not None]
            cursor = (
                min(ends, key=lambda k: k.to_bytes(Keyspace.TAGS))
                if ends else None
            )
            bucket = start >> self._store.bucket_shift
            await self._merge_page(scope, bucket, cursor, set(local), set(page.keys))
            self._progress.positions[scope.pid] = (
                (bucket + 1, None) if cursor is None else (bucket, cursor)
            )

    async def _sync_root_page(self, pids: Iterable[int]) -> int:
        pids = tuple(pids)
        local = await self._store.roots(pids)

        request = RootsRequest(
            scope=Scope(pids=pids, view=self._view.hash(pids)),
            cells=tuple(local[pid].to_bytes() for pid in pids),
        )
        response = RootsResponse.from_dict(await self._rpc("ae.roots", request))

        self._tally.roots_compared += len(pids)
        differing = {pid for pid, _ in response.diffs if pid in local}
        self._tally.pids_diverged += len(differing)

        ordered = sorted(
            differing, key=lambda pid: (pid < self._progress.next_pid, pid)
        )
        for pid in ordered:
            self._progress.next_pid = pid
            await self.sync_pid(pid)
            self._progress.positions.pop(pid, None)
            self._progress.next_pid = pid + 1

        return len(differing)


def _wanted(record: Record, pid: int, requested: list[Key]) -> bool:
    """Accept only owned versions of this partition that were asked for."""
    return (
        record.key.pid.value == pid
        and record.tag.owned()
        and record.key in requested
    )
