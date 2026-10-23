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

Two modes share this code:

- *replica mode*: both nodes replicate the pids; the exchange is two-way;
- *handoff mode*: this node holds versions of pids it no longer replicates
  (a write behind a transfer cursor, a hint delivered as the ring moved, a
  late topology view); it only pushes them to the current replicas and never
  pulls. Nothing is deleted: storage reclamation belongs to the GC.

Only versions older than the grace cutoff are compared; younger ones are
still travelling through the write path and the hint replayer.

The session stops as soon as the replica view goes stale (the ring moved)
or its budget is spent; what is left is found again by a later cycle
because the digests still differ. No progress state needs persisting:
the data and its digests *are* the state.
"""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from tourillon.core.antientropy.local import LocalStore
from tourillon.core.antientropy.view import ReplicaView
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


@dataclass(frozen=True)
class SessionLimits:
    keys_per_request: int
    max_batch_bytes: int
    max_repairs: int
    max_keys_scanned: int


class PeerSync:
    def __init__(
        self,
        local: LocalStore,
        call: Call,
        view: ReplicaView,
        stale: asyncio.Event,
        limits: SessionLimits,
        cutoff_ms: int,
        *,
        push_only: bool = False,
    ) -> None:
        self._local = local
        self._call = call
        self._view = view
        self._stale = stale
        self._limits = limits
        # Pages and batches are clamped to what one envelope holds; anything
        # beyond is paginated, never refused.
        self._page = min(limits.keys_per_request, MAX_PAGE_LIMIT)
        self._batch_bytes = min(limits.max_batch_bytes, MAX_BATCH_BYTES)
        self._cutoff_ms = cutoff_ms
        self._push_only = push_only
        self.tally = SessionTally()
        # Differing pids where this node holds versions at or past the cutoff
        # bucket: those versions were not compared yet, so the pid is not
        # fully reconciled even when the session succeeds.
        self.recent: set[int] = set()

    async def sync_roots(self, pids: Sequence[int]) -> None:
        """Compare roots of ``pids`` with the peer, then reconcile those that differ.

        The pids are compared in as many root exchanges as needed to fit an
        envelope. Returns once every differing pid is reconciled; raises
        ViewChangedError, BudgetSpentError or the transport / reject error that
        stopped it.
        """
        for start in range(0, len(pids), MAX_PIDS_PER_REQUEST):
            await self._sync_root_page(pids[start : start + MAX_PIDS_PER_REQUEST])

    async def _sync_root_page(self, pids: Sequence[int]) -> None:
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

        for pid in sorted(differing):
            await self.sync_pid(pid)

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
        cursor = start

        while cursor is not None:
            page = KeysResponse.from_dict(
                await self._rpc(
                    "antientropy.keys",
                    KeysRequest(scope, cursor, stop, self._page).to_dict(),
                )
            )

            self._spend_scan(len(page.keys))
            window_stop = stop if page.cursor is None else page.cursor + b"\x00"

            missing = await self._push_window(
                scope, pid, cursor, window_stop, set(page.keys)
            )

            if missing and not self._push_only:
                await self._pull(scope, pid, sorted(missing))

            cursor = page.cursor

    async def _push_window(
        self, scope: Scope, pid: int, start: bytes, stop: bytes, theirs: set[bytes]
    ) -> set[bytes]:
        """Push local keys of the window the peer lacks; return the ones we lack."""
        cursor: bytes | None = start

        while cursor is not None:
            keys, cursor = await self._local.keys(cursor, stop, self._page)
            self._spend_scan(len(keys))
            extra: list[Key] = []

            for key in keys:
                raw = key.to_bytes(Keyspace.TAGS)
                if raw in theirs:
                    theirs.discard(raw)
                else:
                    extra.append(key)

            if extra:
                await self._push(scope, pid, extra)

        return theirs

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

            self.tally.pushed += _count(response, "applied")

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

    def _scope(self, pids: Sequence[int]) -> Scope:
        return Scope(tuple(pids), self._view.view_hash(pids))

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


def _wanted(record: Record, pid: int, requested: set[bytes]) -> bool:
    """Accept only owned versions of this partition that were asked for."""
    return (
        record.key.pid.value == pid
        and record.tag.kind in OWNED_KINDS
        and record.key.to_bytes(Keyspace.TAGS) in requested
    )


def _count(payload: dict[str, Any], name: str) -> int:
    value = payload.get(name, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0
