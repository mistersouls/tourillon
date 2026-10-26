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

"""Serve the ``antientropy.*`` requests of peers.

Admission is the anti-storm valve of the protocol. A responder serves at
most ``concurrency`` requests at once and refuses the rest immediately with
``busy`` and a ``retry_after`` hint instead of queueing them: queued work
would only grow while the initiators wait, and the client path shares the
same storage. Initiators back off on ``busy``, so a hot node sheds load.

Every request is checked against the local replica view first:

- ``not_ready``: the service is stopped or the ring is settling;
- ``stale_view``: the requester computed other replicas for these pids;
- ``not_replica``: this node does not replicate one of them.

Only then is storage touched, and only for the partitions named.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

from tourillon.core.ae.local import LocalStore
from tourillon.core.ae.tracker import ViewTracker
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.antientropy import (
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
from tourillon.core.models.storageschema import Key, Keyspace
from tourillon.core.storage.session import OWNED_KINDS, DigestCell

# Hint returned with ``busy``: long enough for in-flight requests to finish.
BUSY_RETRY_AFTER = 1.0


class AntiEntropyResponder:
    def __init__(
        self,
        local: LocalStore,
        tracker: ViewTracker,
        concurrency: int,
    ) -> None:
        self._local = local
        self._tracker = tracker
        self._slots = concurrency
        self._active = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._running = False
        self.applied = 0
        self.rejected = 0

    def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        """Refuse new requests and wait for the admitted ones to finish."""
        self._running = False
        await self._idle.wait()

    async def roots(self, request: RootsRequest) -> RootsResponse:
        async with self._admit(request.scope):
            local = await self._local.roots(request.scope.pids)

            diffs = tuple(
                (pid, local[pid].to_bytes())
                for pid, theirs in zip(request.scope.pids, request.cells, strict=True)
                if local[pid].to_bytes() != theirs
            )

            return RootsResponse(diffs)

    async def buckets(self, request: BucketsRequest) -> BucketsResponse:
        async with self._admit(request.scope):
            cells, cursor = await self._local.bucket_page(
                request.scope.pid, request.after, request.limit
            )
            return BucketsResponse(
                tuple((bucket, cell.to_bytes()) for bucket, cell in cells), cursor
            )

    async def range(self, request: RangeRequest) -> DigestCell:
        async with self._admit(request.scope):
            return await self._local.range_digest(request.start, request.stop)

    async def keys(self, request: KeysRequest) -> KeysResponse:
        async with self._admit(request.scope):
            keys, cursor = await self._local.keys(
                request.start, request.stop, request.limit
            )
            return KeysResponse(
                tuple(key.to_bytes(Keyspace.TAGS) for key in keys), cursor
            )

    async def fetch(self, request: FetchRequest) -> FetchResponse:
        async with self._admit(request.scope):
            keys = [Key.from_bytes(raw, Keyspace.TAGS) for raw in request.keys]

            records, consumed = await self._local.read(
                request.scope.pid, keys, request.max_bytes
            )

            return FetchResponse(tuple(records), max(1, consumed))

    async def push(self, request: PushRequest) -> int:
        async with self._admit(request.scope):
            records = [r for r in request.records if r.tag.kind in OWNED_KINDS]

            for record in records:
                self._local.foreign(record)

            applied = await self._local.apply(request.scope.pid, records)
            self.applied += applied

            return applied

    async def serve[T](self, scope: Scope, work: Callable[[], Awaitable[T]]) -> T:
        """Run ``work`` under admission; for callers that are not wire handlers."""
        async with self._admit(scope):
            return await work()

    @contextlib.asynccontextmanager
    async def _admit(self, scope: Scope) -> AsyncIterator[None]:
        if self._active >= self._slots:
            self.rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.BUSY, "responder at capacity", BUSY_RETRY_AFTER
            )

        self._check(scope)
        self._active += 1
        self._idle.clear()

        try:
            yield
        finally:
            self._active -= 1
            if not self._active:
                self._idle.set()

    def _check(self, scope: Scope) -> None:
        view = self._tracker.view

        if not self._running or view is None:
            self.rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.NOT_READY,
                "anti-entropy is not serving",
                self._tracker.settle_delay,
            )

        if not view.covers(scope.pids):
            self.rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.INVALID, "unknown partition"
            )

        if view.view_hash(scope.pids) != scope.view:
            self.rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.STALE_VIEW,
                "replica views differ",
                self._tracker.settle_delay,
            )

        if not all(view.is_replica(pid) for pid in scope.pids):
            self.rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.NOT_REPLICA, "not a replica of every partition"
            )
