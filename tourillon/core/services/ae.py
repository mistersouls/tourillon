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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from tourillon.core.ae.tracker import ViewTracker
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.ae import (
    BucketsRequest,
    BucketsResponse,
    KeysRequest,
    KeysResponse,
    RootsRequest,
    RootsResponse,
    Scope,
    RangeRequest,
    RangeResponse, FetchRequest, FetchResponse, PushRequest, PushResponse,
)
from tourillon.core.storage.local import LocalStore

BUSY_RETRY_AFTER = 1.0  # seconds


class AeService:
    def __init__(
        self,
        local_store: LocalStore,
        tracker: ViewTracker,
        gate: asyncio.Event,
        concurrency: int,
    ) -> None:
        self._local_store = local_store
        self._tracker = tracker
        self._gate = gate
        self._concurrency = concurrency
        self._active: int = 0
        self._rejected: int = 0
        self._applied: int = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def roots(self, request: RootsRequest) -> RootsResponse:
        async with self._admit(request.scope):
            local = await self._local_store.roots(request.scope.pids)

            diffs = tuple(
                (pid, local[pid].to_bytes())
                for pid, theirs in zip(request.scope.pids, request.cells, strict=True)
                if local[pid].to_bytes() != theirs
            )
            return RootsResponse(diffs=diffs)

    async def buckets(self, request: BucketsRequest) -> BucketsResponse:
        async with self._admit(request.scope):
            cells = await self._local_store.list_buckets(
                request.scope.pid, request.after, request.limit
            )
            cursor = cells[-1][0] if cells else None
            return BucketsResponse(
                cells=tuple((bucket, cell.to_bytes()) for bucket, cell in cells),
                cursor=cursor,
            )

    async def range(self, request: RangeRequest) -> RangeResponse:
        async with self._admit(request.scope):
            cell = await self._local_store.range_digest(request.start, request.stop)
            return RangeResponse(cell=cell)

    async def keys(self, request: KeysRequest) -> KeysResponse:
        async with self._admit(request.scope):
            keys = await self._local_store.keys(
                request.start, request.stop, request.limit
            )
            return KeysResponse(tuple(keys), keys[-1] if keys else None)

    async def fetch(self, request: FetchRequest) -> FetchResponse:
        async with self._admit(request.scope):
            records, consumed = await self._local_store.read_records(request.keys, request.max_bytes)
            return FetchResponse(
                records=tuple(records),
                consumed=max(1, consumed),
            )

    async def push(self, request: PushRequest) -> PushResponse:
        async with self._admit(request.scope, write=True):
            results = await self._local_store.apply(request.records)
            applied = sum(results)
            already_present = len(results) - applied
            self._applied += applied
            return PushResponse(applied=applied, already_present=already_present)

    @asynccontextmanager
    async def _admit(self, scope: Scope, *, write: bool = False) -> AsyncIterator[None]:
        if self._active >= self._concurrency:
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.BUSY, "capacity exceeded", BUSY_RETRY_AFTER
            )

        self._check(scope, write)
        self._active += 1
        self._idle.clear()

        try:
            yield
        finally:
            self._active -= 1
            if not self._active:
                self._idle.set()

    def _check(self, scope: Scope, write: bool) -> None:
        view = self._tracker.view

        if not self._gate.is_set() or view is None:
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.NOT_READY,
                "ae is not serving",
                self._tracker.settle_quiet,
            )

        if not view.covers(scope.pids):
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.INVALID, "unknown partition"
            )

        if view.hash(scope.pids) != scope.view:
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.STALE_VIEW,
                "replica views differ",
                self._tracker.settle_quiet,
            )

        if write and not all(view.is_replica(pid) for pid in scope.pids):
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.NOT_REPLICA, "not a replica of every partition"
            )
