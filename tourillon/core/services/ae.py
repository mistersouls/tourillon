import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from tourillon.core.ae.tracker import ViewTracker
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.ae import RootsRequest, RootsResponse, Scope
from tourillon.core.storage.local import LocalStore

BUSY_RETRY_AFTER = 1.0  # seconds


class AeService:
    def __init__(
        self,
        local_store: LocalStore,
        tracker: ViewTracker,
        gate: asyncio.Event,
        concurrency: int
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
                self._tracker.settle_quiet
            )

        if not view.covers(scope.pids):
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.INVALID, "unknown partition")

        if view.hash(scope.pids) != scope.view:
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.STALE_VIEW, "replica views differ", self._tracker.settle_quiet
            )

        if write and not all(view.is_replica(pid) for pid in scope.pids):
            self._rejected += 1
            raise AntiEntropyRejectError(
                AntiEntropyRejectError.NOT_REPLICA, "not a replica of every partition"
            )
