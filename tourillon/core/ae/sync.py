import asyncio
import logging
from dataclasses import dataclass, field
import time
from typing import Iterable, Any, Literal

from tourillon.core.ae.view import ReplicaView
from tourillon.core.exceptions import ViewChangedError, AntiEntropyRejectError
from tourillon.core.log import logger
from tourillon.core.models.ae import MAX_PAGE_LIMIT, MAX_BATCH_BYTES, MAX_PIDS_PER_REQUEST, RootsResponse, RootsRequest, \
    Scope
from tourillon.core.peer import PeerPool
from tourillon.core.storage.local import LocalStore
from tourlib import Serializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ProtocolError
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
    positions: dict[int, tuple[int, bytes | None]] = field(default_factory=dict)
    next_pid: int = 0


Goal = Literal["sweep", "sync_in", "sync_out"]


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
        goal: Goal = "sweep"
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
            max(1, limits.max_repairs)
        )
        self._batch_bytes = min(limits.max_batch_bytes, MAX_BATCH_BYTES)
        self._tally = SessionTally()

        # Differing pids where this node holds versions at or past the cutoff
        # bucket: those versions were not compared yet, so the pid is not
        # fully reconciled even when the session succeeds.
        self.recent: set[int] = set()

    @property
    def tally(self) -> SessionTally:
        return self._tally

    async def sync_pid(self, pid: int) -> None:
        logger.info("pid to sync pid=%d", pid)

    async def sync_roots(self, pids: Iterable[int]) -> None:
        pids = sorted(pids, key=lambda pid: (pid < self._progress.next_pid, pid))
        pages = -(-len(pids) // MAX_PIDS_PER_REQUEST)

        for index, start in enumerate(range(0, len(pids), MAX_PIDS_PER_REQUEST)):
            page = pids[start: start + MAX_PIDS_PER_REQUEST]
            before = self._tally.copied()
            started = time.monotonic()
            diverged = await self._sync_root_page(page)
            self._report_page(index + 1, pages, page, diverged, before, started)

    async def _rpc(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._stale.is_set():
            raise ViewChangedError(kind)


        request = build_envelope(
            kind=kind,
            payload=self._serializer.encode(payload),
            schema_id=self._serializer.schema_id
        )
        async with self._pool.lease(self._peer) as client:
            resp = await client.request(request)

        if resp.kind == f"{kind}.accept":
            data = self._serializer.decode(resp.payload)
            return data if isinstance(data, dict) else {}

        if resp.kind == f"{kind}.reject":
            raise AntiEntropyRejectError.from_dict(self._serializer.decode(resp.payload))

        raise ProtocolError(f"unexpected response kind '{resp.kind}' for {kind!r} request")

    def _report_page(
        self,
        index: int,
        pages: int,
        pids: list[int],
        diverged: int,
        before: tuple[int, int, int, int],
        started: float
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


    async def _sync_root_page(self, pids: Iterable[int]) -> int:
        pids = tuple(pids)
        local = await self._store.roots(pids)

        request = RootsRequest(
            scope=Scope(pids=pids, view=self._view.hash(pids)),
            cells=tuple(local[pid].to_bytes() for pid in pids)
        )
        response = RootsResponse.from_dict(await self._rpc("ae.roots", request.to_dict()))

        self._tally.roots_compared += len(pids)
        differing = {pid for pid, _ in response.diffs if pid in local}
        self._tally.pids_diverged += len(differing)

        ordered = sorted(differing, key=lambda pid: (pid < self._progress.next_pid, pid))
        for pid in ordered:
            self._progress.next_pid = pid
            await self.sync_pid(pid)
            self._progress.positions.pop(pid, None)
            self._progress.next_pid = pid + 1

        return len(differing)
