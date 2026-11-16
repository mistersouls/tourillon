import asyncio
import contextlib
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Callable, Awaitable, Mapping, Any, Iterable

from tourillon.core.ae.sync import SyncProgress, PeerSync, SessionTally, SessionLimits, Goal
from tourillon.core.ae.tracker import ViewTracker
from tourillon.core.ae.view import ReplicaView
from tourillon.core.exceptions import AntiEntropyRejectError, ViewChangedError, BudgetSpentError
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.config import AeConfig
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer import PeerPool
from tourillon.core.ports import Telemetry
from tourillon.core.storage.local import LocalStore
from tourillon.core.log import logger, describe
from tourlib import Serializer
from tourlib.exceptions import ProtocolError
from tourlib.utils.format import human_bytes


_SESSION_ERRORS = (
    AntiEntropyRejectError,
    ProtocolError,
    RuntimeError,
    OSError,
    TimeoutError,
    ValueError,
    KeyError,
    TypeError,
)

# Phases of the peers a session or a delivery may target. A JOINING node is
# being filled and a DRAINING one still serves the comparison of what it
# hands off.
_SESSION_PHASES = frozenset(
    {MemberPhase.JOINING, MemberPhase.READY, MemberPhase.DRAINING}
)

_SOURCE_PHASES = frozenset({MemberPhase.READY, MemberPhase.DRAINING})

_YIELD_EVERY = 4096


@dataclass
class AeStats:
    sweeps: int = 0
    sessions: int = 0
    skipped: int = 0
    roots_compared: int = 0
    pids_diverged: int = 0
    keys_scanned: int = 0
    pulled: int = 0
    pushed: int = 0
    handed_off: int = 0
    bytes_pulled: int = 0
    bytes_pushed: int = 0
    view_changes: int = 0
    budget_stops: int = 0
    sync_in_passes: int = 0
    sync_out_passes: int = 0
    errors: int = 0
    rejects: dict[str, int] = field(default_factory=dict)
    last_failure: str | None = None
    last_failure_at: str | None = None

    def failed(self, exc: Exception) -> None:
        self.last_failure = describe(exc)
        self.last_failure_at = datetime.now(UTC).isoformat()
        logger.error("ae.error %s", exc, exc_info=exc)

    def progress(self) -> dict[str, int]:
        """Snapshot of the counters a sweep summary reports as deltas."""
        return {
            "sessions": self.sessions,
            "diverged": self.pids_diverged,
            "pulled": self.pulled,
            "pushed": self.pushed + self.handed_off,
            "bytes_pulled": self.bytes_pulled,
            "bytes_pushed": self.bytes_pushed,
            "errors": self.errors,
        }

    def merge(self, tally: SessionTally, *, handoff: bool) -> None:
        self.roots_compared += tally.roots_compared
        self.pids_diverged += tally.pids_diverged
        self.keys_scanned += tally.keys_scanned
        self.pulled += tally.pulled
        self.bytes_pulled += tally.pulled_bytes
        self.bytes_pushed += tally.pushed_bytes

        if handoff:
            self.handed_off += tally.pushed
        else:
            self.pushed += tally.pushed


@dataclass(frozen=True, slots=True)
class Unit:
    peer: str | None
    pids: tuple[int, ...] = ()
    span: tuple[int, int] = (0, 0)


@dataclass
class Exchange:
    """
    Outcome of sessions with several peers over their pids.

    - ``reached[pid]``: peers that completed a session covering ``pid``;
    - ``skipped``: peers that are down or not taking part, not retried;
    - ``failed``: peers to retry (deferred, refused, budget, view change)
    """
    reached: dict[int, int] = field(default_factory=dict)
    skipped: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)


@dataclass(slots=True, frozen=True)
class Work:
    peer: str
    fn: Callable[..., Awaitable[None]]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)


class PeerBackoff:
    def __init__(self, backoff: BackoffRetry) -> None:
        self._backoff = backoff
        self._until: dict[str, float] = {}
        self._delay: dict[str, float] = {}

    def deferred(self, peer: str) -> bool:
        """Check if the peer is currently in backoff."""
        until = self._until.get(peer)
        return until is not None and self._clock() < until

    def succeeded(self, peer: str) -> None:
        """Mark the peer as having succeeded; reset its backoff."""
        self._until.pop(peer, None)
        self._delay.pop(peer, None)

    def defer(self, peer: str, seconds: float) -> None:
        """Honor without growing the failure backoff."""
        self._until[peer] = max(self._until.get(peer, 0.0), self._clock() + seconds)

    def failed(self, peer: str) -> None:
        """Mark the peer as having failed; grow its backoff."""
        delay = self._delay.get(peer, self._backoff.initial_delay)
        jitter = self._backoff.jitter
        pause = max(0, delay * (1 + random.uniform(-jitter, jitter)))
        self._until[peer] = self._clock() + pause
        self._delay[peer] = min(delay * self._backoff.factor, self._backoff.max_delay)

    @staticmethod
    def _clock() -> float:
        return time.monotonic()


class AeReconciler:
    def __init__(
        self,
        node_id: str,
        config: AeConfig,
        replication_factor: int,
        local_store: LocalStore,
        partitioner: Partitioner,
        topologymgr: TopologyManager,
        peer_pool: PeerPool,
        serializer: Serializer,
        telemetry: Telemetry,
        backoff: BackoffRetry | None = None
    ) -> None:
        self._node_id = node_id
        self._gate = asyncio.Event()
        self._config = config
        self._local_store = local_store
        self._partitioner = partitioner
        self._topologymgr = topologymgr
        self._pool = peer_pool
        self._serializer = serializer
        self._telemetry = telemetry

        self._progress: dict[tuple[str, str], SyncProgress] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._stats = AeStats()
        self._backoff = PeerBackoff(backoff or BackoffRetry())
        self._retry_delay = max(1.0, self._config.min_interval)

        # pid -> (view hash, local root) at the last complete handoff: an
        # orphan partition is only pushed again once it or its replicas change.
        self._handed: dict[int, tuple[bytes, bytes]] = {}

        self._tracker = ViewTracker(
            node_id=self._node_id,
            partitioner=self._partitioner,
            topologymgr=self._topologymgr,
            replication_factor=replication_factor,
            settle_delay=5.0,
            settle_quiet=0.5
        )

    @property
    def running(self) -> bool:
        """Check if the service is running."""
        return any(not task.done() for task in self._tasks)

    @property
    def gate(self) -> asyncio.Event:
        return self._gate

    @property
    def tracker(self) -> ViewTracker:
        return self._tracker

    async def start(self) -> None:
        if self.running:
            return

        await self._tracker.start()
        self._gate.set()
        self._tasks = [
            # asyncio.create_task(self._deliver_loop(), name="ae-delivery"),
            asyncio.create_task(self._run(), name="ae-loop"),
        ]

        logger.info("ae.started node=%s", self._node_id)

    async def stop(self) -> None:
        tasks, self._tasks = self._tasks, []

        for task in tasks:
            task.cancel()

        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._gate.clear()
        await self._tracker.stop()

    def plan(self, view: ReplicaView) -> list[Unit]:
        units: list[Unit] = []
        pids_per_cycle = self._config.pids_per_cycle
        total = self._partitioner.total_partitions

        for peer, pids in sorted(view.shared.items()):
            if initiates(view.node_id, peer):
                units.extend(
                    Unit(peer, pids[i:i + pids_per_cycle])
                    for i in range(0, len(pids), pids_per_cycle)
                )

        units.extend(
            Unit(None, span=(lo, min(lo + pids_per_cycle, total)))
            for lo in range(0, total, pids_per_cycle)
        )

        return units

    async def run_unit(self, unit: Unit, view: ReplicaView, stale: asyncio.Event) -> None:
        if unit.peer is None:
            await self._handoff(unit, view, stale)
            return

        if not await self._eligible(unit.peer):
            self._stats.skipped += 1
            return

        sync = self._session(unit.peer, view, stale, self._cutoff_ms())
        outcome = await self._guard(Work(unit.peer, sync.sync_roots, (unit.pids,)))
        self._settle("sweep", sync.tally, outcome)

    async def sweep(self, view: ReplicaView, stale: asyncio.Event) -> bool:
        units = self.plan(view)

        random.shuffle(units)
        interval = max(self._config.min_interval, self._config.sweep_period / len(units))

        # A random first delay de-synchronizes nodes that all rebuilt their
        # view at the same moment after a ring change.
        delay = random.uniform(0, interval)
        started = time.monotonic()
        before = self._stats.progress()

        for unit in units:
            await self._pause(delay, stale)

            # obligations go first: the sweep only finds what they missed.
            # todo(souls): handle obligations and deprecate hint replay

            if stale.is_set():
                self._stats.view_changes += 1
                self._sweep_done("interrupted", len(units), before, started)
                return False

            await self.run_unit(unit, view, stale)
            delay = self._jittered(interval)

        self._stats.sweeps += 1
        self._sweep_done("completed", len(units), before, started)
        return True

    async def sync_in(self) -> None:
        started = time.monotonic()
        passes = 0
        copied = (self._stats.pulled, self._stats.bytes_pulled)
        logger.info("ae.sync_in started node=%s", self._node_id)

        while True:
            view = await self._tracker.wait_ready()
            stale = self._tracker.stale_signal()

            if not view.owns:
                logger.debug("ae.sync_in waiting for a view that places node=%s", self._node_id)
                await stale.wait()
                continue

            failed = await self._sync_in_pass(view, stale)
            self._stats.sync_in_passes += 1
            passes += 1

            if not failed and not stale.is_set():
                logger.info(
                    "ae.sync_in done node=%s passes=%d pulled=%d (%s) in %.2fs",
                    self._node_id, passes, self._stats.pulled - copied[0],
                    human_bytes(self._stats.bytes_pulled - copied[1]),
                    time.monotonic() - started
                )
                return

            _log_incomplete("sync_in", passes, stale, self._retry_delay)
            await self._pause(self._retry_delay, stale)

    async def sync_out(self) -> None:
        ...

    def _cutoff_ms(self) -> int:
        return int((time.time() - self._config.grace) * 1000)

    async def _exchange(
        self,
        targets: dict[str, list[int]],
        view: ReplicaView,
        stale: asyncio.Event,
        cutoff_ms: int,
        phases: frozenset[MemberPhase],
        *,
        goal: Goal = "sweep"
    ) -> Exchange:
        """Run one session per peer over its pids"""
        result = Exchange()

        for peer, pids in sorted(targets.items()):
            if not await self._reachable(peer, phases):
                result.skipped.add(peer)
                self._stats.skipped += 1
                continue

            if self._backoff.deferred(peer):
                result.failed.add(peer)
                self._stats.skipped += 1
                continue

            sync = self._session(peer, view, stale, cutoff_ms, goal=goal)
            started = time.monotonic()
            outcome = await self._guard(Work(peer, sync.sync_roots, (pids,)))
            ok = self._settle(goal, sync.tally, outcome)

            if goal is not None:
                state = "done" if ok else "stopped"
                tally = sync.tally
                logger.info(
                    "ae.%s session %s peer=%s pids=%d diverged=%d pulled=%d (%s) pushed=%d (%s) in %.2fs",
                    goal, state, peer, len(pids), tally.pids_diverged, tally.pulled, human_bytes(tally.pulled_bytes),
                    tally.pushed, human_bytes(tally.pushed_bytes), time.monotonic() - started
                )

            if not ok:
                result.failed.add(peer)
                continue

            _count_reached(result.reached, pids, sync.recent if goal is None else set())

        return result

    async def _eligible(self, peer: str) -> bool:
        if self._backoff.deferred(peer):
            return False

        return await self._reachable(peer, _SESSION_PHASES)

    def _failed(self, peer: str, exc: Exception) -> str:
        self._stats.failed(exc)

        if isinstance(exc, AntiEntropyRejectError):
            self._stats.rejects[exc.code] = self._stats.rejects.get(exc.code, 0) + 1
            if exc.retry_after > 0:
                self._backoff.defer(peer, exc.retry_after)
                logger.debug(
                    "ae.session deferred peer=%s code=%s retry_after=%.2fs",
                    peer, exc.code, exc.retry_after
                )
                return "deferred"
        else:
            self._stats.errors += 1

        self._backoff.failed(peer)
        logger.warning("ae.session failed peer=%s: %s", peer, describe(exc))
        return "failed"

    async def _guard(self, work: Work) -> str:
        """
        Run one session; turn its failure into backoff and counters.

        Return the session outcome: ``ok``, ``view_changed``, ``budget_spent``,
        ``deferred`` or ``failed``.
        """
        try:
            await work.fn(*work.args, **work.kwargs)
            self._backoff.succeeded(work.peer)
            return "ok"
        except ViewChangedError:
            self._stats.view_changes += 1
            return "view_changed"
        except BudgetSpentError:
            self._stats.budget_stops += 1
            return "budget_spent"
        except _SESSION_ERRORS as exc:
            return self._failed(work.peer, exc)

    async def _handoff(self, unit: Unit, view: ReplicaView, stale: asyncio.Event) -> None:
        """
        Push versions of partitions this node does not replicate.

        Direction follows ownership: the peers replicate these pids and this
        node does not, so versions only flow towards them. The local copy is
        left for the GC.
        """
        marks = await self._orphans(unit.span, view)
        result = await self._exchange(_targets(view, marks), view, stale, self._cutoff_ms(), _SESSION_PHASES)

        for pid, mark in marks.items():
            if result.reached.get(pid, 0) == len(view.replicas[pid]):
                self._handed[pid] = mark

    def _jittered(self, delay: float) -> float:
        jitter = self._config.jitter
        return max(0, delay * (1 + random.uniform(-jitter, jitter)))

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    async def _orphans(
        self,
        span: tuple[int, int],
        view: ReplicaView
    ) -> dict[int, tuple[bytes, bytes]]:
        """Find stored pids of span this node does not replicate."""
        lo, hi = span
        roots = await self._local_store.list_roots(lo - 1 if lo else None, hi - lo)
        marks: dict[int, tuple[bytes, bytes]] = {}

        for pid, cell in roots:
            if pid >= hi or view.is_replica(pid) or not view.replicas[pid]:
                continue

            mark = (view.hash((pid,)), cell.to_bytes())
            if self._handed.get(pid) != mark:
                marks[pid] = mark

        return marks

    @staticmethod
    async def _pause(delay: float, stale: asyncio.Event) -> None:
        if delay <= 0:
            return

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stale.wait(), timeout=delay)

    async def _reachable(self, peer: str, phases: frozenset[MemberPhase]) -> bool:
        topology = await self._topologymgr.snapshot()
        member = topology.registry.get(peer)

        if member is None or member.phase not in phases:
            return False

        fd = self._pool.failure_detector(peer)
        return fd is None or fd.is_alive()

    async def _run(self) -> None:
        while True:
            view = await self._tracker.wait_ready()
            stale = self._tracker.stale_signal()

            try:
                await self.sweep(view, stale)
            except asyncio.CancelledError:
                raise
            except Exception as exc:    # the loop must outlive any bug
                self._stats.errors += 1
                self._stats.failed(exc)
                logger.exception("ae.sweep failed")
                await self._pause(self._config.min_interval, stale)

    def _session(
        self,
        peer: str,
        view: ReplicaView,
        stale: asyncio.Event,
        cutoff_ms: int,
        goal: Goal = "sweep",
    ) -> PeerSync:
        self._stats.sessions += 1

        limits = SessionLimits(
            keys_per_request=self._config.keys_per_request,
            max_batch_bytes=self._config.max_batch_bytes,
            max_repairs=self._config.max_repairs_per_cycle,
            max_keys_scanned=self._config.max_keys_per_cycle
        )

        return PeerSync(
            peer=peer,
            store=self._local_store,
            serializer=self._serializer,
            peer_pool=self._pool,
            view=view,
            limits=limits,
            stale=stale,
            cutoff_ms=cutoff_ms,
            progress=self._sync_progress(peer, view),
            goal=goal
        )

    def _settle(self, goal: Goal, tally: SessionTally, outcome: str) -> bool:
        self._stats.merge(tally, handoff=(goal != "sweep"))
        self._telemetry.ae_sessions_total.add(1, attributes={"outcome": outcome, "goal": goal})

        for direction, versions, size in (
            ("pull", tally.pulled, tally.pulled_bytes),
            ("push", tally.pushed, tally.pushed_bytes)
        ):
            if versions:
                attributes = {"direction": direction}
                self._telemetry.ae_versions_total.add(versions, attributes=attributes)
                self._telemetry.ae_bytes_total.add(size, attributes=attributes)

        return outcome == "ok"

    async def _sync_in_pass(self, view: ReplicaView, stale: asyncio.Event) -> bool:
        primaries = await self._sources(view)
        cutoff_ms = self._now_ms()
        goal = "sync_in"
        result = await self._exchange(
            primaries, view, stale, cutoff_ms, _SOURCE_PHASES, goal=goal
        )
        failed = bool(result.failed)

        fallback = _fallback_sources(view, primaries, result.skipped)
        logger.debug(
            "ae.%s pass sources=%d pids=%d failed=%d fallback=%d",
            goal, len(primaries), sum(len(p) for p in primaries.values()),
            len(result.failed), len(fallback)
        )
        if fallback:
            again = await self._exchange(
                fallback, view, stale, cutoff_ms, _SOURCE_PHASES, goal=goal
            )
            failed = failed or bool(again.failed)

        return failed

    @staticmethod
    async def _sources(view: ReplicaView) -> dict[str, list[int]]:
        sources: dict[str, list[int]] = {}

        for pid in range(len(view.replicas)):
            if pid % _YIELD_EVERY == 0:
                await asyncio.sleep(0)

            if not view.is_replica(pid):
                continue

            primary = view.primary_source(pid)
            if primary is not None:
                sources.setdefault(primary, []).append(pid)

        return sources

    def _sync_progress(self, peer: str, view: ReplicaView) -> SyncProgress:
        key = (peer, view.serial)
        if key not in self._progress:
            self._progress = {k: v for k, v in self._progress.items() if k[1] == view.serial}
            if len(self._progress) >= 256: # todo(souls): do not hardcode
                self._progress.pop(next(iter(self._progress)))
            self._progress[key] = SyncProgress()

        return self._progress[key]

    def _sweep_done(
        self,
        outcome: str,
        units: int,
        before: dict[str, int],
        started: float
    ) -> None:
        self._telemetry.ae_sweeps_total.add(1, attributes={"outcome": outcome})
        after = self._stats.progress()
        delta = {k: after[k] - before[k] for k in after}
        busy = delta["pulled"] or delta["pushed"] or delta["errors"]
        logger.log(
            logging.INFO if busy else logging.DEBUG,
            "ae.sweep %s units=%d sessions=%d diverged=%d pulled=%d (%s) pushed=%d (%s) errors=%d in %.2fs",
            outcome, units, delta["sessions"], delta["diverged"], delta["pulled"], human_bytes(delta["bytes_pulled"]),
            delta["pushed"], human_bytes(delta["bytes_pushed"]), delta["errors"], time.monotonic() - started
        )


def initiates(node_id: str, peer: str) -> bool:
    """
    Whether ``node_id`` initiates the sessions of the pair it forms with ``peer``.

    Both sides evaluate the same hash of the sorted pair, so exactly one of
    them initiates; hashing (rather than "smaller id wins") spreads the
    initiator role evenly across nodes.
    """
    lo, hi = sorted((node_id, peer))

    hash_ = hashlib.blake2b(lo.encode() + b"\x00" + hi.encode(), digest_size=1)
    digest = hash_.digest()[0]

    return (node_id == lo) == (digest & 1 == 0)


def _count_reached(reached: dict[int, int], pids: list[int], recent: set[int]) -> None:
    """
    Count a replica as reached for every pid fully handed off to it.

    A pid holding versions still inside the grace window is not counted: its
    root will not change once they age, so marking it handed now would skip
    them forever.
    """
    for pid in pids:
        if pid not in recent:
            reached[pid] = reached.get(pid, 0) + 1


def _fallback_sources(
    view: ReplicaView,
    primaries: dict[str, list[int]],
    down: set[str]
) -> dict[str, list[int]]:
    fallback: dict[str, list[int]] = {}

    for primary in down:
        for pid in primaries.get(primary, []):
            for peer in view.sources_of(pid):
                if peer not in (primary, view.node_id):
                    fallback.setdefault(peer, []).append(pid)

    return fallback


def _log_incomplete(goal: str, passes: int, stale: asyncio.Event, retry: float) -> None:
    if stale.is_set():
        logger.debug("ae.%s pass=%d interrupted by a view change", goal, passes)
        return

    logger.debug("ae.%s pass=%d incomplete, retrying in %.2fs", goal, passes, retry)


def _targets(view: ReplicaView, pids: Iterable[int]) -> dict[str, list[int]]:
    """Group pids by target replicas"""
    targets: dict[str, list[int]] = {}

    for pid in pids:
        for peer in view.replicas[pid]:
            targets.setdefault(peer, []).append(pid)

    return targets
