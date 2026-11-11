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

"""Data anti-entropy: the one mechanism that copies versions between nodes.

Versions are immutable, so replicating them is only ever "make node N hold
version V". Every producer of such a need goes through this service:

- the write path and read repair record a durable *replication obligation*
  (META) when a replica missed a version or a handoff stands in for it; the
  obligation is then scheduled in a bounded in-memory hotqueue and pushed by
  the *delivery worker*;
- the *sweep* compares digests with every co-replica and hands off the
  versions of partitions this node stores but no longer replicates;
- the *join goal* pulls, from the nodes serving them today, every partition
  a JOINING node will own; the *drain goal* pushes every version a DRAINING
  node stores to the replicas that replace it.

The direction of every copy follows target ownership (see ``ReplicaView``):
callers never choose between push and pull.

Priorities
----------

Obligations are explicit, cheap and urgent; the sweep is a safety net. The
delivery worker runs on its own task and the sweep waits for the hotqueue to
be idle before each unit. Obligations that do not fit in the hotqueue, or
that were persisted before a restart, are reloaded from META once it drains,
still ahead of the sweep. Obligations of a peer that cannot be reached now
stay in META and are retried after its backoff.

How a sweep is organised
------------------------

Once per ``sweep_period`` this node walks a *plan* of units, one at a time,
spaced evenly over the period (``sweep_period / len(plan)``, at least
``min_interval``, randomised by ``jitter``). Units come in two kinds:

- *replica units*: for every co-replica ``peer``, the pids both nodes
  replicate, cut into slices of ``pids_per_cycle``. Only one side of each
  pair initiates (a hash of the sorted pair decides), so a pair is compared
  once per sweep, not twice;
- *handoff units*: the pid space cut into slices of ``pids_per_cycle``; for
  every pid of the slice this node stores but does not replicate, its
  versions are pushed to the target replicas.

Work per node is therefore flat in the cluster size: a larger cluster means
fewer partitions per node, not more requests.

Storm safety
------------

- one sweep session at a time per node as initiator; the responder serves at
  most ``concurrency`` requests and refuses the rest with ``busy``;
- every refusal carrying ``retry_after`` defers that peer; transport
  failures defer it with exponential backoff;
- randomised start and spacing, so nodes never fall into step after a ring
  change;
- per-session budgets on scanned keys and copied versions.

Ring changes
------------

The replica view is rebuilt by ``ViewTracker`` after the ring or an owner's
phase settles. A change sets the view's stale signal: the running session
stops at its next request, the sweep is abandoned and a new plan is drawn on
the new view. Nothing is lost by stopping: the digests still differ and the
obligations are still in META, so the work is found again.
"""

import asyncio
import contextlib
import hashlib
import logging
import random
import time
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import groupby
from typing import Any

from tourillon.core.ae.obligations import ObligationQueue
from tourillon.core.ae.responder import AntiEntropyResponder
from tourillon.core.ae.sync import (
    BudgetSpentError,
    Call,
    PeerSync,
    SessionLimits,
    SessionTally,
    SyncProgress,
    ViewChangedError,
    delivery_count,
    human_bytes,
)
from tourillon.core.ae.tracker import ViewTracker
from tourillon.core.ae.view import ReplicaView
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import describe, logger
from tourillon.core.models.antientropy import (
    MAX_BATCH_BYTES,
    MAX_PAGE_LIMIT,
    PushRequest,
    Scope,
    record_size,
)
from tourillon.core.models.config import AntiEntropyConfig
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    ObligationKey,
    Record,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.gc import CollectionScheduler
from tourillon.core.ports.telemetry import Telemetry
from tourillon.core.storage.store import LocalStore
from tourillon.core.telemetry import build_noop_telemetry
from tourlib import Serializer
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ProtocolError

# Errors that end one session or one delivery without stopping the loops.
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

# Phases of the nodes a joining node catches up from.
_SOURCE_PHASES = frozenset({MemberPhase.READY, MemberPhase.DRAINING})

# Pids walked between two event-loop yields when scanning the whole map.
_YIELD_EVERY = 4096


@dataclass
class AntiEntropyStats:
    """Counters of this node's anti-entropy activity since start."""

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
    errors: int = 0
    obligations_delivered: int = 0
    obligations_dropped: int = 0
    obligations_deferred: int = 0
    join_passes: int = 0
    drain_passes: int = 0
    rejects: dict[str, int] = field(default_factory=dict)
    last_failure: str | None = None
    last_failure_at: str | None = None

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

    def failed(self, exc: Exception) -> None:
        self.last_failure = describe(exc)
        self.last_failure_at = datetime.now(UTC).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "sweeps": self.sweeps,
            "sessions": self.sessions,
            "skipped": self.skipped,
            "roots_compared": self.roots_compared,
            "pids_diverged": self.pids_diverged,
            "keys_scanned": self.keys_scanned,
            "pulled": self.pulled,
            "pushed": self.pushed,
            "handed_off": self.handed_off,
            "bytes_pulled": self.bytes_pulled,
            "bytes_pushed": self.bytes_pushed,
            "view_changes": self.view_changes,
            "budget_stops": self.budget_stops,
            "errors": self.errors,
            "obligations_delivered": self.obligations_delivered,
            "obligations_dropped": self.obligations_dropped,
            "obligations_deferred": self.obligations_deferred,
            "join_passes": self.join_passes,
            "drain_passes": self.drain_passes,
            "rejects": dict(self.rejects),
            "last_failure": self.last_failure,
            "last_failure_at": self.last_failure_at,
        }


class PeerBackoff:
    """Per-peer deferral: explicit ``retry_after`` hints and failure backoff."""

    def __init__(self, backoff: BackoffRetry, clock: Callable[[], float]) -> None:
        self._backoff = backoff
        self._clock = clock
        self._until: dict[str, float] = {}
        self._delay: dict[str, float] = {}

    def deferred(self, peer: str) -> bool:
        until = self._until.get(peer)
        return until is not None and self._clock() < until

    def succeeded(self, peer: str) -> None:
        self._until.pop(peer, None)
        self._delay.pop(peer, None)

    def defer(self, peer: str, seconds: float) -> None:
        """Honour a responder hint without growing the failure backoff."""
        self._until[peer] = max(self._until.get(peer, 0.0), self._clock() + seconds)

    def failed(self, peer: str) -> None:
        delay = self._delay.get(peer, self._backoff.initial_delay)
        jitter = self._backoff.jitter
        pause = max(0.0, delay * (1 + random.uniform(-jitter, jitter)))
        self._until[peer] = self._clock() + pause
        self._delay[peer] = min(delay * self._backoff.factor, self._backoff.max_delay)


@dataclass(frozen=True, slots=True)
class Unit:
    """One step of a sweep: a replica slice with a peer, or a handoff span."""

    peer: str | None
    pids: tuple[int, ...] = ()
    span: tuple[int, int] = (0, 0)


@dataclass
class Exchange:
    """Outcome of sessions with several peers over their pids.

    - ``reached[pid]``: peers that completed a session covering ``pid``;
    - ``skipped``: peers that are down or not taking part, not retried;
    - ``failed``: peers to retry (deferred, refused, budget, view change).
    """

    reached: dict[int, int] = field(default_factory=dict)
    skipped: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)


def initiates(node_id: str, peer: str) -> bool:
    """Whether ``node_id`` initiates the sessions of the pair it forms with ``peer``.

    Both sides evaluate the same hash of the sorted pair, so exactly one of
    them initiates; hashing (rather than "smaller id wins") spreads the
    initiator role evenly across nodes.
    """
    low, high = sorted((node_id, peer))

    digest = hashlib.blake2b(
        low.encode() + b"\x00" + high.encode(), digest_size=1
    ).digest()

    return (node_id == low) == (digest[0] & 1 == 0)


def plan(view: ReplicaView, total: int, pids_per_cycle: int) -> list[Unit]:
    """Cut one sweep into units of at most ``pids_per_cycle`` partitions."""
    units: list[Unit] = []

    for peer, pids in sorted(view.shared.items()):
        if initiates(view.node_id, peer):
            units.extend(
                Unit(peer, pids[i : i + pids_per_cycle])
                for i in range(0, len(pids), pids_per_cycle)
            )

    units.extend(
        Unit(None, span=(lo, min(lo + pids_per_cycle, total)))
        for lo in range(0, total, pids_per_cycle)
    )

    return units


class AntiEntropyService:
    def __init__(
        self,
        node_id: str,
        store: LocalStore,
        partitioner: Partitioner,
        topologymgr: TopologyManager,
        peer_pool: PeerPool,
        serializer: Serializer,
        replication_factor: int,
        config: AntiEntropyConfig,
        *,
        obligations: ObligationQueue | None = None,
        clock: Callable[[], float] = time.time,
        backoff: BackoffRetry | None = None,
        telemetry: Telemetry | None = None,
        gc: CollectionScheduler,
    ) -> None:
        self._node_id = node_id
        self._telemetry = telemetry or build_noop_telemetry()
        self._partitioner = partitioner
        self._topologymgr = topologymgr
        self._pool = peer_pool
        self._serializer = serializer
        self._config = config
        self._clock = clock
        self._store = store
        if gc is None:
            raise ValueError("GC scheduler is required")
        self._gc = gc
        self.obligations = obligations or ObligationQueue(config.hotqueue_size)

        self.tracker = ViewTracker(
            node_id,
            topologymgr,
            partitioner,
            replication_factor,
            config.settle_delay,
            config.settle_quiet,
        )

        self.responder = AntiEntropyResponder(
            self._store, self.tracker, config.concurrency, gc
        )

        self._backoff = PeerBackoff(backoff or BackoffRetry(), time.monotonic)
        # pid -> (view hash, local root) at the last complete handoff: an
        # orphan partition is only pushed again once it or its replicas change.
        self._handed: dict[int, tuple[bytes, bytes]] = {}
        self._stats = AntiEntropyStats()
        self._tasks: list[asyncio.Task[None]] = []
        self._batch = min(config.keys_per_request, MAX_PAGE_LIMIT)
        self._batch_bytes = min(config.max_batch_bytes, MAX_BATCH_BYTES)
        self._retry_delay = max(1.0, config.min_interval)
        self._progress: dict[tuple[str, str], SyncProgress] = {}

    @property
    def stats(self) -> AntiEntropyStats:
        return self._stats

    @property
    def running(self) -> bool:
        return any(not task.done() for task in self._tasks)

    def to_dict(self) -> dict[str, Any]:
        view = self.tracker.view
        return {
            "running": self.running,
            "serial": None if view is None else view.serial,
            "served_applied": self.responder.applied,
            "served_rejected": self.responder.rejected,
            "hotqueue": self.obligations.to_dict(),
            **self._stats.to_dict(),
        }

    async def start(self) -> None:
        """Start the view tracker, the responder, delivery and the sweep."""
        if self.running:
            return

        self.tracker.start()
        self.responder.start()
        self._tasks = [
            asyncio.create_task(self._deliver_loop(), name="ae-delivery"),
            asyncio.create_task(self._run(), name="ae-loop"),
        ]

        logger.info("ae.started node=%s", self._node_id)

    async def stop(self) -> None:
        """Stop the loops, then drain admitted requests; never touches data."""
        tasks, self._tasks = self._tasks, []

        for task in tasks:
            task.cancel()

        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await self.responder.stop()
        await self.tracker.stop()

    # -- join and drain goals ------------------------------------------------

    async def wait_joined(self) -> None:
        """Return once every partition this node will own is caught up.

        Each owned pid is copied from a single primary source
        (``ReplicaView.primary_source``): joiners replacing as many nodes
        pair one to one with them, so the load spreads instead of every
        joiner reading every replica. Only when a primary is down does the
        pid fall back to its other sources, so a join cannot hang on a dead
        node. Writes that land meanwhile reach this node as obligations;
        versions held only by a staying replica are left to the sweep.
        Refused or interrupted sessions are retried, on the new view if the
        ring moved.
        """
        started = time.monotonic()
        passes = 0
        copied = (self._stats.pulled, self._stats.bytes_pulled)
        logger.info("ae.join started node=%s", self._node_id)

        while True:
            view = await self.tracker.wait_ready()
            stale = self.tracker.stale_signal()

            if not view.owns:
                # The view predates this node's entry in the ring.
                logger.debug(
                    "ae.join waiting for a view that places node=%s",
                    self._node_id,
                )
                await stale.wait()
                continue

            failed = await self._join_pass(view, stale)
            self._stats.join_passes += 1
            passes += 1

            if not failed and not stale.is_set():
                logger.info(
                    "ae.join synced node=%s passes=%d pulled=%d (%s) " "elapsed=%.2fs",
                    self._node_id,
                    passes,
                    self._stats.pulled - copied[0],
                    human_bytes(self._stats.bytes_pulled - copied[1]),
                    time.monotonic() - started,
                )
                return

            _log_incomplete("join", passes, stale, self._retry_delay)
            await _pause(self._retry_delay, stale)

    async def _join_pass(self, view: ReplicaView, stale: asyncio.Event) -> bool:
        primaries = await self._join_sources(view)
        cutoff_ms = self._now_ms()
        result = await self._exchange(
            primaries, view, stale, cutoff_ms, _SOURCE_PHASES, goal="join"
        )
        failed = bool(result.failed)

        fallback = _fallback_sources(view, primaries, result.skipped)
        logger.debug(
            "ae.join pass sources=%d pids=%d failed=%d fallback=%d",
            len(primaries),
            sum(len(pids) for pids in primaries.values()),
            len(result.failed),
            len(fallback),
        )
        if fallback:
            again = await self._exchange(
                fallback, view, stale, cutoff_ms, _SOURCE_PHASES, goal="join"
            )
            failed = failed or bool(again.failed)

        return failed

    async def drain(self) -> None:
        """Return once every stored version reached the replicas replacing us.

        Called while this node is DRAINING, so it replicates nothing in the
        target view and every stored pid is handed off. A pid is done when
        every live target replica completed a session for it, and at least
        one did.
        """
        started = time.monotonic()
        passes = 0
        copied = (self._stats.handed_off, self._stats.bytes_pushed)
        logger.info("ae.drain started node=%s", self._node_id)

        while True:
            view = await self.tracker.wait_ready()
            stale = self.tracker.stale_signal()

            if view.owns:
                # The view predates this node's DRAINING phase.
                logger.debug(
                    "ae.drain waiting for a view without node=%s",
                    self._node_id,
                )
                await stale.wait()
                continue

            done = await self._drain_pass(view, stale)
            self._stats.drain_passes += 1
            passes += 1

            if done:
                logger.info(
                    "ae.drain handed off node=%s passes=%d pushed=%d (%s) "
                    "elapsed=%.2fs",
                    self._node_id,
                    passes,
                    self._stats.handed_off - copied[0],
                    human_bytes(self._stats.bytes_pushed - copied[1]),
                    time.monotonic() - started,
                )
                return

            _log_incomplete("drain", passes, stale, self._retry_delay)
            await _pause(self._retry_delay, stale)

    @staticmethod
    async def _join_sources(view: ReplicaView) -> dict[str, list[int]]:
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

    async def _drain_pass(self, view: ReplicaView, stale: asyncio.Event) -> bool:
        cutoff_ms = self._now_ms()
        complete = True
        after: int | None = None

        while True:
            roots = await self._store.list_root(after, self._config.pids_per_cycle)
            after = roots[-1][0] if roots else None
            pids = [pid for pid, _ in roots]
            result = await self._exchange(
                _targets(view, pids),
                view,
                stale,
                cutoff_ms,
                _SESSION_PHASES,
                goal="drain",
            )
            complete = complete and _handed_all(view, pids, result)

            if after is None or stale.is_set():
                return complete and not stale.is_set()

    async def _run(self) -> None:
        while True:
            view = await self.tracker.wait_ready()
            stale = self.tracker.stale_signal()

            try:
                await self.sweep(view, stale)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # the loop must outlive any bug
                self._stats.errors += 1
                self._stats.failed(exc)
                logger.exception("ae.sweep failed")
                await _pause(self._config.min_interval, stale)

    async def sweep(self, view: ReplicaView, stale: asyncio.Event) -> bool:
        """Run one sweep over ``view``; return False if the view went stale."""
        units = plan(
            view, self._partitioner.total_partitions, self._config.pids_per_cycle
        )

        random.shuffle(units)

        interval = max(
            self._config.min_interval, self._config.sweep_period / len(units)
        )

        # A random first delay de-synchronises nodes that all rebuilt their
        # view at the same moment after a ring change.
        delay = random.uniform(0.0, interval)
        started = time.monotonic()
        before = self._stats.progress()

        for unit in units:
            await _pause(delay, stale)
            # Obligations go first: the sweep only finds what they missed.
            await self.obligations.wait_idle()

            if stale.is_set():
                self._stats.view_changes += 1
                self._sweep_done("interrupted", len(units), before, started)
                return False

            await self.run_unit(unit, view, stale)
            delay = self._jittered(interval)

        self._stats.sweeps += 1
        self._sweep_done("complete", len(units), before, started)

        return True

    def _sweep_done(
        self, outcome: str, units: int, before: dict[str, int], started: float
    ) -> None:
        """Count the sweep and summarise what it did.

        A sweep that copied nothing and met no error is the steady state: it is
        logged at DEBUG so that INFO only shows sweeps that repaired data.
        """
        self._telemetry.ae_sweeps_total.add(1, attributes={"outcome": outcome})
        after = self._stats.progress()
        delta = {name: after[name] - before[name] for name in after}
        busy = delta["pulled"] or delta["pushed"] or delta["errors"]
        logger.log(
            logging.INFO if busy else logging.DEBUG,
            "ae.sweep %s units=%d sessions=%d diverged=%d pulled=%d (%s) "
            "pushed=%d (%s) errors=%d in %.2fs",
            outcome,
            units,
            delta["sessions"],
            delta["diverged"],
            delta["pulled"],
            human_bytes(delta["bytes_pulled"]),
            delta["pushed"],
            human_bytes(delta["bytes_pushed"]),
            delta["errors"],
            time.monotonic() - started,
        )

    async def run_unit(
        self, unit: Unit, view: ReplicaView, stale: asyncio.Event
    ) -> None:
        if unit.peer is None:
            await self._handoff(unit, view, stale)
            return

        if not await self._eligible(unit.peer):
            self._stats.skipped += 1
            return

        sync = self._session(unit.peer, view, stale, self._cutoff_ms())
        outcome = await self._guard(unit.peer, sync.sync_roots(unit.pids))
        self._settle("sweep", sync.tally, outcome)

    async def _handoff(
        self, unit: Unit, view: ReplicaView, stale: asyncio.Event
    ) -> None:
        """Push versions of partitions this node does not replicate.

        Direction follows ownership: the peers replicate these pids and this
        node does not, so versions only flow towards them. The local copy is
        left for the GC.
        """
        marks = await self._orphans(unit.span, view)
        result = await self._exchange(
            _targets(view, marks), view, stale, self._cutoff_ms(), _SESSION_PHASES
        )

        for pid, mark in marks.items():
            if result.reached.get(pid, 0) == len(view.replicas[pid]):
                self._handed[pid] = mark

    async def _orphans(
        self, span: tuple[int, int], view: ReplicaView
    ) -> dict[int, tuple[bytes, bytes]]:
        """Find stored pids of ``span`` this node does not replicate."""
        lo, hi = span
        roots = await self._store.list_root(lo - 1 if lo else None, hi - lo)
        marks: dict[int, tuple[bytes, bytes]] = {}

        for pid, cell in roots:
            if pid >= hi or view.is_replica(pid) or not view.replicas[pid]:
                continue

            mark = (view.view_hash((pid,)), cell.to_bytes())

            if self._handed.get(pid) != mark:
                marks[pid] = mark

        return marks

    async def _exchange(
        self,
        targets: dict[str, list[int]],
        view: ReplicaView,
        stale: asyncio.Event,
        cutoff_ms: int,
        phases: frozenset[MemberPhase],
        *,
        goal: str | None = None,
    ) -> Exchange:
        """Run one session per peer over its pids.

        A ``goal`` (join or drain) counts a pid as reached even with versions
        past the cutoff: goals use the current time as cutoff, so nothing is
        left. Its sessions report their progress at INFO.
        """
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

            sync = self._session(peer, view, stale, cutoff_ms, goal)
            started = time.monotonic()
            outcome = await self._guard(peer, sync.sync_roots(pids))
            ok = self._settle(goal or "handoff", sync.tally, outcome)

            if goal is not None:
                _log_session(goal, peer, len(pids), ok, sync.tally, started)

            if not ok:
                result.failed.add(peer)
                continue

            _count_reached(result.reached, pids, sync.recent if goal is None else set())

        return result

    # -- obligation delivery -------------------------------------------------

    async def _deliver_loop(self) -> None:
        while True:
            view = await self.tracker.wait_ready()

            try:
                await self._deliver_step(view)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # the loop must outlive any bug
                self._stats.errors += 1
                self._stats.failed(exc)
                self.obligations.backlog = True
                logger.exception("ae.delivery failed")
                await asyncio.sleep(self._retry_delay)

    async def _deliver_step(self, view: ReplicaView) -> None:
        queue = self.obligations

        if len(queue):
            batch = queue.take(self._batch)
            try:
                await self.deliver(batch, view)
            finally:
                queue.settle(len(batch))
            return

        if queue.backlog:
            await self._reload()

        if not len(queue):
            await queue.wait(self._retry_delay if queue.backlog else None)

    async def _reload(self) -> None:
        """Schedule the obligations persisted in META, as many as fit.

        Obligations of deferred peers stay in META; the backlog flag stays
        raised so they are reloaded once their backoff expires.
        """
        queue = self.obligations
        queue.backlog = False
        after: bytes | None = None
        pending = False

        while True:
            page = await self._store.list_obligations(after, self._batch)
            after = page[-1].to_bytes() if len(page) == self._batch else None
            ready = [ob for ob in page if not self._backoff.deferred(ob.target)]
            pending = pending or len(ready) < len(page)
            self.obligations.submit(ready)

            if after is None or self.obligations.backlog:
                break

        queue.backlog = pending

    async def deliver(self, batch: Sequence[ObligationKey], view: ReplicaView) -> None:
        """Push the versions of ``batch`` to their targets, then forget them.

        A captured debt is covered by the latest local version. Missing
        covering data is reported and retained; only a target no longer in
        the preference list makes a debt obsolete without delivery.
        """
        owed = {ob for ob in batch if self._owed(ob, view)}
        records = await self._store.record_from_obligations(list(owed))
        obsolete = [ob for ob in batch if ob not in owed]
        unavailable = [ob for ob in owed if ob.key not in records]
        if unavailable:
            self.obligations.backlog = True
            logger.error(
                "ae.obligations no covering local version count=%d", len(unavailable)
            )
        ready = sorted((ob for ob in owed if ob.key in records), key=_delivery_order)

        for target, group in groupby(ready, key=lambda ob: ob.target):
            await self._deliver_to(target, list(group), records, view)

        if obsolete:
            await self._store.remove_obligations(obsolete)
            self._stats.obligations_dropped += len(obsolete)
            self._obligation_event("dropped", len(obsolete))
            logger.debug(
                "ae.obligations dropped count=%d (version gone or target no "
                "longer a replica)",
                len(obsolete),
            )

    def _owed(self, obligation: ObligationKey, view: ReplicaView) -> bool:
        pid = obligation.key.pid.value
        return (
            obligation.target != self._node_id
            and view.covers((pid,))
            and view.is_replica(pid, obligation.target)
        )

    async def _deliver_to(
        self,
        target: str,
        obligations: list[ObligationKey],
        records: Mapping[Key, Record],
        view: ReplicaView,
    ) -> None:
        if not await self._eligible(target):
            self._park(target, len(obligations))
            return

        for pid, group in groupby(obligations, key=lambda ob: ob.key.pid.value):
            if not await self._push_obligations(
                target, pid, list(group), records, view
            ):
                self.obligations.backlog = True
                return

        self._backoff.succeeded(target)
        logger.debug(
            "ae.obligations delivered target=%s count=%d", target, len(obligations)
        )

    def _park(self, target: str, count: int) -> None:
        """Leave obligations of an unreachable target in META for later."""
        if not self._backoff.deferred(target):
            self._backoff.defer(target, self._retry_delay)

        self.obligations.backlog = True
        self._stats.obligations_deferred += count
        self._obligation_event("deferred", count)
        logger.debug(
            "ae.obligations deferred target=%s count=%d (peer unreachable)",
            target,
            count,
        )

    async def _push_obligations(
        self,
        target: str,
        pid: int,
        obligations: list[ObligationKey],
        records: Mapping[Key, Record],
        view: ReplicaView,
    ) -> bool:
        call = self._caller(target)
        scope = Scope((pid,), view.view_hash((pid,)), self._store.retention_versions)

        for chunk in _chunks(obligations, records, self._batch_bytes):
            unique = {records[ob.key].key: records[ob.key] for ob in chunk}
            request = PushRequest(scope, tuple(unique.values()))

            try:
                result = await call("antientropy.push", request.to_dict())
                acknowledged = delivery_count(result)
                if acknowledged != len(unique):
                    raise ProtocolError(
                        "obligation push did not acknowledge every record"
                    )
            except _SESSION_ERRORS as exc:
                self._failed(target, exc)
                return False

            await self._store.remove_obligations(chunk)
            self._stats.obligations_delivered += len(chunk)
            self._obligation_event("delivered", len(chunk))

        return True

    # -- sessions and peers --------------------------------------------------

    def _session(
        self,
        peer: str,
        view: ReplicaView,
        stale: asyncio.Event,
        cutoff_ms: int,
        goal: str | None = None,
    ) -> PeerSync:
        self._stats.sessions += 1

        limits = SessionLimits(
            keys_per_request=self._config.keys_per_request,
            max_batch_bytes=self._config.max_batch_bytes,
            max_repairs=self._config.max_repairs_per_cycle,
            max_keys_scanned=self._config.max_keys_per_cycle,
        )

        return PeerSync(
            self._store,
            peer,
            self._caller(peer),
            view,
            stale,
            limits,
            cutoff_ms,
            goal,
            progress=self._sync_progress(peer, view),
            gc=self._gc,
        )

    def _sync_progress(self, peer: str, view: ReplicaView) -> SyncProgress:
        key = (peer, view.serial)
        if key not in self._progress:
            self._progress = {
                k: v for k, v in self._progress.items() if k[1] == view.serial
            }
            if len(self._progress) >= 256:
                self._progress.pop(next(iter(self._progress)))
            self._progress[key] = SyncProgress()

        return self._progress[key]

    def _cutoff_ms(self) -> int:
        return int((self._clock() - self._config.grace) * 1000)

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    async def _guard(self, peer: str, work: Coroutine[Any, Any, None]) -> str:
        """Run one session; turn its failure into backoff and counters.

        Return the session outcome: ``ok``, ``view_changed``, ``budget_spent``,
        ``deferred`` or ``failed``.
        """
        try:
            await work
        except ViewChangedError:
            self._stats.view_changes += 1
            return "view_changed"
        except BudgetSpentError as exc:
            self._stats.budget_stops += 1
            logger.info("ae.budget spent peer=%s: %s", peer, exc)
            return "budget_spent"
        except _SESSION_ERRORS as exc:
            return self._failed(peer, exc)

        self._backoff.succeeded(peer)

        return "ok"

    def _settle(self, goal: str, tally: SessionTally, outcome: str) -> bool:
        """Account one finished session; return whether it completed."""
        self._stats.merge(tally, handoff=goal != "sweep")
        metrics = self._telemetry
        metrics.ae_sessions_total.add(1, attributes={"goal": goal, "outcome": outcome})

        for direction, versions, size in (
            ("pull", tally.pulled, tally.pulled_bytes),
            ("push", tally.pushed, tally.pushed_bytes),
        ):
            if versions:
                attributes = {"direction": direction}
                metrics.ae_versions_total.add(versions, attributes=attributes)
                metrics.ae_bytes_total.add(size, attributes=attributes)

        return outcome == "ok"

    def _obligation_event(self, event: str, count: int) -> None:
        self._telemetry.obligations_total.add(count, attributes={"event": event})

    def _failed(self, peer: str, exc: Exception) -> str:
        self._stats.failed(exc)

        if isinstance(exc, AntiEntropyRejectError):
            self._stats.rejects[exc.code] = self._stats.rejects.get(exc.code, 0) + 1
            if exc.retry_after > 0:
                self._backoff.defer(peer, exc.retry_after)
                logger.debug(
                    "ae.session deferred peer=%s code=%s retry_after=%.2fs",
                    peer,
                    exc.code,
                    exc.retry_after,
                )
                return "deferred"
        else:
            self._stats.errors += 1

        self._backoff.failed(peer)
        logger.warning("ae.session failed peer=%s: %s", peer, describe(exc))
        return "failed"

    async def _eligible(self, peer: str) -> bool:
        """Alive, non-deferred peers in a phase that takes part in sessions."""
        if self._backoff.deferred(peer):
            return False

        return await self._reachable(peer, _SESSION_PHASES)

    async def _reachable(self, peer: str, phases: frozenset[MemberPhase]) -> bool:
        member = (await self._topologymgr.snapshot()).registry.get(peer)

        if member is None or member.phase not in phases:
            return False

        fd = self._pool.failure_detector(peer)

        return fd is None or fd.is_alive()

    def _caller(self, peer: str) -> Call:
        async def call(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
            envelope = build_envelope(
                kind=kind,
                payload=self._serializer.encode(payload),
                schema_id=self._serializer.schema_id,
            )

            async with self._pool.lease(peer) as client:
                response = await client.request(
                    envelope, timeout=ENVELOPE_RESPONSE_TIMEOUT
                )

            return self._expect(response, kind)

        return call

    def _expect(self, response: Envelope, kind: str) -> dict[str, Any]:
        if response.kind == f"{kind}.accept":
            data = self._serializer.decode(response.payload)
            return data if isinstance(data, dict) else {}

        if response.kind == f"{kind}.reject":
            raise AntiEntropyRejectError.from_dict(
                self._serializer.decode(response.payload)
            )

        raise ProtocolError(f"unexpected response kind {response.kind!r} to {kind!r}")

    def _jittered(self, delay: float) -> float:
        jitter = self._config.jitter
        return max(0.0, delay * (1 + random.uniform(-jitter, jitter)))


def _targets(view: ReplicaView, pids: Iterable[int]) -> dict[str, list[int]]:
    """Group ``pids`` by target replica."""
    targets: dict[str, list[int]] = {}

    for pid in pids:
        for peer in view.replicas[pid]:
            targets.setdefault(peer, []).append(pid)

    return targets


def _handed_all(view: ReplicaView, pids: Iterable[int], result: Exchange) -> bool:
    """Every pid reached each live target replica, and at least one."""
    for pid in pids:
        needed = [p for p in view.replicas[pid] if p not in result.skipped]

        if not needed or result.reached.get(pid, 0) < len(needed):
            return False

    return True


def _delivery_order(obligation: ObligationKey) -> tuple[str, bytes]:
    return obligation.target, obligation.key.to_bytes(Keyspace.TAGS)


def _chunks(
    obligations: list[ObligationKey], records: Mapping[Key, Record], max_bytes: int
) -> list[list[ObligationKey]]:
    """Split obligations into push batches of at most ``max_bytes`` of records."""
    chunks: list[list[ObligationKey]] = []
    current: list[ObligationKey] = []
    size = 0

    grouped: dict[Key, list[ObligationKey]] = {}
    for obligation in obligations:
        grouped.setdefault(records[obligation.key].key, []).append(obligation)

    for debts in grouped.values():
        cost = record_size(records[debts[0].key])

        if current and size + cost > max_bytes:
            chunks.append(current)
            current, size = [], 0

        current.extend(debts)
        size += cost

    if current:
        chunks.append(current)

    return chunks


def _fallback_sources(
    view: ReplicaView, primaries: dict[str, list[int]], down: set[str]
) -> dict[str, list[int]]:
    """Route the pids of unreachable primaries to their other sources."""
    fallback: dict[str, list[int]] = {}

    for primary in down:
        for pid in primaries.get(primary, ()):
            for peer in view.sources_of(pid):
                if peer not in (primary, view.node_id):
                    fallback.setdefault(peer, []).append(pid)

    return fallback


def _count_reached(reached: dict[int, int], pids: list[int], recent: set[int]) -> None:
    """Count a replica as reached for every pid fully handed off to it.

    A pid holding versions still inside the grace window is not counted: its
    root will not change once they age, so marking it handed now would skip
    them forever.
    """
    for pid in pids:
        if pid not in recent:
            reached[pid] = reached.get(pid, 0) + 1


def _log_session(
    goal: str, peer: str, pids: int, ok: bool, tally: SessionTally, started: float
) -> None:
    logger.info(
        "ae.%s session %s peer=%s pids=%d diverged=%d "
        "pulled=%d (%s) pushed=%d (%s) in %.2fs",
        goal,
        "done" if ok else "stopped",
        peer,
        pids,
        tally.pids_diverged,
        tally.pulled,
        human_bytes(tally.pulled_bytes),
        tally.pushed,
        human_bytes(tally.pushed_bytes),
        time.monotonic() - started,
    )


def _log_incomplete(goal: str, passes: int, stale: asyncio.Event, retry: float) -> None:
    if stale.is_set():
        logger.debug("ae.%s pass=%d interrupted by a view change", goal, passes)
        return

    logger.debug("ae.%s pass=%d incomplete, retrying in %.2fs", goal, passes, retry)


async def _pause(delay: float, stale: asyncio.Event) -> None:
    """Sleep ``delay`` seconds, waking early if the view goes stale."""
    if delay <= 0:
        return

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stale.wait(), delay)
