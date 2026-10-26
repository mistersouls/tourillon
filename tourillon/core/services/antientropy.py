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

"""Data anti-entropy: a background loop that heals diverged replicas.

Why: the write path, hinted handoff and partition transfers each leave
narrow gaps (a write landing behind a transfer cursor, a hint delivered while
the ring moves, a node that applied a topology late). Anti-entropy closes
them by comparing digests, independently of how the gap appeared.

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
  every pid of the slice this node stores but no longer replicates, its
  versions are pushed (one way) to the current replicas.

Work per node is therefore flat in the cluster size: a larger cluster means
fewer partitions per node, not more requests.

Storm safety
------------

- one session at a time per node as initiator; the responder serves at most
  ``concurrency`` requests and refuses the rest with ``busy``;
- every refusal carrying ``retry_after`` defers that peer; transport
  failures defer it with exponential backoff;
- randomised start and spacing, so nodes never fall into step after a ring
  change;
- per-session budgets on scanned keys and copied versions.

Ring changes
------------

The replica view is rebuilt by ``ViewTracker`` after the ring settles. A
change sets the view's stale signal: the running session stops at its next
request, the sweep is abandoned and a new plan is drawn on the new view.
Nothing is lost by stopping: the digests still differ, so the next sweep
finds the same work again.
"""

import asyncio
import contextlib
import hashlib
import random
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from tourillon.core.ae.local import LocalStore
from tourillon.core.ae.responder import AntiEntropyResponder
from tourillon.core.ae.sync import (
    BudgetSpentError,
    Call,
    PeerSync,
    SessionLimits,
    SessionTally,
    ViewChangedError,
)
from tourillon.core.ae.tracker import ViewTracker
from tourillon.core.ae.view import ReplicaView
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.config import AntiEntropyConfig
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.storage import Storage
from tourlib import Serializer
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ProtocolError

# Errors that end one session without stopping the loop.
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
    view_changes: int = 0
    budget_stops: int = 0
    errors: int = 0
    rejects: dict[str, int] = field(default_factory=dict)
    last_failure: str | None = None
    last_failure_at: str | None = None

    def merge(self, tally: SessionTally, *, handoff: bool) -> None:
        self.roots_compared += tally.roots_compared
        self.pids_diverged += tally.pids_diverged
        self.keys_scanned += tally.keys_scanned
        self.pulled += tally.pulled

        if handoff:
            self.handed_off += tally.pushed
        else:
            self.pushed += tally.pushed

    def failed(self, exc: Exception) -> None:
        self.last_failure = f"{type(exc).__name__}: {exc}"
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
            "view_changes": self.view_changes,
            "budget_stops": self.budget_stops,
            "errors": self.errors,
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
        storage: Storage,
        partitioner: Partitioner,
        topologymgr: TopologyManager,
        peer_pool: PeerPool,
        serializer: Serializer,
        replication_factor: int,
        config: AntiEntropyConfig,
        *,
        clock: Callable[[], float] = time.time,
        backoff: BackoffRetry | None = None,
    ) -> None:
        self._node_id = node_id
        self._partitioner = partitioner
        self._topologymgr = topologymgr
        self._pool = peer_pool
        self._serializer = serializer
        self._config = config
        self._clock = clock
        self.local = LocalStore(storage, partitioner)

        self.tracker = ViewTracker(
            node_id, topologymgr, partitioner, replication_factor, config.settle_delay
        )

        self.responder = AntiEntropyResponder(
            self.local, self.tracker, config.concurrency
        )

        self._backoff = PeerBackoff(backoff or BackoffRetry(), time.monotonic)
        # pid -> (view hash, local root) at the last complete handoff: an
        # orphan partition is only pushed again once it or its replicas change.
        self._handed: dict[int, tuple[bytes, bytes]] = {}
        self._stats = AntiEntropyStats()
        self._task: asyncio.Task[None] | None = None

    @property
    def stats(self) -> AntiEntropyStats:
        return self._stats

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def to_dict(self) -> dict[str, Any]:
        view = self.tracker.view
        return {
            "enabled": self._config.enabled,
            "running": self.running,
            "serial": None if view is None else view.serial,
            "served_applied": self.responder.applied,
            "served_rejected": self.responder.rejected,
            **self._stats.to_dict(),
        }

    async def start(self) -> None:
        """Start the view tracker, the responder and the sweep loop."""
        if not self._config.enabled or self.running:
            return

        self.tracker.start()
        self.responder.start()
        self._task = asyncio.create_task(self._run(), name="antientropy-loop")
        logger.info("antientropy.started node=%s", self._node_id)

    async def stop(self) -> None:
        """Stop sweeping, then drain admitted requests; never touches data."""
        task, self._task = self._task, None

        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await self.responder.stop()
        await self.tracker.stop()

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
                logger.exception("antientropy.sweep failed")
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

        for unit in units:
            await _pause(delay, stale)

            if stale.is_set():
                self._stats.view_changes += 1
                return False

            await self.run_unit(unit, view, stale)
            delay = self._jittered(interval)

        self._stats.sweeps += 1

        return True

    async def run_unit(
        self, unit: Unit, view: ReplicaView, stale: asyncio.Event
    ) -> None:
        if unit.peer is None:
            await self._handoff(unit, view, stale)
            return

        if not await self._eligible(unit.peer):
            self._stats.skipped += 1
            return

        sync = self._session(unit.peer, view, stale)
        await self._guard(unit.peer, sync.sync_roots(unit.pids))
        self._stats.merge(sync.tally, handoff=False)

    async def _handoff(
        self, unit: Unit, view: ReplicaView, stale: asyncio.Event
    ) -> None:
        """Push versions of partitions this node no longer replicates.

        One way only: the replicas own these partitions now, so nothing is
        pulled, and the local copy is left for the GC.
        """
        marks, targets = await self._orphans(unit.span, view)
        reached: dict[int, int] = {}

        for peer, pids in sorted(targets.items()):
            if not await self._eligible(peer):
                self._stats.skipped += 1
                continue

            sync = self._session(peer, view, stale, push_only=True)
            ok = await self._guard(peer, sync.sync_roots(pids))
            self._stats.merge(sync.tally, handoff=True)

            if ok:
                _count_reached(reached, pids, sync.recent)

        for pid, mark in marks.items():
            if reached.get(pid, 0) == len(view.replicas[pid]):
                self._handed[pid] = mark

    async def _orphans(
        self, span: tuple[int, int], view: ReplicaView
    ) -> tuple[dict[int, tuple[bytes, bytes]], dict[str, list[int]]]:
        """Find stored pids of ``span`` this node does not replicate, by target."""
        lo, hi = span
        roots, _ = await self.local.root_page(lo - 1 if lo else None, hi - lo)
        marks: dict[int, tuple[bytes, bytes]] = {}
        targets: dict[str, list[int]] = {}

        for pid, cell in roots:
            if pid >= hi or view.is_replica(pid) or not view.replicas[pid]:
                continue

            mark = (view.view_hash((pid,)), cell.to_bytes())

            if self._handed.get(pid) == mark:
                continue

            marks[pid] = mark

            for peer in view.replicas[pid]:
                targets.setdefault(peer, []).append(pid)

        return marks, targets

    def _session(
        self,
        peer: str,
        view: ReplicaView,
        stale: asyncio.Event,
        *,
        push_only: bool = False,
    ) -> PeerSync:
        self._stats.sessions += 1

        limits = SessionLimits(
            keys_per_request=self._config.keys_per_request,
            max_batch_bytes=self._config.max_batch_bytes,
            max_repairs=self._config.max_repairs_per_cycle,
            max_keys_scanned=self._config.max_keys_per_cycle,
        )

        cutoff_ms = int((self._clock() - self._config.grace) * 1000)

        return PeerSync(
            self.local,
            self._caller(peer),
            view,
            stale,
            limits,
            cutoff_ms,
            push_only=push_only,
        )

    async def _guard(self, peer: str, work: Coroutine[Any, Any, None]) -> bool:
        """Run one session; turn its failure into backoff and counters."""
        try:
            await work
        except ViewChangedError:
            self._stats.view_changes += 1
            return False
        except BudgetSpentError as exc:
            self._stats.budget_stops += 1
            logger.info("antientropy.budget spent peer=%s: %s", peer, exc)
            return False
        except _SESSION_ERRORS as exc:
            self._failed(peer, exc)
            return False

        self._backoff.succeeded(peer)

        return True

    def _failed(self, peer: str, exc: Exception) -> None:
        self._stats.failed(exc)

        if isinstance(exc, AntiEntropyRejectError):
            self._stats.rejects[exc.code] = self._stats.rejects.get(exc.code, 0) + 1
            if exc.retry_after > 0:
                self._backoff.defer(peer, exc.retry_after)
                return
        else:
            self._stats.errors += 1

        self._backoff.failed(peer)
        logger.warning("antientropy.session failed peer=%s: %s", peer, exc)

    async def _eligible(self, peer: str) -> bool:
        """Only READY, alive, non-deferred peers take part in sessions."""
        if self._backoff.deferred(peer):
            return False

        member = (await self._topologymgr.snapshot()).registry.get(peer)

        if member is None or member.phase is not MemberPhase.READY:
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


def _count_reached(reached: dict[int, int], pids: list[int], recent: set[int]) -> None:
    """Count a replica as reached for every pid fully handed off to it.

    A pid holding versions still inside the grace window is not counted: its
    root will not change once they age, so marking it handed now would skip
    them forever.
    """
    for pid in pids:
        if pid not in recent:
            reached[pid] = reached.get(pid, 0) + 1


async def _pause(delay: float, stale: asyncio.Event) -> None:
    """Sleep ``delay`` seconds, waking early if the view goes stale."""
    if delay <= 0:
        return

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stale.wait(), delay)
