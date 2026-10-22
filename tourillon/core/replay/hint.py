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
import contextlib
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from tourillon.core.exceptions import StoreOperationError
from tourillon.core.log import logger
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import MAX_VALUE_DATA_LEN, Hint, Record, Tag
from tourillon.core.partition.placement import PlacementStrategy, Preference
from tourillon.core.peer import PeerPool
from tourillon.core.ports.storage import SessionProvider, Storage
from tourillon.core.storage.group import PartitionGroupHint
from tourlib import Serializer
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ProtocolError
from tourlib.transport import EnvelopeClient


@dataclass
class HintReplayStats:
    applied_total: int = 0
    failed_total: int = 0
    last_failure: str | None = None
    last_failure_at: str | None = None


@dataclass
class _Delivery:
    hint: Hint
    record: Record
    owners: frozenset[str]
    targets: tuple[Preference, ...]
    check_exists: bool
    complete: bool = True


class _TargetBackoff:
    """Defer retries per target node.

    A single unreachable peer holding many hints would otherwise be leased,
    dialled and timed out once per hint on every cycle.
    Deferring by target collapses that to one attempt
    and lets the delay grow while the peer stays down.

    There is no attempt ceiling: a hint represents an acknowledged write, so
    giving up would lose it. The backoff bounds the cost of waiting, not the
    number of tries.
    """

    def __init__(self, backoff: BackoffRetry) -> None:
        self._backoff = backoff
        self._until: dict[str, float] = {}
        self._delay: dict[str, float] = {}

    def deferred(self, target_node_id: str) -> bool:
        until = self._until.get(target_node_id)
        return until is not None and time.monotonic() < until

    def succeeded(self, target_node_id: str) -> None:
        self._until.pop(target_node_id, None)
        self._delay.pop(target_node_id, None)

    def failed(self, target_node_id: str) -> None:
        delay = self._delay.get(target_node_id, self._backoff.initial_delay)
        jitter = self._backoff.jitter
        pause = max(0.0, delay * (1 + random.uniform(-jitter, jitter)))
        self._until[target_node_id] = time.monotonic() + pause
        self._delay[target_node_id] = min(
            delay * self._backoff.factor, self._backoff.max_delay
        )


class HintReplayer:
    def __init__(
        self,
        storage: Storage,
        peer_pool: PeerPool,
        serializer: Serializer,
        total_groups: int,
        placement_strategy: PlacementStrategy,
        node_id: str,
        poll_interval: float = 0.5,
        batch_size: int = 32,
        backoff: BackoffRetry | None = None,
        concurrency: int = 4,
        max_batch_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if batch_size < 1 or concurrency < 1:
            raise ValueError("batch_size and concurrency must be positive")
        if max_batch_bytes < MAX_VALUE_DATA_LEN:
            raise ValueError("max_batch_bytes must accommodate one maximum-size value")
        self._storage = storage
        self._peer_pool = peer_pool
        self._serializer = serializer
        self._total_groups = total_groups
        self._placement = placement_strategy
        self._node_id = node_id
        self._local_writable = True
        self._local_delivery = asyncio.Lock()
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._window_size = min(batch_size, max_batch_bytes // MAX_VALUE_DATA_LEN)
        self._concurrency = concurrency
        self._backoff = _TargetBackoff(backoff or BackoffRetry())

        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._stats = HintReplayStats()

    def pause_local_writes(self) -> None:
        self._local_writable = False

    def resume_local_writes(self) -> None:
        self._local_writable = True

    async def drain_local_writes(self) -> None:
        """Wait for local deliveries admitted before pause to finish."""
        async with self._local_delivery:
            pass

    async def start(self) -> None:
        """Start the hint replay process."""
        if self._task and not self._task.done():
            return

        self._stop.clear()
        self._task = asyncio.create_task(self._replay_loop(), name="hint-replay-loop")

    async def stop(self) -> None:
        """Stop the hint replay process."""
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def wait(self) -> None:
        """Wait for durable exhaustion; callers must first settle hint producers."""
        while await self._pending_hints():
            if self._task is None or self._task.done():
                raise RuntimeError("Cannot wait for hints without a running replayer")
            await asyncio.sleep(self._poll_interval)

    async def _pending_hints(self) -> bool:
        for provider in await self._session_providers():
            hints = PartitionGroupHint(provider)
            if await hints.pending():
                return True
        return False

    def _envelope(self, kind: str, payload: dict[str, Any]) -> Envelope:
        return build_envelope(
            kind=kind,
            payload=self._serializer.encode(payload),
            schema_id=self._serializer.schema_id,
        )

    def _expect(self, response: Envelope, kind: str) -> dict[str, Any]:
        """Return the payload of a `kind` response; raise on a rejection or another kind."""
        if response.kind == kind:
            data = self._serializer.decode(response.payload)
            return data if isinstance(data, dict) else {}
        if response.kind in {"replication.write.reject", "replication.exists.reject"}:
            raise StoreOperationError.from_dict(
                self._serializer.decode(response.payload)
            )
        raise ProtocolError(
            f"unexpected response kind {response.kind!r}, expected {kind!r}"
        )

    async def _replay_loop(self) -> None:
        """Continuously poll for hints and replay them."""
        while not self._stop.is_set():
            try:
                await self._replay_once()
            except Exception as exc:
                self._stats.last_failure = f"{type(exc).__name__}: {exc}"
                self._stats.last_failure_at = datetime.now(UTC).isoformat()
                logger.warning("hint.replay cycle failed: %s", exc)
            await asyncio.sleep(self._poll_interval)

    async def _replay_once(self) -> None:
        for session_provider in await self._session_providers():
            hints = PartitionGroupHint(session_provider)
            await self._replay_group(hints)

    async def _replay_group(self, hint_group: PartitionGroupHint) -> None:
        window: list[Hint] = []
        async for hint in hint_group.iter(batch_size=self._window_size):
            window.append(hint)
            if len(window) == self._window_size:
                await self._replay_window(hint_group, window)
                window.clear()
        if window:
            await self._replay_window(hint_group, window)

    async def _prepare(self, hint: Hint) -> _Delivery | None:
        pending = self._pending(hint)
        if pending is None:
            return None
        _, original = pending
        preference_list = await self._placement.preference_list(original.key.address)
        preferences = preference_list.preferences
        owners = frozenset(entry.node_id for entry in preferences)
        if not owners:
            return None
        direct = hint.intent in owners
        targets = tuple(
            entry for entry in preferences if not direct or entry.node_id == hint.intent
        )
        return _Delivery(hint, original, owners, targets, not direct)

    async def _replay_window(
        self, group: PartitionGroupHint, hints: list[Hint]
    ) -> None:
        deliveries = []
        for hint in hints:
            delivery = await self._prepare(hint)
            if delivery is not None:
                deliveries.append(delivery)
        targets: dict[str, list[tuple[_Delivery, Preference]]] = {}
        for delivery in deliveries:
            for entry in delivery.targets:
                targets.setdefault(entry.node_id, []).append((delivery, entry))
        local = targets.pop(self._node_id, [])
        await self._deliver_local_batch(group, local)
        await self._deliver_targets(targets)
        await self._satisfy(
            group, [(item.hint, item.owners) for item in deliveries if item.complete]
        )

    async def _deliver_targets(
        self, targets: dict[str, list[tuple[_Delivery, Preference]]]
    ) -> None:
        pending = iter(targets.items())

        async def worker() -> None:
            for node_id, deliveries in pending:
                for delivery, entry in deliveries:
                    await self._deliver_target(node_id, delivery, entry)

        async with asyncio.TaskGroup() as tasks:
            for _ in range(min(self._concurrency, len(targets))):
                tasks.create_task(worker())

    async def _deliver_target(
        self, node_id: str, delivery: _Delivery, entry: Preference
    ) -> None:
        if self._backoff.deferred(node_id):
            delivery.complete = False
            return
        if await self._replicate_to(
            node_id,
            delivery.record,
            check_exists=delivery.check_exists,
            writable=entry.writable,
        ):
            self._backoff.succeeded(node_id)
        else:
            self._backoff.failed(node_id)
            delivery.complete = False

    async def _satisfy(
        self, group: PartitionGroupHint, delivered: list[tuple[Hint, frozenset[str]]]
    ) -> None:
        stable: list[Hint] = []
        for hint, owners in delivered:
            current = await self._placement.preference_list(hint.record.key.address)
            if frozenset(entry.node_id for entry in current.preferences) == owners:
                stable.append(hint)
        await group.satisfy(stable)

    def _pending(self, hint: Hint) -> tuple[str, Record] | None:
        """Split a hint into its destination and the record to deliver."""
        record = hint.record
        try:
            original = Record(
                key=record.key,
                tag=Tag.unhint(record.tag),
                value=record.value,
            )
        except ValueError as exc:
            logger.warning(
                "Failed to decode hint tag for key %s",
                record.key,
            )
            self._record_failure(exc)
            return None
        return hint.intent, original

    def _record_failure(self, exc: Exception) -> None:
        self._stats.failed_total += 1
        self._stats.last_failure = f"{type(exc).__name__}: {exc}"
        self._stats.last_failure_at = datetime.now(UTC).isoformat()

    async def _replicate_to(
        self,
        target_node_id: str,
        record: Record,
        *,
        check_exists: bool = False,
        writable: bool = True,
    ) -> bool:
        try:
            async with self._peer_pool.lease(target_node_id) as client:
                if check_exists and await self._exists_remote(client, record):
                    return True
                if not writable:
                    return False
                await self._request(
                    client,
                    "replication.write",
                    {"record": record.to_dict()},
                    "replication.write.accept",
                )
                self._stats.applied_total += 1
                return True
        except (
            RuntimeError,
            ProtocolError,
            StoreOperationError,
            OSError,
            TimeoutError,
        ) as exc:
            self._record_failure(exc)
            logger.warning(
                "Failed to replicate hint to %s for key %s: %s",
                target_node_id,
                record.key,
                exc,
            )
        return False

    async def _deliver_local_batch(
        self, group: PartitionGroupHint, deliveries: list[tuple[_Delivery, Preference]]
    ) -> None:
        if not deliveries:
            return
        if self._backoff.deferred(self._node_id):
            for delivery, _ in deliveries:
                delivery.complete = False
            return
        try:
            async with self._local_delivery:
                await self._apply_local_batch(group, deliveries)
        except (RuntimeError, ValueError, OSError, TimeoutError) as exc:
            self._record_failure(exc)
            self._backoff.failed(self._node_id)
            for delivery, _ in deliveries:
                delivery.complete = False
            logger.warning(
                "Failed to apply local hints: node=%s error=%s", self._node_id, exc
            )

    async def _apply_local_batch(
        self, group: PartitionGroupHint, deliveries: list[tuple[_Delivery, Preference]]
    ) -> None:
        for writable in (False, True):
            selected = [
                item
                for item, entry in deliveries
                if (entry.writable and self._local_writable) == writable
            ]
            confirmed, applied = await group.promote(
                [item.record for item in selected], writable=writable
            )
            for item, present in zip(selected, confirmed, strict=True):
                if not present or (not writable and not item.check_exists):
                    item.complete = False
            self._stats.applied_total += applied
        self._backoff.succeeded(self._node_id)

    async def _exists_remote(self, client: EnvelopeClient, record: Record) -> bool:
        response = await self._request(
            client,
            "replication.exists",
            {"key": record.key.to_dict()},
            "replication.exists.accept",
        )
        if set(response) != {"owned"} or type(response["owned"]) is not bool:
            raise ProtocolError(
                "replication.exists response must contain an owned boolean"
            )
        return response["owned"]

    async def _request(
        self,
        client: EnvelopeClient,
        kind: str,
        payload: dict[str, Any],
        expected: str,
        timeout: float = ENVELOPE_RESPONSE_TIMEOUT,
    ) -> dict[str, Any]:
        response = await client.request(self._envelope(kind, payload), timeout=timeout)
        return self._expect(response, expected)

    async def _session_providers(self) -> list[SessionProvider]:
        """Return a list of session providers for all groups."""
        return [
            await self._storage.session_provider_for(group)
            for group in range(self._total_groups)
        ]
