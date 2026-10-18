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
from datetime import datetime, UTC
from typing import Any

from tourillon.core.exceptions import StoreOperationError
from tourillon.core.log import logger
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import Tag, Record
from tourillon.core.peer import PeerPool
from tourillon.core.ports.storage import Storage, SessionProvider
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


class _TargetBackoff:
    """Defer retries per target node.

    Hints are grouped by key, not by destination, so a single unreachable peer
    holding many hints would otherwise be leased, dialled and timed out once
    per hint on every cycle. Deferring by target collapses that to one attempt
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
        self._delay[target_node_id] = min(delay * self._backoff.factor, self._backoff.max_delay)


class HintReplayer:
    def __init__(
        self,
        storage: Storage,
        peer_pool: PeerPool,
        serializer: Serializer,
        total_groups: int,
        poll_interval: float = 0.5,
        batch_size: int = 32,
        backoff: BackoffRetry | None = None,
    ) -> None:
        self._storage = storage
        self._peer_pool = peer_pool
        self._serializer = serializer
        self._total_groups = total_groups
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._backoff = _TargetBackoff(backoff or BackoffRetry())

        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._stats = HintReplayStats()


    async def start(self) -> None:
        """Start the hint replay process."""
        if self._task and not self._task.done():
            return

        self._stop.clear()
        self._task = asyncio.create_task(
            self._replay_loop(), name="hint-replay-loop"
        )

    async def stop(self) -> None:
        """Stop the hint replay process."""
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

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
        if response.kind == "replication.write.reject":
            raise StoreOperationError.from_dict(self._serializer.decode(response.payload))
        raise ProtocolError(f"unexpected response kind {response.kind!r}, expected {kind!r}")

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
            await self._replay_group(PartitionGroupHint(session_provider))

    async def _replay_group(self, hint_group: PartitionGroupHint) -> None:
        satisfied: list[Record] = []
        async for record in hint_group.iter(batch_size=self._batch_size):
            pending = self._pending(record)
            if pending is None:
                continue

            target_node_id, original = pending
            if self._backoff.deferred(target_node_id):
                continue

            if not await self._replicate_to(target_node_id, original):
                self._backoff.failed(target_node_id)
                continue

            self._backoff.succeeded(target_node_id)
            satisfied.append(record)
            if len(satisfied) >= self._batch_size:
                await hint_group.satisfy(satisfied)
                satisfied.clear()

        if satisfied:
            await hint_group.satisfy(satisfied)

    def _pending(self, record: Record) -> tuple[str, Record] | None:
        """Split a hint into its destination and the record to deliver."""
        try:
            target_node_id, _ = record.tag.payload.extract_source_prefix()
            original = Record(
                key=record.key,
                tag=Tag.unhint(record.tag),
                value=record.value,
            )
        except ValueError as exc:
            logger.warning(
                "Failed to extract source prefix from hint tag for key %s",
                record.key,
            )
            self._record_failure(exc)
            return None
        return target_node_id, original

    def _record_failure(self, exc: Exception) -> None:
        self._stats.failed_total += 1
        self._stats.last_failure = f"{type(exc).__name__}: {exc}"
        self._stats.last_failure_at = datetime.now(UTC).isoformat()

    async def _replicate_to(self, target_node_id: str, record: Record) -> bool:
        try:
            async with self._peer_pool.lease(target_node_id) as client:
                try:
                    await self._request(
                        client,
                        "replication.write",
                        {"record": record.to_dict()},
                        "replication.write.accept",
                    )
                    self._stats.applied_total += 1
                    return True
                except (ProtocolError, StoreOperationError) as exc:
                    self._record_failure(exc)
                    logger.warning(
                        "Failed to replicate hint to %s for key %s: %s",
                        target_node_id,
                        record.key,
                        exc,
                    )
        except RuntimeError as exc:
            logger.warning(
                "Failed to replicate hint to %s for key %s: %s",
                target_node_id,
                record.key,
                exc,
            )
        return False

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
