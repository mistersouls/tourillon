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

"""Bounded local collection; no network operations or distributed GC votes."""

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from time import monotonic
from typing import Any
from uuid import uuid4

from tourillon.core.log import logger
from tourillon.core.models.config import RetentionConfig
from tourillon.core.models.storageschema import Keyspace, PartitionAddress
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Storage
from tourillon.core.ports.telemetry import Telemetry
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import GroupSession, GroupSessionManager


@dataclass(frozen=True)
class CollectionStatus:
    job_id: str = ""
    state: str = "idle"
    dry_run: bool = False
    scanned: int = 0
    collected: int = 0
    bytes_removed: int = 0
    started_at: str | None = None
    error: str | None = None

    def updated(self, **changes: Any) -> "CollectionStatus":
        return replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class GarbageCollector:
    def __init__(
        self,
        storage: Storage,
        partitioner: Partitioner,
        config: RetentionConfig,
        *,
        node_id: str,
        telemetry: Telemetry,
    ) -> None:
        self._storage = storage
        self._node_id = node_id
        self._telemetry = telemetry
        self._retention = RetentionPolicy(config)
        self._partitioner = partitioner
        self.config = config
        self._hot: OrderedDict[PartitionAddress, None] = OrderedDict()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._pending = False
        self._status = CollectionStatus()
        self.overflowed = 0

    @property
    def status(self) -> CollectionStatus:
        return self._status

    def submit(self, address: PartitionAddress) -> None:
        if address in self._hot:
            return
        if len(self._hot) >= self.config.hotqueue_size:
            self.overflowed += 1
            return

        self._hot[address] = None
        self._telemetry.gc_hot_queue_size.set(len(self._hot))
        self._wake.set()

    def request(self, *, dry_run: bool = False) -> CollectionStatus:
        if self._task is None or self._task.done():
            raise ValueError("GC worker is not running")
        if self._pending or self._status.state == "running":
            raise ValueError("GC pass already queued or running; inspect its progress")

        self._status = CollectionStatus(
            job_id=str(uuid4()), state="queued", dry_run=dry_run
        )
        self._pending = True
        self._wake.set()

        return self._status

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="gc.worker")
            self._telemetry.gc_hot_queue_size.set(len(self._hot))
            logger.info(
                "gc.started node=%s versions=%d interval=%.2fs batch_size=%d",
                self._node_id,
                self._retention.versions,
                self.config.interval,
                self.config.batch_size,
            )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
            if self._status.state in ("queued", "running"):
                self._pending = False
                self._status = self._status.updated(state="cancelled")
            logger.info("gc.stopped node=%s", self._node_id)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_pass = loop.time() + self.config.interval

        while True:
            await self._wait(max(0.0, next_pass - loop.time()))
            if self._pending or loop.time() >= next_pass:
                await self._pass()
                next_pass = loop.time() + self.config.interval
            else:
                await self._hot_step()

    async def _wait(self, timeout: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout)
        self._wake.clear()

    async def _pass(self) -> None:
        source = "requested" if self._pending else "periodic"
        started = monotonic()
        if not self._pending:
            self._status = CollectionStatus(job_id=str(uuid4()), state="queued")
        self._pending = False
        self._status = self._status.updated(
            state="running", started_at=datetime.now(UTC).isoformat()
        )

        try:
            await self._scan()
        except asyncio.CancelledError:
            self._status = self._status.updated(state="cancelled")
            raise
        except Exception as exc:
            logger.exception(
                "gc.pass failed node=%s job=%s source=%s",
                self._node_id,
                self._status.job_id,
                source,
            )
            self._status = self._status.updated(
                state="failed", error=f"{type(exc).__name__}: {exc}"
            )
            return
        finally:
            outcome = (
                self._status.state if self._status.state != "running" else "completed"
            )
            labels = {
                "source": source,
                "mode": self._mode(self._status.dry_run),
                "outcome": outcome,
            }
            self._telemetry.gc_passes_total.add(1, attributes=labels)
            self._telemetry.gc_pass_duration_seconds.record(
                monotonic() - started, attributes=labels
            )

        self._status = self._status.updated(state="completed")
        if self._hot:
            self._wake.set()
        logger.info(
            "gc.pass completed node=%s mode=%s scanned=%d "
            "%s=%d record_bytes=%d duration=%.3fs",
            self._node_id,
            "estimate" if self._status.dry_run else source,
            self._status.scanned,
            "collectible" if self._status.dry_run else "collected",
            self._status.collected,
            self._status.bytes_removed,
            monotonic() - started,
        )

    async def _scan(self) -> None:
        for group in range(self._partitioner.total_groups):
            after: bytes | None = None

            while True:
                async with self._session(
                    group, "scan", self._status.dry_run
                ) as session:
                    keys = await session.log_page(after, self.config.batch_size)
                    count, size = await self._retention.collect(
                        session, keys, dry_run=self._status.dry_run
                    )

                self._record_collection(
                    "scan", self._status.dry_run, len(keys), count, size
                )
                self._status = self._status.updated(
                    scanned=self._status.scanned + len(keys),
                    collected=self._status.collected + count,
                    bytes_removed=self._status.bytes_removed + size,
                )
                if len(keys) < self.config.batch_size:
                    break

                after = keys[-1].to_bytes(Keyspace.TAGS)
                if not self._status.dry_run:
                    await self._hot_step()
                await asyncio.sleep(0)

    async def _hot_step(self) -> None:
        if not self._hot:
            return
        address, _ = self._hot.popitem(last=False)
        self._telemetry.gc_hot_queue_size.set(len(self._hot))

        try:
            await self._collect_address(address)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "gc.hot failed node=%s pid=%s; periodic pass will retry: %s",
                self._node_id,
                address.pid.value,
                exc,
            )

        if self._hot:
            self._wake.set()

    async def _collect_address(self, address: PartitionAddress) -> None:
        group = self._partitioner.group_for(address.pid.value)
        async with self._session(group, "hot", False) as session:
            records = await session.oldest_keys(address, self.config.batch_size)
            count, size = await self._retention.collect(session, records)

        self._record_collection("hot", False, len(records), count, size)

        if count == self.config.batch_size:
            self.submit(address)

    @staticmethod
    def _mode(dry_run: bool) -> str:
        return "estimate" if dry_run else "collect"

    def _record_collection(
        self, source: str, dry_run: bool, scanned: int, count: int, size: int
    ) -> None:
        labels = {"source": source, "mode": self._mode(dry_run)}
        self._telemetry.gc_versions_total.add(
            scanned, attributes={**labels, "event": "scanned"}
        )
        self._telemetry.gc_versions_total.add(
            count,
            attributes={**labels, "event": "collectible" if dry_run else "collected"},
        )
        if not dry_run:
            self._telemetry.gc_record_bytes_removed_total.add(
                size, attributes={"source": source}
            )

    @asynccontextmanager
    async def _session(
        self, group: int, source: str, dry_run: bool
    ) -> AsyncIterator[GroupSession]:
        started = monotonic()
        outcome = "completed"
        try:
            provider = await self._storage.session_provider_for(group)
            async with GroupSessionManager(provider).session(
                write=not dry_run
            ) as session:
                yield session
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception:
            outcome = "failed"
            self._telemetry.gc_errors_total.add(1, attributes={"source": source})
            logger.exception(
                "gc.transaction failed node=%s group=%d source=%s",
                self._node_id,
                group,
                source,
            )
            raise
        finally:
            self._telemetry.gc_transaction_duration_seconds.record(
                monotonic() - started,
                attributes={
                    "source": source,
                    "mode": self._mode(dry_run),
                    "outcome": outcome,
                },
            )
