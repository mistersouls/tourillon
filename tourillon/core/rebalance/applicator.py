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
"""RebalanceApplicator — orchestrates per-pid transfer coroutines."""

from __future__ import annotations

import asyncio
import base64
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from tourillon.core.lifecycle.state import NodeState
from tourillon.core.rebalance.digest import compute_transfer_digest
from tourillon.core.rebalance.plan import (
    RebalancePlan,
    TransferHandle,
    TransferState,
)
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import record_from_dict
from tourillon.core.structure.waitgroup import WaitGroup

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.state import StatePort
    from tourillon.core.ports.storage import Storage
    from tourillon.core.transport.pool import PeerClientPool

logger = logging.getLogger(__name__)

_BACKOFF_INITIAL: float = 1.0
_BACKOFF_MAX: float = 30.0
_BACKOFF_MULTIPLIER: float = 2.0
_MAX_RETRIES: int = 10


class RebalanceApplicator:
    """Orchestrates partition transfer coroutines for one rebalance epoch.

    apply(new_plan) diffs the new plan against the current handles: pids
    removed from the plan have their cancel_event set; pids added start new
    coroutines; pids in the intersection continue unchanged.

    crash_recover() must be called on startup before apply() when the node
    restarts mid-transfer. It reconciles persisted state with the current
    gossip epoch, cleans up stale staging entries, and auto-heals pids whose
    LMDB transaction committed before state.toml was updated.

    status() returns a paginated dict for the rebalance.status wire response.
    The applicator enforces the WaitGroup invariant: the phase transition
    (JOINING → READY or DRAINING → IDLE) is only signalled when wait() returns
    with an empty failed_list.
    """

    def __init__(
        self,
        node_id: str,
        pool: PeerClientPool,
        state_port: StatePort,
        storage: Storage,
        serializer: SerializerPort,
        peer_addresses: dict[str, str],
        max_concurrent_transfers: int = 4,
        max_chunk_bytes: int = 1_048_576,
        total_partitions: int | None = None,
    ) -> None:
        self._node_id = node_id
        self._pool = pool
        self._state_port = state_port
        self._storage = storage
        self._serializer = serializer
        self._peer_addresses = peer_addresses
        self._max_concurrent = max_concurrent_transfers
        self._max_chunk_bytes = max_chunk_bytes
        self._total_partitions = total_partitions
        self._handles: dict[int, TransferHandle] = {}
        self._plan: RebalancePlan | None = None
        self._wg: WaitGroup[int] = WaitGroup()
        self._semaphore = asyncio.Semaphore(max_concurrent_transfers)
        self._tasks: dict[int, asyncio.Task[None]] = {}
        # Serialises concurrent state.toml writes from parallel transfer tasks.
        self._state_lock = asyncio.Lock()

    async def crash_recover(
        self,
        stored_epoch: int,
        gossip_epoch: int,
        staging_pids: tuple[int, ...],
        committed_pids: tuple[int, ...],
    ) -> None:
        """Startup reconciliation: detect epoch drift and repair stale state.

        Step 1 — if stored_epoch < gossip_epoch (epoch drift): clean up all
        staging entries under the old epoch for each pid in staging_pids, then
        rewrite state.toml with epoch=gossip_epoch and an empty staging_pids.

        Step 2 — if stored_epoch == gossip_epoch (same epoch): for each pid in
        staging_pids, check exists(). When exists() is False the LMDB
        transaction already committed the staging entries to the committed tag
        before the process crashed; state.toml was not yet updated. Auto-heal
        by moving those pids from staging_pids to committed_pids.

        Call this method before apply() whenever the node restarts while a
        transfer was in progress.
        """
        state = await self._state_port.load()
        if state is None:
            return

        if stored_epoch < gossip_epoch:
            await self._recover_epoch_drift(
                state, stored_epoch, gossip_epoch, staging_pids
            )
            return

        await self._recover_same_epoch(
            state, stored_epoch, staging_pids, committed_pids
        )

    async def _recover_epoch_drift(
        self,
        state: NodeState,
        stored_epoch: int,
        gossip_epoch: int,
        staging_pids: tuple[int, ...],
    ) -> None:
        """Clean up stale staging entries and update epoch in state.toml."""
        for pid in staging_pids:
            staging = self._storage.open_partition(pid).staging(stored_epoch)
            await staging.cleanup()
            logger.debug(
                "crash_recover: cleaned up stale staging pid=%d epoch=%d",
                pid,
                stored_epoch,
            )
        new_state = NodeState(
            node_id=state.node_id,
            phase=state.phase,
            generation=state.generation,
            seq=state.seq,
            tokens=state.tokens,
            epoch=gossip_epoch,
            committed_pids=state.committed_pids,
            staging_pids=(),
        )
        await self._state_port.save(new_state)
        logger.info(
            "crash_recover: epoch drift %d→%d; cleared %d staging pids",
            stored_epoch,
            gossip_epoch,
            len(staging_pids),
        )

    async def _recover_same_epoch(
        self,
        state: NodeState,
        epoch: int,
        staging_pids: tuple[int, ...],
        committed_pids: tuple[int, ...],
    ) -> None:
        """Auto-heal pids where LMDB committed but state.toml was not updated."""
        auto_healed: list[int] = []
        for pid in staging_pids:
            staging = self._storage.open_partition(pid).staging(epoch)
            if not await staging.exists():
                auto_healed.append(pid)
                logger.info("crash_recover: auto-heal pid=%d (LMDB committed)", pid)

        if not auto_healed:
            return

        new_committed = committed_pids + tuple(auto_healed)
        new_staging = tuple(p for p in staging_pids if p not in auto_healed)
        new_state = NodeState(
            node_id=state.node_id,
            phase=state.phase,
            generation=state.generation,
            seq=state.seq,
            tokens=state.tokens,
            epoch=state.epoch,
            committed_pids=new_committed,
            staging_pids=new_staging,
        )
        await self._state_port.save(new_state)

    async def apply(self, new_plan: RebalancePlan) -> None:
        """Diff against current plan; cancel removed pids, start added pids."""
        pids_old = set(self._handles.keys())
        pids_new = {pid for r in new_plan.ranges for pid in r}

        to_cancel = pids_old - pids_new
        to_start = pids_new - pids_old

        for pid in to_cancel:
            handle = self._handles[pid]
            handle.cancel_event.set()

        self._plan = new_plan

        started = 0
        for transfer in new_plan.expand():
            if transfer.pid not in to_start:
                continue
            handle = TransferHandle(
                transfer=transfer,
                state=TransferState.PENDING,
            )
            self._handles[transfer.pid] = handle
            await self._wg.add(1)
            task = asyncio.get_running_loop().create_task(self._run_transfer(handle))
            self._tasks[transfer.pid] = task
            started += 1

        if to_cancel:
            logger.info(
                "Rebalance apply (node=%s, epoch=%d): %d transfer(s) queued, "
                "%d cancelled.",
                self._node_id,
                new_plan.epoch,
                started,
                len(to_cancel),
            )
        else:
            logger.info(
                "Rebalance apply (node=%s, epoch=%d): %d transfer(s) queued "
                "(concurrency=%d).",
                self._node_id,
                new_plan.epoch,
                started,
                self._max_concurrent,
            )

    async def wait_for_completion(self) -> tuple[list[int], list[int]]:
        """Await all active transfers; return (success_pids, failed_pids).

        Suspends until every pid added via apply() has been marked done by its
        transfer coroutine.  When the active plan is empty (ranges == ()) the
        WaitGroup counter is zero and this method returns immediately with two
        empty lists — the caller may then advance the phase without any work.

        The JOINING → READY or DRAINING → IDLE transition MUST NOT be executed
        when failed_pids is non-empty (phase-guard invariant §7).  The caller
        is responsible for checking the returned failed list before transitioning.
        """
        success, failed = await self._wg.wait()
        logger.info(
            "Rebalance complete (node=%s): committed=%d failed=%d.",
            self._node_id,
            len(success),
            len(failed),
        )
        return success, failed

    async def status(self, after_pid: int = 0, limit: int = 500) -> dict[str, Any]:
        """Return paginated status dict suitable for wire encoding.

        A handle is *inactive* when it reached COMMITTED with bytes_done == 0:
        the partition existed in the ring but contained no records to transfer.
        Inactive handles are excluded from the transfer table and from the
        active_partitions counter; they are counted in inactive_partitions.
        Handles in non-terminal states (PENDING, RUNNING) or terminal-but-non-
        empty states (COMMITTED with data, FAILED, CANCELLED) are always
        *active* and appear in the table.
        """
        plan = self._plan
        epoch: int | None = plan.epoch if plan else None
        trigger = self._infer_trigger()
        role = "receiving" if trigger == "joining" else "sending"

        summary, blocked, empty_committed = self._scan_handle_summary()
        active_partitions = len(self._handles) - empty_committed
        inactive_partitions = max(0, self._total_owned() - active_partitions)

        page, has_more, next_pid = self._build_transfer_page(after_pid, limit)
        transfers = [self._handle_to_dict(self._handles[pid]) for pid in page]

        return {
            "epoch": epoch,
            "trigger": trigger,
            "role": role,
            "blocked": blocked,
            "active_partitions": active_partitions,
            "summary": summary,
            "inactive_partitions": inactive_partitions,
            "has_more": has_more,
            "next_pid": next_pid,
            "transfers": transfers,
        }

    def _scan_handle_summary(self) -> tuple[dict[str, int], bool, int]:
        """Scan all handles and return (summary_counts, blocked, empty_committed).

        A handle is *inactive* (empty_committed) when it is COMMITTED with
        bytes_done == 0.  Inactive handles are counted separately and excluded
        from the summary table.  The blocked flag is True when any handle is in
        the FAILED terminal state.
        """
        summary: dict[str, int] = {
            "committed": 0,
            "running": 0,
            "pending": 0,
            "failed": 0,
            "cancelled": 0,
        }
        blocked = False
        empty_committed = 0
        for h in self._handles.values():
            if h.state == TransferState.COMMITTED and h.bytes_done == 0:
                empty_committed += 1
                continue
            key = h.state.value
            if key in summary:
                summary[key] += 1
            if h.state == TransferState.FAILED:
                blocked = True
        return summary, blocked, empty_committed

    def _build_transfer_page(
        self,
        after_pid: int,
        limit: int,
    ) -> tuple[list[int], bool, int | None]:
        """Return (page, has_more, next_pid) for the active-pid cursor window.

        Active pids are those whose handle is not an empty COMMITTED entry.
        The page is sorted ascending and starts after after_pid (exclusive).
        has_more is True when the page is full and at least one active pid
        remains beyond it; next_pid is the last pid in the page when has_more
        is True, otherwise None.
        """
        active = sorted(self._active_pids())
        page = [pid for pid in active if pid > after_pid][:limit]
        max_active = active[-1] if active else -1
        has_more = len(page) == limit and bool(page) and page[-1] < max_active
        next_pid: int | None = page[-1] if has_more else None
        return page, has_more, next_pid

    def _active_pids(self) -> list[int]:
        """Return pids whose handle is not an empty COMMITTED entry."""
        return [
            pid
            for pid, h in self._handles.items()
            if not (h.state == TransferState.COMMITTED and h.bytes_done == 0)
        ]

    async def _run_transfer(self, handle: TransferHandle) -> None:
        """Run one pid transfer with retry/backoff."""
        pid = handle.transfer.pid
        state = await self._state_port.load()
        if state and pid in state.committed_pids:
            handle.state = TransferState.COMMITTED
            await self._wg.done(pid, True)
            return

        async with self._semaphore:
            if handle.cancel_event.is_set():
                await self._on_cancelled(handle)
                return
            logger.debug(
                "pid=%d semaphore acquired (src=%s dst=%s).",
                pid,
                handle.transfer.src,
                handle.transfer.dst,
            )
            await self._attempt_transfer(handle)

    async def _attempt_transfer(self, handle: TransferHandle) -> None:
        """Attempt the transfer with exponential backoff on network errors."""
        pid = handle.transfer.pid
        src = handle.transfer.src
        dst = handle.transfer.dst
        delay = _BACKOFF_INITIAL
        handle.state = TransferState.RUNNING
        handle.started_at = datetime.now(UTC)

        for attempt in range(_MAX_RETRIES + 1):
            if handle.cancel_event.is_set():
                await self._on_cancelled(handle)
                return
            try:
                await self._do_transfer(handle)
                await self._on_committed(handle)
                return
            except _ProcessError as exc:
                logger.error(
                    "pid=%d transfer process error: %s (src=%s dst=%s)",
                    pid,
                    exc,
                    src,
                    dst,
                )
                await self._on_failed(handle, str(exc))
                return
            except Exception as exc:
                logger.warning(
                    "pid=%d transfer attempt %d/%d failed: %s",
                    pid,
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                )
                handle.last_error = str(exc)
                if attempt >= _MAX_RETRIES:
                    await self._on_failed(handle, str(exc))
                    return
                try:
                    async with asyncio.timeout(delay):
                        await handle.cancel_event.wait()
                    await self._on_cancelled(handle)
                    return
                except TimeoutError:
                    pass
                delay = min(delay * _BACKOFF_MULTIPLIER, _BACKOFF_MAX)

    async def _do_transfer(self, handle: TransferHandle) -> None:
        """Dispatch to the correct protocol path based on transfer direction."""
        if handle.transfer.dst == self._node_id:
            await self._do_join_transfer(handle)
        elif handle.transfer.src == self._node_id:
            await self._do_drain_transfer(handle)
        else:
            raise _ProcessError(
                f"pid={handle.transfer.pid}: neither src nor dst matches this node"
            )

    async def _do_join_transfer(self, handle: TransferHandle) -> None:
        """Execute the receive-side (JOIN destination) transfer protocol.

        Sends rebalance.plan to the source node, then consumes the stream
        in one loop: plan.ok is skipped; transfer chunks are staged as they
        arrive; after the last chunk the digest is computed and rebalance.commit
        is pushed back on the same correlation_id via client.send() (fire-and-
        forget, no new stream registered). The loop then expects commit.ok or
        commit.reject before breaking. Using a single stream avoids a race
        condition where commit.ok could arrive before a second stream() call
        registers its queue and would be dropped as unsolicited.
        """
        pid = handle.transfer.pid
        src = handle.transfer.src
        epoch = self._plan.epoch if self._plan else 0  # type: ignore[union-attr]

        store = self._storage.open_partition(pid)
        staging = store.staging(epoch)
        resume_from = await staging.last_staged_log_key()
        resume_encoded = base64.b64encode(resume_from).decode() if resume_from else None

        addr = self._peer_addresses.get(src, "")
        client = await self._pool.acquire(src, addr)

        plan_payload = self._serializer.encode(
            {
                "epoch": epoch,
                "transfers": [
                    {"pid_start": pid, "pid_end": pid, "src": src, "dst": self._node_id}
                ],
                "resume_from": resume_encoded,
            }
        )
        plan_env = Envelope(
            kind="rebalance.plan",
            payload=plan_payload,
            schema_id=self._serializer.schema_id,
        )

        records: list[Any] = []
        is_last_seen = False

        async for resp in client.stream(plan_env):
            if resp.kind == "rebalance.plan.reject":
                data = self._serializer.decode(resp.payload)
                raise _ProcessError(f"plan rejected: {data.get('reason')}")
            if resp.kind == "rebalance.plan.ok":
                continue
            if resp.kind == "rebalance.transfer":
                data = self._serializer.decode(resp.payload)
                await self._stage_chunk(handle, staging, data, records)
                is_last_seen = await self._send_commit_if_last(
                    data, records, is_last_seen, epoch, pid, plan_env, client
                )
                continue
            if resp.kind == "rebalance.commit.ok":
                break
            if resp.kind == "rebalance.commit.reject":
                raise _ProcessError("commit rejected: digest mismatch")
            raise _ProcessError(f"unexpected envelope kind: {resp.kind}")

        await staging.commit()
        await self._update_state_committed(pid)

    async def _send_commit_if_last(
        self,
        data: dict[str, Any],
        records: list[Any],
        is_last_seen: bool,
        epoch: int,
        pid: int,
        plan_env: Envelope,
        client: Any,  # noqa: ANN401 — opaque TcpClient/mock, no shared Protocol yet
    ) -> bool:
        """Send rebalance.commit when data['is_last'] is True and not yet sent.

        Return the updated is_last_seen flag so the caller can suppress any
        duplicate is_last chunks that may arrive on a resumed stream.
        """
        if not data.get("is_last") or is_last_seen:
            return is_last_seen
        digest = compute_transfer_digest(iter(records))
        commit_env = Envelope(
            kind="rebalance.commit",
            payload=self._serializer.encode(
                {"epoch": epoch, "pid": pid, "digest": digest}
            ),
            correlation_id=plan_env.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await client.send(commit_env)
        return True

    async def _do_drain_transfer(self, handle: TransferHandle) -> None:
        """Execute the source-side (DRAIN) transfer protocol.

        Sends rebalance.plan to the destination, awaits plan.ok (which includes
        the destination's resume_from cursor), scans the local store and pushes
        rebalance.transfer chunks via client.send() fire-and-forget on the same
        correlation_id. Then waits for rebalance.commit from the destination,
        validates the digest, and sends commit.ok or commit.reject back.
        """
        pid = handle.transfer.pid
        dst = handle.transfer.dst
        epoch = self._plan.epoch if self._plan else 0  # type: ignore[union-attr]

        addr = self._peer_addresses.get(dst, "")
        client = await self._pool.acquire(dst, addr)

        plan_payload = self._serializer.encode(
            {
                "epoch": epoch,
                "transfers": [
                    {"pid_start": pid, "pid_end": pid, "src": self._node_id, "dst": dst}
                ],
            }
        )
        plan_env = Envelope(
            kind="rebalance.plan",
            payload=plan_payload,
            schema_id=self._serializer.schema_id,
        )

        records: list[Any] = []
        received_commit = False

        async for resp in client.stream(plan_env):
            if resp.kind == "rebalance.plan.reject":
                data = self._serializer.decode(resp.payload)
                raise _ProcessError(f"plan rejected: {data.get('reason')}")
            if resp.kind == "rebalance.plan.ok":
                data = self._serializer.decode(resp.payload)
                resume_raw = data.get("resume_from")
                resume: bytes | None = (
                    base64.b64decode(resume_raw) if resume_raw else None
                )
                await self._stream_drain_chunks(
                    client, plan_env, pid, epoch, handle, records, resume
                )
                continue
            if resp.kind == "rebalance.commit":
                data = self._serializer.decode(resp.payload)
                received_digest = data.get("digest", "")
                expected = compute_transfer_digest(iter(records))
                if expected != received_digest:
                    await client.send(
                        Envelope(
                            kind="rebalance.commit.reject",
                            payload=self._serializer.encode({"epoch": epoch}),
                            correlation_id=plan_env.correlation_id,
                            schema_id=self._serializer.schema_id,
                        )
                    )
                    raise _ProcessError("commit rejected: digest mismatch")
                await client.send(
                    Envelope(
                        kind="rebalance.commit.ok",
                        payload=self._serializer.encode({"epoch": epoch}),
                        correlation_id=plan_env.correlation_id,
                        schema_id=self._serializer.schema_id,
                    )
                )
                received_commit = True
                break
            raise _ProcessError(f"unexpected envelope kind: {resp.kind}")

        if not received_commit:
            raise _ProcessError("stream closed before commit received")

        await self._update_state_committed(pid)

    async def _stream_drain_chunks(
        self,
        client: Any,  # noqa: ANN401
        plan_env: Envelope,
        pid: int,
        epoch: int,
        handle: TransferHandle,
        records: list[Any],
        resume: bytes | None,
    ) -> None:
        """Scan local store and push rebalance.transfer chunks to destination."""
        store = self._storage.open_partition(pid)
        chunk_buf: list[Any] = []
        chunk_bytes = 0
        chunk_seq = 0

        async for record in store.scan(resume_from=resume):
            records.append(record)
            rec_dict = record.to_dict()
            encoded = self._serializer.encode(rec_dict)
            chunk_buf.append(rec_dict)
            chunk_bytes += len(encoded)
            handle.bytes_done += len(record.value) if hasattr(record, "value") else 0
            if chunk_bytes >= self._max_chunk_bytes:
                await self._send_drain_chunk(
                    client, plan_env, pid, epoch, chunk_seq, chunk_buf, False
                )
                chunk_seq += 1
                handle.chunks_done = chunk_seq
                chunk_buf = []
                chunk_bytes = 0

        await self._send_drain_chunk(
            client, plan_env, pid, epoch, chunk_seq, chunk_buf, True
        )
        handle.chunks_done = chunk_seq + 1
        handle.chunks_total = chunk_seq + 1

    async def _send_drain_chunk(
        self,
        client: Any,  # noqa: ANN401
        plan_env: Envelope,
        pid: int,
        epoch: int,
        seq: int,
        recs: list[Any],
        is_last: bool,
    ) -> None:
        """Push one rebalance.transfer chunk to the destination."""
        payload = self._serializer.encode(
            {
                "epoch": epoch,
                "pid": pid,
                "chunk_seq": seq,
                "is_last": is_last,
                "records": recs,
            }
        )
        chunk_env = Envelope(
            kind="rebalance.transfer",
            payload=payload,
            correlation_id=plan_env.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await client.send(chunk_env)

    async def _stage_chunk(
        self,
        handle: TransferHandle,
        staging: Any,  # noqa: ANN401
        data: dict[str, Any],
        records: list[Any],
    ) -> None:
        """Stage records from one transfer chunk."""
        for rec_dict in data.get("records", []):
            rec = record_from_dict(rec_dict)
            await staging.stage(rec)
            records.append(rec)
            handle.bytes_done += len(rec.value if hasattr(rec, "value") else b"")
        handle.chunks_done += 1
        if data.get("is_last"):
            handle.chunks_total = handle.chunks_done

    async def _on_committed(self, handle: TransferHandle) -> None:
        """Mark handle as committed and signal the WaitGroup."""
        handle.state = TransferState.COMMITTED
        handle.finished_at = datetime.now(UTC)
        logger.debug("pid=%d committed.", handle.transfer.pid)
        await self._wg.done(handle.transfer.pid, True)

    async def _on_failed(self, handle: TransferHandle, error: str) -> None:
        """Mark handle as failed and signal the WaitGroup."""
        handle.state = TransferState.FAILED
        handle.last_error = error
        handle.finished_at = datetime.now(UTC)
        logger.error(
            "pid=%d permanently failed (src=%s dst=%s): %s",
            handle.transfer.pid,
            handle.transfer.src,
            handle.transfer.dst,
            error,
        )
        await self._wg.done(handle.transfer.pid, False)

    async def _on_cancelled(self, handle: TransferHandle) -> None:
        """Call staging.cleanup() and signal the WaitGroup."""
        pid = handle.transfer.pid
        epoch = self._plan.epoch if self._plan else 0  # type: ignore[union-attr]
        try:
            staging = self._storage.open_partition(pid).staging(epoch)
            await staging.cleanup()
        except Exception:
            logger.warning("pid=%d cleanup failed during cancel", pid)
        handle.state = TransferState.CANCELLED
        handle.finished_at = datetime.now(UTC)
        await self._wg.done(pid, False)

    async def _update_state_committed(self, pid: int) -> None:
        """Move pid from staging_pids to committed_pids in state.toml.

        Uses _state_lock to serialise concurrent writes: parallel transfer
        coroutines all call this method simultaneously, and os.replace() on
        Windows raises WinError 32 when two processes race on the same temp
        file.  The lock ensures only one load-modify-save runs at a time.
        """
        async with self._state_lock:
            state = await self._state_port.load()
            if state is None:
                return
            new_staging = tuple(p for p in state.staging_pids if p != pid)
            new_committed = state.committed_pids + (pid,)
            await self._state_port.save(
                NodeState(
                    node_id=state.node_id,
                    phase=state.phase,
                    generation=state.generation,
                    seq=state.seq,
                    tokens=state.tokens,
                    epoch=state.epoch,
                    committed_pids=new_committed,
                    staging_pids=new_staging,
                )
            )

    def _infer_trigger(self) -> str:
        """Return 'joining' or 'draining' based on transfer directions."""
        for h in self._handles.values():
            if h.transfer.dst == self._node_id:
                return "joining"
        return "draining"

    def _total_owned(self) -> int:
        """Return total owned partition count.

        When total_partitions was injected at construction, returns that value
        (for accurate inactive_partitions calculation). Otherwise falls back to
        the active handle count (both active and inactive appear as active when
        total_partitions is unknown).
        """
        if self._total_partitions is not None:
            return self._total_partitions
        return len(self._handles)

    @staticmethod
    def _handle_to_dict(h: TransferHandle) -> dict[str, Any]:
        """Serialise a TransferHandle to a wire-compatible dict."""
        return {
            "pid": h.transfer.pid,
            "src": h.transfer.src,
            "dst": h.transfer.dst,
            "state": h.state.value,
            "chunks_done": h.chunks_done,
            "chunks_total": h.chunks_total,
            "bytes_done": h.bytes_done,
            "started_at": h.started_at.isoformat() if h.started_at else None,
            "finished_at": h.finished_at.isoformat() if h.finished_at else None,
            "last_error": h.last_error,
        }


class _ProcessError(Exception):
    """Non-retryable protocol-level error in a transfer session."""
