import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from typing import Any

from tourillon.core.exceptions import ProcessError
from tourillon.core.helpers.backoff import Backoff
from tourillon.core.helpers.waitgroup import WaitGroup
from tourillon.core.machinery.namespace import Key, Namespace, TaggedRecord
from tourillon.core.machinery.state import StatePersistence
from tourillon.core.ports.storage import Storage
from tourillon.core.rebalance.transfer import (
    RangeSet,
    RangeTransfer,
    RebalancePlan,
    TransferHandle,
    TransferState,
)
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.storage.staging import PartitionStaging
from tourillon.core.structure.member import NodeState
from tourillon.core.transport.client import PeerClientPool
from tourlib.envelope import Envelope
from tourlib.ports.serializer import Serializer
from tourlib.transport import TcpClient

logger = logging.getLogger(__name__)

class RebalanceApplicator:
    def __init__(
        self,
        node_id: str,
        total_partitions: int,
        storage: Storage,
        serializer: Serializer,
        pool: PeerClientPool,
        topology_mgr: TopologyManager,
        state_persistence: StatePersistence,
        backoff: Backoff | None = None,
        max_concurrent_transfers: int = 4,
        max_chunk_bytes: int = 1_048_576,
    ) -> None:
        self._node_id = node_id
        self._total_partitions = total_partitions
        self._storage = storage
        self._serializer = serializer
        self._pool = pool
        self._topology_mgr = topology_mgr
        self._state_persistence = state_persistence
        self._backoff = backoff or Backoff()
        self._max_concurrent = max_concurrent_transfers
        self._max_chunk_bytes = max_chunk_bytes

        self._handles: dict[str, TransferHandle] = {}
        self._ranges: dict[str, RangeTransfer] = {}
        self._epoch: int = 0
        self._wg = WaitGroup()
        self._semaphore = asyncio.Semaphore(max_concurrent_transfers)
        self._state_lock = asyncio.Lock()

    async def apply(self, plan: RebalancePlan) -> None:
        range_sets = tuple(self._rangeset(plan.ranges))

        pids_old = set(self._handles.keys())
        pids_new = {
            transfer.id
            for ranges in range_sets
            for transfer in ranges.expand(plan.total_partitions)
        }

        to_cancel = pids_old - pids_new
        to_start = pids_new - pids_old
        self._epoch = plan.epoch

        for pid in to_cancel:
            handle = self._handles[pid]
            handle.cancel_event.set()

        tasks = [
            asyncio.create_task(self._initiate_transfers(rangeset, to_start))
            for rangeset in range_sets
        ]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        if pending:
            for task in pending:
                task.cancel()

        started = 0
        for task in done:
            if task.exception():
                raise task.exception()
            if task.done():
                started += task.result()

        if to_cancel:
            logger.info(
                "Rebalance apply (epoch=%d): %d transfer(s) queued, %d cancelled.",
                plan.epoch,
                started,
                len(to_cancel),
            )
        else:
            logger.info(
                "Rebalance apply (epoch=%d): %d transfer(s) queued "
                "(concurrency=%d).",
                plan.epoch,
                started,
                self._max_concurrent,
            )

    async def _attempt_transfer(self, handle: TransferHandle) -> None:
        """Attempt the transfer with exponential backoff on network errors."""
        pid = handle.transfer.pid
        src = handle.transfer.src
        dst = handle.transfer.dst
        delay = self._backoff.initial
        max_retries = self._backoff.max_retries
        handle.state = TransferState.RUNNING
        handle.started_at = datetime.now(UTC)

        for attempt in range(max_retries + 1):
            if handle.cancel_event.is_set():
                await self._on_cancelled(handle)
                return
            try:
                await self._do_transfer(handle)
                await self._on_committed(handle)
                return
            except ProcessError as exc:
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
                    max_retries,
                    exc,
                )
                handle.last_error = str(exc)
                if attempt >= max_retries:
                    await self._on_failed(handle, str(exc))
                    return
                try:
                    async with asyncio.timeout(delay):
                        await handle.cancel_event.wait()
                    await self._on_cancelled(handle)
                    return
                except TimeoutError:
                    pass
                delay = min(delay * self._backoff.step, self._backoff.max_interval)

    @staticmethod
    def _compute_transfer_digest(records: Iterable[TaggedRecord]) -> str:
        h = hashlib.sha256()
        for record in records:
            h.update(record.to_bytes(Namespace.TAGS))
        return h.hexdigest()

    async def _do_incoming_transfer(self, handle: TransferHandle) -> None:
        pid = handle.transfer.pid
        store = await self._storage.open_by_pid(pid)
        staging = store.staging(self._epoch)
        resume_from = await staging.last_staged_key()
        resume_payload = self._serializer.encode({
            "epoch": self._epoch,
            "transfer_id": handle.transfer.id,
            "resume_from": resume_from.to_dict() if resume_from else None,
        })
        resume_env = Envelope.create(
            kind="rebalance.resume",
            payload=resume_payload,
            schema_id=self._serializer.schema_id,
            correlation_id=handle.correlation_id
        )
        await handle.client.send(resume_env)

        records: list[TaggedRecord] = []
        is_last_seen = False
        received_commit_ok = False

        while not handle.cancel_event.is_set():
            resp = await handle.queue.get()

            commit_ok, is_last_seen = await self._handle_incoming_response(
                resp=resp,
                handle=handle,
                staging=staging,
                records=records,
                is_last_seen=is_last_seen,
            )

            if commit_ok:
                received_commit_ok = True
                break

        if not received_commit_ok:
            raise ProcessError("stream closed before commit acknowledgement")

        await staging.commit()
        await self._update_state_committed(pid)

    async def _do_outgoing_transfer(self, handle: TransferHandle) -> None:
        records: list[TaggedRecord] = []
        received_commit = False

        while not handle.cancel_event.is_set():
            resp = await handle.queue.get()

            received_commit = await self._handle_outgoing_response(
                resp=resp,
                handle=handle,
                records=records,
            )
            if received_commit:
                break

        if not received_commit:
            raise ProcessError("stream closed before commit received")

        await self._update_state_committed(handle.transfer.pid)

    async def _do_transfer(self, handle: TransferHandle) -> None:
        """Dispatch to the correct protocol path based on transfer direction."""
        if handle.transfer.dst == self._node_id:
            await self._do_incoming_transfer(handle)
        elif handle.transfer.src == self._node_id:
            await self._do_outgoing_transfer(handle)
        else:
            raise ProcessError(
                f"transfer={handle.transfer.id}: neither src nor dst matches this node"
            )

    async def _get_peer_client(self, peer: str) -> TcpClient:
        topology = await self._topology_mgr.snapshot()
        member = topology.registry.get(peer)
        if member is None:
            raise ProcessError(f"Peer not found in topology: {peer}")

        client = await self._pool.acquire(peer, member.peer_address)
        return client

    async def _handle_incoming_response(
        self,
        *,
        resp: Envelope,
        handle: TransferHandle,
        staging: PartitionStaging,
        records: list[TaggedRecord],
        is_last_seen: bool
    ) -> tuple[bool, bool]:
        """Handle one JOIN stream response.

        Returns (commit_ok_received, updated_is_last_seen).
        """
        if resp.kind == "rebalance.transfer":
            data = self._serializer.decode(resp.payload)
            await self._stage_chunk(handle, staging, data, records)
            is_last_seen = await self._send_commit_if_last(
                data, records, is_last_seen, handle
            )
            return False, is_last_seen
        if resp.kind == "rebalance.commit.ok":
            return True, is_last_seen
        if resp.kind == "rebalance.commit.reject":
            raise ProcessError("commit rejected: digest mismatch")
        raise ProcessError(f"unexpected envelope kind: {resp.kind}")

    async def _handle_outgoing_response(
        self,
        *,
        resp: Envelope,
        handle: TransferHandle,
        records: list[TaggedRecord],
    ) -> bool:
        client = handle.client
        correlation_id = handle.correlation_id
        transfer_id = handle.transfer.id

        if resp.kind == "rebalance.resume":
            data = self._serializer.decode(resp.payload)
            resume_data = data.get("resume_from")
            resume = Key.from_dict(resume_data) if resume_data else None
            await self._stream_chunks(handle, records, resume)
            return False
        if resp.kind == "rebalance.commit":
            data = self._serializer.decode(resp.payload)
            received_digest = data.get("digest", "")
            expected = self._compute_transfer_digest(iter(records))
            if expected != received_digest:
                await client.send(
                    Envelope(
                        kind="rebalance.commit.reject",
                        payload=self._serializer.encode({
                            "epoch": self._epoch,
                            "transfer_id": transfer_id
                        }),
                        correlation_id=correlation_id,
                        schema_id=self._serializer.schema_id,
                    )
                )
                raise ProcessError("commit rejected: digest mismatch")
            await client.send(
                Envelope(
                    kind="rebalance.commit.ok",
                    payload=self._serializer.encode({
                        "epoch": self._epoch,
                        "transfer_id": transfer_id
                    }),
                    correlation_id=correlation_id,
                    schema_id=self._serializer.schema_id,
                )
            )
            return True
        raise ProcessError(f"unexpected envelope kind: {resp.kind}")

    async def _initiate_transfers(self, rangeset: RangeSet, to_start: set[str]) -> int:
        client = await self._get_peer_client(rangeset.peer)
        plan_env = self._plan_envelope(rangeset.transfers)
        streams = client.stream(plan_env)
        resp = await anext(streams)
        data = self._serializer.decode(resp.payload)
        started = 0

        if resp.kind == "rebalance.plan.reject":
            raise ProcessError(f"plan rejected: {data.get('reason')}")
        if resp.kind != "rebalance.plan.ok":
            raise ProcessError(
                f"peer={rangeset.peer}, expected response kind 'rebalance.plan.ok' or "
                f"'rebalance.plan.reject', got '{resp.kind}'"
            )

        loop = asyncio.get_running_loop()
        handles: dict[str, TransferHandle] = {}
        for transfer in rangeset.expand(self._total_partitions):
            if transfer.id not in to_start:
                continue

            handle = TransferHandle(
                transfer=transfer,
                state=TransferState.PENDING,
                client=client,
                correlation_id=plan_env.correlation_id
            )
            self._handles[transfer.id] = handle
            handles[transfer.id] = handle
            await self._wg.add(1)
            loop.create_task(self._run_transfer(handle))
            started += 1

        loop.create_task(self._peer_stream_loop(streams, handles))

        return started

    async def _on_cancelled(self, handle: TransferHandle) -> None:
        """Call staging.cleanup() and signal the WaitGroup."""
        pid = handle.transfer.pid
        transfer_id = handle.transfer.id
        epoch = self._epoch

        try:
            store = await self._storage.open_by_pid(pid)
            staging = store.staging(epoch)
            await staging.cleanup()
        except Exception as ex:
            logger.warning(
                "transfer=%s cleanup failed during cancel",
                transfer_id, exc_info=ex
            )

        handle.state = TransferState.CANCELLED
        handle.finished_at = datetime.now(tz=UTC)
        await self._wg.done(pid, False)

    async def _on_committed(self, handle: TransferHandle) -> None:
        """Mark handle as committed and signal the WaitGroup."""
        handle.state = TransferState.COMMITTED
        handle.finished_at = datetime.now(UTC)
        logger.debug("transfer=%s committed.", handle.transfer.pid)
        await self._wg.done(handle.transfer.id, True)

    async def _on_failed(self, handle: TransferHandle, error: str) -> None:
        """Mark handle as failed and signal the WaitGroup."""
        handle.state = TransferState.FAILED
        handle.last_error = error
        handle.finished_at = datetime.now(UTC)
        logger.error(
            "transfer=%s permanently failed (src=%s dst=%s): %s",
            handle.transfer.id,
            handle.transfer.src,
            handle.transfer.dst,
            error,
        )
        await self._wg.done(handle.transfer.id, False)

    async def _peer_stream_loop(
        self,
        stream: AsyncIterator[Envelope],
        handles: dict[str, TransferHandle]
    ) -> None:
        async for resp in stream:
            data = self._serializer.decode(resp.payload)
            transfer_id = data["transfer_id"]
            handle = handles.get(transfer_id)

            if handle is None:
                logger.warning(
                    "Received transfer message for unknown transfer_id=%s",
                    transfer_id
                )
                continue

            await handle.queue.put(resp)

            kind = resp.kind
            if kind in ("rebalance.commit.ok", "rebalance.commit.reject"):
                handles.pop(transfer_id)
                if not handles:
                    break

    def _plan_envelope(self, transfers: list[RangeTransfer]) -> Envelope:
        plan_payload = self._serializer.encode({
            "epoch": self._epoch,
            "transfers": [transfer.to_dict() for transfer in transfers],
        })

        return Envelope.create(
            kind="rebalance.plan",
            payload=plan_payload,
            schema_id=self._serializer.schema_id,
        )

    def _rangeset(self, ranges: Iterable[RangeTransfer]) -> list[RangeSet]:
        result: dict[str, RangeSet] = {}

        for transfer in ranges:
            src = transfer.src
            dst = transfer.dst

            if src == self._node_id:
                rangeset = result.setdefault(dst, RangeSet(peer=dst, transfers=[]))
                rangeset.transfers.append(transfer)
            elif dst == self._node_id:
                rangeset = result.setdefault(dst, RangeSet(peer=src, transfers=[]))
                rangeset.transfers.append(transfer)

        return list(result.values())

    async def _run_transfer(self, handle: TransferHandle) -> None:
        pid = handle.transfer.pid
        state = await self._state_persistence.load()
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

    async def _send_chunk(
        self,
        handle: TransferHandle,
        seq: int,
        recs: list[dict[str, Any]],
        is_last: bool,
    ) -> None:
        """Push one rebalance.transfer chunk to the destination."""
        payload = self._serializer.encode({
            "epoch": self._epoch,
            "transfer_id": handle.transfer.id,
            "chunk_seq": seq,
            "is_last": is_last,
            "records": recs,
        })
        chunk_env = Envelope(
            kind="rebalance.transfer",
            payload=payload,
            correlation_id=handle.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await handle.client.send(chunk_env)

    async def _send_commit_if_last(
        self,
        data: dict[str, Any],
        records: list[Any],
        is_last_seen: bool,
        handle: TransferHandle,
    ) -> bool:
        """Send rebalance.commit when data['is_last'] is True and not yet sent.

        Return the updated is_last_seen flag so the caller can suppress any
        duplicate is_last chunks that may arrive on a resumed stream.
        """
        if not data.get("is_last") or is_last_seen:
            return is_last_seen
        digest = self._compute_transfer_digest(iter(records))
        commit_env = Envelope(
            kind="rebalance.commit",
            payload=self._serializer.encode({
                "epoch": self._epoch,
                "transfer_id": handle.transfer.id,
                "digest": digest
            }),
            correlation_id=handle.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await handle.client.send(commit_env)
        return True

    @staticmethod
    async def _stage_chunk(
        handle: TransferHandle,
        staging: PartitionStaging,
        data: dict[str, Any],
        records: list[TaggedRecord],
    ) -> None:
        """Stage records from one transfer chunk."""
        for rec_dict in data.get("records", []):
            rec = TaggedRecord.from_dict(rec_dict)
            await staging.stage(rec)
            records.append(rec)
            handle.bytes_done += len(rec.to_bytes(Namespace.TAGS))
        handle.chunks_done += 1
        if data.get("is_last"):
            handle.chunks_total = handle.chunks_done

    async def _stream_chunks(
        self,
        handle: TransferHandle,
        records: list[TaggedRecord],
        resume: Key | None,
    ) -> None:
        """Scan local store and push rebalance.transfer chunks to destination."""
        store = await self._storage.open_by_pid(handle.transfer.pid)
        chunk_buf: list[dict[str, Any]] = []
        chunk_bytes = 0
        chunk_seq = 0

        async for tagged in store.scan(resume_from=resume):
            records.append(tagged)
            rec_dict = tagged.to_dict()
            encoded = self._serializer.encode(tagged.to_bytes(Namespace.TAGS))
            encoded_len = len(encoded)

            if chunk_buf and chunk_bytes + encoded_len > self._max_chunk_bytes:
                await self._send_chunk(
                    handle, chunk_seq, chunk_buf, False
                )
                handle.bytes_done += chunk_bytes
                chunk_seq += 1
                handle.chunks_done = chunk_seq
                chunk_buf = []
                chunk_bytes = 0

            chunk_buf.append(rec_dict)
            chunk_bytes += encoded_len

        if chunk_buf:
            await self._send_chunk(
                handle, chunk_seq, chunk_buf, True
            )
            handle.bytes_done += chunk_bytes
            chunk_seq += 1
            handle.chunks_done = chunk_seq
        else:
            # Explicitly terminate empty scans with a final marker chunk.
            await self._send_chunk(
                handle, chunk_seq, [], True
            )
            chunk_seq += 1
            handle.chunks_done = chunk_seq

        handle.chunks_total = chunk_seq

    async def _update_state_committed(self, pid: int) -> None:
        async with self._state_lock:
            state = await self._state_persistence.load()
            if state is None:
                return
            new_staging = tuple(p for p in state.staging_pids if p != pid)
            new_committed = state.committed_pids + (pid,)
            await self._state_persistence.save(
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
