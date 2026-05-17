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
"""Peer-server connection handlers for all rebalance envelope kinds.

Handlers:
- RebalancePlanHandler   : rebalance.plan  (source-side: JOIN; dst-side: DRAIN plan-only)
- RebalanceTransferHandler: rebalance.transfer (destination-side streaming — DRAIN)
- RebalanceStatusHandler : rebalance.status
"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from tourillon.core.rebalance.digest import compute_transfer_digest
from tourillon.core.rebalance.plan import range_transfer_list_from_dict
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import record_from_dict

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.state import StatePort
    from tourillon.core.ports.storage import Storage
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope
    from tourillon.core.rebalance.applicator import RebalanceApplicator

logger = logging.getLogger(__name__)


class RebalancePlanHandler:
    """Server-side handler for rebalance.plan.

    In JOIN (source role, src == self.node_id):
      1. Validates epoch and that src matches this node.
      2. Sends rebalance.plan.ok.
      3. Scans the partition store and pushes rebalance.transfer chunks.
      4. Awaits rebalance.commit via server-side streaming receive (same cid).
      5. Validates digest; sends commit.ok or commit.reject.

    In DRAIN (destination role, dst == self.node_id):
      1. Validates epoch and that dst matches this node.
      2. Resolves resume_from from staging.
      3. Sends rebalance.plan.ok with resume_from.
      4. Returns (transfer chunks are handled by RebalanceTransferHandler).

    Rejects with reason epoch_mismatch or src_mismatch on validation failure.
    """

    def __init__(
        self,
        node_id: str,
        get_epoch: Callable[[], int],
        storage: Storage,
        serializer: SerializerPort,
        max_chunk_bytes: int = 1_048_576,
    ) -> None:
        self._node_id = node_id
        self._get_epoch = get_epoch
        self._storage = storage
        self._serializer = serializer
        self._max_chunk_bytes = max_chunk_bytes

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one rebalance.plan request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("rebalance.plan: malformed payload")
            return

        plan_epoch = int(data.get("epoch", -1))
        current_epoch = self._get_epoch()
        if plan_epoch != current_epoch:
            await self._reject(send, req, "epoch_mismatch")
            return

        try:
            transfers = range_transfer_list_from_dict(data.get("transfers", []))
        except ValueError as exc:
            reason = str(exc) if "malformed" in str(exc) else "malformed_transfers"
            await self._reject(send, req, reason)
            return

        src_nodes = {t.src for t in transfers}
        dst_nodes = {t.dst for t in transfers}

        if self._node_id in src_nodes:
            resume_raw = data.get("resume_from")
            resume = base64.b64decode(resume_raw) if resume_raw else None
            await self._handle_source(receive, send, req, transfers, resume)
        elif self._node_id in dst_nodes:
            await self._handle_drain_dst(send, req, transfers)
        else:
            await self._reject(send, req, "src_mismatch")

    async def _handle_source(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
        req: Envelope,
        transfers: list[Any],
        resume: bytes | None,
    ) -> None:
        """Source role: stream records then await commit."""
        pids = [pid for t in transfers for pid in t]
        dst_nodes = {t.dst for t in transfers}
        logger.info(
            "Rebalance source: serving %d pid(s) epoch=%d to %s.",
            len(pids),
            self._get_epoch(),
            ", ".join(sorted(dst_nodes)),
        )
        await self._send_plan_ok(send, req, resume_from=None)

        all_records = []
        chunk_seq = 0
        for t in transfers:
            for pid in t:  # iterate pids within each PartitionRangeTransfer
                store = self._storage.open_partition(pid)
                pid_records = []
                chunk_buf = []
                chunk_bytes = 0

                async for record in store.scan(resume_from=resume):
                    pid_records.append(record)
                    rec_dict = record.to_dict()
                    encoded = self._serializer.encode(rec_dict)
                    chunk_buf.append(rec_dict)
                    chunk_bytes += len(encoded)
                    if chunk_bytes >= self._max_chunk_bytes:
                        await self._send_chunk(
                            send, req, pid, chunk_seq, chunk_buf, False
                        )
                        chunk_seq += 1
                        chunk_buf = []
                        chunk_bytes = 0

                await self._send_chunk(send, req, pid, chunk_seq, chunk_buf, True)
                chunk_seq += 1
                all_records.extend(pid_records)

        commit_env = await receive()
        await self._process_commit(send, commit_env, all_records, resume)
        logger.debug(
            "Rebalance source: transfer complete for %d pid(s) to %s.",
            len(pids),
            ", ".join(sorted(dst_nodes)),
        )

    async def _handle_drain_dst(
        self,
        send: SendEnvelope,
        req: Envelope,
        transfers: list[Any],
    ) -> None:
        """Destination role (DRAIN): respond with plan.ok+resume_from."""
        resume_raw: str | None = None
        if transfers:
            t = transfers[0]
            first_pid = t.pid_start  # use pid_start from PartitionRangeTransfer
            store = self._storage.open_partition(first_pid)
            staging = store.staging(self._get_epoch())
            cursor = await staging.last_staged_log_key()
            if cursor:
                resume_raw = base64.b64encode(cursor).decode()
        await self._send_plan_ok(send, req, resume_from=resume_raw)

    async def _process_commit(
        self,
        send: SendEnvelope,
        commit_env: Envelope,
        records: list[Any],
        resume: bytes | None,
    ) -> None:
        """Validate digest and send commit.ok or commit.reject."""
        try:
            data = self._serializer.decode(commit_env.payload)
        except Exception:
            await self._send_commit_response(send, commit_env, ok=False)
            return

        expected = compute_transfer_digest(iter(records))
        received = data.get("digest", "")
        if expected != received:
            logger.error(
                "rebalance.commit digest mismatch: expected=%s received=%s",
                expected,
                received,
            )
            await self._send_commit_response(send, commit_env, ok=False)
            return

        await self._send_commit_response(send, commit_env, ok=True)

    async def _send_chunk(
        self,
        send: SendEnvelope,
        req: Envelope,
        pid: int,
        seq: int,
        recs: list[Any],
        is_last: bool,
    ) -> None:
        """Push one rebalance.transfer chunk to the destination."""
        payload = self._serializer.encode(
            {
                "epoch": self._get_epoch(),
                "pid": pid,
                "chunk_seq": seq,
                "is_last": is_last,
                "records": recs,
            }
        )
        env = Envelope.create(
            payload,
            kind="rebalance.transfer",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)

    async def _send_plan_ok(
        self,
        send: SendEnvelope,
        req: Envelope,
        resume_from: str | None,
    ) -> None:
        """Send rebalance.plan.ok with optional resume_from cursor."""
        body: dict[str, Any] = {"epoch": self._get_epoch()}
        if resume_from is not None:
            body["resume_from"] = resume_from
        payload = self._serializer.encode(body)
        env = Envelope.create(
            payload,
            kind="rebalance.plan.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)

    async def _send_commit_response(
        self,
        send: SendEnvelope,
        req: Envelope,
        *,
        ok: bool,
    ) -> None:
        """Send rebalance.commit.ok or rebalance.commit.reject."""
        kind = "rebalance.commit.ok" if ok else "rebalance.commit.reject"
        payload = self._serializer.encode({"epoch": self._get_epoch()})
        env = Envelope.create(
            payload,
            kind=kind,
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)

    async def _reject(self, send: SendEnvelope, req: Envelope, reason: str) -> None:
        """Send rebalance.plan.reject with the given reason."""
        payload = self._serializer.encode(
            {"epoch": self._get_epoch(), "reason": reason}
        )
        env = Envelope.create(
            payload,
            kind="rebalance.plan.reject",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)


class RebalanceTransferHandler:
    """Server-side streaming handler for rebalance.transfer (DRAIN destination).

    Receives transfer chunks via server-side streaming receive (multiple
    receive() calls), accumulates the SHA-256 digest, stages each record,
    then initiates commit exchange with the source.
    """

    def __init__(
        self,
        node_id: str,
        get_epoch: Callable[[], int],
        storage: Storage,
        state_port: StatePort,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._get_epoch = get_epoch
        self._storage = storage
        self._state_port = state_port
        self._serializer = serializer
        # Serialises concurrent state.toml writes from parallel DRAIN sessions.
        self._state_lock = asyncio.Lock()

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle a streaming rebalance.transfer session."""
        first = await receive()
        try:
            data = self._serializer.decode(first.payload)
        except Exception:
            logger.warning("rebalance.transfer: malformed first chunk")
            return

        pid = int(data.get("pid", -1))
        transfer_epoch = int(data.get("epoch", -1))
        current_epoch = self._get_epoch()
        if transfer_epoch != current_epoch:
            logger.warning(
                "rebalance.transfer epoch mismatch: got %d want %d",
                transfer_epoch,
                current_epoch,
            )
            return

        store = self._storage.open_partition(pid)
        staging = store.staging(current_epoch)
        records = []

        await self._process_chunk(data, staging, records)
        if not data.get("is_last"):
            done = await self._drain_chunks(receive, staging, records)
            if not done:
                return

        digest = compute_transfer_digest(iter(records))
        commit_payload = self._serializer.encode(
            {
                "epoch": current_epoch,
                "pid": pid,
                "digest": digest,
            }
        )
        commit_env = Envelope.create(
            commit_payload,
            kind="rebalance.commit",
            correlation_id=first.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(commit_env)

        ack = await receive()
        if ack.kind == "rebalance.commit.reject":
            logger.error("rebalance.commit.reject received — digest mismatch")
            await staging.cleanup()
            return

        await staging.commit()
        await self._update_state(pid)
        logger.info("pid=%d transfer committed (DRAIN destination)", pid)

    async def _drain_chunks(
        self,
        receive: ReceiveEnvelope,
        staging: Any,  # noqa: ANN401
        records: list[Any],
    ) -> bool:
        """Receive chunks until is_last; return True on success."""
        while True:
            env = await receive()
            try:
                data = self._serializer.decode(env.payload)
            except Exception:
                logger.warning("rebalance.transfer: malformed chunk")
                return False
            await self._process_chunk(data, staging, records)
            if data.get("is_last"):
                return True

    async def _process_chunk(
        self,
        data: dict[str, Any],
        staging: Any,  # noqa: ANN401
        records: list[Any],
    ) -> None:
        """Stage records from one chunk."""
        for rec_dict in data.get("records", []):
            rec = record_from_dict(rec_dict)
            await staging.stage(rec)
            records.append(rec)

    async def _update_state(self, pid: int) -> None:
        """Move pid from staging_pids to committed_pids in state.toml.

        Uses _state_lock to serialise concurrent writes from parallel DRAIN
        sessions.  Without the lock, concurrent os.replace() calls on Windows
        raise WinError 32 when two coroutines race on the same temp file.
        """
        from tourillon.core.lifecycle.state import NodeState

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


class RebalanceStatusHandler:
    """Server-side handler for rebalance.status.

    Calls applicator.status() and returns a rebalance.status.response
    envelope with the paginated transfer list serialised to msgpack.
    When no active rebalance, returns an empty-plan response.
    """

    def __init__(
        self,
        applicator: RebalanceApplicator,
        serializer: SerializerPort,
    ) -> None:
        self._applicator = applicator
        self._serializer = serializer

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one rebalance.status request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            data = {}

        after_pid = int(data.get("after_pid", 0))
        limit = int(data.get("limit", 500))

        result = await self._applicator.status(after_pid=after_pid, limit=limit)
        payload = self._serializer.encode(result)
        resp = Envelope.create(
            payload,
            kind="rebalance.status.response",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)


def register_rebalance_handlers(
    dispatcher: Any,  # noqa: ANN401
    node_id: str,
    get_epoch: Callable[[], int],
    storage: Storage,
    state_port: StatePort,
    applicator: RebalanceApplicator,
    serializer: SerializerPort,
    max_chunk_bytes: int = 1_048_576,
) -> None:
    """Register all rebalance responder handlers on *dispatcher*."""
    plan_handler = RebalancePlanHandler(
        node_id=node_id,
        get_epoch=get_epoch,
        storage=storage,
        serializer=serializer,
        max_chunk_bytes=max_chunk_bytes,
    )
    transfer_handler = RebalanceTransferHandler(
        node_id=node_id,
        get_epoch=get_epoch,
        storage=storage,
        state_port=state_port,
        serializer=serializer,
    )
    status_handler = RebalanceStatusHandler(
        applicator=applicator,
        serializer=serializer,
    )
    dispatcher.register("rebalance.plan", plan_handler)
    dispatcher.register("rebalance.transfer", transfer_handler)
    dispatcher.register("rebalance.status", status_handler)
