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
"""Tests for rebalance server-side handlers — proposal 005 handler scenarios."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest

from tourillon.core.handlers.rebalance import (
    RebalancePlanHandler,
    RebalanceStatusHandler,
    RebalanceTransferHandler,
)
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.rebalance.applicator import RebalanceApplicator
from tourillon.core.rebalance.plan import (
    PartitionRangeTransfer,
    PartitionTransfer,
    RebalancePlan,
    TransferHandle,
    TransferState,
)
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import StoreKey, Tombstone, Version
from tourillon.core.testing.mem_storage import InMemoryStorage


class _JsonSerializer:
    schema_id = 1

    def encode(self, obj: object) -> bytes:
        return json.dumps(obj, default=self._default).encode()

    @staticmethod
    def _default(o: object) -> object:
        if isinstance(o, bytes):
            return {"__bytes__": base64.b64encode(o).decode()}
        return str(o)

    def decode(self, data: bytes) -> object:
        if not data:
            return {}
        return json.loads(data, object_hook=self._object_hook)

    @staticmethod
    def _object_hook(d: dict) -> object:
        if "__bytes__" in d:
            return base64.b64decode(d["__bytes__"])
        return d


class _MockStatePort:
    def __init__(self, node_id: str = "src") -> None:
        self._state = NodeState(
            node_id=node_id,
            phase=MemberPhase.READY,
            generation=1,
            seq=0,
            tokens=(),
            epoch=1,
            committed_pids=(),
            staging_pids=(),
        )

    async def load(self) -> NodeState:
        return self._state

    async def save(self, state: NodeState) -> None:
        self._state = state


def _envelope_queue(*envs: Envelope) -> asyncio.Queue[Envelope]:
    q: asyncio.Queue[Envelope] = asyncio.Queue()
    for e in envs:
        q.put_nowait(e)
    return q


def _ser() -> _JsonSerializer:
    return _JsonSerializer()


@pytest.mark.rebalance
async def test_21_plan_handler_epoch_mismatch() -> None:
    """Source node has epoch 3; plan has epoch 4 → rebalance.plan.reject epoch_mismatch."""
    storage = InMemoryStorage()
    ser = _ser()
    handler = RebalancePlanHandler(
        node_id="src",
        get_epoch=lambda: 3,
        storage=storage,
        serializer=ser,
    )

    plan_payload = ser.encode(
        {
            "epoch": 4,
            "transfers": [{"pid_start": 0, "pid_end": 0, "src": "src", "dst": "dst"}],
            "resume_from": None,
        }
    )
    req = Envelope(kind="rebalance.plan", payload=plan_payload)
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert len(sent) == 1
    assert sent[0].kind == "rebalance.plan.reject"
    resp_data = ser.decode(sent[0].payload)
    assert resp_data["reason"] == "epoch_mismatch"


@pytest.mark.rebalance
async def test_22_plan_handler_src_mismatch() -> None:
    """Plan lists src='other-node' but handler is 'src' → rebalance.plan.reject src_mismatch."""
    storage = InMemoryStorage()
    ser = _ser()
    handler = RebalancePlanHandler(
        node_id="src",
        get_epoch=lambda: 1,
        storage=storage,
        serializer=ser,
    )

    plan_payload = ser.encode(
        {
            "epoch": 1,
            "transfers": [
                {"pid_start": 0, "pid_end": 0, "src": "other-node", "dst": "dst"}
            ],
            "resume_from": None,
        }
    )
    req = Envelope(kind="rebalance.plan", payload=plan_payload)
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert len(sent) == 1
    assert sent[0].kind == "rebalance.plan.reject"
    resp_data = ser.decode(sent[0].payload)
    assert resp_data["reason"] == "src_mismatch"


@pytest.mark.rebalance
async def test_23_transfer_handler_full_stream() -> None:
    """Source streams N chunks with Version/Tombstone, last is_last=true → records staged; commit sent."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    ser = _ser()
    handler = RebalanceTransferHandler(
        node_id="dst",
        get_epoch=lambda: 1,
        storage=storage,
        state_port=state_port,
        serializer=ser,
    )

    hlc1 = HLCTimestamp(wall_ms=1000, counter=0, node_id="src")
    hlc2 = HLCTimestamp(wall_ms=2000, counter=0, node_id="src")
    rec1 = Version(address=StoreKey(b"ks", b"k1"), metadata=hlc1, value=b"val1")
    rec2 = Tombstone(address=StoreKey(b"ks", b"k2"), metadata=hlc2)

    chunk1_payload = ser.encode(
        {
            "epoch": 1,
            "pid": 42,
            "chunk_seq": 0,
            "is_last": False,
            "records": [rec1.to_dict()],
        }
    )
    chunk2_payload = ser.encode(
        {
            "epoch": 1,
            "pid": 42,
            "chunk_seq": 1,
            "is_last": True,
            "records": [rec2.to_dict()],
        }
    )

    chunk1 = Envelope(kind="rebalance.transfer", payload=chunk1_payload)
    chunk2 = Envelope(kind="rebalance.transfer", payload=chunk2_payload)
    commit_ok = Envelope(kind="rebalance.commit.ok", payload=b"{}")
    q = _envelope_queue(chunk1, chunk2, commit_ok)

    async def receive() -> Envelope:
        return await q.get()

    sent: list[Envelope] = []

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)

    assert any(e.kind == "rebalance.commit" for e in sent)
    # Verify staging has the records.
    staging = storage.open_partition(42).staging(1)
    assert len(staging.staged_records()) == 2


@pytest.mark.rebalance
async def test_24_status_handler_active_plan() -> None:
    """3 handles in RUNNING/COMMITTED/FAILED → paginated response with blocked=True."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    ser = _ser()

    from tourillon.core.transport.pool import PeerClientPool

    pool = PeerClientPool()
    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,
        state_port=state_port,
        storage=storage,
        serializer=ser,
        peer_addresses={},
    )

    plan = RebalancePlan(
        epoch=4,
        ranges=(
            PartitionRangeTransfer(pid_start=1, pid_end=1, src="a", dst="self"),
            PartitionRangeTransfer(pid_start=2, pid_end=2, src="a", dst="self"),
            PartitionRangeTransfer(pid_start=3, pid_end=3, src="a", dst="self"),
        ),
    )
    applicator._plan = plan
    applicator._handles = {
        1: TransferHandle(PartitionTransfer(1, "a", "self"), TransferState.RUNNING),
        2: TransferHandle(
            PartitionTransfer(2, "a", "self"),
            TransferState.COMMITTED,
            bytes_done=1024,  # non-zero → classified as active committed
        ),
        3: TransferHandle(
            PartitionTransfer(3, "a", "self"),
            TransferState.FAILED,
            last_error="unreachable",
        ),
    }

    handler = RebalanceStatusHandler(applicator=applicator, serializer=ser)
    req = Envelope(
        kind="rebalance.status",
        payload=ser.encode({"after_pid": 0, "limit": 500}),
    )
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert len(sent) == 1
    assert sent[0].kind == "rebalance.status.response"
    resp = ser.decode(sent[0].payload)
    assert resp["blocked"] is True
    assert resp["summary"]["failed"] == 1
    assert resp["summary"]["running"] == 1
    assert resp["summary"]["committed"] == 1
    assert any(t["last_error"] == "unreachable" for t in resp["transfers"])


@pytest.mark.rebalance
async def test_25_status_handler_no_plan_empty_response() -> None:
    """Applicator has no handles → response with empty transfers, no epoch."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    ser = _ser()
    from tourillon.core.transport.pool import PeerClientPool

    pool = PeerClientPool()
    applicator = RebalanceApplicator(
        node_id="self",
        pool=pool,
        state_port=state_port,
        storage=storage,
        serializer=ser,
        peer_addresses={},
    )

    handler = RebalanceStatusHandler(applicator=applicator, serializer=ser)
    req = Envelope(
        kind="rebalance.status",
        payload=ser.encode({"after_pid": 0, "limit": 500}),
    )
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    resp = ser.decode(sent[0].payload)
    assert resp["epoch"] is None
    assert resp["transfers"] == []
    assert resp["blocked"] is False


# ---------------------------------------------------------------------------
# Additional handler tests for improved coverage
# ---------------------------------------------------------------------------


@pytest.mark.rebalance
async def test_plan_handler_malformed_payload_ignored() -> None:
    """Malformed rebalance.plan payload → handler returns without sending a response."""
    storage = InMemoryStorage()
    ser = _ser()
    handler = RebalancePlanHandler(
        node_id="src", get_epoch=lambda: 1, storage=storage, serializer=ser
    )

    req = Envelope(kind="rebalance.plan", payload=b"not valid json or msgpack")
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert sent == []


@pytest.mark.rebalance
async def test_plan_handler_malformed_transfers_rejected() -> None:
    """Adjacent entries with same (src, dst) not merged → plan.reject malformed_transfers."""
    storage = InMemoryStorage()
    ser = _ser()
    handler = RebalancePlanHandler(
        node_id="src", get_epoch=lambda: 1, storage=storage, serializer=ser
    )

    plan_payload = ser.encode(
        {
            "epoch": 1,
            "transfers": [
                {"pid_start": 0, "pid_end": 4, "src": "src", "dst": "dst"},
                {
                    "pid_start": 5,
                    "pid_end": 9,
                    "src": "src",
                    "dst": "dst",
                },  # adjacent, same src/dst → malformed
            ],
            "resume_from": None,
        }
    )
    req = Envelope(kind="rebalance.plan", payload=plan_payload)
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert any(e.kind == "rebalance.plan.reject" for e in sent)
    reasons = [
        ser.decode(e.payload).get("reason")
        for e in sent
        if e.kind == "rebalance.plan.reject"
    ]
    assert "malformed_transfers" in reasons


@pytest.mark.rebalance
async def test_plan_handler_source_role_streams_then_commit_ok() -> None:
    """Source-side JOIN: plan.ok sent, records streamed, commit.ok sent on digest match."""
    from tourillon.core.rebalance.digest import compute_transfer_digest

    storage = InMemoryStorage()
    ser = _ser()
    pid = 10

    hlc = HLCTimestamp(wall_ms=1000, counter=0, node_id="src")
    rec = Version(address=StoreKey(b"ks", b"k"), metadata=hlc, value=b"v")
    storage.open_partition(pid).add_record(rec)

    handler = RebalancePlanHandler(
        node_id="src", get_epoch=lambda: 1, storage=storage, serializer=ser
    )

    plan_payload = ser.encode(
        {
            "epoch": 1,
            "transfers": [
                {"pid_start": pid, "pid_end": pid, "src": "src", "dst": "dst"}
            ],
            "resume_from": None,
        }
    )
    plan_env = Envelope(kind="rebalance.plan", payload=plan_payload)

    # After plan is processed the handler awaits a commit message.
    expected_digest = compute_transfer_digest(iter([rec]))
    commit_payload = ser.encode({"epoch": 1, "pid": pid, "digest": expected_digest})
    commit_env = Envelope(kind="rebalance.commit", payload=commit_payload)

    call_count = 0

    async def receive() -> Envelope:
        nonlocal call_count
        call_count += 1
        return plan_env if call_count == 1 else commit_env

    sent: list[Envelope] = []

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)

    kinds = [e.kind for e in sent]
    assert "rebalance.plan.ok" in kinds
    assert "rebalance.transfer" in kinds
    assert "rebalance.commit.ok" in kinds


@pytest.mark.rebalance
async def test_plan_handler_source_role_digest_mismatch_sends_reject() -> None:
    """Digest mismatch in rebalance.commit → source sends rebalance.commit.reject."""
    storage = InMemoryStorage()
    ser = _ser()
    pid = 11

    hlc = HLCTimestamp(wall_ms=2000, counter=0, node_id="src")
    rec = Version(address=StoreKey(b"ks", b"kx"), metadata=hlc, value=b"data")
    storage.open_partition(pid).add_record(rec)

    handler = RebalancePlanHandler(
        node_id="src", get_epoch=lambda: 1, storage=storage, serializer=ser
    )

    plan_payload = ser.encode(
        {
            "epoch": 1,
            "transfers": [
                {"pid_start": pid, "pid_end": pid, "src": "src", "dst": "dst"}
            ],
            "resume_from": None,
        }
    )
    plan_env = Envelope(kind="rebalance.plan", payload=plan_payload)
    commit_payload = ser.encode({"epoch": 1, "pid": pid, "digest": "wrong_digest_hex"})
    commit_env = Envelope(kind="rebalance.commit", payload=commit_payload)

    call_count = 0

    async def receive() -> Envelope:
        nonlocal call_count
        call_count += 1
        return plan_env if call_count == 1 else commit_env

    sent: list[Envelope] = []

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)

    assert any(e.kind == "rebalance.commit.reject" for e in sent)


@pytest.mark.rebalance
async def test_plan_handler_drain_dst_sends_plan_ok_with_resume() -> None:
    """DRAIN destination: receives plan as dst → plan.ok with resume_from (null when no staging)."""
    storage = InMemoryStorage()
    ser = _ser()
    pid = 20
    handler = RebalancePlanHandler(
        node_id="dst", get_epoch=lambda: 1, storage=storage, serializer=ser
    )

    plan_payload = ser.encode(
        {
            "epoch": 1,
            "transfers": [
                {"pid_start": pid, "pid_end": pid, "src": "src", "dst": "dst"}
            ],
            # No resume_from — DRAIN plan format
        }
    )
    req = Envelope(kind="rebalance.plan", payload=plan_payload)
    sent: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)

    assert len(sent) == 1
    assert sent[0].kind == "rebalance.plan.ok"
    resp = ser.decode(sent[0].payload)
    # No staging entries → resume_from absent or null
    assert resp.get("resume_from") is None or "resume_from" not in resp


@pytest.mark.rebalance
async def test_transfer_handler_epoch_mismatch_ignored() -> None:
    """rebalance.transfer with wrong epoch → handler returns without staging."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    ser = _ser()
    handler = RebalanceTransferHandler(
        node_id="dst",
        get_epoch=lambda: 2,
        storage=storage,
        state_port=state_port,
        serializer=ser,
    )

    chunk = Envelope(
        kind="rebalance.transfer",
        payload=ser.encode(
            {
                "epoch": 1,  # wrong epoch
                "pid": 42,
                "chunk_seq": 0,
                "is_last": True,
                "records": [],
            }
        ),
    )

    async def receive() -> Envelope:
        return chunk

    sent: list[Envelope] = []

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)
    assert sent == []


@pytest.mark.rebalance
async def test_transfer_handler_commit_reject_cleans_up_staging() -> None:
    """Source returns rebalance.commit.reject → staging.cleanup() is called."""
    storage = InMemoryStorage()
    state_port = _MockStatePort()
    ser = _ser()
    pid = 99
    handler = RebalanceTransferHandler(
        node_id="dst",
        get_epoch=lambda: 1,
        storage=storage,
        state_port=state_port,
        serializer=ser,
    )

    chunk1 = Envelope(
        kind="rebalance.transfer",
        payload=ser.encode(
            {"epoch": 1, "pid": pid, "chunk_seq": 0, "is_last": True, "records": []}
        ),
    )
    commit_reject = Envelope(kind="rebalance.commit.reject", payload=ser.encode({}))
    q = _envelope_queue(chunk1, commit_reject)

    async def receive() -> Envelope:
        return await q.get()

    sent: list[Envelope] = []

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)

    staging = storage.open_partition(pid).staging(1)
    assert not await staging.exists()


@pytest.mark.rebalance
async def test_plan_handler_source_empty_partition_sends_last_chunk() -> None:
    """Source role with zero records → single is_last=True chunk with empty records list."""
    storage = InMemoryStorage()
    ser = _ser()
    pid = 30

    handler = RebalancePlanHandler(
        node_id="src", get_epoch=lambda: 1, storage=storage, serializer=ser
    )

    from tourillon.core.rebalance.digest import compute_transfer_digest

    expected_digest = compute_transfer_digest(iter([]))
    plan_payload = ser.encode(
        {
            "epoch": 1,
            "transfers": [
                {"pid_start": pid, "pid_end": pid, "src": "src", "dst": "dst"}
            ],
            "resume_from": None,
        }
    )
    plan_env = Envelope(kind="rebalance.plan", payload=plan_payload)
    commit_payload = ser.encode({"epoch": 1, "pid": pid, "digest": expected_digest})
    commit_env = Envelope(kind="rebalance.commit", payload=commit_payload)

    call_count = 0

    async def receive() -> Envelope:
        nonlocal call_count
        call_count += 1
        return plan_env if call_count == 1 else commit_env

    sent: list[Envelope] = []

    async def send(env: Envelope) -> None:
        sent.append(env)

    await handler(receive, send)

    transfer_chunks = [e for e in sent if e.kind == "rebalance.transfer"]
    assert len(transfer_chunks) == 1
    chunk_data = ser.decode(transfer_chunks[0].payload)
    assert chunk_data["is_last"] is True
    assert chunk_data["records"] == []
    assert any(e.kind == "rebalance.commit.ok" for e in sent)
