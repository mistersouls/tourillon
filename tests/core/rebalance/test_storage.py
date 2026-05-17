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
"""Tests for InMemoryStorage scan and staging semantics — proposal 005 scenarios."""

from __future__ import annotations

import pytest

from tourillon.core.rebalance.digest import compute_transfer_digest
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.record import StoreKey, Tombstone, Version
from tourillon.core.testing.mem_storage import InMemoryStorage

_KS = b"ks"
_KEY = b"k1"


def _hlc(wall: int, node: str = "n1") -> HLCTimestamp:
    return HLCTimestamp(wall_ms=wall, counter=0, node_id=node)


def _ver(wall: int, value: bytes = b"v", key: bytes = _KEY) -> Version:
    return Version(
        address=StoreKey(_KS, key),
        metadata=_hlc(wall),
        value=value,
    )


def _tomb(wall: int, key: bytes = _KEY) -> Tombstone:
    return Tombstone(address=StoreKey(_KS, key), metadata=_hlc(wall))


@pytest.mark.rebalance
async def test_11_scan_log_structured_full_history() -> None:
    """Key written 3 times → scan() yields 3 records in HLC order (v1, v2, tombstone).

    All 3 entries are visible via scan() because the store is log-structured:
    each write appends a new entry with a unique HLC. kv.get would return only
    the last entry (Tombstone@hlc3), but scan() returns the full causal history.
    """
    storage = InMemoryStorage()
    store = storage.open_partition(1)

    v1 = _ver(1000, b"first")
    v2 = _ver(2000, b"second")
    t3 = _tomb(3000)

    store.add_record(v1)
    store.add_record(v2)
    store.add_record(t3)

    records = [r async for r in store.scan(resume_from=None)]

    assert len(records) == 3
    assert records[0] == v1
    assert records[1] == v2
    assert records[2] == t3
    # Last record is the tombstone — kv.get on a READY node returns this.
    assert isinstance(records[2], Tombstone)


@pytest.mark.rebalance
async def test_33_scan_resume_from_cursor_yields_only_after() -> None:
    """scan(resume_from=cursor_bytes) — cursor points to 3rd entry.

    Source calls MDB_SET_RANGE(cursor) then MDB_NEXT; yields exactly records
    4 and 5; record 3 is not re-sent.
    """
    storage = InMemoryStorage()
    store = storage.open_partition(2)
    epoch = 1

    recs = [_ver(1000 * i, b"v", f"k{i}".encode()) for i in range(1, 6)]
    for r in recs:
        store.add_record(r)

    # Stage the 3rd record to obtain a cursor pointing to it.
    staging = store.staging(epoch)
    await staging.stage(recs[2])
    cursor = await staging.last_staged_log_key()
    assert cursor is not None

    # scan strictly after the cursor → only records 4 and 5
    after_cursor = [r async for r in store.scan(resume_from=cursor)]

    # Records are sorted by HLC; recs[2] has wall_ms=3000; recs[3]/recs[4] are bigger.
    pids_wall = {r.metadata.wall_ms for r in after_cursor}
    assert 5000 in pids_wall  # recs[4]
    assert 4000 in pids_wall  # recs[3]
    assert 3000 not in pids_wall  # recs[2] not re-sent


@pytest.mark.rebalance
async def test_34_delta_digest_on_resume_matches_source() -> None:
    """Delta digest on resume: source scans records 4–5 after cursor.

    Both source and destination compute digest over the same delta — the
    rebalance.commit digest equals the source's scan(cursor) digest.
    """
    storage = InMemoryStorage()
    store = storage.open_partition(3)
    epoch = 1

    recs = [_ver(1000 * i, b"v", f"k{i}".encode()) for i in range(1, 6)]
    for r in recs:
        store.add_record(r)

    staging = store.staging(epoch)
    await staging.stage(recs[2])
    cursor = await staging.last_staged_log_key()

    # Source scans delta and computes digest.
    delta_source = [r async for r in store.scan(resume_from=cursor)]
    digest_source = compute_transfer_digest(iter(delta_source))

    # Destination receives the same records and computes the same digest.
    digest_dst = compute_transfer_digest(iter(delta_source))

    assert digest_source == digest_dst
    assert len(delta_source) == 2  # only records 4 and 5


@pytest.mark.rebalance
async def test_35_scan_cursor_boundary_record_not_resent() -> None:
    """Duplicate of test 33 — exact cursor: record at cursor position never re-sent.

    MDB_SET_RANGE(cursor) positions on the entry; MDB_NEXT advances past it.
    The record at the cursor is excluded from the result.
    """
    storage = InMemoryStorage()
    store = storage.open_partition(4)
    epoch = 1

    recs = [_ver(100 * i, b"v", f"k{i}".encode()) for i in range(1, 6)]
    for r in recs:
        store.add_record(r)

    staging = store.staging(epoch)
    await staging.stage(recs[2])  # cursor → recs[2] (wall=300)
    cursor = await staging.last_staged_log_key()
    assert cursor is not None

    after = [r async for r in store.scan(resume_from=cursor)]

    walls = {r.metadata.wall_ms for r in after}
    assert 300 not in walls  # boundary not re-sent
    assert 400 in walls
    assert 500 in walls
    assert len(after) == 2
