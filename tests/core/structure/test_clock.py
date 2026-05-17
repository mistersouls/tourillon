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
"""Tests for HLCTimestamp and HLCClock — proposal 005 clock structures."""

from __future__ import annotations

import pytest

from tourillon.core.structure.clock import HLCClock, HLCTimestamp
from tourillon.core.structure.record import StoreKey, Version
from tourillon.core.testing.mem_storage import InMemoryStorage


@pytest.mark.rebalance
def test_hlctimestamp_total_order_wall_ms() -> None:
    """HLCTimestamp with higher wall_ms compares greater."""
    a = HLCTimestamp(wall_ms=100, counter=5, node_id="x")
    b = HLCTimestamp(wall_ms=200, counter=0, node_id="x")
    assert a < b
    assert b > a
    assert a != b


@pytest.mark.rebalance
def test_hlctimestamp_total_order_counter() -> None:
    """Same wall_ms: higher counter compares greater."""
    a = HLCTimestamp(wall_ms=100, counter=1, node_id="x")
    b = HLCTimestamp(wall_ms=100, counter=2, node_id="x")
    assert a < b


@pytest.mark.rebalance
def test_hlctimestamp_total_order_node_id() -> None:
    """Same wall_ms and counter: node_id tiebreak is lexicographic."""
    a = HLCTimestamp(wall_ms=100, counter=0, node_id="aaa")
    b = HLCTimestamp(wall_ms=100, counter=0, node_id="bbb")
    assert a < b


@pytest.mark.rebalance
def test_hlctimestamp_round_trip_dict() -> None:
    """to_dict() followed by from_dict() produces an equal HLCTimestamp."""
    ts = HLCTimestamp(wall_ms=999, counter=3, node_id="node-1")
    assert HLCTimestamp.from_dict(ts.to_dict()) == ts


@pytest.mark.rebalance
def test_hlcclock_tick_monotone() -> None:
    """Successive tick() calls return strictly increasing timestamps."""
    clk = HLCClock("node-1")
    prev = clk.tick()
    for _ in range(10):
        curr = clk.tick()
        assert curr > prev
        prev = curr


@pytest.mark.rebalance
def test_hlcclock_tick_counter_increments_when_wall_unchanged() -> None:
    """tick() increments counter when wall_ms does not advance."""
    clk = HLCClock("node-1")
    # Force clock to a fixed wall_ms so next tick stays in same ms bucket.
    clk._wall_ms = 10**18  # Far future; real time.time()*1000 is always smaller.
    t1 = clk.tick()
    t2 = clk.tick()
    assert t2.wall_ms == t1.wall_ms
    assert t2.counter == t1.counter + 1


@pytest.mark.rebalance
def test_hlcclock_update_advances_past_remote() -> None:
    """update() with a remote timestamp in the future advances the clock."""
    clk = HLCClock("node-1")
    future = HLCTimestamp(wall_ms=10**18, counter=5, node_id="node-2")
    result = clk.update(future)
    assert result.wall_ms == future.wall_ms
    assert result.counter == future.counter + 1


@pytest.mark.rebalance
def test_hlcclock_update_concurrent_same_wall() -> None:
    """update() when local and remote share the same wall_ms uses max(counter)+1."""
    clk = HLCClock("node-1")
    clk._wall_ms = 10**18
    clk._counter = 3
    remote = HLCTimestamp(wall_ms=10**18, counter=7, node_id="node-2")
    result = clk.update(remote)
    assert result.wall_ms == 10**18
    assert result.counter == 8  # max(3, 7) + 1


@pytest.mark.rebalance
def test_hlcclock_update_local_wall_ahead() -> None:
    """update() when local wall_ms is strictly ahead: counter increments by 1."""
    clk = HLCClock("node-1")
    clk._wall_ms = 10**18
    clk._counter = 2
    old_remote = HLCTimestamp(wall_ms=100, counter=99, node_id="node-2")
    result = clk.update(old_remote)
    assert result.wall_ms == 10**18
    assert result.counter == 3  # local_counter + 1


@pytest.mark.rebalance
def test_hlcclock_snapshot_returns_wall_and_counter() -> None:
    """snapshot() returns (wall_ms, counter) matching internal state."""
    clk = HLCClock("node-1")
    clk._wall_ms = 12345
    clk._counter = 7
    assert clk.snapshot() == (12345, 7)


@pytest.mark.rebalance
def test_hlcclock_restore_sets_wall_and_counter() -> None:
    """restore() with a higher wall_ms replaces both wall_ms and counter."""
    clk = HLCClock("node-1")
    clk.restore(9999, 42)
    assert clk.snapshot() == (9999, 42)


@pytest.mark.rebalance
def test_hlcclock_restore_ignores_lower_wall_ms() -> None:
    """restore() does not regress when persisted wall_ms is below current."""
    clk = HLCClock("node-1")
    clk._wall_ms = 10**18
    clk._counter = 3
    clk.restore(100, 99)  # stale checkpoint — must be ignored
    assert clk._wall_ms == 10**18
    assert clk._counter == 3


@pytest.mark.rebalance
def test_hlcclock_restore_raises_counter_on_equal_wall_ms() -> None:
    """restore() raises counter when persisted wall_ms equals current."""
    clk = HLCClock("node-1")
    clk._wall_ms = 5000
    clk._counter = 2
    clk.restore(5000, 10)  # same wall, higher counter
    assert clk._wall_ms == 5000
    assert clk._counter == 10


@pytest.mark.rebalance
def test_hlcclock_tick_after_restore_is_strictly_greater() -> None:
    """tick() after restore() never reuses a timestamp below the checkpoint."""
    clk = HLCClock("node-1")
    clk.restore(10**18, 999)  # Far-future checkpoint; real clock is lower.
    ts = clk.tick()
    assert ts.wall_ms == 10**18
    assert ts.counter == 1000  # counter + 1


@pytest.mark.kv
async def test_partition_store_max_hlc_empty_returns_none() -> None:
    """max_hlc() returns None when the partition has no records."""
    store = InMemoryStorage().open_partition(0)
    assert await store.max_hlc() is None


@pytest.mark.kv
async def test_partition_store_max_hlc_returns_greatest_timestamp() -> None:
    """max_hlc() returns the timestamp of the highest-HLC committed record."""
    storage = InMemoryStorage()
    store = storage.open_partition(0)
    ks = b"default"
    low = HLCTimestamp(wall_ms=1000, counter=0, node_id="n1")
    high = HLCTimestamp(wall_ms=2000, counter=5, node_id="n1")
    store.add_record(Version(address=StoreKey(ks, b"k1"), metadata=low, value=b"a"))
    store.add_record(Version(address=StoreKey(ks, b"k2"), metadata=high, value=b"b"))
    result = await store.max_hlc()
    assert result is not None
    assert result.wall_ms == 2000
    assert result.counter == 5
