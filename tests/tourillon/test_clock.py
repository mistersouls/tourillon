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

import pytest

from tourillon.core.models.clock import HLCClock
from tourlib.models.ts import HLCTimestamp


@pytest.mark.tourillon
def test_hlc_timestamp_from_dict_accepts_wall_ms() -> None:
    ts = HLCTimestamp.from_dict({"wall_ms": 123, "counter": 4, "node_id": "node-a"})

    assert ts == HLCTimestamp(wall_ms=123, counter=4, node_id="node-a")


@pytest.mark.tourillon
def test_hlc_timestamp_from_dict_raises_when_wall_ms_missing() -> None:
    with pytest.raises(KeyError, match="wall_ms"):
        HLCTimestamp.from_dict({"wall": 123, "counter": 4, "node_id": "node-a"})


@pytest.mark.tourillon
def test_hlc_timestamp_from_dict_defaults_counter_and_node_id() -> None:
    ts = HLCTimestamp.from_dict({"wall_ms": 123, "counter": 0, "node_id": ""})

    assert ts == HLCTimestamp(wall_ms=123, counter=0, node_id="")


@pytest.mark.tourillon
def test_hlc_timestamp_from_dict_raises_when_wall_missing() -> None:
    with pytest.raises(KeyError, match="wall_ms"):
        HLCTimestamp.from_dict({"counter": 4, "node_id": "node-a"})


@pytest.mark.tourillon
def test_hlc_timestamp_rejects_node_id_over_63_bytes() -> None:
    with pytest.raises(ValueError, match="node_id must be <= 63 bytes"):
        HLCTimestamp(wall_ms=123, counter=0, node_id="a" * 64)


@pytest.mark.tourillon
def test_hlc_clock_rejects_node_id_over_63_bytes() -> None:
    with pytest.raises(ValueError, match="node_id must be <= 63 bytes"):
        HLCClock(node_id="a" * 64)


@pytest.mark.tourillon
def test_hlc_clock_tick_increments_counter_when_time_does_not_advance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tourillon.core.models.clock.time.time", lambda: 1.0)
    clock = HLCClock(node_id="node-a")

    first = clock.tick()
    second = clock.tick()

    assert first.wall_ms == 1000
    assert first.counter == 0
    assert second.wall_ms == 1000
    assert second.counter == 1


@pytest.mark.tourillon
def test_hlc_clock_restore_keeps_max_counter_for_same_wall_ms() -> None:
    clock = HLCClock(node_id="node-a")
    clock.restore(wall_ms=100, counter=2)

    clock.restore(wall_ms=100, counter=1)
    snapshot = clock.snapshot()

    assert snapshot.wall_ms == 100
    assert snapshot.counter == 2


@pytest.mark.tourillon
def test_hlc_clock_update_uses_remote_counter_when_remote_wall_is_max(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tourillon.core.models.clock.time.time", lambda: 0.0)
    clock = HLCClock(node_id="node-a")
    clock.restore(wall_ms=10, counter=1)
    remote = HLCTimestamp(wall_ms=20, counter=7, node_id="node-b")

    merged = clock.update(remote)

    assert merged.wall_ms == 20
    assert merged.counter == 8


@pytest.mark.tourillon
def test_hlc_clock_update_resets_counter_when_now_is_max(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tourillon.core.models.clock.time.time", lambda: 2.0)
    clock = HLCClock(node_id="node-a")
    clock.restore(wall_ms=1000, counter=3)
    remote = HLCTimestamp(wall_ms=1500, counter=4, node_id="node-b")

    merged = clock.update(remote)

    assert merged.wall_ms == 2000
    assert merged.counter == 0

