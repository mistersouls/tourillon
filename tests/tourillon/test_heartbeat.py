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

import math

import pytest

from tourillon.core.gossip.heartbeat import HeartbeatTable, parse_heartbeats
from tourillon.core.probe.detector import PeerStatus

pytestmark = pytest.mark.tourillon


class _Clock:
    def __init__(self, now: float = 10.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def _table(clock: _Clock, interval: float = 1.0) -> HeartbeatTable:
    return HeartbeatTable("self", interval=interval, clock=clock, wall=lambda: 2.5)


def test_counter_starts_from_the_wall_clock_in_milliseconds() -> None:
    table = _table(_Clock())

    assert table.node_id == "self"
    assert table.table() == {"self": 2500}


@pytest.mark.parametrize("interval", [0, -1.0])
def test_interval_must_be_positive(interval: float) -> None:
    with pytest.raises(ValueError, match="interval must be > 0"):
        HeartbeatTable("self", interval=interval)


def test_default_clocks_are_usable() -> None:
    table = HeartbeatTable("self")

    assert table.beat() == table.table()["self"]
    assert table.age("other") is None


def test_beat_advances_own_counter_and_heard_time() -> None:
    clock = _Clock()
    table = _table(clock)

    clock.now = 20.0
    assert table.beat() == 2501
    assert table.beat() == 2502
    assert table.table() == {"self": 2502}
    assert table.age("self") == 0.0


def test_table_returns_a_copy() -> None:
    table = _table(_Clock())

    table.table()["intruder"] = 1

    assert "intruder" not in table.table()


def test_merge_keeps_the_maximum_and_records_heard_time() -> None:
    clock = _Clock()
    table = _table(clock)

    clock.now = 12.0
    assert table.merge({"a": 5, "b": 7, "self": 99_999}) == 2
    clock.now = 15.0
    assert table.merge({"a": 5, "b": 3}) == 0
    clock.now = 16.0
    assert table.merge({"a": 6}) == 1

    assert table.table() == {"self": 2500, "a": 6, "b": 7}
    assert table.age("a", 20.0) == 4.0
    assert table.age("b", 20.0) == 8.0


def test_merge_accepts_a_zero_counter_for_a_new_member() -> None:
    table = _table(_Clock())

    assert table.merge({"a": 0}) == 1
    assert table.table()["a"] == 0


def test_forget_drops_members_outside_the_registry_but_never_self() -> None:
    table = _table(_Clock())
    table.merge({"a": 1, "b": 1})

    table.forget(["a"])

    assert table.table() == {"self": 2500, "a": 1}
    assert table.age("b") is None
    assert table.age("self") == 0.0


def test_age_uses_the_clock_and_never_goes_negative() -> None:
    clock = _Clock()
    table = _table(clock)
    table.merge({"a": 1})

    clock.now = 13.5
    assert table.age("a") == 3.5
    assert table.age("a", now=5.0) == 0.0
    assert table.age("never") is None


@pytest.mark.parametrize("members,rounds", [(0, 1), (1, 1), (2, 1), (8, 3), (1024, 10)])
def test_thresholds_grow_with_log2_of_the_cluster(members: int, rounds: int) -> None:
    table = _table(_Clock(), interval=30.0)

    suspect, dead = table.thresholds(members)

    assert suspect == pytest.approx(30.0 * (rounds + 3))
    assert dead == pytest.approx(3 * suspect)
    assert math.log2(max(2, members)) == rounds


def test_status_follows_the_heartbeat_age() -> None:
    clock = _Clock(0.0)
    table = _table(clock)
    table.merge({"a": 1})
    suspect, dead = table.thresholds(8)

    assert table.status("a", 8, suspect - 0.1) is PeerStatus.ALIVE
    assert table.status("a", 8, suspect) is PeerStatus.SUSPECT
    assert table.status("a", 8, dead) is PeerStatus.DEAD
    assert table.status("self", 8, dead * 10) is PeerStatus.ALIVE
    clock.now = dead
    assert table.status("a", 8) is PeerStatus.DEAD


def test_never_heard_member_is_aged_from_the_local_start() -> None:
    clock = _Clock(100.0)
    table = _table(clock)
    suspect, dead = table.thresholds(2)

    assert table.status("ghost", 2, 100.0 + suspect - 1) is PeerStatus.ALIVE
    assert table.status("ghost", 2, 100.0 + suspect) is PeerStatus.SUSPECT
    assert table.status("ghost", 2, 100.0 + dead) is PeerStatus.DEAD
    assert table.status("ghost", 2, 50.0) is PeerStatus.ALIVE


def test_parse_heartbeats_keeps_only_valid_entries() -> None:
    raw = {
        "a": 1,
        "b": 0,
        "": 3,
        7: 4,
        "neg": -1,
        "flag": True,
        "float": 1.5,
        "text": "9",
    }

    assert parse_heartbeats(raw) == {"a": 1, "b": 0}


@pytest.mark.parametrize("raw", [None, [], "a=1", 3])
def test_parse_heartbeats_ignores_non_tables(raw: object) -> None:
    assert parse_heartbeats(raw) == {}
