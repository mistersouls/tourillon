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

from types import SimpleNamespace

import pytest

import tourillon.core.services.aeinspector as module
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.storageschema import Key, ObligationKey, Pid
from tourillon.core.services.aeinspector import AntiEntropyInspector
from tourillon.core.services.inspector import encode_cursor
from tourlib.exceptions import ValidationError
from tourlib.models.ae import AeStatusRequest, ObligationsRequest
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon

NOW_S = 1_800_000_000.0
NOW_MS = int(NOW_S * 1000)


def _ob(target: str, pid: int, key: bytes, age_s: float, counter: int = 0):
    ts = HLCTimestamp(NOW_MS - int(age_s * 1000), counter, "node-a")
    return ObligationKey(target, Key(Pid(pid), Address(key=key, namespace=b"ns"), ts))


class _Local:
    """Obligations per group, paged like META: sorted, exclusive raw cursor."""

    def __init__(self, groups: dict[int, list[ObligationKey]]) -> None:
        self.groups = {
            g: sorted(obs, key=lambda ob: ob.to_bytes()) for g, obs in groups.items()
        }

    async def obligation_page(self, group, after, limit, target=None):
        rows = [
            ob
            for ob in self.groups.get(group, [])
            if (target is None or ob.target == target)
            and (after is None or ob.to_bytes() > after)
        ]
        page = rows[:limit]
        return page, page[-1].to_bytes() if len(page) == limit else None


class _Ae:
    def __init__(self, local: _Local) -> None:
        self.local = local
        self.running = True
        self.started_at = NOW_S - 3600
        self.stuck_after = 60.0
        self.states: dict[str, tuple] = {}
        self.stats = SimpleNamespace(
            obligations_delivered=12,
            delivery_errors=2,
            sweeps=5,
            pids_diverged=4,
            pulled=31,
            pushed=7,
            handed_off=2,
            errors=3,
        )

    def delivery_state(self, target):
        return self.states.get(target, (0.0, None, None))


class _Topology:
    def __init__(self, registry) -> None:
        self.registry = registry

    async def snapshot(self):
        return SimpleNamespace(registry=self.registry)


class _Members:
    def __init__(self, statuses: dict[str, str]) -> None:
        self.statuses = statuses

    def status_of(self, node_id, count, now):
        return self.statuses.get(node_id, "ALIVE")


def _registry(**phases: MemberPhase) -> dict:
    return {node: SimpleNamespace(phase=phase) for node, phase in phases.items()}


def _inspector(groups, registry=None, statuses=None, total_groups=4):
    ae = _Ae(_Local(groups))
    if registry is None:
        registry = _registry(
            **{"node-1": MemberPhase.READY, "node-b": MemberPhase.READY}
        )
    inspector = AntiEntropyInspector(
        node_id="node-1",
        antientropy=ae,  # type: ignore[arg-type]
        total_groups=total_groups,
        topologymgr=_Topology(registry),  # type: ignore[arg-type]
        members=_Members(statuses or {}),  # type: ignore[arg-type]
        clock=lambda: NOW_S,
        monotonic=lambda: 50.0,
    )
    return inspector, ae


async def test_status_aggregates_targets_oldest_first_with_their_state():
    groups = {
        0: [_ob("node-b", 1, b"k1", 300), _ob("node-b", 2, b"k2", 30)],
        2: [_ob("node-c", 9, b"k3", 900), _ob("node-d", 9, b"k4", 120)],
    }
    registry = _registry(
        **{
            "node-1": MemberPhase.READY,
            "node-b": MemberPhase.READY,
            "node-d": MemberPhase.PAUSED,
        }
    )
    inspector, ae = _inspector(groups, registry, {"node-d": "SUSPECT"})
    ae.states["node-b"] = (0.0, "timeout after 2s", None)

    status = await inspector.status(AeStatusRequest())

    assert [t.target for t in status.items] == ["node-c", "node-b", "node-d"]
    c, b, d = status.items
    assert (c.phase, c.status, c.versions) == (None, "DEAD", 1)
    assert (b.versions, b.oldest_wall_ms) == (2, NOW_MS - 300_000)
    assert b.last_error == "timeout after 2s"
    assert (d.phase, d.status) == ("PAUSED", "SUSPECT")
    assert (status.targets, status.versions, status.truncated) == (3, 4, False)
    assert status.phase == "READY" and status.running
    assert status.started_at_ms == int((NOW_S - 3600) * 1000)
    assert status.snapshot_at_ms == NOW_MS
    counters = status.counters
    assert (counters.delivered, counters.delivery_errors) == (12, 2)
    assert (counters.pushed, counters.sweep_errors) == (9, 1)
    assert [t.stuck for t in status.items] == [False, True, False]
    assert status.stuck == 1


async def test_status_filters_debt_to_one_target():
    groups = {
        0: [_ob("node-b", 1, b"k1", 300), _ob("node-c", 2, b"k2", 30)],
        2: [_ob("node-b", 9, b"k3", 900)],
    }
    inspector, _ = _inspector(groups)

    status = await inspector.status(AeStatusRequest(target="node-b"))

    assert [item.target for item in status.items] == ["node-b"]
    assert status.targets == 1
    assert status.versions == 2


@pytest.mark.parametrize(
    "change",
    [
        lambda ae: ae.states.update({"node-b": (5.0, None, None)}),
        lambda ae: ae.states.update({"node-b": (0.0, None, 10.0)}),
        lambda ae: setattr(ae, "started_at", NOW_S - 30),
        lambda ae: setattr(ae, "started_at", None),
    ],
    ids=["deferred", "recent-delivery", "just-started", "stopped"],
)
async def test_a_target_is_not_stuck_when_something_explains_the_debt(change):
    inspector, ae = _inspector({0: [_ob("node-b", 1, b"k", 300)]})
    change(ae)

    status = await inspector.status(AeStatusRequest())

    assert status.items[0].stuck is False
    assert status.stuck == 0


async def test_young_debt_or_an_unreachable_target_is_not_stuck():
    groups = {0: [_ob("node-b", 1, b"k", 10), _ob("node-c", 1, b"k", 600)]}
    registry = _registry(
        **{
            "node-1": MemberPhase.READY,
            "node-b": MemberPhase.READY,
            "node-c": MemberPhase.READY,
        }
    )
    inspector, _ = _inspector(groups, registry, {"node-c": "SUSPECT"})

    status = await inspector.status(AeStatusRequest())

    assert status.stuck == 0


async def test_status_pages_targets_with_an_opaque_cursor():
    groups = {0: [_ob(f"node-{i}", 1, b"k", 100 - i) for i in range(5)]}
    inspector, _ = _inspector(groups)

    first = await inspector.status(AeStatusRequest(limit=2))
    second = await inspector.status(AeStatusRequest(limit=2, cursor=first.cursor))
    last = await inspector.status(AeStatusRequest(limit=2, cursor=second.cursor))

    assert [t.target for t in first.items] == ["node-0", "node-1"]
    assert [t.target for t in second.items] == ["node-2", "node-3"]
    assert [t.target for t in last.items] == ["node-4"]
    assert last.cursor is None
    assert first.targets == second.targets == 5


@pytest.mark.parametrize(
    "cursor", ["%%%", encode_cursor(("only",)), encode_cursor(("x", "node-b"))]
)
async def test_status_rejects_a_forged_cursor(cursor):
    inspector, _ = _inspector({0: [_ob("node-b", 1, b"k", 1)]})

    with pytest.raises(ValidationError, match="invalid cursor"):
        await inspector.status(AeStatusRequest(cursor=cursor))


async def test_status_counts_stop_at_the_scan_limit(monkeypatch):
    monkeypatch.setattr(module, "SCAN_LIMIT", 2)
    groups = {0: [_ob("node-b", 1, bytes([i]), 10) for i in range(4)]}
    inspector, _ = _inspector(groups)

    status = await inspector.status(AeStatusRequest())

    assert status.truncated is True
    assert status.versions == 2


async def test_status_of_an_unregistered_self_reports_an_unknown_phase():
    inspector, ae = _inspector({}, registry={})
    ae.running = False
    ae.started_at = None

    status = await inspector.status(AeStatusRequest())

    assert (status.phase, status.running, status.started_at_ms) == (
        "UNKNOWN",
        False,
        None,
    )
    assert status.items == () and status.targets == 0


async def test_obligations_pages_across_groups_and_filters_by_target():
    groups = {
        0: [_ob("node-b", 1, b"a", 5), _ob("node-c", 1, b"b", 5)],
        3: [_ob("node-b", 40, b"c", 9, counter=2), _ob("node-b", 41, b"d", 9)],
    }
    inspector, _ = _inspector(groups)

    first = await inspector.obligations(ObligationsRequest(target="node-b", limit=2))
    rest = await inspector.obligations(
        ObligationsRequest(target="node-b", limit=2, cursor=first.cursor)
    )
    every = await inspector.obligations(ObligationsRequest())

    assert [(o.partition, o.key) for o in first.items] == [(1, b"a"), (40, b"c")]
    assert first.items[1].counter == 2
    assert first.items[0].namespace == b"ns"
    assert first.items[0].wall_ms == NOW_MS - 5000
    assert first.total == rest.total == 3
    assert first.cursor is not None
    assert [o.key for o in rest.items] == [b"d"]
    assert rest.cursor is None
    assert every.total == 4 and len(every.items) == 4
    assert {o.target for o in every.items} == {"node-b", "node-c"}


async def test_obligations_mark_a_truncated_scan(monkeypatch):
    monkeypatch.setattr(module, "SCAN_LIMIT", 2)
    groups = {0: [_ob("node-b", 1, bytes([i]), 1) for i in range(5)]}
    inspector, _ = _inspector(groups)

    page = await inspector.obligations(ObligationsRequest(limit=2))

    assert page.truncated is True
    assert page.total == 2
    assert page.cursor is not None


@pytest.mark.parametrize(
    "cursor",
    ["%%%", encode_cursor((1,)), encode_cursor(("x", "00")), encode_cursor((1, "zz"))],
)
async def test_obligations_reject_a_forged_cursor(cursor):
    inspector, _ = _inspector({0: [_ob("node-b", 1, b"k", 1)]})

    with pytest.raises(ValidationError, match="invalid cursor"):
        await inspector.obligations(ObligationsRequest(cursor=cursor))
