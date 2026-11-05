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
"""The enriched node status and the health verdict built by ``NodeReport``."""

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from tests.helpers.inspect import (
    make_antientropy,
    make_gc,
    make_page,
    make_status,
    make_view,
)
from tourillon.core.models.storageschema import StorageUsage
from tourillon.core.services.antientropy import GoalProgress
from tourillon.core.services.report import (
    NodeReport,
    critical_reasons,
    duration,
    warning_reasons,
)
from tourillon.core.transports.handshakes import Rejections
from tourlib.models.health import CRITICAL, OK, WARNING
from tourlib.models.inspect import (
    MembersRequest,
    PhaseCount,
    ProgressInfo,
    StorageInfo,
    TlsInfo,
    WaitingOn,
)

pytestmark = pytest.mark.tourillon

NOW = datetime(2026, 11, 5, 10, tzinfo=UTC).timestamp()
GIB = 1024**3


class _Inspector:
    def __init__(self, status=None, down=None, views=None) -> None:
        self.base = status or make_status()
        self.down = down or make_page(summary=(), total=0)
        self.views = views or {}
        self.requests: list[MembersRequest] = []

    async def status(self):
        return self.base

    async def members(self, request: MembersRequest):
        self.requests.append(request)
        return self.down

    async def member_view(self, node_id: str):
        return self.views.get(node_id)


class _AeInspector:
    def __init__(self, **pending: Any) -> None:
        defaults = dict(
            running=True, versions=0, targets=0, items=(), stuck=0, truncated=False
        )
        self.pending = SimpleNamespace(**{**defaults, **pending})
        self.limits: list[int] = []

    async def status(self, request):
        self.limits.append(request.limit)
        return self.pending


class _AntiEntropy:
    def __init__(self, goal: GoalProgress | None = None, states=None) -> None:
        self.goal = goal
        self.states = states or {}

    def delivery_state(self, target: str):
        return self.states.get(target, (0.0, None, None))


class _Storage:
    def __init__(self, used: int = GIB // 4) -> None:
        self.used = used
        self.groups: list[int] = []

    async def usage(self, total_groups: int) -> StorageUsage:
        self.groups.append(total_groups)
        return StorageUsage(map_size=GIB, used=self.used, versions=42)


class _Handshakes:
    def __init__(self, rejections: Rejections | None = None) -> None:
        self.rejections = rejections or Rejections(0)

    def snapshot(self) -> Rejections:
        return self.rejections


def _report(**kw: Any) -> NodeReport:
    args = dict(
        inspector=_Inspector(),
        ae_inspector=_AeInspector(),
        antientropy=_AntiEntropy(),
        storage=_Storage(),
        total_groups=4,
        handshakes=_Handshakes(),
        certificate_expires_at=datetime(2027, 10, 11, tzinfo=UTC),
        clock=lambda: NOW,
    )
    return NodeReport(**{**args, **kw})


async def test_status_adds_pending_storage_and_tls() -> None:
    item = SimpleNamespace(oldest_wall_ms=123)
    ae = _AeInspector(versions=270, targets=3, items=(item,), stuck=1, truncated=True)
    storage = _Storage()
    rejections = Rejections(2, "bad cert", 30.0)

    status = await _report(
        ae_inspector=ae, storage=storage, handshakes=_Handshakes(rejections)
    ).status()

    info = status.antientropy
    assert (info.pending_versions, info.pending_targets) == (270, 3)
    assert (info.oldest_wall_ms, info.stuck, info.pending_truncated) == (123, 1, True)
    assert ae.limits == [1]
    assert storage.groups == [4]
    assert status.storage == StorageInfo(GIB, GIB // 4, 42, 2)
    assert status.tls == TlsInfo("2027-10-11T00:00:00+00:00", 2, "bad cert", 30.0)
    assert status.progress is None


async def test_status_without_pending_has_no_oldest() -> None:
    status = await _report().status()

    assert status.antientropy.oldest_wall_ms is None
    assert status.antientropy.running is True


async def test_progress_ranks_the_waiting_peers_and_caps_them() -> None:
    waiting = {f"node-{i:02}": 1 for i in range(12)}
    waiting["node-big"] = 5
    goal = GoalProgress("drain", total=48, done=30, waiting=waiting)
    views = {"node-big": make_view("node-big", status="SUSPECT")}
    antientropy = _AntiEntropy(goal, {"node-big": (2.34567, "refused", None)})

    status = await _report(
        inspector=_Inspector(views=views), antientropy=antientropy
    ).status()

    progress = status.progress
    assert (progress.goal, progress.total, progress.done) == ("drain", 48, 30)
    assert progress.waiting_more == 3
    assert [w.node_id for w in progress.waiting][:3] == [
        "node-big",
        "node-00",
        "node-01",
    ]
    assert progress.waiting[0] == WaitingOn(
        "node-big", 5, "READY", "SUSPECT", 90.0, 2.346, "refused"
    )
    assert progress.waiting[1] == WaitingOn("node-00", 1, None, "DEAD", None, 0.0)


async def test_health_of_a_quiet_node_is_ok() -> None:
    inspector = _Inspector()

    health = await _report(inspector=inspector).health()

    assert (health.node_id, health.status, health.reasons) == ("node-3", OK, ())
    assert inspector.requests == [MembersRequest(statuses=("DEAD", "SUSPECT"), limit=1)]


async def test_health_ignores_stopped_ae_worker_on_idle_node() -> None:
    base = make_status(phase="IDLE")
    report = _report(
        inspector=_Inspector(base, make_page()),
        ae_inspector=_AeInspector(running=False),
    )

    health = await report.health()

    assert "ae_stopped" not in [r.code for r in health.reasons]


async def test_health_puts_critical_reasons_first() -> None:
    base = make_status(phase="DRAINING", gc=make_gc(running=False))
    down = make_page(
        make_view("node-9", status="DEAD"),
        summary=(PhaseCount("READY", dead=1, suspect=2),),
        total=3,
    )
    report = _report(
        inspector=_Inspector(base, down),
        ae_inspector=_AeInspector(running=False),
        storage=_Storage(used=int(GIB * 0.97)),
    )

    health = await report.health()

    assert health.status == CRITICAL
    assert [r.code for r in health.reasons] == [
        "storage_full",
        "ae_stopped",
        "gc_stopped",
        "phase",
        "members_down",
    ]
    assert health.reasons[-1].message == (
        "3 members not ALIVE (1 DEAD, 2 SUSPECT): node-9 DEAD for 1m"
    )


def _no_down():
    return make_page(summary=(), total=0)


def test_warning_reasons_cover_storage_certificate_and_pending() -> None:
    status = make_status(
        storage=StorageInfo(100, 85, 1, 2),
        tls=TlsInfo("2026-11-08T10:00:00+00:00"),
        antientropy=make_antientropy(
            pending_versions=7,
            pending_targets=1,
            oldest_wall_ms=int((NOW - 900) * 1000),
        ),
    )

    reasons = warning_reasons(status, _no_down(), NOW)

    assert [(r.code, r.message) for r in reasons] == [
        ("pending_old", "7 versions pending for 1 node, oldest 15m"),
        ("storage_high", "Storage is 85% full"),
        ("certificate_expiring", "Certificate expires in 3d (2026-11-08)"),
    ]
    assert critical_reasons(status, NOW) == []


def test_recent_pending_and_far_certificate_raise_nothing() -> None:
    status = make_status(
        storage=StorageInfo(100, 10, 1, 2),
        tls=TlsInfo("2030-01-01T00:00:00+00:00"),
        antientropy=make_antientropy(oldest_wall_ms=int((NOW - 60) * 1000)),
    )

    assert warning_reasons(status, _no_down(), NOW) == []
    assert critical_reasons(status, NOW) == []


def test_critical_reasons_cover_expiry_and_stuck_delivery() -> None:
    status = make_status(
        tls=TlsInfo("2026-11-01T00:00:00+00:00"),
        antientropy=make_antientropy(stuck=2),
    )

    reasons = critical_reasons(status, NOW)

    assert [(r.code, r.message, r.critical) for r in reasons] == [
        ("certificate_expired", "Certificate expired on 2026-11-01", True),
        (
            "delivery_stuck",
            "Delivery to 2 nodes is stuck; see: tourctl ae status 10.0.0.3:7701",
            True,
        ),
    ]


@pytest.mark.parametrize(
    ("progress", "message"),
    [
        (None, "Node is JOINING"),
        (ProgressInfo("join", 0, 0), "Node is JOINING"),
        (ProgressInfo("join", 8, 3), "Node is JOINING: 3 of 8 partitions done"),
    ],
)
def test_phase_reason_shows_progress(progress, message: str) -> None:
    status = make_status(phase="JOINING", progress=progress)

    reasons = warning_reasons(status, _no_down(), NOW)

    assert [(r.code, r.message) for r in reasons] == [("phase", message)]


def test_members_reason_without_listed_member() -> None:
    down = make_page(summary=(PhaseCount("READY", suspect=1),), total=1)
    down = replace(down, items=())

    reasons = warning_reasons(make_status(), down, NOW)

    assert reasons[0].message == "1 member not ALIVE (1 SUSPECT)"


def test_members_reason_without_since() -> None:
    view = replace(make_view("node-9", status="DEAD"), since_s=None)
    down = make_page(view, summary=(PhaseCount("READY", dead=1),), total=1)

    reasons = warning_reasons(make_status(), down, NOW)

    assert reasons[0].message == "1 member not ALIVE (1 DEAD): node-9 DEAD"


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(-5, "0s"), (45, "45s"), (840, "14m"), (7200, "2h"), (3 * 86400, "3d")],
)
def test_duration(seconds: float, text: str) -> None:
    assert duration(seconds) == text


async def test_health_with_warnings_only_is_a_warning() -> None:
    report = _report(inspector=_Inspector(make_status(phase="PAUSED")))

    health = await report.health()

    assert health.status == WARNING
    assert [(r.code, r.message) for r in health.reasons] == [
        ("phase", "Node is PAUSED")
    ]
