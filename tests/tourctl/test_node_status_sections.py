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
"""Storage, TLS and join or drain progress in ``tourctl node status``."""

from datetime import datetime

import pytest

from tests.helpers.inspect import make_antientropy, make_status
from tourctl.core.nodestatus import (
    format_count,
    render_status,
    status_messages,
    waiting_reason,
)
from tourlib.models.inspect import ProgressInfo, StorageInfo, TlsInfo, WaitingOn

pytestmark = pytest.mark.tourctl

NOW_MS = int(datetime.fromisoformat("2026-11-05T10:00:00+00:00").timestamp() * 1000)
GIB = 1024**3


def _storage(used: int = GIB // 4, versions: int = 1_200_000) -> StorageInfo:
    return StorageInfo(map_size=GIB, used=used, versions=versions, retention=2)


def _tls(expires_at: str = "2027-10-11T00:00:00+00:00", **kw) -> TlsInfo:
    return TlsInfo(expires_at=expires_at, **kw)


def test_render_status_shows_storage_tls_and_pending() -> None:
    status = make_status(
        antientropy=make_antientropy(
            pending_versions=270,
            pending_targets=3,
            oldest_wall_ms=NOW_MS - 14 * 60_000,
        ),
        storage=_storage(),
        tls=_tls(),
    )

    text = render_status(status, now_ms=NOW_MS)

    assert "  Pending:    270 versions to 3 nodes, oldest 14m\n" in text
    assert "  Retention" not in text
    assert (
        "Storage:\n"
        "  Map size: 1.0 GiB, 256.0 MiB used (25%)\n"
        "  Versions: 1.2 M (retention 2 per key)\n"
    ) in text
    assert (
        "TLS:\n"
        "  Certificate:         expires in 339d (2027-10-11)\n"
        "  Rejected handshakes: none in the last hour\n"
    ) in text
    assert text.endswith(
        "Messages:\n"
        "  270 versions wait for 3 nodes; see: tourctl ae status 10.0.0.3:7701"
    )


def test_pending_marks_a_truncated_scan_and_a_missing_age() -> None:
    info = make_antientropy(
        pending_versions=5, pending_targets=1, pending_truncated=True
    )

    text = render_status(make_status(antientropy=info), now_ms=NOW_MS)

    assert "  Pending:    at least 5 versions to 1 node\n" in text


def test_stopped_and_stuck_delivery_are_reported() -> None:
    info = make_antientropy(running=False, stuck=2)
    status = make_status(antientropy=info)

    text = render_status(status, now_ms=NOW_MS)

    assert "  Worker:     stopped\n" in text
    assert status_messages(status, NOW_MS) == [
        "Delivery to 2 nodes is stuck although they are ALIVE; "
        "see: tourctl ae status 10.0.0.3:7701",
        "The anti-entropy worker is stopped; nothing is delivered.",
    ]


def test_backlog_is_not_reported_while_the_worker_is_stopped() -> None:
    info = make_antientropy(running=False, backlog=True)
    status = make_status(phase="IDLE", antientropy=info)

    assert not any("backing up" in line for line in status_messages(status, NOW_MS))


def test_stopped_worker_is_expected_on_idle_node() -> None:
    status = make_status(phase="IDLE", antientropy=make_antientropy(running=False))

    assert not any(
        "anti-entropy worker" in line for line in status_messages(status, NOW_MS)
    )


@pytest.mark.parametrize(
    ("count", "text"),
    [(912, "912"), (45_300, "45.3 k"), (1_200_000, "1.2 M"), (3 * 10**9, "3.0 G")],
)
def test_format_count(count: int, text: str) -> None:
    assert format_count(count) == text


def test_full_storage_is_reported() -> None:
    status = make_status(storage=_storage(used=int(GIB * 0.89)))

    assert status_messages(status, NOW_MS) == [
        "Storage is 89% full. Collected versions free space for reuse; "
        "the file does not shrink."
    ]


def test_expiring_certificate_and_rejections_are_reported() -> None:
    tls = _tls(
        "2026-11-10T10:00:00+00:00",
        rejected=4,
        last_rejection="certificate verify failed",
        last_rejection_ago_s=120,
    )
    status = make_status(tls=tls)

    text = render_status(status, now_ms=NOW_MS)

    assert "expires in 5d (2026-11-10)" in text
    assert "4 in the last hour (last: certificate verify failed, 2m ago)" in text
    assert status_messages(status, NOW_MS) == [
        "The node certificate expires in 5d; renew it.",
        "4 TLS handshakes were rejected in the last hour; "
        "check peer and client certificates.",
    ]


def test_expired_certificate_and_rejection_without_reason() -> None:
    status = make_status(tls=_tls("2026-11-04T10:00:00+00:00", rejected=1))

    text = render_status(status, now_ms=NOW_MS)

    assert "EXPIRED 1d ago (2026-11-04)" in text
    assert "Rejected handshakes: 1 in the last hour\n" in text
    assert status_messages(status, NOW_MS) == [
        "The node certificate has EXPIRED; renew it and restart the node.",
        "1 TLS handshake was rejected in the last hour; "
        "check peer and client certificates.",
    ]


def test_rejection_without_age() -> None:
    status = make_status(tls=_tls(rejected=1, last_rejection="bad cert"))

    assert "1 in the last hour (last: bad cert)" in render_status(status, now_ms=NOW_MS)


def test_drain_progress_lists_the_blocking_peers() -> None:
    progress = ProgressInfo(
        goal="drain",
        total=48,
        done=46,
        waiting=(
            WaitingOn("node-0412", 2, "READY", "DEAD", since_s=1320),
            WaitingOn("node-0007", 1, "READY", "ALIVE"),
        ),
        waiting_more=3,
    )
    status = make_status(phase="DRAINING", progress=progress)

    text = render_status(status, now_ms=NOW_MS)

    assert (
        "Version: tourillon 1.2.3\n"
        "\n"
        "Progress:\n"
        "  46 of 48 partitions handed off\n"
        "\n"
        "WAITING ON   PARTITIONS   REASON\n"
        "node-0412    2            DEAD for 22m\n"
        "node-0007    1            in progress\n"
        "... 3 more   -            -\n"
        "\n"
        "Ring:\n"
    ) in text
    messages = status_messages(status, NOW_MS)
    assert "The drain resumes when node-0412 is ALIVE again." in messages


def test_join_first_pass_has_no_table() -> None:
    status = make_status(progress=ProgressInfo(goal="join", total=0, done=0))

    text = render_status(status, now_ms=NOW_MS)

    assert "Progress:\n  first pass in progress\n\nRing:" in text
    assert "WAITING ON" not in text


def test_join_progress_counts_copied_partitions() -> None:
    status = make_status(progress=ProgressInfo(goal="join", total=10, done=4))

    assert "4 of 10 partitions copied" in render_status(status, now_ms=NOW_MS)


@pytest.mark.parametrize(
    ("item", "reason"),
    [
        (WaitingOn("n", 1, None, "DEAD"), "not a member"),
        (WaitingOn("n", 1, "READY", "SUSPECT"), "SUSPECT"),
        (WaitingOn("n", 1, "READY", "ALIVE", retry_in_s=30), "retry in 30s"),
        (
            WaitingOn("n", 1, "READY", "ALIVE", retry_in_s=30, last_error="boom"),
            "retry in 30s: boom",
        ),
        (WaitingOn("n", 1, "READY", "ALIVE", last_error="boom"), "failed: boom"),
        (WaitingOn("n", 1, "READY", "ALIVE"), "in progress"),
    ],
)
def test_waiting_reason(item: WaitingOn, reason: str) -> None:
    assert waiting_reason(item) == reason


def test_certificate_expiring_within_a_day_shows_hours() -> None:
    status = make_status(tls=_tls("2026-11-05T13:00:00+00:00"))

    assert "expires in 3h (2026-11-05)" in render_status(status, now_ms=NOW_MS)
