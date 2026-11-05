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

import json
import socket
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

import tourctl.core.nodestatus as nodestatus_module
from tests.helpers.inspect import (
    inspect_server,
    make_antientropy,
    make_gc,
    make_status,
    make_tourconf,
    reply,
)
from tourctl.bootstrap.main import main
from tourctl.core.error import EXIT_FAILED, EXIT_INVALID, EXIT_UNREACHABLE, CliError
from tourctl.core.nodestatus import node_status, render_status, status_messages
from tourlib.models.inspect import (
    MembersSummary,
    RingPositionInspect,
    VnodeRangeInspect,
)

pytestmark = pytest.mark.tourctl

NOW = "2026-11-05T10:00:00+00:00"


def _ms(text: str) -> int:
    return int(datetime.fromisoformat(text).timestamp() * 1000)


NOW_MS = _ms(NOW)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _run_ok(tokens: list[str]) -> None:
    try:
        main(tokens)
    except SystemExit as exc:
        assert exc.code in (0, None)


def test_render_status_shows_every_section() -> None:
    status = make_status(
        antientropy=make_antientropy(
            queued=3,
            inflight=1,
            errors=2,
            delivered=1234,
            last_sweep_at="2026-11-05T09:58:00+00:00",
            last_sweep_diverged=7,
        ),
        gc=make_gc(
            job_id="gc-1",
            state="completed",
            scanned=10,
            collected=4,
            finished_at="2026-11-05T09:55:00+00:00",
        ),
    )

    text = render_status(status, now_ms=NOW_MS)

    assert text == (
        "Name:    node-3\n"
        "Address: 10.0.0.3:7701\n"
        "Phase:   READY (epoch 3), since 2m5s\n"
        "Version: tourillon 1.2.3\n"
        "\n"
        "Ring:\n"
        "  Partitions: 256 of 1,024 (25%)\n"
        "  Vnodes:     2\n"
        "\n"
        "Members (view from this node):\n"
        "  3 members: 3 READY ALIVE, 0 others\n"
        "  See them with: tourctl cluster status\n"
        "\n"
        "Anti-entropy:\n"
        "  Pending:    none\n"
        "  Queue:      3 obligations queued, 1 in flight\n"
        "  Last sweep: 2m ago, 7 diverged\n"
        "  Delivered:  1,234\n"
        "  Errors:     2\n"
        "\n"
        "Garbage collection:\n"
        "  Last pass: 5m ago, 4 versions collected\n"
        "  Next pass: in 5m\n"
        "  Retention: 2 versions per key\n"
        "\n"
        "Messages:\n"
        "  Nothing to report."
    )


def test_render_status_with_vnodes_lists_tokens_and_wrapped_arcs() -> None:
    ring = RingPositionInspect(
        partition_shift=10,
        total_partitions=1024,
        owned_partitions=256,
        owned_percent=25.0,
        vnode_ranges=(
            VnodeRangeInspect(token="0x1a", arc=(900, 120), partitions=244),
            VnodeRangeInspect(token="0x2b", arc=(120, 132), partitions=12),
            VnodeRangeInspect(token="0x3c", arc=None, partitions=0),
        ),
    )

    text = render_status(make_status(ring_position=ring), vnodes=True)

    assert (
        "TOKEN   PARTITIONS    COUNT\n"
        "0x1a    901-120 (w)   244\n"
        "0x2b    121-132       12\n"
        "0x3c    -             0"
    ) in text


def test_vnode_label_of_an_empty_grid_keeps_the_raw_arc() -> None:
    item = VnodeRangeInspect(token="0x1", arc=(3, 5), partitions=2)

    assert nodestatus_module._partition_label(item, 0) == "3-5"


def test_render_status_without_phase_age_or_sweep() -> None:
    status = make_status(since_s=None, members=MembersSummary(total=1, ready_alive=1))

    text = render_status(status)

    assert "Phase:   READY (epoch 3)\n" in text
    assert "1 member: 1 READY ALIVE, 0 others" in text
    assert "Pending:    none" in text
    assert "Last sweep: none yet" in text
    assert "Errors" not in text
    assert "Last pass: none yet" in text
    assert "Next pass: within 10m" in text


@pytest.mark.parametrize(
    "gc,last,next_",
    [
        (make_gc(), "none yet", "within 10m"),
        (
            make_gc(job_id="j", state="running", scanned=1500),
            "running (j), 1,500 versions scanned",
            "within 10m",
        ),
        (
            make_gc(job_id="j", state="queued"),
            "queued (j), 0 versions scanned",
            "within 10m",
        ),
        (
            make_gc(
                job_id="j",
                state="completed",
                dry_run=True,
                scanned=9,
                collected=9,
                finished_at="2026-11-05T09:59:00+00:00",
            ),
            "1m ago, 9 versions collectable (dry run)",
            "in 9m",
        ),
        (
            make_gc(
                job_id="j",
                state="failed",
                error="disk full",
                finished_at="2026-11-05T09:00:00+00:00",
            ),
            "1h ago, failed: disk full",
            "due now",
        ),
        (
            make_gc(job_id="j", state="failed", finished_at=NOW),
            "0s ago, failed: unknown error",
            "in 10m",
        ),
        (
            make_gc(job_id="j", state="cancelled", finished_at=NOW),
            "0s ago, cancelled",
            "in 10m",
        ),
        (make_gc(running=False), "none yet", "worker stopped"),
    ],
)
def test_gc_last_and_next_pass(gc, last: str, next_: str) -> None:
    text = render_status(make_status(gc=gc), now_ms=NOW_MS)

    assert f"  Last pass: {last}\n" in text
    assert f"  Next pass: {next_}\n" in text


def test_gc_next_pass_without_interval_is_on_demand() -> None:
    gc = make_gc()
    object.__setattr__(gc, "interval", 0)

    assert nodestatus_module._next_pass(gc, NOW_MS) == "on demand only"


@pytest.mark.parametrize(
    "phase,message",
    [
        ("JOINING", "This node is JOINING; anti-entropy is filling its partitions."),
        ("DRAINING", "This node is DRAINING; anti-entropy is moving its partitions"),
        ("PAUSED", "This node is PAUSED; stand-ins serve its partitions."),
        ("IDLE", "Add it with: tourctl node join"),
        ("FAILED", "This node is FAILED; it owns no partitions."),
    ],
)
def test_status_messages_explain_the_phase(phase: str, message: str) -> None:
    lines = status_messages(make_status(phase=phase))

    assert len(lines) == 1
    assert lines[0].startswith(message) or message in lines[0]


@pytest.mark.parametrize(
    "ready,expected",
    [
        (2, "1 member is not READY and ALIVE; see: tourctl cluster status"),
        (0, "3 members are not READY and ALIVE; see: tourctl cluster status"),
    ],
)
def test_status_messages_count_the_other_members(ready: int, expected: str) -> None:
    status = make_status(members=MembersSummary(total=3, ready_alive=ready))

    assert status_messages(status) == [expected]


def test_status_messages_report_backlog_and_gc_problems() -> None:
    status = make_status(
        antientropy=make_antientropy(backlog=True, last_failure="peer gone"),
        gc=make_gc(running=False, job_id="j", state="failed", finished_at=NOW),
    )

    assert status_messages(status) == [
        "Obligations are backing up; delivery is slower than new work arrives.",
        "The GC worker is stopped; old versions are not collected.",
        "The last GC pass failed; run it again with: tourctl gc run 10.0.0.3:7701",
    ]


def test_status_messages_default_to_nothing_to_report() -> None:
    assert status_messages(make_status()) == ["Nothing to report."]


def test_render_status_caps_the_messages() -> None:
    status = make_status(
        phase="JOINING",
        members=MembersSummary(total=3, ready_alive=0),
        antientropy=make_antientropy(backlog=True),
        gc=make_gc(running=False, job_id="j", state="failed", finished_at=NOW),
    )

    assert len(status_messages(status)) == 5
    assert "... and" not in render_status(status, now_ms=NOW_MS)


async def test_node_status_renders_text_and_json_from_the_node(
    tmp_path: Path,
) -> None:
    status = make_status(antientropy=make_antientropy(last_failure="peer gone"))
    seen: list[Any] = []
    handler = reply("node.inspect.accept", status.to_document(), seen)

    async with inspect_server(tmp_path, {"node.inspect": handler}) as (
        address,
        tourconf,
    ):
        text = await node_status(address, tourconf=tourconf, now_ms=NOW_MS)
        document = json.loads(
            await node_status(address, as_json=True, tourconf=tourconf)
        )

    assert seen == [{}, {}]
    assert text == render_status(status, now_ms=NOW_MS)
    assert document["node_id"] == "node-3"
    assert document["antientropy"]["last_failure"] == "peer gone"
    assert document["messages"] == ["Nothing to report."]


async def test_node_status_maps_an_unreachable_node_to_exit_3(tmp_path: Path) -> None:
    tourconf, _ = await make_tourconf(tmp_path)

    with pytest.raises(CliError) as info:
        await node_status(f"127.0.0.1:{_free_port()}", tourconf=tourconf)

    assert info.value.code == EXIT_UNREACHABLE
    assert info.value.error == "unreachable"
    assert "status" in str(info.value)


async def test_node_status_maps_an_invalid_answer_to_exit_1(tmp_path: Path) -> None:
    handler = reply("node.inspect.accept", {"node_id": "x"})

    async with inspect_server(tmp_path, {"node.inspect": handler}) as (
        address,
        tourconf,
    ):
        with pytest.raises(CliError) as info:
            await node_status(address, tourconf=tourconf)

    assert info.value.code == EXIT_FAILED
    assert info.value.error == "invalid_response"


async def test_node_status_rejects_a_service_of_the_wrong_type(monkeypatch) -> None:
    @asynccontextmanager
    async def fake_client(*_args: Any, **_kwargs: Any):
        yield object()

    monkeypatch.setattr(nodestatus_module, "client", fake_client)

    with pytest.raises(CliError) as info:
        await node_status("127.0.0.1:1")

    assert info.value.code == EXIT_INVALID
    assert info.value.error == "unknown_service"


def test_cli_forwards_node_status_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_node_status(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "x" * 300

    monkeypatch.setattr("tourctl.bootstrap.usecases.node.node_status", fake_node_status)

    _run_ok(
        [
            "node",
            "status",
            "10.0.0.3:7701",
            "--vnodes",
            "--json",
            "--tourconf",
            "conf.toml",
            "--context",
            "prod",
        ]
    )

    assert capsys.readouterr().out == "x" * 300 + "\n"
    assert captured == {
        "address": "10.0.0.3:7701",
        "vnodes": True,
        "as_json": True,
        "tourconf": "conf.toml",
        "context_name": "prod",
    }


def test_cli_node_status_requires_an_address() -> None:
    with pytest.raises(SystemExit) as info:
        main(["node", "status"])

    assert info.value.code == EXIT_INVALID
