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
"""``tourctl node health``: verdict text, JSON and supervision exit codes."""

import json
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

import tourctl.core.nodehealth as nodehealth_module
from tests.helpers.inspect import inspect_server, make_tourconf, reply
from tourctl.bootstrap.main import main
from tourctl.core.nodehealth import HealthOutput, node_health, render_health
from tourlib.models.health import HealthReason, NodeHealth

pytestmark = pytest.mark.tourctl


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _warning() -> NodeHealth:
    return NodeHealth.from_reasons(
        "node-1",
        [
            HealthReason("members_down", "1 member not ALIVE (1 DEAD)"),
            HealthReason("pending_old", "270 versions pending for 3 nodes"),
        ],
    )


def test_render_health_lists_the_reasons() -> None:
    assert render_health(_warning()) == (
        "WARNING: node-1\n"
        "  1 member not ALIVE (1 DEAD)\n"
        "  270 versions pending for 3 nodes"
    )
    assert render_health(NodeHealth("node-1", "OK")) == "OK: node-1"


def test_render_health_caps_the_reasons_at_five() -> None:
    reasons = [HealthReason(f"c{i}", f"reason {i}", critical=True) for i in range(7)]

    text = render_health(NodeHealth.from_reasons("node-1", reasons))

    assert text.splitlines() == [
        "CRITICAL: node-1",
        *[f"  reason {i}" for i in range(5)],
        "  ... and 2 more",
    ]


@pytest.mark.parametrize(
    ("health", "code"),
    [
        (NodeHealth("node-1", "OK"), 0),
        (_warning(), 1),
        (
            NodeHealth.from_reasons(
                "node-1", [HealthReason("gc_stopped", "GC stopped", critical=True)]
            ),
            2,
        ),
    ],
)
async def test_node_health_maps_the_verdict_to_its_exit_code(
    tmp_path: Path, health: NodeHealth, code: int
) -> None:
    seen: list[Any] = []
    handler = reply("node.health.accept", health.to_document(), seen)

    async with inspect_server(tmp_path, {"node.health": handler}) as (
        address,
        tourconf,
    ):
        text = await node_health(address, tourconf=tourconf)
        document = await node_health(address, as_json=True, tourconf=tourconf)

    assert seen == [{}, {}]
    assert text == HealthOutput(render_health(health), code)
    assert document.exit_code == code
    assert json.loads(document.text) == {
        "node_id": "node-1",
        "status": health.status,
        "reasons": [
            {"code": reason.code, "message": reason.message}
            for reason in health.reasons
        ],
    }


async def test_unreachable_node_is_unknown(tmp_path: Path) -> None:
    tourconf, _ = await make_tourconf(tmp_path)
    address = f"127.0.0.1:{_free_port()}"

    text = await node_health(address, tourconf=tourconf)
    document = await node_health(address, as_json=True, tourconf=tourconf)

    assert text.exit_code == 3
    assert text.text.startswith(f"UNKNOWN: unable to reach {address} (")
    assert document.exit_code == 3
    payload = json.loads(document.text)
    assert payload["status"] == "UNKNOWN"
    assert payload["node_id"] is None
    assert payload["reasons"][0]["code"] == "unreachable"


async def test_invalid_answer_is_unknown(tmp_path: Path) -> None:
    handler = reply("node.health.accept", {"node_id": "x", "status": "BAD"})

    async with inspect_server(tmp_path, {"node.health": handler}) as (
        address,
        tourconf,
    ):
        output = await node_health(address, tourconf=tourconf)

    assert output.exit_code == 3
    assert output.text.startswith(
        "UNKNOWN: Tourillon node returned an invalid status response"
    )


async def test_wrong_service_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def fake_client(*_args: Any, **_kwargs: Any):
        yield object()

    monkeypatch.setattr(nodehealth_module, "client", fake_client)

    output = await node_health("127.0.0.1:1")

    assert output == HealthOutput("UNKNOWN: Unknown service: inspect", 3)


@pytest.mark.parametrize("code", [0, 2])
def test_cli_prints_the_verdict_and_exits_with_its_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], code: int
) -> None:
    captured: dict[str, Any] = {}

    async def fake_node_health(**kwargs: Any) -> HealthOutput:
        captured.update(kwargs)
        return HealthOutput("verdict", code)

    monkeypatch.setattr("tourctl.bootstrap.usecases.node.node_health", fake_node_health)

    with pytest.raises(SystemExit) as info:
        main(["node", "health", "10.0.0.1:7001", "--json", "--context", "prod"])

    assert info.value.code in ((0, None) if code == 0 else (code,))
    assert capsys.readouterr().out == "verdict\n"
    assert captured == {
        "address": "10.0.0.1:7001",
        "as_json": True,
        "tourconf": None,
        "context_name": "prod",
    }


def test_cli_node_health_requires_an_address() -> None:
    with pytest.raises(SystemExit) as info:
        main(["node", "health"])

    assert info.value.code == 2
