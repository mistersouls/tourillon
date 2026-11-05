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
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest

from tourclient.core.services.ae import AeService
from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.main import main
from tourctl.bootstrap.usecases.ae import ae_obligations_cmd, ae_status_cmd
from tourctl.core.ae import (
    ERROR_WIDTH,
    ae_obligations,
    ae_status,
    key_text,
    next_try,
    short_error,
    status_messages,
)
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.ae import (
    AeCounters,
    AeStatus,
    AeStatusRequest,
    AeTarget,
    ObligationsPage,
    ObligationsRequest,
    ObligationView,
)

pytestmark = pytest.mark.tourctl

WIRE = MsgPackSerializer()
NOW_MS = 1_800_000_000_000


class _Transport:
    def __init__(self, responses: list[tuple[str, Any]]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, object]] = []

    async def request(self, kind: str, document: object):
        self.requests.append((kind, document))
        response_kind, payload = self.responses.pop(0)
        return build_envelope(
            kind=response_kind,
            payload=WIRE.encode(payload),
            schema_id=WIRE.schema_id,
        )


def _client(monkeypatch, transport: _Transport) -> list:
    calls = []
    service = AeService(cast(Transport, transport), WIRE)

    @asynccontextmanager
    async def context(*args, **kwargs):
        calls.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.ae.client", context)
    return calls


def _target(name="node-2", **changes) -> AeTarget:
    fields = {
        "target": name,
        "versions": 30,
        "oldest_wall_ms": NOW_MS - 86_000,
        "phase": "READY",
        "status": "ALIVE",
    }
    fields.update(changes)
    return AeTarget(**fields)


def _status(*items: AeTarget, **changes) -> AeStatus:
    fields = {
        "node_id": "node-1",
        "phase": "READY",
        "running": True,
        "started_at_ms": NOW_MS - 3_600_000,
        "snapshot_at_ms": NOW_MS,
        "stuck_after_s": 60.0,
        "counters": AeCounters(delivered=12, delivery_errors=1, sweeps=2),
        "items": items,
        "targets": len(items),
        "versions": sum(item.versions for item in items),
        "stuck": sum(item.stuck for item in items),
    }
    fields.update(changes)
    return AeStatus(**fields)


def _view(target="node-2", namespace=b"", key=b"", partition=7) -> ObligationView:
    return ObligationView(
        target,
        partition,
        namespace or WIRE.encode("accounts"),
        key or WIRE.encode("user:42"),
        NOW_MS,
        2,
        "node-1",
    )


@pytest.mark.parametrize(
    "item,expected",
    [
        (_target(status="SUSPECT"), "when ALIVE"),
        (_target(phase=None), "not a member"),
        (_target(phase="IDLE"), "when READY"),
        (_target(retry_in_s=1.2), "in 2s"),
        (_target(), "now"),
    ],
)
def test_next_try_explains_what_defers_delivery(item, expected):
    assert next_try(item) == expected


def test_short_error_keeps_one_table_cell():
    assert short_error(None) is None
    assert short_error("timeout") == "timeout"
    text = short_error("x" * 100)
    assert len(text) == ERROR_WIDTH and text.endswith("...")


async def test_status_renders_worker_counters_and_targets(monkeypatch, capsys):
    status = _status(_target(status="DEAD", last_error="OSError: unreachable"))
    transport = _Transport([("ae.status.accept", status.to_document())])
    calls = _client(monkeypatch, transport)
    await ae_status_cmd("127.0.0.1:7001")
    text = capsys.readouterr().out
    assert transport.requests == [("ae.status", AeStatusRequest().to_document())]
    assert calls[0][0] == ("ae",)
    assert calls[0][1]["endpoint"] == "peer"
    assert calls[0][1]["bind"] == "127.0.0.1:7001"
    assert "Worker: running" in text
    assert "Delivery" in text and "Sweep" in text
    assert "node-2" in text and "1m26s" in text and "when ALIVE" in text
    assert "OSError: unreachable" in text
    assert "Showing 1 of 1." in text
    assert "Next page" not in text


async def test_status_without_debt_says_so(monkeypatch):
    _client(monkeypatch, _Transport([("ae.status.accept", _status().to_document())]))
    text = await ae_status("peer:7001")
    assert "No replication obligations are pending." in text
    assert "Messages" not in text


async def test_status_footer_repeats_non_default_options(monkeypatch):
    status = _status(_target(), targets=4, versions=90, cursor="abc")
    _client(monkeypatch, _Transport([("ae.status.accept", status.to_document())]))
    text = await ae_status("peer:7001", limit=1, context_name="prod")
    assert (
        "Showing 1 of 4. Next page: tourctl ae status peer:7001 --limit 1 "
        "--context prod --cursor abc"
    ) in text


def test_status_messages_cap_stuck_targets_and_flag_the_worker():
    items = tuple(_target(f"node-{i}", stuck=True) for i in range(7))
    status = _status(*items, stuck=9, running=False, truncated=True)
    lines = status_messages(status)
    assert sum("is ALIVE and nothing defers it" in line for line in lines) == 5
    assert any("more" in line for line in lines)
    assert any("check this node's logs" in line for line in lines)
    assert any("worker is stopped" in line for line in lines)
    assert any("lower bounds" in line for line in lines)


def test_status_messages_count_stuck_targets_beyond_the_page():
    lines = status_messages(_status(_target(stuck=True), stuck=3))
    assert "1 version" not in lines[0]
    assert "2 more targets look stuck beyond this page." in lines


async def test_status_json_includes_messages(monkeypatch):
    status = _status(_target(stuck=True))
    _client(monkeypatch, _Transport([("ae.status.accept", status.to_document())]))
    document = json.loads(await ae_status("peer:7001", as_json=True))
    assert document["items"][0]["target"] == "node-2"
    assert document["messages"] == status_messages(status)


async def test_obligations_render_decoded_keys_and_next_page(monkeypatch, capsys):
    page = ObligationsPage("node-1", NOW_MS, "node-2", (_view(),), 7, "c1")
    transport = _Transport([("ae.obligations.accept", page.to_document())])
    _client(monkeypatch, transport)
    await ae_obligations_cmd("peer:7001", target="node-2", limit=1)
    text = capsys.readouterr().out
    assert transport.requests == [
        ("ae.obligations", ObligationsRequest("node-2", 1).to_document())
    ]
    assert "Target:  node-2" in text
    assert "accounts/user:42" in text
    assert "c=2 node-1" in text
    assert (
        "Showing 1 of 7. Next page: tourctl ae obligations peer:7001 "
        "--target node-2 --limit 1 --cursor c1"
    ) in text


def test_key_text_falls_back_to_raw_bytes():
    assert "user:42" in key_text(_view(namespace=b"\xc1", key=WIRE.encode("user:42")))
    assert key_text(_view(namespace=b"\xc1")) != "accounts/user:42"


@pytest.mark.parametrize(
    "page,target,expected",
    [
        (ObligationsPage("node-1", NOW_MS, None, (), 0), None, "pending."),
        (ObligationsPage("node-1", NOW_MS, "n", (), 0), "n", "pending to n."),
        (ObligationsPage("node-1", NOW_MS, None, (), 3), None, "after this cursor."),
    ],
)
async def test_obligations_empty_pages(monkeypatch, page, target, expected):
    _client(monkeypatch, _Transport([("ae.obligations.accept", page.to_document())]))
    text = await ae_obligations("peer:7001", target=target)
    assert text.endswith(expected)


async def test_obligations_json_decodes_keys(monkeypatch):
    page = ObligationsPage("node-1", NOW_MS, None, (_view(),), 9_000, "c", True)
    _client(monkeypatch, _Transport([("ae.obligations.accept", page.to_document())]))
    document = json.loads(await ae_obligations("peer:7001", as_json=True))
    item = document["items"][0]
    assert (item["namespace"], item["key"]) == ("accounts", "user:42")
    assert item["version"]["counter"] == 2
    assert document["truncated"] is True and document["cursor"] == "c"


@pytest.mark.parametrize(
    "call",
    [
        lambda: ae_status("peer:7001", limit=0),
        lambda: ae_obligations("peer:7001", cursor=""),
    ],
)
async def test_invalid_options_exit_before_connecting(call):
    with pytest.raises(SystemExit) as caught:
        await call()
    assert caught.value.code == 2


async def test_rejected_cursor_exits_as_invalid(monkeypatch):
    reject = {"code": "invalid_request", "message": "invalid cursor"}
    _client(monkeypatch, _Transport([("ae.obligations.reject", reject)]))
    with pytest.raises(SystemExit, match="invalid cursor") as caught:
        await ae_obligations("peer:7001", cursor="zzz")
    assert caught.value.code == 2


@pytest.mark.parametrize(
    "kind,payload,error",
    [
        ("ae.status.reject", {"message": "busy"}, RequestRejectedError),
        ("ae.status.reject", {"message": ""}, ResponseError),
        ("ae.status.accept", {}, ResponseError),
        ("node.info", {}, ResponseError),
    ],
)
async def test_client_rejects_invalid_or_rejected_responses(kind, payload, error):
    service = AeService(cast(Transport, _Transport([(kind, payload)])), WIRE)
    with pytest.raises(error):
        await service.status(AeStatusRequest())


async def test_failures_map_to_cli_errors(monkeypatch):
    @asynccontextmanager
    async def context(*args, **kwargs):
        raise OSError("offline")
        yield

    monkeypatch.setattr("tourctl.core.ae.client", context)
    with pytest.raises(SystemExit) as caught:
        await ae_status("peer:7001")
    assert caught.value.code == 3


async def test_rejects_wrong_service(monkeypatch):
    @asynccontextmanager
    async def context(*args, **kwargs):
        yield object()

    monkeypatch.setattr("tourctl.core.ae.client", context)
    with pytest.raises(SystemExit, match="Unknown service: ae"):
        await ae_obligations("peer:7001")


def test_main_runs_ae_status(monkeypatch, capsys):
    _client(monkeypatch, _Transport([("ae.status.accept", _status().to_document())]))
    try:
        main(["ae", "status", "peer:7001"])
    except SystemExit as exc:
        assert not exc.code
    assert "No replication obligations are pending." in capsys.readouterr().out
