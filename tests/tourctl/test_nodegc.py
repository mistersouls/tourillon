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

from tourclient.core.services.node import NodeService
from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.usecases.node import node_gc
from tourctl.core.nodegc import _render, collect, format_record_bytes
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.gc import GcRequest, GcResponse, GcStatus, GcStatusRequest

pytestmark = pytest.mark.tourctl


class _Transport:
    def __init__(self, responses: list[tuple[str, Any]]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, object]] = []

    async def request(self, kind: str, document: object):
        self.requests.append((kind, document))
        response_kind, payload = self.responses.pop(0)
        serializer = MsgPackSerializer()
        return build_envelope(
            kind=response_kind,
            payload=serializer.encode(payload),
            schema_id=serializer.schema_id,
        )


def _response(state: str = "queued", **options) -> dict:
    return GcResponse("node", GcStatus(job_id="job", state=state, **options)).to_dict()


def _client(monkeypatch: pytest.MonkeyPatch, transport: _Transport) -> list:
    calls = []
    service = NodeService(cast(Transport, transport), MsgPackSerializer())

    @asynccontextmanager
    async def context(*args, **kwargs):
        calls.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.nodegc.client", context)
    return calls


@pytest.mark.parametrize("dry_run", [False, True])
async def test_command_submits_peer_job_with_scope_and_follow_hint(
    monkeypatch, dry_run
):
    transport = _Transport(
        [("node.gc.accept", _response(dry_run=dry_run, partition=7))]
    )
    calls = _client(monkeypatch, transport)
    text = await node_gc("127.0.0.1:7001", dry_run=dry_run, partition=7)
    assert "completion is asynchronous" in text
    assert "tourctl node inspect 127.0.0.1:7001" in text
    assert transport.requests == [("node.gc", GcRequest(dry_run, 7))]
    assert calls[0][1]["endpoint"] == "peer"
    assert calls[0][1]["bind"] == "127.0.0.1:7001"


@pytest.mark.parametrize("dry_run", [False, True])
async def test_wait_tracks_exact_job_and_keeps_json_bytes(monkeypatch, dry_run):
    payload = _response(
        "completed", dry_run=dry_run, scanned=9, collected=3, record_bytes=38400
    )
    transport = _Transport(
        [
            ("node.gc.accept", _response(dry_run=dry_run)),
            ("node.gc.status.accept", payload),
        ]
    )
    _client(monkeypatch, transport)
    text = await node_gc("peer:7001", dry_run=dry_run, wait=True, json=True)
    assert json.loads(text) == payload
    assert transport.requests[-1] == ("node.gc.status", GcStatusRequest("job"))


async def test_wait_timeout_never_cancels_worker(monkeypatch):
    transport = _Transport([("node.gc.accept", _response())])
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit, match="not cancelled.*node inspect"):
        await node_gc("peer:7001", wait=True, timeout=0.001)
    assert transport.requests == [("node.gc", GcRequest())]


@pytest.mark.parametrize("state", ["failed", "cancelled"])
async def test_failed_jobs_have_nonzero_exit_and_structured_result(monkeypatch, state):
    result = _response(state, error="unavailable" if state == "failed" else None)
    transport = _Transport(
        [("node.gc.accept", _response()), ("node.gc.status.accept", result)]
    )
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit) as caught:
        await node_gc("peer:7001", wait=True, json=True)
    assert json.loads(str(caught.value)) == result


async def test_wait_rejects_different_job(monkeypatch):
    result = GcResponse("node", GcStatus(job_id="other", state="completed"))
    transport = _Transport(
        [("node.gc.accept", _response()), ("node.gc.status.accept", result.to_dict())]
    )
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit, match="different node or job"):
        await node_gc("peer:7001", wait=True)


@pytest.mark.parametrize(
    "kind,payload,error",
    [
        ("node.gc.reject", {"message": "already queued"}, RequestRejectedError),
        ("node.gc.reject", {"message": 1}, ResponseError),
        ("node.gc.accept", {}, ResponseError),
        ("node.info", {}, ResponseError),
    ],
)
async def test_client_rejects_invalid_or_rejected_responses(kind, payload, error):
    service = NodeService(
        cast(Transport, _Transport([(kind, payload)])), MsgPackSerializer()
    )
    with pytest.raises(error):
        await service.gc(GcRequest())


@pytest.mark.parametrize(
    "error,message",
    [
        (ConnectionRefusedError("offline"), "refused the GC connection"),
        (OSError("offline"), "Unable to reach"),
        (ResponseError("bad"), "invalid GC response"),
        (RequestRejectedError("node.gc", {"message": "busy"}), "busy"),
    ],
)
async def test_command_maps_failures(monkeypatch, error, message):
    @asynccontextmanager
    async def context(*args, **kwargs):
        raise error
        yield

    monkeypatch.setattr("tourctl.core.nodegc.client", context)
    with pytest.raises(SystemExit, match=message):
        await collect("peer:7001")


@pytest.mark.parametrize(
    "size,text",
    [(53, "53 B"), (38400, "37.5 KiB"), (1 << 20, "1.0 MiB"), (1 << 30, "1.0 GiB")],
)
def test_binary_record_units(size, text):
    assert format_record_bytes(size) == text


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
async def test_timeout_validation_happens_before_submission(timeout):
    with pytest.raises(SystemExit, match="positive and finite"):
        await collect("peer:7001", timeout=timeout)


@pytest.mark.parametrize("dry_run,label", [(False, "Collected"), (True, "Collectible")])
def test_completed_human_output_uses_binary_units(dry_run, label):
    result = GcResponse(
        "node",
        GcStatus(
            job_id="job",
            state="completed",
            dry_run=dry_run,
            scanned=9,
            collected=3,
            record_bytes=38400,
        ),
    )
    output = _render(result, "peer:7001", False)
    assert "GC completed on node" in output
    assert label in output
    assert "37.5 KiB" in output
    assert "Follow with" not in output


async def test_gc_rejects_wrong_service(monkeypatch):
    @asynccontextmanager
    async def context(*args, **kwargs):
        yield object()

    monkeypatch.setattr("tourctl.core.nodegc.client", context)
    with pytest.raises(SystemExit, match="Unknown service: node"):
        await collect("peer:7001")
