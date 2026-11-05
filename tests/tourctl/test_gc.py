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
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from tourclient.core.services.gc import GcService
from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.main import main
from tourctl.bootstrap.usecases.gc import gc_run_cmd, gc_status_cmd
from tourctl.core.gc import gc_status, render_job, run_gc
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.gc import GcInfo, GcJobs, GcRequest, GcResponse, GcStatus

pytestmark = pytest.mark.tourctl

NOW_MS = int(datetime(2026, 11, 5, 10, 40, tzinfo=UTC).timestamp() * 1000)
STARTED = "2026-11-05T10:37:00+00:00"
FINISHED = "2026-11-05T10:37:01.400000+00:00"


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


def _job(job_id: str = "6505a1f2-0000", state: str = "queued", **options) -> GcStatus:
    if state != "queued":
        options.setdefault("started_at", STARTED)
    if state in {"completed", "failed", "cancelled"}:
        options.setdefault("finished_at", FINISHED)
    return GcStatus(job_id=job_id, state=state, **options)


def _ack(**options) -> dict:
    return GcResponse("node-1", _job(**options)).to_dict()


def _jobs(*jobs: GcStatus, running: bool = True, next_at: str | None = None) -> GcJobs:
    info = GcInfo(running, 2, 600, 3, jobs[0] if jobs else GcStatus())
    return GcJobs("node-1", info, next_at, tuple(jobs))


def _client(monkeypatch: pytest.MonkeyPatch, transport: _Transport) -> list:
    calls = []
    service = GcService(cast(Transport, transport), MsgPackSerializer())

    @asynccontextmanager
    async def context(*args, **kwargs):
        calls.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.gc.client", context)
    return calls


@pytest.mark.parametrize(
    "dry_run,mode", [(False, "collect"), (True, "estimate (read-only)")]
)
async def test_run_submits_on_peer_endpoint_and_hides_counts_while_queued(
    monkeypatch, capsys, dry_run, mode
):
    transport = _Transport([("gc.run.accept", _ack(dry_run=dry_run, partition=7))])
    calls = _client(monkeypatch, transport)
    await gc_run_cmd("127.0.0.1:7001", dry_run=dry_run, partition=7)
    text = capsys.readouterr().out
    assert transport.requests == [("gc.run", GcRequest(dry_run, 7))]
    assert calls[0][0] == ("gc",)
    assert calls[0][1]["endpoint"] == "peer"
    assert calls[0][1]["bind"] == "127.0.0.1:7001"
    assert f"Mode:  {mode}" in text
    assert "Scope: partition 7" in text
    assert "Job:   6505a1f2\n" in text
    assert "SCANNED" not in text
    assert "Follow with: tourctl gc status 127.0.0.1:7001 --job 6505a1f2" in text


@pytest.mark.parametrize("dry_run", [False, True])
async def test_wait_polls_status_for_the_exact_job_and_prints_json(
    monkeypatch, dry_run
):
    done = _job(
        state="completed", dry_run=dry_run, scanned=9, collected=3, record_bytes=38400
    )
    other = _job("aaaa", state="running")
    transport = _Transport(
        [
            ("gc.run.accept", _ack(dry_run=dry_run)),
            ("gc.status.accept", _jobs(other, done).to_dict()),
        ]
    )
    _client(monkeypatch, transport)
    text = await run_gc("peer:7001", dry_run=dry_run, wait=True, as_json=True)
    assert json.loads(text) == GcResponse("node-1", done).to_dict()
    assert transport.requests[-1] == ("gc.status", {})


@pytest.mark.parametrize("dry_run,label", [(False, "COLLECTED"), (True, "COLLECTIBLE")])
def test_completed_job_shows_counts_and_logical_bytes_note(dry_run, label):
    status = _job(
        state="completed",
        dry_run=dry_run,
        scanned=1204,
        collected=37,
        record_bytes=38400,
    )
    text = render_job("node-1", status, "peer:7001")
    assert "State: completed in 1.4s" in text
    assert label in text
    assert "1,204" in text
    assert "37.5 KiB" in text
    assert "the data file does not shrink" in text
    assert "Follow with" not in text


def test_running_job_shows_current_group_and_elapsed():
    status = _job(state="running", current_group=3)
    text = render_job("node-1", status, "peer:7001", NOW_MS)
    assert "State: running for 3m" in text
    assert "Group: 3" in text
    assert "Follow with" in text


async def test_wait_timeout_never_cancels_the_pass(monkeypatch):
    transport = _Transport([("gc.run.accept", _ack())])
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit, match="not cancelled") as caught:
        await run_gc("peer:7001", wait=True, timeout=0.001)
    assert caught.value.code == 5
    assert "tourctl gc status peer:7001 --job 6505a1f2" in str(caught.value)
    assert transport.requests == [("gc.run", GcRequest())]


@pytest.mark.parametrize("state", ["failed", "cancelled"])
async def test_failed_pass_exits_one_with_the_job(monkeypatch, state):
    failed = _job(state=state, error="disk unavailable" if state == "failed" else None)
    transport = _Transport(
        [("gc.run.accept", _ack()), ("gc.status.accept", _jobs(failed).to_dict())]
    )
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit) as caught:
        await run_gc("peer:7001", wait=True)
    assert caught.value.code == 1
    assert caught.value.details == GcResponse("node-1", failed).to_dict()
    assert f"State: {state} after 1.4s" in str(caught.value)


async def test_wait_fails_when_the_job_is_no_longer_retained(monkeypatch):
    transport = _Transport(
        [("gc.run.accept", _ack()), ("gc.status.accept", _jobs(_job("x")).to_dict())]
    )
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit, match="invalid GC response"):
        await run_gc("peer:7001", wait=True)


async def test_wait_fails_when_status_comes_from_another_node(monkeypatch):
    other = GcJobs("node-2", _jobs(_job()).worker, None, (_job(),))
    transport = _Transport(
        [("gc.run.accept", _ack()), ("gc.status.accept", other.to_dict())]
    )
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit, match="invalid GC response"):
        await run_gc("peer:7001", wait=True)


async def test_run_rejects_a_mismatched_acknowledgement(monkeypatch):
    transport = _Transport([("gc.run.accept", _ack(partition=3))])
    _client(monkeypatch, transport)
    with pytest.raises(SystemExit, match="invalid GC response"):
        await run_gc("peer:7001")


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
async def test_timeout_validation_happens_before_submission(timeout):
    with pytest.raises(SystemExit, match="positive and finite") as caught:
        await run_gc("peer:7001", timeout=timeout)
    assert caught.value.code == 2


def _many(count: int) -> list[GcStatus]:
    return [
        _job(f"{i:04d}aaaa-job", state="completed", scanned=i, collected=0)
        for i in range(count, 0, -1)
    ]


async def test_status_lists_five_recent_jobs_and_worker_settings(monkeypatch, capsys):
    jobs = _jobs(*_many(7), next_at="2026-11-05T10:44:00+00:00")
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    text = await gc_status("peer:7001", now_ms=NOW_MS)
    assert "Worker:    running" in text
    assert "Retention: 2 versions per key" in text
    assert "Interval:  every 10m, next in 4m" in text
    assert "Hot queue: 3 partitions" in text
    assert "JOB" in text and "COLLECTED" in text
    assert "0007aaaa" in text and "0002aaaa" not in text
    assert "Showing 5 of 7. Use --all to list every job." in text
    assert "Messages" not in text


async def test_status_all_lists_every_job(monkeypatch, capsys):
    jobs = _jobs(*_many(7))
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    await gc_status_cmd("peer:7001", all=True)
    text = capsys.readouterr().out
    assert "0001aaaa" in text
    assert "Showing 7 of 7.\n" in text


async def test_status_without_jobs_and_stopped_worker(monkeypatch):
    jobs = _jobs(running=False)
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    text = await gc_status("peer:7001", now_ms=NOW_MS)
    assert "Worker:    stopped" in text
    assert "Interval:  every 10m\n" in text
    assert "No GC pass has run since the node started." in text
    assert "The GC worker is stopped" in text


async def test_status_suggests_a_new_run_after_a_failed_pass(monkeypatch):
    jobs = _jobs(_job(state="failed", error="boom"))
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    text = await gc_status("peer:7001", now_ms=NOW_MS)
    assert "run it again with: tourctl gc run peer:7001" in text


async def test_status_json_is_filtered_with_total_and_messages(monkeypatch):
    jobs = _jobs(*_many(7))
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    document = json.loads(await gc_status("peer:7001", as_json=True))
    assert document["total"] == 7
    assert len(document["jobs"]) == 5
    assert document["node_id"] == "node-1"
    assert document["worker"]["versions"] == 2
    assert document["messages"] == []


async def test_status_job_prefix_shows_one_job(monkeypatch):
    jobs = _jobs(*_many(3))
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    text = await gc_status("peer:7001", job="0002", now_ms=NOW_MS)
    assert "Job:   0002aaaa" in text
    assert "SCANNED" in text


async def test_status_job_prefix_json(monkeypatch):
    jobs = _jobs(*_many(3))
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    document = json.loads(await gc_status("peer:7001", job="0003", as_json=True))
    assert document == {"node_id": "node-1", "job": jobs.jobs[0].to_dict()}


@pytest.mark.parametrize("prefix,code", [("000", 2), ("9", 4), ("", 2)])
async def test_status_job_prefix_must_be_unique_and_known(monkeypatch, prefix, code):
    jobs = _jobs(*_many(3))
    _client(monkeypatch, _Transport([("gc.status.accept", jobs.to_dict())]))
    with pytest.raises(SystemExit) as caught:
        await gc_status("peer:7001", job=prefix)
    assert caught.value.code == code


@pytest.mark.parametrize(
    "kind,payload,error",
    [
        ("gc.run.reject", {"message": "already queued"}, RequestRejectedError),
        ("gc.run.reject", {"message": 1}, ResponseError),
        ("gc.run.accept", {}, ResponseError),
        ("node.info", {}, ResponseError),
    ],
)
async def test_client_run_rejects_invalid_or_rejected_responses(kind, payload, error):
    service = GcService(
        cast(Transport, _Transport([(kind, payload)])), MsgPackSerializer()
    )
    with pytest.raises(error):
        await service.run(GcRequest())


async def test_client_status_rejects_invalid_payload():
    service = GcService(
        cast(Transport, _Transport([("gc.status.accept", {})])), MsgPackSerializer()
    )
    with pytest.raises(ResponseError, match="gc.status"):
        await service.status()


@pytest.mark.parametrize(
    "error,message",
    [
        (ConnectionRefusedError("offline"), "refused the GC connection"),
        (OSError("offline"), "Unable to reach"),
        (ResponseError("bad"), "invalid GC response"),
        (RequestRejectedError("gc.run", {"message": "busy"}), "busy"),
    ],
)
async def test_failures_map_to_cli_errors(monkeypatch, error, message):
    @asynccontextmanager
    async def context(*args, **kwargs):
        raise error
        yield

    monkeypatch.setattr("tourctl.core.gc.client", context)
    with pytest.raises(SystemExit, match=message):
        await run_gc("peer:7001")
    with pytest.raises(SystemExit, match=message):
        await gc_status("peer:7001")


async def test_rejects_wrong_service(monkeypatch):
    @asynccontextmanager
    async def context(*args, **kwargs):
        yield object()

    monkeypatch.setattr("tourctl.core.gc.client", context)
    with pytest.raises(SystemExit, match="Unknown service: gc"):
        await run_gc("peer:7001")


def test_main_renders_failed_status_as_json_on_stderr(monkeypatch, capsys):
    @asynccontextmanager
    async def context(*args, **kwargs):
        raise OSError("offline")
        yield

    monkeypatch.setattr("tourctl.core.gc.client", context)
    with pytest.raises(SystemExit) as caught:
        main(["gc", "status", "peer:7001", "--json"])
    assert caught.value.code == 3
    assert "error" in json.loads(capsys.readouterr().err)


def test_node_gc_is_gone(capsys):
    with pytest.raises(SystemExit) as caught:
        main(["node", "gc", "peer:7001"])
    assert caught.value.code == 2
