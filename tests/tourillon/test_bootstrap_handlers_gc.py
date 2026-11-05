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

import tourillon.bootstrap.handlers.gc as gc_handler
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import build_envelope
from tourlib.models.gc import GcInfo, GcJobs, GcStatus

pytestmark = pytest.mark.tourillon


class _GcStub:
    def __init__(self):
        self.calls = []
        self.error = None
        self.status = GcStatus(job_id="job", state="queued")
        self.next_pass_at = "2026-11-05T10:44:00+00:00"

    def request(self, **options):
        self.calls.append(options)
        if self.error:
            raise self.error
        self.status = GcStatus(job_id="job", state="queued", **options)

    def inspect(self):
        return GcInfo(True, 2, 600, 0, self.status)

    def jobs(self):
        return (self.status,)


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.node_id = "node-local"
        self.gc = _GcStub()


async def _call(monkeypatch, handler, kind, document):
    manager = _ManagerStub()
    monkeypatch.setattr(gc_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind=kind,
        payload=manager.serializer.encode(document),
        schema_id=manager.serializer.schema_id,
    )
    responses = [item async for item in handler(request)]
    assert len(responses) == 1
    assert responses[0].correlation_id == request.correlation_id
    return manager, responses[0], manager.serializer.decode(responses[0].payload)


@pytest.mark.parametrize(
    "document,accepted",
    [({"dry_run": True, "partition": 7}, True), ({"force": True}, False)],
)
async def test_gc_run_validates_and_preserves_correlation(
    monkeypatch, document, accepted
):
    manager, response, payload = await _call(
        monkeypatch, gc_handler.run, "gc.run", document
    )
    assert response.kind == f"gc.run.{'accept' if accepted else 'reject'}"
    if accepted:
        assert payload["status"]["job_id"] == "job"
        assert manager.gc.calls == [{"dry_run": True, "partition": 7}]


async def test_gc_run_reports_busy_worker(monkeypatch):
    manager = _ManagerStub()
    manager.gc.error = ValueError("GC pass already queued or running")
    monkeypatch.setattr(gc_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="gc.run",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )
    responses = [item async for item in gc_handler.run(request)]
    assert responses[0].kind == "gc.run.reject"
    assert (
        "already queued" in manager.serializer.decode(responses[0].payload)["message"]
    )


async def test_gc_status_returns_worker_and_jobs(monkeypatch):
    _, response, payload = await _call(monkeypatch, gc_handler.status, "gc.status", {})
    assert response.kind == "gc.status.accept"
    jobs = GcJobs.from_document(payload)
    assert jobs.node_id == "node-local"
    assert jobs.next_pass_at == "2026-11-05T10:44:00+00:00"
    assert [job.job_id for job in jobs.jobs] == ["job"]


@pytest.mark.parametrize("document", [{"job_id": "job"}, [], None])
async def test_gc_status_rejects_arguments(monkeypatch, document):
    _, response, payload = await _call(
        monkeypatch, gc_handler.status, "gc.status", document
    )
    assert response.kind == "gc.status.reject"
    assert "no arguments" in payload["message"]
