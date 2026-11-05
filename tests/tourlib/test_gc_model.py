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

from dataclasses import asdict

import pytest

from tourlib.adapter import MsgPackSerializer
from tourlib.exceptions import ValidationError
from tourlib.models.gc import GcInfo, GcJobs, GcRequest, GcResponse, GcStatus

pytestmark = pytest.mark.tourlib


def test_gc_models_roundtrip():
    serializer = MsgPackSerializer()
    status = GcStatus(
        job_id="job",
        state="completed",
        partition=7,
        scanned=9,
        collected=3,
        record_bytes=38400,
    )
    response = GcResponse("node", status)
    assert (
        GcResponse.from_document(
            serializer.decode(serializer.encode(response.to_dict()))
        )
        == response
    )
    info = GcInfo(True, 2, 600, 0, status)
    assert GcInfo.from_document(asdict(info)) == info
    assert GcRequest.from_document({"dry_run": True, "partition": 7}) == GcRequest(
        True, 7
    )
    jobs = GcJobs("node", info, "2026-01-01T00:00:00+00:00", (status,))
    assert (
        GcJobs.from_document(serializer.decode(serializer.encode(jobs.to_dict())))
        == jobs
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"node_id": ""},
        {"worker": None},
        {"next_pass_at": ""},
        {"jobs": [GcStatus()]},
        {"jobs": ("job",)},
    ],
)
def test_gc_jobs_validation(changes):
    info = GcInfo(True, 2, 600, 0, GcStatus())
    document = {"node_id": "node", "worker": info, "next_pass_at": None, "jobs": ()}
    with pytest.raises(ValidationError):
        GcJobs(**{**document, **changes})


@pytest.mark.parametrize(
    "document",
    [
        None,
        {"node_id": "node"},
        {"node_id": "n", "worker": {}, "next_pass_at": None, "jobs": {}},
    ],
)
def test_gc_jobs_document_validation(document):
    if isinstance(document, dict) and "worker" in document:
        document["worker"] = asdict(GcInfo(True, 2, 600, 0, GcStatus()))
    with pytest.raises(ValidationError):
        GcJobs.from_document(document)


@pytest.mark.parametrize(
    "document",
    [None, [], {"force": True}, {"dry_run": 1}, {"partition": True}, {"partition": -1}],
)
def test_gc_request_validation(document):
    with pytest.raises(ValidationError):
        GcRequest.from_document(document)


@pytest.mark.parametrize(
    "changes",
    [
        {"state": "bad"},
        {"job_id": 1},
        {"state": "running"},
        {"scanned": True},
        {"record_bytes": -1},
        {"collected": 1},
        {"dry_run": 1},
        {"error": ""},
        {"current_group": -1},
    ],
)
def test_gc_status_validation(changes):
    with pytest.raises(ValidationError):
        GcStatus(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"running": 1},
        {"versions": 0},
        {"interval": float("nan")},
        {"hot_queue_size": -1},
        {"status": {}},
    ],
)
def test_gc_worker_validation(changes):
    values = dict(
        running=True, versions=2, interval=600, hot_queue_size=0, status=GcStatus()
    )
    with pytest.raises(ValidationError):
        GcInfo(**{**values, **changes})
