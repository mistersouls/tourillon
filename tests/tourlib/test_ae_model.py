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

from tourlib.adapter import MsgPackSerializer
from tourlib.exceptions import ValidationError
from tourlib.models.ae import (
    MAX_LIMIT,
    AeCounters,
    AeStatus,
    AeStatusRequest,
    AeTarget,
    ObligationsPage,
    ObligationsRequest,
    ObligationView,
)

pytestmark = pytest.mark.tourlib


def _status(**changes) -> AeStatus:
    fields = {
        "node_id": "node-1",
        "phase": "READY",
        "running": True,
        "started_at_ms": 1_000,
        "snapshot_at_ms": 2_000,
        "stuck_after_s": 60.0,
        "counters": AeCounters(delivered=3, sweeps=1),
        "items": (
            AeTarget("node-2", 2, 1_500, "READY", "SUSPECT", 1.5, "timeout"),
            AeTarget("node-3", 1, 1_700, None, "DEAD"),
        ),
        "targets": 2,
        "versions": 3,
        "stuck": 0,
        "cursor": "abc",
    }
    fields.update(changes)
    return AeStatus(**fields)


def _page() -> ObligationsPage:
    item = ObligationView("node-2", 7, b"ns", b"\x00k", 1_500, 2, "node-1")
    return ObligationsPage("node-1", 2_000, "node-2", (item,), 1, "c", True)


@pytest.mark.parametrize("model", [_status(), _page()], ids=["status", "page"])
def test_responses_round_trip_through_the_wire_serializer(model):
    serializer = MsgPackSerializer()
    document = serializer.decode(serializer.encode(model.to_document()))
    assert type(model).from_document(document) == model


@pytest.mark.parametrize(
    "request_type,document",
    [
        (AeStatusRequest, {"limit": 5, "cursor": "x"}),
        (ObligationsRequest, {"target": "node-2", "limit": 5, "cursor": None}),
    ],
)
def test_requests_round_trip_and_default_missing_fields(request_type, document):
    assert request_type.from_document(document).to_document() == {
        **request_type().to_document(),
        **document,
    }
    assert request_type.from_document({}) == request_type()


@pytest.mark.parametrize(
    "build",
    [
        lambda: AeStatusRequest(limit=0),
        lambda: AeStatusRequest(limit=MAX_LIMIT + 1),
        lambda: AeStatusRequest(limit=True),
        lambda: AeStatusRequest(cursor=""),
        lambda: ObligationsRequest(target=""),
        lambda: AeStatusRequest.from_document({"limit": 5, "force": True}),
        lambda: ObligationsRequest.from_document(["target"]),
    ],
)
def test_requests_reject_invalid_input(build):
    with pytest.raises(ValidationError):
        build()


@pytest.mark.parametrize(
    "build",
    [
        lambda: AeTarget("", 1, 1, None, "ALIVE"),
        lambda: AeTarget("n", -1, 1, None, "ALIVE"),
        lambda: AeTarget("n", 1, 1, None, "LOST"),
        lambda: AeTarget("n", 1, 1, None, "ALIVE", float("inf")),
        lambda: AeTarget("n", 1, 1, None, "ALIVE", stuck=1),
        lambda: AeCounters(delivered=-1),
        lambda: _status(node_id=""),
        lambda: _status(running="yes"),
        lambda: _status(started_at_ms=-1),
        lambda: _status(items=({"target": "n"},)),
        lambda: ObligationView("n", 1, "ns", b"k", 1, 0, "node-1"),
        lambda: ObligationView("n", 1, b"ns", b"k", 1, 0, ""),
        lambda: ObligationsPage("node-1", 1, None, ({},), 0),
    ],
)
def test_responses_reject_invalid_fields(build):
    with pytest.raises(ValidationError):
        build()


def test_responses_reject_unknown_or_malformed_documents():
    document = _status().to_document()
    with pytest.raises(ValidationError, match="unknown fields: extra"):
        AeStatus.from_document({**document, "extra": 1})
    with pytest.raises(ValidationError, match="items must be a list"):
        AeStatus.from_document({**document, "items": None})
    with pytest.raises(ValidationError, match="counters must be a table"):
        AeStatus.from_document({**document, "counters": 3})
    page = _page().to_document()
    with pytest.raises(ValidationError, match="items must be a list"):
        ObligationsPage.from_document({**page, "items": "x"})
