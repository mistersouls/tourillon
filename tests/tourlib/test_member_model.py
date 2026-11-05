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

from tourlib.exceptions import ValidationError
from tourlib.models.member import (
    MemberDebt,
    MemberReport,
    MemberRequest,
    StandIn,
    UnansweredNeighbor,
)

pytestmark = pytest.mark.tourlib


def _report() -> MemberReport:
    return MemberReport(
        node_id="node-1",
        view_from="node-viewer",
        address="127.0.0.1:7001",
        phase="READY",
        epoch=3,
        status="DEAD",
        since_s=30,
        last_heard_s=30,
        ring_share_percent=12.5,
        partitions=32,
        total_partitions=256,
        vnodes=4,
        answered=1,
        neighbors=2,
        owed_by=(
            MemberDebt(
                holder="node-2",
                versions=4,
                oldest_wall_ms=1_800_000_000_000,
                phase="READY",
                status="ALIVE",
                retry_in_s=0,
                last_error=None,
                stuck=False,
            ),
        ),
        unanswered=(UnansweredNeighbor("node-3", "SUSPECT"),),
        stand_ins=(StandIn("node-4", 32),),
    )


def test_member_request_round_trips_and_rejects_unknown_fields() -> None:
    request = MemberRequest("node-1")

    assert MemberRequest.from_document(request.to_document()) == request
    with pytest.raises(ValidationError, match="only node_id"):
        MemberRequest.from_document({"node_id": "node-1", "extra": True})


def test_member_report_round_trips_nested_rows() -> None:
    report = _report()

    assert MemberReport.from_document(report.to_document()) == report


@pytest.mark.parametrize(
    "change,message",
    [
        (lambda data: data.update({"surprise": True}), "missing or unknown"),
        (lambda data: data.update({"answered": 3}), "answered and neighbors"),
        (lambda data: data.update({"partitions": 257}), "cannot exceed"),
    ],
)
def test_member_report_rejects_invalid_documents(change, message: str) -> None:
    document = _report().to_document()
    change(document)

    with pytest.raises(ValidationError, match=message):
        MemberReport.from_document(document)
