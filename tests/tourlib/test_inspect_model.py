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

from datetime import datetime

import pytest

from tests.helpers.inspect import make_page, make_status, make_view
from tourlib.adapter import MsgPackSerializer
from tourlib.exceptions import ValidationError
from tourlib.models.inspect import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    MembersPage,
    MembersRequest,
    NodeStatus,
    PhaseCount,
    ProgressInfo,
    RingPositionInspect,
    StorageInfo,
    TlsInfo,
    WaitingOn,
    now_iso,
)

pytestmark = pytest.mark.tourlib


def test_members_request_defaults_list_anomalies() -> None:
    request = MembersRequest()

    assert request.limit == DEFAULT_LIMIT
    assert request.cursor is None
    assert request.anomalies_only is True


def test_members_request_upper_cases_and_dedupes_filters() -> None:
    request = MembersRequest.from_document(
        {
            "phases": ["ready", "JOINING", "Ready"],
            "statuses": ["dead", "DEAD", "suspect"],
            "prefix": "node-eu-",
            "limit": MAX_LIMIT,
            "cursor": "abc",
        }
    )

    assert request.phases == ("READY", "JOINING")
    assert request.statuses == ("DEAD", "SUSPECT")
    assert request.prefix == "node-eu-"
    assert request.limit == MAX_LIMIT
    assert request.cursor == "abc"


@pytest.mark.parametrize(
    "document",
    [
        {"list_all": True},
        {"phases": ["IDLE"]},
        {"statuses": ["ALIVE"]},
        {"prefix": "n"},
    ],
)
def test_members_request_any_filter_or_all_disables_anomalies_only(
    document: dict,
) -> None:
    assert MembersRequest.from_document(document).anomalies_only is False


def test_members_request_to_document_round_trips() -> None:
    request = MembersRequest(phases=("idle",), statuses=("dead",), limit=3)

    document = request.to_document()

    assert document == {
        "phases": ["IDLE"],
        "statuses": ["DEAD"],
        "prefix": None,
        "list_all": False,
        "limit": 3,
        "cursor": None,
    }
    assert MembersRequest.from_document(document) == request


@pytest.mark.parametrize(
    "document,match",
    [
        ({"limit": 0}, "limit must be an integer from 1 to 1000"),
        ({"limit": MAX_LIMIT + 1}, "limit must be"),
        ({"limit": True}, "limit must be"),
        ({"limit": "5"}, "limit must be"),
        ({"phases": ["SLEEPING"]}, "phases must be among READY"),
        ({"phases": [1]}, "phases must be among"),
        ({"phases": "READY"}, "phases must be a list"),
        ({"statuses": ["LIVE"]}, "statuses must be among ALIVE, SUSPECT, DEAD"),
        ({"prefix": ""}, "prefix must be a non-empty string"),
        ({"cursor": 7}, "cursor must be a non-empty string"),
        ({"list_all": "yes"}, "list_all must be a boolean"),
        ({"limit": 5, "only_anomalies": True}, "unknown fields: only_anomalies"),
        ([], "node.members request must be a table"),
    ],
)
def test_members_request_rejects_invalid_documents(
    document: object, match: str
) -> None:
    with pytest.raises(ValidationError, match=match):
        MembersRequest.from_document(document)


def test_members_request_accepts_none_filters() -> None:
    request = MembersRequest(phases=None, statuses=None)  # type: ignore[arg-type]

    assert request.phases == ()
    assert request.statuses == ()


def test_phase_count_total_and_member_view_anomalous() -> None:
    assert PhaseCount("READY", alive=3, suspect=2, dead=1).total == 6
    assert make_view("a").anomalous is False
    assert make_view("b", phase="JOINING").anomalous is True
    assert make_view("c", status="SUSPECT").anomalous is True


def test_members_page_round_trips_through_the_serializer() -> None:
    page = make_page(
        make_view("node-2", status="DEAD"),
        make_view("node-4", phase="JOINING"),
        summary=(PhaseCount("READY", alive=1, dead=1), PhaseCount("JOINING", 1)),
        total=5,
        cursor="xyz",
    )
    serializer = MsgPackSerializer()

    decoded = serializer.decode(serializer.encode(page.to_document()))

    assert MembersPage.from_document(decoded) == page


def test_members_page_rejects_non_table_documents() -> None:
    with pytest.raises(ValidationError, match="node.members response"):
        MembersPage.from_document(["not", "a", "table"])
    with pytest.raises(ValidationError, match="member must be a table"):
        MembersPage.from_document(
            {**make_page().to_document(), "items": ["not-a-member"]}
        )


def test_node_status_round_trips_through_the_serializer() -> None:
    status = make_status()
    serializer = MsgPackSerializer()

    decoded = serializer.decode(serializer.encode(status.to_document()))

    assert NodeStatus.from_document(decoded) == status


def test_node_status_round_trips_storage_tls_and_progress() -> None:
    status = make_status(
        storage=StorageInfo(map_size=100, used=25, versions=7, retention=2),
        tls=TlsInfo(
            expires_at="2027-01-01T00:00:00+00:00",
            rejected=2,
            last_rejection="bad cert",
            last_rejection_ago_s=3.5,
        ),
        progress=ProgressInfo(
            goal="drain",
            total=4,
            done=3,
            waiting=(WaitingOn("node-2", 1, "READY", "DEAD", since_s=9.0),),
            waiting_more=1,
        ),
    )
    serializer = MsgPackSerializer()

    decoded = serializer.decode(serializer.encode(status.to_document()))

    assert NodeStatus.from_document(decoded) == status
    assert status.storage.used_ratio == 0.25


def test_storage_ratio_of_an_empty_map_is_zero() -> None:
    assert StorageInfo(map_size=0, used=5, versions=0, retention=1).used_ratio == 0.0


def test_progress_rejects_a_waiting_entry_that_is_not_a_table() -> None:
    with pytest.raises(ValidationError, match="waiting must be a table"):
        ProgressInfo.from_document(
            {"goal": "join", "total": 1, "done": 0, "waiting": [1]}
        )


def test_node_status_rejects_invalid_documents() -> None:
    document = make_status().to_document()

    with pytest.raises(ValidationError, match="node.inspect response"):
        NodeStatus.from_document("nope")
    with pytest.raises(ValidationError, match="members must be a table"):
        NodeStatus.from_document({**document, "members": 3})
    with pytest.raises(KeyError):
        NodeStatus.from_document({**document, "ring_position": {}})


def test_ring_position_keeps_null_arcs() -> None:
    ring = RingPositionInspect.from_document(
        {
            "partition_shift": 1,
            "total_partitions": 2,
            "owned_partitions": 0,
            "owned_percent": 0.0,
            "vnode_ranges": [{"token": "0x1", "arc": None, "partitions": 0}],
        }
    )

    assert ring.vnode_ranges[0].arc is None


def test_now_iso_is_an_aware_timestamp() -> None:
    assert datetime.fromisoformat(now_iso()).tzinfo is not None
