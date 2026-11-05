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
"""The ``node.health`` wire model and its verdict ordering."""

import pytest

from tourlib.adapter import MsgPackSerializer
from tourlib.exceptions import ValidationError
from tourlib.models.health import CRITICAL, OK, WARNING, HealthReason, NodeHealth

pytestmark = pytest.mark.tourlib


def test_no_reason_is_ok() -> None:
    assert NodeHealth.from_reasons("n", []) == NodeHealth("n", OK)


def test_warnings_only_give_a_warning() -> None:
    health = NodeHealth.from_reasons("n", [HealthReason("phase", "joining")])

    assert health.status == WARNING


def test_critical_reasons_come_first_and_win() -> None:
    warning = HealthReason("phase", "joining")
    critical = HealthReason("gc_stopped", "GC stopped", critical=True)

    health = NodeHealth.from_reasons("n", [warning, critical])

    assert health.status == CRITICAL
    assert health.reasons == (critical, warning)


def test_round_trips_through_the_serializer() -> None:
    health = NodeHealth.from_reasons(
        "n", [HealthReason("storage_full", "full", critical=True)]
    )
    serializer = MsgPackSerializer()

    decoded = serializer.decode(serializer.encode(health.to_document()))

    assert NodeHealth.from_document(decoded) == health


def test_reasons_default_to_none() -> None:
    assert NodeHealth.from_document({"node_id": "n", "status": "OK"}) == NodeHealth(
        "n", OK
    )


@pytest.mark.parametrize(
    ("document", "match"),
    [
        ("nope", "must be a table"),
        ({"node_id": "n", "status": "FINE"}, "health status must be among"),
        ({"node_id": "n", "status": "OK", "reasons": "x"}, "must be a list"),
        ({"node_id": "n", "status": "OK", "reasons": ["x"]}, "reason must be a table"),
    ],
)
def test_invalid_documents_are_rejected(document, match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        NodeHealth.from_document(document)
