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
from tourlib.models.inspect import NodeInspectRequest


@pytest.mark.tourlib
def test_node_inspect_request_from_document_parses_nominal() -> None:
    request = NodeInspectRequest.from_document(
        {
            "phases": ["ready", "joining"],
            "probe_statuses": ["live", "dead"],
            "node_id_prefix": "node-eu-",
            "only_anomalies": True,
            "limit": 200,
            "cursor": "node-0200",
        }
    )

    assert request.phases == ("READY", "JOINING")
    assert request.probe_statuses == ("LIVE", "DEAD")
    assert request.node_id_prefix == "node-eu-"
    assert request.only_anomalies is True
    assert request.limit == 200
    assert request.cursor == "node-0200"


@pytest.mark.tourlib
def test_node_inspect_request_from_document_rejects_invalid_limit() -> None:
    with pytest.raises(ValidationError, match="limit must be > 0"):
        NodeInspectRequest.from_document({"limit": 0})


@pytest.mark.tourlib
def test_node_inspect_request_from_document_rejects_invalid_probe_statuses() -> None:
    with pytest.raises(
        ValidationError, match="probe_statuses must be a list of non-empty strings"
    ):
        NodeInspectRequest.from_document({"probe_statuses": ["LIVE", ""]})
