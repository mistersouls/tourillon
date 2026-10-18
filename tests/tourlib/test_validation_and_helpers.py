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

from tourlib import ValidationError
from tourlib.adapter import MsgPackSerializer
from tourlib.helpers import parse_bind, scan
from tourlib.models.probe import ProbePong
from tourlib.validation import require_mapping, require_named_table, require_str


@pytest.mark.tourlib
def test_parse_bind_round_trip() -> None:
    assert parse_bind("localhost:8080") == ("localhost", 8080)


@pytest.mark.tourlib
@pytest.mark.parametrize(
    ("endpoint", "message"),
    [
        ("localhost", "invalid endpoint format"),
        (":8080", "invalid endpoint format"),
        ("localhost:port", "invalid endpoint format"),
    ],
)
def test_parse_bind_rejects_invalid_endpoints(endpoint: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_bind(endpoint)


@pytest.mark.tourlib
def test_validation_helpers_cover_success_and_failure_paths() -> None:
    document = {
        "name": "tourillon",
        "table": {"alpha": {"value": 1}},
    }

    assert require_str(document, "name") == "tourillon"
    assert require_mapping(document, "table") == {"alpha": {"value": 1}}
    assert require_named_table(document, "table", lambda item: item["value"]) == {
        "alpha": 1
    }

    with pytest.raises(ValidationError, match="name must be a non-empty string"):
        require_str({"name": ""}, "name")

    with pytest.raises(ValidationError, match="table must be a table"):
        require_mapping({}, "table")

    with pytest.raises(ValidationError, match="table keys must be non-empty strings"):
        require_named_table({"table": {"": {"value": 1}}}, "table", lambda item: item)

    with pytest.raises(ValidationError, match="table.alpha must be a table"):
        require_named_table({"table": {"alpha": 1}}, "table", lambda item: item)


@pytest.mark.tourlib
def test_msgpack_serializer_encodes_dataclass_documents() -> None:
    serializer = MsgPackSerializer()
    payload = serializer.encode(ProbePong(node_id="node-1", timestamp_ms=1, recv_ms=2))

    assert serializer.decode(payload) == {
        "node_id": "node-1",
        "timestamp_ms": 1,
        "recv_ms": 2,
    }


@pytest.mark.tourlib
def test_scan_decorator_with_sync_function() -> None:
    """Test scan decorator on synchronous functions."""
    scan_calls = []

    @scan("tourlib")
    def sync_func(value: int) -> int:
        scan_calls.append("called")
        return value * 2

    result = sync_func(5)
    assert result == 10
    assert len(scan_calls) == 1


@pytest.mark.tourlib
async def test_scan_decorator_with_async_function() -> None:
    """Test scan decorator on asynchronous functions."""
    scan_calls = []

    @scan("tourlib")
    async def async_func(value: int) -> int:
        scan_calls.append("called")
        return value * 3

    result = await async_func(5)
    assert result == 15
    assert len(scan_calls) == 1
