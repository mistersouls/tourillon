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

from tourillon.core.models.repair import (
    RepairIssue,
    RepairResult,
    decode_token,
    encode_token,
)
from tourillon.core.models.storageschema import Key, Pid
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


def _key() -> Key:
    return Key(
        pid=Pid(7),
        address=Address(namespace=b"ns", key=b"key"),
        ts=HLCTimestamp(wall_ms=1700, counter=3, node_id="node-a"),
    )


@pytest.mark.tourillon
def test_token_roundtrips_a_version_key() -> None:
    key = _key()

    assert decode_token(encode_token(key)) == key


@pytest.mark.tourillon
def test_token_is_url_safe_and_unpadded() -> None:
    token = encode_token(_key())

    assert "=" not in token
    assert "+" not in token and "/" not in token


@pytest.mark.tourillon
@pytest.mark.parametrize(
    "token",
    [
        "",
        "not base64!!",
        "AAAA",
    ],
)
def test_decode_token_rejects_malformed_handles(token: str) -> None:
    with pytest.raises(ValueError):
        decode_token(token)


@pytest.mark.tourillon
def test_decode_token_rejects_a_truncated_handle() -> None:
    token = encode_token(_key())

    with pytest.raises(ValueError):
        decode_token(token[: len(token) // 2])


@pytest.mark.tourillon
def test_repair_result_roundtrips_through_its_document() -> None:
    result = RepairResult(issue=RepairIssue.ALREADY_DURABLE, replicas=3, token=encode_token(_key()))

    assert RepairResult.from_dict(result.to_dict()) == result
