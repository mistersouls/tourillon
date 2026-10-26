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

"""Wire models, rejects and configuration of the data anti-entropy protocol."""

from typing import Any

import pytest

from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.antientropy import (
    MAX_BATCH_BYTES,
    MAX_PAGE_LIMIT,
    BucketsRequest,
    BucketsResponse,
    FetchRequest,
    FetchResponse,
    KeysRequest,
    KeysResponse,
    PushRequest,
    RangeRequest,
    RootsRequest,
    RootsResponse,
    Scope,
    decode_record,
    encode_record,
    record_size,
)
from tourillon.core.models.config import AntiEntropyConfig, PlacementConfig
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourlib import ValidationError
from tourlib.constant import MAX_ENVELOPE_PAYLOAD
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon

VIEW = b"v" * 16
CELL = b"c" * 24
SCOPE = Scope((7,), VIEW)


def _record(pid: int = 7, data: bytes = b"value") -> Record:
    return Record(
        key=Key(
            pid=Pid(pid),
            address=Address(namespace=b"ns", key=b"k"),
            ts=HLCTimestamp(wall_ms=1, counter=0, node_id="n"),
        ),
        tag=Tag(kind=TagKind.LIVE),
        value=Value(data=data, quorum_write=1),
    )


def _raw(pid: int = 7) -> bytes:
    return _record(pid).key.to_bytes(Keyspace.TAGS)


def _scope(**overrides: Any) -> dict[str, Any]:
    return {**SCOPE.to_dict(), **overrides}


def test_requests_and_responses_round_trip() -> None:
    raw = _raw()
    stop = (7).to_bytes(4, "big") + b"\xff"

    messages = [
        RootsRequest(Scope((1, 2), VIEW), (CELL, CELL)),
        RootsResponse(((1, CELL),)),
        BucketsRequest(SCOPE, None, 5),
        BucketsRequest(SCOPE, 3, 5),
        BucketsResponse(((0, CELL),), 0),
        RangeRequest(SCOPE, raw, stop),
        KeysRequest(SCOPE, raw, stop, 10),
        KeysResponse((raw,), raw),
        KeysResponse((), None),
        FetchRequest(SCOPE, (raw,), 100),
        FetchResponse((_record(),), 1),
        PushRequest(SCOPE, (_record(),)),
    ]

    for message in messages:
        assert type(message).from_dict(message.to_dict()) == message


def test_page_sizes_and_byte_budgets_are_clamped_not_refused() -> None:
    raw = _raw()
    stop = (7).to_bytes(4, "big") + b"\xff"

    buckets = BucketsRequest.from_dict(
        {"scope": SCOPE.to_dict(), "after": None, "limit": MAX_PAGE_LIMIT * 4}
    )

    keys = KeysRequest.from_dict(
        {"scope": SCOPE.to_dict(), "start": raw, "stop": stop, "limit": 1 << 30}
    )

    fetch = FetchRequest.from_dict(
        {"scope": SCOPE.to_dict(), "keys": [raw], "max_bytes": 1 << 40}
    )

    assert buckets.limit == keys.limit == MAX_PAGE_LIMIT
    assert fetch.max_bytes == MAX_BATCH_BYTES < MAX_ENVELOPE_PAYLOAD


@pytest.mark.parametrize(
    "scope",
    [
        _scope(pids=[]),
        _scope(pids=[-1]),
        _scope(pids=[True]),
        _scope(pids="1"),
        _scope(view=b"short"),
        _scope(view="text"),
    ],
)
def test_scope_rejects_malformed_values(scope: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Scope.from_dict(scope)


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (RootsRequest, {"scope": SCOPE.to_dict(), "cells": []}),
        (RootsRequest, {"scope": SCOPE.to_dict(), "cells": [b"short"]}),
        (RootsResponse, {"diffs": [[1]]}),
        (RootsResponse, {"diffs": [[-1, CELL]]}),
        (BucketsRequest, {"scope": _scope(pids=[1, 2]), "after": None, "limit": 1}),
        (BucketsRequest, {"scope": SCOPE.to_dict(), "after": None, "limit": 0}),
        (BucketsRequest, {"scope": SCOPE.to_dict(), "after": "x", "limit": 1}),
        (BucketsResponse, {"cells": [[0, CELL]] * (MAX_PAGE_LIMIT + 1)}),
        (RangeRequest, {"scope": SCOPE.to_dict(), "start": _raw(8), "stop": _raw(8)}),
        (RangeRequest, {"scope": SCOPE.to_dict(), "start": _raw(), "stop": _raw()}),
        (KeysResponse, {"keys": ["text"], "cursor": None}),
        (FetchRequest, {"scope": SCOPE.to_dict(), "keys": [], "max_bytes": 1}),
        (FetchRequest, {"scope": SCOPE.to_dict(), "keys": [_raw(8)], "max_bytes": 1}),
        (FetchRequest, {"scope": SCOPE.to_dict(), "keys": [_raw()], "max_bytes": 0}),
        (FetchResponse, {"records": [], "consumed": 0}),
        (FetchResponse, {"records": [[b"a", b"b"]], "consumed": 1}),
        (PushRequest, {"scope": SCOPE.to_dict(), "records": []}),
        (
            PushRequest,
            {"scope": SCOPE.to_dict(), "records": [encode_record(_record(8))]},
        ),
    ],
)
def test_models_reject_malformed_payloads(model: Any, payload: dict[str, Any]) -> None:
    with pytest.raises((ValueError, KeyError)):
        model.from_dict(payload)


def test_record_codec_and_size_bound() -> None:
    record = _record(data=b"x" * 100)
    assert decode_record(encode_record(record)) == record
    assert record_size(record) >= sum(len(part) for part in encode_record(record))

    with pytest.raises(ValueError):
        decode_record("nope")


def test_reject_round_trips_and_tolerates_bad_retry_hints() -> None:
    reject = AntiEntropyRejectError(AntiEntropyRejectError.BUSY, "full", 1.5)
    copy = AntiEntropyRejectError.from_dict(reject.to_dict())
    assert (copy.code, copy.reason, copy.retry_after) == ("busy", "full", 1.5)
    assert str(copy) == "full"

    assert (
        AntiEntropyRejectError.from_dict(
            {"code": "busy", "retry_after": "x"}
        ).retry_after
        == 0
    )

    assert AntiEntropyRejectError.from_dict({"retry_after": -3}).retry_after == 0
    assert AntiEntropyRejectError.from_dict([]).code == AntiEntropyRejectError.INVALID


# --- configuration --------------------------------------------------------------


def test_anti_entropy_config_round_trips_through_placement() -> None:
    config = AntiEntropyConfig(
        enabled=False,
        sweep_period=60.0,
        min_interval=0.5,
        jitter=0.0,
        grace=30.0,
        settle_delay=1.0,
        concurrency=2,
        pids_per_cycle=1 << 17,
        keys_per_request=1 << 20,
        max_batch_bytes=1 << 30,
        max_repairs_per_cycle=5,
        max_keys_per_cycle=7,
    )

    placement = PlacementConfig(anti_entropy=config)
    assert PlacementConfig.from_document(placement.to_document()) == placement
    assert AntiEntropyConfig.from_document({}) == AntiEntropyConfig()


@pytest.mark.parametrize(
    "document",
    [
        {"enabled": "yes"},
        {"sweep_period": 0.5},
        {"sweep_period": "fast"},
        {"jitter": 1.5},
        {"grace": -1},
        {"concurrency": 0},
        {"keys_per_request": True},
        {"max_keys_per_cycle": 0},
    ],
)
def test_anti_entropy_config_rejects_invalid_values(document: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        AntiEntropyConfig.from_document(document)


def test_placement_requires_an_anti_entropy_table() -> None:
    with pytest.raises(ValidationError):
        PlacementConfig.from_document({"anti_entropy": 3})
