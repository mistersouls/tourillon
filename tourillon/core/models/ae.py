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

from dataclasses import dataclass
from typing import Any

from tourillon.core.ae.view import VIEW_BYTES
from tourillon.core.models.storageschema import Key, Keyspace, Pid, Record
from tourillon.core.storage.cell import DigestCell, DIGEST_SIZE, BUCKET_SIZE
from tourlib.constant import MAX_ENVELOPE_PAYLOAD

MAX_PIDS_PER_REQUEST = 1 << 17
MAX_KEYS_PER_REQUEST = 1 << 14
MAX_PAGE_LIMIT = 1 << 14
MAX_BATCH_BYTES = MAX_ENVELOPE_PAYLOAD - 1024 * 1024

_CELL_BYTES: int = DIGEST_SIZE + BUCKET_SIZE


@dataclass(frozen=True, slots=True)
class Scope:
    pids: tuple[int, ...]
    view: bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "pids": list(self.pids),
            "view": self.view,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Scope":
        pids = _list(data, "pids", MAX_PIDS_PER_REQUEST)
        if not pids or any(
            isinstance(p, bool) or not isinstance(p, int) or p < 0 for p in pids
        ):
            raise ValueError("pids must be a non-empty list of integers >= 0")

        return cls(pids=tuple(pids), view=_bytes(data["view"], "view", VIEW_BYTES))

    @property
    def pid(self) -> int:
        if len(self.pids) != 1:
            raise ValueError("Scope must address exactly one partition to access pid")
        return self.pids[0]


@dataclass(frozen=True, slots=True)
class RootsRequest:
    scope: Scope
    cells: tuple[bytes, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"scope": self.scope.to_dict(), "cells": list(self.cells)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootsRequest":
        scope = Scope.from_dict(data["scope"])
        cells = _list(data, "cells", MAX_PIDS_PER_REQUEST)

        if len(cells) != len(scope.pids):
            raise ValueError("cells length must match pids one to one")

        return cls(scope, tuple(_bytes(c, "cell", _CELL_BYTES) for c in cells))


@dataclass(frozen=True, slots=True)
class RootsResponse:
    diffs: tuple[tuple[int, bytes], ...]

    def to_dict(self) -> dict[str, Any]:
        return {"diffs": [[pid, cell] for pid, cell in self.diffs]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootsResponse":
        diffs = _list(data, "diffs", MAX_PIDS_PER_REQUEST)
        return cls(tuple(_pair(entry) for entry in diffs))


@dataclass(frozen=True, slots=True)
class BucketsRequest:
    scope: Scope
    after: int | None
    limit: int

    def to_dict(self) -> dict[str, Any]:
        return {"scope": self.scope.to_dict(), "after": self.after, "limit": self.limit}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BucketsRequest":
        scope = Scope.from_dict(data["scope"])
        _ = scope.pid

        return cls(scope=scope, after=_opt_int(data, "after"), limit=_limit(data))


@dataclass(frozen=True, slots=True)
class BucketsResponse:
    cells: tuple[tuple[int, bytes]]
    cursor: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cells": [[pid, cell] for pid, cell in self.cells],
            "cursor": self.cursor,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BucketsResponse":
        cells = _list(data, "cells", MAX_PAGE_LIMIT)
        return cls(
            cells=tuple(_pair(entry) for entry in cells),
            cursor=_opt_int(data, "cursor"),
        )

@dataclass(frozen=True, slots=True)
class RangeRequest:
    scope: Scope
    start: Key
    stop: Key

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "start": self.start.to_tag_bytes(),
            "stop": self.stop.to_tag_bytes(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RangeRequest":
        scope = Scope.from_dict(data["scope"])
        start = Key.from_tag_bytes(data["start"])
        stop = Key.from_tag_bytes(data["stop"])
        return cls(scope=scope, start=start, stop=stop)


@dataclass(frozen=True, slots=True)
class RangeResponse:
    cell: DigestCell

    def to_dict(self) -> dict[str, Any]:
        return {"cell": self.cell.to_bytes()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RangeResponse":
        cell = DigestCell.from_bytes(data["cell"])
        return cls(cell=cell)


@dataclass(frozen=True, slots=True)
class KeysRequest:
    scope: Scope
    start: Key
    stop: Key
    limit: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "start": self.start.to_tag_bytes(),
            "stop": self.stop.to_tag_bytes(),
            "limit": self.limit,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeysRequest":
        scope = Scope.from_dict(data["scope"])
        start, stop = _bounds(scope.pid, data)

        return cls(
            scope,
            Key.from_tag_bytes(start),
            Key.from_tag_bytes(stop),
            _limit(data),
        )


@dataclass(frozen=True, slots=True)
class KeysResponse:
    keys: tuple[Key, ...]
    cursor: Key | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "keys": [key.to_tag_bytes() for key in self.keys],
            "cursor": None if self.cursor is None else self.cursor.to_tag_bytes()
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeysResponse":
        keys = _list(data, "keys", MAX_PAGE_LIMIT)
        cursor = _opt_bytes(data, "cursor")
        return cls(
            keys=tuple(Key.from_bytes(_bytes(k, "key"), Keyspace.TAGS) for k in keys),
            cursor=None if cursor is None else Key.from_bytes(cursor, Keyspace.TAGS),
        )


@dataclass(frozen=True, slots=True)
class FetchRequest:
    scope: Scope
    keys: tuple[Key, ...]
    max_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "keys": [key.to_tag_bytes() for key in self.keys],
            "max_bytes": self.max_bytes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FetchRequest":
        scope = Scope.from_dict(data["scope"])
        raw_keys = _list(data, "keys", MAX_KEYS_PER_REQUEST)
        keys = tuple(Key.from_tag_bytes(_bytes(k, "key")) for k in raw_keys)
        pid = scope.pid

        if not keys or any(k.pid.value != pid for k in keys):
            raise ValueError("keys must be a non-empty list of keys within the partition")

        # Never trust the requester's budget beyond what one envelope holds.
        max_bytes = min(_int(data, "max_bytes", 1), MAX_BATCH_BYTES)

        return cls(scope=scope, keys=keys, max_bytes=max_bytes)


@dataclass(frozen=True, slots=True)
class FetchResponse:
    """
    Records read, and how many requested keys were consumed.

    Keys whose version is absent or not owned are consumed without a record.
    The requester resumes from ``consumed``; a response always consumes at
    least one key, so fetching always progresses.
    """

    records: tuple[Record, ...]
    consumed: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [rec.to_dict() for rec in self.records],
            "consumed": self.consumed,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FetchResponse":
        raw_records = _list(data, "records", MAX_KEYS_PER_REQUEST)
        records = tuple(Record.from_dict(r) for r in raw_records)
        return cls(records=records, consumed=_int(data, "consumed", 1))


def _list(data: dict[str, Any], name: str, limit: int) -> list[Any]:
    value = data[name]

    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")

    if len(value) > limit:
        raise ValueError(f"{name} holds more than {limit} entries")

    return value


def _limit(data: dict[str, Any]) -> int:
    # A page larger than one envelope holds is clamped, not refused: the
    # requester simply continues from the returned cursor.
    return min(_int(data, "limit", 1), MAX_PAGE_LIMIT)


def _bytes(value: Any, name: str, size: int | None = None) -> bytes:
    if not isinstance(value, bytes):
        raise ValueError(f"{name} must be bytes")

    if size is not None and len(value) != size:
        raise ValueError(f"{name} must be {size} bytes")

    return value


def _opt_bytes(data: dict[str, Any], name: str) -> bytes | None:
    value = data.get(name)
    return None if value is None else _bytes(value, name)


def _pair(entry: Any) -> tuple[int, bytes]:
    if not isinstance(entry, list) or len(entry) != 2:
        raise ValueError("entry must be a [number, cell] pair")

    number, cell = entry
    if isinstance(number, bool) or not isinstance(number, int) or number < 0:
        raise ValueError("entry number must be an integer >= 0")

    return number, _bytes(cell, "cell", _CELL_BYTES)


def _int(data: dict[str, Any], name: str, minimum: int = 0) -> int:
    value = data[name]

    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")

    return value


def _opt_int(data: dict[str, Any], name: str) -> int | None:
    return None if data.get(name) is None else _int(data, name)


def _bounds(pid: int, data: dict[str, Any]) -> tuple[bytes, bytes]:
    start = _bytes(data["start"], "start")
    stop = _bytes(data["stop"], "stop")
    prefix = Pid(pid).to_bytes()

    if not (start.startswith(prefix) and stop.startswith(prefix)) or start >= stop:
        raise ValueError("start and stop must bound a range within the partition")

    return start, stop
