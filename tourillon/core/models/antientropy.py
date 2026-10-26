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

"""Wire messages of the data anti-entropy protocol (``antientropy.*`` kinds).

Every request names the partitions it is about together with a *view*: a
16-byte hash of the replica set the sender computed for each of them. The
responder recomputes it from its own ring and refuses with ``stale_view``
when the two disagree, so both sides always reason about the same replicas
without having to agree on the whole ring.

Version keys travel as their raw TAGS bytes and records as
``[tags_key, tag, value]`` byte triples: compact, and decoded with the same
codecs the storage layer already uses.
"""

from dataclasses import dataclass
from typing import Any

from tourillon.core.models.storageschema import Key, Keyspace, Record, Tag, Value
from tourlib.constant import MAX_ENVELOPE_PAYLOAD

VIEW_BYTES = 16
_CELL_BYTES = 24

# Envelope budget. A payload never exceeds MAX_ENVELOPE_PAYLOAD (18 MiB). Senders
# paginate so they never exceed the bounds below; receivers enforce them as a
# safety net, and clamp page sizes instead of refusing them.
#
# - pid lists: 1 << 17 pids x ~31 bytes (pid + cell + framing) ~= 4 MiB;
# - key lists: a TAGS key is at most ~300 bytes (63-byte node id, 63-byte
#   namespace, 128-byte key, fixed fields), so 16384 keys ~= 4.7 MiB;
# - record batches: MAX_BATCH_BYTES of encoded records, but always at least
#   one record; a single maximal record (16 MiB value) is still below the
#   batch ceiling, so a batch never exceeds MAX_BATCH_BYTES.
MAX_PIDS_PER_REQUEST = 1 << 17
MAX_KEYS_PER_REQUEST = 1 << 14
MAX_PAGE_LIMIT = 1 << 14
MAX_BATCH_BYTES = MAX_ENVELOPE_PAYLOAD - 1024 * 1024
# Per-record msgpack framing on top of the three byte strings.
_RECORD_FRAMING = 16


def _int(data: dict[str, Any], name: str, minimum: int = 0) -> int:
    value = data[name]

    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")

    return value


def _opt_int(data: dict[str, Any], name: str) -> int | None:
    return None if data.get(name) is None else _int(data, name)


def _bytes(value: Any, name: str, size: int | None = None) -> bytes:
    if not isinstance(value, bytes):
        raise ValueError(f"{name} must be bytes")

    if size is not None and len(value) != size:
        raise ValueError(f"{name} must be {size} bytes")

    return value


def _opt_bytes(data: dict[str, Any], name: str) -> bytes | None:
    value = data.get(name)
    return None if value is None else _bytes(value, name)


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


@dataclass(frozen=True, slots=True)
class Scope:
    """Partitions a request is about, with the sender's replica view of them."""

    pids: tuple[int, ...]
    view: bytes

    def to_dict(self) -> dict[str, Any]:
        return {"pids": list(self.pids), "view": self.view}

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
        """The partition of a single-partition request."""
        if len(self.pids) != 1:
            raise ValueError("request must address exactly one partition")

        return self.pids[0]


@dataclass(frozen=True, slots=True)
class RootsRequest:
    """Compare per-partition roots: the sender's cell for every scoped pid."""

    scope: Scope
    cells: tuple[bytes, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"scope": self.scope.to_dict(), "cells": list(self.cells)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootsRequest":
        scope = Scope.from_dict(data["scope"])
        cells = _list(data, "cells", MAX_PIDS_PER_REQUEST)

        if len(cells) != len(scope.pids):
            raise ValueError("cells must match pids one to one")

        return cls(scope, tuple(_bytes(c, "cell", _CELL_BYTES) for c in cells))


@dataclass(frozen=True, slots=True)
class RootsResponse:
    """The responder's cell for every pid whose root differs."""

    diffs: tuple[tuple[int, bytes], ...]

    def to_dict(self) -> dict[str, Any]:
        return {"diffs": [[pid, cell] for pid, cell in self.diffs]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootsResponse":
        diffs = _list(data, "diffs", MAX_PIDS_PER_REQUEST)
        return cls(tuple(_pair(entry) for entry in diffs))


@dataclass(frozen=True, slots=True)
class BucketsRequest:
    """Page through the bucket cells of one partition after a bucket cursor."""

    scope: Scope
    after: int | None
    limit: int

    def to_dict(self) -> dict[str, Any]:
        return {"scope": self.scope.to_dict(), "after": self.after, "limit": self.limit}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BucketsRequest":
        scope = Scope.from_dict(data["scope"])
        _ = scope.pid

        return cls(scope, _opt_int(data, "after"), _limit(data))


@dataclass(frozen=True, slots=True)
class BucketsResponse:
    cells: tuple[tuple[int, bytes], ...]
    cursor: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cells": [[bucket, cell] for bucket, cell in self.cells],
            "cursor": self.cursor,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BucketsResponse":
        cells = _list(data, "cells", MAX_PAGE_LIMIT)
        return cls(tuple(_pair(entry) for entry in cells), _opt_int(data, "cursor"))


@dataclass(frozen=True, slots=True)
class RangeRequest:
    """Digest of the owned versions in the TAGS range ``(start, stop)``."""

    scope: Scope
    start: bytes
    stop: bytes

    def to_dict(self) -> dict[str, Any]:
        return {"scope": self.scope.to_dict(), "start": self.start, "stop": self.stop}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RangeRequest":
        scope = Scope.from_dict(data["scope"])
        start, stop = _bounds(scope.pid, data)

        return cls(scope, start, stop)


@dataclass(frozen=True, slots=True)
class KeysRequest:
    """List owned version keys in the TAGS range ``(start, stop)``."""

    scope: Scope
    start: bytes
    stop: bytes
    limit: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "start": self.start,
            "stop": self.stop,
            "limit": self.limit,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeysRequest":
        scope = Scope.from_dict(data["scope"])
        start, stop = _bounds(scope.pid, data)

        return cls(scope, start, stop, _limit(data))


@dataclass(frozen=True, slots=True)
class KeysResponse:
    """Owned keys found, and the last physical key read when the page is full."""

    keys: tuple[bytes, ...]
    cursor: bytes | None

    def to_dict(self) -> dict[str, Any]:
        return {"keys": list(self.keys), "cursor": self.cursor}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeysResponse":
        keys = _list(data, "keys", MAX_PAGE_LIMIT)
        return cls(tuple(_bytes(k, "key") for k in keys), _opt_bytes(data, "cursor"))


@dataclass(frozen=True, slots=True)
class FetchRequest:
    """Read exact versions, answering at most ``max_bytes`` of records."""

    scope: Scope
    keys: tuple[bytes, ...]
    max_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "keys": list(self.keys),
            "max_bytes": self.max_bytes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FetchRequest":
        scope = Scope.from_dict(data["scope"])
        keys = _list(data, "keys", MAX_KEYS_PER_REQUEST)
        pid = scope.pid.to_bytes(4, "big")

        if not keys or any(not _bytes(k, "key").startswith(pid) for k in keys):
            raise ValueError("keys must be a non-empty list within the partition")

        # Never trust the requester's budget beyond what one envelope holds.
        max_bytes = min(_int(data, "max_bytes", 1), MAX_BATCH_BYTES)

        return cls(scope, tuple(keys), max_bytes)


@dataclass(frozen=True, slots=True)
class FetchResponse:
    """Records read, and how many requested keys were consumed.

    Keys whose version is absent or not owned are consumed without a record.
    The requester resumes from ``consumed``; a response always consumes at
    least one key, so fetching always progresses.
    """

    records: tuple[Record, ...]
    consumed: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [encode_record(r) for r in self.records],
            "consumed": self.consumed,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FetchResponse":
        records = _list(data, "records", MAX_KEYS_PER_REQUEST)
        return cls(tuple(decode_record(r) for r in records), _int(data, "consumed", 1))


@dataclass(frozen=True, slots=True)
class PushRequest:
    """Owned versions the receiver is missing, all within one partition."""

    scope: Scope
    records: tuple[Record, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope.to_dict(),
            "records": [encode_record(r) for r in self.records],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PushRequest":
        scope = Scope.from_dict(data["scope"])
        records = _list(data, "records", MAX_KEYS_PER_REQUEST)
        decoded = tuple(decode_record(r) for r in records)

        if not decoded or any(r.key.pid.value != scope.pid for r in decoded):
            raise ValueError("records must be a non-empty list within the partition")

        return cls(scope, decoded)


def encode_record(record: Record) -> list[bytes]:
    return [
        record.key.to_bytes(Keyspace.TAGS),
        record.tag.to_bytes(),
        record.value.to_bytes(),
    ]


def decode_record(entry: Any) -> Record:
    if not isinstance(entry, list) or len(entry) != 3:
        raise ValueError("record must be a [key, tag, value] triple")

    key, tag, value = (_bytes(part, "record") for part in entry)

    return Record(
        key=Key.from_bytes(key, Keyspace.TAGS),
        tag=Tag.from_bytes(tag),
        value=Value.from_bytes(value),
    )


def record_size(record: Record) -> int:
    """Encoded size of a record on the wire (upper bound), used to cap batches."""
    return (
        len(record.key.to_bytes(Keyspace.TAGS))
        + len(record.tag.to_bytes())
        + len(record.value.data)
        + 1
        + _RECORD_FRAMING
    )


def _pair(entry: Any) -> tuple[int, bytes]:
    if not isinstance(entry, list) or len(entry) != 2:
        raise ValueError("entry must be a [number, cell] pair")

    number, cell = entry

    if isinstance(number, bool) or not isinstance(number, int) or number < 0:
        raise ValueError("entry number must be an integer >= 0")

    return number, _bytes(cell, "cell", _CELL_BYTES)


def _bounds(pid: int, data: dict[str, Any]) -> tuple[bytes, bytes]:
    start = _bytes(data["start"], "start")
    stop = _bytes(data["stop"], "stop")
    prefix = pid.to_bytes(4, "big")

    if not (start.startswith(prefix) and stop.startswith(prefix)) or start >= stop:
        raise ValueError("start and stop must bound a range within the partition")

    return start, stop
