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
"""Namespace — unified codec for NamespaceLog and NamespaceTagKey storage keys."""

import hashlib
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import Any, Protocol

from tourillon.core.structure.buffer import BytesWalker
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.record import Address, Record, Tombstone, Version

_PID_SIZE: int = 4


class Namespace(StrEnum):
    LOG = "log"
    TAGS = "tags"


class Prefix(Protocol):
    def to_bytes(self) -> bytes:
        """
        Encode this prefix to the binary form.
        """


@dataclass(frozen=True, order=True)
class Pid:
    value: int

    def to_bytes(self) -> bytes:
        return self.value.to_bytes(_PID_SIZE, "big")

    @classmethod
    def from_bytes(cls, walker: BytesWalker) -> "Pid":
        return Pid(walker.read_int(_PID_SIZE, "pid"))


@dataclass(frozen=True)
class KeyPartition:
    pid: Pid
    addr: Address

    def to_bytes(self) -> bytes:
        """Encode this key to its binary Namespace.LOG representation."""
        return self.pid.to_bytes() + self.addr.to_bytes()


@dataclass(frozen=True, order=True)
class Key:
    """Decoded storage key carrying partition id, address, and HLC timestamp.

    The same three fields are laid out differently depending on the namespace:

    * **Namespace.LOG Key** (ordered by key): ``pid (4B BE) | addr | hlc``
    * **Namespace.TAGS Key** (ordered by time): ``pid (4B BE) | hlc | addr``

    where ``addr`` is ``StoreKey.to_bytes()`` and ``hlc`` is
    ``HLCTimestamp.to_bytes()``.  Use the ``from_log_bytes`` /
    ``to_log_bytes`` and ``from_tag_bytes`` / ``to_tag_bytes`` pairs
    accordingly.
    """

    pid: Pid
    ts: HLCTimestamp
    addr: Address

    @classmethod
    def from_bytes(cls, data: bytes, ns: Namespace) -> "Key":
        """Parse a raw Namespace key and return a ``Key``.

        Layout:
            log: ``pid (4B BE) | len_ks (2B BE) | keyspace | len_key (2B BE) | key | hlc``.
            tags: ``pid (4B BE) | hlc | len_ks (2B BE) | keyspace | len_key (2B BE) | key``.
        Raise ``ValueError`` when the buffer is too short.
        Raise ``UnicodeDecodeError`` when node_id is not valid UTF-8.
        """
        w = BytesWalker(data)

        if ns == Namespace.TAGS:
            pid = Pid.from_bytes(w)
            ts = HLCTimestamp.from_bytes(w)
            addr = Address.from_bytes(w)
            return cls(pid=pid, ts=ts, addr=addr)
        else:
            pid = Pid.from_bytes(w)
            addr = Address.from_bytes(w)
            ts = HLCTimestamp.from_bytes(w)
            return cls(pid=pid, ts=ts, addr=addr)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Key":
        return cls(
            pid=Pid(data["pid"]),
            ts=HLCTimestamp.from_dict(data["ts"]),
            addr=Address.from_dict(data["addr"]),
        )

    def to_bytes(self, ns: Namespace) -> bytes:
        """Encode this key to its binary Namespace.TAGS representation."""
        if ns == Namespace.TAGS:
            return self.pid.to_bytes() + self.ts.to_bytes() + self.addr.to_bytes()
        return self.pid.to_bytes() + self.addr.to_bytes() + self.ts.to_bytes()

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid.value,
            "ts": self.ts.to_dict(),
            "addr": self.addr.to_dict()
        }

    def decrement_key(self, ns: Namespace) -> bytes:
        """Return the last Namespace key strictly before this one.

        Used as the *start* of the next reverse batch in paginated reads.
        """
        key = self.to_bytes(ns)

        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0x00:
                return key[:i] + bytes([key[i] - 1]) + b"\xff" * (len(key) - i - 1)
        return b""

    def increment_key(self, ns: Namespace) -> bytes:
        """Return the first Namespace key strictly after this one.

        Used as the *start* of the next forward batch in paginated reads.
        """
        key = self.to_bytes(ns)

        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0xFF:
                return key[:i] + bytes([key[i] + 1])
        return key + b"\x00"

    def __str__(self) -> str:
        return hashlib.sha256(self.to_bytes(Namespace.LOG)).hexdigest()


@dataclass(frozen=True)
class Value:
    """
    Binary-encoded Namespace.LOG value: quorum_write + kind flag + optional payload.

    Layout: ``quorum_write (1B BE) | payload``.
    When kind is ``0x01`` (Tombstone), payload is empty. When kind is ``0x00``
    (Version), payload carries the raw user bytes. Symmetric with ``to_bytes()``.
    """

    quorum_write: int
    payload: bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> "Value":
        if len(data) < 2:
            raise ValueError(f"value too short: {len(data)} < 2 bytes")

        qw = int.from_bytes(data[:1], "big")
        payload = data[1:]
        return cls(quorum_write=qw, payload=payload)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Value":
        return cls(
            quorum_write=data["quorum_write"],
            payload=data["payload"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "quorum_write": self.quorum_write,
            "payload": self.payload,
        }

    def to_bytes(self) -> bytes:
        return self.quorum_write.to_bytes(1, "big") + self.payload


class TagKind(Enum):
    """
    | Byte prefix | Name        | Full encoding                                | Sub-byte meaning                                                                   |
    |-------------|-------------|----------------------------------------------|------------------------------------------------------------------------------------|
    | `b"\x00"`   | `LIVE`      | `b"\x00"` (1 byte)                           | —                                                                                  |
    | `b"\x01"`   | `TOMBSTONE` | `b"\x01"` (1 byte)                           | —                                                                                  |
    | `b"\x02"`   | `STAGING`   | `b"\x02" + sub + epoch (4B BE)` (6 bytes)    | `\x00` live, `\x01` tombstone                                                      |
    | `b"\x03"`   | `HINT`      | `b"\x03" + sub + node_id (bytes)` (variable) | `\x00` live, `\x01` tombstone                                                      |
    | `b"\x04"`   | `STALE`     | `b"\x04" + sub` (2 bytes)                    | `\x00` was live, `\x01` was tombstone                                              |
    | `b"\xff"`   | `PHANTOM`   | `b"\xff" + sub` (N bytes)
    """
    LIVE = b"\x00"
    TOMBSTONE = b"\x01"
    STAGING = b"\x02"
    HINT = b"\x03"
    STALE = b"\x04"
    PHANTOM = b"\xff"


@dataclass(frozen=True)
class TagPayload:
    sub: TagKind | None = None
    payload: bytes = b""

    def to_bytes(self) -> bytes:
        sub = self.sub.value if self.sub is not None else b""
        payload = self.payload if self.payload is not None else b""
        return sub + payload

    @classmethod
    def from_bytes(cls, data: bytes) -> "TagPayload":
        if data:
            sub = data[0]
            payload = data[1:]
            return cls(sub=TagKind(sub), payload=payload)
        return TagPayload()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TagPayload":
        return cls(
            sub=TagKind[data["sub"]] if data.get("sub") is not None else None,
            payload=data["payload"] if data.get("payload") is not None else b"",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sub": self.sub,
            "payload": self.payload,
        }

@dataclass(frozen=True)
class Tag:
    kind: TagKind
    payload: TagPayload = field(default_factory=TagPayload)

    def to_bytes(self) -> bytes:
        """Encode this tag to its binary representation.

        Layout: ``kind (1B) | payload``. When payload is absent the result is
        a single-byte kind discriminant. Symmetric with ``from_bytes``.
        """
        return self.kind.value + self.payload.to_bytes()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Tag":
        w = BytesWalker(data)
        b_kind = w.read(1, "kind")
        payload = data[1:]
        return cls(kind=TagKind(b_kind), payload=TagPayload.from_bytes(payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "payload": self.payload.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Tag":
        return cls(
            kind=TagKind[data["kind"]],
            payload=TagPayload.from_dict(data["payload"]),
        )


@dataclass(frozen=True)
class TaggedRecord:
    key: Key
    value: Value
    tag: Tag

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaggedRecord":
        return cls(
            key=Key.from_dict(data["key"]),
            value=Value.from_dict(data["value"]),
            tag=Tag.from_dict(data["tag"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key.to_dict(),
            "value": self.value.to_dict(),
            "tag": self.tag.to_dict()
        }

    def to_bytes(self, ns: Namespace) -> bytes:
        return self.key.to_bytes(ns) + self.value.to_bytes() + self.tag.to_bytes()

    def record_kind(self) -> TagKind | None:
        """Return True if this tag represents an available record version."""
        if self.tag.kind in (TagKind.LIVE, TagKind.TOMBSTONE):
            return self.tag.kind
        if self.tag.kind == TagKind.HINT:
            if not self.tag.payload:
                raise ValueError("HINT tag must have payload")
            if self.tag.payload.sub in (TagKind.LIVE, TagKind.TOMBSTONE):
                return self.tag.payload.sub
        return None

    def to_record(self) -> Record | None:
        kind = self.record_kind()

        if kind is None:
            return None
        if kind == TagKind.LIVE:
            return Version(
                address=self.key.addr,
                metadata=self.key.ts,
                value=self.value.payload,
                quorum_write=self.value.quorum_write,
            )

        return Tombstone(
            address=self.key.addr,
            metadata=self.key.ts,
            quorum_write=self.value.quorum_write,
        )
