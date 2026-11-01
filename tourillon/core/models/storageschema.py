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

import hashlib
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import Any

from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address
from tourlib.utils.buffer import BytesWalker
from tourlib.validation import validate_node_id_size

PID_SIZE = 4
MAX_QUORUM_WRITE = 255
MAX_VALUE_DATA_LEN = 16 * 1024 * 1024


class Keyspace(StrEnum):
    LOGS = "logs"
    TAGS = "tags"
    META = "meta"


class MetaKind(Enum):
    OBLIGATION = b"\x01"
    DIGEST = b"\x02"


@dataclass(frozen=True, slots=True, eq=True)
class ObligationPrefix:
    """
    META prefix grouping replication obligations.

    Without a target the prefix covers every obligation; with a target it
    covers the obligations owed to that single node.
    """

    target: str | None = None

    def __post_init__(self) -> None:
        if self.target is not None:
            _validate_target(self.target)

    def to_bytes(self) -> bytes:
        if self.target is None:
            return MetaKind.OBLIGATION.value
        return MetaKind.OBLIGATION.value + _length_prefixed(self.target.encode("utf-8"))


def _length_prefixed(value: bytes) -> bytes:
    if len(value) > 0xFFFF:
        raise ValueError("metadata field exceeds 65535 bytes")
    return len(value).to_bytes(2, "big") + value


def _validate_target(target: str) -> None:
    if not isinstance(target, str) or not target:
        raise ValueError("obligation target must be a non-empty string")
    validate_node_id_size(target)


@dataclass(frozen=True, slots=True, eq=True, order=True)
class Pid:
    value: int

    def to_bytes(self) -> bytes:
        """Return a 4-byte big-endian representation of the pid."""
        return self.value.to_bytes(PID_SIZE, "big")

    @classmethod
    def from_bytes(cls, b: bytes) -> "Pid":
        """Create a Pid instance from a 4-byte big-endian representation."""
        if len(b) != PID_SIZE:
            raise ValueError(f"Expected { PID_SIZE } bytes, got {len(b)} bytes")
        return cls(int.from_bytes(b, "big"))

    @classmethod
    def walk(cls, walker: BytesWalker) -> "Pid":
        """Create a Pid instance by reading 4 bytes from a BytesWalker."""
        value = walker.read_int(PID_SIZE, "pid")
        return cls(value)


@dataclass(frozen=True, slots=True, eq=True)
class PartitionAddress:
    pid: Pid
    address: Address

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the PartitionAddress."""
        return {"pid": self.pid.value, "address": self.address.to_dict()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PartitionAddress":
        """Create a PartitionAddress instance from a dictionary representation."""
        return cls(pid=Pid(data["pid"]), address=Address.from_dict(data["address"]))

    def to_bytes(self) -> bytes:
        """Return a bytes representation of the PartitionAddress."""
        return self.pid.to_bytes() + self.address.to_bytes()

    @classmethod
    def from_bytes(cls, b: bytes) -> "PartitionAddress":
        """Create a PartitionAddress instance from a bytes representation."""
        walker = BytesWalker(b)
        pid = Pid.walk(walker)
        address = Address.walk(walker)
        return cls(pid=pid, address=address)


@dataclass(frozen=True, slots=True, eq=True)
class MetaKey:
    pid: Pid
    address: Address

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the Key."""
        return {"pid": self.pid.value, "address": self.address.to_dict()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MetaKey":
        """Create a MetaKey instance from a dictionary representation."""
        return cls(pid=Pid(data["pid"]), address=Address.from_dict(data["address"]))

    def to_bytes(self) -> bytes:
        return self.pid.to_bytes() + self.address.to_bytes()

    @classmethod
    def from_bytes(cls, b: bytes) -> "MetaKey":
        walker = BytesWalker(b)
        return cls.walk(walker)

    @classmethod
    def walk(cls, walker: BytesWalker) -> "MetaKey":
        """Create a MetaKey instance from a bytes representation."""
        pid = Pid.walk(walker)
        address = Address.walk(walker)
        return cls(pid=pid, address=address)


@dataclass(frozen=True, slots=True, eq=True)
class Key:
    pid: Pid
    address: Address
    ts: HLCTimestamp

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the Key."""
        return {
            "pid": self.pid.value,
            "address": self.address.to_dict(),
            "ts": self.ts.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Key":
        """Create a Key instance from a dictionary representation."""
        return cls(
            pid=Pid(data["pid"]),
            address=Address.from_dict(data["address"]),
            ts=HLCTimestamp.from_dict(data["ts"]),
        )

    def to_bytes(self, ks: Keyspace) -> bytes:
        """
        Return a bytes representation of the Key.
        The order of the fields in the bytes representation depends on the keyspace:
        - For Keyspace.TAGS: pid | ts | address
        - For Keyspace.LOGS: pid | address | ts
        """
        if ks == Keyspace.TAGS:
            return self.pid.to_bytes() + self.ts.to_bytes() + self.address.to_bytes()
        if ks == Keyspace.LOGS:
            return self.pid.to_bytes() + self.address.to_bytes() + self.ts.to_bytes()
        return MetaKey(pid=self.pid, address=self.address).to_bytes()

    @classmethod
    def from_bytes(cls, b: bytes, ks: Keyspace) -> "Key":
        walker = BytesWalker(b)
        return cls.walk(walker, ks)

    @classmethod
    def walk(cls, walker: BytesWalker, ks: Keyspace) -> "Key":
        """Create a Key instance from a bytes representation."""
        if ks == Keyspace.TAGS:
            pid = Pid.walk(walker)
            ts = HLCTimestamp.walk(walker)
            address = Address.walk(walker)
            return cls(pid=pid, ts=ts, address=address)
        pid = Pid.walk(walker)
        address = Address.walk(walker)
        ts = HLCTimestamp.walk(walker)
        return cls(pid=pid, address=address, ts=ts)

    def decrement_key(self, ks: Keyspace) -> bytes:
        """
        Return the lexicographically previous key in the specified keyspace.
        """
        key = self.to_bytes(ks)

        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0x00:
                return key[:i] + bytes([key[i] - 1]) + b"\xff" * (len(key) - i - 1)
        return b""

    def increment_key(self, ks: Keyspace) -> bytes:
        """
        Return the lexicographically next key in the specified keyspace.
        """
        key = self.to_bytes(ks)

        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0xFF:
                return key[:i] + bytes([key[i] + 1])
        return key + b"\x00"

    def __str__(self) -> str:
        return hashlib.sha256(self.to_bytes(Keyspace.LOGS)).hexdigest()


@dataclass(frozen=True, slots=True, eq=True)
class ObligationKey:
    """
    Immutable debt: `target` must process this address at least through `key.ts`.

    A newer stored version may cover the debt. Layout remains OBLIGATION |
    len(target) | target | TAGS(key); the timestamp identifies the immutable
    debt, not necessarily the record delivered.
    """

    target: str
    key: "Key"

    @property
    def required_ts(self) -> HLCTimestamp:
        return self.key.ts

    @property
    def address(self) -> PartitionAddress:
        return PartitionAddress(self.key.pid, self.key.address)

    def __post_init__(self) -> None:
        _validate_target(self.target)

    def to_bytes(self) -> bytes:
        return ObligationPrefix(self.target).to_bytes() + self.key.to_bytes(
            Keyspace.TAGS
        )

    @classmethod
    def from_bytes(cls, encoded: bytes) -> "ObligationKey":
        walker = BytesWalker(encoded)
        if walker.read(1, "metadata kind") != MetaKind.OBLIGATION.value:
            raise ValueError("metadata key is not a replication obligation")
        try:
            target = walker.read_field("obligation target").decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("obligation target is not valid UTF-8") from exc
        key = Key.walk(walker, Keyspace.TAGS)
        if walker.read_remaining():
            raise ValueError("obligation key contains trailing bytes")
        return cls(target=target, key=key)


@dataclass(frozen=True, slots=True, eq=True)
class Value:
    data: bytes
    quorum_write: int

    def __post_init__(self) -> None:
        if len(self.data) > MAX_VALUE_DATA_LEN:
            raise OverflowError(
                f"Value.data length {len(self.data)} exceeds {MAX_VALUE_DATA_LEN} bytes"
            )

    def to_bytes(self) -> bytes:
        """Return a bytes representation of the Value."""
        quorum_bytes = self.quorum_write.to_bytes(1, "big")
        return quorum_bytes + self.data

    @classmethod
    def from_bytes(cls, b: bytes) -> "Value":
        """Create a Value instance from a bytes representation."""
        if len(b) < 1:
            raise ValueError("Input bytes too short to contain quorum_write")
        quorum_write = b[0]
        data = b[1:]
        return cls(data=data, quorum_write=quorum_write)

    @classmethod
    def walk(cls, walker: BytesWalker) -> "Value":
        """Create a Value instance by reading from a BytesWalker."""
        quorum_write = walker.read_int(1, "quorum_write")
        data = walker.read_remaining("data")
        return cls(data=data, quorum_write=quorum_write)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the Value."""
        return {"data": self.data, "quorum_write": self.quorum_write}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Value":
        """Create a Value instance from a dictionary representation."""
        return cls(data=data["data"], quorum_write=data["quorum_write"])


class TagKind(Enum):
    LIVE = b"\x00"
    TOMBSTONE = b"\x01"
    STALE = b"\x04"
    PHANTOM = b"\x05"
    RESERVED = b"\x06"
    META = b"\x07"


@dataclass(frozen=True, slots=True, eq=True)
class TagPayload:
    sub: TagKind | None = None
    payload: bytes = b""

    def to_bytes(self) -> bytes:
        """Return a bytes representation of the TagPayload."""
        sub_byte = self.sub.value if self.sub else b""
        return sub_byte + self.payload

    @classmethod
    def from_bytes(cls, b: bytes) -> "TagPayload":
        """Create a TagPayload instance from a bytes representation."""
        if not b:
            return cls()
        sub = TagKind(b[0:1])
        payload = b[1:]
        return cls(sub=sub, payload=payload)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the TagPayload."""
        return {"sub": self.sub.name if self.sub else None, "payload": self.payload}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TagPayload":
        """Create a TagPayload instance from a dictionary representation."""
        sub = TagKind[data["sub"]] if data["sub"] else None
        payload = data["payload"]
        return cls(sub=sub, payload=payload)


@dataclass(frozen=True, slots=True, eq=True)
class Tag:
    kind: TagKind
    payload: TagPayload = field(default_factory=TagPayload)

    def to_bytes(self) -> bytes:
        """Return a bytes representation of the Tag."""
        return self.kind.value + self.payload.to_bytes()

    @classmethod
    def from_bytes(cls, b: bytes) -> "Tag":
        """Create a Tag instance from a bytes representation."""
        if not b:
            raise ValueError("Input bytes too short to contain Tag")
        kind = TagKind(b[0:1])
        payload = TagPayload.from_bytes(b[1:])
        return cls(kind=kind, payload=payload)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the Tag."""
        return {"kind": self.kind.name, "payload": self.payload.to_dict()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Tag":
        """Create a Tag instance from a dictionary representation."""
        kind = TagKind[data["kind"]]
        payload = TagPayload.from_dict(data["payload"])
        return cls(kind=kind, payload=payload)


@dataclass(frozen=True, slots=True, eq=True)
class KeyTag:
    key: Key
    tag: Tag

    def to_bytes(self, ks: Keyspace) -> bytes:
        """Return a bytes representation of the KeyTag."""
        return self.key.to_bytes(ks) + self.tag.to_bytes()

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the KeyTag."""
        return {"key": self.key.to_dict(), "tag": self.tag.to_dict()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeyTag":
        """Create a KeyTag instance from a dictionary representation."""
        key = Key.from_dict(data["key"])
        tag = Tag.from_dict(data["tag"])
        return cls(key=key, tag=tag)


@dataclass(frozen=True, slots=True, eq=True)
class KeyValue:
    key: Key
    value: Value

    def to_bytes(self, ks: Keyspace) -> bytes:
        """Return a bytes representation of the KeyValue."""
        return self.key.to_bytes(ks) + self.value.to_bytes()

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the KeyValue."""
        return {"key": self.key.to_dict(), "value": self.value.to_dict()}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeyValue":
        """Create a KeyValue instance from a dictionary representation."""
        key = Key.from_dict(data["key"])
        value = Value.from_dict(data["value"])
        return cls(key=key, value=value)


@dataclass(frozen=True, slots=True, eq=True)
class Record(KeyTag):
    value: Value

    def to_bytes(self, ks: Keyspace) -> bytes:
        """Return a bytes representation of the Record."""
        return self.key.to_bytes(ks) + self.value.to_bytes() + self.tag.to_bytes()

    @classmethod
    def from_bytes(cls, b: bytes, ks: Keyspace) -> "Record":
        """Create a Record instance from a bytes representation."""
        walker = BytesWalker(b)
        key = Key.walk(walker, ks)
        value = Value.walk(walker)
        tag = Tag.from_bytes(walker.read_remaining("tag"))
        return cls(key=key, value=value, tag=tag)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the Record."""
        return {
            "key": self.key.to_dict(),
            "value": self.value.to_dict(),
            "tag": self.tag.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Record":
        """Create a Record instance from a dictionary representation."""
        key = Key.from_dict(data["key"])
        value = Value.from_dict(data["value"])
        tag = Tag.from_dict(data["tag"])
        return cls(key=key, value=value, tag=tag)


@dataclass(frozen=True, slots=True, order=True, eq=True)
class KVRef:
    key: bytes
    keyspace: Keyspace


@dataclass(frozen=True, slots=True, order=True, eq=True)
class KVPair:
    key: bytes
    value: bytes
    keyspace: Keyspace
