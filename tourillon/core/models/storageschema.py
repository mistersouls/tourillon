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
    HINT = b"\x01"
    DIGEST = b"\x02"


@dataclass(frozen=True, slots=True, eq=True)
class HintPrefix:
    intent: str | None = None

    def __post_init__(self) -> None:
        if self.intent is not None:
            _validate_intent(self.intent)

    def to_bytes(self) -> bytes:
        if self.intent is None:
            return MetaKind.HINT.value
        _validate_intent(self.intent)
        return MetaKind.HINT.value + _length_prefixed(self.intent.encode("utf-8"))

    @classmethod
    def from_bytes(cls, b: bytes) -> "HintPrefix":
        walker = BytesWalker(b)
        if walker.read(1, "metadata kind") != MetaKind.HINT.value:
            raise ValueError("metadata prefix is not a hint prefix")
        if b == MetaKind.HINT.value:
            return cls()
        intent = walker.read_field("hint intent").decode("utf-8")
        if walker.read_remaining():
            raise ValueError("hint prefix contains trailing bytes")
        return cls(intent=intent)


def _length_prefixed(value: bytes) -> bytes:
    if len(value) > 0xFFFF:
        raise ValueError("metadata field exceeds 65535 bytes")
    return len(value).to_bytes(2, "big") + value


def _validate_intent(intent: str) -> None:
    if not isinstance(intent, str) or not intent:
        raise ValueError("hint intent must be a non-empty string")
    validate_node_id_size(intent)


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
class HintKey:
    intent: str
    hlc: HLCTimestamp

    def __post_init__(self) -> None:
        _validate_intent(self.intent)

    def to_bytes(self) -> bytes:
        return HintPrefix(self.intent).to_bytes() + self.hlc.to_bytes()

    @classmethod
    def from_bytes(cls, encoded: bytes) -> "HintKey":
        walker = BytesWalker(encoded)
        kind = walker.read(1, "metadata kind")
        if kind != MetaKind.HINT.value:
            raise ValueError("metadata key is not a hint obligation")
        intent_bytes = walker.read_field("hint intent")
        try:
            intent = intent_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("hint intent is not valid UTF-8") from exc
        hlc = HLCTimestamp.walk(walker)
        if walker.read_remaining():
            raise ValueError("hint key contains trailing bytes")
        return cls(intent=intent, hlc=hlc)

    def prefix(self) -> bytes:
        return HintPrefix(self.intent).to_bytes()


@dataclass(frozen=True, slots=True, eq=True)
class HintReference:
    key: Key

    def to_bytes(self) -> bytes:
        return self.key.to_bytes(Keyspace.TAGS)

    @classmethod
    def from_bytes(cls, encoded: bytes) -> "HintReference":
        key = Key.from_bytes(encoded, Keyspace.TAGS)
        if key.to_bytes(Keyspace.TAGS) != encoded:
            raise ValueError("hint reference is not canonical")
        return cls(key=key)


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
    STAGING = b"\x02"
    HINT = b"\x03"
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

    def with_source_prefix(self, source: str) -> bytes:
        """
        Prefix the serialized payload with the source length and source bytes.
        Layout: len_src(2B) || src || original_payload_bytes

        The original payload is kept whole (sub-kind included) so that
        `Tag.unstaging` can restore the original tag exactly.
        """
        src = source.encode("utf-8")
        len_src = len(src).to_bytes(2, "big")
        return len_src + src + self.to_bytes()

    def extract_source_prefix(self) -> tuple[str, bytes]:
        """
        Extract the source prefix added by `with_source_prefix`.

        Returns a tuple of `(source, original_payload_bytes)`.
        """
        if len(self.payload) < 2:
            raise ValueError(
                "Source-prefixed payload too short to contain source length"
            )
        len_src = int.from_bytes(self.payload[0:2], "big")
        if len(self.payload) < 2 + len_src:
            raise ValueError("Source-prefixed payload too short to contain full source")
        src = self.payload[2 : 2 + len_src]
        return src.decode("utf-8"), self.payload[2 + len_src :]


@dataclass(frozen=True, slots=True, eq=True)
class Tag:
    kind: TagKind
    payload: TagPayload = field(default_factory=TagPayload)

    @classmethod
    def hint(cls, original: "Tag") -> "Tag":
        return cls(
            kind=TagKind.HINT,
            payload=TagPayload(sub=original.kind, payload=original.payload.to_bytes()),
        )

    @classmethod
    def unhint(cls, hinted: "Tag") -> "Tag":
        """Reverse `Tag.hint` and recover the original tag."""
        if hinted.kind != TagKind.HINT:
            raise ValueError("Cannot unhint a non-HINT tag")

        if hinted.payload.sub is None:
            raise ValueError("Invalid HINT tag: missing original kind in payload.sub")

        return cls(
            kind=hinted.payload.sub,
            payload=TagPayload.from_bytes(hinted.payload.payload),
        )

    @classmethod
    def staging(cls, original: "Tag", source: str) -> "Tag":
        payload = original.payload.with_source_prefix(source)
        return cls(
            kind=TagKind.STAGING,
            payload=TagPayload(sub=original.kind, payload=payload),
        )

    @classmethod
    def unstaging(cls, staged: "Tag", source: str | None = None) -> "Tag":
        """
        Reverse `Tag.staging` and recover the original Tag.

        If `source` is provided, validate that the staged source matches it.
        """
        if staged.kind != TagKind.STAGING:
            raise ValueError("Cannot unstaging a non-STAGING tag")

        if staged.payload.sub is None:
            raise ValueError(
                "Invalid STAGING tag: missing original kind in payload.sub"
            )

        staged_source, original_payload = staged.payload.extract_source_prefix()
        if source is not None and staged_source != source:
            raise ValueError(
                f"Staging source mismatch: expected '{source}', got '{staged_source}'"
            )

        return cls(
            kind=staged.payload.sub,
            payload=TagPayload.from_bytes(original_payload),
        )

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


@dataclass(frozen=True, slots=True)
class Hint:
    record: Record
    intent: str
    hlc: HLCTimestamp

    def __post_init__(self) -> None:
        _validate_intent(self.intent)
        original = Tag.unhint(self.record.tag)
        if original.kind not in {TagKind.LIVE, TagKind.TOMBSTONE}:
            raise ValueError("hint must wrap a LIVE or TOMBSTONE tag")


@dataclass
class MetaStaging:
    digest: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the MetaStaging."""
        return {"digest": self.digest}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MetaStaging":
        """Create a MetaStaging instance from a dictionary representation."""
        digest = data.get("digest")
        return cls(digest=digest)


@dataclass
class MetaValue:
    staging: dict[str, MetaStaging] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation of the MetaValue."""
        return {
            "staging": {source: meta.to_dict() for source, meta in self.staging.items()}
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MetaValue":
        """Create a MetaValue instance from a dictionary representation."""
        staging_data = data.get("staging", {})
        staging = {
            source: MetaStaging.from_dict(meta) for source, meta in staging_data.items()
        }
        return cls(staging=staging)


@dataclass(frozen=True, slots=True, order=True, eq=True)
class KVRef:
    key: bytes
    keyspace: Keyspace


@dataclass(frozen=True, slots=True, order=True, eq=True)
class KVPair:
    key: bytes
    value: bytes
    keyspace: Keyspace
