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
"""StoreKey, Version, Tombstone, KvMetadata — typed storage record model."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any

from tourillon.core.structure.clock import HLCTimestamp

type Record = Version | Tombstone


@dataclass(frozen=True)
class KvMetadata:
    """HLC timestamp plus write-quorum stored with every KV record.

    quorum_write is the W value in effect when this version was written.
    The coordinator uses it during reads to decide whether a version is
    confirmed: count(replicas returning V) >= V.quorum_write → confirmed.
    """

    hlc: HLCTimestamp
    quorum_write: int = 1

    def to_dict(self) -> dict[str, object]:
        """Return a wire-compatible dict."""
        return {"hlc": self.hlc.to_dict(), "quorum_write": self.quorum_write}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> KvMetadata:
        """Reconstruct a KvMetadata from its wire dict."""
        hlc_raw = data.get("hlc") or data  # allow flat wire format
        qw = int(data.get("quorum_write", 1))
        return cls(hlc=HLCTimestamp.from_dict(hlc_raw), quorum_write=qw)  # type: ignore[arg-type]


@dataclass(frozen=True)
class StoreKey:
    """Canonical addressing unit: logical keyspace + record key, both raw bytes.

    keyspace and key are encoding-agnostic raw bytes. Callers responsible for
    human-readable names encode to bytes before constructing a StoreKey,
    keeping the encoding decision at the boundary.
    """

    keyspace: bytes
    key: bytes

    def to_dict(self) -> dict[str, object]:
        """Return a wire-compatible dict with base64-free bytes fields."""
        return {"keyspace": self.keyspace, "key": self.key}

    def to_routing_bytes(self) -> bytes:
        """Return a length-prefixed byte encoding for use as a hash input.

        Layout: ``>H`` (2-byte big-endian keyspace length) + keyspace bytes +
        ``>H`` (2-byte big-endian key length) + key bytes.  Using explicit
        length prefixes instead of a bare separator prevents collisions between
        keyspaces with different lengths that share a common prefix.
        Raises OverflowError when keyspace or key exceeds 65535 bytes.
        """
        ks_len = len(self.keyspace)
        k_len = len(self.key)
        if ks_len > 0xFFFF or k_len > 0xFFFF:
            raise OverflowError("keyspace and key must each be at most 65535 bytes")
        return (
            struct.pack(">H", ks_len)
            + self.keyspace
            + struct.pack(">H", k_len)
            + self.key
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StoreKey:
        """Reconstruct a StoreKey from a wire dict."""
        ks = data["keyspace"]
        k = data["key"]
        return cls(
            keyspace=bytes(ks) if not isinstance(ks, bytes) else ks,
            key=bytes(k) if not isinstance(k, bytes) else k,
        )


@dataclass(frozen=True)
class Version:
    """Immutable snapshot of a key's value at a specific causal instant.

    metadata carries the HLC ordering handle. Value is the raw record bytes;
    callers determine encoding. Never compare records by value bytes for
    ordering; always use metadata. quorum_write stores the W used at write
    time so reads can determine whether this version is confirmed.
    """

    address: StoreKey
    metadata: HLCTimestamp
    value: bytes
    quorum_write: int = 1

    @property
    def meta(self) -> KvMetadata:
        """Return a KvMetadata view of this record's HLC and quorum_write."""
        return KvMetadata(hlc=self.metadata, quorum_write=self.quorum_write)

    def to_dict(self) -> dict[str, object]:
        """Return a kind-discriminated wire dict for msgpack serialisation."""
        return {
            "kind": "version",
            "address": self.address.to_dict(),
            "metadata": self.metadata.to_dict(),
            "value": self.value,
            "quorum_write": self.quorum_write,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Version:
        """Reconstruct a Version from its wire dict representation."""
        v = data["value"]
        return cls(
            address=StoreKey.from_dict(data["address"]),
            metadata=HLCTimestamp.from_dict(data["metadata"]),
            value=bytes(v) if not isinstance(v, bytes) else v,
            quorum_write=int(data.get("quorum_write", 1)),
        )


@dataclass(frozen=True)
class Tombstone:
    """Deletion marker that causally supersedes earlier Versions.

    A Tombstone has no value field. Its presence in DBI_KEYS signals that the
    key was deleted at the HLC instant encoded in metadata. kv.get returns a
    Tombstone when the last write to a key was a delete. quorum_write stores
    the W used at write time so reads can determine whether this tombstone is
    confirmed.
    """

    address: StoreKey
    metadata: HLCTimestamp
    quorum_write: int = 1

    @property
    def meta(self) -> KvMetadata:
        """Return a KvMetadata view of this record's HLC and quorum_write."""
        return KvMetadata(hlc=self.metadata, quorum_write=self.quorum_write)

    def to_dict(self) -> dict[str, object]:
        """Return a kind-discriminated wire dict for msgpack serialisation."""
        return {
            "kind": "tombstone",
            "address": self.address.to_dict(),
            "metadata": self.metadata.to_dict(),
            "quorum_write": self.quorum_write,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Tombstone:
        """Reconstruct a Tombstone from its wire dict representation."""
        return cls(
            address=StoreKey.from_dict(data["address"]),
            metadata=HLCTimestamp.from_dict(data["metadata"]),
            quorum_write=int(data.get("quorum_write", 1)),
        )


def record_from_dict(data: dict[str, Any]) -> Record:
    """Reconstruct a Version or Tombstone from its kind-discriminated dict.

    Raise ValueError when the kind field is absent or unrecognised.
    """
    kind = data.get("kind")
    if kind == "version":
        return Version.from_dict(data)
    if kind == "tombstone":
        return Tombstone.from_dict(data)
    raise ValueError(f"Unknown record kind: {kind!r}")
