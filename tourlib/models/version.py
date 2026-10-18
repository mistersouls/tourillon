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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tourlib.constant import MAX_KEY_LEN, MAX_NAMESPACE_LEN
from tourlib.models.ts import HLCTimestamp
from tourlib.utils.buffer import BytesWalker


@dataclass(frozen=True, slots=True, eq=True)
class Address:
    key: bytes
    namespace: bytes

    def digest(self) -> str:
        return hashlib.md5(self.to_bytes()).hexdigest()

    def to_dict(self) -> dict[str, bytes]:
        return {"key": self.key, "namespace": self.namespace}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Address":
        return cls(key=data["key"], namespace=data["namespace"])

    def to_bytes(self) -> bytes:
        ns_len = len(self.namespace)
        key_len = len(self.key)

        if ns_len > MAX_NAMESPACE_LEN:
            raise OverflowError(
                f"Namespace length {ns_len} exceeds {MAX_NAMESPACE_LEN} bytes"
            )
        if key_len > MAX_KEY_LEN:
            raise OverflowError(f"Key length {key_len} exceeds {MAX_KEY_LEN} bytes")

        return (
            ns_len.to_bytes(1, "big")
            + self.namespace
            + key_len.to_bytes(1, "big")
            + self.key
        )

    @classmethod
    def from_bytes(cls, b: bytes) -> "Address":
        if len(b) < 2:
            raise ValueError("Input bytes too short to contain Address")

        ns_len = b[0]
        if len(b) < 1 + ns_len + 1:
            raise ValueError("Input bytes too short for namespace length")

        namespace = b[1 : 1 + ns_len]
        key_len = b[1 + ns_len]

        if len(b) < 1 + ns_len + 1 + key_len:
            raise ValueError("Input bytes too short for key length")

        key = b[1 + ns_len + 1 : 1 + ns_len + 1 + key_len]
        return cls(key=key, namespace=namespace)

    @classmethod
    def walk(cls, walker: BytesWalker) -> "Address":
        namespace = walker.read_field("namespace", 1)
        key = walker.read_field("key", 1)
        return cls(key=key, namespace=namespace)


@dataclass(frozen=True, slots=True, eq=True)
class Meta:
    ts: HLCTimestamp
    quorum_write: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts.to_dict(),
            "quorum_write": self.quorum_write,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Meta":
        return cls(
            ts=HLCTimestamp.from_dict(data["ts"]),
            quorum_write=data["quorum_write"],
        )


@dataclass(frozen=True, slots=True, eq=True)
class Version:
    address: Address
    data: bytes
    meta: Meta

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address.to_dict(),
            "data": self.data,
            "meta": self.meta.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Version":
        return cls(
            address=Address.from_dict(data["address"]),
            data=data["data"],
            meta=Meta.from_dict(data["meta"]),
        )
