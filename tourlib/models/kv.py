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

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Version


@dataclass(frozen=True)
class GetRequest:
    namespace: bytes
    key: bytes
    quorum_read: int = 1


@dataclass(frozen=True)
class PutRequest:
    namespace: bytes
    key: bytes
    value: bytes
    quorum_write: int = 1


@dataclass(frozen=True)
class DeleteRequest:
    namespace: bytes
    key: bytes
    quorum_write: int = 1


@dataclass(frozen=True)
class RepairRequest:
    """Resume a write that was persisted without reaching its quorum.

    The token is the opaque handle returned with the quorum failure. It
    designates an existing version, so a repair copies it rather than writing
    a new one, which is what makes retrying harmless.
    """

    token: str
    quorum_write: int = 1


@dataclass(frozen=True)
class WriteOutcome:
    """A put or delete that met its quorum.

    ``replicas`` is the number of replicas the key has right now, which may
    be below the configured replication factor while members are missing.
    """

    version: Version
    acknowledged: int
    required: int
    replicas: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version.to_dict(),
            "acknowledged": self.acknowledged,
            "required": self.required,
            "replicas": self.replicas,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WriteOutcome":
        return cls(
            version=Version.from_dict(data["version"]),
            acknowledged=int(data["acknowledged"]),
            required=int(data["required"]),
            replicas=int(data["replicas"]),
        )


@dataclass(frozen=True)
class ReadOutcome:
    """A get that met its quorum.

    ``version`` is None when the key is absent or deleted; ``tombstone``
    carries the timestamp of the delete that hides it, if any.
    """

    version: Version | None
    tombstone: HLCTimestamp | None
    answered: int
    required: int
    replicas: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": None if self.version is None else self.version.to_dict(),
            "tombstone": None if self.tombstone is None else self.tombstone.to_dict(),
            "answered": self.answered,
            "required": self.required,
            "replicas": self.replicas,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReadOutcome":
        version = data.get("version")
        tombstone = data.get("tombstone")
        return cls(
            version=None if version is None else Version.from_dict(version),
            tombstone=None if tombstone is None else HLCTimestamp.from_dict(tombstone),
            answered=int(data["answered"]),
            required=int(data["required"]),
            replicas=int(data["replicas"]),
        )
