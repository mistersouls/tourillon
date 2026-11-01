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

import base64
from dataclasses import dataclass
from enum import Enum
from typing import Any

from tourillon.core.models.storageschema import Key, Keyspace, KeyTag, Record


class RepairIssue(Enum):
    """What a repair actually did, which is never implied by success alone."""

    REPAIRED = "repaired"
    ALREADY_DURABLE = "already_durable"


@dataclass(frozen=True)
class VersionLookup:
    """One local snapshot: exact source and bounded retained version identities."""

    record: Record | None
    retained: tuple[KeyTag, ...] = ()
    outside_retention: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "record": None if self.record is None else self.record.to_dict(),
            "retained": [r.to_dict() for r in self.retained],
            "outside_retention": self.outside_retention,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VersionLookup":
        if not isinstance(data, dict) or set(data) != {
            "record",
            "retained",
            "outside_retention",
        }:
            raise ValueError("invalid version lookup payload")
        if not isinstance(data["retained"], list) or len(data["retained"]) > 64:
            raise ValueError("retained lookup identities must be bounded")
        if not isinstance(data["outside_retention"], bool):
            raise ValueError("outside_retention must be boolean")

        return cls(
            None if data["record"] is None else Record.from_dict(data["record"]),
            tuple(KeyTag.from_dict(r) for r in data["retained"]),
            data["outside_retention"],
        )


@dataclass(frozen=True)
class RepairResult:
    """Outcome of a repair: what happened, and how many replicas now hold it."""

    issue: RepairIssue
    replicas: int
    token: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue": self.issue.value,
            "replicas": self.replicas,
            "token": self.token,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RepairResult":
        return cls(
            issue=RepairIssue(data["issue"]),
            replicas=int(data["replicas"]),
            token=str(data["token"]),
        )


def encode_token(key: Key) -> str:
    """Render a version key as the opaque handle a client hands back.

    The handle is the physical `LOGS` key, base64url without padding. Keeping
    it opaque is deliberate: clients and the operator CLI never need the
    storage schema, so the layout stays free to change without a wire break.
    """
    return (
        base64.urlsafe_b64encode(key.to_bytes(Keyspace.LOGS))
        .decode("ascii")
        .rstrip("=")
    )


def decode_token(token: str) -> Key:
    """Rebuild the version key a handle designates.

    Raises `ValueError` on anything that is not a well-formed handle. The
    partition id it carries is a checksum only: the caller recomputes it from
    the address and never trusts the client's copy.
    """
    if not isinstance(token, str) or not token:
        raise ValueError("repair token must be a non-empty string")
    padding = "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(token + padding)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"repair token is not valid base64url: {exc}") from exc
    try:
        return Key.from_bytes(raw, Keyspace.LOGS)
    except (ValueError, OverflowError, IndexError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"repair token does not decode to a version key: {exc}"
        ) from exc
