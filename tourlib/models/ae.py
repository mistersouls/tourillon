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

"""Wire models of the anti-entropy views of one node.

``ae.status`` returns an :class:`AeStatus`: the worker, its counters and one
page of the targets this node owes versions to, oldest debt first.
``ae.obligations`` returns an :class:`ObligationsPage`: one page of the
individual obligations, optionally for a single target.

Both scan the obligations persisted on the node up to ``SCAN_LIMIT``; past
it, counts are lower bounds and ``truncated`` is set.
"""

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from tourlib.exceptions import ValidationError

SCAN_LIMIT = 100_000
STATUS_DEFAULT_LIMIT = 20
OBLIGATIONS_DEFAULT_LIMIT = 50
MAX_LIMIT = 1000
STATUSES = ("ALIVE", "SUSPECT", "DEAD")


def _mapping(document: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(document, Mapping):
        raise ValidationError(f"{name} must be a table")
    return document


def _exact(document: Any, cls: type, name: str) -> dict[str, Any]:
    data = dict(_mapping(document, name))
    unknown = set(data) - set(cls.__dataclass_fields__)
    if unknown:
        raise ValidationError(f"unknown fields: {', '.join(sorted(unknown))}")
    return data


def _count(value: Any, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValidationError(f"{name} must be a non-negative integer")


def _limit(value: Any) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not (1 <= value <= MAX_LIMIT)
    ):
        raise ValidationError(f"limit must be an integer from 1 to {MAX_LIMIT}")


def _optional_str(value: Any, name: str) -> None:
    if value is not None and (not isinstance(value, str) or not value):
        raise ValidationError(f"{name} must be a non-empty string when provided")


def _seconds(value: Any, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValidationError(f"{name} must be a non-negative number")


@dataclass(frozen=True)
class AeStatusRequest:
    limit: int = STATUS_DEFAULT_LIMIT
    cursor: str | None = None
    target: str | None = None

    def __post_init__(self) -> None:
        _limit(self.limit)
        _optional_str(self.cursor, "cursor")
        _optional_str(self.target, "target")

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "AeStatusRequest":
        return cls(**_exact(document, cls, "ae.status request"))


@dataclass(frozen=True)
class AeTarget:
    """Versions this node owes one target, and why they are still pending.

    ``phase`` is None when the target is no longer a member. ``retry_in_s``
    is the remaining backoff; ``stuck`` means nothing explains the delay.
    """

    target: str
    versions: int
    oldest_wall_ms: int
    phase: str | None
    status: str
    retry_in_s: float = 0.0
    last_error: str | None = None
    stuck: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.target, str) or not self.target:
            raise ValidationError("target must be a non-empty string")
        _count(self.versions, "versions")
        _count(self.oldest_wall_ms, "oldest_wall_ms")
        _optional_str(self.phase, "phase")
        if self.status not in STATUSES:
            raise ValidationError(f"status must be among {', '.join(STATUSES)}")
        _seconds(self.retry_in_s, "retry_in_s")
        _optional_str(self.last_error, "last_error")
        if not isinstance(self.stuck, bool):
            raise ValidationError("stuck must be a boolean")


@dataclass(frozen=True)
class AeCounters:
    """Activity since the node started."""

    delivered: int = 0
    delivery_errors: int = 0
    sweeps: int = 0
    diverged: int = 0
    pulled: int = 0
    pushed: int = 0
    sweep_errors: int = 0

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            _count(getattr(self, name), name)


@dataclass(frozen=True)
class AeStatus:
    node_id: str
    phase: str
    running: bool
    started_at_ms: int | None
    snapshot_at_ms: int
    stuck_after_s: float
    counters: AeCounters
    items: tuple[AeTarget, ...]
    targets: int
    versions: int
    stuck: int
    cursor: str | None = None
    truncated: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValidationError("node_id must be a non-empty string")
        if not isinstance(self.running, bool) or not isinstance(self.truncated, bool):
            raise ValidationError("running and truncated must be booleans")
        if self.started_at_ms is not None:
            _count(self.started_at_ms, "started_at_ms")
        for name in ("snapshot_at_ms", "targets", "versions", "stuck"):
            _count(getattr(self, name), name)
        _seconds(self.stuck_after_s, "stuck_after_s")
        _optional_str(self.cursor, "cursor")
        if not isinstance(self.counters, AeCounters) or not all(
            isinstance(item, AeTarget) for item in self.items
        ):
            raise ValidationError("invalid ae.status counters or items")

    def to_document(self) -> dict[str, Any]:
        document = asdict(self)
        document["items"] = [asdict(item) for item in self.items]
        return document

    @classmethod
    def from_document(cls, document: Any) -> "AeStatus":
        data = _exact(document, cls, "ae.status response")
        data["counters"] = AeCounters(**_mapping(data.get("counters"), "counters"))
        items = data.get("items")
        if not isinstance(items, list | tuple):
            raise ValidationError("items must be a list")
        data["items"] = tuple(AeTarget(**_mapping(item, "target")) for item in items)
        return cls(**data)


@dataclass(frozen=True)
class ObligationsRequest:
    target: str | None = None
    limit: int = OBLIGATIONS_DEFAULT_LIMIT
    cursor: str | None = None

    def __post_init__(self) -> None:
        _optional_str(self.target, "target")
        _limit(self.limit)
        _optional_str(self.cursor, "cursor")

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "ObligationsRequest":
        return cls(**_exact(document, cls, "ae.obligations request"))


@dataclass(frozen=True)
class ObligationView:
    """One version owed to ``target``; namespace and key are the stored bytes."""

    target: str
    partition: int
    namespace: bytes
    key: bytes
    wall_ms: int
    counter: int
    node_id: str

    def __post_init__(self) -> None:
        for name in ("target", "node_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValidationError(f"{name} must be a non-empty string")
        for name in ("partition", "wall_ms", "counter"):
            _count(getattr(self, name), name)
        if not isinstance(self.namespace, bytes) or not isinstance(self.key, bytes):
            raise ValidationError("namespace and key must be bytes")


@dataclass(frozen=True)
class ObligationsPage:
    node_id: str
    snapshot_at_ms: int
    target: str | None
    items: tuple[ObligationView, ...]
    total: int
    cursor: str | None = None
    truncated: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValidationError("node_id must be a non-empty string")
        _count(self.snapshot_at_ms, "snapshot_at_ms")
        _count(self.total, "total")
        _optional_str(self.target, "target")
        _optional_str(self.cursor, "cursor")
        if not isinstance(self.truncated, bool) or not all(
            isinstance(item, ObligationView) for item in self.items
        ):
            raise ValidationError("invalid ae.obligations items")

    def to_document(self) -> dict[str, Any]:
        document = asdict(self)
        document["items"] = [asdict(item) for item in self.items]
        return document

    @classmethod
    def from_document(cls, document: Any) -> "ObligationsPage":
        data = _exact(document, cls, "ae.obligations response")
        items = data.get("items")
        if not isinstance(items, list | tuple):
            raise ValidationError("items must be a list")
        data["items"] = tuple(
            ObligationView(**_mapping(item, "obligation")) for item in items
        )
        return cls(**data)
