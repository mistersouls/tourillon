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

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from tourlib.exceptions import ValidationError
from tourlib.validation import validate_node_id_size

_STATUSES = ("ALIVE", "SUSPECT", "DEAD")


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{name} must be a table")
    return value


def _node_id(value: Any, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{name} must be a non-empty string")
    try:
        validate_node_id_size(value)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc


def _count(value: Any, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValidationError(f"{name} must be a non-negative integer")


def _seconds(value: Any, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValidationError(f"{name} must be a non-negative number")


@dataclass(frozen=True)
class MemberRequest:
    node_id: str

    def __post_init__(self) -> None:
        _node_id(self.node_id, "node_id")

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "MemberRequest":
        data = _mapping(document, "node.member request")
        if set(data) != {"node_id"}:
            raise ValidationError("node.member request must contain only node_id")
        return cls(**data)


@dataclass(frozen=True)
class MemberDebt:
    holder: str
    versions: int
    oldest_wall_ms: int
    phase: str | None
    status: str
    retry_in_s: float
    last_error: str | None
    stuck: bool

    def __post_init__(self) -> None:
        _node_id(self.holder, "holder")
        _count(self.versions, "versions")
        _count(self.oldest_wall_ms, "oldest_wall_ms")
        if self.phase is not None and not isinstance(self.phase, str):
            raise ValidationError("phase must be a string or null")
        if self.status not in _STATUSES:
            raise ValidationError(f"status must be among {', '.join(_STATUSES)}")
        _seconds(self.retry_in_s, "retry_in_s")
        if self.last_error is not None and not isinstance(self.last_error, str):
            raise ValidationError("last_error must be a string or null")
        if not isinstance(self.stuck, bool):
            raise ValidationError("stuck must be a boolean")


@dataclass(frozen=True)
class UnansweredNeighbor:
    node_id: str
    status: str

    def __post_init__(self) -> None:
        _node_id(self.node_id, "node_id")
        if self.status not in _STATUSES:
            raise ValidationError(f"status must be among {', '.join(_STATUSES)}")


@dataclass(frozen=True)
class StandIn:
    node_id: str
    partitions: int

    def __post_init__(self) -> None:
        _node_id(self.node_id, "node_id")
        _count(self.partitions, "partitions")


@dataclass(frozen=True)
class MemberReport:
    node_id: str
    view_from: str
    address: str
    phase: str
    epoch: int
    status: str
    since_s: float | None
    last_heard_s: float | None
    ring_share_percent: float
    partitions: int
    total_partitions: int
    vnodes: int
    answered: int
    neighbors: int
    owed_by: tuple[MemberDebt, ...]
    unanswered: tuple[UnansweredNeighbor, ...]
    stand_ins: tuple[StandIn, ...]

    def __post_init__(self) -> None:
        self._validate_identity()
        self._validate_metrics()
        self._validate_rows()

    def _validate_identity(self) -> None:
        _node_id(self.node_id, "node_id")
        _node_id(self.view_from, "view_from")
        if not isinstance(self.address, str) or not self.address:
            raise ValidationError("address must be a non-empty string")
        if not isinstance(self.phase, str) or not self.phase:
            raise ValidationError("phase must be a non-empty string")
        _count(self.epoch, "epoch")
        if self.status not in _STATUSES:
            raise ValidationError(f"status must be among {', '.join(_STATUSES)}")

    def _validate_metrics(self) -> None:
        for name in ("since_s", "last_heard_s"):
            value = getattr(self, name)
            if value is not None:
                _seconds(value, name)
        if (
            isinstance(self.ring_share_percent, bool)
            or not isinstance(self.ring_share_percent, int | float)
            or not math.isfinite(self.ring_share_percent)
            or not 0 <= self.ring_share_percent <= 100
        ):
            raise ValidationError("ring_share_percent must be between 0 and 100")
        for name in (
            "partitions",
            "total_partitions",
            "vnodes",
            "answered",
            "neighbors",
        ):
            _count(getattr(self, name), name)
        if self.partitions > self.total_partitions:
            raise ValidationError("partitions cannot exceed total_partitions")
        if self.answered > self.neighbors or len(self.unanswered) != (
            self.neighbors - self.answered
        ):
            raise ValidationError("answered and neighbors are inconsistent")

    def _validate_rows(self) -> None:
        for name, kind in (
            ("owed_by", MemberDebt),
            ("unanswered", UnansweredNeighbor),
            ("stand_ins", StandIn),
        ):
            if not isinstance(getattr(self, name), tuple) or not all(
                isinstance(item, kind) for item in getattr(self, name)
            ):
                raise ValidationError(f"{name} contains invalid items")

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "MemberReport":
        data = dict(_mapping(document, "node.member response"))
        expected = set(cls.__dataclass_fields__)
        if set(data) != expected:
            raise ValidationError("node.member response has missing or unknown fields")
        for name, kind in (
            ("owed_by", MemberDebt),
            ("unanswered", UnansweredNeighbor),
            ("stand_ins", StandIn),
        ):
            values = data[name]
            if not isinstance(values, list | tuple):
                raise ValidationError(f"{name} must be a list")
            data[name] = tuple(kind(**_mapping(item, name)) for item in values)
        return cls(**data)
