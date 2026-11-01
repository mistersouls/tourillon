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

"""Local GC submission and observable asynchronous results."""

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from tourlib.exceptions import ValidationError


def _count(value: Any, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValidationError(f"GC {name} must be a non-negative integer")


@dataclass(frozen=True)
class GcRequest:
    dry_run: bool = False
    partition: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.dry_run, bool):
            raise ValidationError("GC dry_run must be boolean")
        if self.partition is not None:
            _count(self.partition, "partition")

    @classmethod
    def from_document(cls, document: Any) -> "GcRequest":
        if not isinstance(document, Mapping) or set(document) - {
            "dry_run",
            "partition",
        }:
            raise ValidationError("node.gc accepts only dry_run and partition")
        return cls(**document)


@dataclass(frozen=True)
class GcStatusRequest:
    job_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, str) or not self.job_id:
            raise ValidationError("GC job_id must be non-empty")

    @classmethod
    def from_document(cls, document: Any) -> "GcStatusRequest":
        if not isinstance(document, Mapping) or set(document) != {"job_id"}:
            raise ValidationError("node.gc.status requires job_id")
        return cls(**document)


@dataclass(frozen=True)
class GcStatus:
    job_id: str = ""
    state: str = "idle"
    dry_run: bool = False
    partition: int | None = None
    scanned: int = 0
    collected: int = 0
    record_bytes: int = 0
    current_group: int | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        self._validate_state()
        for name in ("scanned", "collected", "record_bytes"):
            _count(getattr(self, name), name)
        for name in ("partition", "current_group"):
            if getattr(self, name) is not None:
                _count(getattr(self, name), name)
        if not isinstance(self.dry_run, bool) or self.collected > self.scanned:
            raise ValidationError("Invalid GC dry_run or collected count")

    def _validate_state(self) -> None:
        if self.state not in (
            "idle",
            "queued",
            "running",
            "completed",
            "failed",
            "cancelled",
        ):
            raise ValidationError("Invalid GC state")
        if not isinstance(self.job_id, str) or (
            self.state != "idle" and not self.job_id
        ):
            raise ValidationError("GC pass requires a job_id")
        for value in (self.started_at, self.finished_at, self.error):
            if value is not None and (not isinstance(value, str) or not value):
                raise ValidationError("Invalid GC timestamp or error")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "GcStatus":
        if not isinstance(document, Mapping) or set(document) != set(
            cls.__dataclass_fields__
        ):
            raise ValidationError("Invalid GC status fields")
        return cls(**document)


@dataclass(frozen=True)
class GcInfo:
    running: bool
    versions: int
    interval: float
    hot_queue_size: int
    status: GcStatus

    def __post_init__(self) -> None:
        _count(self.versions, "versions")
        _count(self.hot_queue_size, "hot_queue_size")
        if not isinstance(self.running, bool) or not 1 <= self.versions <= 64:
            raise ValidationError("Invalid GC worker or retention")
        if (
            isinstance(self.interval, bool)
            or not isinstance(self.interval, (int, float))
            or not math.isfinite(self.interval)
            or self.interval <= 0
        ):
            raise ValidationError("GC interval must be positive and finite")
        if not isinstance(self.status, GcStatus):
            raise ValidationError("Invalid GC pass status")

    @classmethod
    def from_document(cls, document: Any) -> "GcInfo":
        if not isinstance(document, Mapping) or set(document) != set(
            cls.__dataclass_fields__
        ):
            raise ValidationError("Invalid GC info fields")
        return cls(**{**document, "status": GcStatus.from_document(document["status"])})


@dataclass(frozen=True)
class GcResponse:
    node_id: str
    status: GcStatus

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValidationError("GC response node_id must be non-empty")
        if not isinstance(self.status, GcStatus):
            raise ValidationError("Invalid GC response status")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "GcResponse":
        if not isinstance(document, Mapping) or set(document) != {"node_id", "status"}:
            raise ValidationError("Invalid GC response fields")
        return cls(document["node_id"], GcStatus.from_document(document["status"]))
