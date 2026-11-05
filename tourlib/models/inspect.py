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
"""Wire models of the operator views: one node's status and the member list.

``node.inspect`` returns a :class:`NodeStatus`, a fixed-size summary of the
node that answers. ``node.members`` returns one :class:`MembersPage` of the
cluster as that node sees it through gossip: phase x status counts over all
members, then one page of members, either the anomalies or a filtered list.
"""

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from tourlib.exceptions import ValidationError
from tourlib.models.gc import GcInfo

PHASES = ("READY", "JOINING", "DRAINING", "PAUSED", "IDLE", "FAILED")
STATUSES = ("ALIVE", "SUSPECT", "DEAD")
DEFAULT_LIMIT = 50
MAX_LIMIT = 1000


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _mapping(document: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(document, Mapping):
        raise ValidationError(f"{name} must be a table")
    return document


def _upper_tuple(raw: Any, name: str, allowed: tuple[str, ...]) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list | tuple):
        raise ValidationError(f"{name} must be a list")
    values = []
    for item in raw:
        if not isinstance(item, str) or item.upper() not in allowed:
            raise ValidationError(f"{name} must be among {', '.join(allowed)}")
        if item.upper() not in values:
            values.append(item.upper())
    return tuple(values)


def _optional_str(value: Any, name: str) -> str | None:
    if value is not None and (not isinstance(value, str) or not value):
        raise ValidationError(f"{name} must be a non-empty string when provided")
    return value


@dataclass(frozen=True)
class MembersRequest:
    """Filters and page of a ``node.members`` request.

    Without any filter and without ``list_all`` the page lists anomalies:
    members that are not both READY and ALIVE.
    """

    phases: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ()
    prefix: str | None = None
    list_all: bool = False
    limit: int = DEFAULT_LIMIT
    cursor: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "phases", _upper_tuple(self.phases, "phases", PHASES))
        object.__setattr__(
            self, "statuses", _upper_tuple(self.statuses, "statuses", STATUSES)
        )
        _optional_str(self.prefix, "prefix")
        _optional_str(self.cursor, "cursor")
        if not isinstance(self.list_all, bool):
            raise ValidationError("list_all must be a boolean")
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or not 1 <= self.limit <= MAX_LIMIT
        ):
            raise ValidationError(f"limit must be an integer from 1 to {MAX_LIMIT}")

    @property
    def anomalies_only(self) -> bool:
        return not (self.list_all or self.phases or self.statuses or self.prefix)

    def to_document(self) -> dict[str, Any]:
        document = asdict(self)
        document["phases"] = list(self.phases)
        document["statuses"] = list(self.statuses)
        return document

    @classmethod
    def from_document(cls, document: Any) -> "MembersRequest":
        data = _mapping(document, "node.members request")
        unknown = set(data) - set(cls.__dataclass_fields__)
        if unknown:
            raise ValidationError(f"unknown fields: {', '.join(sorted(unknown))}")
        return cls(**data)


@dataclass(frozen=True)
class PhaseCount:
    phase: str
    alive: int = 0
    suspect: int = 0
    dead: int = 0

    @property
    def total(self) -> int:
        return self.alive + self.suspect + self.dead


@dataclass(frozen=True)
class MemberView:
    node_id: str
    address: str
    phase: str
    status: str
    epoch: int
    last_heard_s: float | None = None
    since_s: float | None = None

    @property
    def anomalous(self) -> bool:
        return self.phase != "READY" or self.status != "ALIVE"


@dataclass(frozen=True)
class MembersPage:
    node_id: str
    snapshot_at: str
    members: int
    summary: tuple[PhaseCount, ...]
    items: tuple[MemberView, ...]
    total: int
    cursor: str | None = None
    anomalies_only: bool = True

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "MembersPage":
        data = dict(_mapping(document, "node.members response"))
        data["summary"] = tuple(
            PhaseCount(**_mapping(item, "summary")) for item in data["summary"]
        )
        data["items"] = tuple(
            MemberView(**_mapping(item, "member")) for item in data["items"]
        )
        return cls(**data)


@dataclass(frozen=True)
class VnodeRangeInspect:
    token: str
    arc: tuple[int, int] | None
    partitions: int


@dataclass(frozen=True)
class RingPositionInspect:
    partition_shift: int
    total_partitions: int
    owned_partitions: int
    owned_percent: float
    vnode_ranges: tuple[VnodeRangeInspect, ...] = ()

    @classmethod
    def from_document(cls, document: Any) -> "RingPositionInspect":
        data = dict(_mapping(document, "ring_position"))
        data["vnode_ranges"] = tuple(
            VnodeRangeInspect(
                token=item["token"],
                arc=None if item["arc"] is None else tuple(item["arc"]),
                partitions=item["partitions"],
            )
            for item in data["vnode_ranges"]
        )
        return cls(**data)


@dataclass(frozen=True)
class MembersSummary:
    """How many members this node knows, and how many are READY and ALIVE."""

    total: int
    ready_alive: int


@dataclass(frozen=True)
class AntiEntropyInfo:
    queued: int
    inflight: int
    backlog: bool
    sweeps: int
    last_sweep_at: str | None
    last_sweep_diverged: int
    delivered: int
    errors: int
    last_failure: str | None = None
    running: bool = True
    pending_versions: int = 0
    pending_targets: int = 0
    oldest_wall_ms: int | None = None
    stuck: int = 0
    pending_truncated: bool = False


@dataclass(frozen=True)
class StorageInfo:
    """Space of the storage files and versions held, summed over groups."""

    map_size: int
    used: int
    versions: int
    retention: int

    @property
    def used_ratio(self) -> float:
        return 0.0 if self.map_size <= 0 else self.used / self.map_size


@dataclass(frozen=True)
class TlsInfo:
    """Expiry of the certificate this node presents, and rejected handshakes."""

    expires_at: str
    rejected: int = 0
    last_rejection: str | None = None
    last_rejection_ago_s: float | None = None


@dataclass(frozen=True)
class WaitingOn:
    """A peer a join or drain waits on, with what this node knows of it."""

    node_id: str
    partitions: int
    phase: str | None
    status: str
    since_s: float | None = None
    retry_in_s: float = 0.0
    last_error: str | None = None


@dataclass(frozen=True)
class ProgressInfo:
    """Last pass of a running join or drain; ``total == 0`` before the first."""

    goal: str
    total: int
    done: int
    waiting: tuple[WaitingOn, ...] = ()
    waiting_more: int = 0

    @classmethod
    def from_document(cls, document: Any) -> "ProgressInfo":
        data = dict(_mapping(document, "progress"))
        data["waiting"] = tuple(
            WaitingOn(**_mapping(item, "waiting")) for item in data.get("waiting", ())
        )
        return cls(**data)


@dataclass(frozen=True)
class NodeStatus:
    node_id: str
    address: str
    version: str
    snapshot_at: str
    phase: str
    epoch: int
    since_s: float | None
    ring_position: RingPositionInspect
    members: MembersSummary
    antientropy: AntiEntropyInfo
    gc: GcInfo
    gossip_stats: dict[str, Any] = field(default_factory=dict)
    storage: StorageInfo | None = None
    tls: TlsInfo | None = None
    progress: ProgressInfo | None = None

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "NodeStatus":
        data = dict(_mapping(document, "node.inspect response"))
        data["ring_position"] = RingPositionInspect.from_document(data["ring_position"])
        data["members"] = MembersSummary(**_mapping(data["members"], "members"))
        data["antientropy"] = AntiEntropyInfo(
            **_mapping(data["antientropy"], "antientropy")
        )
        data["gc"] = GcInfo.from_document(data["gc"])
        if data.get("storage") is not None:
            data["storage"] = StorageInfo(**_mapping(data["storage"], "storage"))
        if data.get("tls") is not None:
            data["tls"] = TlsInfo(**_mapping(data["tls"], "tls"))
        if data.get("progress") is not None:
            data["progress"] = ProgressInfo.from_document(data["progress"])
        return cls(**data)
