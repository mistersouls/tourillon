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
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from tourlib.exceptions import ValidationError


@dataclass(frozen=True)
class NodeInspectRequest:
    """Input payload for node.inspect requests."""

    phases: tuple[str, ...] = ()
    probe_statuses: tuple[str, ...] = ()
    node_id_prefix: str | None = None
    only_anomalies: bool = False
    limit: int = 1024
    cursor: str | None = None

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "NodeInspectRequest":
        phases = cls._as_tuple_of_str(document.get("phases"), "phases")
        probe_statuses = cls._as_tuple_of_str(
            document.get("probe_statuses"), "probe_statuses"
        )
        node_id_prefix = document.get("node_id_prefix")
        if node_id_prefix is not None and (
            not isinstance(node_id_prefix, str) or not node_id_prefix
        ):
            raise ValidationError(
                "node_id_prefix must be a non-empty string when provided"
            )

        only_anomalies = cls._as_bool(
            document.get("only_anomalies", False), "only_anomalies"
        )
        limit = cls._as_limit(document.get("limit", 500))
        cursor = document.get("cursor")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValidationError("cursor must be a non-empty string when provided")

        return cls(
            phases=phases,
            probe_statuses=probe_statuses,
            node_id_prefix=node_id_prefix,
            only_anomalies=only_anomalies,
            limit=limit,
            cursor=cursor,
        )

    @staticmethod
    def _as_tuple_of_str(raw: Any, field_name: str) -> tuple[str, ...]:
        if raw is None:
            return ()
        if not isinstance(raw, list):
            raise ValidationError(f"{field_name} must be a list of non-empty strings")
        values: list[str] = []
        for item in raw:
            if not isinstance(item, str) or not item:
                raise ValidationError(
                    f"{field_name} must be a list of non-empty strings"
                )
            values.append(item.upper())
        return tuple(values)

    @staticmethod
    def _as_bool(raw: Any, field_name: str) -> bool:
        if not isinstance(raw, bool):
            raise ValidationError(f"{field_name} must be a boolean")
        return raw

    @staticmethod
    def _as_limit(raw: Any) -> int:
        if not isinstance(raw, int) or isinstance(raw, bool):
            raise ValidationError("limit must be an integer")
        if raw <= 0:
            raise ValidationError("limit must be > 0")
        return raw


@dataclass(frozen=True)
class MemberInspect:
    node_id: str
    phase: str
    status: str
    epoch: int


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


@dataclass(frozen=True)
class NodeInfo:
    node_id: str
    snapshot_at: str
    chunk_index: int
    has_more: bool
    next_cursor: str | None
    ring_position: RingPositionInspect
    self_member: MemberInspect
    members: tuple[MemberInspect, ...] = ()
    gossip_stats: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def now_iso() -> str:
        return datetime.now(UTC).isoformat()
