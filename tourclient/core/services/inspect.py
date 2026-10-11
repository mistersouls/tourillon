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
from dataclasses import asdict, replace
from typing import Any

from tourclient.core.transport import Transport
from tourlib import Serializer
from tourlib.exceptions import ResponseError
from tourlib.models.inspect import (
    MemberInspect,
    NodeInfo,
    NodeInspectRequest,
    RingPositionInspect,
    VnodeRangeInspect,
)


class InspectService:
    """Application service for node inspection operations."""

    def __init__(
        self,
        transport: Transport,
        serializer: Serializer,
    ) -> None:
        self._transport = transport
        self._serializer = serializer

    async def inspect(self, request: NodeInspectRequest | None = None) -> list[NodeInfo]:
        effective_request = request or NodeInspectRequest()
        chunks: list[NodeInfo] = []
        cursor = effective_request.cursor

        for _ in range(10_000):
            page_request = replace(effective_request, cursor=cursor)
            response = await self._transport.request(
                kind="node.inspect",
                document=_request_to_document(page_request),
            )
            if response.kind != "node.info":
                raise ResponseError(f"unexpected inspect response kind: {response.kind}")

            data = self._serializer.decode(response.payload)
            if not isinstance(data, Mapping):
                raise ResponseError("invalid node.inspect response payload")
            node_info = _node_info_from_document(data)
            chunks.append(node_info)

            if not node_info.has_more:
                return chunks
            if not node_info.next_cursor:
                raise ResponseError(
                    "inspect response indicated more data but next_cursor is missing"
                )
            if node_info.next_cursor == cursor:
                raise ResponseError("inspect response cursor did not advance")
            cursor = node_info.next_cursor

        raise ResponseError("inspect pagination exceeded safety limit")


def _request_to_document(request: NodeInspectRequest) -> dict[str, Any]:
    document = asdict(request)
    document["phases"] = list(request.phases)
    document["probe_statuses"] = list(request.probe_statuses)
    return document


def _node_info_from_document(data: Mapping[str, Any]) -> NodeInfo:
    ring_data = _require_mapping(data, "ring_position")
    members_data = _require_list(data, "members")
    gossip_stats = data.get("gossip_stats")
    if gossip_stats is None:
        gossip_stats = {}
    if not isinstance(gossip_stats, dict):
        raise ResponseError("invalid node.inspect response gossip_stats")

    return NodeInfo(
        node_id=_require_str(data, "node_id"),
        snapshot_at=_require_str(data, "snapshot_at"),
        chunk_index=_require_int(data, "chunk_index"),
        has_more=_require_bool(data, "has_more"),
        next_cursor=_require_optional_str(data, "next_cursor"),
        ring_position=_ring_position_from_document(ring_data),
        self_member=_require_member(data, "self_member"),
        members=tuple(_member_from_document(item) for item in members_data),
        gossip_stats=dict(gossip_stats),
    )


def _ring_position_from_document(data: Mapping[str, Any]) -> RingPositionInspect:
    vnode_ranges_data = _require_list(data, "vnode_ranges")
    return RingPositionInspect(
        partition_shift=_require_int(data, "partition_shift"),
        total_partitions=_require_int(data, "total_partitions"),
        owned_partitions=_require_int(data, "owned_partitions"),
        owned_percent=float(data.get("owned_percent", 0.0)),
        vnode_ranges=tuple(_vnode_range_from_document(item) for item in vnode_ranges_data),
    )


def _vnode_range_from_document(data: Any) -> VnodeRangeInspect:
    mapping = _ensure_mapping(data, "vnode_range")
    return VnodeRangeInspect(
        token=_require_str(mapping, "token"),
        arc=_require_arc(mapping, "arc"),
        partitions=_require_int(mapping, "partitions"),
    )


def _require_arc(data: Mapping[str, Any], field_name: str) -> tuple[int, int] | None:
    value = data.get(field_name)
    if value is None:
        return None
    if not isinstance(value, list | tuple) or len(value) != 2:
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    start, end = value
    if not isinstance(start, int) or isinstance(start, bool):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    if not isinstance(end, int) or isinstance(end, bool):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return start, end


def _member_from_document(data: Any) -> MemberInspect:
    mapping = _ensure_mapping(data, "member")
    return MemberInspect(
        node_id=_require_str(mapping, "node_id"),
        phase=_require_str(mapping, "phase"),
        status=_require_str(mapping, "status"),
        epoch=_require_int(mapping, "epoch"),
    )


def _require_member(data: Mapping[str, Any], field_name: str) -> MemberInspect:
    value = data.get(field_name)
    if not isinstance(value, Mapping):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return _member_from_document(value)


def _require_mapping(data: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    value = data.get(field_name)
    return _ensure_mapping(value, field_name)


def _ensure_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return value


def _require_list(data: Mapping[str, Any], field_name: str) -> list[Any]:
    value = data.get(field_name)
    if not isinstance(value, list):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return value


def _require_str(data: Mapping[str, Any], field_name: str) -> str:
    value = data.get(field_name)
    if not isinstance(value, str) or not value:
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return value


def _require_optional_str(data: Mapping[str, Any], field_name: str) -> str | None:
    value = data.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return value


def _require_int(data: Mapping[str, Any], field_name: str) -> int:
    value = data.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return value


def _require_bool(data: Mapping[str, Any], field_name: str) -> bool:
    value = data.get(field_name)
    if not isinstance(value, bool):
        raise ResponseError(f"invalid node.inspect response field '{field_name}'")
    return value

