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

from tourillon.core.models.member import Member
from tourlib.exceptions import ValidationError
from tourlib.validation import require_str


@dataclass
class GossipPush:
    sender: str
    hop_limit: int
    members: list[Member]

    @classmethod
    def from_document(cls, data: Mapping[str, Any]) -> "GossipPush":
        members_data = data.get("members", [])
        if not isinstance(members_data, list):
            raise ValidationError("members must be a list")

        members: list[Member] = []
        for index, member_data in enumerate(members_data):
            if not isinstance(member_data, Mapping):
                raise ValidationError(f"members[{index}] must be a table")
            members.append(Member.from_dict(dict(member_data)))

        return cls(
            sender=require_str(data, "sender"),
            hop_limit=cls.as_int(data.get("hop_limit"), "hop_limit", min_value=0),
            members=members,
        )

    @staticmethod
    def as_int(raw: Any, field_name: str, *, min_value: int | None = None) -> int:
        if not isinstance(raw, int) or isinstance(raw, bool):
            raise ValidationError(f"{field_name} must be an integer")
        if min_value is not None and raw < min_value:
            raise ValidationError(f"{field_name} must be >= {min_value}")
        return raw


@dataclass
class GossipPushAccept:
    sender: str
    accepted: int
    ignored: int


@dataclass
class GossipPing:
    sender: str
    fingerprint: str
    count: int

    @classmethod
    def from_document(cls, data: Mapping[str, Any]) -> "GossipPing":
        return cls(
            sender=require_str(data, "sender"),
            fingerprint=require_str(data, "fingerprint"),
            count=GossipPush.as_int(data.get("count"), "count", min_value=0),
        )


@dataclass
class GossipPong:
    sender: str
    same: bool


@dataclass
class GossipNodeDigest:
    node_id: str
    epoch: int


@dataclass
class GossipDigest:
    sender: str
    has_more: bool
    after_node_id: str
    digest: list[GossipNodeDigest]

    @classmethod
    def from_document(cls, data: Mapping[str, Any]) -> "GossipDigest":
        digest_data = data.get("digest", [])
        if not isinstance(digest_data, list):
            raise ValidationError("digest must be a list")

        digest = [
            cls._parse_node_digest(item, index)
            for index, item in enumerate(digest_data)
        ]

        after_node_id = data.get("after_node_id", "")
        if not isinstance(after_node_id, str):
            raise ValidationError("after_node_id must be a string")

        return cls(
            sender=require_str(data, "sender"),
            has_more=cls._as_bool(data.get("has_more", False), "has_more"),
            after_node_id=after_node_id,
            digest=digest,
        )

    @staticmethod
    def _as_bool(raw: Any, field_name: str) -> bool:
        if not isinstance(raw, bool):
            raise ValidationError(f"{field_name} must be a boolean")
        return raw

    @staticmethod
    def _parse_node_digest(raw: Any, index: int) -> GossipNodeDigest:
        if not isinstance(raw, Mapping):
            raise ValidationError(f"digest[{index}] must be a table")
        return GossipNodeDigest(
            node_id=require_str(raw, "node_id"),
            epoch=GossipPush.as_int(
                raw.get("epoch"), f"digest[{index}].epoch", min_value=0
            ),
        )


@dataclass
class GossipDelta:
    sender: str
    members: list[Member]
    has_more: bool
    after_node_id: str
    wanted: list[str]
