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

from tourlib.exceptions import ValidationError


@dataclass
class JoinRequest:
    seeds: set[str] | None = None


@dataclass
class JoinResponse:
    success: bool
    message: str


@dataclass(frozen=True)
class LeaveRequest:
    """Initiate asynchronous draining of the addressed node."""

    @classmethod
    def from_document(cls, document: Any) -> "LeaveRequest":
        if not isinstance(document, Mapping) or document:
            raise ValidationError("node.leave request must be an empty mapping")
        return cls()


@dataclass(frozen=True)
class LeaveResponse:
    """Acknowledgement of initiation, not completion of draining."""

    message: str

    def __post_init__(self) -> None:
        if not isinstance(self.message, str) or not self.message:
            raise ValidationError("node.leave response message must be non-empty")

    @classmethod
    def from_document(cls, document: Any) -> "LeaveResponse":
        if not isinstance(document, Mapping) or set(document) != {"message"}:
            raise ValidationError("node.leave response must contain a message")
        return cls(message=document["message"])
