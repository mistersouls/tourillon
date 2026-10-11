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

from dataclasses import dataclass
from typing import Any

from tourillon.core.models.storageschema import Record


@dataclass(frozen=True)
class HintRequest:
    target_node_id: str
    record: Record

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_node_id": self.target_node_id,
            "record": self.record.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HintRequest":
        return cls(
            target_node_id=data["target_node_id"],
            record=Record.from_dict(data["record"]),
        )

