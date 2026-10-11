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

from tourlib.utils import BytesWalker
from tourlib.validation import validate_node_id_size


@dataclass(frozen=True, order=True, eq=True, slots=True)
class HLCTimestamp:
    """Immutable hybrid logical clock timestamp.

    Total order: (wall_ms, counter, node_id) -- lexicographic. Two timestamps
    from different nodes that share the same (wall_ms, counter) are still
    distinct because node_id is included. The ordering is preserved when
    serialising to bytes for use as ordered index keys.

    Advance timestamps by calling HLCClock.tick() or HLCClock.update(); never
    construct HLCTimestamp directly in application code.
    """

    wall_ms: int
    counter: int
    node_id: str

    def __post_init__(self) -> None:
        validate_node_id_size(self.node_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "wall_ms": self.wall_ms,
            "counter": self.counter,
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HLCTimestamp":
        return cls(
            wall_ms=data["wall_ms"],
            counter=data["counter"],
            node_id=data["node_id"],
        )

    def to_bytes(self) -> bytes:
        """
        Serialize the HLCTimestamp to a bytes representation.
        Layout: wall_ms (8B BE) | counter (2B BE) | node_id_len (2B BE) | node_id
        """
        node_id = self.node_id.encode("utf-8")
        node_id_len = len(node_id)
        return (
            self.wall_ms.to_bytes(8, "big")
            + self.counter.to_bytes(2, "big")
            + node_id_len.to_bytes(2, "big")
            + node_id
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "HLCTimestamp":
        walker = BytesWalker(data)
        return cls.walk(walker)

    @classmethod
    def walk(cls, walker: BytesWalker) -> "HLCTimestamp":
        wall_ms = walker.read_int(8, "hlc wall_ms")
        counter = walker.read_int(2, "hlc counter")
        node_id = walker.read_field("hlc node_id").decode("utf-8")
        return cls(wall_ms=wall_ms, counter=counter, node_id=node_id)



