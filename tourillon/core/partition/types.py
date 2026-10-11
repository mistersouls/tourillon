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

from tourillon.core.spaces.vnode import VNode


@dataclass(frozen=True, eq=True, slots=True)
class LogicalPartition:
    pid: int
    start: int
    end: int # half open [start, end)

    def contains(self, h: int) -> bool:
        if self.start < self.end:
            return self.start <= h < self.end
        else:
            return h >= self.start or h < self.end


@dataclass
class PartitionPlacement:
    group: int
    vnode: VNode
    partition: LogicalPartition
