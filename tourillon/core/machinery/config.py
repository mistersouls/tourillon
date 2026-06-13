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
"""Node configuration dataclasses and NodeSize enumeration."""

from enum import StrEnum


class NodeSize(StrEnum):
    """Token count per physical node on the consistent-hash ring.

    The size is set once at config-generate time and is immutable after the
    node joins a cluster. Changing it requires the node to leave, reconfigure,
    and re-join. A heterogeneous cluster is fully supported; larger nodes
    receive proportionally more ring partitions.
    """

    XS = "XS"  # 1 token
    S = "S"  # 2 tokens
    M = "M"  # 4 tokens  (default)
    L = "L"  # 8 tokens
    XL = "XL"  # 16 tokens
    XXL = "XXL"  # 32 tokens

    @property
    def token_count(self) -> int:
        """Return the number of virtual-node tokens for this size class."""
        return {"XS": 1, "S": 2, "M": 4, "L": 8, "XL": 16, "XXL": 32}[self.value]
