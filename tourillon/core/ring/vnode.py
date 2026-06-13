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
"""VNode — virtual-node token on the consistent-hash ring."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VNode:
    """One virtual-node token owned by a physical node.

    A physical node contributes node_size.token_count VNode instances to the
    ring. Tokens are chosen randomly at the start of the join transition and
    never change. The token value is always in [0, 2**bits).
    """

    node_id: str
    token: int  # ∈ [0, 2**bits)
