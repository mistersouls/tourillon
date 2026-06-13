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
"""MemberDigestEntry — compact version-vector entry for gossip.digest envelopes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MemberDigestEntry:
    """Compact version summary for anti-entropy version vector.

    Used only inside gossip.digest envelopes. Never persisted or sent alone.
    Wire size with configured serializer: ~25–30 bytes per entry. 10 000 entries ≈ 300 KB —
    acceptable for the occasional anti-entropy exchange, never sent every cycle.
    """

    node_id: str
    generation: int
    seq: int
