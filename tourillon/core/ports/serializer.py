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
"""SerializerPort — payload codec abstraction used throughout the core layer.

The core domain never imports msgpack directly. All encode/decode operations
go through this Protocol so that tests can inject a trivially-decodable stub
and future codecs (e.g. a JSON debug adapter) can be swapped per connection
without modifying any business logic.
"""

from __future__ import annotations

from typing import Any, Protocol


class SerializerPort(Protocol):
    """Payload codec used to encode and decode Envelope payloads.

    Implementors must declare schema_id so that the transport layer can embed
    the correct codec identifier in the Envelope header. The default infra
    adapter (MsgpackSerializerAdapter) uses MessagePack and declares
    schema_id = 1. schema_id = 0 is reserved for raw bytes (no codec).
    """

    schema_id: int  # must match the schema_id written into Envelope headers

    def encode(self, obj: Any) -> bytes:
        """Encode *obj* to bytes using this codec."""

    def decode(self, data: bytes) -> Any:
        """Decode *data* from bytes using this codec."""
