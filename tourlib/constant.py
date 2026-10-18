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

"""Protocol and runtime constants shared across Tourillon components."""

# Envelope header fixed size in bytes (wire-format invariant).
ENVELOPE_HEADER_LEN = 32

# Max user-facing payload size for client operations.
MAX_USER_ENVELOPE_PAYLOAD = 16 * 1024 * 1024

# Max internode payload size accepted by transport runtime.
MAX_ENVELOPE_PAYLOAD = 18 * 1024 * 1024

# Max time to wait for an envelope response before timing out.
ENVELOPE_RESPONSE_TIMEOUT = 30.0  # seconds

# Max time to wait for a connection to be established before timing out.
ENVELOPE_CONNECTION_TIMEOUT = 5.0  # seconds

# Current protocol version used for wire-format compatibility.
PROTOCOL_VERSION = 1

# Max UTF-8 byte length of an envelope kind string.
MAX_KIND_LEN = 255

# Max UTF-8 byte length for node identifiers.
MAX_NODE_ID_LEN_BYTES = 63

# Max raw byte length for a namespace in Address encoding.
MAX_NAMESPACE_LEN = 63

# Max raw byte length for a key in Address encoding.
MAX_KEY_LEN = 128

# Max quorum a request may ask for. A stored version records the durability it
# was written for in a single byte, which bounds what a request can mean.
MAX_QUORUM = 255

# How far ahead of local wall time a timestamp submitted by a client may be
# before it is treated as fabricated rather than as clock drift.
MAX_CLOCK_SKEW_MS = 60_000

# Max concurrently in-flight RPCs handled per connection/session.
MAX_IN_FLIGHT = 32

# Reserved schema id for raw bytes payloads.
RAW_SCHEMA_ID = 0

# Reserved schema id for msgpack codec payloads.
MSGPACK_SCHEMA_ID = 1

# Canonical envelope kind used for transport-layer errors.
TRANSPORT_ERROR_KIND = "transport.error"

# Filename used to persist node lifecycle state on disk.
NODE_STATE_FILENAME = "state.toml"
