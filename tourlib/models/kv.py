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


@dataclass(frozen=True)
class GetRequest:
    namespace: bytes
    key: bytes
    quorum_read: int = 1


@dataclass(frozen=True)
class PutRequest:
    namespace: bytes
    key: bytes
    value: bytes
    quorum_write: int = 1


@dataclass(frozen=True)
class DeleteRequest:
    namespace: bytes
    key: bytes
    quorum_write: int = 1


@dataclass(frozen=True)
class RepairRequest:
    """Resume a write that was persisted without reaching its quorum.

    The token is the opaque handle returned with the quorum failure. It
    designates an existing version, so a repair copies it rather than writing
    a new one, which is what makes retrying harmless.
    """

    token: str
    quorum_write: int = 1
