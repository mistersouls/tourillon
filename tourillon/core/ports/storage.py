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
"""BackendStorage, PartitionStaging, PartitionHint, PartitionStore, Storage.

All hexagonal storage port protocols. The core domain depends only on these
Protocol interfaces. Storage-engine specifics (transaction semantics, cursor
positioning, named data spaces) are confined to tourillon/infra/store/ and
never leak into core/.

Concrete implementations of PartitionStaging, PartitionHint, and PartitionStore
live in core/kv/store.py (domain layer) and are provided by proposal 005.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Protocol

from tourillon.core.machinery.namespace import Key, Namespace, Prefix, Tag, TaggedRecord
from tourillon.core.storage.store import PartitionStore

if TYPE_CHECKING:
    from tourillon.core.structure.record import Address, KvMetadata, Record


class Storage(Protocol):
    """Open a per-partition store backed by the underlying storage engine.

    One BackendStorage instance is maintained per **segment**. Multiple pids
    sharing the same segment (as determined by Partitioner.segment_for(pid))
    share the same BackendStorage. open_by_pid() resolves the segment for
    *pid* internally (infra concern) and returns a PartitionStore scoped to
    that pid.

    Callers never hold a reference to BackendStorage directly.
    """

    async def open_by_pid(self, pid: int) -> PartitionStore:
        """Return the PartitionStore for *pid*.

        The infra adapter resolves the segment via Partitioner.segment_for(pid),
        opens (or returns a cached) BackendStorage for that segment, and wraps
        it in a PartitionStore scoped to *pid*.
        """

    async def close(self) -> None:
        """Close the underlying storage engine."""
