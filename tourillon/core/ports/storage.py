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

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Protocol

from tourillon.core.models.clock import HLCTimestamp
from tourillon.core.models.config import StorageConfig
from tourillon.core.models.storageschema import (
    Keyspace,
    KVPair,
    KVRef,
)
from tourlib.models.types import SupportsBytes


class Batch(Protocol):
    async def __aenter__(self) -> "Batch":
        """Enter the batch context manager."""

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: BaseException | None,
    ) -> bool:
        """Exit the batch context manager."""

    async def result[T](self) -> T:
        """Wait for the batch operations to complete and return the result."""

    def get(self, ref: KVRef) -> None:
        """Schedule a get operation for a key-value pair by its reference."""

    def exists(self, ref: KVRef) -> None:
        """Schedule a key-presence check without retrieving its value."""

    def put(self, kv: KVPair) -> None:
        """Schedule a put operation for a key-value pair into the storage."""

    def delete(self, ref: KVRef) -> None:
        """Schedule a delete operation for a key-value pair by its reference."""

    def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> None:
        """
        Schedule a get operation for the last key-value pair in the given keyspace,
        optionally filtered by prefix and predicate.
        """

    def list(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> None:
        """
        Schedule a list operation for key-value pairs in the given keyspace,
        optionally filtered by prefix, start key, limit, and order.

        For forward scans, start is exclusive: pass the last returned key
        unchanged to resume, even if that key has since been deleted.
        """


class Session(Protocol):
    async def exists(self, ref: KVRef) -> bool:
        """Check exact key presence without retrieving its value."""

    def batch(self) -> Batch:
        """Return a batch context manager for heavy multiple operations."""

    async def get(self, ref: KVRef) -> KVPair | None:
        """Get a key-value pair by its reference."""

    async def put(self, kv: KVPair) -> None:
        """Put a key-value pair into the storage."""

    async def delete(self, ref: KVRef) -> None:
        """Delete a key-value pair by its reference."""

    async def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> KVPair | None:
        """
        Get the last key-value pair in the given keyspace,
        optionally filtered by prefix and predicate.
        """

    def iter(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        reverse: bool = False,
        batch_size: int = 32,
        limit: int | None = None,
    ) -> AsyncIterator[KVPair]:
        """Iterate over key-value pairs in the given keyspace."""


class SessionProvider(Protocol):
    @property
    def group_id(self) -> int:
        """Return the group ID associated with this session provider."""

    @asynccontextmanager
    def session(self, write: bool = False) -> AsyncIterator[Session]:
        """Provide a session context manager for read or write operations."""

    async def close(self) -> None:
        """Close the session provider and release any resources."""


class Storage(Protocol):
    async def session_provider_for(self, group: int) -> SessionProvider:
        """Return a session provider for the given group ID."""

    async def latest_hlc(self, total_groups: int) -> HLCTimestamp:
        """Return the latest hybrid logical clock timestamp."""

    async def close(self) -> None:
        """Close the storage and release any resources."""


class StorageProvider(Protocol):
    def create(self, config: StorageConfig) -> Storage:
        """Create a new storage instance."""
