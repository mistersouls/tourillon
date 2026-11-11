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

import builtins
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import Any, Protocol

from tourillon.core.models.config import StorageConfig
from tourillon.core.models.storageschema import KeyList, Keyspace, KVList, KVPair, KVRef
from tourillon.core.utils.defer import Result
from tourlib.models.types import SupportsBytes


class Transaction(Protocol):
    async def results(self) -> list[Any]:
        """Wait for the transaction operations to complete and return the results."""

    def exists(self, ref: KVRef) -> Result[bool]:
        """Check key-presence."""

    def get(self, ref: KVRef) -> Result[KVPair | None]:
        """Get a key-value pair by its reference."""

    def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> Result[KVPair | None]:
        """Get the last key-value pair matching the given criteria."""

    def keys(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int = 128,
        reverse: bool = False,
    ) -> Result[list[KVRef]]:
        """Get a bounded page of KVRef without materializing values."""

    def list(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> Result[list[KVPair]]:
        """Get a bounded page of KVPair matching the given criteria."""

    def put(self, kv: KVPair, overwrite: bool = True) -> Result[bool]:
        """Put a key-value pair into the storage, optionally overwriting existing values."""

    def delete(self, ref: KVRef) -> Result[KVPair | None]:
        """
        Delete a key-value pair from the storage by its reference.
        Returns the deleted KVPair if it existed, otherwise None.
        """

    def get_many(self, refs: Sequence[KeyList]) -> Result[builtins.list[KVPair]]:
        """Get multiple key-value pairs by their references."""

    def put_many(
        self, kvs: Sequence[KVList], overwrite: bool = True
    ) -> Result[tuple[int, int]]:
        """Put multiple key-value pairs into the storage, optionally overwriting existing values."""

    def delete_many(
        self, refs: Sequence[KeyList]
    ) -> Result[builtins.list[KVPair | None]]:
        """
        Delete multiple key-value pairs from the storage by their references.
        Returns a list of deleted KVPairs if they existed, otherwise None
        for each non-existent reference.
        """


class Transactional(Protocol):
    @asynccontextmanager
    def transaction(self, write: bool = False) -> AsyncIterator[Transaction]:
        """Enter a transactional context."""

    async def close(self) -> None:
        """Close the transactional storage, releasing any resources."""


class Storage(Protocol):
    async def transactional_for(self, group: int) -> Transactional:
        """Return a transactional for the given group ID."""

    async def close(self) -> None:
        """Close the storage and release any resources."""


class StorageProvider(Protocol):
    def create(self, config: StorageConfig) -> Storage:
        """Create a new storage instance."""
