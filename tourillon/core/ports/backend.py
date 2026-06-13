from collections.abc import AsyncIterator
from typing import Protocol

from tourillon.core.machinery.namespace import Key, Namespace, Prefix, Tag, TaggedRecord


class BackendStorage(Protocol):
    async def put(self, record: TaggedRecord) -> bool:
        """Write kvt atomically to both namespaces. Return False on no-op."""

    async def delete(self, record: TaggedRecord) -> bool:
        """Physically delete kvt from both namespaces (GC / cleanup only)."""

    async def tag(self, key: Key, tag: Tag) -> bool:
        """Update only the namespace_tags entry for key. Return False if absent."""

    def iter(
        self,
        namespace: Namespace,
        prefix: Prefix | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
        batch_size: int = 1024,
    ) -> AsyncIterator[TaggedRecord]:
        """Yield tagged records in key order within prefix scope."""

    async def close(self) -> None:
        """Close the Backend storage."""

