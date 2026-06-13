import logging
from collections.abc import AsyncIterator

from tourillon.core.machinery.namespace import (
    Key,
    Namespace,
    Pid,
    Tag,
    TaggedRecord,
    TagKind,
    TagPayload,
    Value,
)
from tourillon.core.ports.backend import BackendStorage
from tourillon.core.structure.record import Record, Version

logger = logging.getLogger(__name__)


class PartitionStaging:
    def __init__(
        self,
        pid: int,
        epoch: int,
        backend: BackendStorage,
        batch_size: int = 1024
    ) -> None:
        self._pid = Pid(pid)
        self._epoch = epoch
        self._epoch_bytes = epoch.to_bytes(4, byteorder="big")
        self._backend = backend
        self._batch_size = batch_size

    async def stage(self, tagged: TaggedRecord) -> None:
        """Write one record to the staging area for this (pid, epoch)."""
        payload = self._epoch_bytes + tagged.tag.payload.to_bytes()

        staged = TaggedRecord(
            key=tagged.key,
            value=tagged.value,
            tag=Tag(
                kind=TagKind.STAGING,
                payload=TagPayload(sub=tagged.tag.kind, payload=payload),
            ),
        )
        await self._backend.put(staged)

    async def commit(self) -> None:
        """Atomically promote all staged entries to committed visibility.

        Must be called before updating the node state file (write-before-announce).
        """
        async for tagged in self._collect_staged():
            sub = tagged.tag.payload.sub
            assert sub is not None
            await self._backend.tag(key=tagged.key, tag=Tag(kind=sub))

    async def cleanup(self) -> None:
        """Delete all staging entries for this (pid, epoch) without promoting."""
        async for tagged in self._collect_staged():
            await self._backend.delete(tagged)

    async def exists(self) -> bool:
        """Return True if staging entries exist for this (pid, epoch)."""
        async for _ in self._collect_staged():
            return True
        return False

    async def last_staged_key(self) -> Key | None:
        """Return the cursor key of the highest-HLC staging entry, or None."""
        async for tagged in self._collect_staged(reverse=True):
            return tagged.key
        return None

    async def _collect_staged(self, reverse: bool = False) -> AsyncIterator[TaggedRecord]:
        async for tagged in self._backend.iter(
            namespace=Namespace.TAGS,
            prefix=self._pid,
            batch_size=self._batch_size,
            reverse=reverse
        ):
            if tagged.tag.kind == TagKind.STAGING:
                sub = tagged.tag.payload.sub
                epoch_bytes = tagged.tag.payload.payload
                if sub is not None and epoch_bytes == self._epoch_bytes:
                    yield tagged
