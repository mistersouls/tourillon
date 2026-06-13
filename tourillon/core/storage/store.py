from collections.abc import AsyncIterator

from tourillon.core.machinery.namespace import (
    Key,
    KeyPartition,
    Namespace,
    Pid,
    Tag,
    TaggedRecord,
    TagKind,
    Value,
)
from tourillon.core.ports.backend import BackendStorage
from tourillon.core.storage.hint import PartitionHint
from tourillon.core.storage.staging import PartitionStaging
from tourillon.core.structure.record import (
    Address,
    KvMetadata,
    Record,
    Tombstone,
    Version,
)


class PartitionStore:
    def __init__(self, pid: int, backend: BackendStorage) -> None:
        self._pid = Pid(pid)
        self._backend = backend

    async def scan(
        self,
        resume_from: Key | None = None,
        batch_size: int = 1024
    ) -> AsyncIterator[TaggedRecord]:
        """Yield all committed records for this partition, optionally from a cursor."""
        if resume_from is not None:
            if resume_from.pid != self._pid:
                raise ValueError(
                    f"resume_from pid {resume_from.pid} does not match "
                    f"partition pid {self._pid}"
                )
            start = resume_from.to_bytes(Namespace.TAGS)
        else:
            start = None

        async for rec in self._backend.iter(
            namespace=Namespace.TAGS,
            start=start,
            prefix=self._pid,
            batch_size=batch_size,
        ):
            yield rec

    def staging(self, epoch: int) -> PartitionStaging:
        """Return a PartitionStaging context scoped to (pid, epoch)."""
        return PartitionStaging(self._pid.value, epoch, self._backend)

    def hint(self, node_id: str) -> PartitionHint:
        """Return a PartitionHint context scoped to (pid, node_id)."""
        return PartitionHint(self._pid.value, node_id, self._backend)

    async def get(self, addr: Address) -> Record | None:
        prefix = KeyPartition(pid=self._pid, addr=addr)
        async for tagged in self._backend.iter(
            namespace=Namespace.LOG,
            prefix=prefix,
            reverse=True,
            limit=1,
            batch_size=1,
        ):
            if tagged.tag.kind == TagKind.STALE:
                return None
            return tagged.to_record()
        return None

    async def put(
        self, addr: Address, value: bytes, meta: KvMetadata
    ) -> Version:
        tagged = TaggedRecord(
            key=Key(pid=self._pid, addr=addr, ts=meta.hlc),
            value=Value(payload=value, quorum_write=meta.quorum_write),
            tag=Tag(kind=TagKind.LIVE)
        )
        await self._backend.put(tagged)
        return Version(
            address=addr,
            value=value,
            metadata=meta.hlc,
            quorum_write=meta.quorum_write,
        )

    async def tombstone(self, addr: Address, meta: KvMetadata) -> Tombstone:
        tagged = TaggedRecord(
            key=Key(pid=self._pid, addr=addr, ts=meta.hlc),
            value=Value(payload=b"", quorum_write=meta.quorum_write),
            tag=Tag(kind=TagKind.TOMBSTONE),
        )
        await self._backend.put(tagged)
        return Tombstone(
            address=addr,
            metadata=meta.hlc,
            quorum_write=meta.quorum_write,
        )
