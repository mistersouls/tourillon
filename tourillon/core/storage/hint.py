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


class PartitionHint:
    def __init__(self, pid: int, node_id: str, backend: BackendStorage) -> None:
        self._pid = Pid(pid)
        self._node_id = node_id
        self._node_bytes = node_id.encode()
        self._backend = backend

    async def put(self, record: Record) -> None:
        """Write a hinted put for addr."""
        if isinstance(record, Version):
            tagged = TaggedRecord(
                key=Key(pid=self._pid, addr=record.address, ts=record.metadata),
                value=Value(quorum_write=record.quorum_write, payload=record.value),
                tag=Tag(kind=TagKind.HINT, payload=TagPayload(sub=TagKind.LIVE, payload=self._node_bytes))
            )
        else:
            tagged = TaggedRecord(
                key=Key(pid=self._pid, addr=record.address, ts=record.metadata),
                value=Value(quorum_write=record.quorum_write, payload=b""),
                tag=Tag(kind=TagKind.HINT, payload=TagPayload(sub=TagKind.TOMBSTONE, payload=self._node_bytes))
            )

        await self._backend.put(tagged)

    async def iter(
        self,
        resume_from: Key | None = None,
        batch_size: int = 1024
    ) -> AsyncIterator[TaggedRecord]:
        async for tagged in self._collect_hint(
            resume_from=resume_from,
            batch_size=batch_size
        ):
            yield tagged

    async def _collect_hint(
        self,
        resume_from: Key | None = None,
        batch_size: int = 1024
    ) -> AsyncIterator[TaggedRecord]:
        if resume_from is not None:
            if resume_from.pid != self._pid:
                raise ValueError(
                    f"resume_from pid {resume_from.pid} does not match "
                    f"partition pid {self._pid}"
                )
            start = resume_from.to_bytes(Namespace.TAGS)
        else:
            start = None

        async for tagged in self._backend.iter(
            namespace=Namespace.TAGS,
            start=start,
            prefix=self._pid,
            batch_size=batch_size,
        ):
            if self._is_hint(tagged.tag):
                yield tagged

    def _is_hint(self, tag: Tag) -> bool:
        node_bytes = tag.payload.payload
        return tag.kind == TagKind.HINT and self._node_bytes == node_bytes
