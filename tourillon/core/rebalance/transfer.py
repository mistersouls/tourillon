import asyncio
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from tourlib.envelope import Envelope
from tourlib.transport import TcpClient


@dataclass(frozen=True)
class PartitionTransfer:
    """Internal unit used by the applicator for per-pid tracking and staging.

    Never serialised on the wire. Produced by RangeTransfer.expand().
    """
    pid: int
    src: str
    dst: str

    @property
    def id(self) -> str:
        return f"{self.src}->{self.dst}:{self.pid}"

    def to_dict(self) -> dict[str, Any]:
        return {"pid": self.pid, "src": self.src, "dst": self.dst}


@dataclass(frozen=True, eq=True)
class RangeTransfer:
    start_pid: int
    end_pid: int
    count: int  # deprecated
    src: str
    dst: str

    @property
    def id(self) -> str:
        return f"{self.src}->{self.dst}:{self.start_pid}-{self.end_pid}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_pid": self.start_pid,
            "end_pid": self.end_pid,
            "src": self.src,
            "dst": self.dst,
        }

    def size(self, total_partitions: int) -> int:
        return (
            (total_partitions - self.start_pid) + self.end_pid + 1
            if self.start_pid > self.end_pid
            else self.end_pid - self.start_pid + 1
        )

    def pids(self, total_partitions: int) -> Iterator[int]:
        """Yield all partition ids covered by this range in logical order.

        Handles wrap-around when start_pid > end_pid (ring topology).
        """
        if self.start_pid > self.end_pid:
            yield from range(self.start_pid, total_partitions)
            yield from range(0, self.end_pid + 1)
        else:
            yield from range(self.start_pid, self.end_pid + 1)


@dataclass(frozen=True, eq=True)
class RangeSet:
    peer: str
    transfers: dict[str, RangeTransfer]
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    def expand(self, total_partitions) -> Iterator[PartitionTransfer]:
        for transfer in self.transfers.values():
            for pid in transfer.pids(total_partitions):
                yield PartitionTransfer(pid=pid, src=transfer.src, dst=transfer.dst)

class RebalancePlan:
    ranges: tuple[RangeTransfer]
    epoch: int
    total_partitions: int

    def expand(self) -> Iterator[PartitionTransfer]:
        for transfer in self.ranges:
            for pid in transfer.pids(self.total_partitions):
                yield PartitionTransfer(pid=pid, src=transfer.src, dst=transfer.dst)


class TransferState(StrEnum):
    """Lifecycle state of a single range transfer."""

    PENDING = "pending"
    RUNNING = "running"
    COMMITTED = "committed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class TransferMessage:
    client: TcpClient
    kind: str
    payload: dict[str, Any]
    correlation_id: uuid.UUID


@dataclass
class TransferHandle:
    """Mutable tracking record for one in-flight range transfer.

    cancel_event.set() causes the transfer coroutine to abort cleanly:
    staging.cleanup() is called for all pids in the range and
    WaitGroup.done(range_key, False) is signalled.
    last_error holds the human-readable description of the most recent failure,
    or None when no error has occurred.
    """

    transfer: PartitionTransfer
    state: TransferState
    queue: asyncio.Queue[TransferMessage] = field(default_factory=asyncio.Queue)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    chunks_done: int = 0
    chunks_total: int | None = None
    bytes_done: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_error: str | None = None


@dataclass
class PeerStream:
    peer: str
    transfers: list[RangeTransfer] = field(default_factory=list)
    handles: dict[str, TransferHandle] = field(default_factory=dict)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    last_error: str | None = None
