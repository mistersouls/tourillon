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

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from tourillon.core.models.storageschema import Key


def _resumes_to_dict(resumes: Mapping[int, Key]) -> dict[str, Any]:
    return {str(pid): key.to_dict() for pid, key in resumes.items()}


def _resumes_from_dict(data: Mapping[str, Any]) -> dict[int, Key]:
    return {int(pid): Key.from_dict(key) for pid, key in data.items()}


@dataclass(frozen=True, slots=True, eq=True, order=True)
class PartitionTransfer:
    pid: int
    src: str
    dst: str

    @property
    def id(self) -> str:
        """Return a unique identifier for this partition transfer."""
        return f"{self.pid}:{self.src}->{self.dst}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the PartitionTransfer to a dictionary."""
        return {"pid": self.pid, "src": self.src, "dst": self.dst}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PartitionTransfer":
        """Deserialize a dictionary to a PartitionTransfer."""
        return cls(pid=data["pid"], src=data["src"], dst=data["dst"])


@dataclass(frozen=True, slots=True)
class RangeTransfer:
    segment: tuple[int, int]
    src: str
    dst: str

    @property
    def id(self) -> str:
        """Return a unique identifier for this range transfer."""
        return f"{self.segment[0]}-{self.segment[1]}:{self.src}->{self.dst}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the RangeTransfer to a dictionary."""
        return {"segment": self.segment, "src": self.src, "dst": self.dst}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RangeTransfer":
        """Deserialize a dictionary to a RangeTransfer.

        The segment is normalised to a tuple: msgpack decodes it as a list,
        which would break equality and hashing.
        """
        lo, hi = data["segment"]
        return cls(segment=(int(lo), int(hi)), src=data["src"], dst=data["dst"])


@dataclass
class TransferPlan:
    ranges: list[RangeTransfer]
    serial: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferPlan to a dictionary."""
        return {
            "ranges": [range_.to_dict() for range_ in self.ranges],
            "serial": self.serial,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferPlan":
        """Deserialize a dictionary to a TransferPlan."""
        return cls(
            ranges=[RangeTransfer.from_dict(item) for item in data["ranges"]],
            serial=data["serial"],
        )


class TransferState(StrEnum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


@dataclass
class TransferHandle:
    """Progress of one range transfer on this node.

    `attempts` counts the attempts made so far and `transferred` the bytes
    of record frames received (pull) or acknowledged (push) over all of them.
    """

    range: RangeTransfer
    state: TransferState
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_error: str | None = None
    attempts: int = 0
    transferred: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferHandle to a dictionary."""
        return {
            "range": self.range.to_dict(),
            "state": self.state.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "last_error": self.last_error,
            "attempts": self.attempts,
            "transferred": self.transferred,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferHandle":
        """Deserialize a dictionary to a TransferHandle."""
        return cls(
            range=RangeTransfer.from_dict(data["range"]),
            state=TransferState(data["state"]),
            started_at=(
                datetime.fromisoformat(data["started_at"])
                if data.get("started_at")
                else None
            ),
            finished_at=(
                datetime.fromisoformat(data["finished_at"])
                if data.get("finished_at")
                else None
            ),
            last_error=data.get("last_error"),
            attempts=int(data.get("attempts") or 0),
            transferred=int(data.get("transferred") or 0),
        )


@dataclass(slots=True, frozen=True)
class TransferClaim:
    """Request one window of a range from its source (pull).

    The source sends the transferable records of the segment in TAGS key
    order (pid, ts, address) and skips:

    - for every pid of `resumes`, the keys lower than or equal to its
      resume key (the last key already staged by the destination);
    - every key lower than or equal to `after` (the last key received
      during the current attempt).

    `window` bounds the bytes of record frames sent: once reached, the
    source ends the window at a record boundary with `more=True`.
    """

    serial: str
    resumes: Mapping[int, Key]
    range: RangeTransfer
    after: Key | None = None
    window: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferClaim to a dictionary."""
        return {
            "serial": self.serial,
            "resumes": _resumes_to_dict(self.resumes),
            "range": self.range.to_dict(),
            "after": self.after.to_dict() if self.after is not None else None,
            "window": self.window,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferClaim":
        """Deserialize a dictionary to a TransferClaim."""
        after = data.get("after")
        return cls(
            serial=data["serial"],
            resumes=_resumes_from_dict(data["resumes"]),
            range=RangeTransfer.from_dict(data["range"]),
            after=Key.from_dict(after) if after is not None else None,
            window=data.get("window"),
        )


@dataclass(slots=True, frozen=True)
class TransferInit:
    serial: str
    range: RangeTransfer

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferInit to a dictionary."""
        return {
            "serial": self.serial,
            "range": self.range.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferInit":
        """Deserialize a dictionary to a TransferInit."""
        return cls(
            serial=data["serial"],
            range=RangeTransfer.from_dict(data["range"]),
        )


@dataclass(frozen=True, slots=True)
class TransferInitAccept:
    """Open push session: where to resume and the session the chunks belong to."""

    resumes: Mapping[int, Key]
    session: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferInitAccept to a dictionary."""
        return {
            "resumes": _resumes_to_dict(self.resumes),
            "session": self.session,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferInitAccept":
        """Deserialize a dictionary to a TransferInitAccept."""
        return cls(
            resumes=_resumes_from_dict(data["resumes"]),
            session=str(data.get("session") or ""),
        )


@dataclass(slots=True, frozen=True)
class TransferChunk:
    """One piece of an ordered stream of length-prefixed record frames.

    `seq` numbers the chunks of a stream from 0 so that the receiver detects
    any gap or reordering. The stream ends with `last=True`; `more=True` on
    that chunk means the window was full and the range is not exhausted.
    `session` ties pushed chunks to their push session.
    """

    range: RangeTransfer
    raw: bytes
    last: bool = False
    seq: int = 0
    more: bool = False
    session: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferChunk to a dictionary."""
        return {
            "range": self.range.to_dict(),
            "raw": self.raw,
            "last": self.last,
            "seq": self.seq,
            "more": self.more,
            "session": self.session,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferChunk":
        """Deserialize a dictionary to a TransferChunk."""
        return cls(
            range=RangeTransfer.from_dict(data["range"]),
            raw=data["raw"],
            last=bool(data.get("last")),
            seq=int(data.get("seq") or 0),
            more=bool(data.get("more")),
            session=str(data.get("session") or ""),
        )


@dataclass(slots=True, frozen=True)
class TransferCommit:
    """Ask the destination to activate a range fully pushed in `session`."""

    range: RangeTransfer
    session: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferCommit to a dictionary."""
        return {
            "range": self.range.to_dict(),
            "session": self.session,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransferCommit":
        """Deserialize a dictionary to a TransferCommit."""
        return cls(
            range=RangeTransfer.from_dict(data["range"]),
            session=str(data.get("session") or ""),
        )


@dataclass(slots=True, frozen=True)
class TransferReject:
    """Typed refusal of a transfer request.

    A non fatal rejection (serial mismatch, unknown session, out of order
    chunk...) is retried by the requester; a fatal one reports an
    inconsistency that no retry can fix.
    """

    reason: str
    fatal: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize the TransferReject to a dictionary."""
        return {"reason": self.reason, "fatal": self.fatal}

    @classmethod
    def from_dict(cls, data: Any) -> "TransferReject":
        """Deserialize a TransferReject, tolerating missing or malformed fields."""
        if not isinstance(data, Mapping):
            data = {}
        return cls(
            reason=str(data.get("reason") or "unknown"),
            fatal=bool(data.get("fatal")),
        )
