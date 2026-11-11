import hashlib
from dataclasses import dataclass

from tourillon.core.models.storageschema import Pid, Key, Keyspace, KVPair, MetaKind

_ROOT = b"\x00"
_BUCKET = b"\x01"
_COUNT_SIZE: int = 8
ROOT_PREFIX: bytes = MetaKind.DIGEST.value + _ROOT  # type: ignore
BUCKET_PREFIX: bytes = MetaKind.DIGEST.value + _BUCKET  # type: ignore
BUCKET_SIZE: int  = 8
DIGEST_SIZE: int = 16



@dataclass(frozen=True, slots=True, eq=True, order=True)
class Root:
    pid: int

    def to_bytes(self) -> bytes:
        """Convert the Root instance to bytes."""
        return ROOT_PREFIX + Pid(self.pid).to_bytes()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Root":
        """Create a Root instance from bytes."""
        if not data.startswith(ROOT_PREFIX):
            raise ValueError("Invalid root prefix")
        pid = Pid.from_bytes(data[len(ROOT_PREFIX):])
        return cls(pid=pid.value)


@dataclass(frozen=True, slots=True, eq=True, order=True)
class Bucket:
    id: int
    pid: int

    def to_bytes(self) -> bytes:
        """Convert the Bucket instance to bytes."""
        return BUCKET_PREFIX + Pid(self.pid).to_bytes() + self.id.to_bytes(BUCKET_SIZE, "big")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Bucket":
        """Create a Bucket instance from bytes."""
        if not data.startswith(BUCKET_PREFIX):
            raise ValueError("Invalid bucket prefix")
        pid = Pid.from_bytes(data[len(BUCKET_PREFIX):len(BUCKET_PREFIX) + 8])
        bucket_id = int.from_bytes(data[len(BUCKET_PREFIX) + 8:], "big")
        return cls(id=bucket_id, pid=pid.value)

    def bounds(self, shift: int) -> tuple[bytes, bytes]:
        """Return the TAGS ``(start, stop)`` bounds, both exclusive, of a bucket.

            TAGS keys are ``pid | wall_ms | ...``, so a bucket is a contiguous range.
            No real key equals the 12-byte ``pid | lo`` start, so the exclusive cursor
            skips nothing.
            """
        pid_bytes = Pid(self.pid).to_bytes()
        lo = self.id << shift
        hi = (self.id + 1) << shift

        return pid_bytes + lo.to_bytes(BUCKET_SIZE, "big"), pid_bytes + hi.to_bytes(BUCKET_SIZE, "big")


@dataclass(frozen=True, slots=True, order=True)
class DigestCell:
    """Order-independent fingerprint of a set of version keys."""

    count: int = 0
    digest: int = 0

    @classmethod
    def of(cls, key: Key) -> "DigestCell":
        hasher = hashlib.blake2b(key.to_bytes(Keyspace.TAGS), digest_size=DIGEST_SIZE)
        return cls(count=1, digest=int.from_bytes(hasher.digest(), "big"))

    def __add__(self, other: "DigestCell") -> "DigestCell":
        return DigestCell(self.count + other.count, self.digest ^ other.digest)

    def __sub__(self, other: "DigestCell") -> "DigestCell":
        return DigestCell(self.count - other.count, self.digest ^ other.digest)

    @property
    def empty(self) -> bool:
        return self.count == 0 and self.digest == 0

    def to_bytes(self) -> bytes:
        return self.count.to_bytes(_COUNT_SIZE, "big") + self.digest.to_bytes(DIGEST_SIZE, "big")

    @classmethod
    def from_bytes(cls, b: bytes) -> "DigestCell":
        total_size = _COUNT_SIZE + DIGEST_SIZE
        if len(b) != total_size:
            raise ValueError(f"digest cell must be {total_size} bytes, got {len(b)}")

        return cls(
            count=int.from_bytes(b[:_COUNT_SIZE], "big"),
            digest=int.from_bytes(b[_COUNT_SIZE:], "big"),
        )


def cell_keys(key: Key, bucket_shift: int) -> tuple[bytes, bytes]:
    """Return the root and bucket keys for a given key."""
    pid = key.pid.value
    root = Root(pid)
    bucket = Bucket(id=key.ts.wall_ms >> bucket_shift, pid=pid)
    return root.to_bytes(), bucket.to_bytes()

