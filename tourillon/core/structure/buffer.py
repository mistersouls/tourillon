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

"""BytesWalker — stateful cursor for decoding binary buffers."""


class BytesWalker:
    """Stateful read cursor over an immutable byte buffer.

    Advances an internal position on every read. All methods raise
    ``ValueError`` when the remaining buffer is too short for the requested
    read, so callers never receive a silently truncated slice.

    Typical usage::

        w = BytesWalker(data)
        pid = w.read_int(4, "pid")
        hlc = HLCTimestamp.from_bytes(w)
        keyspace = w.read_field("keyspace")
        key = w.read_field("key")
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos: int = 0

    def read(self, n: int, label: str) -> bytes:
        """Return the next *n* bytes and advance the cursor.

        Raise ``ValueError`` when fewer than *n* bytes remain.
        """
        if len(self._data) < self._pos + n:
            raise ValueError(
                f"Buffer too short for {label}: "
                f"need {self._pos + n} bytes, got {len(self._data)}"
            )
        chunk = self._data[self._pos : self._pos + n]
        self._pos += n
        return chunk

    def read_int(self, n: int, label: str) -> int:
        """Read *n* bytes as a big-endian unsigned integer and advance.

        Raise ``ValueError`` when fewer than *n* bytes remain.
        """
        return int.from_bytes(self.read(n, label), "big")

    def read_field(self, label: str, len_size: int = 2) -> bytes:
        """Read a length-prefixed field and return its payload.

        Reads *len_size* bytes (default 2) as a big-endian unsigned integer to
        obtain the payload length, then reads that many bytes and advances past
        both.  Raise ``ValueError`` when the buffer is too short for either part.
        """
        length = self.read_int(len_size, f"{label} length prefix")
        return self.read(length, label)
