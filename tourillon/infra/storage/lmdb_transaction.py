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

from collections.abc import Callable

import lmdb

from tourillon.core.models.storageschema import Keyspace, KVPair, KVRef
from tourlib.models.types import SupportsBytes


class LmdbTransaction:
    def __init__(
        self,
        txn: lmdb.Transaction,
        dbi_logs: lmdb._Database,
        dbi_tags: lmdb._Database,
        dbi_meta: lmdb._Database,
    ) -> None:
        self._txn = txn
        self._dbis = {
            Keyspace.LOGS: dbi_logs,
            Keyspace.TAGS: dbi_tags,
            Keyspace.META: dbi_meta,
        }

    def get(self, ref: KVRef) -> KVPair | None:
        dbi = self._dbis[ref.keyspace]
        value = self._txn.get(ref.key, db=dbi)
        if value is None:
            return None
        return KVPair(ref.key, bytes(value), ref.keyspace)

    def exists(self, ref: KVRef) -> bool:
        with self._txn.cursor(db=self._dbis[ref.keyspace]) as cursor:
            return cursor.set_key(ref.key)

    def put(self, kv: KVPair, overwrite: bool = True) -> bool:
        key = kv.key
        value = kv.value
        dbi = self._dbis[kv.keyspace]
        return self._txn.put(key, value, db=dbi, overwrite=overwrite)

    def delete(self, ref: KVRef) -> None:
        key = ref.key
        dbi = self._dbis[ref.keyspace]
        self._txn.delete(key, db=dbi)

    def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None,
        predicate: Callable[[KVPair], bool] | None,
    ) -> KVPair | None:
        dbi = self._dbis[keyspace]
        cursor = self._txn.cursor(db=dbi)
        prefix_bytes = prefix.to_bytes() if prefix is not None else None

        if prefix_bytes is not None and not self._cursor_range(
            cursor, prefix_bytes, reverse=True
        ):
            return None

        while True:
            encoded_key = bytes(cursor.key())
            if prefix_bytes is not None and not encoded_key.startswith(prefix_bytes):
                return None

            kv = KVPair(encoded_key, bytes(cursor.value()), keyspace)
            if predicate is None or predicate(kv):
                return kv

            if not self._advance_cursor(cursor, reverse=True):
                return None

    def list(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> list[KVPair]:
        if limit is not None and limit <= 0:
            return []

        dbi = self._dbis[keyspace]
        cursor = self._txn.cursor(db=dbi)
        prefix_bytes = prefix.to_bytes() if prefix is not None else None

        if not self._position_cursor(cursor, prefix_bytes, start, reverse):
            return []

        items: list[KVPair] = []
        while True:
            key = bytes(cursor.key())
            if prefix_bytes is not None and not key.startswith(prefix_bytes):
                break

            kv = KVPair(key, bytes(cursor.value()), keyspace)
            items.append(kv)

            if limit is not None and len(items) >= limit:
                break

            if not self._advance_cursor(cursor, reverse):
                break

        return items

    @staticmethod
    def _advance_cursor(cursor: lmdb.Cursor, reverse: bool) -> bool:
        if reverse:
            return cursor.prev()
        else:
            return cursor.next()

    @staticmethod
    def _cursor_range(cursor: lmdb.Cursor, key: bytes, reverse: bool) -> bool:
        """Position cursor at or near the given key range.

        For reverse iteration, finds the last key >= key by seeking to key+0xFF
        and stepping back. For forward iteration, seeks to key and advances past
        exact matches to start with keys strictly greater than key.
        """
        if reverse:
            key_end = key + b"\xff"
            if cursor.set_range(key_end):
                return cursor.prev()
            return cursor.last()
        else:
            if not cursor.set_range(key):
                return False

            if bytes(cursor.key()) == key:
                return cursor.next()
            return True

    def _position_cursor(
        self,
        cursor: lmdb.Cursor,
        prefix: bytes | None,
        start: bytes | None,
        reverse: bool,
    ) -> bool:
        if start is not None:
            return self._cursor_range(cursor, start, reverse)
        if prefix is not None:
            return self._cursor_range(cursor, prefix, reverse)

        if reverse:
            return cursor.last()
        else:
            return cursor.first()
