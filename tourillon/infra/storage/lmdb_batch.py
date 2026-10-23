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

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from tourillon.core.models.storageschema import Keyspace, KVPair, KVRef
from tourillon.core.ports.storage import Batch
from tourillon.infra.storage.lmdb_transaction import LmdbTransaction
from tourlib.contrib.threadpool import AsyncExecutor
from tourlib.models.types import SupportsBytes


@dataclass(slots=True)
class BatchItem:
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: Mapping[str, Any]


class LmdbBatch:
    def __init__(
        self,
        txn: LmdbTransaction,
        executor: AsyncExecutor,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as exc:
                raise RuntimeError(
                    "No running event loop found. Please provide an event loop."
                ) from exc

        self._txn = txn
        self._executor = executor
        self._loop = loop

        self._result_future = self._loop.create_future()
        self._items: list[BatchItem] = []

    async def __aenter__(self) -> Batch:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            # Propagate exception
            if not self._result_future.done():
                self._result_future.set_exception(exc_val)
            return False

        # Flush everything in one executor job
        await self._executor.execute(self._flush)
        return True

    async def result[T](self) -> T:
        return await self._result_future

    def get(self, ref: KVRef) -> None:
        item = BatchItem(
            fn=self._txn.get,
            args=(ref,),
            kwargs={},
        )
        self._items.append(item)

    def exists(self, ref: KVRef) -> None:
        item = BatchItem(fn=self._txn.exists, args=(ref,), kwargs={})
        self._items.append(item)

    def put(self, kv: KVPair, overwrite: bool = True) -> None:
        item = BatchItem(
            fn=self._txn.put,
            args=(kv,),
            kwargs={"overwrite": overwrite},
        )
        self._items.append(item)

    def delete(self, ref: KVRef) -> None:
        item = BatchItem(
            fn=self._txn.delete,
            args=(ref,),
            kwargs={},
        )
        self._items.append(item)

    def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> None:
        item = BatchItem(
            fn=self._txn.last,
            args=(keyspace, prefix, predicate),
            kwargs={},
        )
        self._items.append(item)

    def list(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> None:
        item = BatchItem(
            fn=self._txn.list,
            args=(keyspace, prefix, start, limit, reverse),
            kwargs={},
        )
        self._items.append(item)

    def _flush(self) -> None:
        results = []
        try:
            for item in self._items:
                result = item.fn(*item.args, **item.kwargs)
                results.append(result)
        except Exception as exc:
            self._loop.call_soon_threadsafe(self._result_future.set_exception, exc)
        else:
            self._loop.call_soon_threadsafe(self._result_future.set_result, results)

        self._items.clear()
