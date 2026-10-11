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
import queue
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncGenerator
from concurrent.futures import ThreadPoolExecutor


@dataclass(slots=True)
class WorkItem:
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[Any]
    task: tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]


class AsyncExecutor:
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError("AsyncExecutor must be created in a running event loop")

        self._loop = loop
        self._queue: queue.SimpleQueue[WorkItem | None] = queue.SimpleQueue()
        self._shutdown = False
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def execute[T](self, fn, /, *args, **kwargs) -> asyncio.Future[T]:
        if self._shutdown:
            raise RuntimeError("Executor shutdown")

        fut = self._loop.create_future()
        item = WorkItem(self._loop, fut, (fn, args, kwargs))
        self._queue.put(item)
        return fut

    def shutdown(self, wait=True, *, cancel_futures=False) -> None:
        """Stop the worker thread and optionally cancel queued futures.

        This method is primarily lifecycle plumbing for ``AsyncThreadPool.close()``.
        It is not intended to run concurrently with new ``execute()`` submissions.

        Args:
            wait: When True, block the caller until the worker thread exits.
                Because this calls ``thread.join()``, it can block for the duration
                of the currently running task.
            cancel_futures: When True, cancel work items that are still queued and
                have not started yet. The task currently running on the worker is
                never interrupted.
        """
        if self._shutdown:
            if wait and self._thread.is_alive():
                self._thread.join()
            return

        self._shutdown = True

        if cancel_futures:
            while True:
                try:
                    item = self._queue.get_nowait()
                    if item is not None:
                        self._submit(item.loop, self._safe_cancel, item.future)
                except queue.Empty:
                    break

        self._queue.put(None)
        if wait:
            self._thread.join()

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return

            fut = item.future
            if fut.cancelled():
                continue

            loop = item.loop
            fn, args, kwargs = item.task

            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                self._submit(loop, self._safe_set_exception, fut, exc)
            else:
                self._submit(loop, self._safe_set_result, fut, result)

    @staticmethod
    def _submit(
        loop: asyncio.AbstractEventLoop,
        callback: Callable[..., None],
        *args: Any,
    ) -> None:
        try:
            loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            return

    @staticmethod
    def _safe_set_result[T](fut: asyncio.Future[T], result: T) -> None:
        if not fut.done():
            fut.set_result(result)

    @staticmethod
    def _safe_set_exception(fut: asyncio.Future[Any], exc: Exception) -> None:
        if not fut.done():
            fut.set_exception(exc)

    @staticmethod
    def _safe_cancel(fut: asyncio.Future[Any]) -> None:
        if not fut.done():
            fut.cancel()


class AsyncThreadPool:
    def __init__(self, max_workers: int = 4):
        self._max_workers = max_workers
        self._sema = asyncio.Semaphore(max_workers)
        self._closed = False
        self.free = set()
        self.busy = set()

    @asynccontextmanager
    async def executor(self) -> AsyncGenerator[AsyncExecutor, None]:
        if self._closed:
            raise RuntimeError("Thread pool closed")

        await self._sema.acquire()

        if self._closed:
            self._sema.release()
            raise RuntimeError("Thread pool closed")

        if self.free:
            executor = self.free.pop()
        else:
            executor = AsyncExecutor()

        self.busy.add(executor)

        try:
            yield executor
        finally:
            self.busy.remove(executor)
            if self._closed:
                await asyncio.to_thread(executor.shutdown)
            else:
                self.free.add(executor)
            self._sema.release()

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        for _ in range(self._max_workers):
            await self._sema.acquire()

        for executor in self.free:
            await asyncio.to_thread(executor.shutdown)
        self.free.clear()

        for executor in self.busy:
            await asyncio.to_thread(executor.shutdown)
        self.busy.clear()
