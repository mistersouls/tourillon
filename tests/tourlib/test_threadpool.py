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
import threading

import pytest

from tourlib.contrib.threadpool import AsyncExecutor, AsyncThreadPool


@pytest.mark.tourlib
def test_async_executor_requires_running_loop() -> None:
	with pytest.raises(RuntimeError, match="running event loop"):
		AsyncExecutor()


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_async_executor_execute_returns_result_and_exception() -> None:
	executor = AsyncExecutor()

	try:
		result = await executor.execute(lambda a, b: a + b, 2, 3)
		assert result == 5

		with pytest.raises(ValueError, match="boom"):
			await executor.execute(
				lambda: (_ for _ in ()).throw(ValueError("boom"))
			)
	finally:
		executor.shutdown()


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_async_executor_shutdown_cancels_queued_futures_and_rejects_new_work() -> None:
	executor = AsyncExecutor()
	started = threading.Event()
	release = threading.Event()

	def blocking() -> str:
		started.set()
		release.wait(timeout=2)
		return "first"

	first = executor.execute(blocking)
	while not started.is_set():
		await asyncio.sleep(0)

	second = executor.execute(lambda: "second")
	executor.shutdown(wait=False, cancel_futures=True)
	release.set()

	assert await first == "first"
	with pytest.raises(asyncio.CancelledError):
		await second

	executor.shutdown(wait=True)
	with pytest.raises(RuntimeError, match="shutdown"):
		executor.execute(lambda: None)


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_async_executor_shutdown_is_idempotent() -> None:
	executor = AsyncExecutor()

	try:
		assert await executor.execute(lambda: "ok") == "ok"
	finally:
		executor.shutdown()
		executor.shutdown()


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_async_thread_pool_reuses_free_executor() -> None:
	pool = AsyncThreadPool(max_workers=1)

	async with pool.executor() as first:
		assert await first.execute(lambda: 1) == 1

	async with pool.executor() as second:
		assert second is first
		assert await second.execute(lambda: 2) == 2

	await pool.close()


@pytest.mark.tourlib
@pytest.mark.asyncio
async def test_async_thread_pool_close_waits_for_busy_executor_and_then_rejects_new_contexts() -> None:
	pool = AsyncThreadPool(max_workers=1)
	entered = asyncio.Event()
	release = asyncio.Event()
	seen = []

	async def hold_executor() -> None:
		async with pool.executor() as executor:
			seen.append(executor)
			entered.set()
			await release.wait()

	holder = asyncio.create_task(hold_executor())
	await entered.wait()

	close_task = asyncio.create_task(pool.close())
	await asyncio.sleep(0)
	assert not close_task.done()

	release.set()
	await holder
	await close_task

	assert seen
	assert pool.free == set()
	assert pool.busy == set()

	with pytest.raises(RuntimeError, match="closed"):
		async with pool.executor():
			raise AssertionError("unreachable")

