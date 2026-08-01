import asyncio
from collections.abc import AsyncGenerator

from tourlib.exceptions import ClosedError


class Subscription[T]:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.waiter = loop.create_future()

    def publish(self, value: T) -> None:
        waiter = self.waiter
        if waiter.done():
            raise ClosedError("subscription closed.")

        waiter = self.waiter
        self.waiter = self._loop.create_future()
        waiter.set_result((value, self.waiter))

    async def subscribe(self) -> AsyncGenerator[T]:
        waiter = self.waiter
        while True:
            try:
                value, waiter = await asyncio.shield(waiter)
                yield value
            except StopAsyncIteration:
                return

    def close(self):
        if not self.waiter.done():
            self.waiter.set_exception(StopAsyncIteration())

    __aiter__ = subscribe
