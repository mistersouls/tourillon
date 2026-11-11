import asyncio
from typing import Any, Protocol


class Resolver(Protocol):
    async def resolve(self) -> Any:
        """Resolve all pending operations in the transaction."""


class Result[T]:
    def __init__(
        self,
        resolver: Resolver,
        future: asyncio.Future[T],
    ) -> None:
        self._resolver = resolver
        self._future = future

    async def get(self) -> T:
        return await self._resolve()

    async def _resolve(self) -> T:
        if not self._future.done():
            await self._resolver.resolve()
        return await self._future
