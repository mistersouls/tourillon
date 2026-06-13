from tourillon.core.machinery.state import StatePersistence


class NodeDrainer:
    def __init__(self, state: StatePersistence) -> None:
        self._state = state

    async def drain(self) -> None:
        ...

