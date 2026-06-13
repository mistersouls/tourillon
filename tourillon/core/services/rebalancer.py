from tourillon.core.machinery.state import StatePersistence


class NodeRebalancer:
    def __init__(self, state: StatePersistence) -> None:
        self._state = state

    async def rebalance(self) -> None:
        ...
