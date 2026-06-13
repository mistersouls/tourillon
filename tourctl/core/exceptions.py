class NodeClientError(Exception):
    """User-facing failure returned by tourctl node client services."""

    def __init__(self, message: str, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code

