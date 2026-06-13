from dataclasses import dataclass


@dataclass(frozen=True)
class Backoff:
    initial: float = 1.0
    max_retries: int = 10
    step: float = 2.0
    max_interval: float = 30
