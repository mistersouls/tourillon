from __future__ import annotations

from typing import Any


class NodeStartError(Exception):
    """Fatal startup error mapped to a process exit code by the CLI layer."""

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class BootstrapError(Exception):
    exit_code: int

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class StateError(Exception):
    """Raised by StatePersistence implementations on I/O or decode failure."""


class NodeIdMismatchError(Exception):
    """Raised when config and persisted node IDs differ."""


class ConfigError(Exception):
    """Fatal configuration error from duration/bytes parsing."""


class BootstrapAttemptError(Exception):
    """Raised when a single bootstrap attempt fails (all seeds unreachable)."""


class BootstrapPartitionShiftError(Exception):
    """Raised when a seed delta contains a Member with a mismatched partition_shift.

    Never retried. Propagates immediately out of GossipBootstrapper.run so the
    daemon can exit with a clear diagnostic rather than exhausting retries.
    Seeds and local values are included for logging.
    """

    def __init__(self, seed: str, seed_shift: int, local_shift: int) -> None:
        super().__init__(
            f"partition_shift mismatch from seed {seed}: "
            f"seed={seed_shift} local={local_shift}"
        )
        self.seed = seed
        self.seed_shift = seed_shift
        self.local_shift = local_shift


class GossipError(Exception):
    """Fatal Gossip error."""
    def __init__(self, kind: str, payload: dict[str, Any]) -> None:
        self.kind = kind
        self.payload = payload


class JoinError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message


class ProcessError(Exception):
    """Non-retryable protocol-level error"""


class RebalanceError(Exception):
    def __init__(self, reason: str, data: dict[str, Any] | None = None) -> None:
        self.reason = reason
        self.data = data or {}
