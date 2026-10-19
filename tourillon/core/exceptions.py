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

from collections.abc import Sequence
from typing import Any

from tourlib.constant import MAX_QUORUM


class BootstrapPartitionShiftError(Exception):
    """
    Raised when there is a partition shift mismatch  during
    the bootstrap process of the node.
    """


class BootstrapAttemptError(Exception):
    """
    Raised when there is an error during
    the bootstrap attempt of the node.
    """


class BootstrapError(Exception):
    """
    Raised when there is an error during
    the bootstrap process of the node.
    """


class StartupError(Exception):
    """
    Raised when there is an error during
    the startup of the node.
    """


class JoinError(Exception):
    """
    Raised when there is an error during
    the join process of the node.
    """

    def __init__(self, error: str) -> None:
        super().__init__(error)
        self.error = error


class LeaveError(Exception):
    """Raised when a node cannot begin or complete a graceful departure."""

    def __init__(self, error: str) -> None:
        super().__init__(error)
        self.error = error


class StoreOperationError(Exception):
    """Raised when a distributed store operation fails across one or more peers."""

    code = "store_operation_failed"

    def __init__(
        self, operation: str, errors: Sequence[Exception], message: str | None = None
    ) -> None:
        if not errors:
            raise ValueError(
                "StoreOperationError requires at least one underlying exception"
            )
        self._set_details(
            operation, errors, message or f"{operation} replication failed"
        )

    def _set_details(
        self, operation: str, errors: Sequence[Exception], message: str
    ) -> None:
        Exception.__init__(self, message)
        self.operation = operation
        self.errors = tuple(errors)
        self.message = message

    def __str__(self) -> str:
        details = "; ".join(f"{type(error).__name__}: {error}" for error in self.errors)
        return f"{self.message} - details: {details}" if details else self.message

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "code": self.code,
            "message": self.message,
            "errors": [
                {
                    "type": type(error).__name__,
                    "message": str(error),
                }
                for error in self.errors
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StoreOperationError":
        operation = data.get("operation", "unknown")
        errors_data = data.get("errors", [])
        errors = []
        for error_data in errors_data:
            error_type = error_data.get("type", "Exception")
            error_message = error_data.get("message", "")
            # Create a generic Exception with the message; we can't reconstruct the exact type
            errors.append(Exception(f"{error_type}: {error_message}"))
        message = data.get("message", f"{operation} replication failed")
        return cls(operation=operation, errors=errors, message=message)


class NodeDrainingError(StoreOperationError):
    """Reject new work after an explicit departure has begun."""

    code = "node_draining"

    def __init__(self, operation: str) -> None:
        self._set_details(operation, (), f"{operation}: node is draining or idle")


class QuorumNotMetError(StoreOperationError):
    """Raised when fewer replicas than required agreed on the operation outcome.

    Underlying errors are optional: replicas may all have answered and still
    disagree, or some of them may not have answered in time.

    A write carries the handle of the version it produced. The write is
    persisted on the replicas that did accept it, so the failure is partial by
    nature: the handle is what lets the client finish the job with `kv.repair`
    instead of writing a second version.
    """

    code = "quorum_not_met"

    def __init__(
        self,
        operation: str,
        required: int,
        received: int,
        errors: Sequence[Exception] = (),
        token: str | None = None,
    ) -> None:
        self.required = required
        self.received = received
        self.token = token
        message = (
            f"{operation} quorum not met: successes={received}, required={required}"
        )
        self._set_details(operation, errors, message)

    def to_dict(self) -> dict[str, Any]:
        data = {
            **super().to_dict(),
            "required": self.required,
            "received": self.received,
        }
        if self.token is not None:
            data["token"] = self.token
        return data


class NoReplicaAvailableError(StoreOperationError):
    """Raised when the preference list offers no replica to contact."""

    code = "no_replica_available"

    def __init__(self, operation: str) -> None:
        self._set_details(
            operation, (), f"{operation}: preference list has no replication target"
        )


class InvalidQuorumError(StoreOperationError):
    """Raised when a request asks for a quorum the store cannot record.

    The requested durability is persisted with the version, in a single byte,
    so a quorum outside that range has no storable meaning and is refused
    before anything is written.
    """

    code = "invalid_quorum"

    def __init__(self, operation: str, quorum: int) -> None:
        self.quorum = quorum
        self._set_details(
            operation,
            (),
            f"{operation}: quorum {quorum} is out of range [1, {MAX_QUORUM}]",
        )

    def to_dict(self) -> dict[str, Any]:
        return {**super().to_dict(), "quorum": self.quorum}


class InvalidRepairTokenError(StoreOperationError):
    """Raised when a repair handle cannot designate a version of this store.

    The handle is unsigned on purpose, so a fabricated one must be caught by
    what it says rather than by who sent it: a malformed encoding, a timestamp
    no clock here could have emitted, or a partition id that disagrees with
    the address it is paired with. That last case is the dangerous one — a
    record filed under a foreign partition id lands where reads for its
    address never look.
    """

    code = "invalid_repair_token"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        self._set_details("repair", (), f"repair: invalid token: {reason}")

    def to_dict(self) -> dict[str, Any]:
        return {**super().to_dict(), "reason": self.reason}


class VersionNotFoundError(StoreOperationError):
    """Raised when no replica holds the version a repair was asked to spread.

    Distinct from a met quorum with nothing to do: here there is nothing to
    copy, and repair never creates a version it did not find.
    """

    code = "version_not_found"

    def __init__(self, token: str) -> None:
        self.token = token
        self._set_details("repair", (), "repair: no replica holds this version")

    def to_dict(self) -> dict[str, Any]:
        return {**super().to_dict(), "token": self.token}


class ProcessError(Exception):
    """
    Halt the process due to a critical error that cannot be recovered from.
    """


class TransferError(Exception):
    """
    Raised when there is an error during
    the transfer process of the node.
    """


class DataStorageError(Exception):
    """
    Raised when there is an error during
    the data storage process of the node.
    """
