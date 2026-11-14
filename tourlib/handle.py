from typing import Any, Protocol, Self

from tourlib import Serializer
from tourlib.envelope import Envelope, replay_envelope


class MalformedError(Exception):
    """Raised when a payload is malformed or cannot be parsed."""


class DictPayload(Protocol):
    def to_dict(self) -> dict[str, Any]:
        """Convert the payload to a dictionary representation."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a payload instance from a dictionary representation."""


class DictReplier:
    def __init__(self, serializer: Serializer, request: Envelope) -> None:
        self._serializer = serializer
        self._request = request

    def decode[T](self, model: type[T]) -> T:
        try:
            data = self._serializer.decode(self._request.payload)

            if not isinstance(data, dict):
                raise TypeError("payload must be a mapping")

            return model.from_dict(data)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise MalformedError(
                f"malformed {self._request.kind} payload: {exc}"
            ) from exc

    def reply(self, kind: str, data: DictPayload) -> Envelope:
        """Create a reply envelope with the given payload."""
        return replay_envelope(
            kind=kind,
            request=self._request,
            payload=self._serializer.encode(data.to_dict())
        )


def reply(
    serializer: Serializer,
    request: Envelope,
    kind: str,
    model: DictPayload
) -> Envelope:
    return Envelope(
        proto_version=request.proto_version,
        schema_id=request.schema_id,
        mode=request.mode,
        correlation_id=request.correlation_id,
        kind=kind,
        payload=serializer.encode(model.to_dict()),
    )
