from dataclasses import dataclass
from typing import Any

from tourillon.core.ae.view import VIEW_BYTES
from tourlib.constant import MAX_ENVELOPE_PAYLOAD

MAX_PIDS_PER_REQUEST = 1 << 17
MAX_KEYS_PER_REQUEST = 1 << 14
MAX_PAGE_LIMIT = 1 << 14
MAX_BATCH_BYTES = MAX_ENVELOPE_PAYLOAD - 1024 * 1024

BUCKET_SIZE: int  = 8
DIGEST_SIZE: int = 16
_CELL_BYTES: int = DIGEST_SIZE + BUCKET_SIZE


@dataclass(frozen=True, slots=True)
class Scope:
    pids: tuple[int, ...]
    view: bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "pids": list(self.pids),
            "view": self.view,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Scope":
        pids = _list(data, "pids", MAX_PIDS_PER_REQUEST)
        if not pids or any(isinstance(p, bool) or not isinstance(p, int) or p < 0 for p in pids):
            raise ValueError("pids must be a non-empty list of integers >= 0")

        return cls(pids=tuple(pids), view=_bytes(data["view"], "view", VIEW_BYTES))


@dataclass(frozen=True, slots=True)
class RootsRequest:
    scope: Scope
    cells: tuple[bytes, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"scope": self.scope.to_dict(), "cells": list(self.cells)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootsRequest":
        scope = Scope.from_dict(data["scope"])
        cells = _list(data, "cells", MAX_PIDS_PER_REQUEST)

        if len(cells) != len(scope.pids):
            raise ValueError("cells length must match pids one to one")

        return cls(scope, tuple(_bytes(c, "cell", _CELL_BYTES) for c in cells))


@dataclass(frozen=True, slots=True)
class RootsResponse:
    diffs: tuple[tuple[int, bytes], ...]

    def to_dict(self) -> dict[str, Any]:
        return {"diffs": [[pid, cell] for pid, cell in self.diffs]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootsResponse":
        diffs = _list(data, "diffs", MAX_PIDS_PER_REQUEST)
        return cls(tuple(_pair(entry) for entry in diffs))


def _list(data: dict[str, Any], name: str, limit: int) -> list[Any]:
    value = data[name]

    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")

    if len(value) > limit:
        raise ValueError(f"{name} holds more than {limit} entries")

    return value


def _bytes(value: Any, name: str, size: int | None = None) -> bytes:
    if not isinstance(value, bytes):
        raise ValueError(f"{name} must be bytes")

    if size is not None and len(value) != size:
        raise ValueError(f"{name} must be {size} bytes")

    return value


def _pair(entry: Any) -> tuple[int, bytes]:
    if not isinstance(entry, list) or len(entry) != 2:
        raise ValueError("entry must be a [number, cell] pair")

    number, cell = entry
    if isinstance(number, bool) or not isinstance(number, int) or number < 0:
        raise ValueError("entry number must be an integer >= 0")

    return number, _bytes(cell, "cell", _CELL_BYTES)
