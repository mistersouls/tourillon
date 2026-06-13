from pathlib import Path
from typing import Any, Protocol


class ConfigReadWriter(Protocol):
    def read(self, path: Path) -> dict[str, Any]:
        """Read the node config file at path"""

    def write(self, path: Path, payload: dict[str, Any]) -> None:
        """Write the node config file at path"""
