import contextlib
import os
import stat
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import tomli_w

from tourillon.core.exceptions import ConfigError


class TomlConfigReadWriter:
    @staticmethod
    def read(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ConfigError(f"config file not found: {path}")
        try:
            return tomllib.loads(path.read_text())
        except Exception as e:
            raise ConfigError(f"Failed to parse TOML file {path}: {e}") from e

    @staticmethod
    def write(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = tomli_w.dumps(payload).encode("utf-8")

        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            os.write(fd, data)
            os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR)
        finally:
            os.close(fd)

        try:
            os.replace(tmp_path, path)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
