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

import os
from pathlib import Path

from tourlib.client_config import ClientConfig
from tourlib.port import Persistence

ENV_TOURCONF = "TOURCONF"


def default_tourconf_path() -> Path:
    """Return the platform-correct fallback tourconf path under the user's home."""
    return Path.home() / ".tourillon" / "tourconf.toml"


def resolve_tourconf_path(tourconf: str | Path | None = None) -> Path:
    """Resolve the client config path from an explicit path, env, or fallback file."""
    if tourconf is not None:
        return Path(tourconf).expanduser()

    env_tourconf = os.environ.get(ENV_TOURCONF)
    if env_tourconf:
        return Path(env_tourconf).expanduser()

    fallback = default_tourconf_path()
    if fallback.is_file():
        return fallback

    raise FileNotFoundError(
        "tourconf not found: provide `tourconf`, set the environment variable "
        f"{ENV_TOURCONF}, or create {fallback}"
    )


async def load_config(
    tourconf: str | Path | None = None,
    *,
    persistence: Persistence,
) -> ClientConfig:
    """Load a `ClientConfig` using Persistence with tourconf.toml resolution rules."""
    path = resolve_tourconf_path(tourconf)
    if not path.is_file():
        fallback = default_tourconf_path()
        raise FileNotFoundError(
            "tourconf file not found: "
            f"{path}. Provide `tourconf`, set {ENV_TOURCONF}, or create {fallback}"
        )

    document = await persistence.load(path)
    return ClientConfig.from_document(document)

