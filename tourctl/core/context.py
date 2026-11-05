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

from pathlib import Path

from tourclient.core.helpers import load_config
from tourlib.adapter import TomlPersistence
from tourlib.client_config import ClientConfig


async def load_client_config(tourconf: str | Path | None) -> ClientConfig:
    return await load_config(tourconf, persistence=TomlPersistence())


async def resolve_namespace(
    namespace: str | None,
    *,
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    """Return ``namespace``, or the namespace of the selected context."""
    if namespace is not None:
        return namespace
    config = await load_client_config(tourconf)
    return config.context(context_name).namespace
