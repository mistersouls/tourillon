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

from typing import Annotated

from cyclopts import Parameter

from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.core.kv import TypeName, kv_delete, kv_get, kv_put


async def put(
    key: str,
    value: str,
    namespace: Annotated[str, Parameter(name=("--namespace", "-n"))] = "default",
    key_type: TypeName = "str",
    value_type: TypeName | None = None,
    namespace_type: TypeName = "auto",
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Write a key/value pair and return the resulting Version."""
    opts = opts or GlobalOptions()
    return await kv_put(
        key=key,
        value=value,
        namespace=namespace,
        key_type=key_type,
        value_type=value_type,
        namespace_type=namespace_type,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


async def get(
    key: str,
    namespace: Annotated[str, Parameter(name=("--namespace", "-n"))] = "default",
    key_type: TypeName = "str",
    namespace_type: TypeName = "auto",
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Read a key and return the latest Version, if any."""
    opts = opts or GlobalOptions()
    return await kv_get(
        key=key,
        namespace=namespace,
        key_type=key_type,
        namespace_type=namespace_type,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


async def delete(
    key: str,
    namespace: Annotated[str, Parameter(name=("--namespace", "-n"))] = "default",
    key_type: TypeName = "str",
    namespace_type: TypeName = "auto",
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Delete a key and return the resulting tombstone Version."""
    opts = opts or GlobalOptions()
    return await kv_delete(
        key=key,
        namespace=namespace,
        key_type=key_type,
        namespace_type=namespace_type,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )





