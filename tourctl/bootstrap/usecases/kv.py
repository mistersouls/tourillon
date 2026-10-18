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
from tourctl.core.kv import TypeName, kv_delete, kv_get, kv_put, kv_repair


async def put(
    key: str,
    value: str,
    namespace: Annotated[str, Parameter(name=("--namespace", "-n"))] = "default",
    key_type: TypeName = "str",
    value_type: TypeName | None = None,
    namespace_type: TypeName = "auto",
    quorum_write: Annotated[int | None, Parameter(name=("--quorum-write", "-w"))] = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Write a key/value pair and print the resulting Version."""
    opts = opts or GlobalOptions()
    resp = await kv_put(
        key=key,
        value=value,
        namespace=namespace,
        key_type=key_type,
        value_type=value_type,
        namespace_type=namespace_type,
        quorum_write=quorum_write,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(resp)


async def get(
    key: str,
    namespace: Annotated[str, Parameter(name=("--namespace", "-n"))] = "default",
    key_type: TypeName = "str",
    namespace_type: TypeName = "auto",
    quorum_read: Annotated[int | None, Parameter(name=("--quorum-read", "-r"))] = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Read a key and print the latest Version, if any."""
    opts = opts or GlobalOptions()
    resp = await kv_get(
        key=key,
        namespace=namespace,
        key_type=key_type,
        namespace_type=namespace_type,
        quorum_read=quorum_read,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(resp)


async def delete(
    key: str,
    namespace: Annotated[str, Parameter(name=("--namespace", "-n"))] = "default",
    key_type: TypeName = "str",
    namespace_type: TypeName = "auto",
    quorum_write: Annotated[int | None, Parameter(name=("--quorum-write", "-w"))] = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Delete a key and print the resulting tombstone Version."""
    opts = opts or GlobalOptions()
    resp = await kv_delete(
        key=key,
        namespace=namespace,
        key_type=key_type,
        namespace_type=namespace_type,
        quorum_write=quorum_write,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(resp)


async def repair(
    token: str,
    quorum_write: Annotated[int | None, Parameter(name=("--quorum-write", "-w"))] = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Resume an under-replicated write from the handle its failure returned."""
    opts = opts or GlobalOptions()
    resp = await kv_repair(
        token=token,
        quorum_write=quorum_write,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(resp)
