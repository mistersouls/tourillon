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

import sys
from typing import Annotated

from cyclopts import Parameter

from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.core.kv import TypeName, kv_delete, kv_get, kv_put, kv_repair

_KEY_TYPE_HELP = (
    "Type the key is decoded as. Keys are typed: 42 as str and 42 as int "
    "are two different keys."
)


async def put(
    key: str,
    value: Annotated[str, Parameter(allow_leading_hyphen=True)],
    namespace: Annotated[str | None, Parameter(name=("--namespace", "-n"))] = None,
    key_type: Annotated[TypeName, Parameter(help=_KEY_TYPE_HELP)] = "str",
    value_type: TypeName | None = None,
    namespace_type: TypeName = "str",
    quorum_write: Annotated[int, Parameter(name=("--quorum-write", "-w"))] = 1,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Write a value.

    VALUE is read as --value-type (auto infers str, int, float, bool or JSON).
    @FILE reads a file (bytes unless a type is given), - reads stdin, and @@
    starts a literal value with @.
    """
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
    namespace: Annotated[str | None, Parameter(name=("--namespace", "-n"))] = None,
    key_type: Annotated[TypeName, Parameter(help=_KEY_TYPE_HELP)] = "str",
    namespace_type: TypeName = "str",
    quorum_read: Annotated[int, Parameter(name=("--quorum-read", "-r"))] = 1,
    json: bool = False,
    raw: Annotated[
        bool, Parameter(help="Print only the value, for scripts and pipes.")
    ] = False,
    output: Annotated[str | None, Parameter(help="Write the value to a file.")] = None,
    opts: GlobalOptions | None = None,
) -> None:
    """Read a key. An absent or deleted key exits with code 4."""
    opts = opts or GlobalOptions()
    resp = await kv_get(
        key=key,
        namespace=namespace,
        key_type=key_type,
        namespace_type=namespace_type,
        quorum_read=quorum_read,
        as_json=json,
        raw=raw,
        output=output,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    if isinstance(resp, bytes):
        sys.stdout.flush()
        sys.stdout.buffer.write(resp)
        sys.stdout.buffer.flush()
        return
    print(resp)


async def delete(
    key: str,
    namespace: Annotated[str | None, Parameter(name=("--namespace", "-n"))] = None,
    key_type: Annotated[TypeName, Parameter(help=_KEY_TYPE_HELP)] = "str",
    namespace_type: TypeName = "str",
    quorum_write: Annotated[int, Parameter(name=("--quorum-write", "-w"))] = 1,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Delete a key by writing a tombstone; deleting an absent key succeeds."""
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
    quorum_write: Annotated[int, Parameter(name=("--quorum-write", "-w"))] = 1,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Finish a write whose quorum failed, from the token it printed."""
    opts = opts or GlobalOptions()
    resp = await kv_repair(
        token=token,
        quorum_write=quorum_write,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(resp)
