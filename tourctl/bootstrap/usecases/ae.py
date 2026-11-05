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

from cyclopts import App

from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.core.ae import ae_obligations, ae_status
from tourlib.models.ae import OBLIGATIONS_DEFAULT_LIMIT, STATUS_DEFAULT_LIMIT

ae_app = App(
    name="ae", help="Anti-entropy of one node: what it still owes, and to whom."
)


@ae_app.command(name="status")
async def ae_status_cmd(
    address: str,
    *,
    limit: int = STATUS_DEFAULT_LIMIT,
    cursor: str | None = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Show the anti-entropy worker and the targets this node owes data.

    :param address: Direct peer address of the node, never a load balancer.
    :param limit: Targets per page, oldest debt first (1 to 1000).
    :param cursor: Continue after the page that printed this cursor.
    :param json: Print the worker, counters and targets as JSON.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    output = await ae_status(
        address,
        limit=limit,
        cursor=cursor,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(output)


@ae_app.command(name="obligations")
async def ae_obligations_cmd(
    address: str,
    *,
    target: str | None = None,
    limit: int = OBLIGATIONS_DEFAULT_LIMIT,
    cursor: str | None = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """List the replication obligations persisted on the node.

    :param address: Direct peer address of the node, never a load balancer.
    :param target: Only the obligations owed to this node identifier.
    :param limit: Obligations per page (1 to 1000).
    :param cursor: Continue after the page that printed this cursor.
    :param json: Print the obligations as JSON; keys are decoded.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    output = await ae_obligations(
        address,
        target=target,
        limit=limit,
        cursor=cursor,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(output)
