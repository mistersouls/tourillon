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

from collections.abc import AsyncIterator, Mapping

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ValidationError
from tourlib.models.gc import GcJobs, GcRequest, GcResponse

dispatcher = peer_dispatcher()


@dispatcher.on("gc.run")
async def run(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        model = GcRequest.from_document(serializer.decode(request.payload))
        manager.gc.request(dry_run=model.dry_run, partition=model.partition)
    except (ValidationError, ValueError) as exc:
        yield replay_envelope(
            kind="gc.run.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    result = GcResponse(manager.node_id, manager.gc.inspect().status)
    yield replay_envelope(
        kind="gc.run.accept",
        request=request,
        payload=serializer.encode(result.to_dict()),
    )


@dispatcher.on("gc.status")
async def status(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    document = serializer.decode(request.payload)
    if not isinstance(document, Mapping) or document:
        yield replay_envelope(
            kind="gc.status.reject",
            request=request,
            payload=serializer.encode({"message": "gc.status takes no arguments"}),
        )
        return
    gc = manager.gc
    result = GcJobs(manager.node_id, gc.inspect(), gc.next_pass_at, gc.jobs())
    yield replay_envelope(
        kind="gc.status.accept",
        request=request,
        payload=serializer.encode(result.to_dict()),
    )
