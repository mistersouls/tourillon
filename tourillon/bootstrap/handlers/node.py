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

from collections.abc import AsyncIterator
from typing import Any

from tourillon.bootstrap.deps import get_manager, peer_dispatcher
from tourillon.core.exceptions import (
    DrainError,
    JoinError,
    MemberNotFoundError,
    PauseError,
    ResumeError,
)
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ValidationError
from tourlib.models.inspect import MembersRequest
from tourlib.models.member import MemberRequest
from tourlib.models.node import (
    CancelDrainRequest,
    CancelJoinRequest,
    DrainRequest,
    JoinRequest,
    PauseRequest,
    ResumeRequest,
)

dispatcher = peer_dispatcher()


@dispatcher.on("node.inspect")
async def inspect(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    status = await manager.inspect()
    yield replay_envelope(
        kind="node.inspect.accept",
        request=request,
        payload=serializer.encode(status.to_document()),
    )


@dispatcher.on("node.health")
async def health(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    verdict = await manager.health()
    yield replay_envelope(
        kind="node.health.accept",
        request=request,
        payload=manager.serializer.encode(verdict.to_document()),
    )


@dispatcher.on("node.members")
async def members(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        model = MembersRequest.from_document(serializer.decode(request.payload))
        page = await manager.members(model)
    except (ValidationError, TypeError) as exc:
        yield replay_envelope(
            kind="node.members.reject",
            request=request,
            payload=serializer.encode({"code": "invalid_request", "message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.members.accept",
        request=request,
        payload=serializer.encode(page.to_document()),
    )


@dispatcher.on("node.member")
async def member(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        model = MemberRequest.from_document(serializer.decode(request.payload))
        report = await manager.member(model.node_id)
    except MemberNotFoundError as exc:
        yield replay_envelope(
            kind="node.member.reject",
            request=request,
            payload=serializer.encode(
                {"code": "member_not_found", "message": str(exc)}
            ),
        )
        return
    except (ValidationError, TypeError) as exc:
        yield replay_envelope(
            kind="node.member.reject",
            request=request,
            payload=serializer.encode({"code": "invalid_request", "message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.member.accept",
        request=request,
        payload=serializer.encode(report.to_document()),
    )


@dispatcher.on("node.join")
async def join(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    data: dict[str, Any] = serializer.decode(request.payload)
    join_req = JoinRequest(**data)
    seeds = list(join_req.seeds) if join_req.seeds else []
    started = await manager.join(seeds)
    message = (
        "Join operation initiated."
        if started
        else f"{manager.node_id} is already READY; nothing to do."
    )
    yield replay_envelope(
        kind="node.join.accept",
        request=request,
        payload=serializer.encode({"message": message}),
    )


@dispatcher.on("node.join.cancel")
async def cancel_join(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        CancelJoinRequest.from_document(serializer.decode(request.payload))
        state = await manager.cancel_join()
    except JoinError as exc:
        yield replay_envelope(
            kind="node.join.cancel.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    except (ValidationError, TypeError) as exc:
        yield replay_envelope(
            kind="node.join.cancel.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.join.cancel.accept",
        request=request,
        payload=serializer.encode(
            {"message": f"{state.node_id}: JOINING -> IDLE (epoch {state.epoch})"}
        ),
    )


@dispatcher.on("node.drain")
async def drain(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    DrainRequest.from_document(serializer.decode(request.payload))
    try:
        await manager.drain()
    except DrainError as exc:
        yield replay_envelope(
            kind="node.drain.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.drain.accept",
        request=request,
        payload=serializer.encode(
            {"message": "Drain initiated; draining continues asynchronously."}
        ),
    )


@dispatcher.on("node.drain.cancel")
async def cancel_drain(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        CancelDrainRequest.from_document(serializer.decode(request.payload))
        state = await manager.cancel_drain()
    except DrainError as exc:
        yield replay_envelope(
            kind="node.drain.cancel.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    except (ValidationError, TypeError) as exc:
        yield replay_envelope(
            kind="node.drain.cancel.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.drain.cancel.accept",
        request=request,
        payload=serializer.encode(
            {
                "message": (
                    f"{state.node_id}: DRAINING -> READY (epoch {state.epoch})\n"
                    "Partitions already handed off are synced back by "
                    "anti-entropy; nothing is lost."
                )
            }
        ),
    )


@dispatcher.on("node.pause")
async def pause(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        PauseRequest.from_document(serializer.decode(request.payload))
        await manager.pause()
    except PauseError as exc:
        yield replay_envelope(
            kind="node.pause.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    except (ValidationError, TypeError) as exc:
        yield replay_envelope(
            kind="node.pause.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.pause.accept",
        request=request,
        payload=serializer.encode({"message": "Node is paused."}),
    )


@dispatcher.on("node.resume")
async def resume(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        ResumeRequest.from_document(serializer.decode(request.payload))
        await manager.resume()
    except ResumeError as exc:
        yield replay_envelope(
            kind="node.resume.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    except (ValidationError, TypeError) as exc:
        yield replay_envelope(
            kind="node.resume.reject",
            request=request,
            payload=serializer.encode({"message": str(exc)}),
        )
        return
    yield replay_envelope(
        kind="node.resume.accept",
        request=request,
        payload=serializer.encode(
            {"message": "Resume initiated; node remains PAUSED while synchronising."}
        ),
    )
