from collections.abc import AsyncIterator

from tourillon.bootstrap.deps import get_core, peer_dispatcher
from tourillon.core.exceptions import JoinError
from tourlib.envelope import Envelope
from tourlib.framing import replay_envelope

dispatcher = peer_dispatcher()


@dispatcher.on("node.join")
async def node_join(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    req = core.serializer.decode(envelope.payload)
    seeds = req.get("seeds")

    try:
        resp_payload = await core.node.join(seeds_override=seeds)
        resp = replay_envelope(
            kind="node.join.ok",
            serializer=core.serializer,
            payload=resp_payload,
            correlation_id=envelope.correlation_id,
        )
    except JoinError as exc:
        resp = replay_envelope(
            kind="node.join.error",
            serializer=core.serializer,
            payload={"code": exc.code, "message": exc.message},
            correlation_id=envelope.correlation_id,
        )
    except Exception as exc:
        resp = replay_envelope(
            kind="node.join.error",
            serializer=core.serializer,
            payload={"code": "internal_error", "message": str(exc)},
            correlation_id=envelope.correlation_id,
        )

    yield resp
    return
