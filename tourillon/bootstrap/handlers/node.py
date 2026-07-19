from tourillon.bootstrap.deps import get_core, peer_dispatcher
from tourillon.core.exceptions import JoinError
from tourillon.core.transport.conn import ReceiveEnvelope, SendEnvelope
from tourlib.framing import replay_envelope

dispatcher = peer_dispatcher()


@dispatcher.on("node.join")
async def node_join(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    req = core.serializer.decode(env.payload)
    seeds = req.get("seeds")

    try:
        resp_payload = await core.node.join(seeds_override=seeds)
        resp = replay_envelope(
            kind="node.join.ok",
            serializer=core.serializer,
            payload=resp_payload,
            correlation_id=env.correlation_id,
        )
    except JoinError as exc:
        resp = replay_envelope(
            kind="node.join.error",
            serializer=core.serializer,
            payload={"code": exc.code, "message": exc.message},
            correlation_id=env.correlation_id,
        )
    except Exception as exc:
        resp = replay_envelope(
            kind="node.join.error",
            serializer=core.serializer,
            payload={"code": "internal_error", "message": str(exc)},
            correlation_id=env.correlation_id,
        )

    await send(resp)

