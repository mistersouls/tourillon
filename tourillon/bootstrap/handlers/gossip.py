import logging

from tourillon.bootstrap.deps import get_core, peer_dispatcher
from tourillon.core.exceptions import GossipError
from tourillon.core.transport.conn import ReceiveEnvelope, SendEnvelope
from tourlib.framing import replay_envelope

dispatcher = peer_dispatcher()
logger = logging.getLogger(__name__)


@dispatcher.on("gossip.push")
async def gossip_push(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    try:
        payload = core.serializer.decode(env.payload)
    except Exception as exc:
        logger.warning("gossip.push: malformed payload", exc_info=exc)
        return

    try:
        response = await core.node.handle_gossip_push(payload)
    except GossipError as exc:
        await send(
            replay_envelope(
                kind=exc.kind,
                payload=exc.payload,
                serializer=core.serializer,
                correlation_id=env.correlation_id,
            )
        )
        return

    await send(
        replay_envelope(
            kind="gossip.push.ok",
            payload=response,
            serializer=core.serializer,
            correlation_id=env.correlation_id,
        )
    )


@dispatcher.on("gossip.ping")
async def gossip_ping(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    try:
        payload = core.serializer.decode(env.payload)
    except Exception as exc:
        logger.warning("gossip.ping: malformed payload", exc_info=exc)
        return

    try:
        response = await core.node.handle_gossip_ping(payload)
    except GossipError as exc:
        await send(replay_envelope(
            kind=exc.kind,
            payload=exc.payload,
            serializer=core.serializer,
            correlation_id=env.correlation_id,
        ))
        return

    await send(
        replay_envelope(
            kind="gossip.pong",
            payload=response,
            serializer=core.serializer,
            correlation_id=env.correlation_id,
        )
    )


@dispatcher.on("gossip.digest")
async def gossip_digest(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    try:
        payload = core.serializer.decode(env.payload)
    except Exception as exc:
        logger.warning("gossip.digest: malformed payload", exc_info=exc)
        return

    try:
        response = await core.node.handle_gossip_digest(payload)
    except GossipError as exc:
        await send(replay_envelope(
            kind=exc.kind,
            payload=exc.payload,
            serializer=core.serializer,
            correlation_id=env.correlation_id,
        ))
        return

    await send(
        replay_envelope(
            kind="gossip.delta",
            payload=response,
            serializer=core.serializer,
            correlation_id=env.correlation_id,
        )
    )

