import logging
from collections.abc import AsyncIterator

from tourillon.bootstrap.deps import get_core, peer_dispatcher
from tourillon.core.exceptions import GossipError
from tourlib.envelope import Envelope
from tourlib.framing import replay_envelope

dispatcher = peer_dispatcher()
logger = logging.getLogger(__name__)


@dispatcher.on("gossip.push")
async def gossip_push(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    try:
        payload = core.serializer.decode(envelope.payload)
    except Exception as exc:
        logger.warning("gossip.push: malformed payload", exc_info=exc)
        return

    try:
        response = await core.node.handle_gossip_push(payload)
        yield replay_envelope(
            kind="gossip.push.ok",
            payload=response,
            serializer=core.serializer,
            correlation_id=envelope.correlation_id,
        )
    except GossipError as exc:
        yield replay_envelope(
            kind=exc.kind,
            payload=exc.payload,
            serializer=core.serializer,
            correlation_id=envelope.correlation_id,
        )

    return


@dispatcher.on("gossip.ping")
async def gossip_ping(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    try:
        payload = core.serializer.decode(envelope.payload)
    except Exception as exc:
        logger.warning("gossip.ping: malformed payload", exc_info=exc)
        return

    try:
        response = await core.node.handle_gossip_ping(payload)
        yield replay_envelope(
            kind="gossip.pong",
            payload=response,
            serializer=core.serializer,
            correlation_id=envelope.correlation_id,
        )
    except GossipError as exc:
        yield replay_envelope(
            kind=exc.kind,
            payload=exc.payload,
            serializer=core.serializer,
            correlation_id=envelope.correlation_id,
        )

    return


@dispatcher.on("gossip.digest")
async def gossip_digest(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    try:
        payload = core.serializer.decode(envelope.payload)
    except Exception as exc:
        logger.warning("gossip.digest: malformed payload", exc_info=exc)
        return

    try:
        response = await core.node.handle_gossip_digest(payload)
        yield replay_envelope(
            kind="gossip.delta",
            payload=response,
            serializer=core.serializer,
            correlation_id=envelope.correlation_id,
        )
    except GossipError as exc:
        yield replay_envelope(
            kind=exc.kind,
            payload=exc.payload,
            serializer=core.serializer,
            correlation_id=envelope.correlation_id,
        )
        return

