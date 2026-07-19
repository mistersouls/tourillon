import logging
from collections.abc import AsyncIterator

from tourillon.bootstrap.deps import get_core, peer_dispatcher
from tourillon.core.exceptions import RebalanceError
from tourlib.envelope import Envelope
from tourlib.framing import replay_envelope

dispatcher = peer_dispatcher()
logger = logging.getLogger(__name__)


@dispatcher.on("rebalance.plan")
async def rebalance_plan(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    req = core.serializer.decode(envelope.payload)

    try:
        accept = await core.node.accept_plan(req)
        epoch = accept["epoch"]
        resp = replay_envelope(
            kind="rebalance.plan.ok",
            serializer=core.serializer,
            payload={"epoch": epoch},
            correlation_id=envelope.correlation_id,
        )
        yield resp

        for range_transfer in accept["ingoing"]:
            async for resume_payload in core.node.resume_transfer(epoch, range_transfer):
                resume = replay_envelope(
                    kind="rebalance.resume",
                    serializer=core.serializer,
                    payload=resume_payload,
                    correlation_id=envelope.correlation_id,
                )
                yield resume
    except RebalanceError as exc:
        reject_payload = {"reason": exc.reason, "data": exc.data}
        reject = replay_envelope(
            kind="rebalance.plan.reject",
            serializer=core.serializer,
            payload=reject_payload,
            correlation_id=envelope.correlation_id,
        )
        yield reject

    return


@dispatcher.on("rebalance.resume")
async def rebalance_resume(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    req = core.serializer.decode(envelope.payload)

    async for resume_payload in core.node.transfer(req):
        replay = replay_envelope(
            kind="rebalance.transfer",
            serializer=core.serializer,
            payload=resume_payload,
            correlation_id=envelope.correlation_id,
        )
        logger.debug("Sending rebalance.transfer: %s", resume_payload["transfer_id"])
        yield replay

    return


@dispatcher.on("rebalance.commit")
async def rebalance_commit(envelope: Envelope) -> AsyncIterator[Envelope]:
    core = get_core()
    req = core.serializer.decode(envelope.payload)

    try:
        accept = await core.node.commit_transfer(req)
        replay = replay_envelope(
            kind="rebalance.commit.ok",
            payload=accept,
            correlation_id=envelope.correlation_id,
            serializer=core.serializer,
        )
        logger.debug("Sending rebalance.commit.ok: %s", accept["transfer_id"])
        yield replay
    except RebalanceError as exc:
        reject_payload = {"reason": exc.reason, "data": exc.data}
        reject = replay_envelope(
            kind="rebalance.commit.reject",
            payload=reject_payload,
            correlation_id=envelope.correlation_id,
            serializer=core.serializer,
        )
        logger.debug("Sending rebalance.commit.reject: %s", reject_payload)
        yield reject

    return
