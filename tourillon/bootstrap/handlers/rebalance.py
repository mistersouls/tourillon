import logging

from tourillon.bootstrap.deps import get_core, peer_dispatcher
from tourillon.core.exceptions import RebalanceError
from tourillon.core.transport.conn import ReceiveEnvelope, SendEnvelope
from tourlib.framing import replay_envelope

dispatcher = peer_dispatcher()
logger = logging.getLogger(__name__)


@dispatcher.on("rebalance.plan")
async def rebalance_plan(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    req = core.serializer.decode(env.payload)

    try:
        accept = await core.node.accept_plan(req)
        epoch = accept["epoch"]
        resp = replay_envelope(
            kind="rebalance.plan.ok",
            serializer=core.serializer,
            payload={"epoch": epoch},
            correlation_id=env.correlation_id,
        )
        await send(resp)

        for range_transfer in accept["ingoing"]:
            async for resume_payload in core.node.resume_transfer(epoch, range_transfer):
                resume = replay_envelope(
                    kind="rebalance.resume",
                    serializer=core.serializer,
                    payload=resume_payload,
                    correlation_id=env.correlation_id,
                )
                await send(resume)
    except RebalanceError as exc:
        reject_payload = {"reason": exc.reason, "data": exc.data}
        reject = replay_envelope(
            kind="rebalance.plan.reject",
            serializer=core.serializer,
            payload=reject_payload,
            correlation_id=env.correlation_id,
        )
        await send(reject)


@dispatcher.on("rebalance.resume")
async def rebalance_resume(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    req = core.serializer.decode(env.payload)

    async for resume_payload in core.node.transfer(req):
        replay = replay_envelope(
            kind="rebalance.transfer",
            serializer=core.serializer,
            payload=resume_payload,
            correlation_id=env.correlation_id,
        )
        logger.debug("Sending rebalance.transfer: %s", resume_payload["transfer_id"])
        await send(replay)


@dispatcher.on("rebalance.commit")
async def rebalance_commit(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    env = await receive()
    core = get_core()
    req = core.serializer.decode(env.payload)

    try:
        accept = await core.node.commit_transfer(req)
        replay = replay_envelope(
            kind="rebalance.commit.ok",
            payload=accept,
            correlation_id=env.correlation_id,
            serializer=core.serializer,
        )
        logger.debug("Sending rebalance.commit.ok: %s", accept["transfer_id"])
        await send(replay)
    except RebalanceError as exc:
        reject_payload = {"reason": exc.reason, "data": exc.data}
        reject = replay_envelope(
            kind="rebalance.commit.reject",
            payload=reject_payload,
            correlation_id=env.correlation_id,
            serializer=core.serializer,
        )
        logger.debug("Sending rebalance.commit.reject: %s", reject_payload)
        await send(reject)
