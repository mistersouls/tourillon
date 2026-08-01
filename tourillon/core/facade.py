from tourillon.core.machinery.state import StatePersistence
from tourillon.core.ports.pki import X509CertificateIssuer
from tourillon.core.ports.storage import Storage
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.services.config import NodeConfigService
from tourillon.core.services.manager import NodeManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.dispatcher import Dispatcher
from tourlib.ports.loader import ConfigReadWriter
from tourlib.ports.serializer import Serializer
from tourlib.ports.tls import TlsContext


class TourillonCore:
    def __init__(
        self,
        config_rw: ConfigReadWriter,
        tls_ctx: TlsContext,
        cert_issuer: X509CertificateIssuer,
        serializer: Serializer,
        peer_dispatcher: Dispatcher,
        kv_dispatcher: Dispatcher,
    ) -> None:
        self._config_rw = config_rw
        self._tls_ctx = tls_ctx
        self._cert_issuer = cert_issuer
        self._serializer = serializer
        self._peer_dispatcher = peer_dispatcher
        self._kv_dispatcher = kv_dispatcher
        self._config = NodeConfigService(config_rw, tls_ctx, cert_issuer)

        self._node: NodeManager | None = None
        self._state: StatePersistence | None = None
        self._initialized = False

    def setup(
        self, *,
        cfg: TourillonConfig,
        state: StatePersistence,
        partitioner: Partitioner,
        storage: Storage,
    ) -> None:
        if self._initialized:
            raise RuntimeError("NodeManager already initialized.")

        self._initialized = True
        self._node = NodeManager(
            cfg=cfg,
            tls_ctx=self._tls_ctx,
            peer_dispatcher=self._peer_dispatcher,
            kv_dispatcher=self._kv_dispatcher,
            state_persistence=state,
            serializer=self._serializer,
            partitioner=partitioner,
            storage=storage,
        )
        self._state = state

    @property
    def config(self) -> NodeConfigService:
        return self._config

    @property
    def node(self) -> NodeManager:
        if self._node is None:
            raise RuntimeError("NodeManager not initialized. Call setup first.")
        return self._node

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def state(self) -> StatePersistence:
        if self._state is None:
            raise RuntimeError("StatePersistence not initialized. Call setup first.")
        return self._state
