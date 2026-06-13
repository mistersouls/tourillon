import functools
import logging
from pathlib import Path

from tourillon.core.facade import TourillonCore
from tourillon.core.machinery.state import FileStatePersistence, StatePersistence
from tourillon.core.ports.pki import X509CertificateIssuer
from tourillon.core.services.config import NodeConfigService
from tourillon.core.services.manager import NodeManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.infra.x509 import CryptographyX509Issuer
from tourlib.infra.crypto_tls import CryptographyTlsContext
from tourlib.infra.msgpack import MsgpackSerializer
from tourlib.infra.toml_rw import TomlConfigReadWriter
from tourlib.ports.serializer import Serializer
from tourlib.ports.tls import TlsContext


@functools.lru_cache(maxsize=1)
def peer_dispatcher() -> Dispatcher:
    return Dispatcher()


@functools.lru_cache(maxsize=1)
def kv_dispatcher() -> Dispatcher:
    return Dispatcher()


@functools.lru_cache(maxsize=1)
def get_cert_issuer() -> X509CertificateIssuer:
    return CryptographyX509Issuer()


@functools.lru_cache(maxsize=1)
def get_config_rw():
    return TomlConfigReadWriter()


@functools.lru_cache(maxsize=1)
def get_config() -> NodeConfigService:
    return NodeConfigService(
        config_rw=get_config_rw(),
        tls_ctx=get_tls_ctx(),
        cert_issuer=get_cert_issuer()
    )


@functools.lru_cache(maxsize=1)
def get_serializer() -> Serializer:
    return MsgpackSerializer()


@functools.lru_cache(maxsize=1)
def get_tls_ctx() -> TlsContext:
    return CryptographyTlsContext()


@functools.lru_cache(maxsize=1)
def get_core() -> TourillonCore:
    return TourillonCore(
        tls_ctx=get_tls_ctx(),
        serializer=get_serializer(),
        peer_dispatcher=peer_dispatcher(),
        kv_dispatcher=kv_dispatcher(),
        cert_issuer=get_cert_issuer(),
        config_rw=get_config_rw(),
    )


def get_state(cfg: TourillonConfig) -> StatePersistence:
    return FileStatePersistence(cfg.data_dir / "state.toml", get_config_rw())


def setup_logging(level: str = "INFO") -> None:
    resolved = getattr(logging, level.upper(), logging.INFO)
    fmt = (
        "%(asctime)s %(levelname)-8s [%(name)s:%(funcName)s] %(message)s"
        if resolved <= logging.DEBUG
        else "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
    )
    logging.basicConfig(
        level=resolved,
        format=fmt,
        datefmt="%Y-%m-%dT%H:%M:%S",
        force=True,
    )
