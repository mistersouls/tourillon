import functools

from tourctl.core.services.node import NodeClientService
from tourlib.infra.crypto_tls import CryptographyTlsContext
from tourlib.infra.msgpack import MsgpackSerializer
from tourlib.infra.toml_rw import TomlConfigReadWriter
from tourlib.ports.loader import ConfigReadWriter
from tourlib.ports.serializer import Serializer
from tourlib.ports.tls import TlsContext


@functools.lru_cache(maxsize=1)
def get_config_rw() -> ConfigReadWriter:
    return TomlConfigReadWriter()

@functools.lru_cache(maxsize=1)
def get_tls_ctx() -> TlsContext:
    return CryptographyTlsContext()

@functools.lru_cache(maxsize=1)
def get_serializer() -> Serializer:
    return MsgpackSerializer()


@functools.lru_cache(maxsize=1)
def get_node_client() -> NodeClientService:
    return NodeClientService(
        config_rw=get_config_rw(),
        tls_ctx=get_tls_ctx(),
        serializer=get_serializer()
    )
