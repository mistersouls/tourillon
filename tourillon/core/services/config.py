from pathlib import Path
from typing import Any

from tourillon.core.exceptions import ConfigError
from tourlib.exceptions import TlsValidationError
from tourillon.core.helpers.utils import b64
from tourillon.core.machinery.config import NodeSize
from tourillon.core.ports.pki import X509CertificateIssuer
from tourlib.ports.tls import TlsContext
from tourillon.core.structure.cert import CaRequest, CertRequest
from tourillon.core.structure.config import (
    ConfigRequest,
    GossipBootstrapConfig,
    GossipConfig,
    KvServerConfig,
    PeerServerConfig,
    TlsConfig,
    TourillonConfig,
)
from tourlib.contexts import ContextConfigurer
from tourlib.models import ContextsFile
from tourlib.ports.loader import ConfigReadWriter


class NodeConfigService:
    def __init__(
        self,
        config_rw: ConfigReadWriter,
        tls_ctx: TlsContext,
        cert_issuer: X509CertificateIssuer,
    ) -> None:
        self._config_rw = config_rw
        self._tls_validator = tls_ctx
        self._cert_issuer = cert_issuer

    def load_config(self, path: Path) -> TourillonConfig:
        """Load, validate, and return an immutable TourillonConfig from *path*.

        Validation steps (all fatal — raise ConfigError):
          1. TOML parse error.
          2. Missing mandatory sections ([node], [kv_server], [peer_server], [tls]).
          3. Invalid NodeSize value.
          4. Invalid duration/size strings (parse_duration / parse_bytes called on each).
          5. TLS cert expired (validate_cert_not_expired).
          6. TLS cert/key mismatch (validate_cert_key_match).
        """
        data = self._config_rw.read(path)
        self._require_keys(data)

        node_data = data["node"]
        tls_data = data["tls"]
        servers_data = data["servers"]
        gossip_data = data.get("gossip", {})

        assert isinstance(node_data, dict)
        assert isinstance(tls_data, dict)
        assert isinstance(servers_data, dict)

        kv_server_data = servers_data["kv"]
        peer_server_data = servers_data["peer"]

        assert isinstance(kv_server_data, dict)
        assert isinstance(peer_server_data, dict)

        node_size = self._parse_node_size(node_data)
        partition_shift = int(node_data.get("partition_shift", 10))
        if partition_shift < 1:
            raise ConfigError("partition_shift must be >= 1")
        if partition_shift >= 128:
            raise ConfigError("partition_shift must be strictly less than 128")

        self._validate_tls_config(tls_data)

        conf = TourillonConfig(
            node_id=node_data["id"],
            node_size=node_size,
            data_dir=Path(node_data["data_dir"]),
            tls=self._build_tls(tls_data),
            kv_server=KvServerConfig(bind=str(kv_server_data["bind"])),
            peer_server=PeerServerConfig(
                bind=peer_server_data["bind"],
                advertise=peer_server_data.get("advertise") or peer_server_data["bind"],
            ),
            seeds=node_data.get("seeds", []),
            replication_factor=int(node_data.get("replication_factor", 3)),
            partition_shift=partition_shift,
            schema_version=data.get("schema_version", 1),
            gossip=self._build_gossip(gossip_data),
        )

        if conf.segment_shift >= conf.partition_shift:
            raise ConfigError(
                "derived segment_shift must be strictly less than partition_shift"
            )

        return conf

    def generate_ca(self, ca: CaRequest) -> None:
        self._cert_issuer.generate_ca(ca)

    def generate_node_config(self, req: ConfigRequest) -> None:
        cert = CertRequest(
            common_name=req.node_id,
            san_dns=tuple(),
            san_ip=tuple(),
            valid_days=req.cert_valid_days,
            ca_cert=req.ca_cert,
            ca_key=req.ca_key,
            out_cert=req.out_cert,
            out_key=req.out_key,
        )
        self.issue_cert(cert)
        conf = TourillonConfig(
            node_id=req.node_id,
            node_size=req.size,
            data_dir=req.data_dir,
            tls=TlsConfig(
                cert_data=b64(req.out_cert),
                key_data=b64(req.out_key),
                ca_data=b64(req.ca_cert),
            ),
            kv_server=KvServerConfig(bind=req.kv_bind),
            peer_server=PeerServerConfig(
                bind=req.peer_bind,
                advertise=req.peer_advertise,
            ),
            partition_shift=req.partition_shift,
            replication_factor=req.replication_factor,
            seeds=req.seeds,
            schema_version=1,
        )
        self._config_rw.write(req.out, conf.to_dict())

    def load_contexts(self, path: Path) -> ContextsFile:
        configurer = ContextConfigurer(self._config_rw)
        return configurer.load_contexts(path)

    def issue_cert(self, cert: CertRequest) -> None:
        self._cert_issuer.issue_cert(cert)

    def save_contexts(self, path: Path, file: ContextsFile) -> None:
        configurer = ContextConfigurer(self._config_rw)
        configurer.save_contexts(path, file)

    @staticmethod
    def _build_tls(tls_data: dict[str, Any]) -> TlsConfig:
        return TlsConfig(
            cert_data=str(tls_data["cert_data"]),
            key_data=str(tls_data["key_data"]),
            ca_data=str(tls_data["ca_data"]),
        )

    @staticmethod
    def _parse_node_size(node_data: dict[str, Any]) -> NodeSize:
        try:
            return NodeSize(str(node_data.get("size", "M")))
        except ValueError as e:
            raise ConfigError(f"Invalid node size: {node_data.get('size')}") from e

    @staticmethod
    def _require_keys(data: dict[str, Any]) -> None:
        if "node" not in data:
            raise ConfigError("Missing mandatory section: [node]")
        if "tls" not in data:
            raise ConfigError("Missing mandatory section: [tls]")
        if "servers" not in data:
            raise ConfigError("Missing mandatory section: [servers]")
        servers = data["servers"]
        if (
            not isinstance(servers, dict)
            or "kv" not in servers
            or "peer" not in servers
        ):
            raise ConfigError(
                "Missing mandatory section: [servers.kv] or [servers.peer]"
            )

    def _validate_tls_config(self, tls_data: dict[str, Any]) -> None:
        cert_data = str(tls_data["cert_data"])
        key_data = str(tls_data["key_data"])
        try:
            self._tls_validator.validate_cert_not_expired(cert_data)
            self._tls_validator.validate_cert_key_match(cert_data, key_data)
        except TlsValidationError as e:
            raise ConfigError(f"TLS validation error: {e}") from e

    @staticmethod
    def _build_gossip(gossip_data: Any) -> GossipConfig:
        if not isinstance(gossip_data, dict):
            return GossipConfig()
        bootstrap_data = gossip_data.get("bootstrap", {})
        if not isinstance(bootstrap_data, dict):
            bootstrap_data = {}
        bootstrap = GossipBootstrapConfig(
            initial_delay_s=float(
                bootstrap_data.get(
                    "initial_delay_s", GossipBootstrapConfig.initial_delay_s
                )
            ),
            max_delay_s=float(
                bootstrap_data.get("max_delay_s", GossipBootstrapConfig.max_delay_s)
            ),
            multiplier=float(
                bootstrap_data.get("multiplier", GossipBootstrapConfig.multiplier)
            ),
            jitter=float(bootstrap_data.get("jitter", GossipBootstrapConfig.jitter)),
            max_retries=int(
                bootstrap_data.get("max_retries", GossipBootstrapConfig.max_retries)
            ),
            connect_timeout=float(
                bootstrap_data.get(
                    "connect_timeout", GossipBootstrapConfig.connect_timeout
                )
            ),
        )
        return GossipConfig(
            bootstrap=bootstrap,
            anti_entropy_interval=float(
                gossip_data.get(
                    "anti_entropy_interval", GossipConfig.anti_entropy_interval
                )
            ),
            max_fan_out=int(gossip_data.get("max_fan_out", GossipConfig.max_fan_out)),
            max_payload_bytes=int(
                gossip_data.get("max_payload_bytes", GossipConfig.max_payload_bytes)
            ),
            max_digest_entries=int(
                gossip_data.get("max_digest_entries", GossipConfig.max_digest_entries)
            ),
            max_gossip_per_peer_rps=float(
                gossip_data.get(
                    "max_gossip_per_peer_rps", GossipConfig.max_gossip_per_peer_rps
                )
            ),
        )
