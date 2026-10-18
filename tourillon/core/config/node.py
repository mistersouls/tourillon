# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
from dataclasses import dataclass
from pathlib import Path

from tourillon.core.models.config import (
    ClientServerConfig,
    NodeConfig,
    NodeSize,
    PeerServerConfig,
    StorageConfig,
    TlsConfig,
    TourillonConfig,
)
from tourillon.core.models.validator import validate_node_id
from tourlib.exceptions import ValidationError
from tourlib.port import MtlsCrypto, Persistence


@dataclass(frozen=True)
class GenerateNodeConfigResult:
    """Filesystem location of the generated node config."""

    config_path: Path


class NodeConfigGenerator:
    """Generate and persist node identity config document."""

    def __init__(self, crypto: MtlsCrypto, persistence: Persistence) -> None:
        self._crypto = crypto
        self._persistence = persistence

    async def generate(
        self,
        *,
        output_file: Path,
        node_id: str,
        ca_certificate_path: Path,
        ca_private_key_path: Path,
        seeds: list[str] | None = None,
        data_dir: str,
        group_size: str,
        node_size: NodeSize = NodeSize.M,
        client_bind: str = "127.0.0.1:7000",
        peer_bind: str = "127.0.0.1:7001",
        peer_advertise: str | None = None,
        validity_days: int,
        key_size: int,
        overwrite: bool,
    ) -> GenerateNodeConfigResult:
        validate_node_id(node_id, "node_id")
        if not isinstance(node_size, NodeSize):
            raise ValidationError("node_size must be one of: XS, S, M, L, XL, XXL")
        if seeds is not None and not all(
            isinstance(seed, str) and seed for seed in seeds
        ):
            raise ValidationError("seeds must be a list of non-empty strings")
        data_dir = StorageConfig.validate_data_dir(data_dir)
        if not client_bind:
            raise ValidationError("client_bind must not be empty")
        if not peer_bind:
            raise ValidationError("peer_bind must not be empty")
        if peer_advertise is not None and not peer_advertise:
            raise ValidationError("peer_advertise must not be empty")
        if not overwrite:
            self._assert_absent(output_file)

        output_file.parent.mkdir(parents=True, exist_ok=True)
        ca_cert_pem = ca_certificate_path.read_bytes()
        cert_pem, key_pem = self._crypto.generate_cert(
            common_name=node_id,
            ca_certificate_pem=ca_cert_pem,
            ca_private_key_pem=ca_private_key_path.read_bytes(),
            validity_days=validity_days,
            key_size=key_size,
        )
        config = TourillonConfig(
            node=NodeConfig(id=node_id, size=node_size, seeds=seeds),
            storage=StorageConfig.from_document(
                {"data_dir": data_dir, "group_size": group_size}
            ),
            tls=TlsConfig(
                ca_data=self._b64(ca_cert_pem),
                cert_data=self._b64(cert_pem),
                key_data=self._b64(key_pem),
            ),
            server_client=ClientServerConfig(bind=client_bind),
            server_peer=PeerServerConfig(
                bind=peer_bind,
                advertise=peer_advertise or peer_bind,
            ),
        )
        await self._persistence.write(output_file, config.to_document())
        return GenerateNodeConfigResult(config_path=output_file)

    @staticmethod
    def _b64(value: bytes) -> str:
        return base64.b64encode(value).decode("ascii")

    @staticmethod
    def _assert_absent(path: Path) -> None:
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing file: {path}")
