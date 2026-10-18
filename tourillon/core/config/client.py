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

from tourlib.client_config import (
    ClientAuthConfig,
    ClientCluster,
    ClientConfig,
    ClientContext,
)
from tourlib.port import MtlsCrypto, Persistence


@dataclass(frozen=True)
class GenerateClientConfigResult:
    """Filesystem location of the generated client config."""

    config_path: Path


class ClientConfigGenerator:
    """Generate and persist client identity config document."""

    def __init__(self, crypto: MtlsCrypto, persistence: Persistence) -> None:
        self._crypto = crypto
        self._persistence = persistence

    async def generate(
        self,
        *,
        output_file: Path,
        common_name: str,
        ca_certificate_path: Path,
        ca_private_key_path: Path,
        validity_days: int,
        key_size: int,
        client_address: str = "127.0.0.1:9000",
        peer_address: str = "127.0.0.1:9001",
        overwrite: bool,
    ) -> GenerateClientConfigResult:
        if not common_name:
            raise ValueError("common_name must not be empty")
        if not overwrite:
            self._assert_absent(output_file)

        output_file.parent.mkdir(parents=True, exist_ok=True)
        ca_cert_pem = ca_certificate_path.read_bytes()
        crypto = self._crypto
        cert_pem, key_pem = crypto.generate_cert(
            common_name=common_name,
            ca_certificate_pem=ca_cert_pem,
            ca_private_key_pem=ca_private_key_path.read_bytes(),
            validity_days=validity_days,
            key_size=key_size,
            client_only=True,
        )
        current_context = common_name
        context_cluster = common_name
        context_auth = common_name
        resolved_contexts = {
            current_context: ClientContext(
                cluster=context_cluster,
                namespace="default",
                auth=context_auth,
            )
        }
        resolved_clusters = {
            context_cluster: ClientCluster(client=client_address, peer=peer_address)
        }
        resolved_auth = {
            context_auth: ClientAuthConfig(
                ca_data=self._b64(ca_cert_pem),
                cert_data=self._b64(cert_pem),
                key_data=self._b64(key_pem),
            )
        }
        config = ClientConfig(
            version="1",
            current_context=current_context,
            contexts=resolved_contexts,
            clusters=resolved_clusters,
            auth=resolved_auth,
        )
        await self._persistence.write(output_file, config.to_document())
        return GenerateClientConfigResult(config_path=output_file)

    @staticmethod
    def _b64(value: bytes) -> str:
        return base64.b64encode(value).decode("ascii")

    @staticmethod
    def _assert_absent(path: Path) -> None:
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing file: {path}")
