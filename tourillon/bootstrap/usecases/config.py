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

from pathlib import Path

from cyclopts import App

from tourillon.bootstrap.deps import get_app
from tourillon.core.log import logger
from tourillon.core.models.config import NodeSize

config_app = App(name="config", help="Configuration helpers.")


@config_app.command(name="ca", alias="generate-ca")
def config_ca(
    output_dir: Path = Path("."),
    common_name: str = "tourillon-ca",
    organization: str = "Tourillon",
    country: str | None = None,
    validity_days: int = 3650,
    key_size: int = 4096,
    certificate_filename: str = "ca-cert.pem",
    private_key_filename: str = "ca-key.pem",
    overwrite: bool = False,
) -> str:
    """Generate a self-signed CA used by Tourillon mTLS.

    :param output_dir: Target directory where CA files are created.
    :param common_name: Certificate subject CN used to identify the CA.
    :param organization: Certificate subject O written into the CA certificate.
    :param country: Optional certificate subject C (2-letter ISO code).
    :param validity_days: Lifetime of the generated CA certificate in days.
    :param key_size: RSA key length for the generated CA private key.
    :param certificate_filename: Filename for the generated CA certificate PEM.
    :param private_key_filename: Filename for the generated CA private key PEM.
    :param overwrite: Allow replacing existing output files when ``True``.
    """
    logger.info("Generating CA files in '%s'", output_dir)
    app = get_app()
    result = app.cert_issuer.ca(
        output_dir=output_dir,
        common_name=common_name,
        organization=organization,
        country=country,
        validity_days=validity_days,
        key_size=key_size,
        certificate_filename=certificate_filename,
        private_key_filename=private_key_filename,
        overwrite=overwrite,
    )
    logger.info(
        "Generated CA certificate='%s' private_key='%s'",
        result.certificate_path,
        result.private_key_path,
    )
    return (
        "Generated mTLS CA files:\n"
        f"certificate={result.certificate_path}\n"
        f"private_key={result.private_key_path}"
    )


@config_app.command(name="client", alias="generate-client")
async def config_client(
    common_name: str,
    ca_cert: Path = Path("ca-cert.pem"),
    ca_key: Path = Path("ca-key.pem"),
    output_file: Path = Path("tourconf.toml"),
    client_address: str = "127.0.0.1:9000",
    peer_address: str = "127.0.0.1:9001",
    validity_days: int = 365,
    key_size: int = 2048,
    overwrite: bool = False,
) -> str:
    """Generate a client TOML config with embedded base64 mTLS material.

    :param ca_cert: Path to the CA certificate used to sign the client certificate.
    :param ca_key: Path to the CA private key used for signing operations.
    :param output_file: Destination path for the generated client TOML config.
    :param common_name: Certificate subject CN for the generated client identity.
    :param client_address: Client API endpoint stored in the generated cluster profile.
    :param peer_address: Peer API endpoint stored in the generated cluster profile.
    :param validity_days: Lifetime of the generated client certificate in days.
    :param key_size: RSA key length for the generated client private key.
    :param overwrite: Allow replacing an existing output config when ``True``.
    """
    logger.info("Generating client config at '%s'", output_file)
    app = get_app()
    result = await app.client_config.generate(
        output_file=output_file,
        common_name=common_name,
        ca_certificate_path=ca_cert,
        ca_private_key_path=ca_key,
        client_address=client_address,
        peer_address=peer_address,
        validity_days=validity_days,
        key_size=key_size,
        overwrite=overwrite,
    )
    logger.info("Generated client config file '%s'", result.config_path)
    return f"Generated client config file:\nconfig={result.config_path}"


@config_app.command(name="node", alias="generate-node")
async def config_node(
    node_id: str,
    data_dir: str,
    group_size: str,
    ca_cert: Path,
    ca_key: Path,
    output_file: Path = Path("node.toml"),
    seeds: list[str] | None = None,
    client_bind: str = "127.0.0.1:7000",
    peer_bind: str = "127.0.0.1:7001",
    peer_advertise: str | None = None,
    node_size: NodeSize = NodeSize.M,
    validity_days: int = 365,
    key_size: int = 2048,
    overwrite: bool = False,
) -> str:
    """Generate a node TOML config with embedded base64 mTLS material.

    :param node_id: Stable node identifier and certificate CN for this node.
    :param output_file: Destination path for the generated node TOML config.
    :param ca_cert: Path to the CA certificate used to sign the node certificate.
    :param ca_key: Path to the CA private key used for signing operations.
    :param data_dir: Node storage directory path (absolute or relative to current directory).
    :param group_size: Allocated storage size for a single data group, e.g. ``8Mi`` or ``1Gi``.
    :param seeds: Optional cluster entrypoints used at startup (stored in ``node.seeds``).
    :param client_bind: Address exposed for client traffic (stored in ``servers.client.bind``).
    :param peer_bind: Local bind address for peer traffic (stored in ``servers.peer.bind``).
    :param peer_advertise: Advertised peer address for remote nodes; defaults to ``peer_bind``.
    :param node_size: Node size class controlling vnode token count (XS, S, M, L, XL, XXL).
    :param validity_days: Lifetime of the generated node certificate in days.
    :param key_size: RSA key length for the generated node private key.
    :param overwrite: Allow replacing an existing output config when ``True``.
    """
    logger.info("Generating node config for node '%s' at '%s'", node_id, output_file)
    app = get_app()
    result = await app.node_config.generate(
        output_file=output_file,
        node_id=node_id,
        ca_certificate_path=ca_cert,
        ca_private_key_path=ca_key,
        data_dir=data_dir,
        group_size=group_size,
        seeds=seeds,
        client_bind=client_bind,
        peer_bind=peer_bind,
        peer_advertise=peer_advertise,
        node_size=node_size,
        validity_days=validity_days,
        key_size=key_size,
        overwrite=overwrite,
    )
    logger.info("Generated node config file '%s'", result.config_path)
    return f"Generated node config file:\nconfig={result.config_path}"
