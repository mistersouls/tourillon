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
"""tourillon pki subcommands — CA generation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer

from tourillon.bootstrap.log import setup_logging
from tourillon.core.ports.pki import CaRequest, PkiError
from tourillon.infra.pki.x509 import CryptographyCaAdapter

logger = logging.getLogger(__name__)

pki_app = typer.Typer(no_args_is_help=True)


@pki_app.command("ca")
def pki_ca(
    out_cert: Annotated[
        Path, typer.Option("--out-cert", help="Output CA certificate path")
    ],
    out_key: Annotated[
        Path, typer.Option("--out-key", help="Output CA private key path (mode 0600)")
    ],
    common_name: Annotated[str, typer.Option("--common-name")] = "Tourillon CA",
    valid_days: Annotated[int, typer.Option("--valid-days")] = 3650,
    key_size: Annotated[int, typer.Option("--key-size")] = 4096,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
) -> None:
    """Generate a new self-signed Certificate Authority."""
    setup_logging(log_level)
    logger.debug(
        "Generating %d-bit CA certificate for '%s', valid for %d days.",
        key_size,
        common_name,
        valid_days,
    )
    adapter = CryptographyCaAdapter()
    request = CaRequest(
        common_name=common_name,
        valid_days=valid_days,
        key_size=key_size,
        out_cert=out_cert,
        out_key=out_key,
    )
    try:
        adapter.generate_ca(request)
    except PkiError as exc:
        logger.error("Failed to generate CA certificate: %s.", exc)
        raise typer.Exit(1) from exc

    logger.info("CA certificate written to %s.", out_cert)
    logger.info("CA private key written to %s (mode 0600).", out_key)
