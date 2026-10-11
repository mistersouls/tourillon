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

from typing import Protocol


class MtlsCrypto(Protocol):
    """Port used to generate mTLS certificates (CA and leaf certs)."""

    def generate_ca(
        self,
        *,
        common_name: str,
        organization: str,
        country: str | None,
        validity_days: int,
        key_size: int,
    ) -> tuple[bytes, bytes]:
        """Build a self-signed CA certificate and matching private key."""

    def generate_cert(
        self,
        *,
        common_name: str,
        ca_certificate_pem: bytes,
        ca_private_key_pem: bytes,
        validity_days: int,
        key_size: int,
        client_only: bool = False,
    ) -> tuple[bytes, bytes]:
        """Build a leaf certificate and private key signed by the given CA."""
