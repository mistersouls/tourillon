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

from dataclasses import dataclass
from pathlib import Path

from tourlib.port import MtlsCrypto


@dataclass(frozen=True)
class GenerateCertificateAuthorityResult:
    """Filesystem locations of generated CA assets."""

    certificate_path: Path
    private_key_path: Path


class CertIssuer:
    """Generate and persist a self-signed CA for Tourillon mTLS."""

    def __init__(self, crypto: MtlsCrypto) -> None:
        self._crypto = crypto

    def ca(
        self,
        *,
        output_dir: Path,
        common_name: str,
        organization: str,
        country: str | None,
        validity_days: int,
        key_size: int,
        certificate_filename: str,
        private_key_filename: str,
        overwrite: bool,
    ) -> GenerateCertificateAuthorityResult:
        if not common_name:
            raise ValueError("common_name must not be empty")
        if not organization:
            raise ValueError("organization must not be empty")

        output_dir.mkdir(parents=True, exist_ok=True)

        certificate_path = output_dir / certificate_filename
        private_key_path = output_dir / private_key_filename

        if not overwrite:
            self._assert_absent(certificate_path)
            self._assert_absent(private_key_path)

        certificate_pem, private_key_pem = self._crypto.generate_ca(
            common_name=common_name,
            organization=organization,
            country=country,
            validity_days=validity_days,
            key_size=key_size,
        )

        certificate_path.write_bytes(certificate_pem)
        private_key_path.write_bytes(private_key_pem)

        return GenerateCertificateAuthorityResult(
            certificate_path=certificate_path,
            private_key_path=private_key_path,
        )

    @staticmethod
    def _assert_absent(path: Path) -> None:
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing file: {path}")

