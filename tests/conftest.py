"""Shared pytest fixtures.

The default suite mocks the HTTP layer (via ``respx``) and needs no network
access. Live fixtures (a real account + keypair) arrive with the smoke suite;
they are skipped unless ``SNOWFLAKE_*`` env vars are set.

The keypair fixtures here generate a throwaway RSA key per test session so the
auth matrix can sign and verify JWTs without touching a real account. The
expected public-key fingerprint is computed independently of ``auth.py`` (it is
the test oracle), so a regression in the library's fingerprint logic is caught.
"""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from typing import Tuple

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

PASSPHRASE = b"correct horse battery staple"


@pytest.fixture
def fake_account() -> str:
    """A region-suffixed account locator for auth-normalization tests."""
    return "xy12345.ap-southeast-2"


@pytest.fixture(scope="session")
def rsa_key() -> RSAPrivateKey:
    """A throwaway 2048-bit RSA private key, generated once per session."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def private_key_pem(rsa_key: RSAPrivateKey) -> bytes:
    """Unencrypted PKCS#8 PEM bytes for ``rsa_key``."""
    return rsa_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@pytest.fixture(scope="session")
def private_key_pem_encrypted(rsa_key: RSAPrivateKey) -> Tuple[bytes, bytes]:
    """Passphrase-encrypted PKCS#8 PEM bytes plus the passphrase."""
    pem = rsa_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(PASSPHRASE),
    )
    return pem, PASSPHRASE


@pytest.fixture(scope="session")
def expected_fingerprint(rsa_key: RSAPrivateKey) -> str:
    """The ``SHA256:<base64>`` public-key fingerprint Snowflake's JWT issuer uses.

    Computed here directly from the public key's DER SubjectPublicKeyInfo so the
    library's own derivation is checked against an independent oracle.
    """
    der = rsa_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    digest = hashlib.sha256(der).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii")


@pytest.fixture
def private_key_file(tmp_path: Path, private_key_pem: bytes) -> Path:
    """Write the unencrypted PEM to a temp file and return its path."""
    path = tmp_path / "rsa_key.p8"
    path.write_bytes(private_key_pem)
    return path
