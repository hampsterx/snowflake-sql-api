"""Auth matrix: account normalization, host derivation, JWT claims, key loading."""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from snowflake_sql_api.auth import (
    KeypairAuthenticator,
    account_hostname,
    normalize_account_locator,
)
from snowflake_sql_api.exceptions import SnowflakeConfigError

NO_EXP = {"verify_exp": False, "verify_aud": False}


def _decode(token: str, key: RSAPrivateKey) -> dict:
    return jwt.decode(token, key.public_key(), algorithms=["RS256"], options=NO_EXP)


def test_account_normalization_basic() -> None:
    assert normalize_account_locator("xy12345") == "XY12345"
    assert normalize_account_locator("xy12345.ap-southeast-2") == "XY12345"
    assert normalize_account_locator("xy12345.ap-southeast-2.aws") == "XY12345"
    assert normalize_account_locator("myorg-myaccount") == "MYORG-MYACCOUNT"


def test_account_normalization_requires_value() -> None:
    with pytest.raises(SnowflakeConfigError):
        normalize_account_locator("")


def test_hostname_keeps_region_lowercased() -> None:
    assert (
        account_hostname("XY12345.AP-SOUTHEAST-2")
        == "xy12345.ap-southeast-2.snowflakecomputing.com"
    )
    assert account_hostname("my_org-acct") == "my-org-acct.snowflakecomputing.com"


def test_jwt_claims(
    rsa_key: RSAPrivateKey, private_key_pem: bytes, expected_fingerprint: str
) -> None:
    auth = KeypairAuthenticator("xy12345.ap-southeast-2", "myuser", private_key_pem)
    token = auth.token(now=1000.0)
    claims = _decode(token, rsa_key)
    assert claims["sub"] == "XY12345.MYUSER"
    assert claims["iss"] == f"XY12345.MYUSER.{expected_fingerprint}"
    assert claims["iat"] == 1000
    assert claims["exp"] == 1000 + 3600


def test_fingerprint_matches_independent_oracle(
    private_key_pem: bytes, expected_fingerprint: str
) -> None:
    auth = KeypairAuthenticator("acct", "user", private_key_pem)
    assert auth.issuer.endswith(expected_fingerprint)
    assert expected_fingerprint.startswith("SHA256:")


def test_encrypted_private_key(
    private_key_pem_encrypted: Tuple[bytes, bytes], rsa_key: RSAPrivateKey
) -> None:
    pem, passphrase = private_key_pem_encrypted
    auth = KeypairAuthenticator("acct", "user", pem, private_key_passphrase=passphrase)
    token = auth.token(now=1000.0)
    claims = _decode(token, rsa_key)
    assert claims["sub"] == "ACCT.USER"


def test_encrypted_key_without_passphrase_raises(
    private_key_pem_encrypted: Tuple[bytes, bytes],
) -> None:
    pem, _ = private_key_pem_encrypted
    with pytest.raises(SnowflakeConfigError):
        KeypairAuthenticator("acct", "user", pem)


def test_private_key_as_str(private_key_pem: bytes) -> None:
    auth = KeypairAuthenticator("acct", "user", private_key_pem.decode())
    assert auth.token(now=1.0)


def test_token_is_cached_within_margin(private_key_pem: bytes) -> None:
    auth = KeypairAuthenticator(
        "acct",
        "user",
        private_key_pem,
        lifetime_seconds=3600,
        renewal_margin_seconds=120,
    )
    first = auth.token(now=1000.0)
    # 4400 < exp(4600) - margin(120) = 4480 -> still cached.
    assert auth.token(now=4400.0) == first


def test_token_refreshes_near_expiry(private_key_pem: bytes) -> None:
    auth = KeypairAuthenticator(
        "acct",
        "user",
        private_key_pem,
        lifetime_seconds=3600,
        renewal_margin_seconds=120,
    )
    first = auth.token(now=1000.0)
    # 4500 >= 4480 -> regenerate with a later iat -> different token.
    refreshed = auth.token(now=4500.0)
    assert refreshed != first


def test_user_required(private_key_pem: bytes) -> None:
    with pytest.raises(SnowflakeConfigError):
        KeypairAuthenticator("acct", "", private_key_pem)


def test_lifetime_bounds(private_key_pem: bytes) -> None:
    with pytest.raises(SnowflakeConfigError):
        KeypairAuthenticator("acct", "user", private_key_pem, lifetime_seconds=0)
    with pytest.raises(SnowflakeConfigError):
        KeypairAuthenticator("acct", "user", private_key_pem, lifetime_seconds=7200)


def test_negative_renewal_margin_rejected(private_key_pem: bytes) -> None:
    # A negative margin would widen the cache window past expiry.
    with pytest.raises(SnowflakeConfigError):
        KeypairAuthenticator("a", "u", private_key_pem, renewal_margin_seconds=-1)


def test_bad_key_raises() -> None:
    with pytest.raises(SnowflakeConfigError):
        KeypairAuthenticator("acct", "user", b"not a key")


def test_load_from_file(private_key_file: Path, rsa_key: RSAPrivateKey) -> None:
    auth = KeypairAuthenticator("acct", "user", private_key_file.read_bytes())
    assert _decode(auth.token(now=1.0), rsa_key)["sub"] == "ACCT.USER"
