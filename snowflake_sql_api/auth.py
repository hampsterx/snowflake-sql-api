"""Keypair (JWT) authentication for the Snowflake SQL API.

Snowflake's SQL API authenticates with a short-lived RS256 JWT signed by the
user's private key. This module owns JWT generation, the account-locator
normalization the issuer/subject claims require, host derivation, and a small
token cache so a client reuses a valid token instead of re-signing every call.

Two account forms are derived differently and must not be conflated:

- The **claim** account (``iss``/``sub``) strips any region/cloud suffix and
  uppercases (``xy12345.ap-southeast-2`` -> ``XY12345``). A dot in the claim
  account makes the JWT invalid - this is regression bug #1.
- The **host** keeps the full account (region included), lowercased with ``_``
  swapped for ``-`` (``xy12345.ap-southeast-2`` -> the region routes the call).
"""

from __future__ import annotations

import base64
import hashlib
import time
from typing import Optional, Union

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes

from .exceptions import SnowflakeConfigError

__all__ = [
    "normalize_account_locator",
    "account_hostname",
    "KeypairAuthenticator",
]

PrivateKeyInput = Union[str, bytes]

# Refresh a token this many seconds before it actually expires, so an in-flight
# request never races the expiry.
DEFAULT_RENEWAL_MARGIN_SECONDS = 120


def normalize_account_locator(account: str) -> str:
    """Return the account locator for the JWT claim: region/cloud stripped, uppercased.

    Takes everything before the first ``.`` (dropping ``region``/``cloud``
    segments) and uppercases it. The org-account dash form is preserved because
    it contains no dot: ``myorg-myaccount`` -> ``MYORG-MYACCOUNT``. A region-
    qualified locator collapses: ``xy12345.ap-southeast-2.aws`` -> ``XY12345``.

    Leaving the region in the claim breaks JWT validation (regression bug #1).
    """
    if not account:
        raise SnowflakeConfigError("account is required")
    locator = account.split(".", 1)[0]
    return locator.upper()


def account_hostname(account: str) -> str:
    """Return the SQL API hostname for an account.

    Keeps the full account string (region/cloud included, needed to route),
    lowercases it, and swaps ``_`` for ``-``:
    ``XY12345.AP-SOUTHEAST-2`` -> ``xy12345.ap-southeast-2.snowflakecomputing.com``.

    Privatelink and some org-account setups need a different host; callers can
    pass an explicit ``host`` to bypass this derivation.
    """
    if not account:
        raise SnowflakeConfigError("account is required")
    return account.lower().replace("_", "-") + ".snowflakecomputing.com"


def _load_private_key(
    data: PrivateKeyInput, passphrase: Optional[PrivateKeyInput]
) -> PrivateKeyTypes:
    """Load a PEM (or DER) private key, decrypting with ``passphrase`` if given."""
    raw = data.encode() if isinstance(data, str) else data
    password: Optional[bytes]
    if passphrase is None:
        password = None
    else:
        password = passphrase.encode() if isinstance(passphrase, str) else passphrase
    try:
        return serialization.load_pem_private_key(raw, password=password)
    except ValueError:
        # Not PEM (or wrong content): try DER before giving up.
        try:
            return serialization.load_der_private_key(raw, password=password)
        except (ValueError, TypeError) as exc:
            raise SnowflakeConfigError(f"could not load private key: {exc}") from exc
    except TypeError as exc:
        # Raised when the key is encrypted but no passphrase was supplied (or
        # vice-versa).
        raise SnowflakeConfigError(f"private key passphrase error: {exc}") from exc


def _public_key_fingerprint(private_key: PrivateKeyTypes) -> str:
    """Return the ``SHA256:<base64>`` fingerprint of the key's public half.

    Computed over the DER SubjectPublicKeyInfo encoding, which is what Snowflake
    stores for the user and expects in the JWT issuer claim.
    """
    public_der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    digest = hashlib.sha256(public_der).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii")


class KeypairAuthenticator:
    """Generates and caches RS256 JWTs for SQL API requests."""

    def __init__(
        self,
        account: str,
        user: str,
        private_key: PrivateKeyInput,
        *,
        private_key_passphrase: Optional[PrivateKeyInput] = None,
        lifetime_seconds: int = 3600,
        renewal_margin_seconds: int = DEFAULT_RENEWAL_MARGIN_SECONDS,
    ) -> None:
        if not user:
            raise SnowflakeConfigError("user is required")
        if lifetime_seconds <= 0 or lifetime_seconds > 3600:
            # Snowflake caps the JWT lifetime at one hour regardless.
            raise SnowflakeConfigError("lifetime_seconds must be in (0, 3600]")
        if renewal_margin_seconds < 0:
            # A negative margin would widen the cache window past expiry and
            # hand back an already-expired token.
            raise SnowflakeConfigError("renewal_margin_seconds must be >= 0")
        self.account_claim = normalize_account_locator(account)
        self.user = user.upper()
        self._key = _load_private_key(private_key, private_key_passphrase)
        self._fingerprint = _public_key_fingerprint(self._key)
        self._lifetime = lifetime_seconds
        self._margin = renewal_margin_seconds
        self._cached_token: Optional[str] = None
        self._cached_exp = 0.0

    @property
    def qualified_username(self) -> str:
        """``{ACCOUNT}.{USER}`` - the JWT subject and issuer stem."""
        return f"{self.account_claim}.{self.user}"

    @property
    def issuer(self) -> str:
        """``{ACCOUNT}.{USER}.SHA256:{fingerprint}`` - the JWT issuer claim."""
        return f"{self.qualified_username}.{self._fingerprint}"

    def _generate(self, now: float) -> str:
        payload = {
            "iss": self.issuer,
            "sub": self.qualified_username,
            "iat": int(now),
            "exp": int(now) + self._lifetime,
        }
        return jwt.encode(payload, self._key, algorithm="RS256")  # type: ignore[arg-type]

    def token(self, now: Optional[float] = None) -> str:
        """Return a valid JWT, signing a fresh one if the cache is near expiry.

        ``now`` is injectable for tests; production passes ``None`` (wall clock).
        """
        current = time.time() if now is None else now
        if self._cached_token is not None and current < self._cached_exp - self._margin:
            return self._cached_token
        token = self._generate(current)
        self._cached_token = token
        self._cached_exp = current + self._lifetime
        return token
