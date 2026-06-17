"""Keypair (JWT) authentication for the Snowflake SQL API.

Snowflake's SQL API authenticates with a short-lived RS256 JWT signed by the
user's private key. This module owns JWT generation, the account-locator
normalization that the issuer claim requires, and a small token cache so a
client reuses a valid token instead of re-signing on every request.

Scaffold only: implemented in Phase 2 (the auth matrix, including regression
bug #1 - stripping the region/cloud suffix from the account locator).
"""

from __future__ import annotations

from typing import Optional, Union

__all__ = ["normalize_account_locator", "KeypairAuthenticator"]

PrivateKeyInput = Union[str, bytes]


def normalize_account_locator(account: str) -> str:
    """Return the account locator with any region/cloud suffix stripped.

    The JWT issuer claim uses the bare account locator (uppercased). An account
    like ``xy12345.ap-southeast-2`` or ``xy12345.ap-southeast-2.aws`` must
    collapse to ``XY12345``; leaving the region in breaks signature validation
    (regression bug #1). Org-account form (``myorg-myaccount``) is preserved.

    Implemented in Phase 2.
    """
    raise NotImplementedError


class KeypairAuthenticator:
    """Generates and caches RS256 JWTs for SQL API requests.

    Implemented in Phase 2: public-key ``SHA256:`` fingerprint derivation,
    encrypted private keys (passphrase), <= 1 h lifetime, clock-skew tolerance,
    and token refresh.
    """

    def __init__(
        self,
        account: str,
        user: str,
        private_key: PrivateKeyInput,
        *,
        private_key_passphrase: Optional[str] = None,
        lifetime_seconds: int = 3600,
    ) -> None:
        self.account = account
        self.user = user
        self._private_key = private_key
        self._passphrase = private_key_passphrase
        self._lifetime_seconds = lifetime_seconds

    def token(self) -> str:
        """Return a valid JWT, signing a fresh one if the cache has expired."""
        raise NotImplementedError
