"""Shared pytest fixtures.

The default suite mocks the HTTP layer (via ``respx``) and needs no network
access. Live fixtures (a real account + keypair) arrive with the smoke suite in
Phase 2; they are skipped unless ``SNOWFLAKE_*`` env vars are set.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def fake_account() -> str:
    """A region-suffixed account locator for auth-normalization tests."""
    return "xy12345.ap-southeast-2"
