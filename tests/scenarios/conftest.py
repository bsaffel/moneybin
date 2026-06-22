"""Shared fixtures for scenario tests.

Provides an ephemeral encryption key (the in-memory keyring backend itself
comes from the root conftest's autouse net) so the runner can open encrypted
scenario tempdirs without touching the real keychain or requiring the
production encryption key.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest

import moneybin.database as db_module
from tests.e2e.conftest import FAST_ARGON2_ENV


@pytest.fixture(autouse=True)
def _scenario_encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Set ephemeral encryption key plus subprocess keyring/PYTHONPATH from FAST_ARGON2_ENV."""
    monkeypatch.setenv(
        "MONEYBIN_DATABASE__ENCRYPTION_KEY",
        "scenario-ephemeral-key-tmpdir-only",
    )
    for key, value in FAST_ARGON2_ENV.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(autouse=True)
def _reset_database_module_state() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Reset database module-level state before each scenario test.

    Prevents _cached_encryption_key and _migration_check_done from bleeding
    across scenario tests when running sequentially in the same process.
    """
    db_module._cached_encryption_key = None  # pyright: ignore[reportPrivateUsage]
    db_module._active_write_conn = None  # pyright: ignore[reportPrivateUsage]
    db_module._migration_check_done = set()  # pyright: ignore[reportPrivateUsage]
    db_module._database_accessed = False  # pyright: ignore[reportPrivateUsage]
    db_module._database_written = False  # pyright: ignore[reportPrivateUsage]
    yield
