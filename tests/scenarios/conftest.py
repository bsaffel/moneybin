"""Shared fixtures for scenario tests.

Uses an in-memory keyring + ephemeral encryption key so the runner can open
encrypted scenario tempdirs without touching the real keychain or requiring
the production encryption key.
"""

from __future__ import annotations

from collections.abc import Generator

import keyring
import pytest

import moneybin.database as db_module
from tests.e2e.conftest import FAST_ARGON2_ENV
from tests.e2e.memory_keyring import MemoryKeyring


@pytest.fixture(autouse=True)
def _scenario_keyring() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Swap in the dict-backed keyring for every scenario test."""
    previous = keyring.get_keyring()
    keyring.set_keyring(MemoryKeyring())
    try:
        yield
    finally:
        MemoryKeyring.clear()
        keyring.set_keyring(previous)


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
    db_module._migration_check_done = set()  # pyright: ignore[reportPrivateUsage]
    db_module._database_accessed = False  # pyright: ignore[reportPrivateUsage]
    db_module._database_written = False  # pyright: ignore[reportPrivateUsage]
    yield
