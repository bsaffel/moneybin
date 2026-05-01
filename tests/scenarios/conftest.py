"""Shared fixtures for scenario tests.

Scenarios are slow, real-DB, real-SQLMesh checks. They run in CI under a
single concurrency group and locally via ``make verify-scenarios``. The
fixtures here provide:

- An in-memory keyring so ``SecretStore`` works without a system backend.
- An ephemeral encryption-key env var so ``Database`` can encrypt the
  scenario tempdir's DuckDB file without needing the real key.
"""

from __future__ import annotations

from collections.abc import Generator

import keyring
import pytest

from tests.e2e.memory_keyring import MemoryKeyring


@pytest.fixture(autouse=True)
def _scenario_keyring() -> Generator[None, None, None]:  # type: ignore[reportUnusedFunction]
    """Swap in the dict-backed keyring for every scenario test."""
    previous = keyring.get_keyring()
    keyring.set_keyring(MemoryKeyring())
    try:
        yield
    finally:
        MemoryKeyring.clear()
        keyring.set_keyring(previous)


@pytest.fixture(autouse=True)
def _scenario_encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:  # type: ignore[reportUnusedFunction]
    """Provide an ephemeral encryption key when the system has none."""
    monkeypatch.setenv(
        "MONEYBIN_DATABASE__ENCRYPTION_KEY",
        "scenario-ephemeral-key-tmpdir-only",
    )
