"""Shared fixtures for scenario tests.

Scenarios are slow, real-DB, real-SQLMesh checks. They run in CI under a
single concurrency group and locally via ``make verify-scenarios``. The
fixtures here provide:

- An in-memory keyring so ``SecretStore`` works without a system backend.
- An ephemeral encryption-key env var so ``Database`` can encrypt the
  scenario tempdir's DuckDB file without needing the real key.
- Environment propagation for subprocess steps so they can access the keyring
  and encryption key.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import keyring
import pytest

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
    """Provide ephemeral encryption key + subprocess keyring config."""
    monkeypatch.setenv(
        "MONEYBIN_DATABASE__ENCRYPTION_KEY",
        "scenario-ephemeral-key-tmpdir-only",
    )
    # Configure subprocess to use MemoryKeyring and include tests in PYTHONPATH.
    monkeypatch.setenv(
        "PYTHON_KEYRING_BACKEND",
        "tests.e2e.memory_keyring.MemoryKeyring",
    )
    monkeypatch.setenv(
        "PYTHONPATH",
        str(Path(__file__).resolve().parent.parent.parent)
        + os.pathsep
        + os.environ.get("PYTHONPATH", ""),
    )
