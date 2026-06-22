"""The test suite must never touch the real OS keyring.

A global autouse net (``tests/conftest.py:_in_memory_keyring``) swaps the
platform keychain for an in-memory backend so no test — nor the production
code it drives — reaches the real macOS Keychain / Linux Secret Service.
The real keychain prompts or denies under sandbox + headless CI (the
``PasswordSetError -60008`` this guard prevents) and is platform-specific.
"""

from __future__ import annotations

import keyring

from tests.e2e.memory_keyring import MemoryKeyring


def test_suite_runs_on_in_memory_keyring() -> None:
    """The active keyring backend is the in-memory test backend, not the OS one."""
    assert isinstance(keyring.get_keyring(), MemoryKeyring)
