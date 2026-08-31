"""Shared fixtures for integration tests."""

from collections.abc import Generator
from unittest.mock import MagicMock

import pytest

from moneybin.config import clear_settings_cache, set_current_profile

TEST_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def make_secret_store() -> MagicMock:
    """Return an in-memory secret store for encrypted integration databases."""
    store = MagicMock()
    store.get_key.return_value = TEST_ENCRYPTION_KEY
    return store


@pytest.fixture(autouse=True)
def _set_test_profile() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Ensure a profile is set for integration tests that call get_settings()."""
    clear_settings_cache()
    set_current_profile("test")
    yield
    clear_settings_cache()
