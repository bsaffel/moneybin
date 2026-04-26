"""Shared fixtures for integration tests."""

from collections.abc import Generator

import pytest

from moneybin.config import clear_settings_cache, set_current_profile


@pytest.fixture(autouse=True)
def _set_test_profile() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Ensure a profile is set for integration tests that call get_settings()."""
    clear_settings_cache()
    set_current_profile("test")
    yield
    clear_settings_cache()
