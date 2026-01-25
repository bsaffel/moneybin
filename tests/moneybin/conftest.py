"""Shared pytest fixtures for moneybin tests.

This module provides common fixtures and test utilities used across
the test suite, including profile cleanup and configuration management.
"""

import shutil
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from moneybin.config import clear_settings_cache, set_current_profile


@contextmanager
def temp_profile(profile: str) -> Generator[str, None, None]:
    """Context manager for automatic profile cleanup.

    Use this in tests to automatically clean up profile directories:

    Example:
        def test_something():
            with temp_profile("alice"):
                set_current_profile("alice")
                # ... test code ...
                # automatic cleanup happens when exiting the with block

    Args:
        profile: Profile name to clean up (will be normalized)

    Yields:
        The normalized profile name

    The context manager handles:
    - Profile name normalization (using the same normalization as the config system)
    - Cleanup of data/ and logs/ directories after the with block
    - Works with any profile name, no central list needed
    """
    from moneybin.utils.user_config import normalize_profile_name

    # Use the project-wide normalization function for profile names
    normalized = normalize_profile_name(profile)

    try:
        # Yield the normalized profile name to the test
        yield normalized
    finally:
        # Clean up data directory for this profile
        # Don't check exists() because tests may mock it - just try to remove
        data_dir = Path.cwd() / "data" / normalized
        try:
            shutil.rmtree(data_dir)
        except FileNotFoundError:
            pass  # Directory doesn't exist, nothing to clean up

        # Clean up logs directory for this profile
        logs_dir = Path.cwd() / "logs" / normalized
        try:
            shutil.rmtree(logs_dir)
        except FileNotFoundError:
            pass  # Directory doesn't exist, nothing to clean up


@pytest.fixture(autouse=True)
def clean_profile_state() -> Generator[None, None, None]:
    """Automatically clean up profile state before and after each test.

    This fixture:
    - Runs for every test automatically (autouse=True)
    - Clears the settings cache to prevent test pollution
    - Resets current profile to 'test'

    This ensures tests are isolated and don't affect each other.

    For profile directory cleanup, use the temp_profile() context manager.
    """
    # Setup: clean state before test
    clear_settings_cache()
    set_current_profile("test")

    # Yield to run the test
    yield

    # Cleanup after test
    clear_settings_cache()
    set_current_profile("test")
