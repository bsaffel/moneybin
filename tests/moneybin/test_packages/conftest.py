"""Fixtures for framework integration tests.

The synthetic test package isn't pip-installed; we mount its directory on
sys.path for the test session so `import test_synthetic` works without
forcing an editable install. This mirrors how pytest-installed plugins
self-discover and avoids per-developer setup.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session", autouse=True)
def _mount_synthetic_package() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    """Add fixtures/ to sys.path so test_synthetic imports work."""
    sys.path.insert(0, str(_FIXTURES))
    yield
    sys.path.remove(str(_FIXTURES))


@pytest.fixture
def synthetic_package_root() -> Path:
    """Filesystem root of the synthetic test package."""
    return _FIXTURES / "test_synthetic"
