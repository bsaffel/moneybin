"""Regression coverage for the privacy perf persona availability probe."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

import pytest

from moneybin.database import DatabaseKeyError, DatabaseNotInitializedError
from tests.scenarios import test_privacy_middleware_perf as perf


def test_persona_probe_does_not_skip_database_key_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sandbox/keychain failures must fail visibly, not become skipped perf tests."""

    @contextmanager
    def _raise_key_error(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise DatabaseKeyError("key unavailable")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_key_error)

    with pytest.raises(DatabaseKeyError, match="key unavailable"):
        perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]


def test_persona_probe_skips_missing_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuinely absent persona DB remains an intentional perf-test skip."""

    @contextmanager
    def _raise_missing_db(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise DatabaseNotInitializedError("missing database")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_missing_db)

    reason = perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]

    assert reason is not None
    assert "requires a populated persona DB" in reason
    assert "moneybin synthetic generate family" in reason


def test_persona_probe_skips_missing_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Running perf tests without MONEYBIN_PROFILE is a setup skip."""

    @contextmanager
    def _raise_no_profile(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise RuntimeError("No profile set. Call set_current_profile() first.")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_no_profile)

    reason = perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]

    assert reason is not None
    assert "requires an active MoneyBin profile" in reason
