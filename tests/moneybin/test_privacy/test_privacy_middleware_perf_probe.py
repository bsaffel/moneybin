"""Regression coverage for the privacy perf persona availability probe."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, DatabaseKeyError, DatabaseNotInitializedError
from moneybin.services.budget_service import BudgetService
from tests.scenarios import test_privacy_middleware_perf as perf

pytestmark = pytest.mark.unit


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
    """A genuinely absent persona DB is reported as unavailable."""

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
    """Running perf tests without MONEYBIN_PROFILE reports setup failure."""

    @contextmanager
    def _raise_no_profile(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise RuntimeError("No profile set. Call set_current_profile() first.")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_no_profile)

    reason = perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]

    assert reason is not None
    assert "requires an active MoneyBin profile" in reason


def test_persona_probe_skips_missing_fct_transactions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An untransformed DB with no core table reports setup failure."""
    import duckdb

    @contextmanager
    def _raise_missing_table(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise duckdb.CatalogException(
            "Table with name fct_transactions does not exist!"
        )
        yield

    monkeypatch.setattr(perf, "get_database", _raise_missing_table)

    reason = perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]

    assert reason is not None
    assert "core.fct_transactions" in reason


def test_persona_probe_skips_missing_core_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An initialized but untransformed DB reports setup failure."""
    import duckdb

    @contextmanager
    def _raise_missing_schema(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise duckdb.CatalogException("Schema with name core does not exist!")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_missing_schema)

    reason = perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]

    assert reason is not None
    assert "core.fct_transactions" in reason


def test_persona_probe_reraises_unrelated_catalog_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected catalog failures are infrastructure errors, not setup skips."""
    import duckdb

    @contextmanager
    def _raise_unrelated_catalog_error(
        *, read_only: bool
    ) -> Generator[object, None, None]:
        assert read_only is True
        raise duckdb.CatalogException("Catalog Error: unexpected schema failure")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_unrelated_catalog_error)

    with pytest.raises(duckdb.CatalogException, match="unexpected schema failure"):
        perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]


def test_persona_probe_reraises_malformed_profile_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed profile config is an infrastructure error, not a setup skip."""

    @contextmanager
    def _raise_config_error(*, read_only: bool) -> Generator[object, None, None]:
        assert read_only is True
        raise ValueError("Configuration error for profile 'bad': invalid path")
        yield

    monkeypatch.setattr(perf, "get_database", _raise_config_error)

    with pytest.raises(ValueError, match="Configuration error"):
        perf._persona_db_skip_reason()  # pyright: ignore[reportPrivateUsage]


def test_perf_budget_seed_uses_service_and_requires_categories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The budget perf flow must exercise an active, nonempty status payload."""
    service = MagicMock()
    service.status.return_value = SimpleNamespace(categories=[object()])

    def _budget_service(_: Database) -> BudgetService:
        return cast(BudgetService, service)

    monkeypatch.setattr(perf, "BudgetService", _budget_service)

    perf._seed_active_perf_budget(cast(Database, object()))  # pyright: ignore[reportPrivateUsage]

    service.set_budget.assert_called_once()
    service.status.assert_called_once()


def test_perf_budget_seed_rejects_empty_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty budget status cannot silently become a timed perf flow."""
    service = MagicMock()
    service.status.return_value = SimpleNamespace(categories=[])

    def _budget_service(_: Database) -> BudgetService:
        return cast(BudgetService, service)

    monkeypatch.setattr(perf, "BudgetService", _budget_service)

    with pytest.raises(AssertionError, match="active category"):
        perf._seed_active_perf_budget(cast(Database, object()))  # pyright: ignore[reportPrivateUsage]


def test_raw_static_callback_reuses_the_shared_worker_boundary() -> None:
    """Static raw samples retain the decorator's sync-worker boundary."""
    main_thread = threading.get_ident()

    with asyncio.Runner() as runner:
        first_worker = perf._run_static_raw(  # pyright: ignore[reportPrivateUsage]
            runner, threading.get_ident
        )
        second_worker = perf._run_static_raw(  # pyright: ignore[reportPrivateUsage]
            runner, threading.get_ident
        )

    assert first_worker != main_thread
    assert second_worker == first_worker


def test_perf_timing_summary_is_visible_at_warning_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Successful CI output retains all five flow timings for later diagnosis."""
    measurements = [
        ("transactions_get", 1.0, 2.0, 3.0, 4.0, 2.0, 2.0),
        ("reports_spending", 1.0, 2.0, 3.0, 4.0, 2.0, 2.0),
        ("accounts", 1.0, 2.0, 3.0, 4.0, 2.0, 2.0),
        ("budget_status_service", 1.0, 2.0, 3.0, 4.0, 2.0, 2.0),
        ("reports_networth_history", 1.0, 2.0, 3.0, 4.0, 2.0, 2.0),
    ]

    with caplog.at_level(logging.WARNING, logger=perf.__name__):
        perf._log_timing_summary(  # pyright: ignore[reportPrivateUsage]
            measurements, total_raw_p50=5.0, total_protected_p50=6.0, total_pct=20.0
        )

    assert caplog.records[-1].levelno == logging.WARNING
    for name, *_ in measurements:
        assert name in caplog.text
