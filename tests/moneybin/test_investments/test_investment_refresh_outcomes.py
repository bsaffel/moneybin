"""CLI exit status and text counts honor the investment stage outcome."""

from collections.abc import Generator
from contextlib import contextmanager

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


@pytest.fixture
def refresh_database(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> Database:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr("moneybin.database.get_database", database_context)
    return comparison_db


@pytest.mark.parametrize("flags", [[], ["--output", "json"], ["--quiet"]])
def test_blocked_requested_transform_exits_nonzero(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    flags: list[str],
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected planner failure")

    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    result = CliRunner().invoke(app, ["refresh", "--step", "transform", *flags])
    assert result.exit_code == 1, result.output


def test_normal_refresh_text_shows_investment_pending_counts(
    refresh_database: Database,
) -> None:
    result = CliRunner().invoke(app, ["refresh", "--step", "investment_match"])
    assert result.exit_code == 0, result.output
    assert "1 unique" in result.output
    assert "0 competing" in result.output
