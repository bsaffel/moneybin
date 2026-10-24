"""Investment pending counts remain visible in the cross-domain CLI queue."""

import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.review_service import ReviewService
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


def test_status_total_includes_real_pending_investment_proposals(
    comparison_db: Database,
) -> None:
    planned_investment(comparison_db, competing=True)
    empty = MagicMock()
    empty.count_pending.return_value = 0
    empty.count_uncategorized.return_value = 0
    service: Any = ReviewService
    result = service(
        empty,
        empty,
        empty,
        empty,
        empty,
        investment_matching_service=InvestmentMatchingService(comparison_db),
    ).status()
    assert result.investment_matches_pending == 2
    assert result.total == 2


@pytest.mark.parametrize("output", ["text", "json"])
def test_cli_investment_status_reads_its_queue(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, output: str
) -> None:
    planned_investment(comparison_db, competing=True)

    @contextmanager
    def connection(*args: Any, **kwargs: Any) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.review.get_database", connection
    )
    args = ["review", "--type", "investment-matches", "--status"]
    if output == "json":
        args += ["--output", "json"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    if output == "json":
        assert json.loads(result.stdout)["data"]["investment_matches_pending"] == 2
    else:
        assert "Investment matches pending: 2" in result.output
