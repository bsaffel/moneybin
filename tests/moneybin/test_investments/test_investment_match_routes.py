"""Existing refresh and reviews routes expose durable investment Proposals."""

import json
from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace

import duckdb
import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.orchestration.refresh import expand_steps, refresh
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def _seed(db: Database) -> None:
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    install_comparison_models(db)


def test_catalog_failure_is_a_real_planner_error(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)

    def fail(*args: object, **kwargs: object) -> None:
        raise duckdb.CatalogException("injected missing repository object")

    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    stage = refresh(comparison_db, steps=["investment_match"]).stages[0]
    assert stage.ran
    assert stage.error is not None


def test_cli_pending_text_displays_review_evidence(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)
    refresh(comparison_db, steps=["investment_match"])

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    result = CliRunner().invoke(app, ["investments", "matches", "pending"])
    assert result.exit_code == 0, result.output
    for field in (
        "quantity",
        "amount",
        "Evidence",
        "Downstream",
        "golden_membership_changed",
    ):
        assert field in result.output


def test_bounded_refresh_persists_proposals_without_transform(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    result = refresh(comparison_db, steps=["investment_match"])
    assert not result.applied
    assert [stage.step for stage in result.stages] == ["investment_match"]
    assert result.stages[0].ran
    assert result.stages[0].counts == {
        "pending_unique": 1,
        "pending_competing": 0,
        "stale": 0,
        "suppressed": 0,
    }
    assert comparison_db.execute(
        "SELECT status FROM app.investment_match_decisions"
    ).fetchall() == [("pending",)]


def test_transform_selector_transitively_plans(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)

    def apply_transform(_service: object) -> SimpleNamespace:
        return SimpleNamespace(applied=True, error=None, duration_seconds=0)

    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService.apply", apply_transform
    )
    assert "investment_match" in expand_steps(["transform"])
    result = refresh(comparison_db, steps=["transform"])
    assert [stage.step for stage in result.stages][:2] == [
        "investment_match",
        "transform",
    ]
    assert comparison_db.execute(
        "SELECT COUNT(*) FROM app.investment_match_decisions"
    ).fetchone() == (1,)


def test_cli_run_uses_same_bounded_planner(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    result = CliRunner().invoke(
        app, ["investments", "matches", "run", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["stages"][0]["step"] == "investment_match"
    assert comparison_db.execute(
        "SELECT COUNT(*) FROM app.investment_match_decisions"
    ).fetchone() == (1,)


@pytest.mark.asyncio
async def test_reviews_kind_reads_persisted_evidence(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.mcp.tools.reviews import reviews_coarse

    _seed(comparison_db)
    refresh(comparison_db, steps=["investment_match"])

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr("moneybin.mcp.tools.reviews.get_database", database_context)
    response = await reviews_coarse(kind="investment_matches")
    assert response.data is not None
    payload = response.data.model_dump()
    assert payload["kind"] == "investment_matches"
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["details"]["legs"]
    assert payload["rows"][0]["details"]["evidence"]


def test_pending_proposals_advertise_the_existing_reviews_route(
    comparison_db: Database,
) -> None:
    from moneybin.adapters.refresh_adapters import refresh_envelope

    _seed(comparison_db)
    result = refresh(comparison_db, steps=["investment_match"])
    envelope = refresh_envelope(result, requested=frozenset({"investment_match"}))
    assert any(
        'reviews(kind="investment_matches")' in action for action in envelope.actions
    )
