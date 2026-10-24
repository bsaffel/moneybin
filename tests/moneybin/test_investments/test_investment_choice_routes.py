"""CLI and raw MCP requests validate the same current observed choices."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.mcp.tools.reviews import register_review_coarse_writes
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.tables import AUDIT_LOG
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_decision_routes import (
    use_decision_database,
)
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)
from tests.moneybin.test_mcp.schema_assertions import call_tool_raw, isolated_server


def _conflicting_dividend(db: Database) -> InvestmentMatchingService:
    seed_manual_event(
        db,
        "manual",
        type_="dividend",
        subtype="qualified",
        quantity=None,
        amount="100",
        price=None,
    )
    seed_plaid_event(
        db,
        "native",
        type_="cash",
        subtype="non-qualified dividend",
        quantity=None,
        amount="-100",
    )
    install_comparison_models(db)
    service = InvestmentMatchingService(db)
    service.run()
    assert len(service.pending()) == 1
    return service


def _choices(proposal: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "conflict_id": conflict["conflict_id"],
            "choice_id": next(
                choice["choice_id"]
                for choice in conflict["choices"]
                if choice["source_type"] == "plaid"
            ),
        }
        for conflict in proposal["field_choices"]
    ]


def _revise_dividend(db: Database) -> None:
    revised = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 10),
        name="Revised synthetic context",
        quantity=None,
        amount=Decimal("-100"),
        price=Decimal("100"),
        fees=Decimal("0"),
        type="cash",
        subtype="non-qualified dividend",
    )
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
        [revised],
        "sync_revision",
        datetime(2026, 2, 2, tzinfo=UTC),
        datetime(2026, 2, 2, tzinfo=UTC),
    )


@pytest.mark.parametrize("surface", ["cli", "mcp"])
@pytest.mark.parametrize(
    "selection",
    [
        "complete",
        "missing",
        "duplicate",
        "unknown_conflict",
        "unknown_choice",
        "stale",
        "no_conflict_empty",
    ],
)
@pytest.mark.asyncio
async def test_acceptance_choice_validation_matches_across_routes_without_writes(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    selection: str,
) -> None:
    if selection == "no_conflict_empty":
        service, _ = planned_investment(comparison_db)
    else:
        service = _conflicting_dividend(comparison_db)
    proposal = service.pending()[0]
    choices = _choices(proposal)
    assert bool(choices) == (selection != "no_conflict_empty")
    if selection == "missing":
        choices = []
    elif selection == "duplicate":
        choices += [dict(choices[0])]
    elif selection == "unknown_conflict":
        choices[0]["conflict_id"] = "unknown-conflict"
    elif selection == "unknown_choice":
        choices[0]["choice_id"] = "unknown-choice"
    elif selection == "stale":
        _revise_dividend(comparison_db)
        service.run()
        proposal = service.pending()[0]
        assert _choices(proposal) != choices

    use_decision_database(monkeypatch, comparison_db)
    pending_before, history_before = service.pending(), service.history()
    audit_before = comparison_db.execute(
        f"SELECT * FROM {AUDIT_LOG.full_name}"  # noqa: S608  # fixed TableRef
    ).fetchall()
    comparison_db.execute(
        "CREATE TABLE core.fct_investment_transactions AS SELECT 42 AS sentinel"
    )
    valid = selection in {"complete", "no_conflict_empty"}
    if surface == "cli":
        arguments = [
            "review",
            "--type",
            "investment-matches",
            "--confirm",
            proposal["proposal_id"],
            "--output",
            "json",
        ]
        for choice in choices:
            arguments += [
                "--field-choice",
                f"{choice['conflict_id']}={choice['choice_id']}",
            ]
        result = CliRunner().invoke(app, arguments)
        assert result.exit_code == (2 if selection == "duplicate" else 1), result.output
        message = result.output.lower()
        if selection == "duplicate":
            assert "invalid investment decision or field choices" in message
        else:
            error = json.loads(result.stdout)["error"]
            assert error["code"] == "mutation_invalid_input"
            message = error["details"]["errors"][0]["reason"].lower()
            assert ("acceptance is unavailable" in message) == valid, message
            if not valid:
                assert "investment field conflict" in message
    else:
        result = await call_tool_raw(
            isolated_server(register_review_coarse_writes),
            "reviews_decide",
            {
                "decisions": [
                    {
                        "kind": "investment_match",
                        "decision_id": proposal["proposal_id"],
                        "decision": "accept",
                        "field_choices": choices,
                    }
                ]
            },
        )
        if selection == "duplicate":
            assert result.isError is True
        else:
            assert result.structuredContent is not None
            error = result.structuredContent["error"]
            assert error["code"] == "mutation_invalid_input"
            message = error["details"]["errors"][0]["reason"].lower()
            assert ("acceptance is unavailable" in message) == valid, message
            if not valid:
                assert "investment field conflict" in message

    assert service.pending() == pending_before
    assert service.history() == history_before
    assert (
        comparison_db.execute(
            f"SELECT * FROM {AUDIT_LOG.full_name}"  # noqa: S608  # fixed TableRef
        ).fetchall()
        == audit_before
    )
    assert comparison_db.execute(
        "SELECT * FROM core.fct_investment_transactions"
    ).fetchall() == [(42,)]
