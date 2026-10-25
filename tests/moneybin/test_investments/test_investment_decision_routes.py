"""CLI and MCP decide the same whole Proposal with human confirmation."""

import asyncio
import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import AcceptedElicitation, DeclinedElicitation
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.mcp.tools.reviews import reviews_decide_coarse
from moneybin.tables import AUDIT_LOG
from tests.moneybin.test_investments.comparison_helpers import seed_plaid_event
from tests.moneybin.test_investments.test_investment_decision_contract import (
    investment_request,
)
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


def use_decision_database(monkeypatch: pytest.MonkeyPatch, db: Database) -> None:
    @contextmanager
    def connection(*args: Any, **kwargs: Any) -> Generator[Database, None, None]:
        yield db

    monkeypatch.setattr("moneybin.mcp.tools.reviews.get_database", connection)
    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.review.get_database", connection
    )


@pytest.mark.parametrize("output", ["text", "json"])
def test_cli_rejects_whole_investment_proposal(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, output: str
) -> None:
    service, decision_id = planned_investment(comparison_db)
    use_decision_database(monkeypatch, comparison_db)
    result = CliRunner().invoke(
        app,
        [
            "review",
            "--type",
            "investment-matches",
            "--reject",
            decision_id,
            "--output",
            output,
        ],
    )
    assert result.exit_code == 0, result.output
    assert service.history()[0]["status"] == "rejected"
    if output == "json":
        data = json.loads(result.stdout)["data"]
        assert data["applied_count"] == 1
        assert data["results"][0]["decision_id"] == decision_id
        assert data["operation_id"]


@pytest.mark.parametrize(
    "args",
    [
        ["--field-choice", "a=b"],
        ["--type", "matches", "--confirm", "one", "--field-choice", "a=b"],
        ["--type", "investment-matches", "--reject", "one", "--field-choice", "a=b"],
        [
            "--type",
            "investment-matches",
            "--confirm",
            "one",
            "--confirm",
            "two",
            "--field-choice",
            "a=b",
        ],
        ["--type", "investment-matches", "--confirm-all"],
        ["--type", "all", "--confirm-all"],
    ],
)
def test_cli_invalid_combinations_cannot_open_database(
    monkeypatch: pytest.MonkeyPatch, args: list[str]
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("invalid CLI request reached database")

    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.review.get_database", forbidden
    )
    result = CliRunner().invoke(app, ["review", *args])
    assert result.exit_code == 2, result.output


@pytest.mark.parametrize(
    "response", ["accept", "decline", "unsupported", "drift", "timeout"]
)
@pytest.mark.asyncio
async def test_mcp_rejection_requires_exact_human_prompt(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, response: str
) -> None:
    service, decision_id = planned_investment(comparison_db)
    use_decision_database(monkeypatch, comparison_db)
    messages: list[str] = []
    audit_before = comparison_db.execute(
        f"""SELECT * FROM {AUDIT_LOG.full_name}"""  # noqa: S608  # fixed TableRef
    ).fetchall()

    async def elicit(message: str, **kwargs: Any) -> Any:
        messages.append(message)
        if response == "timeout":
            await asyncio.sleep(1)
        if response == "drift":
            seed_plaid_event(comparison_db, "new-alternative")
        return (
            DeclinedElicitation()
            if response == "decline"
            else AcceptedElicitation(data=True)
        )

    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=elicit)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)

    def supports(_context: object) -> bool:
        return response != "unsupported"

    monkeypatch.setattr("moneybin.mcp.confirmation.supports_elicitation", supports)
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.elicitation_wait_seconds", lambda: 0.01
    )
    result = await reviews_decide_coarse([investment_request(decision_id)])
    if response == "accept":
        assert result.data is not None
        assert result.data.applied_count == 1
        assert service.history()[0]["status"] == "rejected"
        assert len(messages) == 1
        assert decision_id in messages[0]
        assert "superset" in messages[0].lower()
        assert "****ount" in messages[0]
    else:
        assert result.error is not None
        assert service.pending()[0]["status"] == "pending"
        assert not any(row["status"] == "rejected" for row in service.history())
        assert (
            comparison_db.execute(
                f"""SELECT * FROM {AUDIT_LOG.full_name}"""  # noqa: S608  # fixed TableRef
            ).fetchall()
            == audit_before
        )
        if response == "timeout":
            assert result.error.code == "mutation_confirmation_declined"
            assert result.error.details == {"reason": "timeout"}
        if response == "unsupported":
            assert result.error.code == "mutation_confirmation_required"
            assert "confirmation_token" not in str(result.error.details)
