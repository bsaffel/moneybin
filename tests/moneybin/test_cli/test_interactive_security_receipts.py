"""Security review receipts retain decisions committed before refresh failure."""

import re
from contextlib import nullcontext
from dataclasses import replace
from unittest.mock import patch

import pytest
import typer

from moneybin import error_codes
from moneybin.cli.commands.interactive_review import run_review
from moneybin.cli.utils import get_terminal_policy
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.security_links_service import SecurityLinksService

pytestmark = pytest.mark.unit


def _seed(db: Database, ref_kind: str) -> str:
    target = (
        SecuritiesRepo(db)
        .upsert(
            security_id=None,
            name="Synthetic survivor",
            security_type="etf",
            actor="test",
        )
        .target_id
    )
    assert target is not None
    if ref_kind == "plaid_security_id":
        provisional = (
            SecuritiesRepo(db)
            .upsert(
                security_id=None,
                name="Synthetic provisional",
                security_type="etf",
                created_by="plaid",
                actor="test",
            )
            .target_id
        )
        assert provisional is not None
        SecurityLinksRepo(db).insert(
            security_id=provisional,
            ref_kind=ref_kind,
            ref_value="synthetic-ref",
            source_type="plaid",
            decided_by="auto",
            actor="test",
        )
    SecurityLinkDecisionsRepo(db).insert(
        decision_id="selected-decision",
        ref_kind=ref_kind,
        ref_value="synthetic-ref",
        source_type="plaid" if ref_kind == "plaid_security_id" else "tiingo",
        candidate_security_id=target,
        actor="test",
    )
    return target


@pytest.mark.parametrize("ref_kind", ["tiingo_ticker", "plaid_security_id"])
def test_security_refresh_failure_counts_real_committed_accept(
    db: Database,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    ref_kind: str,
) -> None:
    target = _seed(db, ref_kind)
    error = UserError(
        "The security was committed, but reports could not be rebuilt.",
        code=error_codes.REFRESH_MODEL_FAILED,
        hint="Run 'moneybin refresh' before relying on holdings.",
    )
    refresh_cause = RuntimeError("Synthetic rebuild failure")
    error.__cause__ = refresh_cause
    with (
        patch(
            "moneybin.cli.commands.interactive_review.get_database",
            return_value=nullcontext(db),
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            side_effect=["accept", "selected-decision"],
        ),
        patch(
            "moneybin.services.security_links_service.restate_after_accept",
            side_effect=error,
        ),
    ):
        with pytest.raises(typer.Exit) as stopped:
            run_review(
                "security-links", 1, replace(get_terminal_policy(), interactive=True)
            )
    assert stopped.value.exit_code == 1
    decision = SecurityLinkDecisionsRepo(db).fetch_by_id("selected-decision")
    assert decision is not None and decision["status"] == "accepted"
    assert decision["candidate_security_id"] == target
    captured = capsys.readouterr()
    assert re.search(r"Decisions saved:?\s+1\b", captured.out)
    assert "stopped" in captured.out
    assert "moneybin refresh" in captured.out + captured.err
    # The standard text handler logs primary errors and prints recovery hints.
    assert "could not be rebuilt" in caplog.text
    assert stopped.value.__cause__ is error
    assert error.__cause__ is refresh_cause


@pytest.mark.parametrize(
    "code",
    [error_codes.REFRESH_MODEL_FAILED, error_codes.MUTATION_CONSTRAINT_VIOLATION],
)
def test_security_failure_without_commit_does_not_count_saved_decision(
    db: Database, capsys: pytest.CaptureFixture[str], code: str
) -> None:
    _seed(db, "tiingo_ticker")
    with (
        patch(
            "moneybin.cli.commands.interactive_review.get_database",
            return_value=nullcontext(db),
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            side_effect=["accept", "selected-decision"],
        ),
        patch.object(
            SecurityLinksService,
            "accept",
            side_effect=UserError("Refused before commit", code=code),
        ),
    ):
        with pytest.raises(typer.Exit) as stopped:
            run_review(
                "security-links", 1, replace(get_terminal_policy(), interactive=True)
            )
    assert stopped.value.exit_code == 1
    decision = SecurityLinkDecisionsRepo(db).fetch_by_id("selected-decision")
    assert decision is not None and decision["status"] == "pending"
    assert re.search(r"Decisions saved:?\s+0\b", capsys.readouterr().out)
