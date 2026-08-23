"""The one-line summaries an agent reads before opening an account-link decision.

Both rows are a *choice* surface: an agent scanning ``reviews`` picks which
decision to open from the summary alone. A summary that renders an opaque id is
asking the agent to choose between two strings it cannot tell apart.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database

# Reached privately on purpose. reviews.py holds seven sibling row-projections,
# all module-private; exporting two of them to suit a test would leave the module
# with two conventions for one job. The public entry point wraps these in consent
# and envelope machinery irrelevant to what the summary string says.
from moneybin.mcp.tools.reviews import (
    _account_link_history_rows,  # pyright: ignore[reportPrivateUsage]
    _pending_account_link_rows,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.account_resolver import UNNAMED_ACCOUNT_LABEL
from tests.moneybin.db_helpers import create_core_tables

_PROV = "prov1_acct000"
_CAND = "cand_a_acct00"
_DEC = "dec1_id000001"


@pytest.fixture()
def reviewed(db: Database) -> AccountLinksService:
    """One pending prov→cand decision, both accounts named in core."""
    create_core_tables(db)
    for account_id, name in ((_PROV, "Provisional One"), (_CAND, "Candidate Alpha")):
        db.execute(
            "INSERT INTO core.dim_accounts (account_id, display_name, source_type) "
            "VALUES (?, ?, ?)",
            [account_id, name, "csv"],
        )
    AccountLinkDecisionsRepo(db).insert(
        decision_id=_DEC,
        provisional_account_id=_PROV,
        candidate_account_id=_CAND,
        confidence_score=0.9,
        match_signals={"signal": "institution_last4", "value": "***"},
        decided_by="auto",
        actor="system",
        status="pending",
    )
    return AccountLinksService(db, actor="mcp")


@pytest.fixture()
def unnamed_provisional(db: Database) -> AccountLinksService:
    """Same decision, but nothing in core names the provisional account."""
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name, source_type) "
        "VALUES (?, ?, ?)",
        [_CAND, "Candidate Alpha", "csv"],
    )
    AccountLinkDecisionsRepo(db).insert(
        decision_id=_DEC,
        provisional_account_id=_PROV,
        candidate_account_id=_CAND,
        confidence_score=0.9,
        match_signals={"signal": "institution_last4", "value": "***"},
        decided_by="auto",
        actor="system",
        status="pending",
    )
    return AccountLinksService(db, actor="mcp")


def test_an_unnamed_account_reads_the_same_here_as_it_does_in_core(
    unnamed_provisional: AccountLinksService,
) -> None:
    """The MCP twin of the CLI history row, and it must not spell it differently.

    ``core.dim_accounts`` supplies ``UNNAMED_ACCOUNT_LABEL`` for a row it could
    not name; this projection substitutes the same string when no name comes
    back at all. Both can appear in one ``reviews`` result, so two spellings of
    one answer would read to an agent as two different states.
    """
    (row,) = _pending_account_link_rows(unnamed_provisional)

    assert UNNAMED_ACCOUNT_LABEL in row.summary
    assert _PROV in row.summary


def test_a_pending_summary_carries_the_name_and_the_id(
    reviewed: AccountLinksService,
) -> None:
    """The id is what the agent acts on; the name is what it chooses by.

    The summary carried one or the other — ``display_name or account_id`` — so a
    named account was unactionable from the summary and an unnamed one was
    unreadable. Both are needed, and neither is expensive.
    """
    (row,) = _pending_account_link_rows(reviewed)

    assert "Provisional One" in row.summary
    assert _PROV in row.summary


def test_a_history_summary_names_the_accounts_it_merged(
    reviewed: AccountLinksService,
) -> None:
    """History rendered two ids and nothing else — ``Account <id> to <id>``.

    A user asking what a past merge did gets back the two opaque strings they
    could not read at decision time either.
    """
    (row,) = _account_link_history_rows(reviewed)

    assert "Provisional One" in row.summary
    assert "Candidate Alpha" in row.summary
