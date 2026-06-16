"""Tests for AccountLinksService (M1S.5a review-queue service).

Fixture layout:
- ``prov1`` / ``prov2``: provisional (source-minted) accounts.
- ``cand_a`` / ``cand_b``: candidate canonical accounts for merging.
- Each provisional has one accepted ``source_native`` link in ``app.account_links``.
- Decisions are inserted through ``AccountLinkDecisionsRepo`` to exercise the
  full Invariant-10 audited path.
"""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_links_service import AccountLinksService
from tests.moneybin.db_helpers import create_core_tables

# ---------------------------------------------------------------------------
# Test-data constants — opaque IDs (no account numbers or PII)
# ---------------------------------------------------------------------------

_PROV1 = "prov1_acct000"
_PROV2 = "prov2_acct000"
_CAND_A = "cand_a_acct00"
_CAND_B = "cand_b_acct00"

_DEC1 = "dec1_id000001"  # prov1 → cand_a
_DEC2 = "dec1_id000002"  # prov1 → cand_b  (sibling)
_DEC3 = "dec2_id000001"  # prov2 → cand_a

_LINK_PROV1 = "link_prov1_00"
_LINK_PROV2 = "link_prov2_00"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _insert_dim_account(db: Database, account_id: str, display_name: str) -> None:
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name, source_type) "
        "VALUES (?, ?, ?)",
        [account_id, display_name, "csv"],
    )


def _insert_link(
    db: Database,
    *,
    link_id: str,
    account_id: str,
    ref_value: str,
    source_type: str = "csv",
    source_origin: str = "bank_a",
) -> None:
    AccountLinksRepo(db).insert(
        link_id=link_id,
        account_id=account_id,
        ref_kind="source_native",
        ref_value=ref_value,
        source_type=source_type,
        source_origin=source_origin,
        decided_by="auto",
        actor="system",
        status="accepted",
    )


def _insert_decision(
    db: Database,
    *,
    decision_id: str,
    provisional_account_id: str,
    candidate_account_id: str,
    signal: str = "institution_last4",
    confidence: float = 0.9,
) -> None:
    AccountLinkDecisionsRepo(db).insert(
        decision_id=decision_id,
        provisional_account_id=provisional_account_id,
        candidate_account_id=candidate_account_id,
        confidence_score=confidence,
        match_signals={"signal": signal, "value": "***"},
        decided_by="auto",
        actor="system",
        status="pending",
    )


def _decision_status(db: Database, decision_id: str) -> str | None:
    row = db.execute(
        "SELECT status FROM app.account_link_decisions WHERE decision_id = ?",
        [decision_id],
    ).fetchone()
    return row[0] if row else None


def _link_rows(db: Database, **kw: Any) -> list[tuple[Any, ...]]:
    """Fetch (link_id, account_id, ref_kind, ref_value, status) with WHERE filters."""
    clauses: list[str] = []
    params: list[Any] = []
    for col, val in kw.items():
        clauses.append(f"{col} = ?")
        params.append(val)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return db.execute(
        f"SELECT link_id, account_id, ref_kind, ref_value, status FROM app.account_links{where}",  # noqa: S608  # test helper, static pattern
        params,
    ).fetchall()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def svc(db: Database) -> AccountLinksService:
    """Service instance over a fresh test DB."""
    create_core_tables(db)  # materializes core.dim_accounts
    return AccountLinksService(db, actor="cli")


@pytest.fixture()
def seeded(svc: AccountLinksService, db: Database) -> AccountLinksService:
    """Service with a full set of seeded accounts + decisions.

    Seeded state:
    - 4 dim_accounts rows: prov1, prov2, cand_a, cand_b
    - 2 account_links rows: one accepted source_native link per provisional
    - 3 decisions: prov1→cand_a (dec1), prov1→cand_b (dec2), prov2→cand_a (dec3)
    """
    # Dim accounts
    _insert_dim_account(db, _PROV1, "Provisional One")
    _insert_dim_account(db, _PROV2, "Provisional Two")
    _insert_dim_account(db, _CAND_A, "Candidate Alpha")
    _insert_dim_account(db, _CAND_B, "Candidate Beta")
    # Source-native links
    _insert_link(db, link_id=_LINK_PROV1, account_id=_PROV1, ref_value="native-ref-1")
    _insert_link(db, link_id=_LINK_PROV2, account_id=_PROV2, ref_value="native-ref-2")
    # Decisions
    _insert_decision(
        db,
        decision_id=_DEC1,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_A,
        signal="institution_last4",
    )
    _insert_decision(
        db,
        decision_id=_DEC2,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_B,
        signal="name",
    )
    _insert_decision(
        db,
        decision_id=_DEC3,
        provisional_account_id=_PROV2,
        candidate_account_id=_CAND_A,
        signal="institution_last4",
    )
    return svc


# ---------------------------------------------------------------------------
# count_pending
# ---------------------------------------------------------------------------


def test_count_pending_empty(svc: AccountLinksService) -> None:
    """Zero pending when no decisions exist."""
    assert svc.count_pending() == 0


def test_count_pending_two_decisions_one_provisional(
    svc: AccountLinksService, db: Database
) -> None:
    """Two decisions on the same provisional count as 1 (review unit = provisional)."""
    _insert_decision(
        db,
        decision_id=_DEC1,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_A,
    )
    _insert_decision(
        db,
        decision_id=_DEC2,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_B,
    )
    assert svc.count_pending() == 1


def test_count_pending_two_provisionals(svc: AccountLinksService, db: Database) -> None:
    """Decisions on two distinct provisionals count as 2."""
    _insert_decision(
        db,
        decision_id=_DEC1,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_A,
    )
    _insert_decision(
        db,
        decision_id=_DEC3,
        provisional_account_id=_PROV2,
        candidate_account_id=_CAND_A,
    )
    assert svc.count_pending() == 2


# ---------------------------------------------------------------------------
# pending
# ---------------------------------------------------------------------------


def test_pending_empty(svc: AccountLinksService) -> None:
    """Empty list when no pending decisions exist."""
    assert svc.pending() == []


def test_pending_groups_by_provisional(seeded: AccountLinksService) -> None:
    """Decisions are grouped under their provisional; 2 groups expected."""
    groups = seeded.pending()
    assert len(groups) == 2  # prov1 and prov2

    prov_ids = {g.provisional_account_id for g in groups}
    assert prov_ids == {_PROV1, _PROV2}


def test_pending_display_names_resolved(seeded: AccountLinksService) -> None:
    """Display names for provisionals and candidates are pulled from dim_accounts."""
    groups = seeded.pending()
    by_prov = {g.provisional_account_id: g for g in groups}

    g1 = by_prov[_PROV1]
    assert g1.provisional_display_name == "Provisional One"
    cand_names = {
        c.candidate_account_id: c.candidate_display_name for c in g1.candidates
    }
    assert cand_names[_CAND_A] == "Candidate Alpha"
    assert cand_names[_CAND_B] == "Candidate Beta"


def test_pending_candidates_have_correct_signal(seeded: AccountLinksService) -> None:
    """PendingLinkCandidate.signal is decoded from match_signals['signal']."""
    groups = seeded.pending()
    by_prov = {g.provisional_account_id: g for g in groups}
    g1 = by_prov[_PROV1]
    cand_by_id = {c.decision_id: c for c in g1.candidates}
    assert cand_by_id[_DEC1].signal == "institution_last4"
    assert cand_by_id[_DEC2].signal == "name"


def test_pending_display_name_absent_dim_is_empty_string(
    svc: AccountLinksService, db: Database
) -> None:
    """When dim_accounts has no row for an id, display_name resolves to ''."""
    # Insert decision without inserting dim_accounts rows
    _insert_decision(
        db,
        decision_id=_DEC1,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_A,
    )
    groups = svc.pending()
    assert len(groups) == 1
    g = groups[0]
    assert g.provisional_display_name == ""
    assert g.candidates[0].candidate_display_name == ""


# ---------------------------------------------------------------------------
# set — accept (target_account_id = candidate)
# ---------------------------------------------------------------------------


def test_set_accept_repoints_provisional_link(
    seeded: AccountLinksService, db: Database
) -> None:
    """Accepting a decision repoints the provisional's source_native link to the candidate."""
    seeded.set(_DEC1, target_account_id=_CAND_A)

    # Old link (prov1 → native-ref-1) must be reversed
    old = _link_rows(db, link_id=_LINK_PROV1)
    assert len(old) == 1
    assert old[0][4] == "reversed"  # status column

    # New link (cand_a → native-ref-1) must be accepted
    new = _link_rows(
        db, account_id=_CAND_A, ref_kind="source_native", status="accepted"
    )
    assert len(new) == 1
    assert new[0][3] == "native-ref-1"  # ref_value


def test_set_accept_marks_decision_accepted(
    seeded: AccountLinksService, db: Database
) -> None:
    """The named decision transitions to 'accepted'."""
    seeded.set(_DEC1, target_account_id=_CAND_A)
    assert _decision_status(db, _DEC1) == "accepted"


def test_set_accept_auto_rejects_sibling(
    seeded: AccountLinksService, db: Database
) -> None:
    """Sibling pending decision on the same provisional is auto-rejected."""
    seeded.set(_DEC1, target_account_id=_CAND_A)
    assert _decision_status(db, _DEC2) == "rejected"


def test_set_accept_does_not_affect_other_provisional(
    seeded: AccountLinksService, db: Database
) -> None:
    """Decision on a different provisional is unaffected."""
    seeded.set(_DEC1, target_account_id=_CAND_A)
    assert _decision_status(db, _DEC3) == "pending"


# ---------------------------------------------------------------------------
# set — standalone (target_account_id = None)
# ---------------------------------------------------------------------------


def test_set_standalone_rejects_all_pending_decisions(
    seeded: AccountLinksService, db: Database
) -> None:
    """Standalone set rejects every pending decision for the provisional."""
    seeded.set(_DEC1, target_account_id=None)
    assert _decision_status(db, _DEC1) == "rejected"
    assert _decision_status(db, _DEC2) == "rejected"


def test_set_standalone_does_not_repoint_link(
    seeded: AccountLinksService, db: Database
) -> None:
    """Standalone set leaves the provisional's source_native link intact."""
    seeded.set(_DEC1, target_account_id=None)
    rows = _link_rows(db, link_id=_LINK_PROV1)
    assert len(rows) == 1
    assert rows[0][1] == _PROV1  # account_id unchanged
    assert rows[0][4] == "accepted"  # status unchanged


def test_set_standalone_does_not_affect_other_provisional(
    seeded: AccountLinksService, db: Database
) -> None:
    """A standalone on prov1 does not touch prov2's decisions."""
    seeded.set(_DEC1, target_account_id=None)
    assert _decision_status(db, _DEC3) == "pending"


# ---------------------------------------------------------------------------
# set — error cases
# ---------------------------------------------------------------------------


def test_set_wrong_target_raises_user_error(seeded: AccountLinksService) -> None:
    """target_account_id != decision's candidate_account_id → UserError."""
    with pytest.raises(UserError, match="does not match"):
        seeded.set(_DEC1, target_account_id=_CAND_B)  # dec1 names cand_a, not cand_b


def test_set_missing_decision_raises_user_error(svc: AccountLinksService) -> None:
    """Unknown decision_id → UserError with MUTATION_NOT_FOUND code."""
    with pytest.raises(UserError) as exc_info:
        svc.set("no-such-id-xxx", target_account_id=None)
    assert exc_info.value.code == "mutation_not_found"


def test_set_on_already_accepted_raises_user_error(
    seeded: AccountLinksService, db: Database
) -> None:
    """Calling set on an already-accepted decision → UserError."""
    seeded.set(_DEC1, target_account_id=_CAND_A)
    with pytest.raises(UserError) as exc_info:
        seeded.set(_DEC1, target_account_id=_CAND_A)
    assert exc_info.value.code == "mutation_constraint_violation"


def test_set_on_rejected_decision_raises_user_error(
    seeded: AccountLinksService, db: Database
) -> None:
    """Calling set on a rejected decision → UserError."""
    seeded.set(_DEC1, target_account_id=None)  # rejects dec1
    with pytest.raises(UserError):
        seeded.set(_DEC1, target_account_id=None)


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------


def test_history_empty(svc: AccountLinksService) -> None:
    """Empty list when no decisions exist."""
    assert svc.history() == []


def test_history_returns_all_statuses(
    seeded: AccountLinksService, db: Database
) -> None:
    """history() includes accepted, rejected, and pending rows."""
    seeded.set(_DEC1, target_account_id=_CAND_A)  # accepts dec1, rejects dec2
    rows = seeded.history(limit=10)
    statuses = {r["status"] for r in rows}
    assert "accepted" in statuses
    assert "rejected" in statuses
    assert "pending" in statuses  # dec3 is still pending


def test_history_newest_first(seeded: AccountLinksService, db: Database) -> None:
    """Rows are ordered descending by decided_at (most-recently-decided first)."""
    seeded.set(_DEC1, target_account_id=_CAND_A)  # mutates decided_at on dec1 and dec2
    rows = seeded.history(limit=10)
    dates = [r["decided_at"] for r in rows if r["decided_at"] is not None]
    assert dates == sorted(dates, reverse=True)


def test_history_limit_respected(seeded: AccountLinksService) -> None:
    """Limit parameter caps the returned count."""
    rows = seeded.history(limit=2)
    assert len(rows) <= 2


def test_history_match_signals_decoded(seeded: AccountLinksService) -> None:
    """match_signals is returned as a dict, not a raw JSON string."""
    rows = seeded.history(limit=5)
    for row in rows:
        assert isinstance(row["match_signals"], dict)
