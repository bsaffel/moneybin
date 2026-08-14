"""Tests for AccountLinksService (M1S.5a review-queue service).

Fixture layout:
- ``prov1`` / ``prov2``: provisional (source-minted) accounts.
- ``cand_a`` / ``cand_b``: candidate canonical accounts for merging.
- Each provisional has one accepted ``source_native`` link in ``app.account_links``.
- Decisions are inserted through ``AccountLinkDecisionsRepo`` to exercise the
  full Invariant-10 audited path.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_links_service import (
    AccountLinkAcceptImpact,
    AccountLinksService,
)
from moneybin.services.refresh import RefreshResult
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


def _set_account_traits(
    db: Database,
    account_id: str,
    *,
    account_subtype: str | None = None,
    account_type: str | None = None,
    currency_code: str | None = None,
) -> None:
    db.execute(
        "UPDATE core.dim_accounts SET account_subtype = ?, account_type = ?, "
        "currency_code = ? WHERE account_id = ?",
        [account_subtype, account_type, currency_code, account_id],
    )


def _insert_transaction(
    db: Database,
    *,
    transaction_id: str,
    account_id: str,
    on: date,
    amount: str,
    source_type: str = "csv",
    currency_code: str = "USD",
) -> None:
    """Insert one core transaction.

    ``currency_code`` defaults to a stated value rather than NULL because the
    pipeline cannot produce a NULL one under an account that states its own:
    ``fct_transactions.currency_code`` is ``COALESCE(t.currency_code,
    a.currency_code)``. A fixture that left it NULL under a USD account would
    let the overlap probe match rows production would have separated.
    """
    db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, source_type, "
        "currency_code) VALUES (?, ?, ?, ?, ?, ?)",
        [transaction_id, account_id, on, Decimal(amount), source_type, currency_code],
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


@pytest.fixture(autouse=True)
def rematch(mocker: MockerFixture) -> MagicMock:
    """Stand in for the post-merge re-match on every test in this module.

    ``set``'s accept path re-runs match+transform once the repoint commits.
    Every test here asserts merge *mechanics*, so letting that reach a real
    SQLMesh apply costs seconds per accept and proves nothing about the
    mechanics. The two tests that assert the trigger itself take this mock as
    a parameter; the real pipeline is covered by the integration regression
    test instead.
    """
    return mocker.patch(
        "moneybin.services.refresh.refresh",
        return_value=RefreshResult(applied=True, duration_seconds=0.0),
    )


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


@pytest.fixture()
def seeded_ledgers(seeded: AccountLinksService, db: Database) -> AccountLinksService:
    """``seeded`` plus transactions, so the ledger evidence is measurable.

    Hand-derived against ``probe_ledger_overlap``'s ±3-day window, which scopes
    the denominator to the period the *candidate* covers:

    - cand_a spans 2026-05-02 → 2026-05-04, so prov1's comparable window is
      2026-04-29 → 2026-05-07.
    - prov1 holds four transactions; the 2026-01-15 one falls outside that
      window, leaving **3 comparable**.
    - Of those, 10.00 (1 day apart) and 20.00 (2 days apart) have an
      amount-equal counterpart in cand_a; 30.00 has none — **2 matched**.
    - prov1's own ledger is **4**, deliberately different from the 3 comparable
      and the 2 matched so the three numbers cannot be confused for each other.

    Both sides share one currency on purpose. The probe only matches rows that
    agree on ``currency_code``, so a pair that disagreed on it would measure
    zero overlap however equal the amounts looked — which is a different
    fixture answering a different question (see ``test_ledger_overlap.py``).
    Here the subtype is what tells the two accounts apart.
    """
    _set_account_traits(db, _PROV1, account_type="depository", currency_code="USD")
    _set_account_traits(
        db,
        _CAND_A,
        account_subtype="checking",
        account_type="depository",
        currency_code="USD",
    )
    for i, (on, amount) in enumerate([
        (date(2026, 5, 1), "10.00"),
        (date(2026, 5, 2), "20.00"),
        (date(2026, 5, 3), "30.00"),
        (date(2026, 1, 15), "99.00"),
    ]):
        _insert_transaction(
            db,
            transaction_id=f"prov1_txn{i:04d}",
            account_id=_PROV1,
            on=on,
            amount=amount,
            source_type="pdf",
        )
    for i, (on, amount) in enumerate([
        (date(2026, 5, 2), "10.00"),
        (date(2026, 5, 4), "20.00"),
    ]):
        _insert_transaction(
            db,
            transaction_id=f"cand_a_txn{i:04d}",
            account_id=_CAND_A,
            on=on,
            amount=amount,
            source_type="ofx",
        )
    return seeded


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
# pending — browse-time ledger evidence
# ---------------------------------------------------------------------------


def test_pending_measures_each_candidate_against_the_provisional_ledger(
    seeded_ledgers: AccountLinksService,
) -> None:
    """The queue's decisive number is measured, not carried from the proposal.

    ``pending()`` is the only place these two reads are wired together, and the
    CLI test drives a mocked group — so a probe called with its arguments
    reversed, or against the wrong account, is invisible everywhere else.
    """
    by_prov = {g.provisional_account_id: g for g in seeded_ledgers.pending()}
    candidate = next(
        c for c in by_prov[_PROV1].candidates if c.candidate_account_id == _CAND_A
    )

    assert candidate.overlap.comparable == 3
    assert candidate.overlap.matched == 2
    assert candidate.overlap.window_start == date(2026, 5, 1)
    assert candidate.overlap.window_end == date(2026, 5, 3)


def test_pending_carries_the_provisional_ledger_size_not_the_comparable_count(
    seeded_ledgers: AccountLinksService,
) -> None:
    """How much history moves is a different number from how much of it matched.

    The group header states the magnitude of the merge; the candidate row states
    the evidence for it. Seeding four transactions where only three are
    comparable and two match keeps a wiring swap from passing.
    """
    by_prov = {g.provisional_account_id: g for g in seeded_ledgers.pending()}

    assert by_prov[_PROV1].transactions == 4
    assert by_prov[_PROV2].transactions == 0


def test_pending_reports_an_unmeasurable_probe_rather_than_a_zero_ratio(
    seeded_ledgers: AccountLinksService,
) -> None:
    """prov2 holds no transactions, so nothing about the pair is comparable."""
    by_prov = {g.provisional_account_id: g for g in seeded_ledgers.pending()}
    candidate = by_prov[_PROV2].candidates[0]

    assert candidate.candidate_account_id == _CAND_A
    assert not candidate.overlap.measurable
    assert candidate.overlap.comparable == 0


def test_pending_renders_before_core_is_materialized(db: Database) -> None:
    """A profile has proposals before it has a transform.

    ``core.dim_accounts`` and ``core.fct_transactions`` are SQLMesh-owned, so
    the queue has to render in the window between a profile's first sync and
    its first refresh. Three reads touch ``core`` on this path — the display
    name, the overlap probe, and the ledger size — and each guards the catalog
    error separately, so a missing guard raises where the queue should list.
    """
    _insert_decision(
        db,
        decision_id=_DEC1,
        provisional_account_id=_PROV1,
        candidate_account_id=_CAND_A,
    )

    groups = AccountLinksService(db, actor="cli").pending()

    assert len(groups) == 1
    assert groups[0].transactions == 0
    assert not groups[0].candidates[0].overlap.measurable


# ---------------------------------------------------------------------------
# ledger_facts / merge_facts
# ---------------------------------------------------------------------------


def test_merge_facts_before_core_is_materialized_describes_both_sides_as_empty(
    db: Database,
) -> None:
    """The confirm prompt has the same pre-transform window as the queue."""
    facts = AccountLinksService(db, actor="cli").merge_facts(
        absorbed_account_id=_PROV1, survivor_account_id=_CAND_A
    )

    assert facts.absorbed.account_id == _PROV1
    assert facts.absorbed.transactions == 0
    assert facts.absorbed.subtype is None
    assert facts.survivor.account_id == _CAND_A
    assert not facts.overlap.measurable


def test_ledger_facts_describes_an_account_by_what_distinguishes_it(
    seeded_ledgers: AccountLinksService,
) -> None:
    """Source origin, span, size, subtype, currency — never the shared last four."""
    facts = seeded_ledgers.ledger_facts(_PROV1)

    assert facts.account_id == _PROV1
    assert facts.display_name == "Provisional One"
    assert facts.source_types == ("pdf",)
    assert facts.transactions == 4
    assert facts.first_date == date(2026, 1, 15)
    assert facts.last_date == date(2026, 5, 3)
    assert facts.currency_code == "USD"


def test_ledger_facts_prefers_the_subtype_a_human_recognizes(
    seeded_ledgers: AccountLinksService,
) -> None:
    """A subtype tells the two sides apart where the canonical type cannot.

    Both accounts are ``depository``; only cand_a states a subtype. Falling back
    to the type is right when the subtype is absent and wrong when it is not —
    prov1 covers the first half, cand_a the second.
    """
    assert seeded_ledgers.ledger_facts(_PROV1).subtype == "depository"
    assert seeded_ledgers.ledger_facts(_CAND_A).subtype == "checking"


def test_ledger_facts_on_an_account_with_no_ledger_is_empty_not_absent(
    seeded_ledgers: AccountLinksService,
) -> None:
    """An account with no transactions still describes itself."""
    facts = seeded_ledgers.ledger_facts(_PROV2)

    assert facts.display_name == "Provisional Two"
    assert facts.transactions == 0
    assert facts.source_types == ()
    assert facts.first_date is None


def test_merge_facts_measures_the_overlap_in_the_direction_it_was_asked(
    seeded_ledgers: AccountLinksService,
) -> None:
    """Absorbed and survivor are not interchangeable, and neither is the probe.

    Reversing the pair re-scopes the comparable window to the other ledger's
    period, so a builder that ignored the direction would return the same
    numbers both ways.
    """
    forward = seeded_ledgers.merge_facts(
        absorbed_account_id=_PROV1, survivor_account_id=_CAND_A
    )
    reversed_pair = seeded_ledgers.merge_facts(
        absorbed_account_id=_CAND_A, survivor_account_id=_PROV1
    )

    assert forward.absorbed.account_id == _PROV1
    assert forward.survivor.account_id == _CAND_A
    assert (forward.overlap.comparable, forward.overlap.matched) == (3, 2)
    assert reversed_pair.absorbed.account_id == _CAND_A
    assert (reversed_pair.overlap.comparable, reversed_pair.overlap.matched) == (2, 2)


# ---------------------------------------------------------------------------
# accept_impact
# ---------------------------------------------------------------------------


def test_accept_impact_counts_every_row_the_merge_will_mutate(
    seeded: AccountLinksService, db: Database
) -> None:
    """Impact includes all accepted links and decisions touching the provisional."""
    AccountLinksRepo(db).insert(
        link_id="link_prov1_pt0",
        account_id=_PROV1,
        ref_kind="persistent_token",
        ref_value="opaque-token",
        source_type="plaid",
        source_origin="plaid_bank",
        decided_by="auto",
        actor="system",
    )
    _insert_decision(
        db,
        decision_id="qp_dec000001",
        provisional_account_id=_PROV2,
        candidate_account_id=_PROV1,
    )

    impact = seeded.accept_impact(_DEC1, target_account_id=_CAND_A)

    assert impact.provisional_account_id == _PROV1
    assert impact.candidate_account_id == _CAND_A
    assert impact.blast_radius == {
        "accounts": 2,
        "account_links": 2,
        "account_link_decisions": 3,
    }


def test_accept_impact_names_every_row_the_merge_will_mutate(
    seeded: AccountLinksService, db: Database
) -> None:
    """Counts cannot describe a swap, so the impact carries the ids themselves.

    One pending sibling resolved elsewhere while another arrives for the same
    provisional leaves ``account_link_decisions`` unchanged and still changes
    which decision the commit auto-rejects. Both confirmation surfaces bind to
    these tuples, so the counts must be a projection of them rather than a
    separate read that could disagree.
    """
    _insert_decision(
        db,
        decision_id="qp_dec000001",
        provisional_account_id=_PROV2,
        candidate_account_id=_PROV1,
    )

    impact = seeded.accept_impact(_DEC1, target_account_id=_CAND_A)

    assert _DEC1 in impact.decision_ids
    assert "qp_dec000001" in impact.decision_ids
    assert impact.decision_ids == tuple(sorted(impact.decision_ids))
    assert impact.link_ids == tuple(sorted(impact.link_ids))
    assert impact.blast_radius["account_link_decisions"] == len(impact.decision_ids)
    assert impact.blast_radius["account_links"] == len(impact.link_ids)


def test_accept_verifier_receives_live_impact_and_failure_rolls_back(
    seeded: AccountLinksService, db: Database
) -> None:
    """Verification runs inside the transaction immediately before any write."""
    verified = False

    def refuse(impact: AccountLinkAcceptImpact) -> None:
        nonlocal verified
        verified = True
        assert impact.blast_radius == {
            "accounts": 2,
            "account_links": 1,
            "account_link_decisions": 2,
        }
        assert _decision_status(db, _DEC1) == "pending"
        assert _link_rows(db, link_id=_LINK_PROV1)[0][4] == "accepted"
        raise UserError("Confirmation mismatch", code="mutation_confirmation_mismatch")

    with pytest.raises(UserError, match="Confirmation mismatch"):
        seeded.set(
            _DEC1,
            target_account_id=_CAND_A,
            verify_accept=refuse,
        )

    assert verified
    assert _decision_status(db, _DEC1) == "pending"
    assert _decision_status(db, _DEC2) == "pending"
    assert _link_rows(db, link_id=_LINK_PROV1)[0][4] == "accepted"


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
# set — accept re-runs the matcher
# ---------------------------------------------------------------------------


def test_set_accept_reruns_the_matcher(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """Accepting a merge re-matches — the repoint is what makes the rows candidates.

    The matcher blocks candidate pairs on ``a.account_id = b.account_id``
    (``matching/scoring.py``), so a reissued card's two sides cannot match while
    their ids differ. The accept is the moment they become co-resident, and
    nothing else in the pipeline looks again.
    """
    seeded.set(_DEC1, target_account_id=_CAND_A)

    rematch.assert_called_once_with(db, steps=["match", "transform"])


def test_set_accept_carries_a_rejected_pair_onto_the_survivor(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """A user's "these are not duplicates" must survive the account merge.

    ``get_rejected_pairs`` keys its tuple on ``account_id`` and the matcher
    checks membership by exact tuple, so a rejection left on the merged-away
    provisional stops matching the live pair — which now sits under the
    survivor. The re-match fired on the very next line would then treat it as
    brand new and, above ``high_confidence_threshold`` with agreeing
    descriptions, auto-accept the pair the user explicitly rejected.
    """
    from moneybin.matching.persistence import get_rejected_pairs
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00001",
        source_transaction_id_a="ofx1",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="csv1",
        source_type_b="csv",
        source_origin_b="bank",
        account_id=_PROV1,
        confidence_score=0.99,
        match_signals={},
        match_status="rejected",
        match_tier="3",
        decided_by="user",
        actor="test",
    )

    seeded.set(_DEC1, target_account_id=_CAND_A)

    pairs = get_rejected_pairs(db)
    assert [p["account_id"] for p in pairs] == [_CAND_A], (
        "the rejection must name the surviving account, or it stops matching "
        "the pair it was made about"
    )


def test_set_accept_carries_the_transfer_side_of_a_decision_too(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """``account_id_b`` names the merged-away account on a transfer, and goes stale too.

    Isolated from the dedup test by exactly one property: here the provisional
    is the transfer's *second* leg, so only the ``account_id_b`` half of the
    re-key can move it. A version that migrated ``account_id`` alone would pass
    the dedup test and leave this row pointing at an account holding nothing.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00002",
        source_transaction_id_a="ofx9",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx8",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_PROV2,
        confidence_score=0.95,
        match_signals={},
        match_status="accepted",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="user",
        actor="test",
    )

    seeded.set(_DEC1, target_account_id=_CAND_A)

    row = db.execute(
        "SELECT account_id, account_id_b FROM app.match_decisions "
        "WHERE match_id = 'match_id00002'"
    ).fetchone()
    assert row is not None
    assert row[0] == _PROV2, "the untouched leg must stay where it was"
    assert row[1] == _CAND_A, (
        "the merged-away leg must follow the merge, or the transfer names an "
        "account that no longer holds any transactions"
    )


def test_set_accept_retires_a_transfer_whose_two_legs_collapse(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """A transfer between an account and itself is not a thing that can be true.

    When the merged-away account is one leg and the survivor is already the
    other, re-keying would write ``account_id == account_id_b``. Accepted, that
    materializes in ``core.bridge_transfers`` as a transfer from an account to
    itself; pending, it sits in the review queue as a proposal nobody can act
    on. Neither is recoverable by a later pass, so the merge retires it.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00003",
        source_transaction_id_a="ofx7",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx6",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="accepted",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="user",
        actor="test",
    )

    seeded.set(_DEC1, target_account_id=_CAND_A)

    row = db.execute(
        "SELECT match_status, account_id, account_id_b FROM app.match_decisions "
        "WHERE match_id = 'match_id00003'"
    ).fetchone()
    assert row is not None
    assert row[0] == "reversed", (
        "a transfer whose endpoints collapsed must be retired, not left "
        "pointing an account at itself"
    )
    # Retired *and* re-keyed. transform drops the provisional from
    # dim_accounts moments later and app_match_decisions_account_fk checks
    # every row regardless of status, so retiring alone strands this one.
    assert row[1] == _CAND_A
    assert row[2] == _CAND_A


def test_set_accept_rejects_a_pending_transfer_whose_two_legs_collapse(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """The same collapse, retired the other way because the row was never decided.

    Isolated from the accepted-collapse test by exactly one property: the
    status. ``reverse()`` refuses a pending row — there is no accept/reject to
    undo — so the retirement has to go through ``update_status``, and the
    resting status is ``rejected`` rather than ``reversed``. A version that
    called ``reverse()`` on both would raise here and pass there.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00004",
        source_transaction_id_a="ofx5",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx4",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="pending",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="system",
        actor="test",
    )

    seeded.set(_DEC1, target_account_id=_CAND_A)

    row = db.execute(
        "SELECT match_status, account_id, account_id_b FROM app.match_decisions "
        "WHERE match_id = 'match_id00004'"
    ).fetchone()
    assert row is not None
    assert row[0] == "rejected", (
        "a pending transfer whose endpoints collapsed must leave the review "
        "queue, or it sits there as a proposal nobody can action"
    )
    assert row[1] == _CAND_A
    assert row[2] == _CAND_A


def test_set_accept_reports_the_collapsed_transfer_it_retired(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """Retiring the transfer is half the job; telling the caller is the other half.

    Same fixture as the accepted-collapse test above, asserting the *return*
    rather than the row. The matcher is stubbed here and no dedup edge exists,
    so its own reconciliation contributes 0 — a zero total therefore means the
    collapse branch's own retirement went unreported, and every surface prints
    its silent "no transfers retired" over a decision of the user's that this
    call just undid.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00005",
        source_transaction_id_a="ofx9",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx8",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="accepted",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="user",
        actor="test",
    )

    result = seeded.set(_DEC1, target_account_id=_CAND_A)

    assert result is not None
    assert result.transfers_retired == 1, (
        "the merge reversed an accepted transfer; a caller told 0 has no "
        "reason to look for it in the audit log"
    )


def test_set_accept_does_not_report_a_collapsed_pending_transfer(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """The pending twin of the test above — retired, but nothing to disclose.

    One property apart: ``match_status``. The collapse rejects this row too,
    but the counter says "transfers you had already accepted", and the user
    never accepted this one. Counting it would send them to ``audit undo`` to
    restore a proposal they had not yet answered.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00006",
        source_transaction_id_a="ofx11",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx10",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="pending",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="system",
        actor="test",
    )

    result = seeded.set(_DEC1, target_account_id=_CAND_A)

    assert result is not None
    assert result.transfers_retired == 0, (
        "a pending transfer was never the user's decision, so its collapse "
        "is not something they need to be told was undone"
    )


def test_set_accept_does_not_report_a_collapsed_rejected_transfer(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """The third status, and the one the pair above cannot speak for.

    A rejected row shares the ``reverse()`` call with the accepted one and the
    silence with the pending one, so neither existing test constrains it: the
    accepted twin fixes the count at 1 and the pending twin reaches the count
    through a different branch (``update_status``). Asserted together because
    the negative claim needs its positive half — ``transfers_retired == 0``
    over a row that was never retired would pass while proving nothing.

    Reversing a rejection removes nothing from the ledger. Counting it would
    tell the user an accepted transfer had gone and point them at an undo that
    restores a "these are not duplicates" answer they never lost.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00007",
        source_transaction_id_a="ofx13",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx12",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="rejected",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="user",
        actor="test",
    )

    result = seeded.set(_DEC1, target_account_id=_CAND_A)

    row = db.execute(
        "SELECT match_status FROM app.match_decisions WHERE match_id = 'match_id00007'"
    ).fetchone()
    assert row is not None
    assert row[0] == "reversed", "the collapse must retire a rejection too"
    assert result is not None
    assert result.transfers_retired == 0, (
        "a reversed rejection takes nothing out of the ledger, so there is "
        "nothing for the user to restore"
    )


def test_the_collapse_count_reaches_the_batched_path_s_separate_rematch(
    seeded: AccountLinksService, db: Database, rematch: MagicMock
) -> None:
    """The batch splits retirement and disclosure across two calls.

    ``ReviewDecisionsService.apply_identity`` calls ``set(in_outer_txn=True)``
    — which returns before its own post-commit tail — and then
    ``rematch_after_merge()`` separately on the same service. The retirement
    happens in the first call and the count is reported by the second, so a
    count held only in ``set``'s locals would be dropped on exactly the path
    that merges several accounts at once. Shaped like the batch, not mocked
    like it, because what is under test is that seam.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00007",
        source_transaction_id_a="ofx13",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx12",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="accepted",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="user",
        actor="test",
    )

    db.begin()
    assert seeded.set(_DEC1, target_account_id=_CAND_A, in_outer_txn=True) is None
    db.commit()

    assert seeded.rematch_after_merge().transfers_retired == 1


def test_a_rolled_back_accept_leaves_no_collapse_count_behind(
    seeded: AccountLinksService,
    db: Database,
    rematch: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A merge that failed retired nothing, so it owes no disclosure.

    The collapse count is produced inside ``set``'s transaction, ahead of
    writes that can still raise. DuckDB rolls those back; a counter held on the
    service does not roll back with them, so the next merge on the same
    instance would report a transfer retired that is in fact still accepted —
    and send the user to ``audit undo`` over a reversal that never happened.

    Fault-injected because nothing in the fixture can fail at that exact point:
    the failure is the condition under test, and the assertion is on real
    behaviour afterwards. Every caller today builds a fresh service per
    top-level call, so this holds a latent bug closed rather than a live one.
    """
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    MatchDecisionsRepo(db).insert(
        match_id="match_id00008",
        source_transaction_id_a="ofx15",
        source_type_a="ofx",
        source_origin_a="bank",
        source_transaction_id_b="ofx14",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=_CAND_A,
        confidence_score=0.95,
        match_signals={},
        match_status="accepted",
        match_type="transfer",
        account_id_b=_PROV1,
        decided_by="user",
        actor="test",
    )

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("a later write in the same transaction failed")

    monkeypatch.setattr(AccountLinkDecisionsRepo, "update_status", _fail)

    with pytest.raises(RuntimeError):
        seeded.set(_DEC1, target_account_id=_CAND_A)

    assert seeded.rematch_after_merge().transfers_retired == 0, (
        "the accept rolled back, so the transfer it would have retired is "
        "still accepted — reporting it undone points the user at an audit "
        "entry that does not exist"
    )


def test_set_standalone_does_not_rerun_the_matcher(
    seeded: AccountLinksService, rematch: MagicMock
) -> None:
    """A standalone reject repoints nothing, so no account gains new candidates."""
    seeded.set(_DEC1, target_account_id=None)

    rematch.assert_not_called()


def test_set_accept_returns_what_the_rematch_found(
    seeded: AccountLinksService, rematch: MagicMock
) -> None:
    """Callers get the re-match outcome so they can report it to the user.

    The re-match can auto-merge without asking, so an accept that hides the
    number is the silent action the confirm gate exists to prevent.
    """
    rematch.return_value = RefreshResult(
        applied=True,
        duration_seconds=0.0,
        matches_auto_merged=2,
        matches_pending_review=5,
    )

    result = seeded.set(_DEC1, target_account_id=_CAND_A)

    assert result is not None
    assert result.matches_auto_merged == 2
    assert result.matches_pending_review == 5


def test_set_standalone_returns_no_rematch_result(
    seeded: AccountLinksService,
) -> None:
    """A reject ran no match pass, so there is no outcome to report."""
    assert seeded.set(_DEC1, target_account_id=None) is None


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


def test_set_accept_with_no_source_native_link_raises(
    svc: AccountLinksService, db: Database
) -> None:
    """Accepting a merge whose provisional has no source_native link is refused.

    The staging JOIN translates raw rows via source_native links; with none to
    re-point, accepting would record a 'paper merge' that never collapses the
    data — so set() raises and rolls back rather than marking it accepted.
    """
    _insert_dim_account(db, "prov_nolink01", "Prov NoLink")
    _insert_dim_account(db, _CAND_A, "Cand A")
    _insert_decision(
        db,
        decision_id="dec_nolink001",
        provisional_account_id="prov_nolink01",
        candidate_account_id=_CAND_A,
    )
    with pytest.raises(UserError, match="no source_native mapping"):
        svc.set("dec_nolink001", target_account_id=_CAND_A)
    # Rolled back: the decision stays pending.
    assert _decision_status(db, "dec_nolink001") == "pending"


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


# ---------------------------------------------------------------------------
# run — backfill pending proposals
# ---------------------------------------------------------------------------

# Two accounts sharing institution+last4 in dim_accounts (cross-source twins).
_TWIN_A = "twin_a_acct00"
_TWIN_B = "twin_b_acct00"
_TWIN_LINK_A = "twin_link_a_00"
_TWIN_LINK_B = "twin_link_b_00"


def _seed_twin_accounts(db: Database) -> None:
    """Insert two dim_accounts rows sharing institution+last4 (triggers institution_last4 signal)."""
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four, institution_name, display_name, source_type) "
        "VALUES (?, ?, ?, ?, ?)",  # noqa: S608  # test fixture insert
        [_TWIN_A, "7777", "first_bank", "First Bank Checking A", "csv"],
    )
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four, institution_name, display_name, source_type) "
        "VALUES (?, ?, ?, ?, ?)",  # noqa: S608  # test fixture insert
        [_TWIN_B, "7777", "first_bank", "First Bank Checking B", "ofx"],
    )
    # Each twin carries an accepted source_native link (as resolver-imported
    # accounts do) so run() treats them as mergeable provisionals.
    _insert_link(
        db,
        link_id="link_twin_a0",
        account_id=_TWIN_A,
        ref_value="twin_a_native",
        source_type="csv",
        source_origin="first_bank_csv",
    )
    _insert_link(
        db,
        link_id="link_twin_b0",
        account_id=_TWIN_B,
        ref_value="twin_b_native",
        source_type="ofx",
        source_origin="first_bank_ofx",
    )


def test_run_writes_pending_for_cross_source_twins(
    svc: AccountLinksService, db: Database
) -> None:
    """run() writes one pending decision for a twin pair sharing institution+last4."""
    _seed_twin_accounts(db)

    count = svc.run()

    assert count == 1
    row = db.execute(
        "SELECT provisional_account_id, candidate_account_id, status "
        "FROM app.account_link_decisions LIMIT 1"
    ).fetchone()
    assert row is not None
    provisional, candidate, status = row
    # Either direction is valid — the pair is unordered
    pair = {provisional, candidate}
    assert pair == {_TWIN_A, _TWIN_B}
    assert status == "pending"


def test_run_is_idempotent(svc: AccountLinksService, db: Database) -> None:
    """A second run() writes 0 new decisions — the pair is already proposed."""
    _seed_twin_accounts(db)

    first = svc.run()
    second = svc.run()

    assert first == 1
    assert second == 0
    # Still exactly one decision row
    n = db.execute("SELECT COUNT(*) FROM app.account_link_decisions").fetchone()
    assert n is not None and n[0] == 1


def test_run_skips_pair_with_existing_decision_any_status(
    svc: AccountLinksService, db: Database
) -> None:
    """run() does not re-propose a pair that already has a decision (accepted or rejected)."""
    _seed_twin_accounts(db)
    # Seed a pre-existing accepted decision for the twin pair
    _insert_decision(
        db,
        decision_id="pre_dec_00001",
        provisional_account_id=_TWIN_A,
        candidate_account_id=_TWIN_B,
        signal="institution_last4",
    )
    # Mark it accepted
    db.execute(
        "UPDATE app.account_link_decisions SET status = 'accepted' WHERE decision_id = ?",
        ["pre_dec_00001"],
    )

    count = svc.run()

    # The pair is already covered (accepted) — 0 new decisions
    assert count == 0


def test_run_skips_provisionals_without_source_native_link(
    svc: AccountLinksService, db: Database
) -> None:
    """run() does not propose merges for accounts lacking a source_native link.

    set() refuses to merge such a provisional (nothing to re-point), so proposing
    one would be a dead-end. Two twins share institution+last4 but neither has a
    source_native link → zero proposals.
    """
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four, institution_name, display_name, source_type) "
        "VALUES (?, ?, ?, ?, ?)",  # noqa: S608  # test fixture insert
        ["nolink_a0001", "5555", "second_bank", "No-Link A", "csv"],
    )
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four, institution_name, display_name, source_type) "
        "VALUES (?, ?, ?, ?, ?)",  # noqa: S608  # test fixture insert
        ["nolink_b0001", "5555", "second_bank", "No-Link B", "ofx"],
    )
    assert svc.run() == 0
    n = db.execute("SELECT count(*) FROM app.account_link_decisions").fetchone()
    assert n is not None and n[0] == 0


def test_set_accept_repoints_strong_ref_links_too(
    seeded: AccountLinksService, db: Database
) -> None:
    """Accept re-points strong-ref links too, not just source_native.

    Otherwise a later source carrying the same persistent_token / full_number
    would mis-adopt onto the merged-away provisional.
    """
    AccountLinksRepo(db).insert(
        link_id="link_prov1_pt0",
        account_id=_PROV1,
        ref_kind="persistent_token",
        ref_value="prov1-token-xyz",
        source_type="plaid",
        source_origin="plaid_first_bank",
        decided_by="auto",
        actor="system",
    )
    seeded.set(_DEC1, target_account_id=_CAND_A)
    # The persistent_token strong ref now points at the candidate (old reversed).
    accepted_tokens = _link_rows(db, ref_kind="persistent_token", status="accepted")
    assert len(accepted_tokens) == 1
    assert accepted_tokens[0][1] == _CAND_A  # account_id column
    # The provisional retains no accepted links — all re-pointed onto the candidate.
    assert _link_rows(db, account_id=_PROV1, status="accepted") == []


def test_run_skips_reverse_direction_pair(
    svc: AccountLinksService, db: Database
) -> None:
    """run() skips the pair B→A when A→B was already proposed in prior run."""
    _seed_twin_accounts(db)
    # Seed a pre-existing decision in the reverse direction
    _insert_decision(
        db,
        decision_id="rev_dec_00001",
        provisional_account_id=_TWIN_B,
        candidate_account_id=_TWIN_A,
        signal="institution_last4",
    )

    count = svc.run()

    # The pair is already covered (in reverse) — 0 new decisions
    assert count == 0


def test_set_accept_rejects_proposals_where_provisional_is_candidate(
    seeded: AccountLinksService, db: Database
) -> None:
    """Accepting P→C also rejects pending Q→P proposals.

    P is merged away, so a later accept of Q→P would re-point Q onto a dead
    provisional. Unrelated proposals (neither side is P) stay pending.
    """
    # A proposal to merge prov2 INTO prov1 — prov1 is the *candidate* here.
    _insert_decision(
        db,
        decision_id="qp_dec000001",
        provisional_account_id=_PROV2,
        candidate_account_id=_PROV1,
        signal="institution_last4",
    )
    seeded.set(_DEC1, target_account_id=_CAND_A)  # merge prov1 into cand_a
    assert _decision_status(db, "qp_dec000001") == "rejected"
    # The unrelated prov2→cand_a proposal (neither side is prov1) stays pending.
    assert _decision_status(db, _DEC3) == "pending"
