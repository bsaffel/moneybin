"""Tests for AccountResolver (M1S.2 resolution ladder + M1S.4 propose())."""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import AccountProposal, SourceAccount
from moneybin.services.account_resolver import AccountResolver
from tests.moneybin.db_helpers import create_core_tables


def _src(**overrides: Any) -> SourceAccount:
    base: dict[str, Any] = {
        "source_type": "csv",
        "source_origin": "wells_fargo",
        "source_account_key": "wf-checking",
        "account_name": "WF Checking 4267",
        "account_number": None,
        "last_four": "4267",
        "institution": "wells_fargo",
        "persistent_token": None,
        "explicit_account_id": None,
    }
    base.update(overrides)
    return SourceAccount(**base)


def test_explicit_binding_adopts_pinned_id_and_writes_mapping(db: Database) -> None:
    """Ladder step 0: a caller-pinned account_id is adopted above all detection.

    An accepted source_native mapping is written so staging is total.
    """
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(_src(explicit_account_id="acct_pinned1"))

    assert resolved.account_id == "acct_pinned1"
    assert resolved.is_new is False
    row = db.conn.execute(
        "SELECT account_id, ref_kind, status FROM app.account_links "
        "WHERE source_type = ? AND source_origin = ? AND ref_value = ?",
        ["csv", "wells_fargo", "wf-checking"],
    ).fetchone()
    assert row == ("acct_pinned1", "source_native", "accepted")


def test_explicit_rebind_same_id_is_noop(db: Database) -> None:
    """Re-binding the same source key to the same account is idempotent."""
    resolver = AccountResolver(db, actor="system")
    resolver.resolve(_src(explicit_account_id="acct_pinned1"))
    resolver.resolve(_src(explicit_account_id="acct_pinned1"))

    n = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = 'wf-checking'"
    ).fetchone()
    assert n is not None and n[0] == 1


def test_explicit_rebind_to_different_id_raises(db: Database) -> None:
    """A silent re-point would corrupt the staging JOIN — surface the conflict instead."""
    resolver = AccountResolver(db, actor="system")
    resolver.resolve(_src(explicit_account_id="acct_A"))
    with pytest.raises(ValueError, match="different"):
        resolver.resolve(_src(explicit_account_id="acct_B"))


def test_source_native_reimport_is_idempotent(db: Database) -> None:
    """Re-importing the same source account reuses the canonical id (no dup)."""
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(explicit_account_id="acct_wf_1"))
    second = resolver.resolve(_src())  # same source_native key, no explicit_account_id

    assert first.account_id == second.account_id
    assert second.is_new is False
    assert second.outcome == "adopted_strong"
    n = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = 'wf-checking'"
    ).fetchone()
    assert n is not None and n[0] == 1


def test_persistent_token_auto_adopts_across_source_origin(db: Database) -> None:
    """A remembered persistent_token re-links the same account across connections."""
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_type="plaid",
            source_account_key="tok-A",
            persistent_token="pers-1",  # noqa: S106  # test fixture, not a real credential
            explicit_account_id="acct_plaid_1",
        )
    )
    second = resolver.resolve(
        _src(
            source_type="plaid",
            source_origin="plaid_conn_2",
            source_account_key="tok-B",
            persistent_token="pers-1",  # noqa: S106  # test fixture, not a real credential
        )
    )
    assert second.account_id == first.account_id
    assert second.outcome == "adopted_strong"
    # the adopt also wrote tok-B's source_native mapping onto the same account
    row = db.conn.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND source_type = ? AND source_origin = ? AND ref_value = ?",
        ["plaid", "plaid_conn_2", "tok-B"],
    ).fetchone()
    assert row == (first.account_id,)


def test_scoped_full_number_auto_adopts_ofx_then_csv(db: Database) -> None:
    """OFX scoped full_number is a strong confirmer a later CSV auto-adopts onto."""
    resolver = AccountResolver(db, actor="system")
    ofx = resolver.resolve(
        _src(
            source_type="ofx",
            source_account_key="ofx-4267",
            account_number="wells_fargo:111000:4267",  # scoped composite
            explicit_account_id="acct_ofx_1",
        )
    )
    csv = resolver.resolve(
        _src(
            source_type="csv",
            source_account_key="wf-checking",
            account_number="wells_fargo:111000:4267",
        )
    )
    assert csv.account_id == ofx.account_id
    assert csv.outcome == "adopted_strong"


# ---------------------------------------------------------------------------
# Step 2 — candidate pass (A4)
# ---------------------------------------------------------------------------


def _seed_dim_account(
    db: Database,
    *,
    account_id: str,
    last_four: str | None = None,
    institution_name: str | None = None,
    display_name: str | None = None,
) -> None:
    """Insert a minimal core.dim_accounts row (simulates a prior transform run)."""
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four, institution_name, "
        "display_name) VALUES (?, ?, ?, ?)",  # noqa: S608  # test fixture insert
        [account_id, last_four, institution_name, display_name or f"acct {account_id}"],
    )


def test_no_candidate_mints_standalone(db: Database) -> None:
    """Empty (but present) core.dim_accounts -> a brand-new standalone account."""
    create_core_tables(db)  # dim exists but is empty: exercises the real query path
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(_src())
    assert resolved.is_new is True
    assert resolved.outcome == "minted_new"
    assert resolved.pending_decision_ids == ()
    assert len(resolved.account_id) == 12


def test_fuzzy_name_writes_pending(db: Database) -> None:
    """No last4/institution: a fuzzy account_name match -> a pending decision."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_account_key="chase-a",
            account_name="Chase Checking",
            last_four=None,
            institution=None,
        )
    )
    _seed_dim_account(db, account_id=first.account_id, display_name="Chase Checking")
    second = resolver.resolve(
        _src(
            source_type="ofx",
            source_account_key="chase-ofx",
            account_name="Chase Checkng",  # typo -> fuzzy match, not exact
            last_four=None,
            institution=None,
        )
    )
    assert second.outcome == "pending_review"
    assert len(second.pending_decision_ids) == 1
    dec = db.conn.execute(
        "SELECT candidate_account_id, match_reason FROM app.account_link_decisions "
        "WHERE decision_id = ?",
        [second.pending_decision_ids[0]],
    ).fetchone()
    assert dec == (first.account_id, "name")


def test_exact_name_match_writes_pending(db: Database) -> None:
    """An exact display_name slug match is still weak -> pending, never auto-merge."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_account_key="sav-a",
            account_name="Savings Account",
            last_four=None,
            institution=None,
        )
    )
    _seed_dim_account(db, account_id=first.account_id, display_name="Savings Account")
    second = resolver.resolve(
        _src(
            source_type="ofx",
            source_account_key="sav-ofx",
            account_name="Savings Account",  # exact -> match_account.matched=True
            last_four=None,
            institution=None,
        )
    )
    assert second.is_new is True
    assert second.account_id != first.account_id  # never auto-merged
    assert second.outcome == "pending_review"
    assert len(second.pending_decision_ids) == 1


def test_institution_last4_writes_pending_never_merges(db: Database) -> None:
    """A shared institution+last4 produces a pending decision, NOT an auto-merge."""
    # create core.dim_accounts so the candidate pass can see an existing account
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(source_account_key="wf-checking-a"))
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="4267",
        institution_name="wells_fargo",
    )
    second = resolver.resolve(
        _src(source_type="ofx", source_account_key="ofx-4267", last_four="4267")
    )
    assert second.is_new is True
    assert second.account_id != first.account_id
    assert second.outcome == "pending_review"
    assert len(second.pending_decision_ids) == 1
    dec = db.conn.execute(
        "SELECT provisional_account_id, candidate_account_id, status "
        "FROM app.account_link_decisions WHERE decision_id = ?",
        [second.pending_decision_ids[0]],
    ).fetchone()
    assert dec == (second.account_id, first.account_id, "pending")


def test_force_standalone_mints_despite_candidates(db: Database) -> None:
    """force_standalone declares a NEW account, skipping the merge-candidate pass."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(source_account_key="wf-checking-a"))
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="4267",
        institution_name="wells_fargo",
    )
    # Same institution+last4 would normally propose a merge; force_standalone
    # says "this is a distinct new account" — no pending decision is written.
    second = resolver.resolve(
        _src(
            source_type="ofx",
            source_account_key="ofx-4267",
            last_four="4267",
            force_standalone=True,
        )
    )
    assert second.is_new is True
    assert second.outcome == "minted_new"
    assert second.pending_decision_ids == ()
    assert second.account_id != first.account_id


def test_force_standalone_reimport_is_idempotent(db: Database) -> None:
    """A force_standalone re-import adopts the prior source_native, not a duplicate."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(source_account_key="wf-new", force_standalone=True))
    second = resolver.resolve(_src(source_account_key="wf-new", force_standalone=True))
    assert second.account_id == first.account_id
    assert second.is_new is False


def test_propose_force_standalone_reports_clean_new(db: Database) -> None:
    """propose() with force_standalone surfaces a declared-new verdict, no confirm."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(source_account_key="wf-checking-a"))
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="4267",
        institution_name="wells_fargo",
    )
    proposal = resolver.propose(
        _src(source_account_key="ofx-4267", last_four="4267", force_standalone=True)
    )
    assert proposal.is_new is True
    assert proposal.candidates == ()
    assert proposal.requires_confirm is False  # user declared it; no ambiguity


def test_cross_institution_slug_collision_stays_distinct(db: Database) -> None:
    """source_origin scopes source_native: same slug, different bank -> distinct mints."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    a = resolver.resolve(
        _src(source_origin="wells_fargo", source_account_key="checking")
    )
    b = resolver.resolve(
        _src(source_origin="chase", source_account_key="checking", institution="chase")
    )
    assert a.account_id != b.account_id
    assert b.pending_decision_ids == ()


def test_missing_dim_accounts_mints_standalone(db: Database) -> None:
    """First import before any transform: core.dim_accounts absent -> mint, no crash."""
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(_src())
    assert resolved.is_new is True
    assert resolved.outcome == "minted_new"


# ---------------------------------------------------------------------------
# M1S.4 — propose() read-only preview
# ---------------------------------------------------------------------------


def test_propose_surfaces_weak_candidate_without_writing(db: Database) -> None:
    """propose() returns a weak-signal candidate but writes nothing to app tables."""
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="wf_existing_01",
        last_four="4267",
        institution_name="wells_fargo",
        display_name="WF Checking",
    )
    resolver = AccountResolver(db, actor="system")
    src = _src()  # last_four="4267", institution="wells_fargo"
    proposal = resolver.propose(src)

    assert isinstance(proposal, AccountProposal)
    assert proposal.requires_confirm is True
    assert len(proposal.candidates) == 1
    assert proposal.candidates[0].signal == "institution_last4"
    assert proposal.candidates[0].display_name == "WF Checking"
    # Zero side effects — no rows written
    n_links = db.conn.execute("SELECT count(*) FROM app.account_links").fetchone()
    assert n_links is not None and n_links[0] == 0
    n_decisions = db.conn.execute(
        "SELECT count(*) FROM app.account_link_decisions"
    ).fetchone()
    assert n_decisions is not None and n_decisions[0] == 0


def test_propose_strong_ref_adopts_without_writing(db: Database) -> None:
    """propose() on a known source_native key returns adopted verdict with no new writes."""
    resolver = AccountResolver(db, actor="system")
    # Pre-insert one accepted source_native link directly via repo
    AccountLinksRepo(db).insert(
        link_id="link_pre",
        account_id="acct_existing",
        ref_kind="source_native",
        ref_value="wf-checking",
        source_type="csv",
        source_origin="wells_fargo",
        decided_by="auto",
        actor="system",
    )
    src = _src()  # source_native key = "wf-checking"
    proposal = resolver.propose(src)

    assert proposal.is_new is False
    assert proposal.adopted_via == "source_native"
    assert proposal.candidates == ()
    assert proposal.requires_confirm is False
    # propose() must not write new rows — only the pre-inserted link is present
    n_links = db.conn.execute("SELECT count(*) FROM app.account_links").fetchone()
    assert n_links is not None and n_links[0] == 1
    n_decisions = db.conn.execute(
        "SELECT count(*) FROM app.account_link_decisions"
    ).fetchone()
    assert n_decisions is not None and n_decisions[0] == 0


# ---------------------------------------------------------------------------
# M1S.5b — propose_existing() backfill read-only preview
# ---------------------------------------------------------------------------


def test_propose_existing_finds_candidates_excluding_self(db: Database) -> None:
    """propose_existing(A) finds B (same institution+last4) but not A itself."""
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="twin_a",
        last_four="9999",
        institution_name="chase",
        display_name="Chase Checking A",
    )
    _seed_dim_account(
        db,
        account_id="twin_b",
        last_four="9999",
        institution_name="chase",
        display_name="Chase Checking B",
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose_existing("twin_a")

    assert proposal is not None
    assert proposal.proposed_account_id == "twin_a"
    assert proposal.is_new is False
    assert len(proposal.candidates) == 1
    assert proposal.candidates[0].account_id == "twin_b"
    assert proposal.candidates[0].signal == "institution_last4"
    # twin_a must not appear as its own candidate
    assert all(c.account_id != "twin_a" for c in proposal.candidates)


def test_propose_existing_returns_none_for_absent_account(db: Database) -> None:
    """propose_existing on an account not in dim_accounts returns None."""
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    assert resolver.propose_existing("nonexistent_id") is None


def test_propose_existing_returns_none_when_no_candidates(db: Database) -> None:
    """propose_existing with no matching twins returns None (no candidates)."""
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="solo_acct",
        last_four="1111",
        institution_name="wells_fargo",
        display_name="Solo Account",
    )
    resolver = AccountResolver(db, actor="system")
    assert resolver.propose_existing("solo_acct") is None


def test_propose_existing_is_read_only(db: Database) -> None:
    """propose_existing writes nothing to app.account_links or account_link_decisions."""
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="ro_acct_a",
        last_four="5555",
        institution_name="bank_x",
        display_name="RO Account A",
    )
    _seed_dim_account(
        db,
        account_id="ro_acct_b",
        last_four="5555",
        institution_name="bank_x",
        display_name="RO Account B",
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose_existing("ro_acct_a")
    assert proposal is not None  # candidate found, but nothing written

    n_links = db.conn.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n_links is not None and n_links[0] == 0
    n_decisions = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_link_decisions"
    ).fetchone()
    assert n_decisions is not None and n_decisions[0] == 0


def test_propose_existing_guards_catalog_exception(db: Database) -> None:
    """propose_existing returns None when core.dim_accounts does not exist."""
    # No create_core_tables call → dim_accounts absent → CatalogException guarded
    resolver = AccountResolver(db, actor="system")
    assert resolver.propose_existing("any_id") is None
