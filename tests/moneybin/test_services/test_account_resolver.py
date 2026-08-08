"""Tests for AccountResolver (M1S.2 resolution ladder + M1S.4 propose())."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import AccountProposal, SourceAccount
from moneybin.services.account_resolver import (
    _FALLBACK_CANDIDATE_CAP,  # pyright: ignore[reportPrivateUsage]
    AccountResolver,
)
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


def test_same_issuer_same_last_four_merges_silently_today(db: Database) -> None:
    """PIN (finding 2 item 1): two distinct cards sharing a last-four merge silently.

    A PDF discloses only issuer + masked suffix, so both cards derive the SAME
    source-native key ``chase_xxxx1234`` (import_service.py:3368). That key fills
    the *strong*-ref slot, so ``_lookup_strong_ref`` hits on the second card and
    ``_run_ladder`` adopts at step 1 — before the candidate pass ever runs. The
    differing statement alias is contradicting evidence the resolver never looks
    at, because step 1 short-circuits.

    This test asserts TODAY's behavior deliberately. Flipping it to the
    refuse-and-route-to-review expectation requires a discriminator that can tell
    "same card, next statement" (which MUST keep adopting — that is the
    idempotency #371 established) from "second card, same last four". Until that
    discriminator exists, this pins the defect so it cannot regress unnoticed.

    **The account confirm gate does not reach this.** Every import channel now
    stops before load on an unratified account identity, which makes it
    tempting to read this merge as already covered. It is not: the gate fires
    on ``AccountProposal.requires_confirm``, and a step-1 adoption sets
    ``adopted_via`` with no candidates, so the predicate is False and no
    proposal is ever surfaced. This is a known residual, not a desired
    behavior.
    """
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_type="pdf",
            source_origin="chase",
            source_account_key="chase_xxxx1234",
            account_name="sapphire-reserve-june-statement",
            last_four="1234",
            institution="chase",
        )
    )
    second = resolver.resolve(
        _src(
            source_type="pdf",
            source_origin="chase",
            source_account_key="chase_xxxx1234",  # collides: different card, same last4
            account_name="freedom-unlimited-june-statement",
            last_four="1234",
            institution="chase",
        )
    )
    assert second.account_id == first.account_id  # silently merged
    assert second.outcome == "adopted_strong"
    assert second.pending_decision_ids == ()  # no confirm, no review, no trace


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
    institution_slug: str | None = None,
) -> None:
    """Insert a minimal core.dim_accounts row (simulates a prior transform run).

    ``institution_slug`` defaults to ``institution_name`` because most callers
    seed a value that is already slug-shaped. Pass both explicitly to model the
    real dim, where the name is for display and the slug is for matching.
    """
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four, institution_name, "
        "institution_slug, display_name) "
        "VALUES (?, ?, ?, ?, ?)",  # noqa: S608  # test fixture insert
        [
            account_id,
            last_four,
            institution_name,
            institution_slug if institution_slug is not None else institution_name,
            display_name or f"acct {account_id}",
        ],
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


def test_null_last_four_mint_is_quarantined_into_the_review_queue(
    db: Database,
) -> None:
    """A source with no last_four never becomes canonical silently.

    An account with a null last_four cannot participate in last4-based
    resolution at all, so a silent mint is a merge decision nobody ever sees —
    this is exactly how the Chase PDF placeholder (`chase_xxxx`, `last_four:
    null`) grew into a second copy of a card that already existed. Quarantine
    it: surface the pick-list so the mint lands in the identity-review queue.

    Fixture trips ONLY this guard. last_four=None makes the institution+last4
    rung and `_reissue_candidates` structurally unable to fire, and
    `test_find_candidates_no_fallback_by_default_keeps_backfill_quiet` pins
    that this exact source yields no candidates without the guard — so the
    assertion is non-vacuous.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(
        _src(account_name="Imported Statement", last_four=None, institution=None)
    )
    assert resolved.outcome == "pending_review"
    assert len(resolved.pending_decision_ids) == 1


def test_null_last_four_mints_cleanly_on_an_empty_book(db: Database) -> None:
    """The quarantine guard must not gate the very first import.

    With no existing accounts there is nothing to merge into, so there is no
    ambiguity to surface — the pick-list is empty and the mint is clean. This
    is what keeps the guard from turning every fresh profile's first import
    into a review queue.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(
        _src(account_name="Imported Statement", last_four=None, institution=None)
    )
    assert resolved.outcome == "minted_new"
    assert resolved.pending_decision_ids == ()


def test_a_blank_last_four_is_quarantined_like_a_missing_one(db: Database) -> None:
    """A blank mask is a missing last four, not a last four that happens to be empty.

    `SyncAccount.mask` declares only a maximum length, so the sync server — opaque
    to this client by design — can legitimately send `""`. It reaches the resolver
    as `SourceAccount.last_four` and answers the last4 rung with exactly the
    silence `None` does, but a gate written against `None` alone reads it as an
    answer and mints silently. That is the one failure this quarantine exists to
    prevent, arriving through the one source MoneyBin does not control.

    Fixture trips ONLY this guard: identical in shape to the null-last_four
    quarantine above, whose companion `test_known_last_four_with_no_signal_still_
    mints_silently` pins that a *present* last_four still mints cleanly here.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(
        _src(account_name="Imported Statement", last_four="", institution=None)
    )
    assert resolved.outcome == "pending_review"
    assert len(resolved.pending_decision_ids) == 1


def test_a_forced_quarantine_offers_every_account_however_many_exist(
    db: Database,
) -> None:
    """The account you need to merge into is always on the list.

    The pick-list is capped so an opt-in fallback cannot flood the review queue,
    and it is ordered by opaque account id — so past the cap, *which* accounts
    survive is arbitrary. That is tolerable where the human asked for a pick-list
    and can decline it. It is not tolerable here: this review is forced open by
    the quarantine, and `AccountLinksService.set()` accepts only a target already
    attached to the decision, so an account left off the list cannot be chosen at
    all. The user would be asked which account this is, find none of the answers
    right, and have `--standalone` as the only exit — which re-mints the very
    duplicate the quarantine was raised to prevent.
    """
    create_core_tables(db)
    for i in range(_FALLBACK_CANDIDATE_CAP + 5):
        _seed_dim_account(db, account_id=f"acct_{i:03d}", display_name=f"Account {i}")
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(
        _src(account_name="Imported Statement", last_four=None, institution=None)
    )

    assert resolved.outcome == "pending_review"
    assert len(resolved.pending_decision_ids) == _FALLBACK_CANDIDATE_CAP + 5


def test_a_forced_quarantine_reaches_past_a_matching_institution(
    db: Database,
) -> None:
    """An institution match must not hide the account the merge actually needs.

    Lifting the cap only made the list long enough; it did not make it complete.
    When the source resolves an institution that matches *anything*, the scoped
    branch returns that subset and stops — so an account whose `institution_slug`
    is absent or drifted (the cross-source slug drift this fallback already
    documents) never reaches the decision. `AccountLinksService.set()` accepts
    only a target attached to the decision, so that account is unpickable and
    `--standalone` is again the only exit, re-minting the duplicate the
    quarantine exists to prevent.

    Isolation: the cap is irrelevant here — three accounts, far below it. The
    forced review is open (`last_four=None`) and one account matches the source's
    institution. Only reaching past the scoped subset can put the drifted account
    on the list.
    """
    create_core_tables(db)
    _seed_dim_account(db, account_id="acct_same_inst", institution_name="chase")
    _seed_dim_account(db, account_id="acct_no_slug", institution_slug=None)
    _seed_dim_account(db, account_id="acct_other_inst", institution_name="wells")
    resolver = AccountResolver(db, actor="system")

    resolved = resolver.resolve(
        _src(account_name="Imported Statement", last_four=None, institution="chase")
    )

    assert resolved.outcome == "pending_review"
    offered = {
        row[0]
        for row in db.execute(
            "SELECT candidate_account_id FROM app.account_link_decisions"
        ).fetchall()
    }
    assert offered == {"acct_same_inst", "acct_no_slug", "acct_other_inst"}


def test_a_blank_last_four_normalizes_to_none_on_the_source_account() -> None:
    """One spelling of "absent" reaches every consumer.

    The resolver asks whether a last four is missing in two conventions — `is
    None` at the quarantine gates, falsy at the last4 lookup and the reissue
    pass. Canonicalizing at construction is what keeps the two from disagreeing;
    without it the file's own conventions answer the same question differently.
    """
    assert _src(last_four="").last_four is None


def test_a_whitespace_only_last_four_normalizes_like_a_blank_one() -> None:
    """A mask of spaces answers the last4 rung with silence too.

    ``SyncAccount.mask`` declares only a maximum length, so `" "` validates and
    arrives here truthy and non-None — clearing the quarantine gate that `""`
    cannot. Padding around a real mask is the same defect one step along: it
    would miss the exact-match last4 lookup and mint a second account for a
    ledger that already has one.
    """
    assert _src(last_four="   ").last_four is None
    assert _src(last_four=" 1234 ").last_four == "1234"


def test_known_last_four_with_no_signal_still_mints_silently(db: Database) -> None:
    """The guard keys on a MISSING last_four, not on the absence of candidates.

    A source that carries a last_four can participate in last4 resolution; its
    silence is real evidence of a distinct account, not an unanswerable
    question. Pairs with the quarantine test above — same shape, last_four
    present — so the two together pin which condition the guard reads.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(
        _src(account_name="Imported Statement", last_four="9911", institution=None)
    )
    assert resolved.outcome == "minted_new"
    assert resolved.pending_decision_ids == ()


def test_propose_quarantines_null_last_four_without_opting_into_fallback(
    db: Database,
) -> None:
    """propose() must agree with resolve() on the quarantine, at the default.

    The gate calls propose() without fallback, so if the quarantine lived only
    in resolve() an interactive import would load rows first and surface the
    question afterwards — the "magic stays visible" gap the guard exists to
    close.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose(
        _src(account_name="Imported Statement", last_four=None, institution=None)
    )
    assert {c.account_id for c in proposal.candidates} == {"acct_a"}


def test_propose_quarantines_a_blank_last_four_without_opting_into_fallback(
    db: Database,
) -> None:
    """propose() agrees with resolve() on a blank mask, as it does on a null one.

    The interactive import gate calls propose() without fallback. If the blank
    case were quarantined only in resolve(), the gate would load rows first and
    surface the question afterwards — the "magic stays visible" gap in reverse.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose(
        _src(account_name="Imported Statement", last_four="", institution=None)
    )
    assert {c.account_id for c in proposal.candidates} == {"acct_a"}


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


def test_institution_last4_matches_across_case(db: Database) -> None:
    """institution+last4 fires when the stored ORG differs in case from the slug.

    OFX stores institution_name as raw ``<ORG>`` (e.g. ``CHASE``); a later import
    carries the slug (``chase``). An exact text match would never fire — both
    must slugify-compare equal for the cross-source signal to surface.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(source_account_key="chase-a", institution="chase"))
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="4267",
        institution_name="CHASE",  # raw OFX <ORG>, uppercase
    )
    second = resolver.resolve(
        _src(
            source_type="ofx",
            source_account_key="ofx-4267",
            last_four="4267",
            institution="chase",
        )
    )
    assert second.outcome == "pending_review"
    assert len(second.pending_decision_ids) == 1
    dec = db.conn.execute(
        "SELECT candidate_account_id, match_reason "
        "FROM app.account_link_decisions WHERE decision_id = ?",
        [second.pending_decision_ids[0]],
    ).fetchone()
    assert dec == (first.account_id, "institution_last4")


def test_renamed_csv_label_reassociates_via_last4_not_duplicate(db: Database) -> None:
    """Renamed Monarch account re-associates via last4, never mints a duplicate.

    A Monarch account renamed Daily Expense (...1789) -> Fun Money (...1789)
    re-associates onto the original via a PENDING institution_last4 decision —
    not a duplicate mint that silently merges (Decision 8 mutable-label
    behavior). The renamed import has a different source_account_key (slug) so
    the strong-ref lookup misses; the unchanged last4 drives the candidate pass.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_origin="monarch",
            source_account_key="daily-expense-1789",
            account_name="Daily Expense (...1789)",
            last_four="1789",
            institution="wells_fargo",
        )
    )
    # Simulate the transform landing first's derived last4 + institution in dim.
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="1789",
        institution_name="wells_fargo",
        display_name="Daily Expense",
    )
    renamed = resolver.resolve(
        _src(
            source_origin="monarch",
            source_account_key="fun-money-1789",
            account_name="Fun Money (...1789)",
            last_four="1789",
            institution="wells_fargo",
        )
    )
    # Not silently merged: the renamed import minted its own provisional account.
    assert renamed.account_id != first.account_id
    # ...but a pending institution_last4 proposal points back at the original.
    pend = db.execute(
        "SELECT candidate_account_id, match_reason FROM app.account_link_decisions "
        "WHERE status = 'pending'"
    ).fetchall()
    assert any(
        c == first.account_id and reason == "institution_last4" for c, reason in pend
    ), pend


def test_reissued_card_matching_name_still_surfaces_for_review(db: Database) -> None:
    """A reissue whose display name survives is caught by the NAME signal today.

    Probe for finding 1: institution matches, last-four changed. Signal 1 cannot
    fire (last4 is different by definition). This pins which guard actually
    catches the reissue so the fix targets the real gap.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_origin="chase",
            source_account_key="chase_xxxx1234",
            account_name="Chase Sapphire",
            last_four="1234",
            institution="chase",
        )
    )
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="1234",
        institution_name="chase",
        display_name="Chase Sapphire",
    )
    reissued = resolver.resolve(
        _src(
            source_origin="chase",
            source_account_key="chase_xxxx5678",
            account_name="Chase Sapphire",  # name survives the reissue
            last_four="5678",  # ...but the card number does not
            institution="chase",
        )
    )
    assert reissued.outcome == "pending_review"
    assert len(reissued.pending_decision_ids) == 1


def test_reissued_card_dissimilar_alias_surfaces_for_review(db: Database) -> None:
    """A reissue with no surviving name signal must still reach the review queue.

    The PDF path sets ``account_name`` to the per-file filename alias
    (import_service.py:3398), so a reissue changes BOTH the last-four and the
    alias. Neither of ``_find_candidates``' two signals can fire, so before the
    reissue signal the second card minted with no confirm, no review entry, and
    no trace — a bank statement silently fragmenting into a second account.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_type="pdf",
            source_origin="chase",
            source_account_key="chase_xxxx1234",
            account_name="chase-june-2026-statement",
            last_four="1234",
            institution="chase",
        )
    )
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="1234",
        institution_name="chase",
        display_name="chase-june-2026-statement",
    )
    reissued = resolver.resolve(
        _src(
            source_type="pdf",
            source_origin="chase",
            source_account_key="chase_xxxx5678",
            account_name="replacement card ending 5678",
            last_four="5678",
            institution="chase",
        )
    )
    # Never auto-merged — a weak signal proposes, it does not decide.
    assert reissued.account_id != first.account_id
    # ...but it must not vanish either: the same institution with a changed
    # last-four is evidence enough to surface a review, not to mint in silence.
    assert reissued.outcome == "pending_review"
    assert len(reissued.pending_decision_ids) == 1
    dec = db.conn.execute(
        "SELECT candidate_account_id, match_reason FROM app.account_link_decisions "
        "WHERE decision_id = ?",
        [reissued.pending_decision_ids[0]],
    ).fetchone()
    # Its own signal string, not the fallback pick-list's "institution" — the
    # review queue has to show which of the two proposed this pairing.
    assert dec == (first.account_id, "institution_reissue")


def test_institution_last4_skips_when_slug_is_empty(db: Database) -> None:
    """A purely non-alphanumeric institution slugifies to '' and must not match.

    '###' and '@@@' both slugify to '' (slugify strips non-alphanumerics), so an
    empty slug would otherwise spuriously equal any stored institution that also
    slugifies to '' sharing the last_four — a false merge proposal. The
    institution+last4 rung is skipped when the slug is empty.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(source_account_key="a", institution="@@@"))
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="4267",
        institution_name="@@@",  # slugifies to ''
    )
    second = resolver.resolve(
        _src(source_type="ofx", source_account_key="ofx-x", institution="###")
    )
    assert second.outcome == "minted_new"


def test_find_candidates_prefers_institution_last4_over_name(db: Database) -> None:
    """Institution+last4 suppresses the weak name signal (Decision 8 bridge precedence).

    When institution+last4 match an existing dim account, the candidate pass
    returns an institution_last4 proposal and SUPPRESSES the weak name signal.
    The seeded display_name deliberately equals the source account_name so the
    name branch WOULD fire if not short-circuited — making the `no name candidate`
    assertion non-vacuous.
    """
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="acct_ofx",
        last_four="4267",
        institution_name="WELLS FARGO",
        display_name="WF Checking 4267",
    )
    resolver = AccountResolver(db, actor="system")
    candidates = resolver._find_candidates(  # type: ignore[reportPrivateUsage]  # pin candidate-pass precedence
        _src(
            institution="Wells Fargo",
            last_four="4267",
            account_name="WF Checking 4267",
        ),
        exclude_account_id="prov_new",
    )
    assert any(
        c.signal == "institution_last4" and c.account_id == "acct_ofx"
        for c in candidates
    ), candidates
    assert not any(c.signal == "name" for c in candidates), (
        "name must not fire when institution+last4 matches (bridge precedence)"
    )


def test_find_candidates_fallback_surfaces_accounts_when_no_signal(
    db: Database,
) -> None:
    """Gate fallback: no last4/institution/name match -> surface existing accounts.

    The bare single-account import gate had no decision support (candidates: []);
    fallback=True surfaces the user's accounts as a low-confidence pick-list so a
    human picks from a list instead of supplying a raw account_id.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    _seed_dim_account(
        db, account_id="acct_b", display_name="Citi Savings", institution_name="CITI"
    )
    resolver = AccountResolver(db, actor="system")
    src = _src(account_name="Imported Statement", last_four=None, institution=None)
    candidates = resolver._find_candidates(  # type: ignore[reportPrivateUsage]  # exercise fallback directly
        src, exclude_account_id="prov", fallback=True
    )
    assert {c.account_id for c in candidates} == {"acct_a", "acct_b"}, candidates
    assert all(c.signal == "fallback" for c in candidates)


def test_find_candidates_no_fallback_by_default_keeps_backfill_quiet(
    db: Database,
) -> None:
    """Fallback defaults False so the backfill link queue isn't flooded with all-accounts."""
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    src = _src(account_name="Imported Statement", last_four=None, institution=None)
    assert (
        resolver._find_candidates(src, exclude_account_id="prov") == []  # type: ignore[reportPrivateUsage]  # default no-fallback
    )


def test_find_candidates_fallback_scopes_to_institution_when_known(
    db: Database,
) -> None:
    """When the source resolves an institution, the fallback pick-list scopes to it.

    The source carries a last_four so this exercises the *opt-in* fallback alone.
    A null one would additionally force the quarantine open, which deliberately
    reaches past the institution scope — and a fixture that trips both narrowings
    isolates neither.
    """
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="acct_chase",
        display_name="Chase Checking",
        institution_name="CHASE",
    )
    _seed_dim_account(
        db, account_id="acct_citi", display_name="Citi Savings", institution_name="CITI"
    )
    resolver = AccountResolver(db, actor="system")
    src = _src(account_name="Imported Statement", last_four="0000", institution="chase")
    candidates = resolver._find_candidates(  # type: ignore[reportPrivateUsage]  # exercise fallback scoping
        src, exclude_account_id="prov", fallback=True
    )
    assert {c.account_id for c in candidates} == {"acct_chase"}
    assert all(c.signal == "institution" for c in candidates)


def test_find_candidates_fallback_lists_all_when_institution_scope_empty(
    db: Database,
) -> None:
    """Institution-scoping must never produce an empty pick-list when accounts exist.

    The CSV-resolved institution slug often doesn't match dim_accounts'
    institution_name (cross-source slug drift, or an account name polluting a
    saved format's institution). When institution-scoping matches nothing,
    fall through to listing all accounts — the whole point of the fallback is a
    non-empty pick-list, so a mismatched scope must not re-create candidates: [].

    The source carries a last_four so the fallthrough is what produces the full
    list here, not the forced quarantine's own reach-past.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_wf1", display_name="WF Checking", institution_name="WF"
    )
    _seed_dim_account(
        db, account_id="acct_wf2", display_name="WF Savings", institution_name="WF"
    )
    resolver = AccountResolver(db, actor="system")
    # institution resolves to a slug that matches no dim account (WF vs the
    # polluted "wf_checking_9940").
    src = _src(
        account_name="Imported Statement",
        last_four="0000",
        institution="wf_checking_9940",
    )
    candidates = resolver._find_candidates(  # type: ignore[reportPrivateUsage]  # exercise scope-empty fallthrough
        src, exclude_account_id="prov", fallback=True
    )
    assert {c.account_id for c in candidates} == {"acct_wf1", "acct_wf2"}
    assert all(c.signal == "fallback" for c in candidates)


def test_propose_surfaces_fallback_pick_list_at_gate(db: Database) -> None:
    """propose(fallback=True) (the bare gate) returns existing accounts when nothing clears."""
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose(
        _src(account_name="Imported Statement", last_four=None, institution=None),
        fallback=True,
    )
    assert proposal.is_new is True
    assert {c.account_id for c in proposal.candidates} == {"acct_a"}


def test_reissue_signal_outranks_the_fallback_pick_list(db: Database) -> None:
    """When both could fire at the gate, the reissue signal is what surfaces.

    ``_find_candidates`` tries reissue before fallback and short-circuits, so a
    targeted guess beats a blind everyone-at-this-bank list. Nothing pinned that
    ordering: every other ``fallback=True`` test sets ``last_four=None``, which
    makes ``_reissue_candidates`` unable to fire at all, so the two were never
    eligible in the same call.

    This fixture makes both eligible and only both — same institution, so
    fallback applies; last-fours present on both sides and differing, so reissue
    applies; and a dissimilar display name so the fuzzy signal stays silent.
    """
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="acct_a",
        display_name="Sapphire Reserve",
        institution_name="CHASE",
        last_four="1234",
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose(
        _src(
            account_name="replacement card ending 5678",
            last_four="5678",
            institution="chase",
        ),
        fallback=True,
    )
    assert [c.signal for c in proposal.candidates] == ["institution_reissue"]
    assert {c.account_id for c in proposal.candidates} == {"acct_a"}


def test_propose_no_fallback_by_default_keeps_multi_account_mint_silent(
    db: Database,
) -> None:
    """propose() default (multi-account gate) does NOT fall back to a pick-list.

    A no-match named account in a multi-account file must mint silently, not gate
    the whole import — so the default propose() returns no candidates here.

    The source carries a last_four deliberately. This fixture originally set it
    to None, which meant the null-last_four quarantine ALSO suppressed the
    silent mint — two guards on one fixture, so neither was isolated. The
    behavior under test is a *named* account whose name matches nothing; the
    missing-last_four case is its own test above.
    """
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    resolver = AccountResolver(db, actor="system")
    proposal = resolver.propose(
        _src(account_name="Imported Statement", last_four="9911", institution=None)
    )
    assert proposal.is_new is True
    assert proposal.candidates == ()


def test_propose_existing_does_not_flood_with_fallback(db: Database) -> None:
    """Backfill returns None (no proposal) rather than an all-accounts fallback."""
    create_core_tables(db)
    _seed_dim_account(
        db, account_id="acct_a", display_name="Chase Checking", institution_name="CHASE"
    )
    _seed_dim_account(
        db, account_id="acct_b", display_name="Citi Savings", institution_name="CITI"
    )
    resolver = AccountResolver(db, actor="system")
    assert resolver.propose_existing("acct_a") is None


def test_propose_existing_does_not_emit_reissue_candidates(db: Database) -> None:
    """Backfill stays quiet on the reissue signal — an established book is known-distinct.

    Fixture trips ONLY the reissue guard: same institution, both last-fours
    known and different (so signal 1 misses), dissimilar display names below the
    0.6 fuzzy threshold (so signal 2 misses). If ``propose_existing`` passed
    ``reissue=True``, every pair of same-issuer cards in an existing book would
    propose against every other — noise, not signal.
    """
    create_core_tables(db)
    _seed_dim_account(
        db,
        account_id="acct_a",
        display_name="Sapphire Reserve",
        institution_name="CHASE",
        last_four="1234",
    )
    _seed_dim_account(
        db,
        account_id="acct_b",
        display_name="Freedom Unlimited",
        institution_name="CHASE",
        last_four="5678",
    )
    resolver = AccountResolver(db, actor="system")
    assert resolver.propose_existing("acct_a") is None


def test_mint_claims_full_number_strong_ref_for_later_adopt(db: Database) -> None:
    """A minted account claims its scoped full_number so a later source adopts it.

    Without claiming the strong ref on mint, a second source carrying the same
    scoped full number mints a DUPLICATE instead of auto-adopting the same real
    account (step 1 already proved no conflict, so the claim is safe).
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    # First import mints (empty dim_accounts → no candidates) and must claim the
    # scoped full_number strong ref.
    first = resolver.resolve(
        _src(
            source_type="ofx",
            source_origin="chase",
            source_account_key="1111",
            account_number="121000248:1111",
            last_four=None,
            institution=None,
        )
    )
    assert first.is_new is True
    assert first.outcome == "minted_new"
    # A different source carrying the SAME scoped full_number auto-adopts.
    second = resolver.resolve(
        _src(
            source_type="csv",
            source_origin="chase-csv",
            source_account_key="chk",
            account_number="121000248:1111",
            last_four=None,
            institution=None,
        )
    )
    assert second.account_id == first.account_id
    assert second.is_new is False
    assert second.outcome == "adopted_strong"


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


def test_resolve_rolls_back_partial_writes_on_failure(db: Database) -> None:
    """resolve() is atomic per account: a mid-resolve failure rolls everything back.

    _write_strong_ref runs after _write_native_mapping in every branch. Forcing
    it to raise leaves the native mapping mid-flight; without a single enclosing
    transaction the mapping would auto-commit and a later same-id import would
    adopt a half-written account.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    with patch.object(
        AccountResolver, "_write_strong_ref", side_effect=RuntimeError("boom")
    ):
        with pytest.raises(RuntimeError):
            resolver.resolve(_src(account_number="121000248:1111"))

    n = db.conn.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0


def test_institution_matching_compares_slugs_not_display_names(db: Database) -> None:
    """A display name that slugifies away from its own slug must still match.

    `core.dim_accounts` carries a human-readable `institution_name` resolved
    from the OFX <FID> ("U.S. Bank"), while a source supplies the registry slug
    ("us_bank"). Slugifying the display name is not the inverse of the registry:
    `slugify("U.S. Bank")` is `u-s-bank` and `slugify("us_bank")` is `us-bank`,
    so comparing against the name silently drops the candidate.

    U.S. Bank is the fixture precisely because it is the seeds row where the two
    disagree — Chase and Citi are single words and would pass either way, so
    they cannot isolate this. Matching therefore reads `institution_slug`, the
    dim's canonical registry slug, on both sides.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(source_type="ofx", institution="us_bank", last_four="1111")
    )
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="1111",
        institution_name="U.S. Bank",
        institution_slug="us_bank",
        display_name="U.S. Bank Checking …1111",
    )

    # Same institution, last four changed: only the reissue signal may fire, so
    # a hit proves the institution comparison itself succeeded.
    proposal = resolver.propose(
        _src(
            source_type="ofx",
            source_account_key="9876",
            institution="us_bank",
            last_four="9876",
            account_name="replacement card",
        )
    )
    assert [c.signal for c in proposal.candidates] == ["institution_reissue"], (
        proposal.candidates
    )


def test_institution_matching_canonicalizes_a_hand_written_name(db: Database) -> None:
    """A sheet's "U.S. Bank" must meet the registry's "us_bank".

    The mirror of the case above. There the dim held the curated slug and the
    source supplied it too; here the source is a Tiller-style sheet whose
    Institution column is human-written display text, matched against an
    account an OFX statement minted. Slugifying both sides cannot close that
    gap — `u-s-bank` and `us-bank` — because the registry's slug is curated,
    not derived from the name. Only a lookup through the registry collapses
    every spelling of one institution onto a single key.
    """
    create_core_tables(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(source_type="ofx", institution="us_bank", last_four="1111")
    )
    _seed_dim_account(
        db,
        account_id=first.account_id,
        last_four="1111",
        institution_name="U.S. Bank",
        institution_slug="us_bank",
        display_name="U.S. Bank Checking …1111",
    )

    proposal = resolver.propose(
        _src(
            source_type="csv",
            source_account_key="usb-checking",
            institution="U.S. Bank",
            last_four="1111",
            # Deliberately unlike the dim's display_name: the name signal would
            # otherwise answer for the institution signal under test.
            account_name="statement import",
        )
    )
    assert [c.signal for c in proposal.candidates] == ["institution_last4"], (
        proposal.candidates
    )
