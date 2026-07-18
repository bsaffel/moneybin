"""Tests for MerchantLinksService (M1T review-queue service).

Fixture layout:
- ``seeded_pending_decision``: inserts one pending ``merchant_link_decision``
  row via ``MerchantLinkDecisionsRepo`` with ``ref_value="ent_pending"``,
  ``source_type="plaid"``, ``candidate_merchant_id="mCandidate"``.
- Tests use the function-scoped ``db`` fixture from conftest.
"""

from __future__ import annotations

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo
from moneybin.services.merchant_links_service import MerchantLinksService

# ---------------------------------------------------------------------------
# Seeded constants
# ---------------------------------------------------------------------------

_DECISION_ID = "d_seeded"
_REF_VALUE = "ent_pending"
_SOURCE_TYPE = "plaid"
_CANDIDATE_MERCHANT_ID = "mCandidate"


def _seed_merchants(db: Database, *merchant_ids: str) -> None:
    """Make the given merchant ids resolvable in ``core.dim_merchants``.

    The dim is a view over ``app.user_merchants``; seeding the source rows is
    what makes the accept-path existence check pass. The accept validator
    rejects a ``target_merchant_id`` absent from this catalog (no dangling FK).
    """
    for mid in merchant_ids:
        db.execute(
            "INSERT INTO app.user_merchants "
            "(merchant_id, match_type, canonical_name, created_by) "
            "VALUES (?, 'oneOf', ?, 'user')",
            [mid, f"Name {mid}"],
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def seeded_pending_decision(db: Database) -> None:
    """Insert one pending merchant_link_decision row via the repo (Invariant-10 path)."""
    MerchantLinkDecisionsRepo(db).insert(
        decision_id=_DECISION_ID,
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        provider_merchant_name="Starbucks",
        candidate_merchant_id=_CANDIDATE_MERCHANT_ID,
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "Starbucks"},
        decided_by="auto",
        actor="system",
        status="pending",
    )


# ---------------------------------------------------------------------------
# count_pending
# ---------------------------------------------------------------------------


def test_count_pending_empty(db: Database) -> None:
    """Zero pending when no decisions exist."""
    svc = MerchantLinksService(db)
    assert svc.count_pending() == 0


def test_count_pending_one(db: Database, seeded_pending_decision: None) -> None:
    """One pending after seeding a single decision."""
    svc = MerchantLinksService(db)
    assert svc.count_pending() == 1


# ---------------------------------------------------------------------------
# accept_impact
# ---------------------------------------------------------------------------


def test_accept_impact_counts_binding_and_every_decision_row(
    db: Database, seeded_pending_decision: None
) -> None:
    """Impact includes the inserted link and named plus sibling decisions."""
    MerchantLinkDecisionsRepo(db).insert(
        decision_id="d_sibling",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mSibling",
        confidence_score=0.4,
        match_signals={"signal": "fuzzy_name", "value": "sibling"},
        decided_by="auto",
        actor="system",
    )
    _seed_merchants(db, _CANDIDATE_MERCHANT_ID)

    impact = MerchantLinksService(db).accept_impact(
        _DECISION_ID,
        target_merchant_id=_CANDIDATE_MERCHANT_ID,
    )

    assert impact.candidate_merchant_id == _CANDIDATE_MERCHANT_ID
    assert impact.blast_radius == {
        "merchants": 1,
        "merchant_links": 1,
        "merchant_link_decisions": 2,
    }


# ---------------------------------------------------------------------------
# set — accept path
# ---------------------------------------------------------------------------


def test_set_accept_binds_and_clears_pending(
    db: Database, seeded_pending_decision: None
) -> None:
    """Accept creates the binding and clears the pending count to 0."""
    _seed_merchants(db, _CANDIDATE_MERCHANT_ID)
    svc = MerchantLinksService(db, actor="cli")
    assert svc.count_pending() == 1

    svc.set(_DECISION_ID, target_merchant_id=_CANDIDATE_MERCHANT_ID)

    assert svc.count_pending() == 0
    # The accepted binding now exists via MerchantLinksRepo.
    bound = MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE)
    assert bound == _CANDIDATE_MERCHANT_ID


def test_set_accept_auto_rejects_siblings(db: Database) -> None:
    """Accept of one decision auto-rejects sibling pending decisions for same ref_value."""
    decisions = MerchantLinkDecisionsRepo(db)
    # Seed two pending decisions for the same ref_value.
    decisions.insert(
        decision_id="d_primary",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mPrimary",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "primary"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_sibling",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mSibling",
        confidence_score=0.4,
        match_signals={"signal": "fuzzy_name", "value": "sibling"},
        decided_by="auto",
        actor="system",
    )

    _seed_merchants(db, "mPrimary")
    svc = MerchantLinksService(db, actor="cli")
    assert svc.count_pending() == 1  # 2 decisions but 1 distinct ref_value

    svc.set("d_primary", target_merchant_id="mPrimary")

    assert svc.count_pending() == 0

    # Sibling must be rejected.
    sibling = decisions.fetch_by_id("d_sibling")
    assert sibling is not None
    assert sibling["status"] == "rejected"


# ---------------------------------------------------------------------------
# set — reject path
# ---------------------------------------------------------------------------


def test_set_reject_clears_pending_without_binding(
    db: Database, seeded_pending_decision: None
) -> None:
    """Reject (target_merchant_id=None) clears pending without creating a binding."""
    svc = MerchantLinksService(db, actor="cli")

    svc.set(_DECISION_ID, target_merchant_id=None)

    assert svc.count_pending() == 0
    # No binding should exist.
    assert MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE) is None


def test_set_empty_string_target_is_an_input_error_not_a_silent_reject(
    db: Database, seeded_pending_decision: None
) -> None:
    """An empty-string target is an input error — it neither binds NOR rejects.

    Inferring "reject" from an empty string means a caller that MEANT to accept,
    but computed its target into an empty value, silently discards the pairing
    instead — and a reject is a decision the user has to actually make. Only
    ``None`` rejects. Matches ``AccountLinksService``; the decision must survive
    as pending so the mistake is recoverable.
    """
    svc = MerchantLinksService(db, actor="cli")

    with pytest.raises(UserError) as exc:
        svc.set(_DECISION_ID, target_merchant_id="")

    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT
    assert MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE) is None
    decision = MerchantLinkDecisionsRepo(db).fetch_by_id(_DECISION_ID)
    assert decision is not None and decision["status"] == "pending"


def test_set_reject_sweeps_all_siblings(db: Database) -> None:
    """[7] Reject (--new) rejects the named decision AND every pending sibling."""
    decisions = MerchantLinkDecisionsRepo(db)
    decisions.insert(
        decision_id="d_rej_a",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mA",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "a"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_rej_b",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mB",
        confidence_score=0.4,
        match_signals={"signal": "fuzzy_name", "value": "b"},
        decided_by="auto",
        actor="system",
    )

    svc = MerchantLinksService(db, actor="cli")
    assert svc.count_pending() == 1  # 2 decisions, 1 distinct ref_value

    svc.set("d_rej_a", target_merchant_id=None)

    assert svc.count_pending() == 0
    for did in ("d_rej_a", "d_rej_b"):
        row = decisions.fetch_by_id(did)
        assert row is not None and row["status"] == "rejected", (
            f"{did} must be rejected"
        )
    # No binding written by a reject sweep.
    assert MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE) is None


def test_set_accept_unknown_merchant_raises_and_writes_nothing(
    db: Database, seeded_pending_decision: None
) -> None:
    """[3] Accept into the decision's candidate (which doesn't exist) raises UserError, no binding.

    The confirming safety check (target == candidate) passes since we pass the
    decision's own candidate_merchant_id. The merchant-existence check then fires
    because _CANDIDATE_MERCHANT_ID was never seeded into core.dim_merchants.
    """
    # Do NOT seed _CANDIDATE_MERCHANT_ID — we want the existence check to fail.
    _seed_merchants(db, "mReal")  # a different merchant exists, but target does NOT

    svc = MerchantLinksService(db, actor="cli")
    with pytest.raises(UserError) as exc:
        # Pass the decision's own candidate (confirming check) but it's absent from dim.
        svc.set(_DECISION_ID, target_merchant_id=_CANDIDATE_MERCHANT_ID)

    assert exc.value.code == error_codes.MUTATION_NOT_FOUND
    # Nothing written; decision rolled back to pending.
    assert MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE) is None
    assert svc.count_pending() == 1


def test_set_accept_known_merchant_binds(
    db: Database, seeded_pending_decision: None
) -> None:
    """[3] Accept into a merchant present in core.dim_merchants binds successfully."""
    _seed_merchants(db, _CANDIDATE_MERCHANT_ID)

    svc = MerchantLinksService(db, actor="cli")
    svc.set(_DECISION_ID, target_merchant_id=_CANDIDATE_MERCHANT_ID)

    assert (
        MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE) == _CANDIDATE_MERCHANT_ID
    )
    assert svc.count_pending() == 0


def test_set_accept_uniqueness_conflict_raises_usererror(
    db: Database, seeded_pending_decision: None
) -> None:
    """[10] When the entity is already bound to a different merchant, accept surfaces a UserError.

    No raw ``ValueError`` leaks to CLI/MCP, and the message must not embed the
    provider entity id (``ref_value``).
    """
    # The candidate must exist so validation passes and we reach the bind step.
    _seed_merchants(db, _CANDIDATE_MERCHANT_ID)
    # Pre-bind the same (source_type, ref_value) to a different merchant.
    MerchantLinksRepo(db).insert(
        link_id="lk_pre",
        merchant_id="mOther",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        decided_by="auto",
        actor="system",
    )

    svc = MerchantLinksService(db, actor="cli")
    with pytest.raises(UserError) as exc:
        svc.set(_DECISION_ID, target_merchant_id=_CANDIDATE_MERCHANT_ID)

    assert exc.value.code == error_codes.MUTATION_CONSTRAINT_VIOLATION
    assert _REF_VALUE not in str(exc.value), (
        "error must not leak the provider entity id"
    )


# ---------------------------------------------------------------------------
# set — confirming safety check (#3)
# ---------------------------------------------------------------------------


def test_set_wrong_target_raises_user_error(
    db: Database, seeded_pending_decision: None
) -> None:
    """target_merchant_id != decision's candidate_merchant_id → UserError.

    Mirrors test_account_links_service.py::test_set_wrong_target_raises_user_error.
    Passing a merchant id that is not the decision's candidate must raise
    MUTATION_INVALID_INPUT before checking whether the merchant exists at all.
    """
    _seed_merchants(db, "mOtherMerchant")  # different from _CANDIDATE_MERCHANT_ID
    svc = MerchantLinksService(db, actor="cli")

    with pytest.raises(UserError, match="does not match"):
        svc.set(_DECISION_ID, target_merchant_id="mOtherMerchant")

    assert svc.count_pending() == 1  # rolled back; still pending


# ---------------------------------------------------------------------------
# set — sibling sweep scoped to source_type (#2)
# ---------------------------------------------------------------------------


def test_reject_siblings_does_not_sweep_different_source_type(db: Database) -> None:
    """_reject_pending_siblings only rejects siblings with the SAME source_type.

    Two pending decisions share the same ref_value but have DIFFERENT source_types
    (plaid vs simplefin). Accepting/rejecting the plaid decision must NOT sweep
    the simplefin decision — the (source_type, ref_value) pair is the correct
    dedup key.
    """
    decisions = MerchantLinkDecisionsRepo(db)
    decisions.insert(
        decision_id="d_plaid",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_merchant_id="mPlaid",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "plaid"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_simplefin",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type="simplefin",
        candidate_merchant_id="mSimpleFin",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "simplefin"},
        decided_by="auto",
        actor="system",
    )

    _seed_merchants(db, "mPlaid")
    svc = MerchantLinksService(db, actor="cli")

    # Accept the plaid decision.
    svc.set("d_plaid", target_merchant_id="mPlaid")

    # The simplefin decision must still be pending — different source_type,
    # different (source_type, ref_value) pairing, must not be swept.
    simplefin = decisions.fetch_by_id("d_simplefin")
    assert simplefin is not None
    assert simplefin["status"] == "pending", (
        "simplefin decision must not be swept by a plaid sibling-reject"
    )


def test_reject_siblings_sweeps_same_source_type(db: Database) -> None:
    """_reject_pending_siblings sweeps a pending sibling with the same source_type.

    Regression guard: the source_type scoping must not accidentally protect
    same-source siblings — that would break the accept-auto-reject flow.
    """
    decisions = MerchantLinkDecisionsRepo(db)
    decisions.insert(
        decision_id="d_primary_same",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mPrimSame",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "primary"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_sibling_same",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mSibSame",
        confidence_score=0.4,
        match_signals={"signal": "fuzzy_name", "value": "sibling"},
        decided_by="auto",
        actor="system",
    )

    _seed_merchants(db, "mPrimSame")
    svc = MerchantLinksService(db, actor="cli")

    svc.set("d_primary_same", target_merchant_id="mPrimSame")

    sibling = decisions.fetch_by_id("d_sibling_same")
    assert sibling is not None
    assert sibling["status"] == "rejected", (
        "same-source sibling must still be swept (regression guard)"
    )


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------


def test_history_empty(db: Database) -> None:
    """Empty list when no decisions exist."""
    svc = MerchantLinksService(db)
    assert svc.history() == []


def test_history_returns_decisions(db: Database, seeded_pending_decision: None) -> None:
    """history() returns the seeded decision row."""
    svc = MerchantLinksService(db)
    rows = svc.history()
    assert len(rows) == 1
    assert rows[0]["decision_id"] == _DECISION_ID


# ---------------------------------------------------------------------------
# pending / count_pending — composite (source_type, ref_value) key (Finding A)
# ---------------------------------------------------------------------------


def test_pending_groups_by_composite_key(db: Database) -> None:
    """Same ref_value under different source_types produces TWO review groups.

    The grouping key is (source_type, ref_value), not ref_value alone.
    Two providers sharing one opaque ref_value must NOT fold into one group
    (which would show only the first row's source_type and hide the second
    provider's candidate).
    """
    decisions = MerchantLinkDecisionsRepo(db)
    decisions.insert(
        decision_id="d_plaid_comp",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_merchant_id="mPlaidComp",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "plaid"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_sfin_comp",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type="simplefin",
        candidate_merchant_id="mSfinComp",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "sfin"},
        decided_by="auto",
        actor="system",
    )

    svc = MerchantLinksService(db)
    groups = svc.pending()

    assert len(groups) == 2, (
        "same ref_value under different source_types must yield two groups"
    )
    source_types = {g.source_type for g in groups}
    assert source_types == {"plaid", "simplefin"}, (
        "each group must carry its own source_type from the composite key"
    )


def test_count_pending_composite_key(db: Database) -> None:
    """count_pending counts distinct (source_type, ref_value) pairs, not just ref_values.

    Two pending decisions sharing ref_value but with different source_types
    must count as 2, not 1.
    """
    decisions = MerchantLinkDecisionsRepo(db)
    decisions.insert(
        decision_id="d_plaid_cnt",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_merchant_id="mPlaidCnt",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "plaid"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_sfin_cnt",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type="simplefin",
        candidate_merchant_id="mSfinCnt",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "sfin"},
        decided_by="auto",
        actor="system",
    )

    svc = MerchantLinksService(db)
    assert svc.count_pending() == 2, (
        "distinct (source_type, ref_value) pairs: 2 different source_types × same "
        "ref_value must count as 2"
    )


def test_pending_same_source_collapses_candidates(db: Database) -> None:
    """Two candidates under one (source_type, ref_value) collapse into ONE group.

    This is the normal multi-candidate review case: two merchants proposed for
    the same provider entity id from the same source. They must form one group
    with two candidates, not two separate groups.
    """
    decisions = MerchantLinkDecisionsRepo(db)
    decisions.insert(
        decision_id="d_multi_a",
        ref_kind="merchant_entity_id",
        ref_value="ent_multi",
        source_type="plaid",
        candidate_merchant_id="mMultiA",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "a"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_multi_b",
        ref_kind="merchant_entity_id",
        ref_value="ent_multi",
        source_type="plaid",
        candidate_merchant_id="mMultiB",
        confidence_score=0.4,
        match_signals={"signal": "fuzzy_name", "value": "b"},
        decided_by="auto",
        actor="system",
    )

    svc = MerchantLinksService(db)
    groups = svc.pending()

    assert len(groups) == 1, (
        "two candidates under one (source_type, ref_value) must collapse into one group"
    )
    assert len(groups[0].candidates) == 2, (
        "both candidates must appear in the single group"
    )
    assert svc.count_pending() == 1, "one (source_type, ref_value) pair must count as 1"


# ---------------------------------------------------------------------------
# MERCHANT_LINK_OUTCOMES_TOTAL counter (🟡#4)
# ---------------------------------------------------------------------------


def test_accept_increments_merchant_link_outcomes_counter(
    db: Database, seeded_pending_decision: None
) -> None:
    """set() accept path increments MERCHANT_LINK_OUTCOMES_TOTAL{outcome='accepted'} by 1."""
    from moneybin.metrics.registry import (
        MERCHANT_LINK_OUTCOMES_TOTAL,  # type: ignore[attr-defined] — added in 🟡#4
    )

    _seed_merchants(db, _CANDIDATE_MERCHANT_ID)
    svc = MerchantLinksService(db, actor="cli")

    before = MERCHANT_LINK_OUTCOMES_TOTAL.labels(outcome="accepted")._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals

    svc.set(_DECISION_ID, target_merchant_id=_CANDIDATE_MERCHANT_ID)

    after = MERCHANT_LINK_OUTCOMES_TOTAL.labels(outcome="accepted")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1, (
        "MERCHANT_LINK_OUTCOMES_TOTAL{outcome='accepted'} must increment by 1 on accept"
    )


def test_reject_increments_merchant_link_outcomes_counter(
    db: Database, seeded_pending_decision: None
) -> None:
    """set() reject path increments MERCHANT_LINK_OUTCOMES_TOTAL{outcome='rejected'} by 1."""
    from moneybin.metrics.registry import (
        MERCHANT_LINK_OUTCOMES_TOTAL,  # type: ignore[attr-defined] — added in 🟡#4
    )

    svc = MerchantLinksService(db, actor="cli")

    before = MERCHANT_LINK_OUTCOMES_TOTAL.labels(outcome="rejected")._value.get()  # type: ignore[reportPrivateUsage]

    svc.set(_DECISION_ID, target_merchant_id=None)

    after = MERCHANT_LINK_OUTCOMES_TOTAL.labels(outcome="rejected")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1, (
        "MERCHANT_LINK_OUTCOMES_TOTAL{outcome='rejected'} must increment by 1 on reject"
    )
