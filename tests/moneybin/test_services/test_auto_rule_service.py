"""Unit tests for AutoRuleService — proposal lifecycle, override detection, lookups.

Exercises private helpers (``_extract_pattern``) directly to assert internal
invariants — silencing ``reportPrivateUsage`` for this file is deliberate.
"""

# pyright: reportPrivateUsage=false

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin import config as config_module
from moneybin import error_codes
from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.mcp.adapters.categorize_adapters import auto_review_envelope
from moneybin.metrics.registry import (
    AUTO_RULE_BROAD_ACCEPT_BLOCKED_TOTAL,
    AUTO_RULE_BROAD_PENDING,
    AUTO_RULE_PATTERN_DOWNGRADED_TOTAL,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization import CategorizationService
from moneybin.tables import PROPOSED_RULES
from tests.moneybin.db_helpers import create_core_tables

pytestmark = pytest.mark.unit


def _mock_db_with_merchant(
    merchant_id: str = "m_abc",
    raw_pattern: str = "STARBUCKS",
    match_type: str = "contains",
) -> MagicMock:
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (merchant_id,),  # SELECT merchant_id FROM transaction_categories
        (raw_pattern, match_type),  # SELECT raw_pattern, match_type FROM merchants
    ]
    return db


def test_extract_pattern_uses_merchant_raw_pattern_when_present() -> None:
    """Extract pattern prefers merchant raw_pattern (matchable substring) when present."""
    db = _mock_db_with_merchant()
    extracted = AutoRuleService(db)._extract_pattern("t_1")
    assert extracted == ("STARBUCKS", "contains")


def test_strict_decision_preflight_runs_inside_write_transaction() -> None:
    db = MagicMock()
    service = AutoRuleService(db)

    def changed_status(_ids: list[str]) -> dict[str, str]:
        assert db.begin.called
        return {"proposal-1": "approved"}

    service.proposal_statuses = changed_status  # type: ignore[method-assign]

    with pytest.raises(Exception) as exc_info:
        service.decide(
            expected_pending_ids=["proposal-1"],
            accept=["proposal-1"],
            reject=[],
            actor="mcp",
        )

    assert getattr(exc_info.value, "code", None) == error_codes.MUTATION_INVALID_INPUT
    db.rollback.assert_called_once_with()
    db.commit.assert_not_called()


def test_strict_decision_rolls_back_late_external_state_change() -> None:
    db = MagicMock()
    service = AutoRuleService(db)
    service.proposal_statuses = MagicMock(  # type: ignore[method-assign]
        side_effect=[
            {"proposal-1": "pending"},
            {"proposal-1": "rejected"},
        ]
    )
    service.approve = MagicMock(  # type: ignore[method-assign]
        return_value=MagicMock(
            approved=0,
            skipped=1,
            newly_categorized=0,
            rule_ids=[],
        )
    )
    service.reject = MagicMock(  # type: ignore[method-assign]
        return_value=MagicMock(rejected=0, skipped=0)
    )

    with pytest.raises(Exception) as exc_info:
        service.decide(
            expected_pending_ids=["proposal-1"],
            accept=["proposal-1"],
            reject=[],
            actor="mcp",
        )

    assert getattr(exc_info.value, "code", None) == (
        error_codes.MUTATION_CONSTRAINT_VIOLATION
    )
    db.rollback.assert_called_once_with()
    db.commit.assert_not_called()


def test_extract_pattern_falls_back_to_normalized_description() -> None:
    """Extract pattern falls back to normalized description when no merchant_id."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),  # no merchant_id on the categorization row
        ("SQ *STARBUCKS #1234 SEATTLE WA", None),  # (raw description, memo)
    ]
    extracted = AutoRuleService(db)._extract_pattern("t_2")
    assert extracted == ("STARBUCKS", "contains")


def test_extract_pattern_returns_none_when_description_empty() -> None:
    """Extract pattern returns None when description and memo are empty."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [(None,), ("", None)]
    assert AutoRuleService(db)._extract_pattern("t_3") is None


def test_extract_pattern_falls_back_to_normalized_memo_when_description_empty() -> None:
    """Extract pattern falls back to normalized memo when description is empty."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),  # no merchant_id on the categorization row
        ("", "ZELLE PAYMENT TO ALICE"),  # (description, memo) — description empty
    ]
    extracted = AutoRuleService(db)._extract_pattern("t_memo_only")
    assert extracted == ("ZELLE PAYMENT TO ALICE", "contains")


def test_extract_pattern_downgrades_short_invented_pattern_to_exact() -> None:
    """A 2-char invented pattern becomes `exact`, not `contains` (F17).

    The live repro: a Zelle/transfer row whose description normalizes to "TO".
    As a `contains` rule it matches COSTCO, STORE, AUTO, TOTAL — accepting it
    would silently relabel the ledger as Internal Transfer.
    """
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),  # no merchant_id on the categorization row
        ("TO", None),  # (raw description, memo)
    ]
    downgraded_before = AUTO_RULE_PATTERN_DOWNGRADED_TOTAL._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals
    extracted = AutoRuleService(db)._extract_pattern("t_to")
    assert extracted == ("TO", "exact")
    assert (
        AUTO_RULE_PATTERN_DOWNGRADED_TOTAL._value.get()  # type: ignore[reportPrivateUsage]
        == downgraded_before + 1
    )


def test_extract_pattern_keeps_contains_for_long_invented_pattern() -> None:
    """A pattern at or above the floor stays `contains` — the guard is targeted."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),
        ("STARBUCKS COFFEE", None),
    ]
    extracted = AutoRuleService(db)._extract_pattern("t_sbux")
    assert extracted == ("STARBUCKS COFFEE", "contains")


def test_extract_pattern_does_not_downgrade_user_authored_merchant_pattern() -> None:
    """A short merchant raw_pattern is user-authored — the guard must not touch it.

    The guard exists to check the machine's inference, not to second-guess an
    explicit human decision.
    """
    db = _mock_db_with_merchant(raw_pattern="BP", match_type="contains")
    extracted = AutoRuleService(db)._extract_pattern("t_bp")
    assert extracted == ("BP", "contains")


@pytest.fixture
def real_db(db: Database) -> Database:
    """A real DB with schema initialized."""
    create_core_tables(db)
    return db


def _seed_transaction(
    db: Database,
    txn_id: str,
    description: str = "STARBUCKS",
    merchant_id: str | None = None,
    source_type: str = "csv",
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES (?, 'a1', DATE '2026-01-01', -5.00, ?, ?)",
        [txn_id, description, source_type],
    )
    db.execute(
        "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by, merchant_id) "
        "VALUES (?, 'Food & Drink', CURRENT_TIMESTAMP, 'user', ?)",
        [txn_id, merchant_id],
    )


def _seed_uncategorized_transaction(
    db: Database,
    txn_id: str,
    description: str,
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES (?, 'a1', DATE '2026-01-02', -6.00, ?, 'csv')",
        [txn_id, description],
    )


def test_strict_decision_rolls_back_all_real_db_state_after_late_failure(
    real_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later proposal failure rolls back the complete strict batch."""
    _seed_transaction(real_db, "trigger-alpha", description="ALPHA SHOP")
    _seed_transaction(real_db, "trigger-beta", description="BETA SHOP")
    _seed_uncategorized_transaction(real_db, "backfill-alpha", description="ALPHA SHOP")
    _seed_uncategorized_transaction(real_db, "backfill-beta", description="BETA SHOP")
    service = AutoRuleService(real_db)
    alpha_id = service.record_categorization("trigger-alpha", "Food & Drink")
    beta_id = service.record_categorization("trigger-beta", "Food & Drink")
    assert alpha_id is not None
    assert beta_id is not None
    audit_count_before = real_db.execute(
        "SELECT COUNT(*) FROM app.audit_log"
    ).fetchone()
    assert audit_count_before is not None

    original_mark_approved = service._proposed.mark_approved
    approval_calls = 0
    saw_partial_state = False

    def fail_second_approval(*args: Any, **kwargs: Any) -> Any:
        nonlocal approval_calls, saw_partial_state
        approval_calls += 1
        if approval_calls == 2:
            approved_count = real_db.execute(
                "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'approved'"
            ).fetchone()
            rule_count = real_db.execute(
                "SELECT COUNT(*) FROM app.categorization_rules "
                "WHERE created_by = 'auto_rule'"
            ).fetchone()
            backfill_count = real_db.execute(
                "SELECT COUNT(*) FROM app.transaction_categories "
                "WHERE transaction_id LIKE 'backfill-%'"
            ).fetchone()
            assert approved_count is not None and approved_count[0] == 1
            assert rule_count is not None and rule_count[0] == 2
            assert backfill_count is not None and backfill_count[0] == 1
            saw_partial_state = True
            raise RuntimeError("injected second proposal failure")
        return original_mark_approved(*args, **kwargs)

    monkeypatch.setattr(service._proposed, "mark_approved", fail_second_approval)

    with pytest.raises(RuntimeError, match="injected second proposal failure"):
        service.decide(
            expected_pending_ids=[alpha_id, beta_id],
            accept=[alpha_id, beta_id],
            reject=[],
            actor="mcp",
        )

    assert approval_calls == 2
    assert saw_partial_state is True
    proposal_rows = real_db.execute(
        """
        SELECT proposed_rule_id, status, rule_id, decided_by
        FROM app.proposed_rules
        WHERE proposed_rule_id IN (?, ?)
        """,
        [alpha_id, beta_id],
    ).fetchall()
    assert {str(row[0]): (row[1], row[2], row[3]) for row in proposal_rows} == {
        alpha_id: ("pending", None, None),
        beta_id: ("pending", None, None),
    }
    rule_count_after = real_db.execute(
        "SELECT COUNT(*) FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    backfill_count_after = real_db.execute(
        "SELECT COUNT(*) FROM app.transaction_categories "
        "WHERE transaction_id LIKE 'backfill-%'"
    ).fetchone()
    audit_count_after = real_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    assert rule_count_after is not None and rule_count_after[0] == 0
    assert backfill_count_after is not None and backfill_count_after[0] == 0
    assert audit_count_after == audit_count_before


def test_record_creates_proposal_on_first_categorization(real_db: Database) -> None:
    """Creating a proposal on the first categorization stores the expected row."""
    _seed_transaction(real_db, "t1")
    AutoRuleService(real_db).record_categorization(
        "t1", "Food & Drink", subcategory="Coffee"
    )

    rows = real_db.execute(
        f"SELECT merchant_pattern, category, subcategory, trigger_count, status FROM {PROPOSED_RULES.full_name}"  # noqa: S608  # building test input string, not executing SQL
    ).fetchall()
    assert rows == [("STARBUCKS", "Food & Drink", "Coffee", 1, "pending")]


def test_record_increments_trigger_count_on_same_pattern_and_category(
    real_db: Database,
) -> None:
    """Repeated categorizations with the same pattern and category increment trigger_count."""
    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    svc = AutoRuleService(real_db)
    svc.record_categorization("t1", "Food & Drink", subcategory="Coffee")
    svc.record_categorization("t2", "Food & Drink", subcategory="Coffee")

    rows = real_db.execute(
        f"SELECT trigger_count, sample_txn_ids FROM {PROPOSED_RULES.full_name}"  # noqa: S608  # building test input string, not executing SQL
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert sorted(rows[0][1]) == ["t1", "t2"]


def test_record_supersedes_when_same_pattern_different_category(
    real_db: Database,
) -> None:
    """Categorizing a same-pattern txn with a different category supersedes the prior proposal."""
    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    svc = AutoRuleService(real_db)
    svc.record_categorization("t1", "Food & Drink")
    svc.record_categorization("t2", "Groceries")

    rows = real_db.execute(
        f"SELECT category, status FROM {PROPOSED_RULES.full_name} ORDER BY proposed_at"  # noqa: S608  # building test input string, not executing SQL
    ).fetchall()
    assert rows == [("Food & Drink", "superseded"), ("Groceries", "pending")]


def test_record_skips_when_active_rule_already_covers_pattern(
    real_db: Database,
) -> None:
    """No proposal is created when an active rule already covers the merchant pattern."""
    _seed_transaction(real_db, "t1")
    real_db.execute(
        "INSERT INTO app.categorization_rules (rule_id, name, merchant_pattern, match_type, category, priority, is_active) "
        "VALUES ('r1', 'starbucks', 'STARBUCKS', 'contains', 'Food & Drink', 100, true)"
    )
    AutoRuleService(real_db).record_categorization("t1", "Food & Drink")

    count_row = real_db.execute(
        f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name}"  # noqa: S608  # building test input string, not executing SQL
    ).fetchone()
    assert count_row is not None and count_row[0] == 0


def test_record_respects_proposal_threshold(
    real_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proposals stay in 'tracking' status until trigger_count reaches the configured threshold."""
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD", "3")
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "3")
    clear_settings_cache()
    # set_current_profile mutates module-level globals that monkeypatch.setenv
    # cannot revert. Snapshot and restore via setattr so tests after this one
    # don't pick up the "test" profile.
    monkeypatch.setattr(config_module, "_current_profile", None)
    monkeypatch.setattr(config_module, "_current_settings", None)
    set_current_profile("test")

    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    _seed_transaction(real_db, "t3")
    svc = AutoRuleService(real_db)
    svc.record_categorization("t1", "Food & Drink")
    svc.record_categorization("t2", "Food & Drink")
    pending_row = real_db.execute(
        f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} WHERE status = 'pending'"  # noqa: S608  # building test input string, not executing SQL
    ).fetchone()
    assert pending_row is not None and pending_row[0] == 0

    svc.record_categorization("t3", "Food & Drink")
    pending_row = real_db.execute(
        f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} WHERE status = 'pending'"  # noqa: S608  # building test input string, not executing SQL
    ).fetchone()
    assert pending_row is not None and pending_row[0] == 1


def test_approve_promotes_to_active_rule(real_db: Database) -> None:
    """Approving a pending proposal creates an active rule with the correct attributes."""
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink", subcategory="Coffee")
    assert pid is not None

    result = svc.accept(accept=[pid], actor="cli")
    assert result.approved == 1

    rule = real_db.execute(
        "SELECT merchant_pattern, category, subcategory, priority, created_by, is_active "
        "FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule == ("STARBUCKS", "Food & Drink", "Coffee", 200, "auto_rule", True)

    status = real_db.execute(
        f"SELECT status, decided_by FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",  # noqa: S608  # building test input string, not executing SQL
        [pid],
    ).fetchone()
    assert status == ("approved", "user")


def test_approve_cascade_threads_parent_audit_id(real_db: Database) -> None:
    """Rule promotion + proposal approval form one audit chain (Req 5).

    The proposal-approve audit threads the rule-insert's audit id as its
    ``parent_audit_id``, so ``AuditService.chain_for`` returns both as one
    user action — the cascade-threading contract this batch exercises.
    """
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink", subcategory="Coffee")
    assert pid is not None

    svc.accept(accept=[pid], actor="cli")

    rule_row = real_db.execute(
        "SELECT rule_id FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule_row is not None
    rule_id = rule_row[0]

    # The rule-insert audit is the cascade parent.
    rule_insert = real_db.execute(
        "SELECT audit_id FROM app.audit_log "  # noqa: S608  # test query, not executing user SQL
        "WHERE action = 'categorization_rule.insert' AND target_id = ?",
        [rule_id],
    ).fetchone()
    assert rule_insert is not None
    parent_id = rule_insert[0]

    # The proposal-approve audit threads the rule-insert's audit id.
    approve_row = real_db.execute(
        "SELECT parent_audit_id FROM app.audit_log "  # noqa: S608  # test query, not executing user SQL
        "WHERE action = 'proposed_rule.approve' AND target_id = ?",
        [pid],
    ).fetchone()
    assert approve_row is not None
    assert approve_row[0] == parent_id

    # chain_for(parent) returns both the rule insert and the proposal approve.
    chain = AuditService(real_db).chain_for(parent_id)
    actions = {e.action for e in chain}
    assert "categorization_rule.insert" in actions
    assert "proposed_rule.approve" in actions


def test_approve_immediately_categorizes_existing_uncategorized(
    real_db: Database,
) -> None:
    """Approving a proposal back-fills matching uncategorized transactions immediately."""
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink")
    assert pid is not None
    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('t9', 'a1', DATE '2026-01-02', -7.00, 'STARBUCKS DOWNTOWN', 'csv')"
    )
    result = svc.accept(accept=[pid])
    assert result.newly_categorized == 1

    cat = real_db.execute(
        "SELECT category, categorized_by FROM app.transaction_categories WHERE transaction_id = 't9'"
    ).fetchone()
    assert cat == ("Food & Drink", "auto_rule")


def test_check_overrides_matches_memo_when_description_is_empty(
    real_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Override detection consumes match_text — patterns can match memo when description is empty."""
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
    clear_settings_cache()
    monkeypatch.setattr(config_module, "_current_profile", None)
    monkeypatch.setattr(config_module, "_current_settings", None)
    set_current_profile("test")

    # Insert an active auto-rule directly so we control the pattern (no
    # _seed_transaction priming needed). created_at is set well in the past so
    # subsequent override categorizations satisfy the c.categorized_at > rule.created_at
    # filter even when both timestamps land in the same second.
    real_db.execute(
        "INSERT INTO app.categorization_rules "
        "(rule_id, name, merchant_pattern, match_type, category, subcategory, "
        " priority, is_active, created_by, created_at, updated_at) "
        "VALUES ('r1', 'Zelle Alice', 'ZELLE PAYMENT TO ALICE', 'contains', "
        " 'Transfers', NULL, 200, true, 'auto_rule', "
        " TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')"
    )
    # Mirror the rule with an approved proposal so the supersede UPDATE has a row to touch.
    real_db.execute(
        "INSERT INTO app.proposed_rules "
        "(proposed_rule_id, merchant_pattern, category, subcategory, "
        " trigger_count, status, sample_txn_ids) "
        "VALUES ('p1', 'ZELLE PAYMENT TO ALICE', 'Transfers', NULL, 1, 'approved', [])"
    )

    # Two override transactions: empty description, memo carries the merchant
    # signal. Categorized 'user' with a different category (Friends & Family).
    for tid in ("t_memo1", "t_memo2"):
        real_db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            " memo, source_type) "
            "VALUES (?, 'a1', DATE '2026-02-01', -25.00, '', "
            " 'ZELLE PAYMENT TO ALICE', 'ofx')",
            [tid],
        )
        real_db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, categorized_at, categorized_by) "
            "VALUES (?, 'Friends & Family', CURRENT_TIMESTAMP, 'user')",
            [tid],
        )

    deactivated = AutoRuleService(real_db).check_overrides()
    assert deactivated == 1

    active = real_db.execute(
        "SELECT is_active FROM app.categorization_rules WHERE rule_id = 'r1'"
    ).fetchone()
    assert active == (False,)


def test_reject_marks_proposal_rejected_without_creating_rule(
    real_db: Database,
) -> None:
    """Rejecting a proposal marks it rejected without inserting any categorization rule."""
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink")
    assert pid is not None
    svc.accept(reject=[pid], actor="cli")

    status = real_db.execute(
        f"SELECT status, decided_by FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",  # noqa: S608  # building test input string, not executing SQL
        [pid],
    ).fetchone()
    assert status == ("rejected", "user")
    rule_count_row = real_db.execute(
        "SELECT COUNT(*) FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule_count_row is not None and rule_count_row[0] == 0


def test_review_caps_at_limit_and_reports_total(real_db: Database) -> None:
    """review() respects limit and surfaces total_count for has_more."""
    svc = AutoRuleService(real_db)
    for i in range(5):
        _seed_transaction(real_db, f"t{i}", description=f"MERCHANT{i}")
        svc.record_categorization(f"t{i}", "Food & Drink")

    result = svc.review(limit=2)
    assert len(result.proposals) == 2
    assert result.total_count == 5
    envelope = auto_review_envelope(result)
    assert envelope.summary.has_more is True
    assert envelope.summary.total_count == 5


def test_review_uses_configured_default_when_limit_omitted(real_db: Database) -> None:
    """review() with no limit uses categorization.auto_rule_list_default_limit."""
    svc = AutoRuleService(real_db)
    for i in range(3):
        _seed_transaction(real_db, f"t{i}", description=f"M{i}")
        svc.record_categorization(f"t{i}", "Food & Drink")

    # Default limit (100) is well above 3 — no truncation.
    result = svc.review()
    assert len(result.proposals) == 3
    assert result.total_count == 3
    assert auto_review_envelope(result).summary.has_more is False


def test_review_broad_gauge_is_queue_wide_not_page_scoped(real_db: Database) -> None:
    """AUTO_RULE_BROAD_PENDING must reflect the whole pending queue, not just the page.

    Two pending proposals. "A1MERCHANT" has trigger_count=2 (two categorized
    txns) so it sorts first (``trigger_count DESC``) and is the only one on a
    ``limit=1`` page; its estimated match count (2) stays under the broad
    floor. "BROADCO" has trigger_count=1 but a true blast radius of 25 rows —
    comfortably past the floor(20)/10x-evidence ratio — so it IS broad, yet it
    is excluded from the returned page. A page-scoped gauge (the pre-fix
    behavior) would see only A1MERCHANT and report 0 broad proposals; the
    gauge must report 1.
    """
    svc = AutoRuleService(real_db)
    _seed_transaction(real_db, "t_a1a", description="A1MERCHANT")
    _seed_transaction(real_db, "t_a1b", description="A1MERCHANT")
    svc.record_categorization("t_a1a", "Food & Drink")
    svc.record_categorization("t_a1b", "Food & Drink")

    _seed_transaction(real_db, "t_broad", description="BROADCO")
    svc.record_categorization("t_broad", "Food & Drink")
    # 24 more BROADCO rows (uncategorized) so the pattern's true blast radius
    # is 25 — the scan behind _estimate_match_counts only reads description/
    # memo, not categorization state.
    for i in range(24):
        real_db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-01', -5.00, 'BROADCO', 'csv')",
            [f"t_broad_extra_{i}"],
        )

    result = svc.review(limit=1)
    assert len(result.proposals) == 1
    assert result.proposals[0]["merchant_pattern"] == "A1MERCHANT"
    assert result.total_count == 2
    assert AUTO_RULE_BROAD_PENDING._value.get() == 1  # type: ignore[reportPrivateUsage] — prometheus internals


@pytest.mark.parametrize(
    "cls_name",
    ["AutoReviewResult", "AutoConfirmResult", "AutoStatsResult"],
)
def test_auto_rule_results_are_pure_data_carriers(cls_name: str) -> None:
    """Service result dataclasses must not depend on transport-layer types."""
    import moneybin.services.auto_rule_service as service_module

    cls = getattr(service_module, cls_name)
    assert not hasattr(cls, "to_envelope"), (
        f"{cls_name}.to_envelope must live in mcp/adapters/, not on the service dataclass"
    )


# --- Manual-source exemption (transaction-curation spec Req 7) ---------------


def test_manual_user_category_does_not_train_auto_rules(real_db: Database) -> None:
    """User categorizations on manual rows must not seed auto-rule proposals."""
    _seed_transaction(real_db, "t1", source_type="manual")
    pid = AutoRuleService(real_db).record_categorization(
        "t1", "Food & Drink", subcategory="Coffee"
    )
    assert pid is None
    count_row = real_db.execute(
        f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name}"  # noqa: S608  # building test input string, not executing SQL
    ).fetchone()
    assert count_row is not None and count_row[0] == 0


def test_imported_user_category_still_trains_auto_rules(real_db: Database) -> None:
    """Negative-control: same setup with an imported (csv) row still proposes."""
    _seed_transaction(real_db, "t1", source_type="csv")
    pid = AutoRuleService(real_db).record_categorization(
        "t1", "Food & Drink", subcategory="Coffee"
    )
    assert pid is not None
    rows = real_db.execute(
        f"SELECT merchant_pattern, category, subcategory FROM {PROPOSED_RULES.full_name}"  # noqa: S608  # building test input string, not executing SQL
    ).fetchall()
    assert rows == [("STARBUCKS", "Food & Drink", "Coffee")]


def test_manual_user_category_does_not_count_as_override(
    real_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Manual-row corrections do not count toward override threshold.

    User corrections on manual rows must not deactivate auto-rules.
    """
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
    clear_settings_cache()
    monkeypatch.setattr(config_module, "_current_profile", None)
    monkeypatch.setattr(config_module, "_current_settings", None)
    set_current_profile("test")

    # Approve an auto-rule for STARBUCKS -> Food & Drink (from an imported row).
    _seed_transaction(real_db, "t1", source_type="csv")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink")
    assert pid is not None
    svc.accept(accept=[pid])

    # Two override corrections — but on MANUAL rows. These should be ignored.
    for tid in ("m1", "m2"):
        real_db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-03', -8.00, 'STARBUCKS RESERVE', 'manual')",
            [tid],
        )
        real_db.execute(
            "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by) "
            "VALUES (?, 'Groceries', CURRENT_TIMESTAMP, 'user')",
            [tid],
        )

    deactivated = svc.check_overrides()
    assert deactivated == 0
    active_row = real_db.execute(
        "SELECT is_active FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert active_row == (True,)


class TestPromoteProposedRuleDualWrite:
    """Phase 1 dual-write: approving a proposal populates category_id on the rule."""

    def test_promoted_rule_carries_category_id(self, real_db: Database) -> None:
        cat_id = CategorizationService(real_db).create_category("PromoteMe")
        _seed_transaction(real_db, "t_promote")
        svc = AutoRuleService(real_db)
        pid = svc.record_categorization("t_promote", "PromoteMe")
        assert pid is not None

        result = svc.accept(accept=[pid])
        assert result.approved == 1
        rule_id = result.rule_ids[0]

        row = real_db.execute(
            "SELECT category, category_id FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [rule_id],
        ).fetchone()
        assert row == ("PromoteMe", cat_id)


class TestProposedRulesDualWrite:
    """Phase 1 dual-write: proposed_rules writers populate category_id."""

    def test_proposed_rule_carries_category_id(self, real_db: Database) -> None:
        """Initial detection via record_categorization stores the resolved FK."""
        cat_id = CategorizationService(real_db).create_category("Food & Drink")
        _seed_transaction(real_db, "t1")
        svc = AutoRuleService(real_db)
        pid = svc.record_categorization("t1", "Food & Drink")
        assert pid is not None

        row = real_db.execute(
            f"SELECT category, category_id FROM {PROPOSED_RULES.full_name} "  # noqa: S608  # TableRef constant
            "WHERE proposed_rule_id = ?",
            [pid],
        ).fetchone()
        assert row == ("Food & Drink", cat_id)

    def test_deactivation_does_not_create_re_proposal(
        self, real_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Override-threshold deactivation no longer creates a re-proposal row."""
        monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
        clear_settings_cache()
        monkeypatch.setattr(config_module, "_current_profile", None)
        monkeypatch.setattr(config_module, "_current_settings", None)
        set_current_profile("test")

        _seed_transaction(real_db, "t1")
        svc = AutoRuleService(real_db)
        pid = svc.record_categorization("t1", "Food & Drink")
        assert pid is not None
        svc.accept(accept=[pid])

        for tid in ("t10", "t11"):
            real_db.execute(
                "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
                "VALUES (?, 'a1', DATE '2026-01-03', -8.00, 'STARBUCKS RESERVE', 'csv')",
                [tid],
            )
            real_db.execute(
                "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by) "
                "VALUES (?, 'Groceries', CURRENT_TIMESTAMP, 'user')",
                [tid],
            )

        assert svc.check_overrides() == 1

        # No new proposal for Groceries — re-proposal logic was removed.
        row = real_db.execute(
            f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} "  # noqa: S608  # TableRef constant
            "WHERE source = 'pattern_detection' AND status IN ('pending', 'tracking')"
        ).fetchone()
        assert row is not None and row[0] == 0


class TestSupersessionByRuleId:
    """Proposal->rule linkage via rule_id FK; deactivation behavior on proposals."""

    def test_approve_writes_rule_id_to_proposal(self, real_db: Database) -> None:
        """approve() persists the minted rule_id back to its source proposal."""
        _seed_transaction(real_db, "t1")
        svc = AutoRuleService(real_db)
        pid = svc.record_categorization("t1", "Food & Drink")
        assert pid is not None

        result = svc.accept(accept=[pid])
        rule_id = result.rule_ids[0]

        row = real_db.execute(
            f"SELECT rule_id FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",  # noqa: S608  # TableRef constant
            [pid],
        ).fetchone()
        assert row == (rule_id,)

    def test_deactivation_does_not_flip_proposal_status(
        self, real_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deactivation no longer supersedes the linked proposal — proposals retain their status.

        Re-proposal / supersession was removed with the Phase 3 logic.
        Proposals stay in their current state; only the rule is deactivated.
        """
        monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
        clear_settings_cache()
        monkeypatch.setattr(config_module, "_current_profile", None)
        monkeypatch.setattr(config_module, "_current_settings", None)
        set_current_profile("test")

        _seed_transaction(real_db, "t1")
        svc = AutoRuleService(real_db)
        linked_pid = svc.record_categorization("t1", "Food & Drink")
        assert linked_pid is not None
        svc.accept(accept=[linked_pid])

        # Two user overrides correcting STARBUCKS to Groceries.
        for tid in ("t10", "t11"):
            real_db.execute(
                "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
                "VALUES (?, 'a1', DATE '2026-01-03', -8.00, 'STARBUCKS RESERVE', 'csv')",
                [tid],
            )
            real_db.execute(
                "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by) "
                "VALUES (?, 'Groceries', CURRENT_TIMESTAMP, 'user')",
                [tid],
            )

        assert svc.check_overrides() == 1

        # The linked proposal stays 'approved' — no supersede step anymore.
        status = real_db.execute(
            f"SELECT status FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",  # noqa: S608  # TableRef constant
            [linked_pid],
        ).fetchone()
        assert status == ("approved",)


class TestDeactivateOverriddenRules:
    """Threshold deactivation: keeps safety property, drops re-proposal logic."""

    def test_threshold_deactivates_rule_and_emits_audit_event(
        self, real_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Threshold path: deactivate rule, emit audit_log event with full context, do not re-propose."""
        monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
        clear_settings_cache()
        monkeypatch.setattr(config_module, "_current_profile", None)
        monkeypatch.setattr(config_module, "_current_settings", None)
        set_current_profile("test")

        # Approve an auto-rule for STARBUCKS -> Food & Drink.
        _seed_transaction(real_db, "t1")
        svc = AutoRuleService(real_db)
        pid = svc.record_categorization("t1", "Food & Drink")
        assert pid is not None
        svc.accept(accept=[pid])

        # Two user overrides correcting STARBUCKS to Groceries — meets threshold.
        for tid in ("t10", "t11"):
            real_db.execute(
                "INSERT INTO core.fct_transactions "
                "(transaction_id, account_id, transaction_date, amount, description, source_type) "
                "VALUES (?, 'a1', DATE '2026-01-03', -8.00, 'STARBUCKS RESERVE', 'csv')",
                [tid],
            )
            real_db.execute(
                "INSERT INTO app.transaction_categories "
                "(transaction_id, category, categorized_at, categorized_by) "
                "VALUES (?, 'Groceries', CURRENT_TIMESTAMP, 'user')",
                [tid],
            )

        deactivated = svc.check_overrides()
        assert deactivated == 1

        # Rule must be deactivated.
        row = real_db.execute(
            "SELECT is_active FROM app.categorization_rules WHERE created_by = 'auto_rule'"
        ).fetchone()
        assert row == (False,)

        # No new proposal created.
        proposal_count = real_db.execute(
            "SELECT COUNT(*) FROM app.proposed_rules WHERE source = 'pattern_detection' "
            "AND status IN ('pending', 'tracking')"
        ).fetchone()
        assert proposal_count is not None and proposal_count[0] == 0

        # Audit event emitted with full payload — exactly one row, all fields.
        # The override path now routes through CategorizationRulesRepo.deactivate,
        # so it shares the taxonomy-conformant action with manual deletes; the
        # override-vs-manual distinction lives in context.reason.
        audit_count = real_db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE action = 'categorization_rule.deactivate'"
        ).fetchone()
        assert audit_count is not None and audit_count[0] == 1

        audit_row = real_db.execute(
            "SELECT action, actor, target_schema, target_table, target_id, "
            "before_value, after_value, context_json "
            "FROM app.audit_log WHERE action = 'categorization_rule.deactivate'"
        ).fetchone()
        assert audit_row is not None
        (
            action,
            actor,
            target_schema,
            target_table,
            target_id,
            before_raw,
            after_raw,
            context_raw,
        ) = audit_row
        assert action == "categorization_rule.deactivate"
        assert actor == "auto_rule_service"
        assert target_schema == "app"
        assert target_table == "categorization_rules"
        # Verify target_id is the rule_id of the deactivated rule, not just
        # any non-null value — guards against a bug that stores the wrong
        # entity ID in the audit row.
        rule_id_row = real_db.execute(
            "SELECT rule_id FROM app.categorization_rules WHERE created_by = 'auto_rule'"
        ).fetchone()
        assert rule_id_row is not None
        assert target_id == rule_id_row[0]
        # Full before/after row capture (Req 4), not a {is_active} subset.
        before = json.loads(before_raw)
        after = json.loads(after_raw)
        assert before["is_active"] is True
        assert after["is_active"] is False
        assert "merchant_pattern" in before  # full row, not a column subset
        # Override forensics live in context; `reason` keeps the override path
        # distinguishable from a manual deactivation under the shared action.
        context = json.loads(context_raw)
        assert context["reason"] == "override_threshold"
        assert context["override_count"] == 2
        assert len(context["sample_ids"]) == 2


# --- Blast radius (F17 Layer 2) ----------------------------------------------


def test_review_surfaces_blast_radius_and_flags_broad() -> None:
    """review() reports how many transactions a proposal would actually hit (F17).

    "TO" as an exact-match proposal against a ledger where 40 rows are literally
    "TO" is broad: 40 matches on 1 trigger, far past 10x evidence.
    """
    db = MagicMock()
    service = AutoRuleService(db)
    proposals = [
        {
            "proposed_rule_id": "p_broad",
            "merchant_pattern": "TO",
            "match_type": "contains",
            "category": "Transfer",
            "subcategory": "Internal Transfer",
            "trigger_count": 1,
            "sample_txn_ids": ["t_1"],
        }
    ]
    # 40 transactions whose descriptions all contain "TO" (the "TO" in "AUTO").
    # NOTE: "COSTCO WHOLESALE" alone does NOT contain "TO" as a substring
    # ("COSTCO"'s T is followed by C, not O) despite the brief's docstring
    # listing it alongside STORE/AUTO/TOTAL — verified with a literal `in`
    # check. Using "COSTCO AUTO CENTER" keeps the COSTCO flavor while
    # actually exercising the `contains` blast-radius path this test names.
    rows = [("COSTCO AUTO CENTER", None)] * 40
    db.execute.return_value.fetchall.return_value = rows

    counts = service._estimate_match_counts(proposals)
    assert counts["p_broad"] == 40
    assert service._is_broad(40, 1) is True


def test_is_broad_respects_the_floor_and_the_evidence_ratio() -> None:
    """The guard flags disproportionate blast radius, not merely large rules."""
    service = AutoRuleService(MagicMock())
    # Below the 20-match floor: never broad, however thin the evidence.
    assert service._is_broad(8, 1) is False
    # Past the floor and >10x the evidence: broad.
    assert service._is_broad(50, 1) is True
    # Same 50 matches, but 5 triggers of evidence: 50 <= 10*5, so not broad.
    assert service._is_broad(50, 5) is False


def test_estimate_match_counts_uses_exact_semantics_for_exact_patterns() -> None:
    """An `exact` proposal only counts rows whose normalized text IS the pattern.

    This is what makes the Task-2 downgrade safe: "TO" as `exact` has a blast
    radius of 0 against a ledger of COSTCO rows, where as `contains` it had 40.
    """
    db = MagicMock()
    service = AutoRuleService(db)
    proposals = [
        {
            "proposed_rule_id": "p_exact",
            "merchant_pattern": "TO",
            "match_type": "exact",
            "category": "Transfer",
            "subcategory": None,
            "trigger_count": 1,
            "sample_txn_ids": ["t_1"],
        }
    ]
    db.execute.return_value.fetchall.return_value = [("COSTCO AUTO CENTER", None)] * 40
    counts = service._estimate_match_counts(proposals)
    assert counts["p_exact"] == 0


def test_estimate_match_counts_tests_regex_against_normalized_description() -> None:
    r"""An end-anchored ``regex`` proposal must hit what the live matcher would hit.

    ``^STARBUCKS$`` fails against the concatenated ``match_text``
    ("STARBUCKS\nCOFFEE PURCHASE" — the trailing ``$`` anchor can't match
    mid-string once memo is appended) but DOES match the individual normalized
    description ("STARBUCKS"). ``matcher._match_text`` tests exactly this
    per-field candidate for every match type, including ``regex`` — so the
    estimator must count this row too, or an end-anchored regex proposal can
    slip past the reviewer under-counted (and a genuinely broad rule reads as
    not-broad).
    """
    db = MagicMock()
    service = AutoRuleService(db)
    proposals = [
        {
            "proposed_rule_id": "p_regex_anchor",
            "merchant_pattern": r"^STARBUCKS$",
            "match_type": "regex",
            "category": "Food & Drink",
            "subcategory": "Coffee",
            "trigger_count": 1,
            "sample_txn_ids": ["t_1"],
        }
    ]
    db.execute.return_value.fetchall.return_value = [
        ("STARBUCKS 12345", "COFFEE PURCHASE"),
    ]
    counts = service._estimate_match_counts(proposals)
    assert counts["p_regex_anchor"] == 1


# --- Blast radius accept guard (F17 Layer 3) ---------------------------------


def test_approve_refuses_broad_proposal_without_allow_broad(real_db: Database) -> None:
    """Accept-all cannot sweep in a broad proposal (F17, the corruption path).

    This is the test that closes the finding: the live session's "TO" rule was
    one --approve-all away from relabeling every COSTCO/STORE/AUTO row as an
    Internal Transfer, which also drops them out of spend reports.

    The pattern is deliberately LONG ENOUGH to clear the specificity floor
    ("COSTCO" is 6 chars, well over ``auto_rule_min_contains_length``). A short
    pattern like "TO" is refused by the floor before the blast radius is ever
    computed, so using one here would prove nothing about the broad guard —
    the test would pass with the broad check deleted. Isolating the guards is
    the point: this test owns the blast-radius refusal, and
    ``test_approve_refuses_legacy_short_contains_proposal_without_allow_broad``
    owns the specificity refusal.
    """
    service = AutoRuleService(real_db)

    # 40 transactions a `contains "COSTCO"` rule would hit — past the broad
    # floor (20) and >10x the single trigger behind the proposal.
    for i in range(40):
        real_db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-01', -10.0, 'COSTCO AUTO CENTER', 'csv')",
            [f"t_{i}"],
        )
    pid = service._proposed.insert(
        merchant_pattern="COSTCO",
        match_type="contains",
        category="Transfer",
        subcategory="Internal Transfer",
        category_id=None,
        status="pending",
        sample_txn_ids=["t_0"],
        actor="test",
    ).target_id
    assert pid is not None

    blocked_before = AUTO_RULE_BROAD_ACCEPT_BLOCKED_TOTAL._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals

    # The agent's accept-all path: pass every pending id.
    blocked = service.accept(accept=[pid], reject=[], actor="test")
    assert blocked.approved == 0
    assert blocked.skipped == 1
    assert blocked.rule_ids == []
    assert (
        AUTO_RULE_BROAD_ACCEPT_BLOCKED_TOTAL._value.get()  # type: ignore[reportPrivateUsage]
        == blocked_before + 1
    )

    # The human's informed override, after seeing estimated_match_count.
    allowed = service.accept(accept=[pid], reject=[], actor="test", allow_broad=True)
    assert allowed.approved == 1


def test_approve_refuses_legacy_short_contains_proposal_without_allow_broad(
    real_db: Database,
) -> None:
    """The specificity floor must also gate promotion, not just proposal-time (F17).

    ``_invented_match_type`` only downgrades a short pattern to ``exact`` when a
    NEW proposal is created — it cannot retroactively fix a proposal already
    sitting in ``app.proposed_rules`` with ``match_type='contains'`` from before
    this guard shipped. This proposal's blast radius (5 rows) is deliberately
    kept well under ``auto_rule_broad_match_min`` (20), so ``_is_broad`` alone
    would wave it through unflagged — isolating the specificity check as the
    thing that must catch it.
    """
    service = AutoRuleService(real_db)

    # 5 transactions a `contains "TO"` rule would hit — comfortably under the
    # broad floor (20), so the blast-radius check alone would not block this.
    for i in range(5):
        real_db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-01', -10.0, 'COSTCO AUTO CENTER', 'csv')",
            [f"t_{i}"],
        )
    pid = service._proposed.insert(
        merchant_pattern="TO",
        match_type="contains",
        category="Transfer",
        subcategory="Internal Transfer",
        category_id=None,
        status="pending",
        sample_txn_ids=["t_0"],
        actor="test",
    ).target_id
    assert pid is not None

    result = service.approve([pid], actor="test")
    assert result.approved == 0
    assert result.skipped == 1
    assert result.rule_ids == []
    rule_count_row = real_db.execute(
        "SELECT COUNT(*) FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule_count_row is not None and rule_count_row[0] == 0

    # The human's informed override, after seeing the pattern is too short.
    allowed = service.approve([pid], actor="test", allow_broad=True)
    assert allowed.approved == 1


def test_approve_skips_the_blast_radius_scan_when_every_id_is_short(
    real_db: Database,
) -> None:
    """A short-pattern refusal must not pay for a blast-radius scan it can't use.

    ``_estimate_match_counts`` scans all of ``core.fct_transactions`` — the full
    merge/dedup view whose needless re-evaluation is what hung ``system_doctor``
    for >73s. An id already refused by the length check is skipped regardless of
    its blast radius, so estimating it buys nothing. When the whole accept set is
    short-pattern proposals, the scan must not happen at all.
    """
    service = AutoRuleService(real_db)
    pid = service._proposed.insert(
        merchant_pattern="TO",
        match_type="contains",
        category="Transfer",
        subcategory="Internal Transfer",
        category_id=None,
        status="pending",
        sample_txn_ids=["t_0"],
        actor="test",
    ).target_id
    assert pid is not None

    scanned: list[list[dict[str, object]]] = []
    original = service._estimate_match_counts

    def _spy(proposals: list[dict[str, object]]) -> dict[str, int]:
        scanned.append(proposals)
        return original(proposals)  # type: ignore[arg-type]

    service._estimate_match_counts = _spy  # type: ignore[method-assign]
    result = service.approve([pid], actor="test")

    assert result.skipped == 1
    assert result.approved == 0
    # The estimator may be called, but it must be handed nothing to scan —
    # _estimate_match_counts short-circuits on an empty list without touching
    # core.fct_transactions.
    assert all(p == [] for p in scanned), (
        f"blast-radius scan was handed {scanned} for an all-short accept set"
    )


def test_estimated_match_count_agrees_with_what_approval_categorizes(
    real_db: Database,
) -> None:
    """For this fixture, the estimate exactly matches what approval applies (F17).

    The invariant ``_estimate_match_counts`` must uphold is ``actual <=
    estimated``, NOT equality: the estimator counts every transaction the
    pattern matches, while approval's ``_categorize_existing_with_rule``
    writes only the uncategorized, priority-winning subset of those. The
    estimate is an upper bound on blast radius by design — over-counting is
    the fail-safe direction, since it's what a human reviewer must see
    before accepting a proposal.

    This fixture happens to produce exact equality only because every row
    is uncategorized, so the ``==`` assertion below is valid for THIS test.
    Do NOT read it as license to narrow the estimator to uncategorized-only
    rows to make some other fixture's numbers "line up" — that would make it
    UNDER-count: a ``contains`` pattern matching 500 already-categorized rows
    would then estimate 0, sail through the ``is_broad`` gate unflagged, and
    silently relabel all 500 on the rule's next backfill. Under-counting is
    the one direction this guard must never move in; over-counting is safe.
    A failure here still means ``_pattern_hits`` (the estimator) has
    diverged from ``CategorizationService.match_first_rule`` (the matcher) —
    investigate that divergence, don't relax the assertion to match it.
    """
    service = AutoRuleService(real_db)

    for i in range(25):
        real_db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-01', -20.0, 'AMZN MKTP US*1A2B3C', 'csv')",
            [f"t_{i}"],
        )
    for i in range(5):
        real_db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, 'a1', DATE '2026-01-01', -30.0, 'WHOLE FOODS MARKET', 'csv')",
            [f"o_{i}"],
        )

    pid = service._proposed.insert(
        merchant_pattern="AMZN",
        match_type="contains",
        category="Shopping",
        subcategory=None,
        category_id=None,
        status="pending",
        sample_txn_ids=["t_0"],
        actor="test",
    ).target_id
    assert pid is not None

    estimated = service._estimate_match_counts(service.list_pending_proposals())[pid]
    # allow_broad=True: 25 matches on 1 trigger clears the broad floor/ratio
    # (20 floor, 10x factor) — this is the human's informed override after
    # seeing estimated_match_count, not a bypass of the guard under test.
    approved = service.approve([pid], actor="test", allow_broad=True)

    assert estimated == 25
    assert approved.newly_categorized == estimated
