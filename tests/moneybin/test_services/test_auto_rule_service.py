"""Unit tests for AutoRuleService — proposal lifecycle, override detection, lookups.

Exercises private helpers (``_extract_pattern``) directly to assert internal
invariants — silencing ``reportPrivateUsage`` for this file is deliberate.
"""

# pyright: reportPrivateUsage=false

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.services.auto_rule_service import AutoRuleService
from tests.moneybin.db_helpers import create_core_tables


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


def test_extract_pattern_falls_back_to_normalized_description() -> None:
    """Extract pattern falls back to normalized description when no merchant_id."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),  # no merchant_id on the categorization row
        ("SQ *STARBUCKS #1234 SEATTLE WA",),  # raw description
    ]
    extracted = AutoRuleService(db)._extract_pattern("t_2")
    assert extracted == ("STARBUCKS", "contains")


def test_extract_pattern_returns_none_when_description_empty() -> None:
    """Extract pattern returns None when description is empty."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [(None,), ("",)]
    assert AutoRuleService(db)._extract_pattern("t_3") is None


@pytest.fixture
def real_db(tmp_path: Path) -> Generator[Database, None, None]:
    """A real DB with schema initialized."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    yield db
    db.close()


def _seed_transaction(
    db: Database,
    txn_id: str,
    description: str = "STARBUCKS",
    merchant_id: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES (?, 'a1', DATE '2026-01-01', -5.00, ?, 'csv')",
        [txn_id, description],
    )
    db.execute(
        "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by, merchant_id) "
        "VALUES (?, 'Food & Drink', CURRENT_TIMESTAMP, 'user', ?)",
        [txn_id, merchant_id],
    )


def test_record_creates_proposal_on_first_categorization(real_db: Database) -> None:
    """Creating a proposal on the first categorization stores the expected row."""
    _seed_transaction(real_db, "t1")
    AutoRuleService(real_db).record_categorization(
        "t1", "Food & Drink", subcategory="Coffee"
    )

    rows = real_db.execute(
        "SELECT merchant_pattern, category, subcategory, trigger_count, status FROM app.proposed_rules"
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
        "SELECT trigger_count, sample_txn_ids FROM app.proposed_rules"
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
        "SELECT category, status FROM app.proposed_rules ORDER BY proposed_at"
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

    count_row = real_db.execute("SELECT COUNT(*) FROM app.proposed_rules").fetchone()
    assert count_row is not None and count_row[0] == 0


def test_record_respects_proposal_threshold(
    real_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proposals stay in 'tracking' status until trigger_count reaches the configured threshold."""
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD", "3")
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "3")
    clear_settings_cache()
    set_current_profile("test")

    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    _seed_transaction(real_db, "t3")
    svc = AutoRuleService(real_db)
    svc.record_categorization("t1", "Food & Drink")
    svc.record_categorization("t2", "Food & Drink")
    pending_row = real_db.execute(
        "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()
    assert pending_row is not None and pending_row[0] == 0

    svc.record_categorization("t3", "Food & Drink")
    pending_row = real_db.execute(
        "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()
    assert pending_row is not None and pending_row[0] == 1


def test_approve_promotes_to_active_rule(real_db: Database) -> None:
    """Approving a pending proposal creates an active rule with the correct attributes."""
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink", subcategory="Coffee")
    assert pid is not None

    result = svc.confirm(approve=[pid])
    assert result["approved"] == 1

    rule = real_db.execute(
        "SELECT merchant_pattern, category, subcategory, priority, created_by, is_active "
        "FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule == ("STARBUCKS", "Food & Drink", "Coffee", 200, "auto_rule", True)

    status = real_db.execute(
        "SELECT status, decided_by FROM app.proposed_rules WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert status == ("approved", "user")


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
    result = svc.confirm(approve=[pid])
    assert result["newly_categorized"] == 1

    cat = real_db.execute(
        "SELECT category, categorized_by FROM app.transaction_categories WHERE transaction_id = 't9'"
    ).fetchone()
    assert cat == ("Food & Drink", "auto_rule")


def test_override_threshold_deactivates_rule_and_creates_new_proposal(
    real_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When user overrides reach the threshold, deactivate the rule and propose the new category."""
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "2")
    clear_settings_cache()
    set_current_profile("test")

    # Approve an auto-rule for STARBUCKS -> Food & Drink
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink")
    assert pid is not None
    svc.confirm(approve=[pid])

    # Two user overrides correcting STARBUCKS to Groceries
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

    deactivated = svc.check_overrides()
    assert deactivated == 1

    active = real_db.execute(
        "SELECT is_active FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert active == (False,)

    new_proposal = real_db.execute(
        "SELECT category, status FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()
    assert new_proposal == ("Groceries", "pending")

    # Audit row recorded with the override count and the new converged category.
    audit = real_db.execute(
        "SELECT reason, override_count, new_category FROM app.rule_deactivations"
    ).fetchone()
    assert audit == ("override_threshold", 2, "Groceries")


def test_reject_marks_proposal_rejected_without_creating_rule(
    real_db: Database,
) -> None:
    """Rejecting a proposal marks it rejected without inserting any categorization rule."""
    _seed_transaction(real_db, "t1")
    svc = AutoRuleService(real_db)
    pid = svc.record_categorization("t1", "Food & Drink")
    assert pid is not None
    svc.confirm(reject=[pid])

    status = real_db.execute(
        "SELECT status, decided_by FROM app.proposed_rules WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert status == ("rejected", "user")
    rule_count_row = real_db.execute(
        "SELECT COUNT(*) FROM app.categorization_rules WHERE created_by = 'auto_rule'"
    ).fetchone()
    assert rule_count_row is not None and rule_count_row[0] == 0
