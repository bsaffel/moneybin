"""Doctor invariants for app.* integrity (Invariant 10).

Covers the reusable audit-coverage check and the user_categories text-uniqueness
check added with the repository layer. Uses the function-scoped ``db`` fixture
(real encrypted DuckDB with the app schema initialized).
"""

# This module drives the doctor's per-table integrity helpers
# (_run_app_audit_coverage, _run_user_categories_uniqueness) directly —
# protected-member access is intentional.
# pyright: reportPrivateUsage=false
from __future__ import annotations

from moneybin.database import Database
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
from moneybin.repositories.proposed_rules_repo import ProposedRulesRepo
from moneybin.repositories.transaction_categories_repo import (
    TransactionCategoriesRepo,
)
from moneybin.repositories.user_categories_repo import UserCategoriesRepo
from moneybin.repositories.user_merchants_repo import UserMerchantsRepo
from moneybin.services.doctor_service import DoctorService
from moneybin.tables import (
    CATEGORIZATION_RULES,
    PROPOSED_RULES,
    TRANSACTION_CATEGORIES,
    USER_CATEGORIES,
    USER_MERCHANTS,
)
from tests.moneybin.db_helpers import create_core_tables


def _insert_rule(repo: CategorizationRulesRepo) -> str:
    event = repo.insert(
        name="r",
        merchant_pattern="P",
        match_type="contains",
        min_amount=None,
        max_amount=None,
        account_id=None,
        category="Dining",
        subcategory=None,
        category_id=None,
        priority=100,
        created_by="user",
        actor="cli",
    )
    assert event.target_id is not None
    return event.target_id


def _insert_proposal(repo: ProposedRulesRepo, *, status: str = "tracking") -> str:
    event = repo.insert(
        merchant_pattern="P",
        match_type="contains",
        category="Dining",
        subcategory=None,
        category_id=None,
        status=status,
        sample_txn_ids=["t1"],
        actor="system",
    )
    assert event.target_id is not None
    return event.target_id


def _insert_merchant(repo: UserMerchantsRepo, *, name: str) -> str:
    event = repo.insert(
        raw_pattern=None,
        match_type="oneOf",
        canonical_name=name,
        category=None,
        subcategory=None,
        category_id=None,
        created_by="ai",
        exemplars=[],
        actor="system",
    )
    assert event.target_id is not None
    return event.target_id


def _bypass_insert(
    db: Database,
    *,
    category_id: str,
    category: str,
    subcategory: str | None = None,
    days_ago: int = 0,
) -> None:
    """Insert a user_categories row WITHOUT an audit row (simulated bypass)."""
    db.execute(
        "INSERT INTO app.user_categories "  # noqa: S608  # test input, not executing user SQL
        "(category_id, category, subcategory, is_active, created_at, updated_at) "
        "VALUES (?, ?, ?, true, now()::TIMESTAMP - (? * INTERVAL 1 DAY), "
        "now()::TIMESTAMP - (? * INTERVAL 1 DAY))",
        [category_id, category, subcategory, days_ago, days_ago],
    )


def test_audit_coverage_flags_bypass_row(db: Database) -> None:
    _bypass_insert(db, category_id="bypass1", category="Sneaky")
    result = DoctorService(db)._run_app_audit_coverage(USER_CATEGORIES, "category_id")
    assert result.status == "fail"
    assert "bypass1" in result.affected_ids


def test_audit_coverage_passes_for_repo_mutated_row(db: Database) -> None:
    UserCategoriesRepo(db).insert(category="Proper", actor="user")
    result = DoctorService(db)._run_app_audit_coverage(USER_CATEGORIES, "category_id")
    assert result.status == "pass"
    assert result.affected_ids == []


def test_audit_coverage_ignores_rows_outside_lookback(db: Database) -> None:
    # A bypass row last touched 30 days ago is outside the default 7-day window.
    _bypass_insert(db, category_id="old1", category="Ancient", days_ago=30)
    result = DoctorService(db)._run_app_audit_coverage(USER_CATEGORIES, "category_id")
    assert result.status == "pass"


def test_audit_coverage_full_scans_all(db: Database) -> None:
    # full=True bypasses the lookback window so even old bypass rows are caught.
    _bypass_insert(db, category_id="old2", category="Ancient", days_ago=30)
    result = DoctorService(db)._run_app_audit_coverage(
        USER_CATEGORIES, "category_id", full=True
    )
    assert result.status == "fail"
    assert "old2" in result.affected_ids


def test_audit_coverage_flags_audited_then_bypassed(db: Database) -> None:
    # A row audited at insert, then mutated by a raw bypass that advances
    # updated_at past the audit, must still be flagged — "some audit exists"
    # is not enough; the audit must cover the latest mutation.
    cid = UserCategoriesRepo(db).insert(category="WasAudited", actor="user").target_id
    assert cid is not None
    db.execute(
        "UPDATE app.user_categories "  # noqa: S608  # test input, not executing user SQL
        "SET updated_at = now()::TIMESTAMP + INTERVAL 1 DAY WHERE category_id = ?",
        [cid],
    )
    result = DoctorService(db)._run_app_audit_coverage(USER_CATEGORIES, "category_id")
    assert result.status == "fail"
    assert cid in result.affected_ids


def test_user_categories_uniqueness_flags_duplicate(db: Database) -> None:
    _bypass_insert(db, category_id="dup1", category="Dining", subcategory="Coffee")
    _bypass_insert(db, category_id="dup2", category="Dining", subcategory="Coffee")
    result = DoctorService(db)._run_user_categories_uniqueness()
    assert result.status == "fail"
    assert "Dining" in (result.detail or "")


def test_user_categories_uniqueness_passes_when_distinct(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    repo.insert(category="Dining", subcategory="Coffee", actor="user")
    repo.insert(category="Dining", subcategory="Lunch", actor="user")
    result = DoctorService(db)._run_user_categories_uniqueness()
    assert result.status == "pass"


# ---------------------------------------------------------------------------
# user_merchants: audit coverage + orphan warning
# ---------------------------------------------------------------------------


def test_audit_coverage_passes_for_repo_mutated_merchant(db: Database) -> None:
    _insert_merchant(UserMerchantsRepo(db), name="Amazon")
    result = DoctorService(db)._run_app_audit_coverage(USER_MERCHANTS, "merchant_id")
    assert result.status == "pass"


def test_audit_coverage_flags_bypass_merchant(db: Database) -> None:
    db.execute(
        "INSERT INTO app.user_merchants "  # noqa: S608  # test input, not executing user SQL
        "(merchant_id, match_type, canonical_name, created_by, updated_at) "
        "VALUES ('bypassM', 'oneOf', 'Sneaky', 'ai', now()::TIMESTAMP)"
    )
    result = DoctorService(db)._run_app_audit_coverage(USER_MERCHANTS, "merchant_id")
    assert result.status == "fail"
    assert "bypassM" in result.affected_ids


def test_user_merchants_orphan_warns_for_unreferenced_merchant(db: Database) -> None:
    # A merchant no categorization references, last touched outside the lookback
    # window, warns (never fails — deletion-by-design leaves merchants behind).
    mid = _insert_merchant(UserMerchantsRepo(db), name="Stale Co")
    db.execute(
        "UPDATE app.user_merchants "  # noqa: S608  # test input, not executing user SQL
        "SET updated_at = now()::TIMESTAMP - INTERVAL 30 DAY WHERE merchant_id = ?",
        [mid],
    )
    result = DoctorService(db)._run_user_merchants_orphans()
    assert result.status == "warn"
    assert mid in result.affected_ids


def test_user_merchants_orphan_passes_for_recent_merchant(db: Database) -> None:
    # A freshly-created merchant (within the lookback window) is not flagged,
    # even with no referencing categorization yet.
    _insert_merchant(UserMerchantsRepo(db), name="Fresh Co")
    result = DoctorService(db)._run_user_merchants_orphans()
    assert result.status == "pass"


# ---------------------------------------------------------------------------
# categorization_rules + proposed_rules: coverage (proposed_rules uses
# proposed_at as the watermark — no updated_at column) + proposal->rule FK
# ---------------------------------------------------------------------------


def test_audit_coverage_passes_for_repo_mutated_rule(db: Database) -> None:
    _insert_rule(CategorizationRulesRepo(db))
    result = DoctorService(db)._run_app_audit_coverage(CATEGORIZATION_RULES, "rule_id")
    assert result.status == "pass"


def test_audit_coverage_passes_for_repo_mutated_proposal(db: Database) -> None:
    _insert_proposal(ProposedRulesRepo(db))
    result = DoctorService(db)._run_app_audit_coverage(
        PROPOSED_RULES, "proposed_rule_id", updated_col="proposed_at"
    )
    assert result.status == "pass"


def test_audit_coverage_flags_bypass_proposal(db: Database) -> None:
    db.execute(
        "INSERT INTO app.proposed_rules "  # noqa: S608  # test input, not executing user SQL
        "(proposed_rule_id, merchant_pattern, category, status, proposed_at) "
        "VALUES ('bypassP', 'P', 'Dining', 'tracking', now()::TIMESTAMP)"
    )
    result = DoctorService(db)._run_app_audit_coverage(
        PROPOSED_RULES, "proposed_rule_id", updated_col="proposed_at"
    )
    assert result.status == "fail"
    assert "bypassP" in result.affected_ids


def test_proposed_rules_rule_fk_flags_dangling_reference(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    pid = _insert_proposal(repo, status="pending")
    repo.mark_approved(pid, rule_id="ghostrule", actor="cli")  # no such rule
    result = DoctorService(db)._run_proposed_rules_rule_fk()
    assert result.status == "fail"
    assert pid in result.affected_ids


def test_proposed_rules_rule_fk_passes_for_resolved_and_null(db: Database) -> None:
    rule_id = _insert_rule(CategorizationRulesRepo(db))
    proposals = ProposedRulesRepo(db)
    approved = _insert_proposal(proposals, status="pending")
    proposals.mark_approved(approved, rule_id=rule_id, actor="cli")  # resolves
    _insert_proposal(proposals)  # rule_id stays NULL — not an FK violation
    result = DoctorService(db)._run_proposed_rules_rule_fk()
    assert result.status == "pass"


# ---------------------------------------------------------------------------
# transaction_categories: coverage (keys on categorized_at) + transaction FK
# ---------------------------------------------------------------------------


def test_audit_coverage_passes_for_repo_mutated_categorization(db: Database) -> None:
    TransactionCategoriesRepo(db).set(
        "tcov",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    result = DoctorService(db)._run_app_audit_coverage(
        TRANSACTION_CATEGORIES, "transaction_id", updated_col="categorized_at"
    )
    assert result.status == "pass"


def test_transaction_categories_fk_flags_orphan(db: Database) -> None:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.fct_transactions "  # noqa: S608  # test input, not executing user SQL
        "(transaction_id, account_id, transaction_date, amount, source_type) "
        "VALUES ('t_ok', 'a1', DATE '2026-01-01', -5.00, 'csv')"
    )
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "t_ok",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    repo.set(
        "t_orphan",  # no fct_transactions row
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    result = DoctorService(db)._run_transaction_categories_fk()
    assert result.status == "fail"
    assert result.affected_ids == ["t_orphan"]


def test_transaction_categories_fk_passes_when_all_resolve(db: Database) -> None:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.fct_transactions "  # noqa: S608  # test input, not executing user SQL
        "(transaction_id, account_id, transaction_date, amount, source_type) "
        "VALUES ('t_ok', 'a1', DATE '2026-01-01', -5.00, 'csv')"
    )
    TransactionCategoriesRepo(db).set(
        "t_ok",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    result = DoctorService(db)._run_transaction_categories_fk()
    assert result.status == "pass"


def test_run_all_includes_app_integrity_invariants(db: Database) -> None:
    report = DoctorService(db).run_all()
    names = {r.name for r in report.invariants}
    assert "app_audit_coverage_user_categories" in names
    assert "app_user_categories_uniqueness" in names
    assert "app_audit_coverage_user_merchants" in names
    assert "app_user_merchants_orphans" in names
    assert "app_audit_coverage_categorization_rules" in names
    assert "app_audit_coverage_proposed_rules" in names
    assert "app_proposed_rules_rule_fk" in names
    assert "app_audit_coverage_transaction_categories" in names
    assert "app_transaction_categories_fk" in names
