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

from datetime import date
from decimal import Decimal

from moneybin.database import Database
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.repositories.balance_assertions_repo import BalanceAssertionsRepo
from moneybin.repositories.budgets_repo import BudgetsRepo
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
from moneybin.repositories.imports_repo import ImportsRepo
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.proposed_rules_repo import ProposedRulesRepo
from moneybin.repositories.tabular_formats_repo import TabularFormatsRepo
from moneybin.repositories.transaction_categories_repo import (
    TransactionCategoriesRepo,
)
from moneybin.repositories.user_categories_repo import UserCategoriesRepo
from moneybin.repositories.user_merchants_repo import UserMerchantsRepo
from moneybin.services.doctor_service import (
    _BALANCE_ASSERTIONS_PK_EXPR,
    DoctorService,
)
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    BALANCE_ASSERTIONS,
    BUDGETS,
    CATEGORIZATION_RULES,
    IMPORTS,
    MATCH_DECISIONS,
    PROPOSED_RULES,
    TABULAR_FORMATS,
    TRANSACTION_CATEGORIES,
    USER_CATEGORIES,
    USER_MERCHANTS,
)
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables


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


def _insert_match(
    repo: MatchDecisionsRepo,
    *,
    match_id: str,
    account_id: str = "a1",
    account_id_b: str | None = None,
) -> None:
    repo.insert(
        match_id=match_id,
        source_transaction_id_a="sa",
        source_type_a="csv",
        source_origin_a="bank",
        source_transaction_id_b="sb",
        source_type_b="ofx",
        source_origin_b="bank",
        account_id=account_id,
        confidence_score=0.95,
        match_signals={},
        match_tier="3",
        match_status="accepted",
        decided_by="auto",
        account_id_b=account_id_b,
        actor="system",
    )


def test_audit_coverage_passes_for_repo_mutated_tabular_format(db: Database) -> None:
    TabularFormatsRepo(db).set(
        name="chase_credit",
        institution_name="Chase",
        file_type="csv",
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
        sheet=None,
        header_signature=["Date", "Amount"],
        field_mapping={"date": "Date"},
        sign_convention="negative_is_expense",
        date_format="%m/%d/%Y",
        number_format="us",
        skip_trailing_patterns=None,
        multi_account=False,
        source="detected",
        times_used=0,
        last_used_at=None,
        actor="system",
    )
    result = DoctorService(db)._run_app_audit_coverage(TABULAR_FORMATS, "name")
    assert result.status == "pass"


_MATCH_DECISIONS_WATERMARK = "COALESCE(reversed_at, decided_at)"


def test_audit_coverage_passes_for_repo_mutated_match_decision(db: Database) -> None:
    _insert_match(MatchDecisionsRepo(db), match_id="m1")
    result = DoctorService(db)._run_app_audit_coverage(
        MATCH_DECISIONS, "match_id", updated_expr=_MATCH_DECISIONS_WATERMARK
    )
    assert result.status == "pass"


def test_audit_coverage_flags_bypass_reverse_match_decision(db: Database) -> None:
    # Insert via the repo (audited), then a RAW reverse that bumps reversed_at
    # without an audit row. The COALESCE(reversed_at, decided_at) watermark
    # advances past the insert's audit, so the bypass reversal must be flagged —
    # a plain decided_at watermark would miss it.
    _insert_match(MatchDecisionsRepo(db), match_id="mrev")
    db.execute(
        "UPDATE app.match_decisions "  # noqa: S608  # test input, not executing user SQL
        "SET reversed_at = now()::TIMESTAMP, reversed_by = 'user', "
        "match_status = 'reversed' WHERE match_id = 'mrev'"
    )
    result = DoctorService(db)._run_app_audit_coverage(
        MATCH_DECISIONS, "match_id", updated_expr=_MATCH_DECISIONS_WATERMARK
    )
    assert result.status == "fail"
    assert "mrev" in result.affected_ids


def test_audit_coverage_passes_for_repo_mutated_import(db: Database) -> None:
    ImportsRepo(db).set("imp1", labels=["budget-2026"], actor="cli")
    result = DoctorService(db)._run_app_audit_coverage(IMPORTS, "import_id")
    assert result.status == "pass"


def test_match_decisions_account_fk_flags_orphan(db: Database) -> None:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES ('a1')"  # noqa: S608  # test input
    )
    repo = MatchDecisionsRepo(db)
    _insert_match(repo, match_id="m_ok", account_id="a1")
    _insert_match(repo, match_id="m_orphan", account_id="a_missing")
    result = DoctorService(db)._run_match_decisions_account_fk()
    assert result.status == "fail"
    assert result.affected_ids == ["m_orphan"]


def test_match_decisions_account_fk_flags_orphan_counterparty(db: Database) -> None:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES ('a1')"  # noqa: S608  # test input
    )
    repo = MatchDecisionsRepo(db)
    # account_id resolves, but the transfer counterparty account_id_b does not.
    _insert_match(repo, match_id="m_xfer", account_id="a1", account_id_b="a_missing")
    result = DoctorService(db)._run_match_decisions_account_fk()
    assert result.status == "fail"
    assert result.affected_ids == ["m_xfer"]


def test_match_decisions_account_fk_passes_when_all_resolve(db: Database) -> None:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES ('a1'), ('a2')"  # noqa: S608  # test input
    )
    repo = MatchDecisionsRepo(db)
    _insert_match(repo, match_id="m1", account_id="a1")
    _insert_match(repo, match_id="m2", account_id="a1", account_id_b="a2")
    result = DoctorService(db)._run_match_decisions_account_fk()
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
    assert "app_audit_coverage_account_settings" in names
    assert "app_audit_coverage_balance_assertions" in names
    assert "app_audit_coverage_budgets" in names
    assert "app_audit_coverage_tabular_formats" in names
    assert "app_audit_coverage_match_decisions" in names
    assert "app_audit_coverage_imports" in names
    assert "app_account_settings_account_fk" in names
    assert "app_balance_assertions_account_fk" in names
    assert "app_budgets_category_fk" in names
    assert "app_match_decisions_account_fk" in names


# ---------------------------------------------------------------------------
# Batch C: account_settings / balance_assertions / budgets (Invariant 10 PRs 6/9/10)
# ---------------------------------------------------------------------------


def _upsert_settings(repo: AccountSettingsRepo, account_id: str) -> None:
    repo.set(
        account_id=account_id,
        display_name="Checking",
        official_name=None,
        last_four="1234",
        account_subtype="checking",
        holder_category="personal",
        iso_currency_code="USD",
        credit_limit=None,
        archived=False,
        include_in_net_worth=True,
        actor="cli",
    )


def test_audit_coverage_passes_for_repo_mutated_account_settings(db: Database) -> None:
    _upsert_settings(AccountSettingsRepo(db), "acct_cov")
    result = DoctorService(db)._run_app_audit_coverage(ACCOUNT_SETTINGS, "account_id")
    assert result.status == "pass"


def test_audit_coverage_flags_bypass_account_settings(db: Database) -> None:
    db.execute(
        "INSERT INTO app.account_settings (account_id, display_name) "  # noqa: S608  # test input, not executing user SQL
        "VALUES ('bypassA', 'Sneaky')"
    )
    result = DoctorService(db)._run_app_audit_coverage(ACCOUNT_SETTINGS, "account_id")
    assert result.status == "fail"
    assert "bypassA" in result.affected_ids


def test_audit_coverage_passes_for_repo_mutated_balance_assertion(db: Database) -> None:
    BalanceAssertionsRepo(db).set(
        "acct_cov", date(2026, 5, 1), balance=Decimal("10.00"), notes=None, actor="cli"
    )
    result = DoctorService(db)._run_app_audit_coverage(
        BALANCE_ASSERTIONS, "account_id", pk_expr=_BALANCE_ASSERTIONS_PK_EXPR
    )
    assert result.status == "pass"


def test_audit_coverage_flags_bypass_balance_assertion(db: Database) -> None:
    # Raw insert, no audit — the composite pk_expr must reconstruct the same
    # target_id the repo would emit ("account_id|YYYY-MM-DD") to flag it.
    db.execute(
        "INSERT INTO app.balance_assertions (account_id, assertion_date, balance) "  # noqa: S608  # test input, not executing user SQL
        "VALUES ('bypassB', DATE '2026-05-01', 5.00)"
    )
    result = DoctorService(db)._run_app_audit_coverage(
        BALANCE_ASSERTIONS, "account_id", pk_expr=_BALANCE_ASSERTIONS_PK_EXPR
    )
    assert result.status == "fail"
    assert "bypassB|2026-05-01" in result.affected_ids


def test_audit_coverage_passes_for_repo_mutated_budget(db: Database) -> None:
    BudgetsRepo(db).insert(
        category="Dining",
        category_id=None,
        monthly_amount=Decimal("200.00"),
        start_month="2026-05",
        actor="cli",
    )
    result = DoctorService(db)._run_app_audit_coverage(BUDGETS, "budget_id")
    assert result.status == "pass"


def test_account_settings_account_fk_flags_orphan(db: Database) -> None:
    create_core_tables(db)  # dim_accounts exists but empty
    _upsert_settings(AccountSettingsRepo(db), "ghost_acct")
    result = DoctorService(db)._run_account_settings_account_fk()
    assert result.status == "fail"
    assert result.affected_ids == ["ghost_acct"]


def test_account_settings_account_fk_passes_when_resolved(db: Database) -> None:
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts "  # noqa: S608  # test input, not executing user SQL
        "(account_id, account_type, institution_name, source_type) "
        "VALUES ('real_acct', 'CHECKING', 'Bank', 'ofx')"
    )
    _upsert_settings(AccountSettingsRepo(db), "real_acct")
    result = DoctorService(db)._run_account_settings_account_fk()
    assert result.status == "pass"


def test_balance_assertions_account_fk_flags_orphan(db: Database) -> None:
    create_core_tables(db)  # dim_accounts exists but empty
    BalanceAssertionsRepo(db).set(
        "ghost_acct", date(2026, 5, 1), balance=Decimal("9.00"), notes=None, actor="cli"
    )
    result = DoctorService(db)._run_balance_assertions_account_fk()
    assert result.status == "fail"
    assert result.affected_ids == ["ghost_acct"]


def test_budgets_category_fk_flags_dangling_reference(db: Database) -> None:
    create_core_dim_stub_views(db)  # core.dim_categories exists but empty
    BudgetsRepo(db).insert(
        category="Dining",
        category_id="ghostcat",
        monthly_amount=Decimal("100.00"),
        start_month="2026-05",
        actor="cli",
    )
    result = DoctorService(db)._run_budgets_category_fk()
    assert result.status == "fail"
    assert len(result.affected_ids) == 1


def test_budgets_category_fk_passes_for_resolved_and_null(db: Database) -> None:
    # dim_categories view carrying one known category_id; budgets reference it
    # or leave category_id NULL (orphaned legacy row) — neither is a violation.
    db.execute(
        "CREATE OR REPLACE VIEW core.dim_categories AS SELECT 'cat_known' AS category_id"
    )
    repo = BudgetsRepo(db)
    repo.insert(
        category="Dining",
        category_id="cat_known",
        monthly_amount=Decimal("100.00"),
        start_month="2026-05",
        actor="cli",
    )
    repo.insert(
        category="Orphan",
        category_id=None,  # NULL FK — not a violation
        monthly_amount=Decimal("50.00"),
        start_month="2026-05",
        actor="cli",
    )
    result = DoctorService(db)._run_budgets_category_fk()
    assert result.status == "pass"
