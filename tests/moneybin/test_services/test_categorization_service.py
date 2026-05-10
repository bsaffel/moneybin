"""Tests for the categorization service.

Covers merchant normalization, pattern matching, rule engine, merchant
matching, prompt construction, and response parsing.
"""

from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml

from moneybin.database import Database
from moneybin.services._text import normalize_description
from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    CategorizationService,
)
from tests.moneybin.db_helpers import create_core_tables, seed_categories_view


def create_merchant(db: Database, *args: object, **kwargs: object) -> str:
    """Test shim — delegates to CategorizationService.create_merchant."""
    return CategorizationService(db).create_merchant(*args, **kwargs)  # type: ignore[arg-type]


def match_merchant(db: Database, description: str) -> dict[str, str | None] | None:
    """Test shim — delegates to CategorizationService.match_merchant."""
    return CategorizationService(db).match_merchant(description)


def apply_rules(db: Database) -> int:
    """Test shim — delegates to CategorizationService.apply_rules."""
    return CategorizationService(db).apply_rules()


def apply_merchant_categories(db: Database) -> int:
    """Test shim — delegates to CategorizationService.apply_merchant_categories."""
    return CategorizationService(db).apply_merchant_categories()


def apply_deterministic_categorization(db: Database) -> dict[str, int]:
    """Test shim — delegates to CategorizationService.apply_deterministic."""
    return CategorizationService(db).apply_deterministic()


def get_categorization_stats(db: Database) -> dict[str, int | float]:
    """Test shim — delegates to CategorizationService.categorization_stats."""
    return CategorizationService(db).categorization_stats()


def get_active_categories(db: Database) -> list[dict[str, str | bool | None]]:
    """Test shim — delegates to CategorizationService.get_active_categories."""
    return CategorizationService(db).get_active_categories()


@pytest.fixture()
def db(tmp_path: Path) -> Database:
    """Create a Database with all schemas for testing."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-tests"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    # Core tables are managed by SQLMesh in production; create concrete
    # tables here so tests can INSERT fixture data directly.
    create_core_tables(database)
    return database


@pytest.fixture()
def db_with_transactions(db: Database) -> Database:
    """DB with sample transactions in core.fct_transactions."""
    db.conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description, memo,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES
        ('TXN001', 'ACC001', '2025-06-15', -4.50, 4.50, 'expense',
         'SQ *STARBUCKS #1234 SEATTLE WA', 'Coffee', 'DEBIT', false,
         'USD', 'ofx', '2025-01-24', CURRENT_TIMESTAMP,
         2025, 6, 15, 0, '2025-06', '2025-Q2'),
        ('TXN002', 'ACC001', '2025-06-20', 3000.00, 3000.00, 'income',
         'ACME CORP PAYROLL', 'Payroll', 'CREDIT', false, 'USD', 'ofx',
         '2025-01-24', CURRENT_TIMESTAMP,
         2025, 6, 20, 5, '2025-06', '2025-Q2'),
        ('TXN003', 'ACC001', '2025-06-25', -52.13, 52.13, 'expense',
         'AMZN MKTP US*ABC123', 'Amazon order', 'DEBIT', false, 'USD', 'ofx',
         '2025-01-24', CURRENT_TIMESTAMP,
         2025, 6, 25, 3, '2025-06', '2025-Q2'),
        ('TXN004', 'ACC002', '2025-06-26', -150.00, 150.00, 'expense',
         'WHOLEFDS MKT 10234 AUSTIN TX 78701', 'Groceries', 'DEBIT', false,
         'USD', 'ofx', '2025-01-24', CURRENT_TIMESTAMP,
         2025, 6, 26, 4, '2025-06', '2025-Q2')
    """)
    return db


# ---------------------------------------------------------------------------
# Merchant name normalization
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_normalize_cases(
    path: Path | None = None,
) -> list[dict[str, Any]]:
    """Load and validate normalize_description golden cases from YAML."""
    if path is None:
        path = _FIXTURES_DIR / "normalize_description_cases.yaml"
    raw = yaml.safe_load(path.read_text())
    cases = raw["cases"]
    counts = Counter(c["id"] for c in cases)
    duplicates = sorted(i for i, n in counts.items() if n > 1)
    if duplicates:
        raise ValueError(f"Duplicate case ids: {duplicates}")
    for c in cases:
        if not isinstance(c.get("raw"), str) or not isinstance(c.get("expected"), str):
            raise ValueError(
                f"Case {c.get('id')!r}: 'raw' and 'expected' must be strings"
            )
    return cases


class TestNormalizeDescriptionGoldens:
    """Parametrized golden-case tests for normalize_description()."""

    @pytest.mark.unit
    @pytest.mark.parametrize("case", _load_normalize_cases(), ids=lambda c: c["id"])
    def test_case(self, case: dict[str, Any]) -> None:
        assert normalize_description(case["raw"]) == case["expected"]

    @pytest.mark.unit
    def test_loader_rejects_duplicate_ids(self, tmp_path: Path) -> None:
        """The loader must surface duplicate ids loudly at collection time."""
        bad_yaml = tmp_path / "dup.yaml"
        bad_yaml.write_text(
            "cases:\n"
            '  - {id: a, raw: "x", expected: "x"}\n'
            '  - {id: a, raw: "y", expected: "y"}\n'
        )

        with pytest.raises(ValueError, match="Duplicate case ids"):
            _load_normalize_cases(bad_yaml)


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------


class TestMatchesPattern:
    """Tests for _matches_pattern() via match_merchant."""

    @pytest.mark.unit
    def test_exact_match(self, db: Database) -> None:
        create_merchant(
            db,
            "starbucks",
            "Starbucks",
            match_type="exact",
            category="Food & Drink",
        )
        result = match_merchant(db, "starbucks")
        assert result is not None
        assert result["canonical_name"] == "Starbucks"

    @pytest.mark.unit
    def test_exact_case_insensitive(self, db: Database) -> None:
        create_merchant(
            db,
            "STARBUCKS",
            "Starbucks",
            match_type="exact",
            category="Food & Drink",
        )
        result = match_merchant(db, "starbucks")
        assert result is not None

    @pytest.mark.unit
    def test_contains_match(self, db: Database) -> None:
        create_merchant(
            db,
            "AMZN",
            "Amazon",
            match_type="contains",
            category="Shopping",
        )
        result = match_merchant(db, "AMZN MKTP US*ABC123")
        assert result is not None
        assert result["canonical_name"] == "Amazon"

    @pytest.mark.unit
    def test_regex_match(self, db: Database) -> None:
        create_merchant(
            db,
            r"UBER\s*(TRIP|EATS)",
            "Uber",
            match_type="regex",
            category="Transportation",
        )
        result = match_merchant(db, "UBER TRIP")
        assert result is not None
        assert result["canonical_name"] == "Uber"

    @pytest.mark.unit
    def test_no_match_returns_none(self, db: Database) -> None:
        create_merchant(db, "STARBUCKS", "Starbucks", match_type="exact")
        result = match_merchant(db, "DUNKIN DONUTS")
        assert result is None

    @pytest.mark.unit
    def test_exact_takes_priority_over_contains(self, db: Database) -> None:
        create_merchant(
            db,
            "AMZN",
            "Amazon General",
            match_type="contains",
            category="Shopping",
            subcategory="Online Marketplaces",
        )
        create_merchant(
            db,
            "amzn mktp",
            "Amazon Marketplace",
            match_type="exact",
            category="Shopping",
            subcategory="Online Marketplaces",
        )
        result = match_merchant(db, "AMZN MKTP")
        assert result is not None
        # exact match should win
        assert result["canonical_name"] == "Amazon Marketplace"


# ---------------------------------------------------------------------------
# Rule engine
# ---------------------------------------------------------------------------


class TestApplyRules:
    """Tests for rule-based categorization."""

    @pytest.mark.unit
    def test_basic_rule(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category, subcategory,
             priority, is_active, created_by, created_at, updated_at)
            VALUES ('R001', 'Starbucks -> Coffee', 'STARBUCKS', 'contains',
                    'Food & Drink', 'Coffee Shops', 10, true, 'user',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        count = apply_rules(db)
        assert count >= 1

        # Verify the categorization was applied
        row = db.execute("""
            SELECT category, subcategory, categorized_by, rule_id
            FROM app.transaction_categories
            WHERE transaction_id = 'TXN001'
        """).fetchone()
        assert row is not None
        assert row[0] == "Food & Drink"
        assert row[1] == "Coffee Shops"
        assert row[2] == "rule"
        assert row[3] == "R001"

    @pytest.mark.unit
    def test_rule_priority_ordering(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        # Two rules match TXN003 (Amazon), but lower priority wins
        db.execute("""
            INSERT INTO app.categorization_rules VALUES
            ('R001', 'Amazon General', 'AMZN', 'contains', NULL, NULL,
             NULL, 'Shopping', 'Other Shopping', 100, true, 'user',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('R002', 'Amazon Electronics', 'AMZN', 'contains', NULL, NULL,
             NULL, 'Shopping', 'Electronics', 10, true, 'user',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        apply_rules(db)

        row = db.execute("""
            SELECT subcategory FROM app.transaction_categories
            WHERE transaction_id = 'TXN003'
        """).fetchone()
        assert row is not None
        assert row[0] == "Electronics"  # priority 10 wins over 100

    @pytest.mark.unit
    def test_amount_filter(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        # Rule only matches expenses > $100 (amount < -100)
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type,
             min_amount, max_amount, category, priority, is_active,
             created_by, created_at, updated_at)
            VALUES ('R001', 'Large grocery', 'WHOLEFDS', 'contains',
                    NULL, -100.00, 'Food & Drink', 10, true,
                    'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        count = apply_rules(db)
        assert count == 1  # Only TXN004 (-150) matches

    @pytest.mark.unit
    def test_account_filter(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, account_id,
             category, priority, is_active, created_by, created_at, updated_at)
            VALUES ('R001', 'Account-specific', 'STARBUCKS', 'contains',
                    'ACC002', 'Food & Drink', 10, true, 'user',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        count = apply_rules(db)
        # TXN001 has STARBUCKS but is in ACC001, not ACC002
        assert count == 0

    @pytest.mark.unit
    def test_idempotent(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category,
             priority, is_active, created_by, created_at, updated_at)
            VALUES ('R001', 'Amazon', 'AMZN', 'contains', 'Shopping',
                    10, true, 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        first = apply_rules(db)
        second = apply_rules(db)
        assert first > 0
        assert second == 0  # Already categorized, no duplicates

    @pytest.mark.unit
    def test_inactive_rules_skipped(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category,
             priority, is_active, created_by, created_at, updated_at)
            VALUES ('R001', 'Amazon', 'AMZN', 'contains', 'Shopping',
                    10, false, 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        count = apply_rules(db)
        assert count == 0


# ---------------------------------------------------------------------------
# Merchant categories
# ---------------------------------------------------------------------------


class TestApplyMerchantCategories:
    """Tests for merchant-based auto-categorization."""

    @pytest.mark.unit
    def test_applies_merchant_category(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        create_merchant(
            db,
            "STARBUCKS",
            "Starbucks",
            match_type="contains",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        count = apply_merchant_categories(db)
        assert count >= 1

    @pytest.mark.unit
    def test_skips_merchants_without_category(
        self, db_with_transactions: Database
    ) -> None:
        db = db_with_transactions
        create_merchant(db, "STARBUCKS", "Starbucks", match_type="contains")
        count = apply_merchant_categories(db)
        assert count == 0


# ---------------------------------------------------------------------------
# Deterministic categorization pipeline
# ---------------------------------------------------------------------------


class TestApplyDeterministicCategorization:
    """Tests for the combined merchant + rules pipeline."""

    @pytest.mark.unit
    def test_rules_then_merchants(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        # Merchant matches Starbucks
        create_merchant(
            db,
            "STARBUCKS",
            "Starbucks",
            match_type="contains",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        # Rule matches Amazon
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category,
             priority, is_active, created_by, created_at, updated_at)
            VALUES ('R001', 'Amazon', 'AMZN', 'contains', 'Shopping',
                    10, true, 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        stats = apply_deterministic_categorization(db)
        assert stats["merchant"] >= 1
        assert stats["rule"] >= 1
        assert stats["total"] >= 2

    @pytest.mark.unit
    def test_rule_takes_precedence_over_merchant(
        self, db_with_transactions: Database
    ) -> None:
        """A transaction matched by both a rule and a merchant mapping.

        Should receive the rule's category, not the merchant's.
        """
        db = db_with_transactions
        # Merchant mapping matches AMZN → Shopping
        create_merchant(
            db,
            "AMZN",
            "Amazon",
            match_type="contains",
            category="Shopping",
        )
        # Rule also matches AMZN → Business (higher precedence)
        db.execute("""
            INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category,
             priority, is_active, created_by, created_at, updated_at)
            VALUES ('R001', 'Amazon Business', 'AMZN', 'contains', 'Business',
                    10, true, 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        apply_deterministic_categorization(db)
        row = db.execute("""
            SELECT category FROM app.transaction_categories
            WHERE transaction_id = 'TXN003'
        """).fetchone()
        assert row is not None
        assert row[0] == "Business"  # rule wins over merchant mapping


# ---------------------------------------------------------------------------
# Categorization stats
# ---------------------------------------------------------------------------


class TestGetCategorizationStats:
    """Tests for categorization coverage stats."""

    @pytest.mark.unit
    def test_all_uncategorized(self, db_with_transactions: Database) -> None:
        stats = get_categorization_stats(db_with_transactions)
        assert stats["total"] == 4
        assert stats["categorized"] == 0
        assert stats["uncategorized"] == 4

    @pytest.mark.unit
    def test_with_categorized(self, db_with_transactions: Database) -> None:
        db = db_with_transactions
        db.execute("""
            INSERT INTO app.transaction_categories
            (transaction_id, category, categorized_by)
            VALUES ('TXN001', 'Food & Drink', 'user')
        """)
        stats = get_categorization_stats(db)
        assert stats["total"] == 4
        assert stats["categorized"] == 1
        assert stats["by_user"] == 1


# ---------------------------------------------------------------------------
# Categories view (seeds + user, with overrides)
# ---------------------------------------------------------------------------


class TestCategoriesView:
    """Tests for the core.dim_categories view that unions seeds + user_categories."""

    @staticmethod
    def _setup_seeds_and_view(db: Database) -> None:
        seed_categories_view(db)
        db.execute("""
            INSERT INTO seeds.categories VALUES
            ('FND-COF', 'Food & Drink', 'Coffee Shops', 'Coffee', 'FOOD_AND_DRINK_COFFEE')
        """)

    @pytest.mark.unit
    def test_view_exposes_seeds_as_defaults(self, db: Database) -> None:
        self._setup_seeds_and_view(db)
        categories = get_active_categories(db)
        assert len(categories) == 2
        assert all(c["is_default"] is True for c in categories)

    @pytest.mark.unit
    def test_view_unions_user_categories(self, db: Database) -> None:
        self._setup_seeds_and_view(db)
        db.execute("""
            INSERT INTO app.user_categories (category_id, category, subcategory)
            VALUES ('CUSTOM1', 'Childcare', 'Daycare')
        """)
        categories = get_active_categories(db)
        ids = {c["category_id"] for c in categories}
        assert ids == {"FND", "FND-COF", "CUSTOM1"}

    @pytest.mark.unit
    def test_view_applies_default_override_deactivation(self, db: Database) -> None:
        self._setup_seeds_and_view(db)
        db.execute("""
            INSERT INTO app.category_overrides (category_id, is_active)
            VALUES ('FND', false)
        """)
        categories = get_active_categories(db)
        ids = {c["category_id"] for c in categories}
        assert "FND" not in ids
        assert "FND-COF" in ids


# ---------------------------------------------------------------------------
# CategorizationService facade
# ---------------------------------------------------------------------------


@pytest.fixture()
def real_db(tmp_path: Path) -> Database:
    """Real DB with core + app schema, used by service-facade tests."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    return db


def test_service_bulk_categorize_applies_categorization(
    real_db: Database,
) -> None:
    """Service.bulk_categorize writes a category row for the given transaction."""
    real_db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('ts1', 'a1', DATE '2026-03-01', -3.00, 'STARBUCKS', 'csv')"
    )
    svc = CategorizationService(real_db)
    result = svc.bulk_categorize([
        BulkCategorizationItem(transaction_id="ts1", category="Food & Drink")
    ])
    assert result.applied == 1


def test_bulk_categorize_returns_did_you_mean_on_invalid_category(
    real_db: Database,
) -> None:
    """bulk_categorize rejects an invalid category with a structured did_you_mean field."""
    real_db.execute(
        "INSERT INTO app.user_categories (category_id, category, subcategory) "
        "VALUES ('cat001', 'Food & Dining', NULL)"
    )
    svc = CategorizationService(real_db)
    result = svc.bulk_categorize([
        BulkCategorizationItem(transaction_id="txn_dym", category="FOOD"),
    ])

    assert result.errors == 1
    assert result.applied == 0
    detail = result.error_details[0]
    assert detail["error"] == "invalid_category"
    assert detail["invalid_value"] == "FOOD"
    assert "valid_categories" in detail
    assert "did_you_mean" in detail
    assert "Food & Dining" in detail["did_you_mean"]
    # `reason` is the human-readable summary the CLI table renderer
    # (`apply` and `apply-from-file`) reads. Locking the contract.
    assert "reason" in detail
    assert "FOOD" in detail["reason"]


def test_service_auto_review_returns_pending_proposals(real_db: Database) -> None:
    """list_pending_proposals returns proposals recorded via AutoRuleService."""
    from moneybin.services.auto_rule_service import AutoRuleService

    real_db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('ts2', 'a1', DATE '2026-03-02', -3.00, 'AMAZON', 'csv')"
    )
    real_db.execute(
        "INSERT INTO app.transaction_categories "
        "(transaction_id, category, categorized_at, categorized_by) "
        "VALUES ('ts2', 'Shopping', CURRENT_TIMESTAMP, 'user')"
    )
    auto = AutoRuleService(real_db)
    auto.record_categorization("ts2", "Shopping")
    proposals = auto.list_pending_proposals()
    patterns = {p["merchant_pattern"] for p in proposals}
    assert "AMAZON" in patterns


# ---------------------------------------------------------------------------
# T7c contract tests — class-first surface
# ---------------------------------------------------------------------------


def test_no_public_module_level_categorization_functions() -> None:
    """Surface contract: only CategorizationService is the public API."""
    import moneybin.services.categorization_service as mod

    forbidden = {
        "bulk_categorize",
        "apply_rules",
        "seed_categories",
        "get_stats",
        "get_categorization_stats",
        "match_merchant",
        "apply_merchant_categories",
        "ensure_seed_table",
        "get_active_categories",
        "create_merchant",
        "apply_deterministic_categorization",
    }
    leaked = {name for name in forbidden if hasattr(mod, name)}
    assert not leaked, f"These should be class methods only: {leaked}"


def test_service_exposes_consolidated_methods(real_db: Database) -> None:
    """CategorizationService exposes its core categorization surface.

    Auto-rule lifecycle methods now live on ``AutoRuleService`` and are
    asserted in ``test_auto_rule_service.py``.
    """
    expected = {
        "bulk_categorize",
        "apply_rules",
        "apply_deterministic",
        "stats",
        "match_merchant",
        "apply_merchant_categories",
        "get_active_categories",
        "categorization_stats",
        "find_matching_rule",
        "create_rules",
        "deactivate_rule",
        "create_category",
        "toggle_category",
    }
    missing = expected - set(dir(CategorizationService))
    assert not missing, f"Missing methods: {missing}"


def test_find_matching_rule_returns_first_match(real_db: Database) -> None:
    """find_matching_rule returns (rule_id, category, subcategory, created_by) for the highest-priority match."""
    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('fm1', 'a1', DATE '2026-03-01', -10.00, 'STARBUCKS DOWNTOWN', 'csv')"
    )
    real_db.execute(
        "INSERT INTO app.categorization_rules (rule_id, name, merchant_pattern, match_type, category, priority, is_active, created_by) "
        "VALUES ('r1', 'sb', 'STARBUCKS', 'contains', 'Food & Drink', 100, true, 'user')"
    )

    match = CategorizationService(real_db).find_matching_rule("fm1")
    assert match is not None
    rule_id, category, _sub, created_by = match
    assert (rule_id, category, created_by) == ("r1", "Food & Drink", "user")


def test_find_matching_rule_returns_none_when_no_rule_matches(
    real_db: Database,
) -> None:
    """find_matching_rule returns None when no active rule matches the transaction."""
    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('fm2', 'a1', DATE '2026-03-01', -10.00, 'NOTHING MATCHES', 'csv')"
    )
    assert CategorizationService(real_db).find_matching_rule("fm2") is None


def test_apply_rules_writes_auto_rule_provenance_for_auto_rules(
    real_db: Database,
) -> None:
    """apply_rules writes categorized_by='auto_rule' when the matching rule is auto-created."""
    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('ar1', 'a1', DATE '2026-03-01', -5.00, 'CHIPOTLE MEXICAN', 'csv')"
    )
    real_db.execute(
        "INSERT INTO app.categorization_rules (rule_id, name, merchant_pattern, match_type, category, priority, is_active, created_by) "
        "VALUES ('rauto', 'auto: chipotle', 'CHIPOTLE', 'contains', 'Food & Drink', 200, true, 'auto_rule')"
    )

    CategorizationService(real_db).apply_rules()

    row = real_db.execute(
        "SELECT categorized_by FROM app.transaction_categories WHERE transaction_id = 'ar1'"
    ).fetchone()
    assert row == ("auto_rule",)


def test_apply_rules_writes_rule_provenance_for_user_rules(
    real_db: Database,
) -> None:
    """apply_rules writes categorized_by='rule' for non-auto-rule matches."""
    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('ar2', 'a1', DATE '2026-03-01', -5.00, 'WHOLE FOODS', 'csv')"
    )
    real_db.execute(
        "INSERT INTO app.categorization_rules (rule_id, name, merchant_pattern, match_type, category, priority, is_active, created_by) "
        "VALUES ('ruser', 'wf', 'WHOLE FOODS', 'contains', 'Groceries', 100, true, 'user')"
    )

    CategorizationService(real_db).apply_rules()

    row = real_db.execute(
        "SELECT categorized_by FROM app.transaction_categories WHERE transaction_id = 'ar2'"
    ).fetchone()
    assert row == ("rule",)


def test_list_auto_rules_returns_active_auto_rules(real_db: Database) -> None:
    """AutoRuleService.list_active_rules returns active auto-rules after approval."""
    from moneybin.services.auto_rule_service import AutoRuleService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('lt1', 'a1', DATE '2026-03-01', -3.00, 'CHIPOTLE', 'csv')"
    )
    auto = AutoRuleService(real_db)
    pid = auto.record_categorization("lt1", "Food & Drink")
    assert pid is not None
    auto.accept(accept=[pid])

    rules = auto.list_active_rules()
    assert any(r["merchant_pattern"] == "CHIPOTLE" for r in rules)


def test_bulk_categorize_creates_auto_rule_proposal(real_db: Database) -> None:
    """bulk_categorize records a pending proposal for novel txn → category mappings."""
    from moneybin.services.categorization_service import CategorizationService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('tb1', 'a1', DATE '2026-02-01', -4.50, 'STARBUCKS RESERVE', 'csv')"
    )
    svc = CategorizationService(real_db)
    svc.bulk_categorize(
        [
            BulkCategorizationItem(
                transaction_id="tb1",
                category="Food & Drink",
                subcategory="Coffee",
            )
        ],
    )

    rows = real_db.execute(
        "SELECT merchant_pattern, category, status FROM app.proposed_rules"
    ).fetchall()
    assert ("STARBUCKS RESERVE", "Food & Drink", "pending") in rows


# ---------------------------------------------------------------------------
# bulk_categorize — perf shape and in-batch dedup
# ---------------------------------------------------------------------------


def test_bulk_categorize_uses_constant_number_of_db_calls(
    monkeypatch: pytest.MonkeyPatch,
    mock_secret_store: MagicMock,
    tmp_path: Path,
) -> None:
    """bulk_categorize should not scale DB round-trips with item count.

    With N items, the number of read queries (description fetch + merchant
    fetch) must be O(1), not O(N).
    """
    from moneybin.tables import FCT_TRANSACTIONS

    db = Database(
        tmp_path / "perf.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    create_core_tables(db)
    # Seed 25 transactions and 25 corresponding category items.
    for i in range(25):
        db.execute(
            f"""
            INSERT INTO {FCT_TRANSACTIONS.full_name}
            (transaction_id, account_id, transaction_date, amount, description, source_type)
            VALUES (?, 'acct1', DATE '2025-01-01', -10.00, ?, 'csv')
            """,  # noqa: S608  # building test input string, not executing SQL
            [f"txn_{i}", f"Coffee shop {i}"],
        )
    items = [
        BulkCategorizationItem(
            transaction_id=f"txn_{i}", category="Food", subcategory="Coffee"
        )
        for i in range(25)
    ]

    real_execute = db.execute
    select_calls: list[str] = []

    def counting_execute(query: str, params: list[Any] | None = None) -> object:
        if query.strip().upper().startswith("SELECT"):
            select_calls.append(query)
        return real_execute(query, params)

    monkeypatch.setattr(db, "execute", counting_execute)

    result = CategorizationService(db).bulk_categorize(items)

    assert result.applied == 25
    # The bulk_categorize merchant-resolution read path must be batched.
    # Verify a single batched description fetch (WHERE transaction_id IN (...))
    # ran for the whole input, regardless of N. Per-row fetches inside
    # _auto_rule recording are a separate concern and are out of scope here.
    batched = [
        q
        for q in select_calls
        if "fct_transactions" in q.lower() and "transaction_id in (" in q.lower()
    ]
    assert len(batched) == 1, (
        f"Expected exactly 1 batched description fetch, got {len(batched)}:\n"
        + "\n".join(batched)
    )


def test_bulk_categorize_dedupes_merchant_creation_within_batch(
    mock_secret_store: MagicMock,
    tmp_path: Path,
) -> None:
    """Two items with the same description create exactly one merchant."""
    from moneybin.tables import FCT_TRANSACTIONS, MERCHANTS

    db = Database(
        tmp_path / "dedup.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    create_core_tables(db)
    for i in range(3):
        db.execute(
            f"""
            INSERT INTO {FCT_TRANSACTIONS.full_name}
            (transaction_id, account_id, transaction_date, amount, description, source_type)
            VALUES (?, 'acct1', DATE '2025-01-01', -10.00, 'IDENTICAL VENDOR', 'csv')
            """,  # noqa: S608  # building test input string, not executing SQL
            [f"txn_{i}"],
        )

    items = [
        BulkCategorizationItem(
            transaction_id=f"txn_{i}", category="Food", subcategory="Coffee"
        )
        for i in range(3)
    ]

    result = CategorizationService(db).bulk_categorize(items)

    assert result.applied == 3
    assert result.merchants_created == 1, (
        f"Expected 1 merchant created across 3 identical-description items, "
        f"got {result.merchants_created}"
    )

    merchant_count = db.execute(
        f"SELECT COUNT(*) FROM {MERCHANTS.full_name}"  # noqa: S608  # building test input string, not executing SQL
    ).fetchone()
    assert merchant_count is not None
    assert merchant_count[0] == 1


# ---------------------------------------------------------------------------
# find_matching_rule override tests (Task 3 — bulk path preparation)
# ---------------------------------------------------------------------------


def test_find_matching_rule_uses_rules_override(real_db: Database) -> None:
    """When rules_override is provided, the rules table is not queried."""
    svc = CategorizationService(real_db)
    real_db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('csv_test', 'acct_1', DATE '2026-01-01', -5.0, 'STARBUCKS COFFEE', 'csv')"
    )
    # Override rules list — nothing is in app.categorization_rules, so if the
    # method queries the DB it would return no rules and the result would be None.
    override_rules: list[tuple[Any, ...]] = [
        ("rule_1", "STARBUCKS", "contains", None, None, None, "Food", "Coffee", "user")
    ]
    match = svc.find_matching_rule("csv_test", rules_override=override_rules)
    assert match is not None
    assert match[1] == "Food"
    assert match[2] == "Coffee"


def test_find_matching_rule_uses_txn_row_override(real_db: Database) -> None:
    """When txn_row_override is provided, fct_transactions is not queried."""
    svc = CategorizationService(real_db)
    # No INSERT — fct_transactions has no row for ghost_txn.
    override_rules: list[tuple[Any, ...]] = [
        ("rule_1", "AMZN", "contains", None, None, None, "Shopping", None, "user")
    ]
    match = svc.find_matching_rule(
        "ghost_txn",
        rules_override=override_rules,
        txn_row_override=("AMZN MARKETPLACE", -42.0, "acct_1"),
    )
    assert match is not None


# ---------------------------------------------------------------------------
# categorize_assist tests (Task 14 — RED)
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_with_uncategorized_txns(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Database:
    """Database seeded with 10 uncategorized transactions in core.fct_transactions."""
    db = Database(
        tmp_path / "assist.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    descriptions = [
        "STARBUCKS #1234",
        "AMZN MKTP US*ABCD",
        "NETFLIX.COM",
        "VENMO PAYMENT TO J SMITH",
        "RANDOM LOCAL CAFE",
        "SHELL OIL #5678",
        "WHOLE FOODS MKT",
        "UBER EATS",
        "CHECK #2341",
        "COMCAST CABLE 800-555-1234",
    ]
    for i, desc in enumerate(descriptions):
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES (?, ?, DATE '2026-04-01', ?, ?, ?)",
            [f"txn_{i}", "acct_test", -10.00, desc, "csv"],
        )
    return db


def test_categorize_assist_returns_redacted_uncategorized(
    db_with_uncategorized_txns: Database,
) -> None:
    """categorize_assist should return uncategorized txns with redacted descriptions only."""
    from moneybin.services.categorization_service import (
        CategorizationService,
        RedactedTransaction,
    )

    svc = CategorizationService(db_with_uncategorized_txns)
    result = svc.categorize_assist(limit=10)

    assert all(isinstance(r, RedactedTransaction) for r in result)
    for r in result:
        assert hasattr(r, "opaque_id")
        assert hasattr(r, "description_redacted")
        assert hasattr(r, "source_type")
        # Confirm no amount/date/account fields
        assert not hasattr(r, "amount")
        assert not hasattr(r, "date")
        assert not hasattr(r, "account_id")


def test_categorize_assist_respects_limit(db_with_uncategorized_txns: Database) -> None:
    """categorize_assist returns no more rows than the requested limit."""
    from moneybin.services.categorization_service import CategorizationService

    svc = CategorizationService(db_with_uncategorized_txns)
    result = svc.categorize_assist(limit=5)
    assert len(result) <= 5


def test_categorize_assist_clamps_to_max_batch_size(
    db_with_uncategorized_txns: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Server enforces assist_max_batch_size hard ceiling."""
    from unittest.mock import MagicMock as _MagicMock

    from moneybin.services import categorization_service as _cs

    mock_settings = _MagicMock()
    mock_settings.categorization.assist_max_batch_size = 3
    monkeypatch.setattr(_cs, "get_settings", lambda: mock_settings)

    svc = _cs.CategorizationService(db_with_uncategorized_txns)
    result = svc.categorize_assist(limit=100)  # over the ceiling
    assert len(result) <= 3


# ---------------------------------------------------------------------------
# Task 16: user merchant outranks seed on overlap
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_with_seed_and_user_merchants(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Database:
    """Database with both a seed merchant and a user merchant matching the same pattern.

    Seed: STARBUCKS contains → Food & Dining / Coffee Shops
    User: STARBUCKS contains → Business Meals
    The user entry must win in _fetch_merchants ordering.
    """
    db = Database(
        tmp_path / "overlap.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    # The app schema (user_merchants, merchant_overrides) is created by Database init.
    # Simulate seed tables that SQLMesh would create in production.
    db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS seeds.merchants_global (
            merchant_id VARCHAR PRIMARY KEY,
            raw_pattern VARCHAR,
            match_type VARCHAR,
            canonical_name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            country VARCHAR
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS seeds.merchants_us (
            merchant_id VARCHAR PRIMARY KEY,
            raw_pattern VARCHAR,
            match_type VARCHAR,
            canonical_name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            country VARCHAR
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS seeds.merchants_ca (
            merchant_id VARCHAR PRIMARY KEY,
            raw_pattern VARCHAR,
            match_type VARCHAR,
            canonical_name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            country VARCHAR
        )
        """
    )
    db.execute(
        "INSERT INTO seeds.merchants_us VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "seed_us_starbucks",
            "STARBUCKS",
            "contains",
            "Starbucks",
            "Food & Dining",
            "Coffee Shops",
            "US",
        ],
    )
    db.execute(
        "INSERT INTO app.user_merchants "
        "(merchant_id, raw_pattern, match_type, canonical_name, category, subcategory, created_by) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "user_m_starbucks",
            "STARBUCKS",
            "contains",
            "Starbucks Business",
            "Business Meals",
            None,
            "user",
        ],
    )
    from moneybin.seeds import refresh_views

    refresh_views(db)
    return db


def test_user_merchant_outranks_seed_on_overlap(
    db_with_seed_and_user_merchants: Database,
) -> None:
    """When both a user merchant and a seed merchant could match, user wins."""
    from moneybin.services.categorization_service import CategorizationService

    svc = CategorizationService(db_with_seed_and_user_merchants)
    match = svc.match_merchant("STARBUCKS #1234")

    assert match is not None
    assert match["category"] == "Business Meals"  # user wins over seed


class TestSetCategoryAudit:
    """Audit emission for set_category / clear_category (Req 25-31)."""

    @pytest.mark.unit
    def test_set_category_emits_audit_event(self, db: Database) -> None:
        svc = CategorizationService(db)
        svc.set_category("T1", category="Food", subcategory="Coffee", actor="cli")

        rows = db.conn.execute(
            "SELECT action, target_table, target_id, before_value, after_value "
            "FROM app.audit_log WHERE action = 'category.set'"
        ).fetchall()
        assert len(rows) == 1
        action, target_table, target_id, before, after = rows[0]
        assert action == "category.set"
        assert target_table == "transaction_categories"
        assert target_id == "T1"
        assert before is None
        import json as _json

        assert _json.loads(after) == {
            "category": "Food",
            "subcategory": "Coffee",
            "categorized_by": "user",
        }
        cat_rows = db.conn.execute(
            "SELECT category, subcategory, categorized_by "
            "FROM app.transaction_categories WHERE transaction_id = ?",
            ["T1"],
        ).fetchall()
        assert cat_rows == [("Food", "Coffee", "user")]

    @pytest.mark.unit
    def test_clear_category_emits_audit_with_after_null(self, db: Database) -> None:
        svc = CategorizationService(db)
        svc.set_category("T1", category="Food", actor="cli")
        svc.clear_category("T1", actor="cli")

        clear_rows = db.conn.execute(
            "SELECT before_value, after_value FROM app.audit_log "
            "WHERE action = 'category.clear'"
        ).fetchall()
        assert len(clear_rows) == 1
        before, after = clear_rows[0]
        assert after is None
        import json as _json

        b = _json.loads(before)
        assert b["category"] == "Food"
        # Row deleted.
        cnt = db.conn.execute(
            "SELECT COUNT(*) FROM app.transaction_categories WHERE transaction_id = ?",
            ["T1"],
        ).fetchone()
        assert cnt is not None and cnt[0] == 0

    @pytest.mark.unit
    def test_clear_category_noop_when_absent_emits_no_event(self, db: Database) -> None:
        svc = CategorizationService(db)
        svc.clear_category("T-missing", actor="cli")
        cnt = db.conn.execute(
            "SELECT COUNT(*) FROM app.audit_log WHERE action = 'category.clear'"
        ).fetchone()
        assert cnt is not None and cnt[0] == 0

    @pytest.mark.unit
    def test_set_category_overwrite_captures_before_and_after(
        self, db: Database
    ) -> None:
        svc = CategorizationService(db)
        svc.set_category("T1", category="Food", actor="cli")
        svc.set_category("T1", category="Travel", subcategory="Flights", actor="cli")

        rows = db.conn.execute(
            "SELECT before_value, after_value FROM app.audit_log "
            "WHERE action = 'category.set' ORDER BY occurred_at"
        ).fetchall()
        assert len(rows) == 2
        import json as _json

        # First event: no prior.
        assert rows[0][0] is None
        # Second event: before captures the prior {Food}, after has {Travel/Flights}.
        before = _json.loads(rows[1][0])
        after = _json.loads(rows[1][1])
        assert before["category"] == "Food"
        assert after["category"] == "Travel"
        assert after["subcategory"] == "Flights"
