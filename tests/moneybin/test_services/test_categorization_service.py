"""Tests for the categorization service.

Covers merchant normalization, pattern matching, rule engine, merchant
matching, prompt construction, and response parsing.
"""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.services import auto_rule_service
from moneybin.services.categorization_service import (
    CategorizationService,
    apply_deterministic_categorization,
    apply_merchant_categories,
    apply_rules,
    create_merchant,
    ensure_seed_table,
    get_active_categories,
    get_categorization_stats,
    match_merchant,
    normalize_description,
    seed_categories,
)
from tests.moneybin.db_helpers import create_core_tables


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


class TestNormalizeDescription:
    """Tests for normalize_description()."""

    @pytest.mark.unit
    def test_strips_square_prefix(self) -> None:
        assert normalize_description("SQ *STARBUCKS #1234") == "STARBUCKS"

    @pytest.mark.unit
    def test_strips_toast_prefix(self) -> None:
        assert normalize_description("TST*PIZZA PLACE") == "PIZZA PLACE"

    @pytest.mark.unit
    def test_strips_paypal_prefix(self) -> None:
        assert normalize_description("PP*SPOTIFY") == "SPOTIFY"

    @pytest.mark.unit
    def test_strips_trailing_state_zip(self) -> None:
        result = normalize_description("WHOLEFDS MKT AUSTIN TX 78701")
        assert "78701" not in result

    @pytest.mark.unit
    def test_strips_trailing_city_state(self) -> None:
        result = normalize_description("STARBUCKS SEATTLE WA")
        assert "SEATTLE" not in result
        assert "WA" not in result

    @pytest.mark.unit
    def test_strips_trailing_store_id(self) -> None:
        result = normalize_description("TARGET 00012345")
        assert "00012345" not in result

    @pytest.mark.unit
    def test_preserves_core_name(self) -> None:
        assert "STARBUCKS" in normalize_description("SQ *STARBUCKS #1234 SEATTLE WA")

    @pytest.mark.unit
    def test_empty_string(self) -> None:
        assert normalize_description("") == ""

    @pytest.mark.unit
    def test_none_handled(self) -> None:
        # normalize_description expects str but should handle edge cases
        assert normalize_description("   ") == ""

    @pytest.mark.unit
    def test_normalizes_whitespace(self) -> None:
        result = normalize_description("SQ  *  COFFEE   SHOP")
        assert "  " not in result


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
# Seed categories
# ---------------------------------------------------------------------------


class TestEnsureSeedTable:
    """Tests for lazy SQLMesh seed initialization."""

    @pytest.mark.unit
    def test_skips_when_table_exists(self, db: Database) -> None:
        """No SQLMesh call when seed table already exists."""
        db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
        db.execute("CREATE TABLE seeds.categories (category_id VARCHAR)")
        # Should return without calling SQLMesh
        ensure_seed_table(db)

    @pytest.mark.unit
    def test_calls_sqlmesh_when_missing(
        self, db: Database, mocker: MockerFixture
    ) -> None:
        """Runs targeted SQLMesh apply when seed table is missing."""
        from contextlib import contextmanager

        mock_ctx = mocker.MagicMock()

        @contextmanager
        def mock_sqlmesh_ctx(**kwargs: object) -> Generator[MagicMock, None, None]:  # noqa: ARG001 — absorb kwargs
            yield mock_ctx

        mocker.patch(
            "moneybin.services.categorization_service.sqlmesh_context",
            side_effect=mock_sqlmesh_ctx,
        )
        # Table doesn't exist, but after plan() it would — simulate by
        # having plan() create the table as a side effect

        def create_table(*args: object, **kwargs: object) -> None:
            db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
            db.execute("CREATE TABLE seeds.categories (category_id VARCHAR)")

        mock_ctx.plan.side_effect = create_table

        ensure_seed_table(db)

        mock_ctx.plan.assert_called_once_with(
            auto_apply=True,
            no_prompts=True,
            select_models=["seeds.categories"],  # SEED_CATEGORIES.full_name
        )


class TestSeedCategories:
    """Tests for category seeding."""

    @pytest.mark.unit
    def test_seed_idempotent(self, db: Database) -> None:
        # Create a mock seed table
        db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
        db.execute("""
            CREATE TABLE seeds.categories (
                category_id VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                description VARCHAR,
                plaid_detailed VARCHAR
            )
        """)
        db.execute("""
            INSERT INTO seeds.categories VALUES
            ('FND', 'Food & Drink', NULL, 'Food and beverages', 'FOOD_AND_DRINK'),
            ('FND-COF', 'Food & Drink', 'Coffee Shops', 'Coffee', 'FOOD_AND_DRINK_COFFEE')
        """)

        first = seed_categories(db)
        assert first == 2

        second = seed_categories(db)
        assert second == 0  # Idempotent

    @pytest.mark.unit
    def test_get_active_categories(self, db: Database) -> None:
        db.execute("""
            INSERT INTO app.categories
            (category_id, category, subcategory, is_default, is_active)
            VALUES
            ('FND', 'Food & Drink', NULL, true, true),
            ('FND-COF', 'Food & Drink', 'Coffee Shops', true, true),
            ('OLD', 'Deprecated', NULL, true, false)
        """)
        categories = get_active_categories(db)
        assert len(categories) == 2
        assert all(c["category"] == "Food & Drink" for c in categories)


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


def test_service_facade_exposes_required_methods() -> None:
    """CategorizationService exposes the documented method surface."""
    expected = {
        "bulk_categorize",
        "apply_rules",
        "apply_deterministic",
        "seed",
        "stats",
        "auto_review",
        "auto_confirm",
        "auto_stats",
    }
    missing = expected - set(dir(CategorizationService))
    assert not missing, f"CategorizationService missing methods: {missing}"


def test_service_bulk_categorize_delegates_to_module_function(
    real_db: Database,
) -> None:
    """Service.bulk_categorize routes through the module-level function."""
    real_db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('ts1', 'a1', DATE '2026-03-01', -3.00, 'STARBUCKS', 'csv')"
    )
    svc = CategorizationService(real_db)
    result = svc.bulk_categorize([
        {"transaction_id": "ts1", "category": "Food & Drink"}
    ])
    assert result.applied == 1


def test_service_auto_review_returns_pending_proposals(real_db: Database) -> None:
    """auto_review returns pending proposals seeded via auto_rule_service."""
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
    auto_rule_service.record_categorization(real_db, "ts2", "Shopping")

    svc = CategorizationService(real_db)
    proposals = svc.auto_review()
    patterns = {p["merchant_pattern"] for p in proposals}
    assert "AMAZON" in patterns
