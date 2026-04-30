"""Tests for the categorization service.

Covers merchant normalization, pattern matching, rule engine, merchant
matching, prompt construction, and response parsing.
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.services._text import normalize_description
from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    CategorizationService,
)
from tests.moneybin.db_helpers import create_core_tables


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


def seed_categories(db: Database) -> int:
    """Test shim — delegates to CategorizationService.seed."""
    return CategorizationService(db).seed()


def ensure_seed_table(db: Database) -> None:
    """Test shim — delegates to CategorizationService.ensure_seed_table."""
    CategorizationService(db).ensure_seed_table()


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
        "seed",
        "stats",
        "match_merchant",
        "apply_merchant_categories",
        "ensure_seed_table",
        "get_active_categories",
        "categorization_stats",
        "find_matching_rule",
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
    auto.confirm(approve=[pid])

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
