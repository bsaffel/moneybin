"""Tests for the categorization service.

Covers merchant normalization, pattern matching, rule engine, merchant
matching, prompt construction, and response parsing.
"""

from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from moneybin.database import Database
from moneybin.seeds import refresh_views
from moneybin.services._text import normalize_description
from moneybin.services.categorization import (
    CategorizationItem,
    CategorizationService,
    score_match_shape,
)
from moneybin.services.categorization.assist import (
    _amount_sign_label,  # pyright: ignore[reportPrivateUsage]  # tested directly
)
from tests.moneybin.db_helpers import create_core_tables, seed_categories_view


def create_merchant(db: Database, *args: object, **kwargs: object) -> str:
    """Test shim — delegates to CategorizationService.create_merchant."""
    return CategorizationService(db).create_merchant(*args, **kwargs)  # type: ignore[arg-type]


def match_merchant(
    db: Database, description: str, *, memo: str | None = None
) -> dict[str, str | None] | None:
    """Test shim — delegates to CategorizationService.match_merchant."""
    return CategorizationService(db).match_merchant(description, memo=memo)


def apply_rules(db: Database) -> int:
    """Test shim — delegates to CategorizationService.apply_rules.

    ``apply_rules`` returns the set of applied ``transaction_id``s; tests
    historically assert on the count, so wrap with ``len``.
    """
    return len(CategorizationService(db).apply_rules())


def apply_merchant_categories(db: Database) -> int:
    """Test shim — delegates to CategorizationService.apply_merchant_categories."""
    return CategorizationService(db).apply_merchant_categories()


def apply_plaid_categories(db: Database) -> int:
    """Test shim — delegates to CategorizationService.apply_plaid_categories."""
    return CategorizationService(db).apply_plaid_categories()


def categorize_pending(db: Database) -> dict[str, int]:
    """Test shim — delegates to CategorizationService.categorize_pending."""
    return CategorizationService(db).categorize_pending()


def get_categorization_stats(db: Database) -> dict[str, int | float]:
    """Test shim — delegates to CategorizationService.categorization_stats."""
    return CategorizationService(db).categorization_stats()


def get_active_categories(db: Database) -> list[dict[str, str | bool | None]]:
    """Test shim — delegates to CategorizationService.get_active_categories."""
    return CategorizationService(db).get_active_categories()


@pytest.fixture(autouse=True)
def _core_tables(db: Database) -> None:  # pyright: ignore[reportUnusedFunction]
    create_core_tables(db)


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
# OP_SCORES specificity helper
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_score_match_shape_oneof_and_exact_tie_at_10() -> None:
    assert score_match_shape("oneOf") == 10
    assert score_match_shape("exact") == 10


@pytest.mark.unit
def test_score_match_shape_contains_and_regex_score_zero() -> None:
    assert score_match_shape("contains") == 0
    assert score_match_shape("regex") == 0


@pytest.mark.unit
def test_score_match_shape_unknown_returns_zero() -> None:
    """An unknown match type defaults to lowest specificity (forward-compat)."""
    assert score_match_shape("nonexistent_type") == 0
    assert score_match_shape("") == 0


# ---------------------------------------------------------------------------
# amount_sign labelling
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_amount_sign_label_signs_real_amounts() -> None:
    assert _amount_sign_label(-12.34) == "-"
    assert _amount_sign_label(12.34) == "+"


@pytest.mark.unit
def test_amount_sign_label_zero_and_null_collapse_to_zero() -> None:
    """Zero and NULL must surface as ``"0"`` — not ``"+"``.

    Defaulting to ``"+"`` biases the LLM toward income-side categories on
    balance adjustments, voided rows, and rows with missing amounts (which
    are neither income nor expense).
    """
    assert _amount_sign_label(0) == "0"
    assert _amount_sign_label(0.0) == "0"
    assert _amount_sign_label(None) == "0"


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

    @pytest.mark.unit
    def test_exact_matches_description_when_memo_present(self, db: Database) -> None:
        r"""Regression: exact patterns must hit the field, not the concat.

        With ``match_text = description + "\n" + memo`` an exact pattern
        ``"STARBUCKS"`` would never equal ``"STARBUCKS\nREF123"``. The matcher
        compares each candidate text (match_text, normalized description,
        normalized memo) so user-authored exact patterns keep working when
        OFX/aggregator rows carry a non-empty memo.
        """
        create_merchant(db, "STARBUCKS", "Starbucks", match_type="exact")
        result = match_merchant(db, "STARBUCKS", memo="REF123")
        assert result is not None
        assert result["canonical_name"] == "Starbucks"

    @pytest.mark.unit
    def test_anchored_regex_matches_memo_when_description_unrelated(
        self, db: Database
    ) -> None:
        """Anchored regex on memo must hit even though description differs."""
        create_merchant(
            db,
            r"^GOOGLE\s+YOUTUBE$",
            "YouTube",
            match_type="regex",
            category="Entertainment",
        )
        result = match_merchant(db, "PAYPAL INST XFER", memo="GOOGLE YOUTUBE")
        assert result is not None
        assert result["canonical_name"] == "YouTube"


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
            INSERT INTO app.categorization_rules
              (rule_id, name, merchant_pattern, match_type,
               min_amount, max_amount, account_id,
               category, subcategory,
               priority, is_active, created_by,
               created_at, updated_at)
            VALUES
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
# fetch_uncategorized_rows fallback ([2])
# ---------------------------------------------------------------------------


class TestFetchUncategorizedRowsFallback:
    """fetch_uncategorized_rows includes entity-bearing rows even with blank text.

    Also degrades gracefully when the prep view lacks entity columns.
    """

    @pytest.mark.unit
    def test_entity_bearing_row_with_blank_text_is_included(self, db: Database) -> None:
        """Entity-bearing transaction with empty description and memo must be returned.

        Blank-text entity rows were previously excluded by the text-only WHERE,
        so apply_merchant_categories never saw them and rung-0/rung-4 never ran.
        The fix adds ``OR m.merchant_entity_id IS NOT NULL`` to the with_entity
        WHERE so these rows are always scanned.
        """
        from moneybin.services.categorization.matcher import CategorizationMatcher

        # Prep merged view WITH entity columns.
        db.execute("CREATE SCHEMA IF NOT EXISTS prep")
        db.execute("DROP TABLE IF EXISTS prep.int_transactions__merged")
        db.execute(
            "CREATE TABLE prep.int_transactions__merged ("
            "  transaction_id VARCHAR PRIMARY KEY, "
            "  merchant_entity_id VARCHAR, "
            "  merchant_entity_source_type VARCHAR, "
            "  merchant_name VARCHAR"
            ")"
        )
        # Entity-bearing row: merchant_entity_id set, description and memo both empty.
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXNE1', 'plaid_ent_123', 'plaid', NULL)"
        )
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXNE1', 'ACC1', '2025-06-01', -5.00, '', 'plaid')"
        )

        rows = CategorizationMatcher(db).fetch_uncategorized_rows()

        assert rows is not None, "must return rows, not None"
        txn_ids = [r[0] for r in rows]
        assert "TXNE1" in txn_ids, (
            "entity-bearing row with blank description must be included in "
            "fetch_uncategorized_rows (OR m.merchant_entity_id IS NOT NULL)"
        )
        # Confirm entity data is projected at position 5.
        row = next(r for r in rows if r[0] == "TXNE1")
        assert row[5] == "plaid_ent_123", "merchant_entity_id must be projected"

    @pytest.mark.unit
    def test_missing_entity_columns_falls_back(self, db: Database) -> None:
        """Prep view present but without merchant_entity_* columns → BinderException → fallback.

        Simulates a post-code-upgrade, pre-re-transform DB: the prep view exists
        but predates the M1T entity columns. The ``with_entity`` query raises a
        ``duckdb.BinderException`` (missing column, NOT a catalog error). The
        fetch must catch it and fall back to ``without_entity`` so categorize-run
        still returns the uncategorized rows instead of crashing.
        """
        from moneybin.services.categorization.matcher import CategorizationMatcher

        # prep view EXISTS but WITHOUT the M1T entity columns.
        db.execute("CREATE SCHEMA IF NOT EXISTS prep")
        db.execute("DROP TABLE IF EXISTS prep.int_transactions__merged")
        db.execute(
            "CREATE TABLE prep.int_transactions__merged (transaction_id VARCHAR)"
        )
        db.execute("INSERT INTO prep.int_transactions__merged VALUES ('TXNB1')")
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXNB1', 'ACC1', '2025-06-01', -5.00, 'COFFEE SHOP', 'ofx')"
        )

        rows = CategorizationMatcher(db).fetch_uncategorized_rows()

        assert rows is not None, "must degrade to without_entity, not raise/return None"
        row = next((r for r in rows if r[0] == "TXNB1"), None)
        assert row is not None, "uncategorized TXNB1 must be returned via fallback"
        # without_entity projects NULL for merchant_entity_id (pos 5) and
        # merchant_entity_source_type (pos 8).
        assert row[5] is None and row[8] is None


# ---------------------------------------------------------------------------
# _categorize_items_inner BinderException fallback ([4])
# ---------------------------------------------------------------------------


class TestCategorizationOrchestratorBinderFallback:
    """[4] _categorize_items_inner falls back when prep view lacks entity columns."""

    @pytest.mark.unit
    def test_binder_exception_caught_inner_not_outer(
        self, db: Database, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Prep view exists but lacks M1T entity columns → BinderException → inner fallback.

        Mirrors TestFetchUncategorizedRowsFallback: simulates a post-code-upgrade,
        pre-re-transform DB where the prep view predates the M1T entity columns.
        The ``with_entity`` query raises ``duckdb.BinderException`` (missing
        column). The inner ``except`` must catch it so ``txn_rows`` is populated
        from the ``without_entity`` fallback — not left empty by the outer guard.

        Proof: the outer catch emits "Could not batch-fetch transaction rows";
        that warning must NOT appear when the inner fallback handles it.
        """
        import logging

        db.execute("CREATE SCHEMA IF NOT EXISTS prep")
        db.execute("DROP TABLE IF EXISTS prep.int_transactions__merged")
        db.execute(
            "CREATE TABLE prep.int_transactions__merged (transaction_id VARCHAR)"
        )
        db.execute("INSERT INTO prep.int_transactions__merged VALUES ('TXN_BINDER')")
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_BINDER', 'ACC1', '2025-06-01', -5.00, 'COFFEE SHOP', 'ofx')"
        )

        svc = CategorizationService(db)
        with caplog.at_level(logging.WARNING):
            # Category validation is skipped when the categories view is empty
            # (the stub returns 0 rows → valid_category_set is falsy → skip).
            svc.categorize_items([
                CategorizationItem(
                    transaction_id="TXN_BINDER",
                    category="Food & Drink",
                )
            ])

        assert "Could not batch-fetch transaction rows" not in caplog.text, (
            "outer except must NOT fire — BinderException must be caught by "
            "the inner fallback so txn_rows is populated from without_entity"
        )


# ---------------------------------------------------------------------------
# Deterministic categorization pipeline
# ---------------------------------------------------------------------------


class TestCategorizePending:
    """Tests for the combined merchant + rules pipeline (`categorize_pending`)."""

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
        stats = categorize_pending(db)
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
        categorize_pending(db)
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
            ('FND-COF', 'Food & Drink', 'Coffee Shops', 'Coffee', 'expense')
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

    @pytest.mark.unit
    def test_view_exposes_accounting_class(self, db: Database) -> None:
        """get_active_categories() projects the `class` column end-to-end.

        The `class` accounting bucket is a shipped dict key on the payload;
        this asserts it survives the view → service → dict path with its seed
        value, not just that the column exists in the schema.
        """
        self._setup_seeds_and_view(db)
        by_id = {c["category_id"]: c for c in get_active_categories(db)}
        assert all("class" in c for c in by_id.values())
        assert by_id["FND-COF"]["class"] == "expense"


# ---------------------------------------------------------------------------
# CategorizationService facade
# ---------------------------------------------------------------------------


@pytest.fixture()
def real_db(db: Database) -> Database:
    """Real DB with core + app schema, used by service-facade tests."""
    return db


def test_service_categorize_items_applies_categorization(
    real_db: Database,
) -> None:
    """Service.categorize_items writes a category row for the given transaction."""
    real_db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('ts1', 'a1', DATE '2026-03-01', -3.00, 'STARBUCKS', 'csv')"
    )
    svc = CategorizationService(real_db)
    result = svc.categorize_items([
        CategorizationItem(transaction_id="ts1", category="Food & Drink")
    ])
    assert result.applied == 1


def test_categorize_items_returns_did_you_mean_on_invalid_category(
    real_db: Database,
) -> None:
    """categorize_items rejects an invalid category with a structured did_you_mean field."""
    real_db.execute(
        "INSERT INTO app.user_categories (category_id, category, subcategory) "
        "VALUES ('cat001', 'Food & Dining', NULL)"
    )
    svc = CategorizationService(real_db)
    result = svc.categorize_items([
        CategorizationItem(transaction_id="txn_dym", category="FOOD"),
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
    # (`commit` and `commit-from-file`) reads. Locking the contract.
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
    import moneybin.services.categorization as mod

    forbidden = {
        "categorize_items",
        "apply_rules",
        "seed_categories",
        "get_stats",
        "get_categorization_stats",
        "match_merchant",
        "apply_merchant_categories",
        "ensure_seed_table",
        "get_active_categories",
        "create_merchant",
        "categorize_pending",
    }
    leaked = {name for name in forbidden if hasattr(mod, name)}
    assert not leaked, f"These should be class methods only: {leaked}"


def test_service_exposes_consolidated_methods(real_db: Database) -> None:
    """CategorizationService exposes its core categorization surface.

    Auto-rule lifecycle methods now live on ``AutoRuleService`` and are
    asserted in ``test_auto_rule_service.py``.
    """
    expected = {
        "categorize_items",
        "apply_rules",
        "categorize_pending",
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


def test_categorize_items_creates_auto_rule_proposal(real_db: Database) -> None:
    """categorize_items records a pending proposal for novel txn → category mappings."""
    from moneybin.services.categorization import CategorizationService

    real_db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES ('tb1', 'a1', DATE '2026-02-01', -4.50, 'STARBUCKS RESERVE', 'csv')"
    )
    svc = CategorizationService(real_db)
    svc.categorize_items(
        [
            CategorizationItem(
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
# categorize_items — perf shape and in-batch dedup
# ---------------------------------------------------------------------------


def test_categorize_items_uses_constant_number_of_db_calls(
    monkeypatch: pytest.MonkeyPatch,
    db: Database,
) -> None:
    """categorize_items should not scale DB round-trips with item count.

    With N items, the number of read queries (description fetch + merchant
    fetch) must be O(1), not O(N).
    """
    from moneybin.tables import FCT_TRANSACTIONS

    # In production core.fct_transactions is a view over
    # prep.int_transactions__merged, so the batched description fetch joins prep
    # to read merchant_entity_id (M1T rung-0). Provide an (empty) prep view so
    # this perf test exercises the real single-query production path.
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__merged "
        "(transaction_id VARCHAR, merchant_entity_id VARCHAR, "
        "merchant_entity_source_type VARCHAR)"
    )

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
        CategorizationItem(
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

    result = CategorizationService(db).categorize_items(items)

    assert result.applied == 25
    # The categorize_items merchant-resolution read path must be batched.
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


def test_categorize_items_dedupes_merchant_creation_within_batch(
    db: Database,
) -> None:
    """Two items with the same description create exactly one merchant."""
    from moneybin.tables import FCT_TRANSACTIONS, MERCHANTS

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
        CategorizationItem(
            transaction_id=f"txn_{i}", category="Food", subcategory="Coffee"
        )
        for i in range(3)
    ]

    result = CategorizationService(db).categorize_items(items)

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


def test_categorize_items_snowball_fans_out_to_siblings(real_db: Database) -> None:
    """Snowball fan-out after categorize_items commits.

    After categorize_items commits, categorize_pending runs automatically and
    fans the new merchant out to siblings sharing the same match_text.

    Fixes bug 4 from categorization-matching-mechanics.md.
    """
    # Seed 3 transactions with identical description+memo signatures so the
    # exemplar created from t1 matches t2 and t3 on the snowball pass.
    for txn_id in ["t1", "t2", "t3"]:
        real_db.execute(
            """
            INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount,
             description, memo, source_type, is_transfer)
            VALUES (?, 'acct_test', '2026-05-10', -10.00,
                    'STARBUCKS', 'STORE 1234', 'ofx', false)
            """,  # noqa: S608  # test input, not executing SQL
            [txn_id],
        )

    svc = CategorizationService(real_db)
    assert svc.count_uncategorized() == 3

    # Categorize batch 1 (just t1) with a canonical name so an exemplar-merchant
    # is created.
    result = svc.categorize_items([
        CategorizationItem(
            transaction_id="t1",
            category="Food & Dining",
            subcategory="Coffee Shops",
            canonical_merchant_name="Starbucks",
        ),
    ])
    assert result.applied == 1

    # SNOWBALL: t2 and t3 should now be categorized too because categorize_items
    # invoked categorize_pending() after committing, which applied the new
    # exemplar to remaining uncategorized rows.
    assert svc.count_uncategorized() == 0

    rows = real_db.execute(
        "SELECT category, categorized_by FROM app.transaction_categories "
        "ORDER BY transaction_id"
    ).fetchall()
    assert len(rows) == 3
    assert all(r[0] == "Food & Dining" for r in rows)
    # t1 was categorized by the LLM-assist commit ('ai'). t2/t3 were
    # categorized by the snowball merchant fan-out, which stamps the 'rule'
    # method — the merchant's authoring provenance is NOT laundered onto the
    # categorization (categorization-source-model.md Decision 3, reverted). t1's
    # committed 'ai' row is not re-scanned by the snowball: only provider_native
    # rows are re-evaluated across runs, not committed 'ai' ones.
    assert rows[0][1] == "ai"
    assert rows[1][1] == "rule"
    assert rows[2][1] == "rule"


# ---------------------------------------------------------------------------
# find_matching_rule override tests (Task 3 — batch path preparation)
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
        txn_row_override=("AMZN MARKETPLACE", -42.0, "acct_1", None),
    )
    assert match is not None


# ---------------------------------------------------------------------------
# categorize_assist tests (Task 14 — RED)
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_with_uncategorized_txns(db: Database) -> Database:
    """Database seeded with 10 uncategorized transactions in core.fct_transactions."""
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
    from moneybin.services.categorization import (
        CategorizationService,
        RedactedTransaction,
    )

    svc = CategorizationService(db_with_uncategorized_txns)
    result = svc.categorize_assist(limit=10)

    assert all(isinstance(r, RedactedTransaction) for r in result)
    for r in result:
        assert hasattr(r, "transaction_id")
        assert hasattr(r, "description_redacted")
        assert hasattr(r, "source_type")
        # Confirm no amount/date/account fields
        assert not hasattr(r, "amount")
        assert not hasattr(r, "date")
        assert not hasattr(r, "account_id")


def test_categorize_assist_respects_limit(db_with_uncategorized_txns: Database) -> None:
    """categorize_assist returns no more rows than the requested limit."""
    from moneybin.services.categorization import CategorizationService

    svc = CategorizationService(db_with_uncategorized_txns)
    result = svc.categorize_assist(limit=5)
    assert len(result) <= 5


def test_categorize_assist_clamps_to_max_batch_size(
    db_with_uncategorized_txns: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Server enforces assist_max_batch_size hard ceiling."""
    from unittest.mock import MagicMock as _MagicMock

    from moneybin.services import categorization as _cs
    from moneybin.services.categorization import assist as _assist

    mock_settings = _MagicMock()
    mock_settings.categorization.assist_max_batch_size = 3
    monkeypatch.setattr(_assist, "get_settings", lambda: mock_settings)

    svc = _cs.CategorizationService(db_with_uncategorized_txns)
    result = svc.categorize_assist(limit=100)  # over the ceiling
    assert len(result) <= 3


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

        # Full-row capture (Req 4) — the audit records the complete resulting
        # row, not just the changed column subset.
        after_decoded = _json.loads(after)
        assert after_decoded["category"] == "Food"
        assert after_decoded["subcategory"] == "Coffee"
        assert after_decoded["categorized_by"] == "user"
        assert "transaction_id" in after_decoded  # full row, not a subset
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


# ---------------------------------------------------------------------------
# MERCHANT_RESOLUTION_OUTCOME_TOTAL counter (Finding #4)
# ---------------------------------------------------------------------------


class TestMerchantResolutionOutcomeMetric:
    """Spec-mandated MERCHANT_RESOLUTION_OUTCOME_TOTAL counter increments correctly.

    The spec requires one increment per resolved transaction, keyed by the
    resolution outcome (adopted | auto_bound | proposed | minted). The counter
    is incremented in _resolve_entity_merchant after the merchant_id is
    confirmed non-None (covers all four real outcomes).
    """

    @pytest.mark.unit
    def test_minted_outcome_increments_counter(self) -> None:
        """_resolve_entity_merchant increments the metric for the 'minted' outcome."""
        from unittest.mock import MagicMock

        from moneybin.metrics.registry import MERCHANT_RESOLUTION_OUTCOME_TOTAL
        from moneybin.services.categorization.orchestrator import (
            CategorizationOrchestrator,
        )
        from moneybin.services.merchant_resolver import MerchantResolution

        resolver = MagicMock()
        resolver.resolve.return_value = MerchantResolution(
            merchant_id="m_new", outcome="minted", created=True
        )
        before = MERCHANT_RESOLUTION_OUTCOME_TOTAL.labels(outcome="minted")._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals

        result = CategorizationOrchestrator._resolve_entity_merchant(  # pyright: ignore[reportPrivateUsage]
            resolver,
            {},
            MagicMock(),
            rejected=set(),
            pending=set(),
            merchant_entity_id="ent_m1",
            source_type="plaid",
            provider_merchant_name="Cafe",
            name_match=None,
            current_merchant_id=None,
        )

        after = MERCHANT_RESOLUTION_OUTCOME_TOTAL.labels(outcome="minted")._value.get()  # type: ignore[reportPrivateUsage]
        result_id, result_created = result
        assert result_id == "m_new"
        assert result_created is True, "minted outcome must set created=True"
        assert after == before + 1, "minted outcome must increment the counter"

    @pytest.mark.unit
    def test_proposed_outcome_increments_counter(self) -> None:
        """_resolve_entity_merchant increments the metric for the 'proposed' outcome."""
        from unittest.mock import MagicMock

        from moneybin.metrics.registry import MERCHANT_RESOLUTION_OUTCOME_TOTAL
        from moneybin.services.categorization.orchestrator import (
            CategorizationOrchestrator,
        )
        from moneybin.services.merchant_resolver import MerchantResolution

        resolver = MagicMock()
        resolver.resolve.return_value = MerchantResolution(
            merchant_id="m_prop", outcome="proposed"
        )
        before = MERCHANT_RESOLUTION_OUTCOME_TOTAL.labels(
            outcome="proposed"
        )._value.get()  # type: ignore[reportPrivateUsage]

        result = CategorizationOrchestrator._resolve_entity_merchant(  # pyright: ignore[reportPrivateUsage]
            resolver,
            {},
            MagicMock(),
            rejected=set(),
            pending=set(),
            merchant_entity_id="ent_p1",
            source_type="plaid",
            provider_merchant_name="Cafe",
            name_match=None,
            current_merchant_id=None,
        )

        after = MERCHANT_RESOLUTION_OUTCOME_TOTAL.labels(
            outcome="proposed"
        )._value.get()  # type: ignore[reportPrivateUsage]
        result_id, result_created = result
        assert result_id == "m_prop"
        assert result_created is False, "proposed outcome must not set created=True"
        assert after == before + 1, "proposed outcome must increment the counter"


class TestResolveEntityMerchantSourceTypeGuard:
    """_resolve_entity_merchant must not create a binding when source_type is absent.

    If the SQL-layer invariant breaks and merchant_entity_source_type arrives
    as None or "" the guard must short-circuit before resolver.resolve() is
    called — otherwise a binding is created under source_type="" which is
    silently invalid.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize("bad_source_type", [None, ""])
    def test_empty_source_type_returns_current_merchant_id_without_resolving(
        self, bad_source_type: str | None
    ) -> None:
        """Returns current_merchant_id unchanged; resolver.resolve() never called."""
        from unittest.mock import MagicMock

        from moneybin.services.categorization.orchestrator import (
            CategorizationOrchestrator,
        )

        resolver = MagicMock()
        current = "existing_merchant_id"

        result = CategorizationOrchestrator._resolve_entity_merchant(  # pyright: ignore[reportPrivateUsage]
            resolver,
            {},
            MagicMock(),
            rejected=set(),
            pending=set(),
            merchant_entity_id="ent_xyz",
            source_type=bad_source_type,
            provider_merchant_name="Some Merchant",
            name_match=None,
            current_merchant_id=current,
        )

        result_id, result_created = result
        assert result_id == current, (
            f"source_type={bad_source_type!r}: expected current_merchant_id unchanged"
        )
        assert result_created is False, (
            f"source_type={bad_source_type!r}: guard path must return created=False"
        )
        resolver.resolve.assert_not_called()


# ---------------------------------------------------------------------------
# apply_merchant_categories — entity resolution (Finding #6 + #7 regression)
# ---------------------------------------------------------------------------


def _setup_prep_table(db: Database) -> None:
    """Create ``prep.int_transactions__merged`` for entity-resolution tests.

    ``fetch_uncategorized_rows`` LEFT-JOINs this table for entity columns.
    Without it the query falls back to the ``without_entity`` path which
    projects NULL — making entity-resolution unreachable via
    ``apply_merchant_categories``.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute("DROP TABLE IF EXISTS prep.int_transactions__merged")
    db.execute(
        "CREATE TABLE prep.int_transactions__merged ("
        "  transaction_id VARCHAR PRIMARY KEY, "
        "  merchant_entity_id VARCHAR, "
        "  merchant_entity_source_type VARCHAR, "
        "  merchant_name VARCHAR"
        ")"
    )


class TestApplyMerchantCategoriesEntityResolution:
    """Tests for the #6 fix: resolver runs unconditionally in the deterministic pass.

    After the fix, a transaction bearing a ``merchant_entity_id`` that is
    already bound (rung-1 adopt) adopts its merchant's default category even
    when the transaction's description text matches no merchant pattern.
    """

    @pytest.mark.unit
    def test_entity_binding_adopts_category_without_text_match(
        self, db: Database
    ) -> None:
        """Entity-bound txn adopts the bound merchant's category with no text match.

        Spec Decision 3 rung-1: the *first* transaction resolves/mints a
        merchant and binds ``E``; every later transaction with ``E`` — even
        with different description text — hits rung 1 and lands on the same
        merchant. This test verifies the deterministic merchant pass honours
        that guarantee (previously the resolver was only called when a
        name-match was present and had a category).
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # Create a merchant with a category.
        mid = create_merchant(
            db,
            "FANCY_CORP",
            "Fancy Corp",
            match_type="exact",
            category="Business",
            subcategory="Services",
        )

        # Bind entity id "ent_r6a" → that merchant (rung-1 seed).
        MerchantLinksRepo(db).insert(
            link_id="lnk_r6a",
            merchant_id=mid,
            ref_kind="merchant_entity_id",
            ref_value="ent_r6a",
            source_type="plaid",
            decided_by="auto",
            actor="system",
            status="accepted",
        )

        # Transaction description does NOT match "FANCY_CORP" pattern.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R6A', 'ACC1', DATE '2026-01-01', -10.00, 'XYZUNKNOWN STORE', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R6A', 'ent_r6a', 'plaid', 'Fancy Corp')"
        )

        count = apply_merchant_categories(db)

        assert count == 1, "entity-adopted transaction must count as categorized"
        row = db.execute(
            "SELECT category, subcategory, merchant_id "
            "FROM app.transaction_categories WHERE transaction_id = 'TXN_R6A'"
        ).fetchone()
        assert row is not None, "a categorization row must be written"
        assert row[0] == "Business"
        assert row[1] == "Services"
        assert row[2] == mid

    @pytest.mark.unit
    def test_entity_bound_to_categoryless_merchant_skips_write(
        self, db: Database
    ) -> None:
        """Entity bound to a merchant with no default category → no write.

        A Plaid-minted merchant starts with ``category = NULL`` (spec
        Decision 8). Adopting such a merchant records identity but defers
        category to LLM/rules/Tier-2b. This test pins that no NULL-category
        row is inserted into ``app.transaction_categories``.
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # Merchant with no category (category=None default).
        mid = create_merchant(
            db,
            "NOCATCORP",
            "NoCat Corp",
            match_type="exact",
        )

        MerchantLinksRepo(db).insert(
            link_id="lnk_r6b",
            merchant_id=mid,
            ref_kind="merchant_entity_id",
            ref_value="ent_r6b",
            source_type="plaid",
            decided_by="auto",
            actor="system",
            status="accepted",
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R6B', 'ACC1', DATE '2026-01-01', -20.00, 'XYZUNKNOWN2 STORE', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R6B', 'ent_r6b', 'plaid', 'NoCat Corp')"
        )

        count = apply_merchant_categories(db)

        assert count == 0, "category-less entity adoption must NOT write a row"
        row = db.execute(
            "SELECT COUNT(*) FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_R6B'"
        ).fetchone()
        assert row is not None and row[0] == 0

    @pytest.mark.unit
    def test_name_match_categorizes_identically_to_before(self, db: Database) -> None:
        """Regression: a name-match-present row still categorizes as before.

        Ensures the restructure does not break the common path where a
        transaction's description matches a merchant pattern that has a
        category. The ``merchant_id`` written must equal the resolver's
        result (rung-1 adopt when a binding exists, or the name-matched
        merchant when no binding exists).
        """
        _setup_prep_table(db)

        mid = create_merchant(
            db,
            "AMZN",
            "Amazon",
            match_type="contains",
            category="Shopping",
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R6C', 'ACC1', DATE '2026-01-01', -25.00, 'AMZN MKTP ORDER #789', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R6C', NULL, NULL, NULL)"
        )

        count = apply_merchant_categories(db)

        assert count == 1
        row = db.execute(
            "SELECT category, merchant_id FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_R6C'"
        ).fetchone()
        assert row is not None
        assert row[0] == "Shopping"
        assert row[1] == mid

    @pytest.mark.unit
    def test_skip_txn_ids_still_wins(self, db: Database) -> None:
        """Regression: skip_txn_ids guard prevents categorization for matching ids.

        The rule-precedence guard (via ``skip_txn_ids``) must still suppress
        the merchant write for a transaction already handled by the rule pass,
        regardless of whether the transaction has an entity id.
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        mid = create_merchant(
            db,
            "STARBUCKS",
            "Starbucks",
            match_type="contains",
            category="Food & Drink",
        )

        MerchantLinksRepo(db).insert(
            link_id="lnk_r6d",
            merchant_id=mid,
            ref_kind="merchant_entity_id",
            ref_value="ent_r6d",
            source_type="plaid",
            decided_by="auto",
            actor="system",
            status="accepted",
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R6D', 'ACC1', DATE '2026-01-01', -5.00, 'SQ *STARBUCKS', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R6D', 'ent_r6d', 'plaid', 'Starbucks')"
        )

        # Pass TXN_R6D in skip set — simulates the rule pass having handled it.
        svc = CategorizationService(db)
        count = svc.apply_merchant_categories(skip_txn_ids={"TXN_R6D"})

        assert count == 0, "skip_txn_ids guard must suppress the write"
        row = db.execute(
            "SELECT COUNT(*) FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_R6D'"
        ).fetchone()
        assert row is not None and row[0] == 0

    @pytest.mark.unit
    def test_skip_txn_ids_entity_still_bound(self, db: Database) -> None:
        """skip_txn_ids suppresses the categorization write but NOT entity binding.

        Decision 7: a precedence skip (rule-pass already handled the row)
        must suppress only the categorization write, never the entity binding.
        After the fix, _resolve_entity_merchant runs ABOVE the skip guard so the
        entity id is bound in app.merchant_links even for rows in skip_txn_ids.

        Regression for the restructure in apply_merchant_categories: previously
        the entire loop body was skipped (including the resolver call), so a
        Plaid transaction categorized by a rule in the same categorize_pending
        call never got its merchant_entity_id bound.
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # Transaction with a merchant_entity_id that has NO pre-existing binding.
        # Resolver will hit rung-4 (no name match, no binding) → mint + auto-bind.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_D7B', 'ACC1', DATE '2026-01-01', -9.99, 'XYZNOPATTERN99', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_D7B', 'ent_d7b', 'plaid', 'Some Provider Merchant')"
        )

        svc = CategorizationService(db)
        count = svc.apply_merchant_categories(skip_txn_ids={"TXN_D7B"})

        # Categorization write must be suppressed.
        assert count == 0, "skip_txn_ids must suppress the categorization write"
        cat_row = db.execute(
            "SELECT COUNT(*) FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_D7B'"
        ).fetchone()
        assert cat_row is not None and cat_row[0] == 0, (
            "no categorization row must be written for a skip_txn_ids row"
        )

        # Entity binding must have been written (resolver ran above the skip guard).
        bound_mid = MerchantLinksRepo(db).lookup("plaid", "ent_d7b")
        assert bound_mid is not None, (
            "entity id must be bound in app.merchant_links even when txn is in skip_txn_ids"
        )

    @pytest.mark.unit
    def test_adopted_entity_merchant_category_wins_over_name_match(
        self, db: Database
    ) -> None:
        """Rung-1 adopted merchant's category wins over a disagreeing name match.

        When an entity id is bound to merchant A (category C_A) but the
        transaction text matches a DIFFERENT merchant B (category C_B), the
        spec says rung-1 "skip name matching" — the binding is the identity
        source of truth and A's category must win. Bug #9: previously the
        block wrote B's category with A's merchant_id (inconsistent row).
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # Merchant A — the entity is bound to this one (category C_A = "Business").
        mid_a = create_merchant(
            db,
            "CORP_A_UNIQUE_PATTERN",
            "Corp A",
            match_type="exact",
            category="Business",
            subcategory="Services",
        )

        # Merchant B — the transaction text matches this one (category C_B = "Shopping").
        _mid_b = create_merchant(
            db,
            "SHOPSTORE",
            "Shop Store",
            match_type="contains",
            category="Shopping",
            subcategory="General",
        )

        # Bind entity "ent_r9" → merchant A.
        MerchantLinksRepo(db).insert(
            link_id="lnk_r9a",
            merchant_id=mid_a,
            ref_kind="merchant_entity_id",
            ref_value="ent_r9",
            source_type="plaid",
            decided_by="auto",
            actor="system",
            status="accepted",
        )

        # Transaction description matches "SHOPSTORE" (merchant B), not merchant A.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R9A', 'ACC1', DATE '2026-01-01', -30.00, 'SHOPSTORE PURCHASE', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R9A', 'ent_r9', 'plaid', 'Corp A')"
        )

        count = apply_merchant_categories(db)

        assert count == 1, "entity-adopted transaction must be categorized"
        row = db.execute(
            "SELECT category, subcategory, merchant_id "
            "FROM app.transaction_categories WHERE transaction_id = 'TXN_R9A'"
        ).fetchone()
        assert row is not None, "categorization row must be written"
        assert row[0] == "Business", (
            f"adopted merchant A's category must win, got {row[0]!r}"
        )
        assert row[1] == "Services", (
            f"adopted merchant A's subcategory must win, got {row[1]!r}"
        )
        assert row[2] == mid_a, (
            f"merchant_id must be A ({mid_a!r}), not name-match B; got {row[2]!r}"
        )

    @pytest.mark.unit
    def test_name_match_category_wins_when_entity_merchant_has_no_category(
        self, db: Database
    ) -> None:
        """Name-match category is used when the adopted merchant has no default category.

        When an entity id is bound to merchant A (no category, e.g. Plaid-
        minted) but the transaction text matches merchant B (category C_B),
        the name match's category must fill in as the fallback. This pins
        the fallback path in the #9 fix.
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # Merchant A — entity bound here, NO category.
        mid_a_nocat = create_merchant(
            db,
            "NOCAT_ENTITY_CORP",
            "NoCat Entity Corp",
            match_type="exact",
        )

        # Merchant B — name match has a category.
        _mid_b2 = create_merchant(
            db,
            "FALLBACKSTORE",
            "Fallback Store",
            match_type="contains",
            category="Food & Drink",
            subcategory="Restaurants",
        )

        MerchantLinksRepo(db).insert(
            link_id="lnk_r9b",
            merchant_id=mid_a_nocat,
            ref_kind="merchant_entity_id",
            ref_value="ent_r9b",
            source_type="plaid",
            decided_by="auto",
            actor="system",
            status="accepted",
        )

        # Description matches "FALLBACKSTORE" (merchant B with category).
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R9B', 'ACC1', DATE '2026-01-01', -12.00, 'FALLBACKSTORE MEAL', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R9B', 'ent_r9b', 'plaid', 'NoCat Entity Corp')"
        )

        count = apply_merchant_categories(db)

        assert count == 1, "fallback-to-name-match path must categorize the transaction"
        row = db.execute(
            "SELECT category, subcategory, merchant_id "
            "FROM app.transaction_categories WHERE transaction_id = 'TXN_R9B'"
        ).fetchone()
        assert row is not None
        assert row[0] == "Food & Drink", (
            f"name-match fallback category must be used; got {row[0]!r}"
        )
        assert row[2] == mid_a_nocat, (
            "merchant_id must still be the adopted entity merchant A"
        )

    @pytest.mark.unit
    def test_empty_merchant_catalog_still_runs_entity_resolution(
        self, db: Database
    ) -> None:
        """Empty merchant catalog ([] not None) still runs entity resolution (rung-4).

        Bug #11: `if not merchants` exited on both [] (empty list) and None
        (catalog table absent). An empty list means the table exists but no
        merchants have been authored yet. A transaction carrying a
        merchant_entity_id with no name match must reach rung-4 and create a
        binding in app.merchant_links even before any merchant is authored.
        Assert the BINDING, not a category row — the minted merchant starts
        with category=None so no categorization is written.
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # No merchant created — fetch_merchants() returns [] from the empty table.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_11A', 'ACC1', DATE '2026-01-01', -10.00, 'XYZUNKNOWN11', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_11A', 'ent_11a', 'plaid', 'Some New Merchant')"
        )

        apply_merchant_categories(db)

        bound_mid = MerchantLinksRepo(db).lookup("plaid", "ent_11a")
        assert bound_mid is not None, (
            "entity id must be bound after rung-4 mint even when merchant catalog is empty"
        )

    @pytest.mark.unit
    def test_absent_merchant_catalog_returns_zero(self, db: Database) -> None:
        """Catalog table absent (fetch_merchants → None) → returns 0 immediately.

        Preserved behavior: when the merchant table does not exist at all,
        apply_merchant_categories bails early and returns 0. The None guard
        replaces the old `if not merchants` check so this path is still covered.
        """
        from unittest.mock import patch

        from moneybin.services.categorization.matcher import CategorizationMatcher

        _setup_prep_table(db)

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_11B', 'ACC1', DATE '2026-01-01', -10.00, 'XYZUNKNOWN11B', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_11B', 'ent_11b', 'plaid', 'Some Merchant 11B')"
        )

        with patch.object(CategorizationMatcher, "fetch_merchants", return_value=None):
            result = apply_merchant_categories(db)

        assert result == 0, (
            "absent catalog table (fetch_merchants → None) must return 0"
        )


# ---------------------------------------------------------------------------
# Merchant matches stamp the 'rule' method regardless of the merchant's
# authoring provenance. (Task 3's provenance-aware stamping was reverted — see
# categorization-source-model.md Decision 3: stamping provenance leaked
# machine-applied merchant matches into the auto-rule override-detection query,
# which counts 'user'/'ai' as human corrections, and its stated benefit could
# never fire because provider_native never overrides an existing row.)
# ---------------------------------------------------------------------------


class TestApplyMerchantCategoriesStampsRule:
    """A merchant default stamps the 'rule' method, never the merchant's provenance.

    So a machine-applied merchant match is never miscounted as a human
    correction by auto-rule override detection.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize("merchant_created_by", ["ai", "user"])
    def test_merchant_default_stamps_rule(
        self, db: Database, merchant_created_by: str
    ) -> None:
        """Whatever created the merchant, its default stamps categorized_by='rule'."""
        _setup_prep_table(db)

        mid = create_merchant(
            db,
            "AMAZON",
            "Amazon",
            match_type="contains",
            category="Shopping",
            created_by=merchant_created_by,
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_PROV', 'ACC1', DATE '2026-01-01', -30.00, 'AMAZON MKTP ORDER', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_PROV', NULL, NULL, NULL)"
        )

        count = apply_merchant_categories(db)

        assert count == 1
        row = db.execute(
            "SELECT categorized_by, merchant_id FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_PROV'"
        ).fetchone()
        assert row is not None
        assert row[0] == "rule"
        assert row[1] == mid


# ---------------------------------------------------------------------------
# #7 regression: resolver binding written even when categorization is
# precedence-skipped
# ---------------------------------------------------------------------------


class TestResolverBindingPrecedesCategorization:
    """Pins the by-design ordering: resolver write precedes write_categorization.

    The binding to ``app.merchant_links`` (or a proposal to
    ``app.merchant_link_decisions``) is an entity-keyed fact committed
    BEFORE the precedence-guarded ``write_categorization`` call.  A
    precedence skip suppresses only the categorization — never the binding.

    Test driver: ``categorize_items`` (LLM-assist path), where the ordering
    is present in ``_categorize_items_inner``.  That path accepts an explicit
    item list and writes with ``categorized_by='ai'``, which a pre-existing
    ``user`` categorization outranks — producing a controlled
    ``written=False`` outcome. ``categorize_pending``/``apply_merchant_categories``
    cannot drive this scenario instead: ``fetch_uncategorized_rows``'s "pending"
    filter (``c.transaction_id IS NULL OR c.categorized_by = 'provider_native'``)
    excludes an already-``user``-categorized row from the scan entirely, so the
    resolver never runs for it via that path — ``categorize_items`` is the only
    entry point that attempts (and gets precedence-blocked on) a write for a row
    that already carries a higher-priority categorization.

    Covers two rungs of the same invariant: rung 3 (fuzzy name match — pending
    proposal) below, and rung 4 (no name match — mint + bind a new merchant) in
    ``test_novel_entity_mints_merchant_even_when_write_precedence_skipped``.
    """

    @pytest.mark.unit
    def test_binding_written_when_categorization_precedence_skipped(
        self, db: Database
    ) -> None:
        """Resolver writes the proposal before write_categorization is called.

        Even when a ``user`` category already exists (so the ``ai`` write is
        blocked by the precedence guard), the resolver must have proposed the
        entity-id → merchant candidate in ``app.merchant_link_decisions``.
        """
        _setup_prep_table(db)

        create_merchant(
            db,
            "STARBUCKS",
            "Starbucks",
            match_type="contains",
            category="Food & Drink",
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_R7', 'ACC1', DATE '2026-01-01', -4.50, 'STARBUCKS RESERVE', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R7', 'ent_r7', 'plaid', 'Starbucks')"
        )

        # Pre-seed a user categorization so write_categorization returns
        # written=False (user > ai in the precedence ladder).
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, categorized_at, categorized_by) "
            "VALUES ('TXN_R7', 'Entertainment', CURRENT_TIMESTAMP, 'user')"
        )

        svc = CategorizationService(db)
        result = svc.categorize_items([
            CategorizationItem(transaction_id="TXN_R7", category="Food & Drink")
        ])

        # The ai write must have been blocked (applied=0).
        assert result.applied == 0, "user category must block the ai write"
        # The user category must be intact (not overwritten).
        cat_row = db.execute(
            "SELECT categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_R7'"
        ).fetchone()
        assert cat_row is not None and cat_row[0] == "user"

        # The resolver must have proposed a merchant-link decision for ent_r7
        # (rung-3: fuzzy "STARBUCKS contains" match → pending proposal).
        # This is the binding write that must have happened BEFORE write_categorization.
        pending = db.execute(
            "SELECT COUNT(*) FROM app.merchant_link_decisions "
            "WHERE ref_value = 'ent_r7' AND status = 'pending'"
        ).fetchone()
        assert pending is not None and pending[0] >= 1, (
            "resolver must have written a pending proposal for ent_r7 "
            "even though categorization was precedence-skipped"
        )

    @pytest.mark.unit
    def test_novel_entity_mints_merchant_even_when_write_precedence_skipped(
        self, db: Database
    ) -> None:
        """Rung-4 mint fires even when the categorization write is precedence-skipped.

        Characterization test: a stronger case than
        ``test_binding_written_when_categorization_precedence_skipped`` above.
        Here the entity id has no name match at all (empty merchant catalog),
        so the resolver mints a brand-new ``created_by='plaid'`` merchant
        (rung 4) rather than proposing against an existing candidate (rung 3).
        The mint is a Plaid-asserted identity fact, committed regardless of
        whether this row's categorization write lands (spec Decision 7) — this
        test locks that correct-by-design behavior so a future "fix" can't
        silently gate the mint behind ``outcome.written``.
        """
        _setup_prep_table(db)

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_MINT_SKIP', 'ACC1', DATE '2026-01-01', -12.00, "
            "'XYZNOPATTERNMINTSKIP', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_MINT_SKIP', 'ent_mint_skip', 'plaid', 'Brand New Merchant')"
        )

        # Pre-seed a user categorization (priority 1) so the ai write below
        # (priority 7) is blocked by the precedence guard.
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, categorized_at, categorized_by) "
            "VALUES ('TXN_MINT_SKIP', 'Travel', CURRENT_TIMESTAMP, 'user')"
        )

        svc = CategorizationService(db)
        result = svc.categorize_items([
            CategorizationItem(transaction_id="TXN_MINT_SKIP", category="Food & Drink")
        ])

        # The ai write must have been blocked; the user category stays intact.
        assert result.applied == 0, "user category must block the ai write"
        cat_row = db.execute(
            "SELECT category, categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_MINT_SKIP'"
        ).fetchone()
        assert cat_row is not None
        assert cat_row[0] == "Travel"
        assert cat_row[1] == "user"

        # The novel entity id must still have been minted + bound (rung 4),
        # even though the categorization write above was rejected.
        link_row = db.execute(
            "SELECT merchant_id FROM app.merchant_links "
            "WHERE ref_value = 'ent_mint_skip' AND source_type = 'plaid' "
            "AND status = 'accepted'"
        ).fetchone()
        assert link_row is not None, (
            "resolver must mint+bind a merchant for a novel entity id even "
            "though categorization was precedence-skipped"
        )
        merchant_row = db.execute(
            "SELECT created_by, category_id FROM app.user_merchants WHERE merchant_id = ?",
            [link_row[0]],
        ).fetchone()
        assert merchant_row is not None
        assert merchant_row[0] == "plaid"
        assert merchant_row[1] is None, (
            "plaid-minted merchant is inert/category-free by design (M1T)"
        )


# ---------------------------------------------------------------------------
# categorize_items — rung-4 entity mint counted in merchants_created (M1T)
# ---------------------------------------------------------------------------


class TestCategorizeItemsMerchantCreated:
    """categorize_items must count rung-4 entity mints in CategorizationResult.merchants_created.

    When a transaction carries a merchant_entity_id with no pre-existing
    binding, the resolver mints a new merchant (rung-4) and the returned
    CategorizationResult must reflect that mint in merchants_created.

    Regression: _resolve_entity_merchant previously discarded
    MerchantResolution.created, so merchants_created stayed 0 even when a new
    Plaid merchant was minted during the AI categorization path.
    """

    @pytest.mark.unit
    def test_rung4_mint_increments_merchants_created(self, db: Database) -> None:
        """categorize_items counts a rung-4 entity mint in merchants_created.

        A transaction with a merchant_entity_id that has no pre-existing
        binding and no name match triggers rung-4 (mint). The returned
        CategorizationResult.merchants_created must be >= 1.
        """
        _setup_prep_table(db)

        # Transaction with no name-matchable description — forces rung-4 mint.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_MINT_1', 'ACC1', DATE '2026-01-01', -12.50, 'XYZNOPATTERNMINT1', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_MINT_1', 'ent_mint_1', 'plaid', 'Chipotle')"
        )

        svc = CategorizationService(db)
        result = svc.categorize_items([
            CategorizationItem(transaction_id="TXN_MINT_1", category="Food & Drink")
        ])

        assert result.merchants_created >= 1, (
            "rung-4 entity mint must increment merchants_created; "
            f"got {result.merchants_created}"
        )

    @pytest.mark.unit
    def test_rung1_adopt_does_not_increment_merchants_created(
        self, db: Database
    ) -> None:
        """categorize_items does NOT count a rung-1 adopt in merchants_created.

        Control: when the entity id is already bound (rung-1 adopt),
        MerchantResolution.created is False and merchants_created must stay 0.
        """
        from moneybin.repositories.merchant_links_repo import MerchantLinksRepo

        _setup_prep_table(db)

        # Create a merchant and pre-bind an entity id to it (rung-1 seed).
        mid = create_merchant(
            db,
            "BOUND_CORP",
            "Bound Corp",
            match_type="exact",
            category="Business",
        )
        MerchantLinksRepo(db).insert(
            link_id="lnk_ctrl1",
            merchant_id=mid,
            ref_kind="merchant_entity_id",
            ref_value="ent_ctrl_1",
            source_type="plaid",
            decided_by="auto",
            actor="system",
            status="accepted",
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, source_type) "
            "VALUES ('TXN_CTRL_1', 'ACC1', DATE '2026-01-01', -5.00, 'XYZNOPATTERNCTRL', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_CTRL_1', 'ent_ctrl_1', 'plaid', 'Bound Corp')"
        )

        svc = CategorizationService(db)
        result = svc.categorize_items([
            CategorizationItem(transaction_id="TXN_CTRL_1", category="Business")
        ])

        assert result.merchants_created == 0, (
            "rung-1 adopt must NOT increment merchants_created; "
            f"got {result.merchants_created}"
        )


class TestMerchantNameMatchRung2:
    """Spec Decision 3 rung 2: provider merchant_name exact match → auto-bind.

    Both categorization paths (apply_merchant_categories and categorize_items)
    must pass ``merchant_name`` to ``match_merchant_with_name`` so a Plaid row
    with a blank/noisy description but a clean ``merchant_name`` that exactly
    matches an existing merchant is auto-bound (rung 2) instead of minting a
    duplicate (rung 4).
    """

    @pytest.mark.unit
    def test_rung2_auto_bind_via_exact_merchant_name(self, db: Database) -> None:
        """Blank description + exact merchant_name → auto-bind to existing merchant.

        Before the fix: ``match_merchant_with_name`` didn't exist; both call
        sites used ``build_match_inputs + match_merchants`` on description/memo
        only, so a blank description produced no name match and rung-4 minted a
        duplicate. This test must FAIL on old code and PASS after the fix.
        """
        _setup_prep_table(db)

        # Existing "Starbucks" merchant with an exact pattern.
        mid = create_merchant(
            db,
            "Starbucks",
            "Starbucks",
            match_type="exact",
            category="Food & Drink",
        )

        # Transaction: blank description, merchant_name="Starbucks", entity id present.
        # merchant_name must be in core.fct_transactions because fetch_uncategorized_rows
        # selects t.merchant_name (not m.merchant_name from the prep table).
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "merchant_name, source_type) "
            "VALUES ('TXN_R2_EXACT', 'ACC1', DATE '2026-01-01', -5.75, '', 'Starbucks', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R2_EXACT', 'ent_r2_exact', 'plaid', 'Starbucks')"
        )

        apply_merchant_categories(db)

        # The entity must be bound to the EXISTING Starbucks merchant (rung-2 auto-bind).
        row = db.execute(
            "SELECT merchant_id FROM app.merchant_links "
            "WHERE ref_value = ? AND status = 'accepted'",
            ["ent_r2_exact"],
        ).fetchone()
        assert row is not None, "entity must be bound after rung-2 auto-bind"
        assert row[0] == mid, (
            f"entity must bind to EXISTING merchant {mid!r}, not a duplicate; "
            f"got {row[0]!r}"
        )

        # No duplicate merchant should be minted.
        count_row = db.execute("SELECT COUNT(*) FROM app.user_merchants").fetchone()
        assert count_row is not None
        count = count_row[0]
        assert count == 1, f"expected 1 merchant (no duplicate), got {count}"

    @pytest.mark.unit
    def test_rung3_fuzzy_merchant_name_not_auto_bound(self, db: Database) -> None:
        """Fuzzy merchant_name match → rung-3 propose, NOT auto-bound.

        Guards against over-eager binding: a ``contains`` merchant whose
        pattern matches the provider ``merchant_name`` must route to rung-3
        (pending proposal), never rung-2 (auto-bind). Only EXACT name hits
        earn auto-bind.
        """
        _setup_prep_table(db)

        # Merchant matched via "contains" — a fuzzy shape.
        mid_fuzzy = create_merchant(
            db,
            "starbucks",
            "Starbucks Inc",
            match_type="contains",
            category="Food & Drink",
        )

        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "merchant_name, source_type) "
            "VALUES ('TXN_R3_FUZZY', 'ACC1', DATE '2026-01-01', -4.50, '', 'Starbucks', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_R3_FUZZY', 'ent_r3_fuzzy', 'plaid', 'Starbucks')"
        )

        apply_merchant_categories(db)

        # The entity must NOT be auto-bound to mid_fuzzy (fuzzy → rung 3 propose).
        row = db.execute(
            "SELECT merchant_id FROM app.merchant_links "
            "WHERE ref_value = ? AND status = 'accepted'",
            ["ent_r3_fuzzy"],
        ).fetchone()
        assert row is None or row[0] != mid_fuzzy, (
            "fuzzy merchant_name match must NOT auto-bind to the existing merchant; "
            "rung 3 should only propose"
        )

    @pytest.mark.unit
    def test_exact_description_match_wins_over_merchant_name(
        self, db: Database
    ) -> None:
        """Exact description match is used even when merchant_name would match a different merchant.

        Control: the normal description-match path must not regress. When a
        transaction's description exactly matches merchant A, the resolver
        must NOT bind to merchant B even if ``merchant_name`` would match B.
        """
        _setup_prep_table(db)

        mid_a = create_merchant(
            db,
            "FANCY_CORP",
            "Fancy Corp",
            match_type="exact",
            category="Business",
        )
        _mid_b = create_merchant(
            db,
            "OTHERCORP",
            "Other Corp",
            match_type="exact",
            category="Shopping",
        )

        # Description exactly matches merchant A; merchant_name matches merchant B.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "merchant_name, source_type) "
            "VALUES ('TXN_DESC_CTRL', 'ACC1', DATE '2026-01-01', -20.00, 'FANCY_CORP', "
            "'OTHERCORP', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_DESC_CTRL', 'ent_desc_ctrl', 'plaid', 'OTHERCORP')"
        )

        apply_merchant_categories(db)

        cat_row = db.execute(
            "SELECT merchant_id FROM app.transaction_categories "
            "WHERE transaction_id = 'TXN_DESC_CTRL'"
        ).fetchone()
        assert cat_row is not None, "transaction must be categorized"
        assert cat_row[0] == mid_a, (
            f"exact description match must win; expected merchant {mid_a!r}, "
            f"got {cat_row[0]!r}"
        )

    @pytest.mark.unit
    def test_unknown_merchant_name_still_mints(self, db: Database) -> None:
        """Blank description + merchant_name matching nothing → rung-4 mint.

        Control: when no merchant catalog entry matches the provider
        ``merchant_name``, rung-4 minting must still fire and produce a new
        merchant (preserving current behaviour).
        """
        _setup_prep_table(db)

        # No merchants in catalog.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "merchant_name, source_type) "
            "VALUES ('TXN_MINT_CTRL', 'ACC1', DATE '2026-01-01', -9.99, '', "
            "'SomeUnknownBrand', 'plaid')"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__merged VALUES "
            "('TXN_MINT_CTRL', 'ent_mint_ctrl', 'plaid', 'SomeUnknownBrand')"
        )

        apply_merchant_categories(db)

        count_row = db.execute("SELECT COUNT(*) FROM app.user_merchants").fetchone()
        assert count_row is not None
        count = count_row[0]
        assert count == 1, (
            f"unknown merchant_name must mint a new merchant (rung 4); got {count}"
        )


# ---------------------------------------------------------------------------
# apply_plaid_categories — Plaid PFC-detailed categorizer (Task 5)
# ---------------------------------------------------------------------------


def _insert_plaid_txn(
    db: Database,
    transaction_id: str,
    *,
    category_detailed: str | None,
    plaid_category: str | None,
    category_confidence: str,
) -> None:
    """Insert one Plaid transaction into the two prep layers categorizer tests read.

    ``apply_plaid_categories`` reads the gold-keyed merged layer, not
    ``prep.stg_plaid__transactions`` (the pre-merge, native-Plaid-id
    layer) — ``app.transaction_categories`` and every join onto it
    (including ``core.fct_transactions``'s own categorization join) are
    keyed by the gold ``transaction_id`` that ``int_transactions__matched``
    mints, so a native-id write would silently orphan itself (the ship
    bug this fixture change closes; see
    ``tests/moneybin/test_categorize_plaid_e2e.py`` for the full-pipeline
    proof). ``transaction_id`` here IS that gold id — callers pass the
    same value used for the matching ``core.fct_transactions`` row.

    ``prep.int_transactions__merged`` is a SQLMesh VIEW in production —
    building it in a unit-test DB would require a full SQLMesh plan (see
    the e2e test above, which does exactly that). ``apply_plaid_categories``
    reads four columns (the detailed AND primary PFC codes, for the
    two-tier bridge lookup, plus confidence), so this creates just those
    columns as a physical table — mirroring the ``prep.int_transactions__merged``
    precedent already used above for entity-resolution tests (see
    ``_setup_prep_table``).

    The same transaction also exists in the per-source staging layer,
    ``prep.stg_plaid__transactions``, which the ``plaid_unmapped`` coverage
    stat scans (lighter than the merged pipeline, and the natural
    per-Plaid-transaction grain). A matching stub with just the two PFC-code
    columns is seeded so that stat sees the transaction too.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__merged ("
        "  transaction_id VARCHAR PRIMARY KEY, "
        "  category_detailed VARCHAR, "
        "  plaid_category VARCHAR, "
        "  category_confidence VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO prep.int_transactions__merged "
        "(transaction_id, category_detailed, plaid_category, category_confidence) "
        "VALUES (?, ?, ?, ?)",
        [transaction_id, category_detailed, plaid_category, category_confidence],
    )
    # Per-source staging stub for the plaid_unmapped coverage stat.
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.stg_plaid__transactions ("
        "  transaction_id VARCHAR, "
        "  category_detailed VARCHAR, "
        "  plaid_category VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO prep.stg_plaid__transactions "
        "(transaction_id, category_detailed, plaid_category) "
        "VALUES (?, ?, ?)",
        [transaction_id, category_detailed, plaid_category],
    )


def _seed_bridge_mapping(
    db: Database,
    *,
    source_category_code: str,
    code_level: str,
    category_id: str,
    category: str,
    subcategory: str | None,
) -> None:
    """Seed one core.bridge_category_source_map row, real mechanism.

    Inserts into ``seeds.category_source_map`` (the bridge's seed tier —
    mirrors ``test_bridge_category_source_map.py``'s ``_insert_seed_row``)
    plus a matching ``seeds.categories`` row so
    ``core.dim_categories``/``core.bridge_category_source_map`` — both real
    views, not stubs — resolve ``category_id`` to a ``(category,
    subcategory)`` pair the same way production does. Callers must call
    ``refresh_views(db)`` first so the seed tables exist.

    Named-column insert (not positional) on purpose: the bootstrap
    ``seeds.categories`` table (``moneybin.seeds._ensure_seed_tables_exist``)
    still carries its historical 5th column (kept for V014 migration replay,
    unrelated to this bridge), so this leaves it at its column default
    (``NULL``) rather than assuming a name or position for a column this
    test doesn't exercise.
    """
    db.execute(
        "INSERT INTO seeds.category_source_map "
        "(source_type, source_category_code, code_level, category_id, "
        "source_taxonomy_version) VALUES ('plaid', ?, ?, ?, 'plaid_pfc_v2')",
        [source_category_code, code_level, category_id],
    )
    db.execute(
        "INSERT INTO seeds.categories (category_id, category, subcategory, description) "
        "VALUES (?, ?, ?, 'test category')",
        [category_id, category, subcategory],
    )


class TestApplyPlaidCategories:
    """Tests for the two-tier Plaid PFC bridge categorizer.

    Exercises the real ``core.bridge_category_source_map`` reverse lookup
    (detailed-preferred, primary-fallback) rather than the retired
    ``dim_categories.plaid_detailed`` column.
    """

    @pytest.mark.unit
    def test_detailed_match_assigns_category(self, db: Database) -> None:
        """A HIGH-confidence txn whose detailed code is bridge-mapped adopts it."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t1",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )

        n = apply_plaid_categories(db)

        assert n == 1
        row = db.execute(
            "SELECT category_id, categorized_by, source_type, confidence "
            "FROM app.transaction_categories WHERE transaction_id='t1'"
        ).fetchone()
        assert row == ("FND-COF", "provider_native", "plaid", Decimal("0.90"))

    @pytest.mark.unit
    def test_detailed_wins_over_primary_no_fanout(self, db: Database) -> None:
        """Two-tier: when both codes are bridge-mapped, detailed wins, exactly once.

        Regression test for the bug this rebuild fixes: without the QUALIFY
        dedup, a transaction whose detailed AND primary codes both resolve in
        the bridge would fan out into two writes for the same transaction
        (non-deterministic final category, inflated count).
        """
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_FAST_FOOD",
            code_level="detailed",
            category_id="FND-FST",
            category="Food & Drink",
            subcategory="Fast Food",
        )
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK",
            code_level="primary",
            category_id="FND",
            category="Food & Drink",
            subcategory=None,
        )
        _insert_plaid_txn(
            db,
            "t2",
            category_detailed="FOOD_AND_DRINK_FAST_FOOD",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )

        n = apply_plaid_categories(db)

        assert n == 1, "detailed+primary both matching must write once, not fan out"
        row = db.execute(
            "SELECT category_id FROM app.transaction_categories WHERE transaction_id='t2'"
        ).fetchone()
        assert row == ("FND-FST",)

    @pytest.mark.unit
    def test_primary_fallback_when_detailed_unmapped(self, db: Database) -> None:
        """When the detailed code has no bridge row, the primary code's mapping wins."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="TRANSPORTATION",
            code_level="primary",
            category_id="TRP",
            category="Transportation",
            subcategory=None,
        )
        _insert_plaid_txn(
            db,
            "t3",
            # Intentionally NOT mapped in the bridge — only the primary
            # TRANSPORTATION code is seeded above.
            category_detailed="TRANSPORTATION_BIKES_AND_SCOOTERS",
            plaid_category="TRANSPORTATION",
            category_confidence="HIGH",
        )

        n = apply_plaid_categories(db)

        assert n == 1
        row = db.execute(
            "SELECT category_id FROM app.transaction_categories WHERE transaction_id='t3'"
        ).fetchone()
        assert row == ("TRP",)

    @pytest.mark.unit
    def test_skips_low_confidence(self, db: Database) -> None:
        """A LOW confidence Plaid txn is not categorized, even with a bridge match."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t4",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="LOW",
        )

        n = apply_plaid_categories(db)

        assert n == 0
        assert (
            db.execute(
                "SELECT 1 FROM app.transaction_categories WHERE transaction_id='t4'"
            ).fetchone()
            is None
        )


# ---------------------------------------------------------------------------
# Plaid categorizer observability — counters, by_provider_native, coverage
# gap (Task 9, Tier-2b — deferred from the category-source-map bridge PR)
# ---------------------------------------------------------------------------


class TestPlaidCategorizerObservability:
    """Prometheus counters + stats surfaces for the Plaid PFC categorizer."""

    @pytest.mark.unit
    def test_by_provider_native_appears_after_plaid_write(self, db: Database) -> None:
        """A provider_native write surfaces via stats()['by_provider_native']."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t1",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )

        n = apply_plaid_categories(db)

        assert n == 1
        stats = get_categorization_stats(db)
        assert stats["by_provider_native"] == 1

    @pytest.mark.unit
    def test_write_increments_provider_native_counter(self, db: Database) -> None:
        """A successful plaid write increments CATEGORIZE_PROVIDER_NATIVE_TOTAL."""
        from moneybin.metrics.registry import CATEGORIZE_PROVIDER_NATIVE_TOTAL

        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t1",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )
        before = CATEGORIZE_PROVIDER_NATIVE_TOTAL.labels(
            source_type="plaid"
        )._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals

        n = apply_plaid_categories(db)

        after = CATEGORIZE_PROVIDER_NATIVE_TOTAL.labels(
            source_type="plaid"
        )._value.get()  # type: ignore[reportPrivateUsage]
        assert n == 1
        assert after == before + 1

    @pytest.mark.unit
    def test_confidence_skip_increments_below_gate_counter(self, db: Database) -> None:
        """A LOW-confidence match increments the skipped counter with reason='below_gate'."""
        from moneybin.metrics.registry import CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL

        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t4",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="LOW",
        )
        before = CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL.labels(
            source_type="plaid", reason="below_gate"
        )._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals

        n = apply_plaid_categories(db)

        after = CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL.labels(
            source_type="plaid", reason="below_gate"
        )._value.get()  # type: ignore[reportPrivateUsage]
        assert n == 0
        assert after == before + 1

    @pytest.mark.unit
    def test_unknown_confidence_increments_unknown_counter(self, db: Database) -> None:
        """An UNKNOWN confidence level increments the skipped counter reason='unknown'.

        A distinct reason from a below-gate rejection: UNKNOWN maps to a NULL
        numeric confidence (a data-quality signal), so it must not be conflated
        with a genuine low-confidence skip.
        """
        from moneybin.metrics.registry import CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL

        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t5",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="UNKNOWN",
        )
        before = CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL.labels(
            source_type="plaid", reason="unknown"
        )._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals

        n = apply_plaid_categories(db)

        after = CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL.labels(
            source_type="plaid", reason="unknown"
        )._value.get()  # type: ignore[reportPrivateUsage]
        assert n == 0
        assert after == before + 1

    @pytest.mark.unit
    def test_plaid_unmapped_counts_uncovered_pfc_codes(self, db: Database) -> None:
        """A Plaid txn whose PFC codes have no bridge row counts as unmapped."""
        refresh_views(db)
        _insert_plaid_txn(
            db,
            "t_unmapped",
            category_detailed="SOME_UNMAPPED_DETAILED_CODE",
            plaid_category="SOME_UNMAPPED_PRIMARY_CODE",
            category_confidence="HIGH",
        )

        stats = get_categorization_stats(db)

        assert stats["plaid_unmapped"] == 1

    @pytest.mark.unit
    def test_plaid_unmapped_excludes_covered_transactions(self, db: Database) -> None:
        """A bridge-mapped Plaid txn must not inflate the unmapped count."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t_covered",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )
        _insert_plaid_txn(
            db,
            "t_unmapped",
            category_detailed="SOME_UNMAPPED_DETAILED_CODE",
            plaid_category="SOME_UNMAPPED_PRIMARY_CODE",
            category_confidence="HIGH",
        )

        stats = get_categorization_stats(db)

        assert stats["plaid_unmapped"] == 1

    @pytest.mark.unit
    def test_plaid_unmapped_omitted_when_staging_view_absent(
        self, db: Database
    ) -> None:
        """No Plaid staging view materialized — the key is omitted, not zeroed.

        Mirrors the ``by_source`` block's CatalogException degradation so a
        non-Plaid database (no data ever loaded from Plaid) doesn't break
        ``categorization_stats()``.
        """
        stats = get_categorization_stats(db)

        assert "plaid_unmapped" not in stats

    @pytest.mark.unit
    def test_typed_stats_exposes_plaid_unmapped(self, db: Database) -> None:
        """CategorizationService.stats() carries plaid_unmapped through to the typed result."""
        refresh_views(db)
        _insert_plaid_txn(
            db,
            "t_unmapped",
            category_detailed="SOME_UNMAPPED_DETAILED_CODE",
            plaid_category="SOME_UNMAPPED_PRIMARY_CODE",
            category_confidence="HIGH",
        )

        stats = CategorizationService(db).stats()

        assert stats.plaid_unmapped == 1


# ---------------------------------------------------------------------------
# categorize_pending — plaid pass wired in last (Task 6)
# ---------------------------------------------------------------------------


class TestCategorizePendingPlaidPass:
    """Proves the plaid pass is wired into the categorize_pending cascade.

    Uses the bridge fixtures from ``TestApplyPlaidCategories`` (real
    ``core.bridge_category_source_map`` reverse lookup) plus real
    ``core.fct_transactions`` rows so ``fetch_uncategorized_rows`` — the
    scan shared by the rules and merchant passes — sees the transactions
    before the plaid pass runs last.
    """

    @pytest.mark.unit
    def test_plaid_fills_bare_row_but_not_higher_priority_row(
        self, db: Database
    ) -> None:
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t1",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )
        _insert_plaid_txn(
            db,
            "t2",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )
        # fetch_uncategorized_rows (shared by apply_rules/apply_merchant_categories)
        # reads core.fct_transactions — both txns must exist there with
        # non-empty description or the early-return in categorize_pending
        # fires before the plaid pass ever runs.
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, "
            "description, source_type) VALUES "
            "('t1', 'ACC1', '2025-06-01', -4.50, 'COFFEE SHOP', 'plaid'), "
            "('t2', 'ACC1', '2025-06-02', -4.75, 'COFFEE SHOP TWO', 'plaid')"
        )
        # t1 is already categorized by a higher-priority source (user, prio
        # 1) than plaid (provider_native, prio 6) — mirrors
        # TestGetCategorizationStats.test_with_categorized's seeding pattern.
        db.execute("""
            INSERT INTO app.transaction_categories
            (transaction_id, category, categorized_by)
            VALUES ('t1', 'Coffee', 'user')
        """)

        result = categorize_pending(db)

        # No rules or merchants are configured, so rule/merchant passes are
        # no-ops — only the plaid pass contributes, and only for t2 (t1 is
        # already categorized so the plaid query's uncategorized filter
        # excludes it).
        assert result["plaid"] == 1
        assert result["total"] == 1

        t1_row = db.execute(
            "SELECT categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 't1'"
        ).fetchone()
        assert t1_row == ("user",), "higher-priority row must not be overwritten"

        t2_row = db.execute(
            "SELECT categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 't2'"
        ).fetchone()
        assert t2_row == ("provider_native",), "bare row must be filled by plaid"

    @pytest.mark.unit
    def test_categorize_run_rules_and_merchants_excludes_plaid(
        self, db: Database
    ) -> None:
        """categorize_run's explicit methods=[...] selection must not silently add plaid.

        Regression test for the shared-scan fast path in
        ``CategorizationService.categorize_run`` (``effective == ["rules",
        "merchants"]``): before the plaid pass existed, that fast path
        delegated straight to ``categorize_pending()`` because the two were
        equivalent. Now that ``categorize_pending()`` also runs plaid by
        default, the fast path must opt out (``include_plaid=False``) or it
        would apply a third, unrequested engine whose writes go unreported
        in ``applied_by_method`` — and, depending on the requested method
        order, only sometimes (the per-method loop for other orders never
        touches plaid at all).
        """
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t3",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, "
            "description, source_type) VALUES "
            "('t3', 'ACC1', '2025-06-03', -5.25, 'COFFEE SHOP THREE', 'plaid')"
        )

        result = CategorizationService(db).categorize_run()

        assert result["applied_by_method"] == {"rules": 0, "merchants": 0}
        assert result["total_applied"] == 0
        assert (
            db.execute(
                "SELECT 1 FROM app.transaction_categories WHERE transaction_id='t3'"
            ).fetchone()
            is None
        ), "categorize_run(methods=['rules','merchants']) must not trigger plaid"

    @pytest.mark.unit
    def test_plaid_pass_runs_when_fetch_uncategorized_rows_is_empty(
        self, db: Database
    ) -> None:
        """A blank-description Plaid row must not starve the plaid pass.

        ``fetch_uncategorized_rows`` (shared by the rules/merchant passes)
        excludes rows with a blank description AND blank memo (and, absent
        the prep.int_transactions__merged table in this unit DB, has no
        entity-id fallback either) — so it returns ``[]`` for a txn like
        this. ``apply_plaid_categories`` matches on the PFC category code,
        not the description, so it can still categorize the row via its own
        independent query. Before the fix, the ``if not rows: return`` guard
        in ``categorize_pending`` short-circuited before the plaid pass ever
        ran, silently starving any all-blank-description tail.
        """
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="FOOD_AND_DRINK_COFFEE",
            code_level="detailed",
            category_id="FND-COF",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        _insert_plaid_txn(
            db,
            "t5",
            category_detailed="FOOD_AND_DRINK_COFFEE",
            plaid_category="FOOD_AND_DRINK",
            category_confidence="HIGH",
        )
        # Blank description, no memo — fetch_uncategorized_rows excludes this
        # row entirely, so it must be the ONLY uncategorized row in the DB
        # for the starvation bug to reproduce (a non-blank sibling row would
        # make apply_rules/apply_merchant_categories return non-empty rows,
        # masking the bug this test targets).
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, "
            "description, source_type) VALUES "
            "('t5', 'ACC1', '2025-06-05', -6.00, '', 'plaid')"
        )

        # Confirm the premise: the shared scan really does exclude this row.
        from moneybin.services.categorization.matcher import (
            CategorizationMatcher,
        )

        assert CategorizationMatcher(db).fetch_uncategorized_rows() == []

        result = categorize_pending(db)

        assert result["plaid"] == 1, (
            "plaid pass must run and categorize the row even though "
            "fetch_uncategorized_rows returned no rows to share with "
            "the rules/merchant passes"
        )
        assert result["total"] == 1
        row = db.execute(
            "SELECT category, categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 't5'"
        ).fetchone()
        assert row == ("Food & Drink", "provider_native")
