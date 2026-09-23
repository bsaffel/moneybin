"""Tests for the imported-category-source-map curation surface (MB-180 PR2).

Covers ``CategorizationService.list_unmapped_source_terms`` (enumeration of
distinct unmapped imported vocabulary terms) and
``CategorizationService.resolve_source_term`` (mapping one term to an
existing or newly-created MoneyBin category). Mirrors the fixture style of
``TestApplySourceCategoryMap`` in ``test_categorization_service.py`` — same
stub ``prep.int_transactions__matched`` table, same ``seeds.category_source_map``
/ ``seeds.categories`` bridge-seeding helper — but this file is about
curating the bridge, not applying it.
"""

from __future__ import annotations

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.category_source_map_repo import CategorySourceMapRepo
from moneybin.seeds import refresh_views
from moneybin.services.categorization import CategorizationService

# ---------------------------------------------------------------------------
# Fixture helpers (mirrors test_categorization_service.py's helpers of the
# same shape/name; duplicated here so this file stands alone)
# ---------------------------------------------------------------------------


def _insert_matched_txn(
    db: Database,
    transaction_id: str,
    *,
    source_type: str,
    source_origin: str,
    category: str | None,
    subcategory: str | None,
    source_transaction_id: str | None = None,
) -> None:
    """Insert one imported row into a stub ``prep.int_transactions__matched``."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__matched ("
        "  transaction_id VARCHAR, "
        "  source_transaction_id VARCHAR, "
        "  source_type VARCHAR, "
        "  source_origin VARCHAR, "
        "  category VARCHAR, "
        "  subcategory VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO prep.int_transactions__matched "
        "(transaction_id, source_transaction_id, source_type, source_origin, "
        " category, subcategory) VALUES (?, ?, ?, ?, ?, ?)",
        [
            transaction_id,
            source_transaction_id or transaction_id,
            source_type,
            source_origin,
            category,
            subcategory,
        ],
    )


def _seed_bridge_mapping(
    db: Database,
    *,
    source_category_code: str,
    code_level: str,
    category_id: str,
    category: str,
    subcategory: str | None,
    source_type: str = "chase_credit",
    source_subcategory_code: str = "",
) -> None:
    """Seed one core.bridge_category_source_map row via the seed tables."""
    db.execute(
        "INSERT INTO seeds.category_source_map "
        "(source_type, source_category_code, source_subcategory_code, "
        "code_level, category_id, source_taxonomy_version) "
        "VALUES (?, ?, ?, ?, ?, 'test_v1')",
        [
            source_type,
            source_category_code,
            source_subcategory_code,
            code_level,
            category_id,
        ],
    )
    db.execute(
        "INSERT INTO seeds.categories (category_id, category, subcategory, description) "
        "VALUES (?, ?, ?, 'test category')",
        [category_id, category, subcategory],
    )


def _seed_active_category(db: Database, category_id: str, category: str) -> None:
    """Seed a bare active category into seeds.categories (no bridge mapping)."""
    db.execute(
        "INSERT INTO seeds.categories (category_id, category, subcategory, description) "
        "VALUES (?, ?, NULL, 'test category')",
        [category_id, category],
    )


# ---------------------------------------------------------------------------
# list_unmapped_source_terms
# ---------------------------------------------------------------------------


class TestListUnmappedSourceTerms:
    """Tests for CategorizationService.list_unmapped_source_terms."""

    @pytest.mark.unit
    def test_term_with_no_bridge_row_appears(self, db: Database) -> None:
        refresh_views(db)
        _insert_matched_txn(
            db,
            "t1",
            source_type="tabular",
            source_origin="chase_credit",
            category="Some Unmapped Category",
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1
        assert terms[0].source_origin == "chase_credit"
        assert terms[0].category == "Some Unmapped Category"
        assert terms[0].subcategory is None
        assert terms[0].row_count == 1

    @pytest.mark.unit
    def test_term_with_bridge_row_does_not_appear(self, db: Database) -> None:
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="Groceries",
            code_level="detailed",
            category_id="cat-groceries",
            category="Food & Dining",
            subcategory="Groceries",
        )
        _insert_matched_txn(
            db,
            "t2",
            source_type="tabular",
            source_origin="chase_credit",
            category="Groceries",
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert terms == []

    @pytest.mark.unit
    def test_null_and_empty_subcategory_collapse_to_one_term(
        self, db: Database
    ) -> None:
        """A NULL-subcategory row and an ''-subcategory row are the same term."""
        refresh_views(db)
        _insert_matched_txn(
            db,
            "t3",
            source_type="tabular",
            source_origin="chase_credit",
            category="Auto",
            subcategory=None,
        )
        _insert_matched_txn(
            db,
            "t4",
            source_type="tabular",
            source_origin="chase_credit",
            category="Auto",
            subcategory="",
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1, "NULL and '' subcategory must collapse to one term"
        assert terms[0].subcategory is None
        assert terms[0].row_count == 2, "count must reflect both affected rows"

    @pytest.mark.unit
    def test_count_reflects_affected_rows_not_terms(self, db: Database) -> None:
        refresh_views(db)
        for i in range(3):
            _insert_matched_txn(
                db,
                f"t_many_{i}",
                source_type="tabular",
                source_origin="chase_credit",
                category="Repeated Category",
                subcategory=None,
            )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1
        assert terms[0].row_count == 3

    @pytest.mark.unit
    def test_ordered_by_row_count_descending(self, db: Database) -> None:
        refresh_views(db)
        _insert_matched_txn(
            db,
            "t_small",
            source_type="tabular",
            source_origin="chase_credit",
            category="Small Category",
            subcategory=None,
        )
        for i in range(3):
            _insert_matched_txn(
                db,
                f"t_big_{i}",
                source_type="tabular",
                source_origin="chase_credit",
                category="Big Category",
                subcategory=None,
            )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert [t.category for t in terms] == ["Big Category", "Small Category"]

    @pytest.mark.unit
    def test_excludes_null_category_and_null_source_origin(self, db: Database) -> None:
        refresh_views(db)
        _insert_matched_txn(
            db,
            "t_null_cat",
            source_type="tabular",
            source_origin="chase_credit",
            category=None,
            subcategory=None,
        )
        _insert_matched_txn(
            db,
            "t_null_origin",
            source_type="manual",
            source_origin=None,  # type: ignore[arg-type]  # exercising the exclusion guard
            category="Something",
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert terms == []

    @pytest.mark.unit
    def test_suggestions_for_case_only_near_miss(self, db: Database) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-groceries-real", "Groceries")
        _insert_matched_txn(
            db,
            "t_near",
            source_type="tabular",
            source_origin="chase_credit",
            category="groceries",  # case-only difference from the active category
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1
        assert "Groceries" in terms[0].suggestions

    @pytest.mark.unit
    def test_namespace_filter(self, db: Database) -> None:
        refresh_views(db)
        _insert_matched_txn(
            db,
            "t_a",
            source_type="tabular",
            source_origin="chase_credit",
            category="Category A",
            subcategory=None,
        )
        _insert_matched_txn(
            db,
            "t_b",
            source_type="tabular",
            source_origin="amex_gold",
            category="Category B",
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms(
            namespace="amex_gold"
        )

        assert len(terms) == 1
        assert terms[0].source_origin == "amex_gold"
        assert terms[0].category == "Category B"

    @pytest.mark.unit
    def test_no_prep_table_degrades_to_empty(self, db: Database) -> None:
        """No prep.int_transactions__matched materialized yet -> empty, not raise."""
        refresh_views(db)
        terms = CategorizationService(db).list_unmapped_source_terms()
        assert terms == []


# ---------------------------------------------------------------------------
# resolve_source_term
# ---------------------------------------------------------------------------


class TestResolveSourceTerm:
    """Tests for CategorizationService.resolve_source_term."""

    @pytest.mark.unit
    def test_map_to_existing_writes_the_row(self, db: Database) -> None:
        refresh_views(db)
        category_id = CategorizationService(db).create_category(
            "Test Existing Category", actor="test"
        )

        result_id = CategorizationService(db).resolve_source_term(
            source_origin="chase_credit",
            category="Some Imported Text",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        assert result_id == category_id
        row = db.execute(
            "SELECT category_id, source_subcategory_code "
            "FROM app.category_source_map "
            "WHERE source_type = 'chase_credit' "
            "AND source_category_code = 'Some Imported Text'"
        ).fetchone()
        assert row == (category_id, "")

    @pytest.mark.unit
    def test_create_new_creates_category_and_mapping(self, db: Database) -> None:
        refresh_views(db)

        result_id = CategorizationService(db).resolve_source_term(
            source_origin="chase_credit",
            category="Imported Newness",
            subcategory=None,
            new_category="Freshly Minted Category",
            actor="test",
        )

        category_row = db.execute(
            "SELECT category FROM app.user_categories WHERE category_id = ?",
            [result_id],
        ).fetchone()
        assert category_row == ("Freshly Minted Category",)

        mapping_row = db.execute(
            "SELECT category_id FROM app.category_source_map "
            "WHERE source_type = 'chase_credit' "
            "AND source_category_code = 'Imported Newness'"
        ).fetchone()
        assert mapping_row == (result_id,)

    @pytest.mark.unit
    def test_subcategory_normalized_to_empty_sentinel(self, db: Database) -> None:
        refresh_views(db)
        category_id = CategorizationService(db).create_category(
            "Test Sentinel Category", actor="test"
        )

        CategorizationService(db).resolve_source_term(
            source_origin="manual",
            category="Rent",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        row = db.execute(
            "SELECT source_subcategory_code FROM app.category_source_map "
            "WHERE source_type = 'manual' AND source_category_code = 'Rent'"
        ).fetchone()
        assert row == ("",)

    @pytest.mark.unit
    def test_neither_category_id_nor_new_category_raises(self, db: Database) -> None:
        refresh_views(db)
        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Whatever",
                subcategory=None,
                actor="test",
            )
        assert exc_info.value.code == error_codes.MUTATION_INVALID_INPUT

    @pytest.mark.unit
    def test_both_category_id_and_new_category_raises(self, db: Database) -> None:
        refresh_views(db)
        category_id = CategorizationService(db).create_category(
            "Test Both Category", actor="test"
        )
        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Whatever",
                subcategory=None,
                category_id=category_id,
                new_category="Also New",
                actor="test",
            )
        assert exc_info.value.code == error_codes.MUTATION_INVALID_INPUT

    @pytest.mark.unit
    def test_unknown_category_id_raises_not_found(self, db: Database) -> None:
        refresh_views(db)
        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Whatever",
                subcategory=None,
                category_id="does-not-exist",
                actor="test",
            )
        assert exc_info.value.code == error_codes.TAXONOMY_CATEGORY_NOT_FOUND

    @pytest.mark.unit
    def test_mapping_write_failure_rolls_back_created_category(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The category create and the mapping upsert share one transaction.

        If the mapping write fails, a category minted for it must not survive
        as an orphan nobody chose to keep.
        """
        refresh_views(db)

        def _boom(self: CategorySourceMapRepo, **kwargs: object) -> None:
            raise RuntimeError("simulated mapping-write failure")

        monkeypatch.setattr(CategorySourceMapRepo, "upsert", _boom)

        with pytest.raises(RuntimeError):
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Whatever",
                subcategory=None,
                new_category="Should Not Survive",
                actor="test",
            )

        row = db.execute(
            "SELECT 1 FROM app.user_categories WHERE category = 'Should Not Survive'"
        ).fetchone()
        assert row is None, (
            "category creation must roll back when the mapping write fails"
        )
