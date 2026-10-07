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

from typing import TypedDict

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.category_source_map_repo import CategorySourceMapRepo
from moneybin.repositories.transaction_categories_repo import (
    TransactionCategoriesRepo,
)
from moneybin.seeds import refresh_views
from moneybin.services.categorization import CategorizationService
from tests.moneybin.db_helpers import create_core_tables

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
    source_type: str = "csv",
    source_origin: str = "chase_credit",
    source_subcategory_code: str = "",
) -> None:
    """Seed one imported-term mapping the way curation stores it."""
    db.execute(
        "INSERT INTO app.category_source_map "
        "(source_type, source_origin, source_category_code, "
        "source_subcategory_code, code_level, category_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            source_type,
            source_origin,
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


def _carry_term(
    db: Database,
    source_origin: str,
    category: str,
    subcategory: str | None = None,
    *,
    source_type: str = "csv",
) -> None:
    """Make one imported row carry the term, as ``resolve_source_term`` requires."""
    _insert_matched_txn(
        db,
        f"t_{source_type}_{source_origin}_{category}_{subcategory}",
        source_type=source_type,
        source_origin=source_origin,
        category=category,
        subcategory=subcategory,
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
            source_type="csv",
            source_origin="chase_credit",
            category="Some Unmapped Category",
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1
        assert terms[0].source_type == "csv"
        assert terms[0].source_origin == "chase_credit"
        assert terms[0].category == "Some Unmapped Category"
        assert terms[0].subcategory is None
        assert terms[0].transaction_count == 1

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
            source_type="csv",
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
            source_type="csv",
            source_origin="chase_credit",
            category="Auto",
            subcategory=None,
        )
        _insert_matched_txn(
            db,
            "t4",
            source_type="csv",
            source_origin="chase_credit",
            category="Auto",
            subcategory="",
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1, "NULL and '' subcategory must collapse to one term"
        assert terms[0].subcategory is None
        assert terms[0].transaction_count == 2, (
            "count must reflect both affected transactions"
        )

    @pytest.mark.unit
    def test_count_reflects_affected_transactions_not_terms(self, db: Database) -> None:
        refresh_views(db)
        for i in range(3):
            _insert_matched_txn(
                db,
                f"t_many_{i}",
                source_type="csv",
                source_origin="chase_credit",
                category="Repeated Category",
                subcategory=None,
            )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1
        assert terms[0].transaction_count == 3

    @pytest.mark.unit
    def test_ordered_by_transaction_count_descending(self, db: Database) -> None:
        refresh_views(db)
        _insert_matched_txn(
            db,
            "t_small",
            source_type="csv",
            source_origin="chase_credit",
            category="Small Category",
            subcategory=None,
        )
        for i in range(3):
            _insert_matched_txn(
                db,
                f"t_big_{i}",
                source_type="csv",
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
            source_type="csv",
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
            source_type="csv",
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
            source_type="csv",
            source_origin="chase_credit",
            category="Category A",
            subcategory=None,
        )
        _insert_matched_txn(
            db,
            "t_b",
            source_type="csv",
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

    @pytest.mark.unit
    def test_already_categorized_transaction_is_not_counted(self, db: Database) -> None:
        """The apply path never overwrites a categorization, so neither does the count."""
        refresh_views(db)
        for transaction_id in ("t_done", "t_open"):
            _insert_matched_txn(
                db,
                transaction_id,
                source_type="csv",
                source_origin="chase_credit",
                category="Partly Done",
                subcategory=None,
            )
        _insert_matched_txn(
            db,
            "t_only_done",
            source_type="csv",
            source_origin="chase_credit",
            category="All Done",
            subcategory=None,
        )
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, categorized_by, rule_id, merchant_id) "
            "VALUES ('t_done', 'Shopping', 'user', NULL, NULL), "
            "('t_only_done', 'Shopping', 'rule', 'r_1', NULL)"
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert [(t.category, t.transaction_count) for t in terms] == [
            ("Partly Done", 1)
        ]

    @pytest.mark.unit
    def test_merged_members_with_the_same_term_count_once(self, db: Database) -> None:
        """Two source rows merged into one transaction are one transaction to resolve."""
        refresh_views(db)
        for source_transaction_id in ("src_a", "src_b"):
            _insert_matched_txn(
                db,
                "gold_1",
                source_type="csv",
                source_origin="chase_credit",
                category="Duplicated Export",
                subcategory=None,
                source_transaction_id=source_transaction_id,
            )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert len(terms) == 1
        assert terms[0].transaction_count == 1

    @pytest.mark.unit
    def test_transaction_covered_by_another_members_mapping_is_not_counted(
        self, db: Database
    ) -> None:
        """A merge group the bridge already resolves through a sibling member is not work."""
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
            "gold_2",
            source_type="csv",
            source_origin="chase_credit",
            category="Groceries",
            subcategory=None,
            source_transaction_id="src_mapped",
        )
        _insert_matched_txn(
            db,
            "gold_2",
            source_type="csv",
            source_origin="amex_gold",
            category="Unmapped Twin",
            subcategory=None,
            source_transaction_id="src_unmapped",
        )
        _insert_matched_txn(
            db,
            "gold_3",
            source_type="csv",
            source_origin="amex_gold",
            category="Unmapped Twin",
            subcategory=None,
        )

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert [(t.source_origin, t.category, t.transaction_count) for t in terms] == [
            ("amex_gold", "Unmapped Twin", 1)
        ]


# ---------------------------------------------------------------------------
# resolve_source_term
# ---------------------------------------------------------------------------


class TestResolveSourceTerm:
    """Tests for CategorizationService.resolve_source_term."""

    @pytest.mark.unit
    def test_map_to_existing_writes_the_row(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Some Imported Text")
        category_id = CategorizationService(db).create_category(
            "Test Existing Category", actor="test"
        )

        mapping = CategorizationService(db).resolve_source_term(
            source_origin="chase_credit",
            category="Some Imported Text",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        assert mapping.category_id == category_id
        row = db.execute(
            "SELECT category_id, source_subcategory_code "
            "FROM app.category_source_map "
            "WHERE source_type = 'csv' AND source_origin = 'chase_credit' "
            "AND source_category_code = 'Some Imported Text'"
        ).fetchone()
        assert row == (category_id, "")

    @pytest.mark.unit
    def test_create_new_creates_category_and_mapping(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Imported Newness")

        result_id = (
            CategorizationService(db)
            .resolve_source_term(
                source_origin="chase_credit",
                category="Imported Newness",
                subcategory=None,
                new_category="Freshly Minted Category",
                actor="test",
            )
            .category_id
        )

        category_row = db.execute(
            "SELECT category FROM app.user_categories WHERE category_id = ?",
            [result_id],
        ).fetchone()
        assert category_row == ("Freshly Minted Category",)

        mapping_row = db.execute(
            "SELECT category_id FROM app.category_source_map "
            "WHERE source_type = 'csv' AND source_origin = 'chase_credit' "
            "AND source_category_code = 'Imported Newness'"
        ).fetchone()
        assert mapping_row == (result_id,)

    @pytest.mark.unit
    def test_subcategory_normalized_to_empty_sentinel(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "user", "Rent", source_type="manual")
        category_id = CategorizationService(db).create_category(
            "Test Sentinel Category", actor="test"
        )

        CategorizationService(db).resolve_source_term(
            source_origin="user",
            category="Rent",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        row = db.execute(
            "SELECT source_subcategory_code FROM app.category_source_map "
            "WHERE source_type = 'manual' AND source_origin = 'user' "
            "AND source_category_code = 'Rent'"
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
        _carry_term(db, "chase_credit", "Whatever")
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
    def test_inactive_category_id_is_refused(self, db: Database) -> None:
        """An inactive category takes no new categorizations, so no mapping either."""
        refresh_views(db)
        _carry_term(db, "chase_credit", "Whatever")
        service = CategorizationService(db)
        category_id = service.create_category("Retired Category", actor="test")
        service.toggle_category(category_id, is_active=False, actor="test")

        with pytest.raises(UserError) as exc_info:
            service.resolve_source_term(
                source_origin="chase_credit",
                category="Whatever",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.TAXONOMY_CATEGORY_NOT_FOUND
        assert "inactive" in str(exc_info.value)
        assert db.execute(
            "SELECT COUNT(*) FROM app.category_source_map"
        ).fetchone() == (0,)

    @pytest.mark.unit
    def test_mapping_write_failure_rolls_back_created_category(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The category create and the mapping upsert share one transaction.

        If the mapping write fails, a category minted for it must not survive
        as an orphan nobody chose to keep.
        """
        refresh_views(db)
        _carry_term(db, "chase_credit", "Whatever")

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

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("source_origin", "category", "subcategory"),
        [
            pytest.param("chase_credit", "", None, id="empty-category"),
            pytest.param("chase_credit", " \t ", None, id="whitespace-category"),
            pytest.param("chase_credit", "Auto", "  ", id="whitespace-subcategory"),
        ],
    )
    def test_blank_term_part_is_refused(
        self,
        db: Database,
        source_origin: str,
        category: str,
        subcategory: str | None,
    ) -> None:
        """Staging NULLs a blank, so a blank key would store and never match."""
        refresh_views(db)
        category_id = CategorizationService(db).create_category(
            "Refusal Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin=source_origin,
                category=category,
                subcategory=subcategory,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.MUTATION_INVALID_INPUT
        assert db.execute(
            "SELECT COUNT(*) FROM app.category_source_map"
        ).fetchone() == (0,)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("source_origin", "category", "subcategory"),
        [
            pytest.param("chase_credit", "Grocerys", None, id="mistyped-category"),
            pytest.param("mint", "Groceries", None, id="other-namespace"),
            pytest.param(
                "chase_credit", "Groceries", "Produce", id="extra-subcategory"
            ),
            pytest.param("chase_credit", "Auto", None, id="missing-subcategory"),
        ],
    )
    def test_term_no_imported_row_carries_is_refused(
        self,
        db: Database,
        source_origin: str,
        category: str,
        subcategory: str | None,
    ) -> None:
        """A term matching nothing would store a mapping that never applies."""
        refresh_views(db)
        _carry_term(db, "chase_credit", "Groceries")
        _carry_term(db, "chase_credit", "Auto", "Gas")
        category_id = CategorizationService(db).create_category(
            "Unreached Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin=source_origin,
                category=category,
                subcategory=subcategory,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND
        assert db.execute(
            "SELECT COUNT(*) FROM app.category_source_map"
        ).fetchone() == (0,)

    @pytest.mark.unit
    def test_nothing_imported_yet_refuses_every_term(self, db: Database) -> None:
        """No prep.int_transactions__matched yet -> refused, not a raw catalog error."""
        refresh_views(db)
        category_id = CategorizationService(db).create_category(
            "Early Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Groceries",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND

    @pytest.mark.unit
    def test_long_term_an_import_carries_is_accepted(self, db: Database) -> None:
        """Imports bound none of the three parts, so neither may the writer."""
        refresh_views(db)
        origin, category, subcategory = "o" * 80, "c" * 150, "s" * 150
        _carry_term(db, origin, category, subcategory)
        category_id = CategorizationService(db).create_category(
            "Long Term Target", actor="test"
        )

        mapping = CategorizationService(db).resolve_source_term(
            source_origin=origin,
            category=category,
            subcategory=subcategory,
            category_id=category_id,
            actor="test",
        )

        assert mapping.category_id == category_id
        assert mapping.source_type == "csv"

    @pytest.mark.unit
    def test_mapped_term_can_change_after_its_rows_are_gone(self, db: Database) -> None:
        """A reverted import must not strand its mappings beyond correction."""
        refresh_views(db)
        _carry_term(db, "chase_credit", "Groceries")
        service = CategorizationService(db)
        first = service.create_category("Before Revert", actor="test")
        second = service.create_category("After Revert", actor="test")
        service.resolve_source_term(
            source_origin="chase_credit",
            category="Groceries",
            subcategory=None,
            category_id=first,
            actor="test",
        )
        db.execute("DELETE FROM prep.int_transactions__matched")

        mapping = service.resolve_source_term(
            source_origin="chase_credit",
            category="Groceries",
            subcategory=None,
            category_id=second,
            actor="test",
        )

        assert mapping.category_id == second

    @pytest.mark.unit
    def test_padded_term_is_stored_as_staging_trims_it(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Groceries", "Produce")
        category_id = CategorizationService(db).create_category(
            "Trim Target", actor="test"
        )

        mapping = CategorizationService(db).resolve_source_term(
            source_origin="chase_credit",
            category="  Groceries\t",
            subcategory=" Produce ",
            category_id=category_id,
            actor="test",
        )

        row = db.execute(
            "SELECT source_category_code, source_subcategory_code "
            "FROM app.category_source_map WHERE source_origin = 'chase_credit'"
        ).fetchone()
        assert row == ("Groceries", "Produce")
        assert (mapping.category, mapping.subcategory) == row

    @pytest.mark.unit
    def test_padded_source_origin_is_stored_and_returned_trimmed(
        self, db: Database
    ) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Dining")
        category_id = CategorizationService(db).create_category(
            "Origin Trim Target", actor="test"
        )

        mapping = CategorizationService(db).resolve_source_term(
            source_origin="  chase_credit  ",
            category="Dining",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        row = db.execute(
            "SELECT source_origin FROM app.category_source_map "
            "WHERE source_category_code = 'Dining'"
        ).fetchone()
        assert row == ("chase_credit",)
        assert mapping.source_origin == "chase_credit"

    @pytest.mark.unit
    def test_outcome_counter_tells_added_from_updated(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Counted Term")
        service = CategorizationService(db)
        first = service.create_category("Counter First", actor="test")
        second = service.create_category("Counter Second", actor="test")
        added_before = _mapping_outcomes("added")
        updated_before = _mapping_outcomes("updated")

        for category_id in (first, second):
            service.resolve_source_term(
                source_origin="chase_credit",
                category="Counted Term",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        assert _mapping_outcomes("added") == added_before + 1
        assert _mapping_outcomes("updated") == updated_before + 1

    @pytest.mark.unit
    def test_outcome_counter_records_a_refusal(self, db: Database) -> None:
        refresh_views(db)
        refused_before = _mapping_outcomes("refused")
        added_before = _mapping_outcomes("added")

        with pytest.raises(UserError):
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Whatever",
                subcategory=None,
                category_id="does-not-exist",
                actor="test",
            )

        assert _mapping_outcomes("refused") == refused_before + 1
        assert _mapping_outcomes("added") == added_before


def _mapping_outcomes(outcome: str) -> float:
    """Current value of the mapping-outcome counter for one outcome label."""
    from moneybin.metrics.registry import CATEGORY_SOURCE_MAPPING_OUTCOMES_TOTAL

    return CATEGORY_SOURCE_MAPPING_OUTCOMES_TOTAL.labels(outcome=outcome)._value.get()  # type: ignore[reportPrivateUsage]  # prometheus internals


# ---------------------------------------------------------------------------
# (source_type, source_origin) keying
# ---------------------------------------------------------------------------


def _seed_plaid_pfc(db: Database) -> None:
    """Seed a Plaid PFC-coded transaction plus its seed bridge row and category."""
    db.execute(
        "INSERT INTO seeds.category_source_map "
        "(source_type, source_category_code, source_subcategory_code, "
        "code_level, category_id, source_taxonomy_version) "
        "VALUES ('plaid', 'FOOD_AND_DRINK', '', 'primary', 'cat-plaid-food', 'v')"
    )
    db.execute(
        "INSERT INTO seeds.categories (category_id, category, subcategory, description) "
        "VALUES ('cat-plaid-food', 'Food & Drink', NULL, 'test category')"
    )
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__merged ("
        "  transaction_id VARCHAR, category_detailed VARCHAR, "
        "  plaid_category VARCHAR, category_confidence VARCHAR)"
    )
    db.execute(
        "INSERT INTO prep.int_transactions__merged VALUES "
        "('t_plaid', NULL, 'FOOD_AND_DRINK', 'HIGH')"
    )
    _seed_gold_transaction(db, "t_plaid")


def _seed_gold_transaction(db: Database, transaction_id: str) -> None:
    """Seed the core.fct_transactions row a categorization write requires."""
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, amount, transaction_date) "
        "VALUES (?, -10.00, '2026-01-01')",
        [transaction_id],
    )


def _categorized_by(db: Database, transaction_id: str) -> tuple[str, str] | None:
    row = db.execute(
        "SELECT category_id, source_type FROM app.transaction_categories "
        "WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    return (str(row[0]), str(row[1])) if row else None


class TestSourceTypeAndOriginKeying:
    """A mapping applies only to rows with its own (source_type, source_origin)."""

    @pytest.mark.unit
    def test_imported_mapping_under_origin_plaid_leaves_plaid_untouched(
        self, db: Database
    ) -> None:
        """An import whose origin slug is 'plaid' must not claim Plaid's codes."""
        refresh_views(db)
        _seed_plaid_pfc(db)
        _carry_term(db, "plaid", "FOOD_AND_DRINK", source_type="csv")
        # `set` sweeps at once, so the imported row needs its gold transaction.
        _seed_gold_transaction(db, "t_csv_plaid_FOOD_AND_DRINK_None")
        category_id = CategorizationService(db).create_category(
            "Imported Plaid Collision", actor="test"
        )
        CategorizationService(db).resolve_source_term(
            source_origin="plaid",
            category="FOOD_AND_DRINK",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        service = CategorizationService(db)
        service.apply_plaid_categories()

        assert _categorized_by(db, "t_plaid") == ("cat-plaid-food", "plaid")

    @pytest.mark.unit
    def test_plaid_override_does_not_apply_to_csv_rows_with_origin_plaid(
        self, db: Database
    ) -> None:
        """A Plaid-wide override row never matches an imported row of any origin."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="Coffee Shops",
            code_level="detailed",
            category_id="cat-override",
            category="Override",
            subcategory=None,
            source_type="plaid",
            source_origin="",
        )
        _insert_matched_txn(
            db,
            "t_csv",
            source_type="csv",
            source_origin="plaid",
            category="Coffee Shops",
            subcategory=None,
        )
        _seed_gold_transaction(db, "t_csv")

        service = CategorizationService(db)
        assert service.apply_source_category_map() == 0
        assert _categorized_by(db, "t_csv") is None

    @pytest.mark.unit
    def test_csv_mapping_does_not_apply_to_excel_rows(self, db: Database) -> None:
        """The same origin and term exported as csv and excel are two vocabularies."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="Coffee Shops",
            code_level="detailed",
            category_id="cat-coffee",
            category="Food",
            subcategory="Coffee",
        )
        for transaction_id, source_type in (("t_csv", "csv"), ("t_xlsx", "excel")):
            _insert_matched_txn(
                db,
                transaction_id,
                source_type=source_type,
                source_origin="chase_credit",
                category="Coffee Shops",
                subcategory=None,
            )
            _seed_gold_transaction(db, transaction_id)

        service = CategorizationService(db)
        assert service.apply_source_category_map() == 1
        assert _categorized_by(db, "t_csv") == ("cat-coffee", "csv")
        assert _categorized_by(db, "t_xlsx") is None
        terms = CategorizationService(db).list_unmapped_source_terms()
        assert [(t.source_type, t.source_origin) for t in terms] == [
            ("excel", "chase_credit")
        ]

    @pytest.mark.unit
    def test_pending_inbox_reports_each_terms_source_type(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="csv")
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="excel")

        terms = CategorizationService(db).list_unmapped_source_terms()

        assert sorted((t.source_type, t.source_origin) for t in terms) == [
            ("csv", "chase_credit"),
            ("excel", "chase_credit"),
        ]


class TestSourceTypeDerivation:
    """``set`` derives the source type when exactly one carries the term."""

    @pytest.mark.unit
    def test_unique_type_is_derived(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="excel")
        category_id = CategorizationService(db).create_category(
            "Derived Target", actor="test"
        )

        mapping = CategorizationService(db).resolve_source_term(
            source_origin="chase_credit",
            category="Coffee Shops",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        assert mapping.source_type == "excel"
        assert db.execute(
            "SELECT source_type FROM app.category_source_map"
        ).fetchall() == [("excel",)]

    @pytest.mark.unit
    def test_ambiguous_types_are_refused_with_candidates(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="csv")
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="excel")
        category_id = CategorizationService(db).create_category(
            "Ambiguous Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Coffee Shops",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.MUTATION_AMBIGUOUS
        assert exc_info.value.details == {"candidate_source_types": ["csv", "excel"]}
        assert db.execute(
            "SELECT COUNT(*) FROM app.category_source_map"
        ).fetchone() == (0,)

    @pytest.mark.unit
    def test_existing_mapping_counts_toward_the_candidates(self, db: Database) -> None:
        """A mapped type whose rows are gone still competes with a carried one."""
        refresh_views(db)
        _seed_bridge_mapping(
            db,
            source_category_code="Coffee Shops",
            code_level="detailed",
            category_id="cat-coffee",
            category="Food",
            subcategory="Coffee",
            source_type="csv",
        )
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="excel")
        category_id = CategorizationService(db).create_category(
            "Mapped Candidate Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_origin="chase_credit",
                category="Coffee Shops",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.MUTATION_AMBIGUOUS

    @pytest.mark.unit
    def test_explicit_type_is_honored_over_ambiguity(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="csv")
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="excel")
        category_id = CategorizationService(db).create_category(
            "Explicit Target", actor="test"
        )

        mapping = CategorizationService(db).resolve_source_term(
            source_type="excel",
            source_origin="chase_credit",
            category="Coffee Shops",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        assert mapping.source_type == "excel"
        assert db.execute(
            "SELECT source_type FROM app.category_source_map"
        ).fetchall() == [("excel",)]

    @pytest.mark.unit
    def test_explicit_type_must_carry_the_term(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "chase_credit", "Coffee Shops", source_type="csv")
        category_id = CategorizationService(db).create_category(
            "Wrong Type Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_type="excel",
                source_origin="chase_credit",
                category="Coffee Shops",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND

    @pytest.mark.unit
    @pytest.mark.parametrize("explicit", [True, False])
    def test_plaid_is_refused_as_a_set_target(
        self, db: Database, explicit: bool
    ) -> None:
        """Provider vocabularies are not curated through ``set``, explicit or derived."""
        refresh_views(db)
        _seed_plaid_pfc(db)
        # Even a plaid-typed row carrying text must not become a candidate.
        _insert_matched_txn(
            db,
            "t_plaid_text",
            source_type="plaid",
            source_origin="",
            category="FOOD_AND_DRINK",
            subcategory=None,
        )
        category_id = CategorizationService(db).create_category(
            "Plaid Refusal Target", actor="test"
        )

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                source_type="plaid" if explicit else None,
                source_origin="",
                category="FOOD_AND_DRINK",
                subcategory=None,
                category_id=category_id,
                actor="test",
            )

        expected = (
            error_codes.MUTATION_INVALID_INPUT
            if explicit
            else error_codes.MUTATION_NOT_FOUND
        )
        assert exc_info.value.code == expected
        assert db.execute(
            "SELECT COUNT(*) FROM app.category_source_map"
        ).fetchone() == (0,)

    @pytest.mark.unit
    def test_empty_origin_term_can_be_set_and_leaves_plaid_alone(
        self, db: Database
    ) -> None:
        """An import whose account label slugs to '' is a real origin, not blank."""
        refresh_views(db)
        _seed_plaid_pfc(db)
        _carry_term(db, "", "Coffee Shops", source_type="csv")
        _seed_gold_transaction(db, "t_csv__Coffee Shops_None")
        service = CategorizationService(db)
        assert [
            (t.source_type, t.source_origin)
            for t in service.list_unmapped_source_terms()
        ] == [("csv", "")]
        category_id = service.create_category("Empty Origin Target", actor="test")

        mapping = service.resolve_source_term(
            source_origin="",
            category="Coffee Shops",
            subcategory=None,
            category_id=category_id,
            actor="test",
        )

        assert (mapping.source_type, mapping.source_origin) == ("csv", "")
        assert service.list_unmapped_source_terms() == []
        # `set` swept at once: the imported row took the mapping, and the
        # Plaid row kept Plaid's own translation.
        assert mapping.categorized == 1
        assert _categorized_by(db, "t_csv__Coffee Shops_None") == (category_id, "csv")
        assert _categorized_by(db, "t_plaid") == ("cat-plaid-food", "plaid")


# ---------------------------------------------------------------------------
# Ignoring a term, and a changed mapping following through to history
# ---------------------------------------------------------------------------


class _Term(TypedDict):
    source_origin: str
    category: str
    subcategory: None


_TERM: _Term = {
    "source_origin": "chase_credit",
    "category": "Coffee Shops",
    "subcategory": None,
}


def _import_rows(
    db: Database, *transaction_ids: str, category: str = "Coffee Shops"
) -> None:
    """Import csv rows from chase_credit carrying one category term."""
    for transaction_id in transaction_ids:
        _insert_matched_txn(
            db,
            transaction_id,
            source_type="csv",
            source_origin="chase_credit",
            category=category,
            subcategory=None,
        )
        _seed_gold_transaction(db, transaction_id)


def _categorizations(db: Database) -> list[tuple[str, str, str]]:
    """Every categorization as (transaction_id, category_id, categorized_by)."""
    return db.execute(
        "SELECT transaction_id, category_id, categorized_by "
        "FROM app.transaction_categories ORDER BY transaction_id"
    ).fetchall()


class TestIgnoreSourceTerm:
    """``resolve_source_term(ignore=True)`` — a translation to nothing."""

    @pytest.mark.unit
    def test_ignored_term_is_stored_without_a_category(self, db: Database) -> None:
        refresh_views(db)
        _import_rows(db, "t1")

        mapping = CategorizationService(db).resolve_source_term(
            **_TERM, ignore=True, actor="test"
        )

        assert mapping.category_id is None
        assert db.execute(
            "SELECT source_type, source_origin, source_category_code, category_id "
            "FROM app.category_source_map"
        ).fetchall() == [("csv", "chase_credit", "Coffee Shops", None)]

    @pytest.mark.unit
    def test_ignored_term_leaves_the_pending_inbox_and_categorizes_nothing(
        self, db: Database
    ) -> None:
        refresh_views(db)
        _import_rows(db, "t1", "t2")
        service = CategorizationService(db)
        assert len(service.list_unmapped_source_terms()) == 1

        mapping = service.resolve_source_term(**_TERM, ignore=True, actor="test")

        assert service.list_unmapped_source_terms() == []
        assert (mapping.categorized, mapping.recategorized) == (0, 0)
        assert service.apply_source_category_map() == 0
        assert _categorizations(db) == []

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "other", [{"category_id": "cat-a"}, {"new_category": "Brand New"}]
    )
    def test_ignore_with_another_target_is_refused(
        self, db: Database, other: dict[str, str]
    ) -> None:
        refresh_views(db)
        _import_rows(db, "t1")

        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).resolve_source_term(
                **_TERM, ignore=True, actor="test", **other
            )

        assert exc_info.value.code == error_codes.MUTATION_INVALID_INPUT
        assert db.execute(
            "SELECT COUNT(*) FROM app.category_source_map"
        ).fetchone() == (0,)

    @pytest.mark.unit
    def test_ignored_term_abstains_so_another_members_mapping_applies(
        self, db: Database
    ) -> None:
        """One transaction, two imported members: the ignored term yields nothing."""
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1")
        _insert_matched_txn(
            db,
            "t1",
            source_type="csv",
            source_origin="mint",
            category="Cafe",
            subcategory=None,
            source_transaction_id="t1_mint",
        )
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, ignore=True, actor="test")

        service.resolve_source_term(
            source_origin="mint",
            category="Cafe",
            subcategory=None,
            category_id="cat-a",
            actor="test",
        )

        assert _categorizations(db) == [("t1", "cat-a", "provider_native")]

    @pytest.mark.unit
    def test_an_ignored_term_can_be_mapped_later(self, db: Database) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1", "t2")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, ignore=True, actor="test")

        mapping = service.resolve_source_term(
            **_TERM, category_id="cat-a", actor="test"
        )

        assert (mapping.categorized, mapping.recategorized) == (2, 0)
        assert _categorizations(db) == [
            ("t1", "cat-a", "provider_native"),
            ("t2", "cat-a", "provider_native"),
        ]


class TestMappingFollowsThrough:
    """A set, changed, or ignored mapping reaches the transactions at once."""

    @pytest.mark.unit
    def test_fresh_mapping_categorizes_its_rows_at_once(self, db: Database) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1", "t2")
        _import_rows(db, "t_other", category="Travel")

        mapping = CategorizationService(db).resolve_source_term(
            **_TERM, category_id="cat-a", actor="test"
        )

        assert (mapping.categorized, mapping.recategorized) == (2, 0)
        assert _categorizations(db) == [
            ("t1", "cat-a", "provider_native"),
            ("t2", "cat-a", "provider_native"),
        ]

    @pytest.mark.unit
    def test_changed_mapping_moves_the_rows_it_categorized(self, db: Database) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _seed_active_category(db, "cat-b", "Category B")
        _import_rows(db, "t1", "t2")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")

        mapping = service.resolve_source_term(
            **_TERM, category_id="cat-b", actor="test"
        )

        assert (mapping.categorized, mapping.recategorized) == (0, 2)
        assert _categorizations(db) == [
            ("t1", "cat-b", "provider_native"),
            ("t2", "cat-b", "provider_native"),
        ]

    @pytest.mark.unit
    def test_change_counts_new_rows_apart_from_moved_ones(self, db: Database) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _seed_active_category(db, "cat-b", "Category B")
        _import_rows(db, "t1")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")
        _import_rows(db, "t2", "t3")

        mapping = service.resolve_source_term(
            **_TERM, category_id="cat-b", actor="test"
        )

        assert (mapping.categorized, mapping.recategorized) == (2, 1)

    @pytest.mark.unit
    def test_ignoring_a_mapped_term_withdraws_what_it_categorized(
        self, db: Database
    ) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1", "t2")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")

        mapping = service.resolve_source_term(**_TERM, ignore=True, actor="test")

        assert (mapping.categorized, mapping.recategorized) == (0, 2)
        assert _categorizations(db) == []
        assert service.list_unmapped_source_terms() == []

    @pytest.mark.unit
    def test_setting_the_same_category_again_withdraws_nothing(
        self, db: Database
    ) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")
        clears_before = _audit_count(db, "category.clear")

        mapping = service.resolve_source_term(
            **_TERM, category_id="cat-a", actor="test"
        )

        assert (mapping.categorized, mapping.recategorized) == (0, 0)
        assert _audit_count(db, "category.clear") == clears_before
        assert _categorizations(db) == [("t1", "cat-a", "provider_native")]

    @pytest.mark.unit
    @pytest.mark.parametrize("change", ["remap", "ignore"])
    def test_other_sources_categorizations_are_never_touched(
        self, db: Database, change: str
    ) -> None:
        """Manual, rule, and merchant categorizations survive a changed mapping.

        ``t_rule_twin`` is the hard case: a rule row that happens to hold the
        mapping's old category and the term's own source type, so only its
        ``categorized_by`` tells it apart from what the term produced.
        """
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _seed_active_category(db, "cat-b", "Category B")
        _import_rows(db, "t_term", "t_user", "t_rule", "t_merchant", "t_rule_twin")
        categories = TransactionCategoriesRepo(db)
        categories.set(
            "t_user",
            category="Category A",
            subcategory=None,
            category_id="cat-a",
            actor="test",
        )
        for transaction_id, rule_id, merchant_id, source_type in (
            ("t_rule", "rule-1", None, "internal"),
            ("t_merchant", None, "merchant-1", "internal"),
            ("t_rule_twin", "rule-2", None, "csv"),
        ):
            categories.upsert_guarded(
                transaction_id,
                category="Category A",
                subcategory=None,
                category_id="cat-a",
                categorized_by="rule",
                merchant_id=merchant_id,
                rule_id=rule_id,
                confidence=1.0,
                source_type=source_type,
                actor="test",
            )
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")
        assert ("t_term", "cat-a", "provider_native") in _categorizations(db)

        if change == "remap":
            mapping = service.resolve_source_term(
                **_TERM, category_id="cat-b", actor="test"
            )
            term_row = [("t_term", "cat-b", "provider_native")]
        else:
            mapping = service.resolve_source_term(**_TERM, ignore=True, actor="test")
            term_row = []

        assert mapping.recategorized == 1
        assert _categorizations(db) == [
            ("t_merchant", "cat-a", "rule"),
            ("t_rule", "cat-a", "rule"),
            ("t_rule_twin", "cat-a", "rule"),
            *term_row,
            ("t_user", "cat-a", "user"),
        ]

    @pytest.mark.unit
    def test_another_terms_categorizations_are_left_alone(self, db: Database) -> None:
        """Two terms mapped to one category: changing one leaves the other's rows."""
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _seed_active_category(db, "cat-b", "Category B")
        _import_rows(db, "t_coffee")
        _import_rows(db, "t_cafe", category="Cafe")
        service = CategorizationService(db)
        for category in ("Coffee Shops", "Cafe"):
            service.resolve_source_term(
                source_origin="chase_credit",
                category=category,
                subcategory=None,
                category_id="cat-a",
                actor="test",
            )

        mapping = service.resolve_source_term(
            **_TERM, category_id="cat-b", actor="test"
        )

        assert mapping.recategorized == 1
        assert _categorizations(db) == [
            ("t_cafe", "cat-a", "provider_native"),
            ("t_coffee", "cat-b", "provider_native"),
        ]

    @pytest.mark.unit
    def test_withdrawal_is_audited_under_the_callers_actor(self, db: Database) -> None:
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")

        service.resolve_source_term(**_TERM, ignore=True, actor="cli")

        assert db.execute(
            "SELECT actor, target_id FROM app.audit_log WHERE action = 'category.clear'"
        ).fetchall() == [("cli", "t1")]

    @pytest.mark.unit
    def test_shared_transaction_is_recategorized_by_the_surviving_term(
        self, db: Database
    ) -> None:
        """Two members, two terms, one category: the other term restores the row.

        The withdrawal cannot tell which term produced the shared row, so it
        is counted and cleared, and the re-sweep recreates it from the term
        that is still mapped.
        """
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t_shared", "t_user", "t_rule", "t_merchant")
        _insert_matched_txn(
            db,
            "t_shared",
            source_type="csv",
            source_origin="mint",
            category="Cafe",
            subcategory=None,
            source_transaction_id="t_shared_mint",
        )
        categories = TransactionCategoriesRepo(db)
        categories.set(
            "t_user",
            category="Category A",
            subcategory=None,
            category_id="cat-a",
            actor="test",
        )
        for transaction_id, rule_id, merchant_id in (
            ("t_rule", "rule-1", None),
            ("t_merchant", None, "merchant-1"),
        ):
            categories.upsert_guarded(
                transaction_id,
                category="Category A",
                subcategory=None,
                category_id="cat-a",
                categorized_by="rule",
                merchant_id=merchant_id,
                rule_id=rule_id,
                confidence=1.0,
                source_type="internal",
                actor="test",
            )
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")
        service.resolve_source_term(
            source_origin="mint",
            category="Cafe",
            subcategory=None,
            category_id="cat-a",
            actor="test",
        )
        before = _categorizations(db)
        assert ("t_shared", "cat-a", "provider_native") in before
        sets_before = _audit_count(db, "category.set")

        mapping = service.resolve_source_term(**_TERM, ignore=True, actor="test")

        assert _categorizations(db) == before
        # Over-counted by the shared row, and audited as a clear/set pair.
        assert (mapping.categorized, mapping.recategorized) == (0, 1)
        assert _audit_count(db, "category.clear") == 1
        assert _audit_count(db, "category.set") == sets_before + 1

    @pytest.mark.unit
    def test_sweep_failure_reports_what_is_already_stored(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The mapping and withdrawals are committed before the sweep can fail."""
        refresh_views(db)
        _seed_active_category(db, "cat-a", "Category A")
        _import_rows(db, "t1", "t2")
        service = CategorizationService(db)
        service.resolve_source_term(**_TERM, category_id="cat-a", actor="test")
        failure = RuntimeError("sweep broke")

        def _fail() -> dict[str, int]:
            raise failure

        monkeypatch.setattr(service, "categorize_pending", _fail)

        with pytest.raises(UserError) as exc_info:
            service.resolve_source_term(**_TERM, ignore=True, actor="test")

        error = exc_info.value
        assert error.code == error_codes.REFRESH_CATEGORIZE_FAILED
        assert "mapping is stored" in error.message
        assert "2 earlier categorization(s)" in error.message
        assert "moneybin refresh --step categorize" in (error.hint or "")
        assert error.__cause__ is failure
        assert db.execute(
            "SELECT category_id FROM app.category_source_map"
        ).fetchall() == [(None,)]
        assert _categorizations(db) == []


def _audit_count(db: Database, action: str) -> int:
    row = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = ?", [action]
    ).fetchone()
    return int(row[0]) if row else 0
