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


def _carry_term(
    db: Database, source_origin: str, category: str, subcategory: str | None = None
) -> None:
    """Make one imported row carry the term, as ``resolve_source_term`` requires."""
    _insert_matched_txn(
        db,
        f"t_{source_origin}_{category}_{subcategory}",
        source_type="tabular",
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
                source_type="tabular",
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

    @pytest.mark.unit
    def test_already_categorized_transaction_is_not_counted(self, db: Database) -> None:
        """The apply path never overwrites a categorization, so neither does the count."""
        refresh_views(db)
        for transaction_id in ("t_done", "t_open"):
            _insert_matched_txn(
                db,
                transaction_id,
                source_type="tabular",
                source_origin="chase_credit",
                category="Partly Done",
                subcategory=None,
            )
        _insert_matched_txn(
            db,
            "t_only_done",
            source_type="tabular",
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
                source_type="tabular",
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
            source_type="tabular",
            source_origin="chase_credit",
            category="Groceries",
            subcategory=None,
            source_transaction_id="src_mapped",
        )
        _insert_matched_txn(
            db,
            "gold_2",
            source_type="tabular",
            source_origin="amex_gold",
            category="Unmapped Twin",
            subcategory=None,
            source_transaction_id="src_unmapped",
        )
        _insert_matched_txn(
            db,
            "gold_3",
            source_type="tabular",
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
            "WHERE source_type = 'chase_credit' "
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
            "WHERE source_type = 'chase_credit' "
            "AND source_category_code = 'Imported Newness'"
        ).fetchone()
        assert mapping_row == (result_id,)

    @pytest.mark.unit
    def test_subcategory_normalized_to_empty_sentinel(self, db: Database) -> None:
        refresh_views(db)
        _carry_term(db, "manual", "Rent")
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
            pytest.param("", "Auto", None, id="empty-namespace"),
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
            "FROM app.category_source_map WHERE source_type = 'chase_credit'"
        ).fetchone()
        assert row == ("Groceries", "Produce")
        assert (mapping.category, mapping.subcategory) == row

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
