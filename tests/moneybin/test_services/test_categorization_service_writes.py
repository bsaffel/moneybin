"""Tests for CategorizationService write methods extracted from MCP tools.

Covers create_rules, deactivate_rule, create_category, toggle_category, and
the validate_rule_items boundary helper. The MCP-tool tests in
test_categorization_tools.py exercise the wiring; these tests pin the
service-layer behavior independently.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.categorization import (
    CategorizationRuleInput,
    CategorizationService,
    validate_rule_items,
)
from tests.moneybin.db_helpers import create_core_tables, seed_categories_view


@pytest.fixture()
def db(tmp_path: Path) -> Database:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(database)
    return database


# --- CategorizationRuleInput model contract --------------------------------


class TestCategorizationRuleInput:
    """Direct model contract for the typed rule-creation input."""

    @pytest.mark.unit
    def test_strips_whitespace_on_string_fields(self) -> None:
        item = CategorizationRuleInput(
            name="  Starbucks  ",
            merchant_pattern="  STARBUCKS  ",
            category="  Food & Drink  ",
        )
        assert item.name == "Starbucks"
        assert item.merchant_pattern == "STARBUCKS"
        assert item.category == "Food & Drink"

    @pytest.mark.unit
    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(
                name="x",
                merchant_pattern="x",
                category="x",
                surprise="not allowed",  # type: ignore[call-arg]
            )

    @pytest.mark.unit
    def test_empty_name_rejected(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(name="", merchant_pattern="x", category="y")

    @pytest.mark.unit
    def test_empty_merchant_pattern_rejected(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(name="x", merchant_pattern="", category="y")

    @pytest.mark.unit
    def test_empty_category_rejected(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(name="x", merchant_pattern="y", category="")

    @pytest.mark.unit
    def test_name_max_length_enforced(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(
                name="x" * 201,
                merchant_pattern="y",
                category="z",
            )

    @pytest.mark.unit
    def test_merchant_pattern_max_length_enforced(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(
                name="x",
                merchant_pattern="y" * 501,
                category="z",
            )

    @pytest.mark.unit
    def test_priority_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(
                name="x",
                merchant_pattern="y",
                category="z",
                priority=-1,
            )

    @pytest.mark.unit
    def test_priority_above_ceiling_rejected(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(
                name="x",
                merchant_pattern="y",
                category="z",
                priority=10_001,
            )

    @pytest.mark.unit
    def test_match_type_invalid_rejected(self) -> None:
        with pytest.raises(ValueError):
            CategorizationRuleInput(
                name="x",
                merchant_pattern="y",
                category="z",
                match_type="fuzzy",  # type: ignore[arg-type]
            )

    @pytest.mark.unit
    def test_default_priority_is_100(self) -> None:
        item = CategorizationRuleInput(
            name="x",
            merchant_pattern="y",
            category="z",
        )
        assert item.priority == 100

    @pytest.mark.unit
    def test_default_match_type_is_contains(self) -> None:
        item = CategorizationRuleInput(
            name="x",
            merchant_pattern="y",
            category="z",
        )
        assert item.match_type == "contains"


# --- validate_rule_items ---------------------------------------------------


class TestValidateRuleItems:
    """Boundary helper for transactions_categorize_rules_create."""

    @pytest.mark.unit
    def test_accepts_minimal_valid_row(self) -> None:
        items, errors = validate_rule_items([
            {
                "name": "Starbucks",
                "merchant_pattern": "STARBUCKS",
                "category": "Food & Drink",
            },
        ])
        assert errors == []
        assert len(items) == 1
        assert items[0].name == "Starbucks"
        assert items[0].match_type == "contains"  # default
        assert items[0].priority == 100  # default

    @pytest.mark.unit
    def test_accepts_full_row(self) -> None:
        items, errors = validate_rule_items([
            {
                "name": "Big Amazon",
                "merchant_pattern": "AMZN",
                "category": "Shopping",
                "subcategory": "Online",
                "match_type": "contains",
                "min_amount": 100,
                "max_amount": 1000,
                "account_id": "ACC001",
                "priority": 50,
            },
        ])
        assert errors == []
        assert items[0].priority == 50
        assert items[0].subcategory == "Online"
        assert items[0].account_id == "ACC001"

    @pytest.mark.unit
    def test_missing_required_field_records_error(self) -> None:
        items, errors = validate_rule_items([
            {"name": "", "merchant_pattern": "X", "category": "Y"},
        ])
        assert items == []
        assert len(errors) == 1
        assert errors[0]["name"] == "(missing)"
        assert "name" in errors[0]["reason"].lower()

    @pytest.mark.unit
    def test_invalid_match_type_records_error(self) -> None:
        items, errors = validate_rule_items([
            {
                "name": "X",
                "merchant_pattern": "Y",
                "category": "Z",
                "match_type": "fuzzy",
            },
        ])
        assert items == []
        assert len(errors) == 1
        assert "match_type" in errors[0]["reason"]

    @pytest.mark.unit
    def test_invalid_priority_records_error(self) -> None:
        items, errors = validate_rule_items([
            {"name": "X", "merchant_pattern": "Y", "category": "Z", "priority": "high"},
        ])
        assert items == []
        assert len(errors) == 1
        assert "priority" in errors[0]["reason"].lower()

    @pytest.mark.unit
    def test_non_dict_row_records_error(self) -> None:
        items, errors = validate_rule_items(["not a dict"])  # type: ignore[list-item]
        assert items == []
        assert errors[0]["name"] == "(missing)"

    @pytest.mark.unit
    def test_non_list_input_raises(self) -> None:
        with pytest.raises(ValueError, match="JSON array"):
            validate_rule_items("not a list")  # type: ignore[arg-type]


class TestCreateRules:
    """CategorizationService.create_rules — batch INSERT into app.categorization_rules."""

    @pytest.mark.unit
    def test_empty_input_returns_zero_counts(self, db: Database) -> None:
        result = CategorizationService(db).create_rules([])
        assert result.created == 0
        assert result.existing == 0
        assert result.skipped == 0
        assert result.error_details == []
        assert result.rule_ids == []

    @pytest.mark.unit
    def test_writes_rule_row_with_defaults(self, db: Database) -> None:
        items = [
            CategorizationRuleInput(
                name="Starbucks",
                merchant_pattern="STARBUCKS",
                category="Food & Drink",
            )
        ]
        result = CategorizationService(db).create_rules(items)

        assert result.created == 1
        assert len(result.rule_ids) == 1
        rule_id = result.rule_ids[0]
        assert len(rule_id) == 12  # 12-char UUID hex per identifiers.md

        row = db.execute(
            "SELECT name, merchant_pattern, match_type, category, subcategory, "
            "min_amount, max_amount, account_id, priority, is_active, created_by "
            "FROM app.categorization_rules WHERE rule_id = ?",
            [rule_id],
        ).fetchone()
        assert row is not None
        (name, pattern, mt, cat, sub, min_a, max_a, acct, prio, active, by) = row
        assert (name, pattern, mt, cat, sub) == (
            "Starbucks",
            "STARBUCKS",
            "contains",
            "Food & Drink",
            None,
        )
        assert (min_a, max_a, acct, prio) == (None, None, None, 100)
        assert active is True
        assert by == "ai"

    @pytest.mark.unit
    def test_writes_full_rule_row(self, db: Database) -> None:
        items = [
            CategorizationRuleInput(
                name="Big Amazon",
                merchant_pattern="AMZN",
                category="Shopping",
                subcategory="Online",
                match_type="contains",
                min_amount=100,
                max_amount=1000,
                account_id="ACC001",
                priority=50,
            )
        ]
        result = CategorizationService(db).create_rules(items)
        assert result.created == 1

        row = db.execute(
            "SELECT min_amount, max_amount, account_id, priority, subcategory "
            "FROM app.categorization_rules WHERE rule_id = ?",
            [result.rule_ids[0]],
        ).fetchone()
        assert row == (100, 1000, "ACC001", 50, "Online")

    @pytest.mark.unit
    def test_writes_multiple_rules(self, db: Database) -> None:
        items = [
            CategorizationRuleInput(
                name=f"R{i}",
                merchant_pattern=f"P{i}",
                category="Cat",
            )
            for i in range(3)
        ]
        result = CategorizationService(db).create_rules(items)
        assert result.created == 3
        assert len(set(result.rule_ids)) == 3  # all unique

        row = db.execute("SELECT COUNT(*) FROM app.categorization_rules").fetchone()
        assert row == (3,)

    @pytest.mark.unit
    def test_partial_failure_isolates_bad_row(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed INSERT in the middle of a batch must not abort earlier or later rows."""
        svc = CategorizationService(db)
        items = [
            CategorizationRuleInput(
                name=f"R{i}",
                merchant_pattern=f"P{i}",
                category="Cat",
            )
            for i in range(3)
        ]

        original_execute = db.execute

        def fail_on_r1(sql: str, params: object = None) -> object:
            # The INSERT in create_rules binds params in column order:
            # rule_id, name, merchant_pattern, ... — name is index 1.
            if (
                "INSERT INTO" in sql
                and "categorization_rules" in sql
                and isinstance(params, list)
                and len(params) > 1  # pyright: ignore[reportUnknownArgumentType]  # params is object narrowed to list
                and params[1] == "R1"  # pyright: ignore[reportUnknownArgumentType]  # see above
            ):
                raise RuntimeError("injected failure for test")
            return original_execute(sql, params)  # type: ignore[arg-type]

        monkeypatch.setattr(db, "execute", fail_on_r1)

        result = svc.create_rules(items)

        assert result.created == 2
        assert result.skipped == 1
        assert len(result.rule_ids) == 2
        assert len(result.error_details) == 1
        assert result.error_details[0]["name"] == "R1"
        assert "Failed to create rule" in result.error_details[0]["reason"]

    @pytest.mark.unit
    def test_retry_same_payload_is_idempotent(self, db: Database) -> None:
        """Re-running create_rules with the same payload reuses the existing rule_id."""
        svc = CategorizationService(db)
        items = [
            CategorizationRuleInput(
                name="Starbucks",
                merchant_pattern="STARBUCKS",
                category="Food & Drink",
                subcategory="Coffee",
                min_amount=5,
                max_amount=50,
                account_id="ACC001",
            )
        ]
        first = svc.create_rules(items)
        second = svc.create_rules(items)

        assert first.created == 1
        assert first.existing == 0
        assert second.created == 0
        assert second.existing == 1
        assert first.rule_ids == second.rule_ids  # same rule_id returned
        row = db.execute("SELECT COUNT(*) FROM app.categorization_rules").fetchone()
        assert row == (1,)  # no duplicate row

    @pytest.mark.unit
    def test_name_and_priority_excluded_from_dedup_key(self, db: Database) -> None:
        """Same matcher+output with different name/priority is treated as same rule."""
        svc = CategorizationService(db)
        first = svc.create_rules([
            CategorizationRuleInput(
                name="Starbucks v1",
                merchant_pattern="STARBUCKS",
                category="Food & Drink",
                priority=100,
            )
        ])
        second = svc.create_rules([
            CategorizationRuleInput(
                name="Starbucks renamed",
                merchant_pattern="STARBUCKS",
                category="Food & Drink",
                priority=50,
            )
        ])

        assert first.created == 1
        assert second.existing == 1
        assert first.rule_ids == second.rule_ids
        row = db.execute("SELECT COUNT(*) FROM app.categorization_rules").fetchone()
        assert row == (1,)

    @pytest.mark.unit
    def test_different_category_output_is_new_rule(self, db: Database) -> None:
        """Same matcher with a different category is currently treated as a new rule.

        Conflict detection (same matcher, divergent output) is a deferred
        follow-up — see docs/specs/mcp-tool-surface.md "Rule-conflict
        detection (follow-up)".
        """
        svc = CategorizationService(db)
        first = svc.create_rules([
            CategorizationRuleInput(
                name="r1",
                merchant_pattern="AMZN",
                category="Shopping",
            )
        ])
        second = svc.create_rules([
            CategorizationRuleInput(
                name="r2",
                merchant_pattern="AMZN",
                category="Business",
            )
        ])

        assert first.created == 1
        assert second.created == 1
        assert first.rule_ids != second.rule_ids
        row = db.execute("SELECT COUNT(*) FROM app.categorization_rules").fetchone()
        assert row == (2,)

    @pytest.mark.unit
    def test_deactivated_rule_does_not_dedup(self, db: Database) -> None:
        """A deactivated rule with the same content does not block re-creation.

        Dedup is scoped to ``is_active = true`` so users can re-create a
        rule they previously soft-deleted.
        """
        svc = CategorizationService(db)
        first = svc.create_rules([
            CategorizationRuleInput(
                name="r1",
                merchant_pattern="STARBUCKS",
                category="Food & Drink",
            )
        ])
        svc.deactivate_rule(first.rule_ids[0])

        second = svc.create_rules([
            CategorizationRuleInput(
                name="r1",
                merchant_pattern="STARBUCKS",
                category="Food & Drink",
            )
        ])

        assert second.created == 1
        assert second.existing == 0
        assert first.rule_ids != second.rule_ids


class TestDeactivateRule:
    """CategorizationService.deactivate_rule — soft-delete a rule."""

    @pytest.mark.unit
    def test_returns_true_and_sets_inactive(self, db: Database) -> None:
        # Seed an active rule
        svc = CategorizationService(db)
        items = [
            CategorizationRuleInput(
                name="R1",
                merchant_pattern="P1",
                category="C",
            )
        ]
        rule_id = svc.create_rules(items).rule_ids[0]

        result = svc.deactivate_rule(rule_id)
        assert result is True

        row = db.execute(
            "SELECT is_active FROM app.categorization_rules WHERE rule_id = ?",
            [rule_id],
        ).fetchone()
        assert row == (False,)

    @pytest.mark.unit
    def test_returns_false_for_missing_rule(self, db: Database) -> None:
        # Ensure table exists by creating one rule, then ask for a different id.
        svc = CategorizationService(db)
        svc.create_rules([
            CategorizationRuleInput(name="x", merchant_pattern="x", category="x"),
        ])
        assert svc.deactivate_rule("does-not-exist") is False

    @pytest.mark.unit
    def test_does_not_affect_other_rules(self, db: Database) -> None:
        svc = CategorizationService(db)
        items = [
            CategorizationRuleInput(
                name=f"R{i}",
                merchant_pattern=f"P{i}",
                category="C",
            )
            for i in range(3)
        ]
        ids = svc.create_rules(items).rule_ids

        svc.deactivate_rule(ids[1])

        rows = db.execute(
            "SELECT rule_id, is_active FROM app.categorization_rules ORDER BY rule_id"
        ).fetchall()
        active_states = dict(rows)
        assert active_states[ids[0]] is True
        assert active_states[ids[1]] is False
        assert active_states[ids[2]] is True


class TestCreateCategory:
    """CategorizationService.create_category — INSERT into app.user_categories."""

    @pytest.mark.unit
    def test_writes_user_category_and_returns_id(self, db: Database) -> None:
        cat_id = CategorizationService(db).create_category(
            "Childcare",
            subcategory="Daycare",
            description="Kids daycare",
        )
        assert len(cat_id) == 12

        row = db.execute(
            "SELECT category, subcategory, description, is_active "
            "FROM app.user_categories WHERE category_id = ?",
            [cat_id],
        ).fetchone()
        assert row == ("Childcare", "Daycare", "Kids daycare", True)

    @pytest.mark.unit
    def test_top_level_only_category(self, db: Database) -> None:
        cat_id = CategorizationService(db).create_category("Hobbies")
        row = db.execute(
            "SELECT subcategory, description FROM app.user_categories "
            "WHERE category_id = ?",
            [cat_id],
        ).fetchone()
        assert row == (None, None)

    @pytest.mark.unit
    def test_duplicate_raises_user_error(self, db: Database) -> None:
        svc = CategorizationService(db)
        svc.create_category("Childcare", subcategory="Daycare")

        with pytest.raises(UserError) as exc_info:
            svc.create_category("Childcare", subcategory="Daycare")

        assert exc_info.value.code == "CATEGORY_ALREADY_EXISTS"
        assert "Childcare" in exc_info.value.message
        assert "Daycare" in exc_info.value.message

    @pytest.mark.unit
    def test_duplicate_top_level_raises_user_error(self, db: Database) -> None:
        svc = CategorizationService(db)
        svc.create_category("Hobbies")

        with pytest.raises(UserError) as exc_info:
            svc.create_category("Hobbies")

        assert exc_info.value.code == "CATEGORY_ALREADY_EXISTS"
        assert "Hobbies" in exc_info.value.message


class TestToggleCategory:
    """CategorizationService.toggle_category — branches by category origin."""

    @pytest.mark.unit
    def test_default_category_writes_override(self, db: Database) -> None:
        seed_categories_view(db)
        CategorizationService(db).toggle_category("FND", is_active=False)

        rows = db.execute(
            "SELECT category_id, is_active FROM app.category_overrides"
        ).fetchall()
        assert rows == [("FND", False)]

    @pytest.mark.unit
    def test_default_category_upserts_override(self, db: Database) -> None:
        """Toggling twice updates the existing override row, not appending."""
        seed_categories_view(db)
        svc = CategorizationService(db)
        svc.toggle_category("FND", is_active=False)
        svc.toggle_category("FND", is_active=True)

        rows = db.execute(
            "SELECT category_id, is_active FROM app.category_overrides"
        ).fetchall()
        assert rows == [("FND", True)]

    @pytest.mark.unit
    def test_user_category_updates_user_categories(self, db: Database) -> None:
        seed_categories_view(db)
        db.execute("""
            INSERT INTO app.user_categories
            (category_id, category, subcategory, is_active)
            VALUES ('CUSTOM1', 'Childcare', 'Daycare', true)
        """)

        CategorizationService(db).toggle_category("CUSTOM1", is_active=False)

        row = db.execute(
            "SELECT is_active FROM app.user_categories WHERE category_id = ?",
            ["CUSTOM1"],
        ).fetchone()
        assert row == (False,)
        # User toggles must NOT touch the override table.
        count = db.execute("SELECT COUNT(*) FROM app.category_overrides").fetchone()
        assert count == (0,)

    @pytest.mark.unit
    def test_missing_category_raises_user_error(self, db: Database) -> None:
        seed_categories_view(db)
        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).toggle_category("NOPE", is_active=False)
        assert exc_info.value.code == "CATEGORY_NOT_FOUND"
