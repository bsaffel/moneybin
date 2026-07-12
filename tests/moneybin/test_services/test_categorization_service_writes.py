"""Tests for CategorizationService write methods extracted from MCP tools.

Covers create_rules, deactivate_rule, create_category, toggle_category, and
the validate_rule_items boundary helper. The MCP-tool tests in
test_categorization_tools.py exercise the wiring; these tests pin the
service-layer behavior independently.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization import (
    CategorizationRuleInput,
    CategorizationService,
    validate_rule_items,
)
from moneybin.services.categorization.applier import MatchApplier
from tests.moneybin.db_helpers import create_core_tables, seed_categories_view


@pytest.fixture(autouse=True)
def _core_tables(db: Database) -> None:  # pyright: ignore[reportUnusedFunction]
    create_core_tables(db)


@pytest.fixture()
def applier(db: Database) -> MatchApplier:
    """MatchApplier bound to the test database."""
    return MatchApplier(db, audit=AuditService(db))


def _insert_txn(db: Database, transaction_id: str) -> None:
    """Insert a minimal core.fct_transactions row for write-path tests."""
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, amount, transaction_date) "
        "VALUES (?, -10, '2026-05-01')",
        [transaction_id],
    )


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
                merchant_pattern=f"PATTERN{i}",
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
                merchant_pattern=f"PATTERN{i}",
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
        follow-up — see docs/specs/moneybin-mcp.md "Rule-conflict
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


class TestCreateRulesUnselectiveContainsGate:
    """create_rules refuses a direct `contains "TO"`-shaped rule (rules-create-gate).

    The MCP/CLI rule-creation path lets a caller author exactly the pattern
    the auto-rule proposer downgrades to `exact` — an agent filling
    merchant_pattern="TO", match_type="contains" would otherwise relabel
    STORE/AUTO/TOTAL as Internal Transfer. This shares one predicate
    (`_shared.is_unselective_contains`) with `AutoRuleService._invented_match_type`.
    """

    @pytest.mark.unit
    def test_refuses_short_contains_pattern(self, db: Database) -> None:
        items = [
            CategorizationRuleInput(
                name="Transfer TO",
                merchant_pattern="TO",
                category="Transfer",
                subcategory="Internal Transfer",
                match_type="contains",
            )
        ]
        result = CategorizationService(db).create_rules(items)

        assert result.created == 0
        assert result.skipped == 1
        assert result.rule_ids == []
        assert len(result.error_details) == 1
        assert result.error_details[0]["name"] == "Transfer TO"
        assert "too short" in result.error_details[0]["reason"].lower()
        assert "allow_broad" in result.error_details[0]["reason"]

        row = db.execute("SELECT COUNT(*) FROM app.categorization_rules").fetchone()
        assert row == (0,)

    @pytest.mark.unit
    def test_allow_broad_overrides_the_refusal(self, db: Database) -> None:
        items = [
            CategorizationRuleInput(
                name="Transfer TO",
                merchant_pattern="TO",
                category="Transfer",
                subcategory="Internal Transfer",
                match_type="contains",
            )
        ]
        result = CategorizationService(db).create_rules(items, allow_broad=True)

        assert result.created == 1
        assert result.skipped == 0
        assert result.error_details == []
        assert len(result.rule_ids) == 1

        row = db.execute(
            "SELECT merchant_pattern, match_type FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [result.rule_ids[0]],
        ).fetchone()
        assert row == ("TO", "contains")

    @pytest.mark.unit
    def test_exact_short_pattern_is_not_gated(self, db: Database) -> None:
        """The floor is contains-only — `exact "TO"` needs no override."""
        items = [
            CategorizationRuleInput(
                name="Transfer TO exact",
                merchant_pattern="TO",
                category="Transfer",
                subcategory="Internal Transfer",
                match_type="exact",
            )
        ]
        result = CategorizationService(db).create_rules(items)

        assert result.created == 1
        assert result.skipped == 0
        assert result.error_details == []

    @pytest.mark.unit
    def test_normal_broad_contains_pattern_is_not_gated(self, db: Database) -> None:
        """A deliberately broad but selective pattern is never crying-wolf blocked."""
        items = [
            CategorizationRuleInput(
                name="Amazon",
                merchant_pattern="AMAZON",
                category="Shopping",
            )
        ]
        result = CategorizationService(db).create_rules(items)

        assert result.created == 1
        assert result.skipped == 0
        assert result.error_details == []


class TestDeactivateRule:
    """CategorizationService.deactivate_rule — soft-delete a rule."""

    @pytest.mark.unit
    def test_returns_true_and_sets_inactive(self, db: Database) -> None:
        # Seed an active rule
        svc = CategorizationService(db)
        items = [
            CategorizationRuleInput(
                name="R1",
                merchant_pattern="PATTERN1",
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
            CategorizationRuleInput(name="x", merchant_pattern="xxxx", category="x"),
        ])
        assert svc.deactivate_rule("does-not-exist") is False

    @pytest.mark.unit
    def test_does_not_affect_other_rules(self, db: Database) -> None:
        svc = CategorizationService(db)
        items = [
            CategorizationRuleInput(
                name=f"R{i}",
                merchant_pattern=f"PATTERN{i}",
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


class TestResolveCategoryId:
    """The shared resolve_category_id helper used by every writer."""

    @pytest.mark.unit
    def test_resolves_user_category(self, db: Database) -> None:
        from moneybin.services.categorization._shared import resolve_category_id

        svc = CategorizationService(db)
        cat_id = svc.create_category("ResolveMe", subcategory="Sub")

        assert resolve_category_id(db, "ResolveMe", "Sub") == cat_id

    @pytest.mark.unit
    def test_resolves_default_category(self, db: Database) -> None:
        from moneybin.services.categorization._shared import resolve_category_id

        # The module's `db` fixture doesn't seed defaults; seed FND here so the
        # core.dim_categories view exposes it.
        seed_categories_view(db)
        assert resolve_category_id(db, "Food & Drink", None) == "FND"

    @pytest.mark.unit
    def test_returns_none_for_unknown_text(self, db: Database) -> None:
        from moneybin.services.categorization._shared import resolve_category_id

        assert resolve_category_id(db, "NeverDefined", None) is None

    @pytest.mark.unit
    def test_null_subcategory_matches_null(self, db: Database) -> None:
        from moneybin.services.categorization._shared import resolve_category_id

        svc = CategorizationService(db)
        cat_id = svc.create_category("TopLevel")  # subcategory IS NULL

        assert resolve_category_id(db, "TopLevel", None) == cat_id

    @pytest.mark.unit
    def test_null_does_not_match_non_null(self, db: Database) -> None:
        from moneybin.services.categorization._shared import resolve_category_id

        svc = CategorizationService(db)
        svc.create_category("HasSub", subcategory="Specific")

        assert resolve_category_id(db, "HasSub", None) is None


class TestWriteCategorizationDualWrite:
    """Phase 1 dual-write: write_categorization + set_category_in_active_txn populate category_id."""

    @pytest.mark.unit
    def test_write_categorization_populates_category_id(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("Dual")
        db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, amount, transaction_date) "
            "VALUES ('txn-dual', -50, '2026-05-01')"
        )
        svc.write_categorization(
            transaction_id="txn-dual",
            category="Dual",
            subcategory=None,
            categorized_by="user",
        )
        row = db.execute(
            "SELECT category_id FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-dual'"
        ).fetchone()
        assert row == (cat_id,)

    @pytest.mark.unit
    def test_write_categorization_orphan_text_stays_null(self, db: Database) -> None:
        """Unresolvable category text writes a row with category_id IS NULL."""
        svc = CategorizationService(db)
        db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, amount, transaction_date) "
            "VALUES ('txn-orphan', -25, '2026-05-01')"
        )
        svc.write_categorization(
            transaction_id="txn-orphan",
            category="NeverDefined",
            subcategory=None,
            categorized_by="ai",
            confidence=0.5,
        )
        row = db.execute(
            "SELECT category_id, category FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-orphan'"
        ).fetchone()
        assert row == (None, "NeverDefined")

    @pytest.mark.unit
    def test_write_categorization_updates_category_id_on_conflict(
        self, db: Database
    ) -> None:
        """ON CONFLICT DO UPDATE must replace category_id alongside text."""
        svc = CategorizationService(db)
        svc.create_category("First")
        second = svc.create_category("Second")
        db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, amount, transaction_date) "
            "VALUES ('txn-conflict', -10, '2026-05-01')"
        )
        svc.write_categorization(
            transaction_id="txn-conflict",
            category="First",
            subcategory=None,
            categorized_by="ai",
            confidence=0.4,
        )
        svc.write_categorization(
            transaction_id="txn-conflict",
            category="Second",
            subcategory=None,
            categorized_by="user",
        )
        row = db.execute(
            "SELECT category, category_id FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-conflict'"
        ).fetchone()
        assert row == ("Second", second)

    @pytest.mark.unit
    def test_set_category_in_active_txn_populates_category_id(
        self, db: Database
    ) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("ActiveTxn")
        db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, amount, transaction_date) "
            "VALUES ('txn-active', -75, '2026-05-01')"
        )
        svc.set_category(
            "txn-active",
            category="ActiveTxn",
            subcategory=None,
            actor="test-user",
        )
        row = db.execute(
            "SELECT category_id FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-active'"
        ).fetchone()
        assert row == (cat_id,)


class TestCreateRulesDualWrite:
    """Phase 1 dual-write: create_rules populates category_id on categorization_rules."""

    @pytest.mark.unit
    def test_create_rules_populates_category_id(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("RulesCat")
        result = svc.create_rules([
            CategorizationRuleInput(
                name="Resolved rule",
                merchant_pattern="RULESCAT-PATTERN",
                category="RulesCat",
            )
        ])
        rule_id = result.rule_ids[0]
        row = db.execute(
            "SELECT category, category_id FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [rule_id],
        ).fetchone()
        assert row == ("RulesCat", cat_id)

    @pytest.mark.unit
    def test_create_rules_unresolved_text_leaves_fk_null(self, db: Database) -> None:
        """Unresolvable category text stores the text snapshot with category_id NULL."""
        svc = CategorizationService(db)
        result = svc.create_rules([
            CategorizationRuleInput(
                name="Orphan rule",
                merchant_pattern="ORPHAN-PATTERN",
                category="NeverDefined",
            )
        ])
        rule_id = result.rule_ids[0]
        row = db.execute(
            "SELECT category, category_id FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [rule_id],
        ).fetchone()
        assert row == ("NeverDefined", None)


class TestCreateMerchantDualWrite:
    """Phase 1 dual-write: create_merchant populates category_id on user_merchants."""

    @pytest.mark.unit
    def test_create_merchant_populates_category_id(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("Coffee")
        merchant_id = svc.create_merchant(
            raw_pattern="STARBUCKS",
            canonical_name="Starbucks",
            match_type="contains",
            category="Coffee",
            subcategory=None,
            created_by="user",
        )
        row = db.execute(
            "SELECT category_id FROM app.user_merchants WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        assert row == (cat_id,)

    @pytest.mark.unit
    def test_create_merchant_without_category_leaves_fk_null(
        self, db: Database
    ) -> None:
        """Merchants created without a default category have NULL text and NULL FK."""
        svc = CategorizationService(db)
        merchant_id = svc.create_merchant(
            raw_pattern="UNCATEGORIZED PATTERN",
            canonical_name="No-Category Merchant",
            match_type="contains",
            created_by="user",
        )
        row = db.execute(
            "SELECT category, category_id FROM app.user_merchants "
            "WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        assert row == (None, None)

    @pytest.mark.unit
    def test_create_merchant_with_unresolved_text_leaves_fk_null(
        self, db: Database
    ) -> None:
        """Unresolvable category text stores the text snapshot with category_id NULL."""
        svc = CategorizationService(db)
        merchant_id = svc.create_merchant(
            raw_pattern="MYSTERY MERCHANT",
            canonical_name="Mystery Merchant",
            match_type="contains",
            category="NeverDefined",
            subcategory=None,
            created_by="ai",
        )
        row = db.execute(
            "SELECT category, category_id FROM app.user_merchants "
            "WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        assert row == ("NeverDefined", None)


class TestDeleteCategory:
    """CategorizationService.delete_category — hard-delete with refuse/cascade."""

    @pytest.mark.unit
    def test_deletes_unreferenced_user_category(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("TestCat")
        svc.delete_category(cat_id)

        rows = db.execute(
            "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
        ).fetchall()
        assert rows == []

    @pytest.mark.unit
    def test_refuses_when_referenced_by_transactions(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("LinkedCat")
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES (?, ?, ?, 'user')",
            ["txn-test", "LinkedCat", cat_id],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "transactions" in str(exc_info.value)

    @pytest.mark.unit
    def test_refuses_when_referenced_by_budget(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("BudgetCat")
        db.execute(
            "INSERT INTO app.budgets "
            "(budget_id, category, category_id, monthly_amount, start_month) "
            "VALUES (?, ?, ?, ?, ?)",
            ["bdg-test", "BudgetCat", cat_id, "200.00", "2026-01"],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "budgets" in str(exc_info.value)

    @pytest.mark.unit
    def test_refuses_when_referenced_by_merchant(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("MerchCat")
        db.execute(
            "INSERT INTO app.user_merchants "
            "(merchant_id, raw_pattern, match_type, canonical_name, "
            " category, category_id, created_by) "
            "VALUES (?, ?, 'contains', ?, 'MerchCat', ?, 'user')",
            ["mer-test01234", "starbucks", "Starbucks", cat_id],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "merchants" in str(exc_info.value)

    @pytest.mark.unit
    def test_refuses_when_referenced_by_split(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("SplitCat")
        db.execute(
            "INSERT INTO app.transaction_splits "
            "(split_id, transaction_id, amount, category, category_id, created_by) "
            "VALUES (?, ?, ?, 'SplitCat', ?, 'cli')",
            ["spl-test01234", "txn-split", "10.00", cat_id],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "splits" in str(exc_info.value)

    @pytest.mark.unit
    def test_refuses_when_referenced_by_rule(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("RuleCat")
        db.execute(
            "INSERT INTO app.categorization_rules "
            "(rule_id, name, merchant_pattern, match_type, "
            " category, category_id) "
            "VALUES (?, 'Rule Test', 'pattern', 'contains', 'RuleCat', ?)",
            ["rul-test01234", cat_id],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "rules" in str(exc_info.value)

    @pytest.mark.unit
    def test_refuses_when_referenced_by_proposed_rule(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("ProposedCat")
        db.execute(
            "INSERT INTO app.proposed_rules "
            "(proposed_rule_id, merchant_pattern, match_type, "
            " category, category_id, status) "
            "VALUES (?, 'pattern', 'contains', 'ProposedCat', ?, 'pending')",
            ["pro-test01234", cat_id],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "proposed rules" in str(exc_info.value)

    @pytest.mark.unit
    def test_refuses_when_referenced_by_category_source_map(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("MappedCat")
        db.execute(
            "INSERT INTO app.category_source_map "
            "(source_type, source_category_code, code_level, category_id) "
            "VALUES (?, ?, ?, ?)",
            ["plaid", "FOOD_AND_DRINK_GROCERIES", "detailed", cat_id],
        )
        with pytest.raises(UserError) as exc_info:
            svc.delete_category(cat_id)
        assert exc_info.value.code == "CATEGORY_HAS_REFERENCES"
        assert "source mappings" in str(exc_info.value)

    @pytest.mark.unit
    def test_force_cascades_transaction_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("ForceCat")
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES (?, ?, ?, 'user')",
            ["txn-force", "ForceCat", cat_id],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
                ["txn-force"],
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_force_cascades_budget_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("BudgetForceCat")
        db.execute(
            "INSERT INTO app.budgets "
            "(budget_id, category, category_id, monthly_amount, start_month) "
            "VALUES (?, ?, ?, ?, ?)",
            ["bdg-force", "BudgetForceCat", cat_id, "100.00", "2026-01"],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.budgets WHERE budget_id = ?", ["bdg-force"]
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_force_cascades_merchant_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("MerchForceCat")
        db.execute(
            "INSERT INTO app.user_merchants "
            "(merchant_id, raw_pattern, match_type, canonical_name, "
            " category, category_id, created_by) "
            "VALUES (?, ?, 'contains', ?, 'MerchForceCat', ?, 'user')",
            ["mer-force01234", "starbucks", "Starbucks", cat_id],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.user_merchants WHERE merchant_id = ?",
                ["mer-force01234"],
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_force_cascades_split_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("SplitForceCat")
        db.execute(
            "INSERT INTO app.transaction_splits "
            "(split_id, transaction_id, amount, category, category_id, created_by) "
            "VALUES (?, ?, ?, 'SplitForceCat', ?, 'cli')",
            ["spl-force01234", "txn-split-f", "10.00", cat_id],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.transaction_splits WHERE split_id = ?",
                ["spl-force01234"],
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_force_cascades_rule_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("RuleForceCat")
        db.execute(
            "INSERT INTO app.categorization_rules "
            "(rule_id, name, merchant_pattern, match_type, "
            " category, category_id) "
            "VALUES (?, 'Rule Force', 'pattern', 'contains', 'RuleForceCat', ?)",
            ["rul-force01234", cat_id],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.categorization_rules WHERE rule_id = ?",
                ["rul-force01234"],
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_force_cascades_proposed_rule_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("ProposedForceCat")
        db.execute(
            "INSERT INTO app.proposed_rules "
            "(proposed_rule_id, merchant_pattern, match_type, "
            " category, category_id, status) "
            "VALUES (?, 'pattern', 'contains', 'ProposedForceCat', ?, 'pending')",
            ["pro-force01234", cat_id],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.proposed_rules WHERE proposed_rule_id = ?",
                ["pro-force01234"],
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_force_cascades_category_source_map_references(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("MappedForceCat")
        db.execute(
            "INSERT INTO app.category_source_map "
            "(source_type, source_category_code, code_level, category_id) "
            "VALUES (?, ?, ?, ?)",
            ["plaid", "FOOD_AND_DRINK_GROCERIES", "detailed", cat_id],
        )
        svc.delete_category(cat_id, force=True)

        assert (
            db.execute(
                "SELECT 1 FROM app.category_source_map WHERE source_category_code = ?",
                ["FOOD_AND_DRINK_GROCERIES"],
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cat_id]
            ).fetchall()
            == []
        )

    @pytest.mark.unit
    def test_subcategory_match_is_exact(self, db: Database) -> None:
        """Deleting (Childcare/Daycare) must not touch (Childcare/Preschool) refs."""
        svc = CategorizationService(db)
        cat_id = svc.create_category("Childcare", subcategory="Daycare")
        preschool_id = svc.create_category("Childcare", subcategory="Preschool")

        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, subcategory, category_id, categorized_by) "
            "VALUES (?, ?, ?, ?, 'user')",
            ["txn-preschool", "Childcare", "Preschool", preschool_id],
        )
        svc.delete_category(cat_id)

        assert db.execute(
            "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
            ["txn-preschool"],
        ).fetchall() == [(1,)]

    @pytest.mark.unit
    def test_refuses_default_category(self, db: Database) -> None:
        seed_categories_view(db)
        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).delete_category("FND")
        assert exc_info.value.code == "CATEGORY_IS_DEFAULT"

    @pytest.mark.unit
    def test_raises_for_unknown_category(self, db: Database) -> None:
        with pytest.raises(UserError) as exc_info:
            CategorizationService(db).delete_category("does-not-exist")
        assert exc_info.value.code == "CATEGORY_NOT_FOUND"

    @pytest.mark.unit
    def test_unforced_delete_does_not_touch_refs(self, db: Database) -> None:
        """force=False on an unreferenced category must not run cascade DELETEs.

        Regression guard for the TOCTOU window where cascade DELETEs ran
        unconditionally after the refs-check, even when force=False. A
        concurrent INSERT between check and delete would have been silently
        clobbered. After the fix, cascade DELETEs only run when force=True.
        """
        svc = CategorizationService(db)
        cat_id = svc.create_category("Untouchable")
        other_id = svc.create_category("OtherCat")
        # Seed an unrelated row that references OtherCat (not Untouchable).
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES (?, ?, ?, 'user')",
            ["txn-bystander", "OtherCat", other_id],
        )
        svc.delete_category(cat_id)
        # The bystander row must still be present — force=False didn't run
        # cascade DELETEs that could have raced with a concurrent INSERT.
        assert db.execute(
            "SELECT 1 FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-bystander'"
        ).fetchall() == [(1,)]

    @pytest.mark.unit
    def test_cascade_is_atomic(self, db: Database) -> None:
        """All cascade DELETEs run inside a single transaction.

        Guards against the half-applied cascade where transaction_categories
        gets deleted but user_categories doesn't (leaving orphaned snapshot
        rows + a still-present category). Verified indirectly: after a
        successful force-delete the user_categories row is gone AND the
        cascade DELETE ran — both states are observable.
        """
        svc = CategorizationService(db)
        cat_id = svc.create_category("AtomicCat")
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES (?, ?, ?, 'user')",
            ["txn-atomic", "AtomicCat", cat_id],
        )
        svc.delete_category(cat_id, force=True)
        assert (
            db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?",
                [cat_id],
            ).fetchall()
            == []
        )
        assert (
            db.execute(
                "SELECT 1 FROM app.transaction_categories "
                "WHERE transaction_id = 'txn-atomic'"
            ).fetchall()
            == []
        )


class TestWriteCategorizationSourceType:
    """MatchApplier.write_categorization persists source_type alongside categorized_by."""

    @pytest.mark.unit
    def test_write_persists_source_type(
        self, applier: MatchApplier, db: Database
    ) -> None:
        _insert_txn(db, "t1")  # existing helper in this file
        applier.write_categorization(
            transaction_id="t1",
            category="Coffee",
            subcategory=None,
            categorized_by="provider_native",
            source_type="plaid",
            confidence=0.90,
        )
        row = db.execute(
            "SELECT categorized_by, source_type FROM app.transaction_categories "
            "WHERE transaction_id='t1'"
        ).fetchone()
        assert row == ("provider_native", "plaid")

    @pytest.mark.unit
    def test_existing_writers_default_source_type_internal(
        self, applier: MatchApplier, db: Database
    ) -> None:
        _insert_txn(db, "t2")
        applier.write_categorization(
            transaction_id="t2",
            category="Coffee",
            subcategory=None,
            categorized_by="rule",
            rule_id="r1",
            confidence=1.0,
        )
        row = db.execute(
            "SELECT source_type FROM app.transaction_categories WHERE transaction_id='t2'"
        ).fetchone()
        assert row is not None
        assert row[0] == "internal"
