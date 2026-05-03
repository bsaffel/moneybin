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
from moneybin.errors import UserError  # noqa: F401  # used by Tasks 2–5
from moneybin.services.categorization_service import (
    CategorizationRuleInput,
    CategorizationService,  # noqa: F401  # used by Tasks 2–5
    validate_rule_items,
)
from tests.moneybin.db_helpers import create_core_tables


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


# Placeholder sections — Tasks 2/3/4/5 add tests below this line.
