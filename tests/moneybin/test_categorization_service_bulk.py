"""Unit tests for BulkCategorizationItem and _validate_items."""

from __future__ import annotations

import pytest

from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    _validate_items,  # pyright: ignore[reportPrivateUsage]  # testing module-private helper directly
)


class TestBulkCategorizationItem:
    """Tests for the BulkCategorizationItem Pydantic model."""

    def test_valid_item_with_subcategory(self) -> None:
        item = BulkCategorizationItem(
            transaction_id="csv_abc123",
            category="Food",
            subcategory="Groceries",
        )
        assert item.transaction_id == "csv_abc123"
        assert item.category == "Food"
        assert item.subcategory == "Groceries"

    def test_subcategory_optional(self) -> None:
        item = BulkCategorizationItem(transaction_id="csv_abc", category="Food")
        assert item.subcategory is None

    def test_strips_whitespace(self) -> None:
        item = BulkCategorizationItem(
            transaction_id="  csv_abc  ",
            category="  Food  ",
            subcategory="  Groceries  ",
        )
        assert item.transaction_id == "csv_abc"
        assert item.category == "Food"
        assert item.subcategory == "Groceries"

    def test_empty_transaction_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="", category="Food")

    def test_empty_category_rejected(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="csv_abc", category="")

    def test_empty_subcategory_rejected(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(
                transaction_id="csv_abc", category="Food", subcategory=""
            )

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(
                transaction_id="csv_abc",
                category="Food",
                notes="hallucinated by an LLM",  # type: ignore[call-arg]
            )

    def test_transaction_id_max_length(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="x" * 65, category="Food")

    def test_category_max_length(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="csv_abc", category="x" * 101)


class TestValidateItems:
    """Tests for the _validate_items helper function."""

    def test_all_valid_returns_items_no_errors(self) -> None:
        raw = [
            {"transaction_id": "csv_abc", "category": "Food"},
            {
                "transaction_id": "csv_def",
                "category": "Transport",
                "subcategory": "Gas",
            },
        ]
        items, parse_errors = _validate_items(raw)
        assert len(items) == 2
        assert items[0].transaction_id == "csv_abc"
        assert items[1].subcategory == "Gas"
        assert parse_errors == []

    def test_per_item_validation_accumulates_errors(self) -> None:
        raw = [
            {"transaction_id": "csv_abc", "category": "Food"},
            {"transaction_id": "", "category": "Transport"},  # invalid
            {"transaction_id": "csv_def", "category": ""},  # invalid
            {"transaction_id": "csv_ghi", "category": "Shopping"},
        ]
        items, parse_errors = _validate_items(raw)
        assert len(items) == 2
        assert {i.transaction_id for i in items} == {"csv_abc", "csv_ghi"}
        assert len(parse_errors) == 2
        assert parse_errors[0]["transaction_id"] == "(missing)"
        assert "transaction_id" in parse_errors[0]["reason"]
        assert parse_errors[1]["transaction_id"] == "csv_def"
        assert "category" in parse_errors[1]["reason"]

    def test_unknown_field_accumulates(self) -> None:
        raw = [{"transaction_id": "csv_abc", "category": "Food", "notes": "no"}]
        items, parse_errors = _validate_items(raw)
        assert items == []
        assert len(parse_errors) == 1
        assert "notes" in parse_errors[0]["reason"]

    def test_non_dict_row_accumulates(self) -> None:
        raw = [{"transaction_id": "csv_abc", "category": "Food"}, "not a dict"]
        items, parse_errors = _validate_items(raw)
        assert len(items) == 1
        assert len(parse_errors) == 1

    def test_top_level_not_a_list_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON array"):
            _validate_items({"items": []})  # type: ignore[arg-type]
