"""Focused CLI contracts for category and merchant taxonomy commands."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.categories import app as categories_app
from moneybin.cli.commands.merchants import app as merchants_app
from moneybin.privacy.payloads.categories import (
    CategoriesPayload,
    CategoryRow,
    MerchantRow,
    MerchantsPayload,
)

runner = CliRunner()


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.categories.get_database")
def test_categories_list_json_envelope_and_service_args(
    mock_get_db: MagicMock, mock_service_cls: MagicMock
) -> None:
    """List emits the standard envelope and forwards the inactive filter."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service_cls.return_value.get_all_categories.return_value = CategoriesPayload(
        categories=[
            CategoryRow(
                category_id="FOOD",
                category="Food",
                subcategory=None,
                description=None,
                is_default=True,
                is_active=True,
            )
        ]
    )

    result = runner.invoke(
        categories_app,
        ["list", "--include-inactive", "--output", "json", "--quiet"],
    )

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["data"]["categories"][0]["category_id"] == "FOOD"
    assert envelope["summary"]["sensitivity"] == "low"
    mock_service_cls.return_value.get_all_categories.assert_called_once_with(
        include_inactive=True
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.categories.get_database")
def test_categories_create_forwards_subcategory_shape(
    mock_get_db: MagicMock, mock_service_cls: MagicMock
) -> None:
    """A parent creates a subcategory using the service's category-first shape."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service_cls.return_value.create_category.return_value = "cat-1"

    result = runner.invoke(
        categories_app,
        ["create", "Coffee", "--parent", "Food"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "cat-1"
    mock_service_cls.return_value.create_category.assert_called_once_with(
        "Food",
        subcategory="Coffee",
        actor="cli",
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.categories.get_database")
def test_categories_set_forwards_inactive_state(
    mock_get_db: MagicMock, mock_service_cls: MagicMock
) -> None:
    """The inactive flag reaches the category toggle service unchanged."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(categories_app, ["set", "cat-1", "--inactive"])

    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "cat-1"
    mock_service_cls.return_value.toggle_category.assert_called_once_with(
        "cat-1",
        is_active=False,
        actor="cli",
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.merchants.get_database")
def test_merchants_list_json_envelope(
    mock_get_db: MagicMock, mock_service_cls: MagicMock
) -> None:
    """Merchant list emits its MEDIUM envelope while accepting --quiet."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service_cls.return_value.list_merchants.return_value = MerchantsPayload(
        merchants=[
            MerchantRow(
                merchant_id="merchant-1",
                raw_pattern="COFFEE",
                match_type="contains",
                canonical_name="Coffee Shop",
                category="Food",
                subcategory="Coffee",
            )
        ]
    )

    result = runner.invoke(
        merchants_app,
        ["list", "--output", "json", "--quiet"],
    )

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["data"]["merchants"][0]["merchant_id"] == "merchant-1"
    assert envelope["summary"]["sensitivity"] == "medium"
    mock_service_cls.return_value.list_merchants.assert_called_once_with()


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.merchants.get_database")
def test_merchants_create_forwards_mapping_fields(
    mock_get_db: MagicMock, mock_service_cls: MagicMock
) -> None:
    """Create pins the CLI's contains-match defaults and service arguments."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service_cls.return_value.create_merchant.return_value = "merchant-1"

    result = runner.invoke(
        merchants_app,
        [
            "create",
            "COFFEE",
            "Coffee Shop",
            "--default-category",
            "Food",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "merchant-1"
    mock_service_cls.return_value.create_merchant.assert_called_once_with(
        "COFFEE",
        "Coffee Shop",
        match_type="contains",
        category="Food",
        created_by="user",
        actor="cli",
    )
