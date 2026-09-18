"""Human output regressions for category reads."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


def test_categories_list_offers_no_pager_without_initializing_runtime() -> None:
    """A finite category list exposes the shared paging escape hatch."""
    result = CliRunner().invoke(app, ["categories", "list", "--help"])

    assert result.exit_code == 0
    assert "--no-pager" in result.stdout


@patch("moneybin.cli.commands.categories.get_database")
@patch("moneybin.services.categorization.CategorizationService")
@pytest.mark.parametrize(
    ("args", "expected_action"),
    [
        (
            ["categories", "list", "--no-pager"],
            "Try: moneybin categories list --include-inactive",
        ),
        (
            ["categories", "list", "--include-inactive", "--no-pager"],
            "Try: moneybin categories create --help",
        ),
    ],
)
def test_categories_list_empty_explains_the_scope(
    service_class: MagicMock,
    database: MagicMock,
    args: list[str],
    expected_action: str,
) -> None:
    database.return_value = MagicMock()
    service_class.return_value.get_all_categories.return_value = MagicMock(
        categories=[]
    )

    result = CliRunner().invoke(app, args)

    assert result.exit_code == 0
    assert "No categories match this scope." in result.stdout
    assert expected_action in result.stdout


@patch("moneybin.cli.commands.categories.get_database")
@patch("moneybin.services.categorization.CategorizationService")
def test_categories_list_renders_labeled_rows(
    service_class: MagicMock, database: MagicMock
) -> None:
    """The human result keeps category identity and activation state visible."""
    category = MagicMock(
        category_id="cat_food",
        category="Food",
        subcategory="Groceries",
        is_active=True,
    )
    service_class.return_value.get_all_categories.return_value = MagicMock(
        categories=[category]
    )
    database.return_value = MagicMock()

    result = CliRunner().invoke(app, ["categories", "list", "--no-pager"])

    assert result.exit_code == 0, result.stderr
    assert "category_id" in result.stdout
    assert "status" in result.stdout
    assert "cat_food" in result.stdout
