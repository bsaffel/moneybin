"""Tests for `categories mappings` CLI commands.

Mirrors test_merchants_links.py for the category-source-mapping curation
surface. CLI tests mock the service layer and test argument parsing, exit
codes, and output shape.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.categories.mappings import app
from moneybin.services.categorization.queries import UnmappedSourceTerm

runner = CliRunner()


# ---------------------------------------------------------------------------
# mappings pending
# ---------------------------------------------------------------------------


class TestMappingsPending:
    """Tests for `categories mappings pending`."""

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch(
        "moneybin.services.categorization.CategorizationService.list_unmapped_source_terms"
    )
    def test_pending_empty(self, mock_list: MagicMock, mock_get_db: MagicMock) -> None:
        """Empty queue exits 0 with no output."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_list.return_value = []

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch(
        "moneybin.services.categorization.CategorizationService.list_unmapped_source_terms"
    )
    def test_pending_shows_namespace_and_category(
        self, mock_list: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_list.return_value = [
            UnmappedSourceTerm(
                source_origin="chase_credit",
                category="Groceries",
                subcategory=None,
                row_count=42,
                suggestions=["Food & Dining"],
            )
        ]

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "chase_credit" in result.output
        assert "Groceries" in result.output
        assert "42" in result.output

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch(
        "moneybin.services.categorization.CategorizationService.list_unmapped_source_terms"
    )
    def test_pending_json_output_shape(
        self, mock_list: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_list.return_value = [
            UnmappedSourceTerm(
                source_origin="amex_gold",
                category="Auto",
                subcategory="Gas",
                row_count=7,
                suggestions=["Transportation"],
            )
        ]

        result = runner.invoke(app, ["pending", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "data" in parsed
        assert "terms" in parsed["data"]
        terms = parsed["data"]["terms"]
        assert len(terms) == 1
        assert terms[0]["source_origin"] == "amex_gold"
        assert terms[0]["category"] == "Auto"
        assert terms[0]["subcategory"] == "Gas"
        assert terms[0]["row_count"] == 7
        assert terms[0]["suggestions"] == ["Transportation"]

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch(
        "moneybin.services.categorization.CategorizationService.list_unmapped_source_terms"
    )
    def test_pending_namespace_filter_forwarded(
        self, mock_list: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_list.return_value = []

        runner.invoke(app, ["pending", "--namespace", "chase_credit"])
        mock_list.assert_called_once_with(namespace="chase_credit")


# ---------------------------------------------------------------------------
# mappings set
# ---------------------------------------------------------------------------


class TestMappingsSet:
    """Tests for `categories mappings set`."""

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_into_calls_service_with_category_id(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = "cat-groceries"

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--into",
                "cat-groceries",
            ],
        )
        assert result.exit_code == 0
        mock_resolve.assert_called_once_with(
            source_origin="chase_credit",
            category="Groceries",
            subcategory=None,
            category_id="cat-groceries",
            new_category=None,
            ignore=False,
            actor="cli",
        )

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_new_calls_service_with_new_category(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = "cat-new123"

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "mint",
                "--category",
                "Home Improvement",
                "--subcategory",
                "Tools",
                "--new",
                "Housing",
            ],
        )
        assert result.exit_code == 0
        mock_resolve.assert_called_once_with(
            source_origin="mint",
            category="Home Improvement",
            subcategory="Tools",
            category_id=None,
            new_category="Housing",
            ignore=False,
            actor="cli",
        )

    def test_set_requires_into_or_new(self) -> None:
        result = runner.invoke(
            app,
            ["set", "--namespace", "chase_credit", "--category", "Groceries"],
        )
        assert result.exit_code == 2

    def test_set_rejects_both_flags(self) -> None:
        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--into",
                "cat-groceries",
                "--new",
                "New One",
            ],
        )
        assert result.exit_code == 2

    def test_set_rejects_into_and_ignore(self) -> None:
        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--into",
                "cat-groceries",
                "--ignore",
            ],
        )
        assert result.exit_code == 2

    def test_set_rejects_new_and_ignore(self) -> None:
        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--new",
                "New One",
                "--ignore",
            ],
        )
        assert result.exit_code == 2

    def test_set_rejects_all_three_flags(self) -> None:
        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--into",
                "cat-groceries",
                "--new",
                "New One",
                "--ignore",
            ],
        )
        assert result.exit_code == 2

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_ignore_calls_service_with_ignore_true(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = None

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Junk Label",
                "--ignore",
            ],
        )
        assert result.exit_code == 0
        mock_resolve.assert_called_once_with(
            source_origin="chase_credit",
            category="Junk Label",
            subcategory=None,
            category_id=None,
            new_category=None,
            ignore=True,
            actor="cli",
        )

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_ignore_json_output_shape(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = None

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Junk Label",
                "--ignore",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["data"]["source_origin"] == "chase_credit"
        assert parsed["data"]["category"] == "Junk Label"
        assert parsed["data"]["category_id"] is None
        assert parsed["data"]["action"] == "ignored"

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_json_output_shape(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = "cat-groceries"

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--into",
                "cat-groceries",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "data" in parsed
        assert parsed["data"]["source_origin"] == "chase_credit"
        assert parsed["data"]["category"] == "Groceries"
        assert parsed["data"]["category_id"] == "cat-groceries"
        assert parsed["data"]["action"] == "mapped"
