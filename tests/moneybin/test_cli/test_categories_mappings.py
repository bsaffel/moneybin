"""Tests for `categories mappings` CLI commands.

Mirrors test_merchants_links.py for the category-source-mapping curation
surface. CLI tests mock the service layer and test argument parsing, exit
codes, and output shape.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.categories.mappings import app
from moneybin.services.categorization.applier import SourceTermMapping
from moneybin.services.categorization.queries import UnmappedSourceTerm

runner = CliRunner()


def _stored(
    category_id: str | None,
    *,
    source_type: str = "csv",
    source_origin: str = "chase_credit",
    category: str = "Groceries",
    subcategory: str | None = None,
    categorized: int = 0,
    withdrawn_ids: frozenset[str] = frozenset(),
) -> SourceTermMapping:
    """A term as the service reports storing it."""
    return SourceTermMapping(
        source_type=source_type,
        source_origin=source_origin,
        category=category,
        subcategory=subcategory,
        category_id=category_id,
        categorized=categorized,
        withdrawn_ids=withdrawn_ids,
    )


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
        """Empty queue exits 0 and says so rather than printing nothing."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_list.return_value = []

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "No unmapped terms." in result.output

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
                source_type="csv",
                source_origin="chase_credit",
                category="Groceries",
                subcategory=None,
                transaction_count=42,
                suggestions=["Food & Dining"],
            )
        ]

        # Wide terminal: the default 80 columns truncates the table's cells.
        result = runner.invoke(app, ["pending"], env={"COLUMNS": "200"})
        assert result.exit_code == 0
        assert "csv" in result.output
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
                source_type="excel",
                source_origin="amex_gold",
                category="Auto",
                subcategory="Gas",
                transaction_count=7,
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
        assert terms[0]["source_type"] == "excel"
        assert terms[0]["source_origin"] == "amex_gold"
        assert terms[0]["category"] == "Auto"
        assert terms[0]["subcategory"] == "Gas"
        assert terms[0]["transaction_count"] == 7
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
        mock_resolve.return_value = _stored("cat-groceries")

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
            source_type=None,
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
    def test_set_passes_an_explicit_source_type(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored("cat-groceries", source_type="excel")

        result = runner.invoke(
            app,
            [
                "set",
                "--source-type",
                "excel",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--into",
                "cat-groceries",
            ],
        )

        assert result.exit_code == 0
        assert mock_resolve.call_args.kwargs["source_type"] == "excel"

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_new_calls_service_with_new_category(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored(
            "cat-new123",
            source_origin="mint",
            category="Home Improvement",
            subcategory="Tools",
        )

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
            source_type=None,
            source_origin="mint",
            category="Home Improvement",
            subcategory="Tools",
            category_id=None,
            new_category="Housing",
            ignore=False,
            actor="cli",
        )

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_ignore_calls_service_and_reports_ignored(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored(
            None, category="Uncategorized", withdrawn_ids=frozenset({"t1", "t2"})
        )

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Uncategorized",
                "--ignore",
                "--output",
                "json",
            ],
        )

        assert result.exit_code == 0
        mock_resolve.assert_called_once_with(
            source_type=None,
            source_origin="chase_credit",
            category="Uncategorized",
            subcategory=None,
            category_id=None,
            new_category=None,
            ignore=True,
            actor="cli",
        )
        data = json.loads(result.output)["data"]
        assert data["action"] == "ignored"
        assert data["category_id"] is None
        assert (data["categorized"], data["recategorized"]) == (0, 2)

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_ignore_text_receipt_has_no_mapped_to_line(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored(None, category="Uncategorized")

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Uncategorized",
                "--ignore",
            ],
        )

        assert result.exit_code == 0
        assert "Category term ignored" in result.output
        assert "Mapped to" not in result.output

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_text_receipt_reports_both_sweep_counts(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored(
            "cat-groceries", categorized=4, withdrawn_ids=frozenset({"t1"})
        )

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
        assert "categorized 4, recategorized 1" in result.output

    def test_set_requires_into_or_new(self) -> None:
        result = runner.invoke(
            app,
            ["set", "--namespace", "chase_credit", "--category", "Groceries"],
        )
        assert result.exit_code == 2

    @pytest.mark.parametrize(
        "other", [["--into", "cat-groceries"], ["--new", "New One"]]
    )
    def test_set_rejects_ignore_with_another_target(self, other: list[str]) -> None:
        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "Groceries",
                "--ignore",
                *other,
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 2
        assert json.loads(result.stdout)["error"]["code"] == "mutation_invalid_input"

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

    def test_set_usage_error_under_json_is_an_error_envelope(self) -> None:
        """An agent asking for JSON gets a parseable error, not bare stderr text."""
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
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 2
        parsed = json.loads(result.stdout)
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_json_output_shape(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored("cat-groceries")

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
        assert parsed["data"]["categorized"] == 0
        assert parsed["data"]["recategorized"] == 0

    @patch("moneybin.cli.commands.categories.mappings.get_database")
    @patch("moneybin.services.categorization.CategorizationService.resolve_source_term")
    def test_set_receipt_names_the_stored_term_not_the_typed_one(
        self, mock_resolve: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """A padded term is stored trimmed; the receipt must name that row."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_resolve.return_value = _stored(
            "cat-groceries", category="Groceries", subcategory="Produce"
        )

        result = runner.invoke(
            app,
            [
                "set",
                "--namespace",
                "chase_credit",
                "--category",
                "  Groceries\t",
                "--subcategory",
                " Produce ",
                "--into",
                "cat-groceries",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["data"]["category"] == "Groceries"
        assert parsed["data"]["subcategory"] == "Produce"
