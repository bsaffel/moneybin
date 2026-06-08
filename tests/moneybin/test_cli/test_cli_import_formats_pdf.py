"""Tests for PDF format support in `import formats list` and `import formats show`."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app as import_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pdf_format(**kwargs: Any) -> Any:
    """Factory for PdfFormat instances with sensible defaults."""
    from moneybin.repositories.pdf_formats_repo import PdfFormat

    defaults: dict[str, Any] = {
        "name": "chase_a1b2c3d4e5f6",
        "institution_name": "Chase",
        "document_kind": "transactions",
        "layout_fingerprint": {"issuer": "Chase"},
        "front_end": "pdfplumber",
        "extraction_recipe": {"fields": []},
        "routing": "transactions",
        "field_mapping": None,
        "seed_alias": None,
        "sign_convention": "negative_is_expense",
        "date_format": None,
        "number_format": "us",
        "source": "detected",
        "version": 1,
        "times_used": 3,
        "last_used_at": datetime(2026, 5, 30, 10, 0, 0),
        "created_at": datetime(2026, 5, 1, 9, 0, 0),
        "updated_at": datetime(2026, 5, 30, 10, 0, 0),
    }
    defaults.update(kwargs)
    return PdfFormat(**defaults)


@pytest.fixture()
def runner() -> CliRunner:
    """Provide a Typer CliRunner for invoking the import app."""
    return CliRunner()


def _mock_get_database(mocker: Any, pdf_formats: list[Any]) -> None:
    """Patch get_database so CLI commands see a mocked DB with the given PDF formats.

    Tabular formats are left to the real built-in loader. The DB mock only
    needs to serve PdfFormatsRepo.list_all() and not raise on formats_from_db.
    get_database is a deferred import inside command bodies, so we patch at the
    moneybin.database level rather than the import_cmd module level.
    """
    mock_db = MagicMock()

    # PdfFormatsRepo.list_all() executes SQL; stub the execute chain so it
    # returns our PDF formats. We patch PdfFormatsRepo.list_all directly to
    # avoid coupling to the SQL execution details.
    mocker.patch(
        "moneybin.repositories.pdf_formats_repo.PdfFormatsRepo.list_all",
        return_value=pdf_formats,
    )

    # Also stub load_formats_from_db so it returns {} (no extra user tabular
    # formats) without hitting a real DB file.
    mocker.patch(
        "moneybin.extractors.tabular.formats.load_formats_from_db",
        return_value={},
    )

    @contextmanager
    def _fake_cm(*_args: Any, **_kwargs: Any):  # type: ignore[no-untyped-def]
        yield mock_db

    # get_database is imported inside command bodies — patch at the source module.
    mocker.patch(
        "moneybin.database.get_database",
        side_effect=_fake_cm,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFormatsListPdf:
    """formats list --type=pdf / --type=tabular / default (all)."""

    def test_default_includes_both_types_text(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """Default `formats list` shows tabular and PDF sections in text mode."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(import_app, ["formats", "list"])
        assert result.exit_code == 0, result.output
        # Tabular section present (built-in format)
        assert "tiller" in result.output.lower() or "Tabular formats" in result.output
        # PDF section present
        assert "PDF formats" in result.output
        assert "chase_a1b2c3d4e5f6" in result.output

    def test_default_includes_both_types_json(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """Default `formats list --output json` includes both types with discriminator."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(import_app, ["formats", "list", "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        formats = data["formats"]
        types = {f["type"] for f in formats}
        assert "tabular" in types, "Expected tabular rows in JSON output"
        assert "pdf" in types, "Expected pdf rows in JSON output"

        # PDF row has expected fields
        pdf_rows = [f for f in formats if f["type"] == "pdf"]
        assert len(pdf_rows) == 1
        pdf_row = pdf_rows[0]
        assert pdf_row["name"] == "chase_a1b2c3d4e5f6"
        assert pdf_row["institution"] == "Chase"
        assert pdf_row["routing"] == "transactions"
        assert pdf_row["version"] == 1
        assert pdf_row["times_used"] == 3
        assert pdf_row["last_used"] == "2026-05-30"

    def test_filter_pdf_excludes_tabular(self, runner: CliRunner, mocker: Any) -> None:
        """--type=pdf shows only PDF rows; no tabular format names in output."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(import_app, ["formats", "list", "--type", "pdf"])
        assert result.exit_code == 0, result.output
        assert "chase_a1b2c3d4e5f6" in result.output
        # tiller is a built-in tabular format that should be absent
        assert "tiller" not in result.output

    def test_filter_pdf_excludes_tabular_json(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """--type=pdf --output json returns only pdf-typed rows."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(
            import_app, ["formats", "list", "--type", "pdf", "--output", "json"]
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        for row in data["formats"]:
            assert row["type"] == "pdf", f"Expected only pdf rows; got {row['type']!r}"

    def test_filter_tabular_excludes_pdf(self, runner: CliRunner, mocker: Any) -> None:
        """--type=tabular shows only tabular rows; PDF format name absent."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(import_app, ["formats", "list", "--type", "tabular"])
        assert result.exit_code == 0, result.output
        assert "chase_a1b2c3d4e5f6" not in result.output

    def test_filter_tabular_excludes_pdf_json(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """--type=tabular --output json returns only tabular-typed rows."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(
            import_app, ["formats", "list", "--type", "tabular", "--output", "json"]
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        for row in data["formats"]:
            assert row["type"] == "tabular", (
                f"Expected only tabular rows; got {row['type']!r}"
            )

    def test_no_pdf_formats_no_crash(self, runner: CliRunner, mocker: Any) -> None:
        """--type=pdf with empty DB exits 0 (no crash, no tabular section)."""
        _mock_get_database(mocker, [])

        result = runner.invoke(import_app, ["formats", "list", "--type", "pdf"])
        assert result.exit_code == 0, result.output
        # No PDF formats means warning or empty section, not a crash
        assert "chase_a1b2c3d4e5f6" not in result.output


class TestFormatsShowPdf:
    """formats show resolves PDF formats when tabular lookup misses."""

    def test_resolves_pdf_format_text(self, runner: CliRunner, mocker: Any) -> None:
        """Formats show <pdf_name> renders PDF details in text mode."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(import_app, ["formats", "show", "chase_a1b2c3d4e5f6"])
        assert result.exit_code == 0, result.output
        assert "chase_a1b2c3d4e5f6" in result.output
        assert "Chase" in result.output
        assert "transactions" in result.output  # routing
        assert "pdfplumber" in result.output

    def test_resolves_pdf_format_json(self, runner: CliRunner, mocker: Any) -> None:
        """Formats show <pdf_name> --output json includes type=pdf and recipe."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(
            import_app, ["formats", "show", "chase_a1b2c3d4e5f6", "--output", "json"]
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        fmt = data["format"]
        assert fmt["type"] == "pdf"
        assert fmt["name"] == "chase_a1b2c3d4e5f6"
        assert fmt["routing"] == "transactions"
        assert "extraction_recipe" in fmt

    def test_unknown_name_lists_both_namespaces(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """Formats show <unknown> --output json hint includes both tabular + PDF names."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        # Use JSON output so the error hint is in the structured response on stdout.
        result = runner.invoke(
            import_app,
            ["formats", "show", "definitely_not_there", "--output", "json"],
        )
        assert result.exit_code == 1
        data = json.loads(result.output)
        hint = data.get("error", {}).get("hint", "")
        # hint must include the PDF format name alongside tabular names
        assert "chase_a1b2c3d4e5f6" in hint

    def test_tabular_format_still_resolves(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """Existing tabular formats still resolve when PDF formats are present."""
        pdf_fmt = _make_pdf_format()
        _mock_get_database(mocker, [pdf_fmt])

        result = runner.invoke(import_app, ["formats", "show", "tiller"])
        assert result.exit_code == 0, result.output
        assert "Tiller" in result.output
        assert "Field mapping" in result.output
