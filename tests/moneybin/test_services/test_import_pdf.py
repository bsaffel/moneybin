"""Integration tests for ImportService PDF import path (Phase 1: seed-only)."""

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin.database import Database
from moneybin.services.import_service import (
    ImportService,
    _pdf_alias,  # type: ignore[reportPrivateUsage]
)


@pytest.mark.integration
def test_import_pdf_lands_as_seed(db: Database, simple_statement_pdf: Path) -> None:
    result = ImportService(db).import_file(simple_statement_pdf, refresh=False)
    assert result.file_type == "pdf"
    assert result.import_id is not None
    # View name = "pdf_" + _pdf_alias(None, fixture).
    # For "simple_statement.pdf", that resolves to "pdf_simple_statement".
    row = db.execute("SELECT COUNT(*) FROM raw.pdf_simple_statement").fetchone()
    assert row is not None
    assert row[0] == 3  # 3 transaction rows in the simple_statement.pdf fixture


@pytest.mark.integration
def test_import_pdf_is_revertible(db: Database, simple_statement_pdf: Path) -> None:
    svc = ImportService(db)
    result = svc.import_file(simple_statement_pdf, refresh=False)
    assert result.import_id is not None
    out = svc.revert(result.import_id)
    assert out["status"] == "reverted"
    row = db.execute("SELECT COUNT(*) FROM raw.pdf_seeds").fetchone()
    assert row is not None
    assert row[0] == 0
    # Revert must also drop the auto-generated raw.pdf_<alias> view.
    view_exists = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'pdf_simple_statement'"
    ).fetchone()
    assert view_exists is not None
    assert view_exists[0] == 0, (
        "revert should drop the auto-generated raw.pdf_<alias> view"
    )


@pytest.mark.integration
def test_import_pdf_zero_rows_raises(db: Database, empty_statement_pdf: Path) -> None:
    """Importing a text-only PDF with no tables must raise, not silently succeed."""
    with pytest.raises(ValueError, match="No tables extracted"):
        ImportService(db).import_file(empty_statement_pdf, refresh=False)
    # No degenerate view should have been created during the failed import.
    view_count = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name LIKE 'pdf_%'"
    ).fetchone()
    assert view_count is not None
    assert view_count[0] == 0, "failed import must not leave orphan views"


@pytest.mark.parametrize(
    ("alias", "filename", "expected"),
    [
        ("ACME Bank", "ignored.pdf", "acme_bank"),
        (None, "simple_statement.pdf", "simple_statement"),
        (None, "2024_Q4.pdf", "2024_q4"),
        (None, ".pdfrc", "pdfrc"),
        ("Wells Fargo 2024-Q1", "ignored.pdf", "wells_fargo_2024_q1"),
        (None, ("a" * 80) + ".pdf", "a" * 59),
    ],
    ids=[
        "explicit_alias",
        "stem_clean_letter_start",
        "stem_leading_digit_no_prefix",
        "stem_leading_dot_stripped_letter_start",
        "explicit_alias_with_spaces_and_hyphen",
        "long_stem_truncated_to_59_chars",
    ],
)
def test_pdf_alias_resolves(alias: str | None, filename: str, expected: str) -> None:
    assert _pdf_alias(alias, Path(filename)) == expected


@pytest.mark.integration
def test_import_pdf_cleans_orphans_on_view_failure(
    db: Database, simple_statement_pdf: Path
) -> None:
    """Rows written to raw.pdf_seeds must be deleted if view creation fails after the seed insert."""
    with patch(
        "moneybin.extractors.pdf.seed_store.generate_seed_view_sql",
        side_effect=ValueError("forced for test"),
    ):
        with pytest.raises(ValueError, match="forced for test"):
            ImportService(db).import_file(simple_statement_pdf, refresh=False)
    # The just-inserted rows must be cleaned up — no orphan rows should remain.
    row = db.execute("SELECT COUNT(*) FROM raw.pdf_seeds").fetchone()
    assert row is not None
    assert row[0] == 0


@pytest.mark.integration
def test_revert_preserves_view_when_other_imports_remain(
    db: Database, simple_statement_pdf: Path, tmp_path: Path
) -> None:
    """Reverting one PDF import should not drop the view if another import shares its alias."""
    # Two physical files that resolve to the same alias (same stem)
    a = tmp_path / "a" / "simple_statement.pdf"
    b = tmp_path / "b" / "simple_statement.pdf"
    a.parent.mkdir()
    b.parent.mkdir()
    shutil.copy(simple_statement_pdf, a)
    shutil.copy(simple_statement_pdf, b)

    svc = ImportService(db)
    result_a = svc.import_file(a, refresh=False)
    result_b = svc.import_file(b, refresh=False)
    assert result_a.import_id is not None
    assert result_b.import_id is not None

    svc.revert(result_a.import_id)

    # View must still exist because result_b's import remains complete.
    view_count = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'pdf_simple_statement'"
    ).fetchone()
    assert view_count is not None
    assert view_count[0] == 1, (
        "view should remain when another import still references the alias"
    )
    # And the view still returns rows from the second import.
    rows = db.execute("SELECT COUNT(*) FROM raw.pdf_simple_statement").fetchone()
    assert rows is not None
    assert rows[0] > 0
