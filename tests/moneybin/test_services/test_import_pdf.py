"""Integration tests for ImportService PDF import path (Phase 1: seed-only)."""

from pathlib import Path

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
    assert row[0] == 3  # 3 transaction rows in the fixture (T4 set this)


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


@pytest.mark.parametrize(
    ("alias", "filename", "expected"),
    [
        ("ACME Bank", "ignored.pdf", "acme_bank"),
        (None, "simple_statement.pdf", "simple_statement"),
        (None, "2024_Q4.pdf", "pdf_2024_q4"),
        (None, ".pdfrc", "pdfrc"),
        ("Wells Fargo 2024-Q1", "ignored.pdf", "wells_fargo_2024_q1"),
        (None, ("a" * 80) + ".pdf", "a" * 59),
    ],
    ids=[
        "explicit_alias",
        "stem_clean_letter_start",
        "stem_leading_digit_gets_pdf_prefix",
        "stem_leading_dot_stripped_letter_start",
        "explicit_alias_with_spaces_and_hyphen",
        "long_stem_truncated_to_59_chars",
    ],
)
def test_pdf_alias_resolves(alias: str | None, filename: str, expected: str) -> None:
    assert _pdf_alias(alias, Path(filename)) == expected
