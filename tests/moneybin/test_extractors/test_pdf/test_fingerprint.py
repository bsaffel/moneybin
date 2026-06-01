"""Tests for layout fingerprint computation and format lookup."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.fingerprint import (
    _page_bucket,  # pyright: ignore[reportPrivateUsage]
    compute_fingerprint,
    match_format,
)
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RECIPE: dict[str, Any] = {
    "fields": [
        {"name": "date", "anchor": "Date", "type": "date"},
        {"name": "amount", "anchor": "Amount", "type": "decimal"},
    ],
    "routing": "transactions",
}


def _make_doc(
    text_lines: list[str] | None = None,
    tables: list[PdfTable] | None = None,
) -> PdfDocument:
    return PdfDocument(
        source_file="stmt.pdf",
        text_lines=text_lines or [],
        tables=tables or [],
    )


def _chase_doc(page: int = 1) -> PdfDocument:
    """Single-page Chase statement with standard transaction headers."""
    return _make_doc(
        text_lines=["Chase Bank Statement", "Account Summary"],
        tables=[
            PdfTable(
                page=page,
                header=["Date", "Description", "Amount"],
                rows=[["01/01/2024", "Coffee", "-4.50"]],
            )
        ],
    )


# ---------------------------------------------------------------------------
# Unit: compute_fingerprint — same layout hashes identically across months
# ---------------------------------------------------------------------------


def test_same_layout_two_months_fingerprint_equal() -> None:
    """Same issuer + headers + page count → identical fingerprint."""
    jan = _make_doc(
        text_lines=["Chase Bank Statement", "January 2024"],
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[["01/15/2024", "Coffee", "-4.50"]],
            )
        ],
    )
    feb = _make_doc(
        text_lines=["Chase Bank Statement", "February 2024"],
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[
                    ["02/10/2024", "Paycheck", "1500.00"],
                    ["02/11/2024", "Rent", "-1200.00"],
                ],
            )
        ],
    )
    assert compute_fingerprint(jan) == compute_fingerprint(feb)


def test_different_issuer_same_headers_fingerprints_differ() -> None:
    """Swapping the issuer in text_lines produces a different fingerprint."""
    chase_doc = _make_doc(
        text_lines=["Chase Bank Statement"],
        tables=[PdfTable(page=1, header=["Date", "Description", "Amount"], rows=[])],
    )
    wells_doc = _make_doc(
        text_lines=["Wells Fargo Statement"],
        tables=[PdfTable(page=1, header=["Date", "Description", "Amount"], rows=[])],
    )
    assert compute_fingerprint(chase_doc) != compute_fingerprint(wells_doc)


def test_empty_doc_returns_fingerprint_without_crash() -> None:
    """Zero tables, zero text lines → valid fingerprint with safe defaults."""
    fp = compute_fingerprint(_make_doc())
    assert fp == {"issuer": "unknown", "headers": [], "page_bucket": "1"}


def test_unknown_issuer_when_no_known_name_in_text() -> None:
    """Text lines with no known issuer → issuer='unknown'."""
    doc = _make_doc(
        text_lines=["Some Random Credit Union Statement"],
        tables=[PdfTable(page=1, header=["Date", "Amount"], rows=[])],
    )
    fp = compute_fingerprint(doc)
    assert fp["issuer"] == "unknown"


def test_issuer_detection_is_case_insensitive() -> None:
    """Issuer match ignores case."""
    doc = _make_doc(
        text_lines=["CHASE BANK STATEMENT"],
        tables=[PdfTable(page=1, header=["Date", "Amount"], rows=[])],
    )
    fp = compute_fingerprint(doc)
    assert fp["issuer"] == "Chase"


def test_headers_deduplicated_and_sorted() -> None:
    """Headers from multiple tables are de-duplicated and sorted."""
    doc = _make_doc(
        text_lines=["Chase Statement"],
        tables=[
            PdfTable(page=1, header=["Date", "Description", "Amount"], rows=[]),
            PdfTable(page=2, header=["Date", "Description", "Amount"], rows=[]),
        ],
    )
    fp = compute_fingerprint(doc)
    assert fp["headers"] == ["Amount", "Date", "Description"]


def test_headers_scope_to_largest_table_only() -> None:
    """Fingerprint headers come from the single largest table.

    Regression for the codex/claude CONSIDER finding: a secondary table
    (rewards summary, account summary, etc.) whose columns drift month to
    month must NOT flip the fingerprint and break replay. Scoping to the
    largest transaction table keeps the fingerprint stable.
    """
    doc = _make_doc(
        text_lines=["Chase Statement"],
        tables=[
            # Largest: transaction table with 3 rows
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[
                    ["01/01/2024", "x", "1.00"],
                    ["01/02/2024", "y", "2.00"],
                    ["01/03/2024", "z", "3.00"],
                ],
            ),
            # Secondary: rewards summary with 1 row — should be IGNORED
            PdfTable(page=2, header=["Category", "Points"], rows=[["dining", "100"]]),
        ],
    )
    fp = compute_fingerprint(doc)
    assert fp["headers"] == ["Amount", "Date", "Description"]


def test_page_count_derives_from_max_table_page() -> None:
    """page_bucket reflects the highest table page number."""
    doc = _make_doc(
        text_lines=["Chase Statement"],
        tables=[
            PdfTable(page=1, header=["Date", "Amount"], rows=[]),
            PdfTable(page=4, header=["Date", "Amount"], rows=[]),
        ],
    )
    fp = compute_fingerprint(doc)
    assert fp["page_bucket"] == "4+"


# ---------------------------------------------------------------------------
# Unit: _page_bucket
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (1, "1"),
        (2, "2-3"),
        (3, "2-3"),
        (4, "4+"),
        (10, "4+"),
    ],
)
def test_page_bucket(n: int, expected: str) -> None:
    assert _page_bucket(n) == expected


# ---------------------------------------------------------------------------
# Integration: match_format against a real Database
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_match_format_finds_saved_row(db: Database) -> None:
    """match_format returns the saved PdfFormat when fingerprint matches."""
    repo = PdfFormatsRepo(db)
    doc = _chase_doc()
    fp = compute_fingerprint(doc)

    repo.save_new(
        "chase_checking_pdf",
        _RECIPE,
        fingerprint=fp,
        institution_name="Chase",
        document_kind="checking_statement",
        front_end="text",
        routing="transactions",
        actor="cli",
    )

    result = match_format(fp, repo)
    assert result is not None
    assert result.name == "chase_checking_pdf"
    assert result.institution_name == "Chase"
    assert result.version == 1


@pytest.mark.integration
def test_match_format_returns_none_on_miss(db: Database) -> None:
    """match_format returns None when no saved format matches the fingerprint."""
    repo = PdfFormatsRepo(db)
    fp = {"issuer": "nonexistent_bank", "headers": ["A", "B"], "page_bucket": "1"}

    result = match_format(fp, repo)
    assert result is None


@pytest.mark.integration
def test_match_format_uses_repo_get_by_fingerprint(db: Database) -> None:
    """match_format is a thin wrapper — verify it delegates to repo correctly."""
    mock_repo = MagicMock(spec=PdfFormatsRepo)
    mock_repo.get_by_fingerprint.return_value = None
    fp = {"issuer": "Chase", "headers": ["Amount", "Date"], "page_bucket": "1"}

    result = match_format(fp, mock_repo)

    mock_repo.get_by_fingerprint.assert_called_once_with(fp)
    assert result is None
