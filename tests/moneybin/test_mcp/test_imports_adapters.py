"""Unit tests for the shared import-format projection.

`moneybin import formats list` and the `import_formats` MCP tool project one
stored `PdfFormat` through the same adapter. These pin the precision of
`last_used_at`, which had no guard on the MCP side: the CLI's own test asserts
the date-only string its text table prints, so a shared adapter that truncated
would have narrowed the MCP field with every test still green.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from moneybin.adapters.imports_adapters import (
    pdf_format_detail,
    pdf_format_row,
)


def _pdf_format(**overrides: Any) -> Any:
    """One stored PDF format, used at a known timestamp."""
    from moneybin.repositories.pdf_formats_repo import PdfFormat

    defaults: dict[str, Any] = {
        "name": "acme_a1b2c3d4e5f6",
        "institution_name": "Acme Bank",
        "document_kind": "transactions",
        "layout_fingerprint": {"issuer": "Acme Bank"},
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
        "last_used_at": datetime(2026, 5, 30, 10, 15, 30),
        "created_at": datetime(2026, 5, 1, 9, 0, 0),
        "updated_at": datetime(2026, 5, 30, 10, 15, 30),
    }
    defaults.update(overrides)
    return PdfFormat(**defaults)


@pytest.mark.unit
def test_row_keeps_the_full_last_used_timestamp() -> None:
    """`import_formats` has always emitted a timestamp — not the date half."""
    assert pdf_format_row(_pdf_format()).last_used_at == "2026-05-30T10:15:30"


@pytest.mark.unit
def test_detail_keeps_the_full_last_used_timestamp() -> None:
    """`formats show` answers with the same precision the listing does."""
    assert pdf_format_detail(_pdf_format()).last_used_at == "2026-05-30T10:15:30"


@pytest.mark.unit
@pytest.mark.parametrize("project", [pdf_format_row, pdf_format_detail])
def test_never_used_format_reports_none(project: Any) -> None:
    """A format no import has used yet has no timestamp to narrow."""
    assert project(_pdf_format(last_used_at=None)).last_used_at is None
