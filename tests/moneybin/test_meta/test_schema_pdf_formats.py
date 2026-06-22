"""Tests for app.pdf_formats schema."""

import pytest

from moneybin.database import Database
from moneybin.tables import PDF_FORMATS


@pytest.mark.integration
def test_pdf_formats_table_exists_after_init(module_db: Database) -> None:
    """Verify app.pdf_formats table exists with correct columns after schema init."""
    cols = module_db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'app' AND table_name = 'pdf_formats' "
        "ORDER BY ordinal_position"
    ).fetchall()
    column_names = [c[0] for c in cols]
    assert "name" in column_names
    assert "layout_fingerprint" in column_names
    assert "extraction_recipe" in column_names
    assert "routing" in column_names
    assert "version" in column_names
    assert PDF_FORMATS.full_name == "app.pdf_formats"
