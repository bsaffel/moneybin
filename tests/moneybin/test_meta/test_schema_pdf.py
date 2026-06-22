"""Tests for raw.pdf_seeds schema."""

import pytest

from moneybin.database import Database
from moneybin.tables import PDF_SEEDS


@pytest.mark.integration
def test_pdf_seeds_table_exists_after_init(module_db: Database) -> None:
    """Verify raw.pdf_seeds table exists with correct columns after schema init."""
    cols = module_db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'pdf_seeds' "
        "ORDER BY column_name"
    ).fetchall()
    names = {c[0] for c in cols}
    assert names == {
        "alias",
        "data",
        "import_id",
        "loaded_at",
        "page",
        "row_hash",
        "source_file",
    }
    assert PDF_SEEDS.full_name == "raw.pdf_seeds"
