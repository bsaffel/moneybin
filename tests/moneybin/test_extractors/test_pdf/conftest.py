"""Shared fixtures for PDF extractor tests."""

from pathlib import Path

import pytest

# A 1-page native-text PDF with a 2-column table is checked in at
# tests/moneybin/test_extractors/test_pdf/fixtures/simple_statement.pdf.
# Generate once with: uv run python tests/moneybin/test_extractors/test_pdf/_make_fixture.py
# (script uses reportlab; the generated PDF is committed so reportlab is NOT a
# project test dependency).


@pytest.fixture
def simple_statement_pdf() -> Path:
    path = Path(__file__).parent / "fixtures" / "simple_statement.pdf"
    if not path.exists():
        pytest.skip(f"fixture missing: {path} (run _make_fixture.py)")
    return path
