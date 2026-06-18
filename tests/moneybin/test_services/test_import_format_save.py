"""Auto-saved tabular formats must carry columns only, never an account binding."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from moneybin.database import Database


def test_autosaved_format_does_not_store_account_name_as_institution(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """Auto-saved format.institution_name must never contain the per-account label (bug #5)."""
    from moneybin.services.import_service import ImportService

    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2026-01-15,Coffee,-12.50\n")
    db = Database(
        tmp_path / "fmt.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    try:
        svc = ImportService(db)
        svc.import_file(
            csv,
            account_name="WF Checking (...4267)",
            confirm=True,
            actor_kind="human",
            save_format=True,
            refresh=False,
        )
        row = db.execute("SELECT institution_name FROM app.tabular_formats").fetchone()
        assert row is not None, "expected an auto-saved format row"
        assert "4267" not in row[0] and "WF Checking" not in row[0], (
            f"account label leaked into format.institution_name: {row[0]!r}"
        )
    finally:
        db.close()
