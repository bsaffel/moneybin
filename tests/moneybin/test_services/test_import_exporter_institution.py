"""Multi-account exporter formats carry per-account institution, not the tool name."""

from __future__ import annotations

from pathlib import Path

from moneybin.database import Database


def test_multi_account_institution_is_per_account_not_tool_name(
    db: Database, tmp_path: Path
) -> None:
    """Per-row Institution column lands each account's institution, not the tool name.

    A multi-account CSV (Tiller-style) with a per-row Institution column must land
    each account's own institution in raw.tabular_accounts, never a single shared
    tool/format name (Decision 8 exporter/institution split).
    """
    from moneybin.services.import_service import ImportService

    csv = tmp_path / "multi.csv"
    csv.write_text(
        "Date,Description,Amount,Account,Institution\n"
        "2026-01-15,Coffee,-12.50,Checking,Wells Fargo\n"
        "2026-01-16,Dinner,-40.00,Credit Card,Amex\n"
    )
    svc = ImportService(db)
    svc.import_file(csv, confirm=True, actor_kind="human", refresh=False)
    rows = db.execute(
        "SELECT account_name, institution_name FROM raw.tabular_accounts"
    ).fetchall()
    inst_by_name = dict(rows)
    assert inst_by_name.get("Checking") == "Wells Fargo", inst_by_name
    assert inst_by_name.get("Credit Card") == "Amex", inst_by_name
