"""Tests for the gsheet RawSeedAdapter (catch-all escape hatch)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from moneybin.connectors.gsheet.adapters.base import GSheetConnection
from moneybin.connectors.gsheet.adapters.raw_seed import RawSeedAdapter
from moneybin.database import Database

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict[str, Any]:
    return yaml.safe_load((FIXTURES / name).read_text())


def df_from(fix: dict[str, Any]) -> pl.DataFrame:
    headers = fix["sheet"]["headers"]
    rows = fix["sheet"]["rows"]
    return pl.DataFrame({col: [r[i] for r in rows] for i, col in enumerate(headers)})


def make_seed_connection(
    alias: str | None = "subscriptions",
    *,
    typed_columns: dict[str, str] | None = None,
) -> GSheetConnection:
    return GSheetConnection(
        connection_id="conn_seed1",
        spreadsheet_id="1xyz",
        sheet_gid=99,
        sheet_name="Subscriptions",
        workbook_name="Personal Finance",
        adapter="seed",
        alias=alias,
        account_id=None,
        account_name=None,
        column_mapping=typed_columns
        or {
            "Name": "VARCHAR",
            "Amount": "DECIMAL(18,2)",
            "Next Charge": "DATE",
            "Notes": "VARCHAR",
        },
        header_signature=["Name", "Amount", "Next Charge", "Notes"],
        date_format=None,
        sign_convention=None,
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=[],
        status="healthy",
        last_pull_at=None,
        last_pull_import_id=None,
        last_success_at=None,
        last_drift_reason=None,
        consecutive_failure_count=0,
    )


def test_detect_infers_typed_columns() -> None:
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    df = df_from(fix)
    result = adapter.detect(df, account_name=None)
    # Seed adapter never returns "high" — that's transactions territory.
    assert result.confidence in ("medium", "low")
    assert "Name" in result.typed_columns
    assert "Amount" in result.typed_columns
    # Amount should infer to a numeric type
    assert (
        "DECIMAL" in result.typed_columns["Amount"]
        or "DOUBLE" in result.typed_columns["Amount"]
    )
    # Next Charge is YYYY-MM-DD → DATE
    assert result.typed_columns["Next Charge"] == "DATE"


def test_load_writes_json_rows_to_gsheet_seeds(in_memory_db: Database) -> None:
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection()
    df = df_from(fix)
    transformed = adapter.transform(df, conn)
    result = adapter.load(transformed, conn, in_memory_db, import_id="imp1")
    assert result.rows_inserted == 3
    # rows_upserted counts ONLY pre-existing rows that were updated. On a
    # first pull, every row is brand new, so rows_upserted must be 0 —
    # otherwise import_log.rows_imported would double-count to 6.
    assert result.rows_upserted == 0

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.gsheet_seeds WHERE connection_id = ?",
        [conn.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 3


def test_second_load_upserts_unchanged_rows(in_memory_db: Database) -> None:
    """Second pull with identical data → rows_upserted == 3, rows_inserted == 0."""
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection()
    df = df_from(fix)
    transformed = adapter.transform(df, conn)
    adapter.load(transformed, conn, in_memory_db, import_id="imp1")

    # Same data, second pull
    result2 = adapter.load(transformed, conn, in_memory_db, import_id="imp2")
    assert result2.rows_inserted == 0
    assert result2.rows_upserted == 3
    assert result2.rows_soft_deleted == 0


def test_load_creates_per_connection_view(in_memory_db: Database) -> None:
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection()
    adapter.load(adapter.transform(df_from(fix), conn), conn, in_memory_db, "imp1")

    views = in_memory_db.execute(
        "SELECT view_name FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'gsheet_subscriptions'"
    ).fetchall()
    assert len(views) == 1


def test_query_view_returns_typed_rows(in_memory_db: Database) -> None:
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection()
    adapter.load(adapter.transform(df_from(fix), conn), conn, in_memory_db, "imp1")

    rows = in_memory_db.execute(
        "SELECT name, amount FROM raw.gsheet_subscriptions ORDER BY name"
    ).fetchall()
    # Lookup by name — sort order is incidental; what matters is that the view
    # projects typed columns from the JSON data field.
    by_name = {r[0]: r[1] for r in rows}
    assert by_name["Netflix"] == Decimal("15.49")
    assert by_name["Spotify"] == Decimal("9.99")
    assert by_name["NYTimes"] == Decimal("17.00")


def test_load_soft_deletes_missing_rows(in_memory_db: Database) -> None:
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection()
    adapter.load(adapter.transform(df_from(fix), conn), conn, in_memory_db, "imp1")

    # Second pull: drop NYTimes
    fix2 = dict(fix)
    fix2["sheet"] = dict(fix["sheet"])
    fix2["sheet"]["rows"] = fix["sheet"]["rows"][:2]
    result = adapter.load(
        adapter.transform(df_from(fix2), conn), conn, in_memory_db, "imp2"
    )
    assert result.rows_soft_deleted == 1

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.gsheet_subscriptions"
    ).fetchone()
    assert row is not None
    assert row[0] == 2


def test_seed_adapter_permissive_drift_on_added_column() -> None:
    """Adding a new column is NOT drift for the seed adapter — view is regenerated."""
    adapter = RawSeedAdapter()
    conn = make_seed_connection()
    # Current pull has all original headers PLUS an extra "Notes2" column.
    df = pl.DataFrame({
        "Name": ["Netflix", "Spotify"],
        "Amount": ["15.49", "9.99"],
        "Next Charge": ["2026-02-15", "2026-02-10"],
        "Notes": ["", ""],
        "Notes2": ["new col", ""],
    })
    report = adapter.check_drift(conn, df)
    assert report.is_drift is False


def test_load_raises_when_alias_missing(in_memory_db: Database) -> None:
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection(alias=None)
    transformed = adapter.transform(df_from(fix), conn)
    with pytest.raises(ValueError, match="alias"):
        adapter.load(transformed, conn, in_memory_db, import_id="imp1")


def test_row_hash_is_content_only_not_position_sensitive(
    in_memory_db: Database,
) -> None:
    """Inserting a row above existing rows must NOT soft-delete those rows.

    Regression for the round-3 finding: the prior hash formula included a
    1-based row index, so prepending a row reshuffled every downstream
    hash and cascaded soft-delete + re-insert through the entire sheet.
    With a content-only hash, only the truly-new row inserts.
    """
    adapter = RawSeedAdapter()
    fix = load("seed_subscriptions.yaml")
    conn = make_seed_connection()
    initial = adapter.load(
        adapter.transform(df_from(fix), conn), conn, in_memory_db, "imp1"
    )
    assert initial.rows_inserted == 3

    # Capture the hashes from the first pull so we can verify they survive
    # the insertion.
    first_pull_hashes = {
        r[0]
        for r in in_memory_db.execute(
            "SELECT row_hash FROM raw.gsheet_seeds "
            "WHERE connection_id = ? AND deleted_from_source_at IS NULL",
            [conn.connection_id],
        ).fetchall()
    }
    assert len(first_pull_hashes) == 3

    # Prepend a new row above the existing three.
    fix2 = dict(fix)
    fix2["sheet"] = dict(fix["sheet"])
    fix2["sheet"]["rows"] = [
        ["Hulu", "12.99", "2026-02-20", ""],
        *fix["sheet"]["rows"],
    ]
    second = adapter.load(
        adapter.transform(df_from(fix2), conn), conn, in_memory_db, "imp2"
    )
    # Exactly one new row, no soft-deletes — the original three rows
    # retain their content-only hashes despite shifting down by one
    # position.
    assert second.rows_inserted == 1
    assert second.rows_soft_deleted == 0

    # All three original hashes must still be active in the table.
    after = {
        r[0]
        for r in in_memory_db.execute(
            "SELECT row_hash FROM raw.gsheet_seeds "
            "WHERE connection_id = ? AND deleted_from_source_at IS NULL",
            [conn.connection_id],
        ).fetchall()
    }
    assert first_pull_hashes <= after, (
        "Original rows' content-only hashes must survive a prepend; "
        f"missing: {first_pull_hashes - after}"
    )


def test_transform_rejects_duplicate_content_rows(in_memory_db: Database) -> None:
    """Two rows with byte-identical content must raise an explicit error.

    Pure content hashing means the PRIMARY KEY (connection_id, row_hash)
    would otherwise collide silently and one of the duplicates would be
    lost. We surface the collision before SQL runs, with a message that
    tells the user how to fix it (add an ID/Note column).
    """
    from moneybin.connectors.gsheet.errors import GSheetError

    _ = in_memory_db  # fixture not needed; transform is pure
    adapter = RawSeedAdapter()
    conn = make_seed_connection()
    df = pl.DataFrame({
        "Name": ["Netflix", "Netflix"],
        "Amount": ["15.49", "15.49"],
        "Next Charge": ["2026-02-15", "2026-02-15"],
        "Notes": ["", ""],
    })
    with pytest.raises(GSheetError, match="duplicate row"):
        adapter.transform(df, conn)


def test_load_with_empty_df_creates_view_but_inserts_zero(
    in_memory_db: Database,
) -> None:
    """Empty df + no active rows → LoadResult(0, 0, 0) and view still exists."""
    adapter = RawSeedAdapter()
    conn = make_seed_connection()
    empty_df = pl.DataFrame(
        {h: [] for h in conn.header_signature},
        schema=dict.fromkeys(conn.header_signature, pl.Utf8),
    )
    transformed = adapter.transform(empty_df, conn)
    result = adapter.load(transformed, conn, in_memory_db, import_id="imp_empty")
    assert result.rows_inserted == 0
    assert result.rows_soft_deleted == 0
    assert result.rows_upserted == 0

    # View should still exist (regenerated from connection.column_mapping).
    views = in_memory_db.execute(
        "SELECT view_name FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'gsheet_subscriptions'"
    ).fetchall()
    assert len(views) == 1
