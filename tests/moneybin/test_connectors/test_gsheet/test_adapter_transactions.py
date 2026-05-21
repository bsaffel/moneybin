"""Tests for the gsheet TransactionsAdapter."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from moneybin.connectors.gsheet.adapters.base import GSheetConnection
from moneybin.connectors.gsheet.adapters.transactions import TransactionsAdapter
from moneybin.database import Database

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict[str, Any]:
    return yaml.safe_load((FIXTURES / name).read_text())


def df_from_fixture(fix: dict[str, Any]) -> pl.DataFrame:
    headers = fix["sheet"]["headers"]
    rows = fix["sheet"]["rows"]
    return pl.DataFrame({h: [r[i] for r in rows] for i, h in enumerate(headers)})


def test_detect_tiller_basic_returns_high_confidence() -> None:
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    result = adapter.detect(df, account_name=fix["account_name"])
    assert result.confidence == "high"
    assert result.column_mapping["Date"] == "transaction_date"
    assert result.column_mapping["Amount"] == "amount"
    assert result.column_mapping["Description"] == "description"


def test_detect_includes_pinned_signature_in_order() -> None:
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    result = adapter.detect(df, account_name=fix["account_name"])
    assert result.header_signature == fix["sheet"]["headers"]


def test_load_inserts_rows_first_time(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    transformed = adapter.transform(df, sample_connection)
    result = adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp1"
    )
    assert result.rows_inserted == 2
    assert result.rows_soft_deleted == 0


def test_load_soft_deletes_missing_rows(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """First pull inserts 2; second pull omits one → soft-delete."""
    adapter = TransactionsAdapter()
    fix = load_fixture("tiller_basic.yaml")
    df1 = df_from_fixture(fix)
    transformed1 = adapter.transform(df1, sample_connection)
    adapter.load(transformed1, sample_connection, in_memory_db, import_id="imp1")

    # Second pull: drop the Salary row
    fix2 = dict(fix)
    fix2["sheet"] = dict(fix["sheet"])
    fix2["sheet"]["rows"] = [fix["sheet"]["rows"][0]]  # only Whole Foods
    df2 = df_from_fixture(fix2)
    transformed2 = adapter.transform(df2, sample_connection)
    result = adapter.load(
        transformed2, sample_connection, in_memory_db, import_id="imp2"
    )

    assert result.rows_soft_deleted == 1

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions "
        "WHERE source_origin = ? AND deleted_from_source_at IS NOT NULL",
        [sample_connection.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 1


def test_load_undeletes_returning_row(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Row deleted-then-readded gets deleted_from_source_at reset to NULL."""
    adapter = TransactionsAdapter()
    fix = load_fixture("tiller_basic.yaml")
    df1 = df_from_fixture(fix)
    adapter.load(
        adapter.transform(df1, sample_connection),
        sample_connection,
        in_memory_db,
        "imp1",
    )

    fix2 = dict(fix)
    fix2["sheet"] = dict(fix["sheet"])
    fix2["sheet"]["rows"] = [fix["sheet"]["rows"][0]]
    df2 = df_from_fixture(fix2)
    adapter.load(
        adapter.transform(df2, sample_connection),
        sample_connection,
        in_memory_db,
        "imp2",
    )

    df3 = df_from_fixture(fix)
    adapter.load(
        adapter.transform(df3, sample_connection),
        sample_connection,
        in_memory_db,
        "imp3",
    )

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions "
        "WHERE source_origin = ? AND deleted_from_source_at IS NULL",
        [sample_connection.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 2


def test_detect_low_confidence_on_unrecognized_columns() -> None:
    """Unrecognized headers produce non-high confidence."""
    adapter = TransactionsAdapter()
    df = pl.DataFrame({
        "Col_X": ["x1", "x2"],
        "Col_Y": ["y1", "y2"],
        "Col_Z": ["z1", "z2"],
    })
    result = adapter.detect(df, account_name=None)
    # Confidence should be "low" — no date/amount/description matched.
    assert result.confidence == "low"


def test_check_drift_passes_pinned_signature_through(
    sample_connection: GSheetConnection,
) -> None:
    """Drift report identifies a missing pinned header."""
    adapter = TransactionsAdapter()
    # Current headers drop "Amount" — should appear as missing.
    current_headers = ["Date", "Description", "Category", "Account", "Tags"]
    sample = pl.DataFrame({h: ["v1", "v2"] for h in current_headers})
    report = adapter.check_drift(sample_connection, sample)
    assert report.is_drift is True
    assert "Amount" in report.missing_headers


def test_transform_applies_sign_convention(
    sample_connection: GSheetConnection,
) -> None:
    """Negative input under negative_is_expense convention stays negative."""
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    transformed = adapter.transform(df, sample_connection)
    amounts = transformed["amount"].to_list()
    # Whole Foods is -87.42; Salary is +5000.00 under negative_is_expense.
    assert Decimal("-87.42") in amounts
    assert Decimal("5000.00") in amounts


def test_load_with_empty_df_is_no_op(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Empty df → LoadResult(0,0,0) and does not raise on IN ()."""
    adapter = TransactionsAdapter()
    empty_df = pl.DataFrame(
        {h: [] for h in sample_connection.header_signature},
        schema=dict.fromkeys(sample_connection.header_signature, pl.Utf8),
    )
    transformed = adapter.transform(empty_df, sample_connection)
    result = adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp_empty"
    )
    assert result.rows_inserted == 0
    assert result.rows_soft_deleted == 0
    assert result.rows_upserted == 0


def test_load_idempotent_when_called_twice_same_data(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Calling load twice with same df → second call: no soft-deletes; same total rows."""
    adapter = TransactionsAdapter()
    fix = load_fixture("tiller_basic.yaml")
    df = df_from_fixture(fix)
    transformed = adapter.transform(df, sample_connection)
    adapter.load(transformed, sample_connection, in_memory_db, import_id="imp1")
    result = adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp2"
    )
    assert result.rows_soft_deleted == 0

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_origin = ?",
        [sample_connection.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 2
