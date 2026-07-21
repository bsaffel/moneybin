"""Local export renderer contract tests."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
from openpyxl import load_workbook

from moneybin.exports.renderers import render_csv, render_parquet, render_xlsx
from moneybin.exports.snapshot import (
    ExportSubject,
    PreparedColumn,
    PreparedExport,
    PreparedTable,
    ReportExportProvenance,
    build_data_dictionary,
    prepared_table_checksum,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import TableRef


def make_snapshot(*, report: bool = False) -> PreparedExport:
    columns = (
        PreparedColumn("entry_id", "INTEGER", DataClass.AGGREGATE),
        PreparedColumn("amount", "DECIMAL(18,2)", DataClass.TXN_AMOUNT),
        PreparedColumn("entry_date", "DATE", DataClass.TXN_DATE),
        PreparedColumn("recorded_at", "TIMESTAMP", DataClass.TIMESTAMP_OBSERVABILITY),
        PreparedColumn("note", "VARCHAR", DataClass.DESCRIPTION),
    )
    rows = (
        (
            1,
            Decimal("12.30"),
            date(2026, 7, 20),
            datetime(2026, 7, 20, 8, 9, 10),
            "café, quoted",
        ),
        (2, Decimal("-4.50"), date(2026, 7, 21), None, None),
    )
    table = PreparedTable(
        name="activity",
        source=TableRef("reports", "activity"),
        columns=columns,
        rows=rows,
        checksum_sha256=prepared_table_checksum(columns, rows),
    )
    tables = (table,)
    return PreparedExport(
        artifact_version=1,
        profile="personal",
        created_at=datetime(2026, 7, 21, 18, 42, 33, tzinfo=UTC),
        subject=ExportSubject(
            kind="report" if report else "bundle",
            report_id="test:activity" if report else None,
            parameters={"days": 30} if report else None,
        ),
        redaction_mode="redacted",
        tables=tables,
        data_dictionary=build_data_dictionary(tables),
        provenance=(
            ReportExportProvenance(
                report_id="test:activity",
                receipt={"lineage": ["reports.activity"], "sql": None},
            )
            if report
            else None
        ),
    )


def test_csv_round_trips_typed_values_through_duckdb(tmp_path: Path) -> None:
    rendered = render_csv(make_snapshot(), tmp_path / "bundle")

    relation = duckdb.read_csv(
        str(rendered.table_files["activity"]),
        header=True,
        columns={
            "entry_id": "INTEGER",
            "amount": "DECIMAL(18,2)",
            "entry_date": "DATE",
            "recorded_at": "TIMESTAMP",
            "note": "VARCHAR",
        },
        na_values="\\N",
    )

    assert relation.columns == [
        "entry_id",
        "amount",
        "entry_date",
        "recorded_at",
        "note",
    ]
    assert relation.fetchall() == list(make_snapshot().tables[0].rows)


def test_parquet_round_trips_native_typed_values_through_duckdb(
    tmp_path: Path,
) -> None:
    rendered = render_parquet(make_snapshot(), tmp_path / "bundle")

    relation = duckdb.read_parquet(str(rendered.table_files["activity"]))

    assert relation.columns == [
        "entry_id",
        "amount",
        "entry_date",
        "recorded_at",
        "note",
    ]
    assert relation.fetchall() == list(make_snapshot().tables[0].rows)


def test_bundle_manifest_and_sidecars_come_from_the_prepared_snapshot(
    tmp_path: Path,
) -> None:
    snapshot = make_snapshot(report=True)
    rendered = render_csv(snapshot, tmp_path / "bundle")

    manifest = json.loads((rendered.path / "manifest.json").read_text())
    dictionary = json.loads((rendered.path / "data-dictionary.json").read_text())
    table_path = rendered.table_files["activity"]
    table_digest = hashlib.sha256(table_path.read_bytes()).hexdigest()

    assert manifest["created_at"] == "2026-07-21T18:42:33+00:00"
    assert manifest["format"] == "csv"
    assert manifest["redaction_mode"] == "redacted"
    assert manifest["subject"]["report_id"] == "test:activity"
    assert manifest["provenance"]["receipt"]["lineage"] == ["reports.activity"]
    assert manifest["tables"] == [
        {
            "name": "activity",
            "source": "reports.activity",
            "row_count": 2,
            "checksum_sha256": snapshot.tables[0].checksum_sha256,
            "file": "tables/activity.csv",
            "file_checksum_sha256": table_digest,
            "columns": [
                {
                    "name": column.name,
                    "duckdb_type": column.duckdb_type,
                    "data_class": column.data_class.value,
                }
                for column in snapshot.tables[0].columns
            ],
        }
    ]
    assert dictionary == snapshot.data_dictionary
    assert (rendered.path / "checksums.sha256").read_text() == (
        f"{table_digest}  tables/activity.csv\n"
    )


def test_xlsx_contains_data_and_visible_receipt_sheets(tmp_path: Path) -> None:
    rendered = render_xlsx(make_snapshot(report=True), tmp_path)

    workbook = load_workbook(rendered.path, data_only=True)

    assert workbook.sheetnames == [
        "activity",
        "MoneyBin Manifest",
        "MoneyBin Data Dictionary",
    ]
    assert all(workbook[name].sheet_state == "visible" for name in workbook.sheetnames)
    assert list(workbook["activity"].values) == [
        ("entry_id", "amount", "entry_date", "recorded_at", "note"),
        (1, "12.30", "2026-07-20", "2026-07-20T08:09:10", "café, quoted"),
        (2, "-4.50", "2026-07-21", None, None),
    ]
    manifest = json.loads(workbook["MoneyBin Manifest"]["A2"].value)
    dictionary = json.loads(workbook["MoneyBin Data Dictionary"]["A2"].value)
    assert manifest["format"] == "xlsx"
    assert manifest["tables"][0]["worksheet"] == "activity"
    assert manifest["tables"][0]["checksum_sha256"] == (
        make_snapshot().tables[0].checksum_sha256
    )
    assert dictionary == make_snapshot().data_dictionary
