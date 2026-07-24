"""Output-policy boundary for prepared exports."""

from __future__ import annotations

from dataclasses import replace

from moneybin.exports.models import RedactionMode
from moneybin.exports.snapshot import (
    PreparedExport,
    PreparedTable,
    prepared_table_checksum,
)
from moneybin.privacy.redaction import redact_records


def apply_export_redaction(
    snapshot: PreparedExport, mode: RedactionMode
) -> PreparedExport:
    """Return a snapshot carrying the selected per-run output policy."""
    if mode == "unredacted":
        return replace(snapshot, redaction_mode=mode)

    tables: list[PreparedTable] = []
    for table in snapshot.tables:
        column_names = tuple(column.name for column in table.columns)
        records = [dict(zip(column_names, row, strict=True)) for row in table.rows]
        output_classes = {column.name: column.data_class for column in table.columns}
        redacted_records = redact_records(records, output_classes, consent=None)
        rows = tuple(
            tuple(record[column_name] for column_name in column_names)
            for record in redacted_records
        )
        tables.append(
            replace(
                table,
                rows=rows,
                checksum_sha256=prepared_table_checksum(table.columns, rows),
            )
        )
    return replace(snapshot, redaction_mode=mode, tables=tuple(tables))
