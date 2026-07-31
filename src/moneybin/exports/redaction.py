"""Output-policy boundary for prepared exports."""

from __future__ import annotations

from dataclasses import replace

from moneybin.exports.models import RedactionMode
from moneybin.exports.snapshot import (
    PreparedColumn,
    PreparedExport,
    PreparedTable,
    build_data_dictionary,
    prepared_table_checksum,
)
from moneybin.privacy.redaction import MaskStrength, mask_strength, redact_records

#: What every masking transform returns, whatever type it was handed. Measured, not
#: assumed: `test_every_masking_transform_returns_text_whatever_it_is_given` walks
#: `_TRANSFORMS` and fails the moment one answers with something else.
_MASKED_DUCKDB_TYPE = "VARCHAR"


def _redacted_column(column: PreparedColumn) -> PreparedColumn:
    """Declare the type the mask produced, not the one it replaced.

    A column's DuckDB type is captured from the *unredacted* cursor, and a saved
    report's lineage hands a class down through an expression without its type —
    ``SELECT length(last_four) AS n`` keeps ``INSTITUTION_ACCOUNT_NUMBER`` on a
    ``BIGINT``. Masking then makes the value text, and a declaration left behind
    describes an export nobody asked for: ``parquet_schema_for`` believes it and
    fails the whole artifact with ``ArrowInvalid``, while CSV writes happily
    beneath a manifest that promises a number.

    Both masked bundle columns are already ``VARCHAR`` and no built-in report
    declares a masking class at all, so this is a no-op for every artifact that
    exists today — nothing already shipped changes type or checksum.
    """
    if mask_strength(column.data_class) is MaskStrength.PASSTHROUGH:
        return column
    return replace(column, duckdb_type=_MASKED_DUCKDB_TYPE)


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
        columns = tuple(_redacted_column(column) for column in table.columns)
        tables.append(
            replace(
                table,
                columns=columns,
                rows=rows,
                checksum_sha256=prepared_table_checksum(columns, rows),
            )
        )
    redacted_tables = tuple(tables)
    # `manifest` reads `tables` live, but the data dictionary is stored — rebuilt
    # here so the standalone receipt cannot disagree with the manifest beside it.
    return replace(
        snapshot,
        redaction_mode=mode,
        tables=redacted_tables,
        _data_dictionary=build_data_dictionary(redacted_tables),
    )
