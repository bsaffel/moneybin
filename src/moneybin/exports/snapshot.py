"""Immutable, format-neutral export snapshots and receipts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from uuid import UUID

from moneybin.exports.catalog import BUNDLE_TABLES
from moneybin.exports.models import RedactionMode
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass
from moneybin.tables import TableRef

if TYPE_CHECKING:
    from moneybin.database import Database


ARTIFACT_VERSION = 1


@dataclass(frozen=True, slots=True)
class ExportSubject:
    """The immutable semantic subject represented by a snapshot."""

    kind: Literal["bundle", "report"]
    report_id: str | None = None
    parameters: Mapping[str, object] | None = None

    def as_manifest(self) -> dict[str, object]:
        """Return the subject's JSON-safe manifest representation."""
        result: dict[str, object] = {"kind": self.kind}
        if self.report_id is not None:
            result["report_id"] = self.report_id
        if self.parameters is not None:
            result["parameters"] = _json_safe(self.parameters)
        return result


@dataclass(frozen=True, slots=True)
class ReportExportProvenance:
    """A report execution receipt, populated by report exports."""

    report_id: str
    receipt: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class PreparedColumn:
    """One typed and privacy-classified output column."""

    name: str
    duckdb_type: str
    data_class: DataClass


@dataclass(frozen=True, slots=True)
class PreparedTable:
    """One ordered, typed output table with an integrity checksum."""

    name: str
    source: TableRef
    columns: tuple[PreparedColumn, ...]
    rows: tuple[tuple[object, ...], ...]
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class PreparedExport:
    """An immutable snapshot ready for any renderer."""

    artifact_version: int
    profile: str
    created_at: datetime
    subject: ExportSubject
    redaction_mode: RedactionMode
    tables: tuple[PreparedTable, ...]
    data_dictionary: Mapping[str, object]
    provenance: ReportExportProvenance | None

    @property
    def manifest(self) -> dict[str, object]:
        """Return the JSON-safe receipt for this prepared snapshot."""
        return {
            "artifact_version": self.artifact_version,
            "profile": self.profile,
            "created_at": self.created_at.isoformat(),
            "subject": self.subject.as_manifest(),
            "redaction_mode": self.redaction_mode,
            "tables": [
                {
                    "name": table.name,
                    "source": table.source.full_name,
                    "row_count": len(table.rows),
                    "checksum_sha256": table.checksum_sha256,
                    "columns": [
                        {
                            "name": column.name,
                            "duckdb_type": column.duckdb_type,
                            "data_class": column.data_class.value,
                        }
                        for column in table.columns
                    ],
                }
                for table in self.tables
            ],
            "data_dictionary": self.data_dictionary,
            "provenance": _json_safe(self.provenance),
        }


def build_bundle_snapshot(
    db: Database,
    *,
    profile: str,
    created_at: datetime,
) -> PreparedExport:
    """Read the closed canonical catalog into one unredacted snapshot."""
    tables: list[PreparedTable] = []
    for catalog_table in BUNDLE_TABLES:
        order_sql = ", ".join(catalog_table.order_by)
        cursor = db.execute(
            f"SELECT * FROM {catalog_table.source.full_name} ORDER BY {order_sql}"  # noqa: S608  # identifiers come only from the fixed bundle catalog
        )
        descriptions = cast(list[tuple[object, ...]], cursor.description)
        class_map = CLASSIFICATION[
            (catalog_table.source.schema, catalog_table.source.name)
        ]
        columns = tuple(
            PreparedColumn(
                name=str(description[0]),
                duckdb_type=str(description[1]),
                data_class=class_map[str(description[0])],
            )
            for description in descriptions
        )
        fetched_rows = cast(list[tuple[object, ...]], cursor.fetchall())
        rows = tuple(tuple(row) for row in fetched_rows)
        tables.append(
            PreparedTable(
                name=catalog_table.name,
                source=catalog_table.source,
                columns=columns,
                rows=rows,
                checksum_sha256=prepared_table_checksum(columns, rows),
            )
        )

    prepared_tables = tuple(tables)
    return PreparedExport(
        artifact_version=ARTIFACT_VERSION,
        profile=profile,
        created_at=created_at,
        subject=ExportSubject(kind="bundle"),
        redaction_mode="unredacted",
        tables=prepared_tables,
        data_dictionary=build_data_dictionary(prepared_tables),
        provenance=None,
    )


def build_data_dictionary(tables: tuple[PreparedTable, ...]) -> dict[str, object]:
    """Build JSON-safe schema and privacy metadata for prepared tables."""
    return {
        "tables": [
            {
                "name": table.name,
                "source": table.source.full_name,
                "columns": [
                    {
                        "name": column.name,
                        "duckdb_type": column.duckdb_type,
                        "data_class": column.data_class.value,
                    }
                    for column in table.columns
                ],
            }
            for table in tables
        ]
    }


def prepared_table_checksum(
    columns: tuple[PreparedColumn, ...], rows: tuple[tuple[object, ...], ...]
) -> str:
    """Return a stable SHA-256 digest for a prepared table's typed payload."""
    payload = {
        "columns": [
            [column.name, column.duckdb_type, column.data_class.value]
            for column in columns
        ],
        "rows": rows,
    }
    encoded = json.dumps(
        _json_safe(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_safe(value: object) -> object:
    """Encode native cells deterministically without changing prepared rows."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, time):
        return {"$time": value.isoformat()}
    if isinstance(value, bytes):
        return {"$bytes_hex": value.hex()}
    if isinstance(value, (UUID, Path, Enum)):
        return str(value)
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {str(key): _json_safe(item) for key, item in mapping.items()}
    if isinstance(value, (list, tuple)):
        sequence = cast(list[object] | tuple[object, ...], value)
        return [_json_safe(item) for item in sequence]
    if isinstance(value, ReportExportProvenance):
        return {"report_id": value.report_id, "receipt": _json_safe(value.receipt)}
    raise TypeError(f"Unsupported export metadata value: {type(value).__name__}")
