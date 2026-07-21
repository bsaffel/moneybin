"""Pure local renderers for immutable prepared exports."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import cast
from urllib.parse import quote
from uuid import UUID

import duckdb
import pyarrow as pa
from openpyxl import Workbook
from sqlglot import exp, parse_one

from moneybin.exports.manifest import LocalExportFormat, build_local_manifest
from moneybin.exports.snapshot import PreparedExport, PreparedTable

_MANIFEST_SHEET = "MoneyBin Manifest"
_DICTIONARY_SHEET = "MoneyBin Data Dictionary"
_INVALID_SHEET_CHARACTERS = re.compile(r"[\\/*?:\[\]]")


@dataclass(frozen=True, slots=True)
class RenderedArtifact:
    """Paths and receipts produced by one pure renderer."""

    path: Path
    manifest: Mapping[str, object]
    table_files: Mapping[str, Path]
    file_checksums: Mapping[str, str]


def render_csv(snapshot: PreparedExport, staging_root: Path) -> RenderedArtifact:
    """Render one prepared snapshot as a CSV directory bundle."""
    return _render_bundle(snapshot, staging_root, format="csv")


def render_parquet(snapshot: PreparedExport, staging_root: Path) -> RenderedArtifact:
    """Render one prepared snapshot as a native Parquet directory bundle."""
    return _render_bundle(snapshot, staging_root, format="parquet")


def render_xlsx(snapshot: PreparedExport, staging_root: Path) -> RenderedArtifact:
    """Render one prepared snapshot as a workbook with visible receipts."""
    staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    workbook = Workbook()
    active_sheet = workbook.active
    if active_sheet is None:
        raise RuntimeError("new XLSX workbook has no active sheet")
    workbook.remove(active_sheet)
    used_sheet_names = {_MANIFEST_SHEET, _DICTIONARY_SHEET}
    worksheets: dict[str, str] = {}

    for table in snapshot.tables:
        sheet_name = _worksheet_name(table.name, used_sheet_names)
        used_sheet_names.add(sheet_name)
        worksheets[table.name] = sheet_name
        sheet = workbook.create_sheet(sheet_name)
        sheet.append([column.name for column in table.columns])
        for row in table.rows:
            sheet.append([normalize_tabular_cell(value) for value in row])

    manifest = build_local_manifest(
        snapshot,
        format="xlsx",
        worksheets=worksheets,
    )
    manifest_sheet = workbook.create_sheet(_MANIFEST_SHEET)
    manifest_sheet.append(["JSON"])
    manifest_sheet.append([_json_text(manifest)])
    dictionary_sheet = workbook.create_sheet(_DICTIONARY_SHEET)
    dictionary_sheet.append(["JSON"])
    dictionary_sheet.append([_json_text(snapshot.data_dictionary)])

    workbook_path = staging_root / "export.xlsx"
    workbook.save(workbook_path)
    workbook_path.chmod(0o600)
    return RenderedArtifact(
        path=workbook_path,
        manifest=manifest,
        table_files={},
        file_checksums={"export.xlsx": _file_digest(workbook_path)},
    )


def _render_bundle(
    snapshot: PreparedExport,
    staging_root: Path,
    *,
    format: LocalExportFormat,
) -> RenderedArtifact:
    if format == "xlsx":
        raise ValueError("XLSX is a workbook, not a directory bundle")
    staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    tables_root = staging_root / "tables"
    tables_root.mkdir(mode=0o700)
    table_files: dict[str, Path] = {}
    relative_files: dict[str, tuple[str, str]] = {}

    for table in snapshot.tables:
        filename = f"{_filename_stem(table.name)}.{format}"
        table_path = tables_root / filename
        if table_path.exists():
            raise ValueError("prepared table names produce duplicate artifact paths")
        if format == "csv":
            _write_csv(table, table_path)
        else:
            _write_parquet(table, table_path)
        table_path.chmod(0o600)
        digest = _file_digest(table_path)
        relative_path = table_path.relative_to(staging_root).as_posix()
        table_files[table.name] = table_path
        relative_files[table.name] = (relative_path, digest)

    manifest = build_local_manifest(
        snapshot,
        format=format,
        table_files=relative_files,
    )
    _write_json(staging_root / "manifest.json", manifest)
    _write_json(staging_root / "data-dictionary.json", snapshot.data_dictionary)
    checksum_lines = [
        f"{digest}  {relative_path}\n"
        for relative_path, digest in sorted(relative_files.values())
    ]
    checksums_path = staging_root / "checksums.sha256"
    checksums_path.write_text("".join(checksum_lines), encoding="utf-8", newline="")
    checksums_path.chmod(0o600)
    return RenderedArtifact(
        path=staging_root,
        manifest=manifest,
        table_files=table_files,
        file_checksums=dict(relative_files.values()),
    )


def _write_csv(table: PreparedTable, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(column.name for column in table.columns)
        writer.writerows(
            [_csv_payload_cell(value) for value in row] for row in table.rows
        )


def _write_parquet(table: PreparedTable, path: Path) -> None:
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([row[index] for row in table.rows])
            for index, _column in enumerate(table.columns)
        ],
        names=[column.name for column in table.columns],
    )
    relation = duckdb.from_arrow(arrow_table)
    projections: list[str] = []
    for column in table.columns:
        identifier = exp.to_identifier(column.name, quoted=True).sql("duckdb")
        duckdb_type = _validated_duckdb_type(column.duckdb_type)
        projections.append(
            f"CAST({identifier} AS {duckdb_type}) AS {identifier}"  # noqa: S608  # identifier is quoted and type is parsed by sqlglot
        )
    typed_relation = relation.project(", ".join(projections))
    # DuckDB 1.5.4 documents relation.write_parquet(file_name). Passing the path
    # through that Python API avoids interpolating a filesystem path into COPY SQL.
    typed_relation.write_parquet(str(path))


def _validated_duckdb_type(value: str) -> str:
    parsed = parse_one(value, into=exp.DataType, dialect="duckdb")
    return parsed.sql(dialect="duckdb")


def _csv_payload_cell(value: object) -> object:
    if value is None:
        return r"\N"
    if isinstance(value, Mapping):
        return _json_text(_payload_json_safe(cast(Mapping[object, object], value)))
    if isinstance(value, (list, tuple)):
        return _json_text(_payload_json_safe(cast(Sequence[object], value)))
    return normalize_tabular_cell(value)


def normalize_tabular_cell(value: object) -> object:
    """Normalize a prepared cell for lossless text-oriented payloads."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (UUID, Path, Enum)):
        return str(value)
    if isinstance(value, Mapping):
        return _json_text(_payload_json_safe(cast(Mapping[object, object], value)))
    if isinstance(value, (list, tuple)):
        return _json_text(_payload_json_safe(cast(Sequence[object], value)))
    raise TypeError(f"Unsupported export cell value: {type(value).__name__}")


def _payload_json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {str(key): _payload_json_safe(item) for key, item in mapping.items()}
    if isinstance(value, (list, tuple)):
        sequence = cast(Sequence[object], value)
        return [_payload_json_safe(item) for item in sequence]
    return normalize_tabular_cell(value)


def _json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _write_json(path: Path, value: object) -> None:
    path.write_text(f"{_json_text(value)}\n", encoding="utf-8", newline="")
    path.chmod(0o600)


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _filename_stem(name: str) -> str:
    encoded = quote(name, safe="-_.")
    if encoded in {"", ".", ".."}:
        raise ValueError("prepared table name cannot be represented safely")
    return encoded


def _worksheet_name(name: str, used: set[str]) -> str:
    base = _INVALID_SHEET_CHARACTERS.sub("_", name).strip("'") or "Table"
    base = base[:31]
    candidate = base
    suffix = 2
    while candidate in used:
        marker = f"-{suffix}"
        candidate = f"{base[: 31 - len(marker)]}{marker}"
        suffix += 1
    return candidate
