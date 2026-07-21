"""Versioned local export manifest construction."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Literal, cast
from urllib.parse import quote

from moneybin.exports.snapshot import PreparedExport

type LocalExportFormat = Literal["csv", "parquet", "xlsx"]

CSV_ENCODING = {
    "scheme": "moneybin.csv-cell",
    "version": 1,
    "null": r"\N",
    "escape": "\\",
    "escaped_prefixes": ["\\", "=", "+", "-", "@"],
}


def bundle_table_path(name: str, format: Literal["csv", "parquet"]) -> str:
    """Return one safe, deterministic bundle-relative table path."""
    encoded = quote(name, safe="-_.")
    if encoded in {"", ".", ".."}:
        raise ValueError("prepared table name cannot be represented safely")
    return f"tables/{encoded}.{format}"


def build_local_manifest(
    snapshot: PreparedExport,
    *,
    format: LocalExportFormat,
    table_files: Mapping[str, tuple[str, str]] | None = None,
    worksheets: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Add local rendering details to a fresh prepared-snapshot receipt."""
    manifest = deepcopy(snapshot.manifest)
    manifest["format"] = format
    manifest["destination_kind"] = "local"
    if format == "csv":
        manifest["csv_encoding"] = deepcopy(CSV_ENCODING)
    tables = cast(list[dict[str, object]], manifest["tables"])

    for table in tables:
        name = cast(str, table["name"])
        if table_files is not None:
            relative_path, digest = table_files[name]
            table["file"] = relative_path
            table["file_checksum_sha256"] = digest
        if worksheets is not None:
            table["worksheet"] = worksheets[name]

    if table_files is not None:
        manifest["data_dictionary_file"] = "data-dictionary.json"
        manifest["checksums_file"] = "checksums.sha256"
    return manifest
