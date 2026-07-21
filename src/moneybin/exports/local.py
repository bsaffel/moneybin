"""Validated, immutable local export publication."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import shutil
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC
from pathlib import Path, PurePosixPath
from typing import Literal, cast
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb
from openpyxl import load_workbook

from moneybin.exports.manifest import LocalExportFormat
from moneybin.exports.models import ExportDestination, ExportReceipt
from moneybin.exports.renderers import (
    RenderedArtifact,
    normalize_tabular_cell,
    render_csv,
    render_parquet,
    render_xlsx,
)
from moneybin.exports.snapshot import PreparedExport

_BUNDLE_SIDECARS = {
    "manifest.json",
    "checksums.sha256",
    "data-dictionary.json",
}


class LocalExportPublisher:
    """Render, validate, and atomically publish profile-scoped artifacts."""

    def __init__(
        self,
        profile_exports_dir: Path,
        *,
        destination_name: str = "local:exports",
    ) -> None:
        """Bind the resolved local destination without creating it."""
        self._exports_root = profile_exports_dir.expanduser().resolve()
        self._destination_name = destination_name

    def publish(
        self,
        snapshot: PreparedExport,
        *,
        format: LocalExportFormat,
        compress_zip: bool,
    ) -> ExportReceipt:
        """Publish a new artifact without replacing any completed run."""
        if format not in {"csv", "parquet", "xlsx"}:
            raise ValueError(f"Unsupported local export format: {format}")
        if format == "xlsx" and compress_zip:
            raise ValueError("XLSX is already compressed and rejects ZIP compression")

        self._create_exports_root()
        staging_root = self._exports_root / f".staging-{uuid4()}"
        staging_root.mkdir(mode=0o700)
        try:
            rendered = self._render(snapshot, format, staging_root)
            if format == "xlsx":
                validate_xlsx(rendered, snapshot)
            else:
                validate_bundle(rendered, format)
            _apply_restrictive_modes(rendered.path)

            zip_path = None
            if compress_zip:
                zip_path = staging_root / "artifact.zip"
                _write_zip(rendered.path, zip_path)
                validate_zip(rendered.path, zip_path)
                zip_path.chmod(0o600)

            with _publication_lock(self._exports_root):
                stem = self._available_stem(
                    snapshot,
                    workbook=format == "xlsx",
                    compressed=compress_zip,
                )
                if format == "xlsx":
                    final_path = self._exports_root / f"{stem}.xlsx"
                else:
                    final_path = self._exports_root / stem
                rendered.path.rename(final_path)

                final_zip = None
                if zip_path is not None:
                    final_zip = self._exports_root / f"{stem}.zip"
                    zip_path.rename(final_zip)

            return ExportReceipt(
                subject=snapshot.subject.as_manifest(),
                redaction_mode=snapshot.redaction_mode,
                destination=ExportDestination(
                    destination_id=None,
                    name=self._destination_name,
                    kind="local",
                    local_path=self._exports_root,
                    spreadsheet_id=None,
                    managed_tab_prefix=None,
                ),
                artifact_path=final_path.resolve(),
                compressed_artifact_path=(
                    final_zip.resolve() if final_zip is not None else None
                ),
                sheets_identity=None,
                row_counts={table.name: len(table.rows) for table in snapshot.tables},
                checksums=dict(rendered.file_checksums),
                recovery_actions=(),
            )
        finally:
            if staging_root.exists():
                shutil.rmtree(staging_root)

    def _create_exports_root(self) -> None:
        if self._exports_root.exists():
            if not self._exports_root.is_dir():
                raise ValueError("local export destination is not a directory")
            return
        self._exports_root.mkdir(mode=0o700, parents=True)
        self._exports_root.chmod(0o700)

    def _render(
        self,
        snapshot: PreparedExport,
        format: LocalExportFormat,
        staging_root: Path,
    ) -> RenderedArtifact:
        if format == "csv":
            return render_csv(snapshot, staging_root / "artifact")
        if format == "parquet":
            return render_parquet(snapshot, staging_root / "artifact")
        return render_xlsx(snapshot, staging_root)

    def _available_stem(
        self,
        snapshot: PreparedExport,
        *,
        workbook: bool,
        compressed: bool,
    ) -> str:
        timestamp = snapshot.created_at.astimezone(UTC)
        base = f"export-{timestamp:%Y%m%dT%H%M%SZ}"
        suffix = 1
        while True:
            stem = base if suffix == 1 else f"{base}-{suffix}"
            primary = self._exports_root / (f"{stem}.xlsx" if workbook else stem)
            archive = self._exports_root / f"{stem}.zip"
            if not primary.exists() and (not compressed or not archive.exists()):
                return stem
            suffix += 1


def validate_bundle(
    rendered: RenderedArtifact,
    format: Literal["csv", "parquet"],
) -> None:
    """Independently validate emitted bundle bytes and receipt records."""
    root = rendered.path
    manifest = _read_json_object(root / "manifest.json")
    dictionary = _read_json_object(root / "data-dictionary.json")
    if manifest.get("format") != format:
        raise ValueError("export manifest format does not match rendered bundle")
    if manifest.get("data_dictionary") != dictionary:
        raise ValueError("export data dictionary does not match manifest")

    tables = cast(list[dict[str, object]], manifest.get("tables"))
    expected_files = {
        cast(str, table["file"]): cast(str, table["file_checksum_sha256"])
        for table in tables
    }
    checksum_records = _read_checksum_records(root / "checksums.sha256")
    if checksum_records != expected_files:
        raise ValueError("export checksum records do not match manifest")

    actual_paths = {
        path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()
    }
    if actual_paths != _BUNDLE_SIDECARS | set(expected_files):
        raise ValueError("export bundle contains missing or unexpected files")

    table_by_file = {cast(str, table["file"]): table for table in tables}
    for relative_path, expected_digest in expected_files.items():
        artifact_path = _safe_bundle_path(root, relative_path)
        if _file_digest(artifact_path) != expected_digest:
            raise ValueError("export table checksum validation failed")
        expected_rows = cast(int, table_by_file[relative_path]["row_count"])
        if _table_row_count(artifact_path, format) != expected_rows:
            raise ValueError("export table row count validation failed")


def validate_xlsx(rendered: RenderedArtifact, snapshot: PreparedExport) -> None:
    """Read the workbook back and validate its visible data and receipts."""
    workbook = load_workbook(rendered.path, read_only=True, data_only=True)
    if "MoneyBin Manifest" not in workbook.sheetnames:
        raise ValueError("XLSX manifest sheet is missing")
    if "MoneyBin Data Dictionary" not in workbook.sheetnames:
        raise ValueError("XLSX data dictionary sheet is missing")
    manifest = json.loads(workbook["MoneyBin Manifest"]["A2"].value)
    dictionary = json.loads(workbook["MoneyBin Data Dictionary"]["A2"].value)
    if manifest.get("format") != "xlsx" or dictionary != snapshot.data_dictionary:
        raise ValueError("XLSX receipt validation failed")

    tables = cast(list[dict[str, object]], manifest.get("tables"))
    if len(tables) != len(snapshot.tables):
        raise ValueError("XLSX table count validation failed")
    for table_manifest, prepared_table in zip(tables, snapshot.tables, strict=True):
        sheet_name = cast(str, table_manifest["worksheet"])
        if sheet_name not in workbook.sheetnames:
            raise ValueError("XLSX data sheet is missing")
        sheet = workbook[sheet_name]
        headers = tuple(cell.value for cell in next(sheet.iter_rows(max_row=1)))
        if headers != tuple(column.name for column in prepared_table.columns):
            raise ValueError("XLSX column validation failed")
        if sheet.max_row - 1 != len(prepared_table.rows):
            raise ValueError("XLSX row count validation failed")
        actual_rows = tuple(sheet.iter_rows(min_row=2, values_only=True))
        expected_rows = tuple(
            tuple(normalize_tabular_cell(value) for value in row)
            for row in prepared_table.rows
        )
        if actual_rows != expected_rows:
            raise ValueError("XLSX cell validation failed")
        if table_manifest["checksum_sha256"] != prepared_table.checksum_sha256:
            raise ValueError("XLSX semantic checksum validation failed")


def validate_zip(bundle_root: Path, zip_path: Path) -> None:
    """Verify every archived byte against the already validated bundle."""
    expected = {
        path.relative_to(bundle_root).as_posix(): _file_digest(path)
        for path in bundle_root.rglob("*")
        if path.is_file()
    }
    with ZipFile(zip_path) as archive:
        if set(archive.namelist()) != set(expected):
            raise ValueError("ZIP does not contain the complete export bundle")
        for name, digest in expected.items():
            if hashlib.sha256(archive.read(name)).hexdigest() != digest:
                raise ValueError("ZIP member checksum validation failed")


def _write_zip(bundle_root: Path, zip_path: Path) -> None:
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(bundle_root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(bundle_root).as_posix())


@contextmanager
def _publication_lock(exports_root: Path) -> Generator[None]:
    lock_path = exports_root / ".publish.lock"
    with lock_path.open("a+b") as handle:
        lock_path.chmod(0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _table_row_count(path: Path, format: Literal["csv", "parquet"]) -> int:
    if format == "csv":
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            next(reader)
            return sum(1 for _row in reader)
    row = duckdb.read_parquet(str(path)).aggregate("count(*)").fetchone()
    if row is None:
        raise ValueError("Parquet row count validation returned no result")
    return cast(int, row[0])


def _safe_bundle_path(root: Path, relative_path: str) -> Path:
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("export manifest contains an unsafe table path")
    candidate = root.joinpath(*relative.parts).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError("export manifest table path escapes its bundle")
    return candidate


def _read_checksum_records(path: Path) -> dict[str, str]:
    records: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("  ", maxsplit=1)
        if len(parts) != 2 or parts[1] in records:
            raise ValueError("export checksum file is malformed")
        digest, relative_path = parts
        if len(digest) != 64:
            raise ValueError("export checksum file is malformed")
        records[relative_path] = digest
    return records


def _read_json_object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("export JSON receipt must be an object")
    return cast(dict[str, object], value)


def _file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _apply_restrictive_modes(path: Path) -> None:
    if path.is_file():
        path.chmod(0o600)
        return
    path.chmod(0o700)
    for child in path.rglob("*"):
        child.chmod(0o700 if child.is_dir() else 0o600)
