"""Atomic local export publication tests."""

from __future__ import annotations

import stat
from pathlib import Path
from zipfile import ZipFile

import pytest
from openpyxl import load_workbook

import moneybin.exports.local as local_delivery
from moneybin.exports.local import LocalExportPublisher
from moneybin.exports.manifest import LocalExportFormat
from moneybin.exports.renderers import RenderedArtifact
from moneybin.exports.snapshot import PreparedExport
from tests.moneybin.test_exports.test_renderers import make_snapshot


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.mark.parametrize(
    ("format", "extension"), [("csv", "csv"), ("parquet", "parquet")]
)
def test_publish_creates_an_exact_restrictive_timestamped_bundle(
    tmp_path: Path,
    format: LocalExportFormat,
    extension: str,
) -> None:
    exports_root = tmp_path / "exports"
    publisher = LocalExportPublisher(exports_root)

    receipt = publisher.publish(make_snapshot(), format=format, compress_zip=False)

    assert receipt.artifact_path is not None
    assert receipt.artifact_path.is_absolute()
    assert receipt.artifact_path.name == "export-20260721T184233Z"
    assert receipt.destination.local_path == exports_root.resolve()
    assert receipt.subject == {"kind": "bundle"}
    assert receipt.redaction_mode == "redacted"
    assert receipt.row_counts == {"activity": 2}
    assert receipt.compressed_artifact_path is None
    assert receipt.recovery_actions == ()
    assert {
        path.relative_to(receipt.artifact_path).as_posix()
        for path in receipt.artifact_path.rglob("*")
    } == {
        "manifest.json",
        "checksums.sha256",
        "data-dictionary.json",
        "tables",
        f"tables/activity.{extension}",
    }
    assert _mode(receipt.artifact_path) == 0o700
    assert _mode(receipt.artifact_path / "tables") == 0o700
    assert all(
        _mode(path) == 0o600
        for path in receipt.artifact_path.rglob("*")
        if path.is_file()
    )
    assert set(receipt.checksums) == {f"tables/activity.{extension}"}


def test_publish_never_overwrites_a_successful_collision(tmp_path: Path) -> None:
    exports_root = tmp_path / "exports"
    publisher = LocalExportPublisher(exports_root)

    first = publisher.publish(make_snapshot(), format="csv", compress_zip=False)
    second = publisher.publish(make_snapshot(), format="csv", compress_zip=False)

    assert first.artifact_path is not None
    assert second.artifact_path is not None
    assert first.artifact_path.name == "export-20260721T184233Z"
    assert second.artifact_path.name == "export-20260721T184233Z-2"
    assert first.artifact_path.exists()
    assert second.artifact_path.exists()
    assert _mode(exports_root / ".publish.lock") == 0o600


def test_tampered_staging_bundle_is_not_published_or_left_behind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exports_root = tmp_path / "exports"
    original = local_delivery.render_csv

    def render_then_tamper(
        snapshot: PreparedExport, staging_root: Path
    ) -> RenderedArtifact:
        rendered = original(snapshot, staging_root)
        rendered.table_files["activity"].write_bytes(b"tampered")
        return rendered

    monkeypatch.setattr(local_delivery, "render_csv", render_then_tamper)

    with pytest.raises(ValueError, match="checksum"):
        LocalExportPublisher(exports_root).publish(
            make_snapshot(), format="csv", compress_zip=False
        )

    assert not list(exports_root.glob("export-*"))
    assert not list(exports_root.glob(".staging-*"))


def test_zip_is_an_additional_complete_validated_bundle(tmp_path: Path) -> None:
    receipt = LocalExportPublisher(tmp_path / "exports").publish(
        make_snapshot(), format="csv", compress_zip=True
    )

    assert receipt.artifact_path is not None
    assert receipt.artifact_path.is_dir()
    assert receipt.compressed_artifact_path is not None
    assert receipt.compressed_artifact_path.name == f"{receipt.artifact_path.name}.zip"
    assert _mode(receipt.compressed_artifact_path) == 0o600
    with ZipFile(receipt.compressed_artifact_path) as archive:
        assert set(archive.namelist()) == {
            "manifest.json",
            "checksums.sha256",
            "data-dictionary.json",
            "tables/activity.csv",
        }
        for member in archive.namelist():
            assert archive.read(member) == (receipt.artifact_path / member).read_bytes()


def test_xlsx_rejects_zip_before_creating_staging(tmp_path: Path) -> None:
    exports_root = tmp_path / "exports"

    with pytest.raises(ValueError, match="XLSX"):
        LocalExportPublisher(exports_root).publish(
            make_snapshot(), format="xlsx", compress_zip=True
        )

    assert not exports_root.exists()


def test_tampered_xlsx_data_is_not_published(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exports_root = tmp_path / "exports"
    original = local_delivery.render_xlsx

    def render_then_tamper(
        snapshot: PreparedExport, staging_root: Path
    ) -> RenderedArtifact:
        rendered = original(snapshot, staging_root)
        workbook = load_workbook(rendered.path)
        workbook["activity"]["B2"] = "999.99"
        workbook.save(rendered.path)
        return rendered

    monkeypatch.setattr(local_delivery, "render_xlsx", render_then_tamper)

    with pytest.raises(ValueError, match="cell"):
        LocalExportPublisher(exports_root).publish(
            make_snapshot(), format="xlsx", compress_zip=False
        )

    assert not list(exports_root.glob("export-*"))
    assert not list(exports_root.glob(".staging-*"))


def test_xlsx_publishes_one_restrictive_timestamped_workbook(tmp_path: Path) -> None:
    receipt = LocalExportPublisher(tmp_path / "exports").publish(
        make_snapshot(), format="xlsx", compress_zip=False
    )

    assert receipt.artifact_path is not None
    assert receipt.artifact_path.name == "export-20260721T184233Z.xlsx"
    assert receipt.artifact_path.is_file()
    assert _mode(receipt.artifact_path) == 0o600
    assert set(receipt.checksums) == {"export.xlsx"}
