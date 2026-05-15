# ruff: noqa: S101
"""Batch import behavior: per-file results + end-of-batch transform apply."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.import_service import ImportService

pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "ofx"
_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _make_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _ENCRYPTION_KEY
    return store


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    secret_store = _make_secret_store()
    db_path = tmp_path / "batch.duckdb"
    db = Database(db_path, secret_store=secret_store)
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
    return db


def test_import_files_runs_transforms_once_for_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three good files → transforms applied once at end."""
    db = _build_db(tmp_path, monkeypatch)
    paths = [
        FIXTURES_DIR / "sample_minimal.ofx",
        FIXTURES_DIR / "multi_account_sample.ofx",
        FIXTURES_DIR / "qbo_bank_sample.qbo",
    ]
    for p in paths:
        assert p.exists(), f"missing fixture: {p}"

    result = ImportService(db).import_files(list(paths), apply_transforms=True)
    assert result.imported_count == 3
    assert result.failed_count == 0
    assert result.transforms_applied is True
    assert result.transforms_duration_seconds is not None
    assert result.transforms_duration_seconds > 0


def test_import_files_continues_past_per_file_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-file failures don't abort the batch; transforms still run on successes."""
    db = _build_db(tmp_path, monkeypatch)
    bogus = tmp_path / "bogus.ofx"
    bogus.write_text("not actually OFX content\n")
    paths: list[str | Path] = [
        FIXTURES_DIR / "sample_minimal.ofx",
        bogus,
        FIXTURES_DIR / "multi_account_sample.ofx",
    ]
    result = ImportService(db).import_files(paths, apply_transforms=True)
    assert result.imported_count == 2
    assert result.failed_count == 1
    assert result.transforms_applied is True
    assert any(r.status == "failed" for r in result.per_file)


def test_import_files_skips_apply_when_zero_succeeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If all files fail, transforms must not run."""
    db = _build_db(tmp_path, monkeypatch)
    bogus = tmp_path / "bogus.ofx"
    bogus.write_text("not actually OFX content\n")
    result = ImportService(db).import_files([bogus], apply_transforms=True)
    assert result.imported_count == 0
    assert result.failed_count == 1
    assert result.transforms_applied is False


def test_import_files_respects_apply_transforms_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """apply_transforms=False suppresses end-of-batch transform run."""
    db = _build_db(tmp_path, monkeypatch)
    paths: list[str | Path] = [FIXTURES_DIR / "sample_minimal.ofx"]
    result = ImportService(db).import_files(paths, apply_transforms=False)
    assert result.imported_count == 1
    assert result.transforms_applied is False
