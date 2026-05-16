# ruff: noqa: S101
"""MCP import_files tool: list-shaped, end-of-batch apply (integration)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.mcp.tools.import_tools import import_files

pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "ofx"
_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _make_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _ENCRYPTION_KEY
    return store


def _setup_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Build an encrypted test DB and patch get_database()/Path.home()."""
    secret_store = _make_secret_store()
    db_path = tmp_path / "mcp_files.duckdb"
    Database(db_path, secret_store=secret_store).close()

    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = False
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
    monkeypatch.setattr("moneybin.database.SecretStore", lambda: secret_store)

    # The MCP tool validates paths against Path.home(); steer it to tmp_path
    # so fixtures copied under tmp_path pass validation in the sandbox.
    monkeypatch.setattr(Path, "home", lambda: tmp_path)


def _copy_fixture(src: Path, dest_dir: Path) -> Path:
    """Copy a fixture into tmp_path so it lives under the patched Path.home()."""
    dest = dest_dir / src.name
    dest.write_bytes(src.read_bytes())
    return dest


async def test_import_files_accepts_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three good files -> all imported, transforms applied once."""
    _setup_db(tmp_path, monkeypatch)
    fixtures = [
        _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path),
        _copy_fixture(FIXTURES_DIR / "multi_account_sample.ofx", tmp_path),
        _copy_fixture(FIXTURES_DIR / "qbo_bank_sample.qbo", tmp_path),
    ]
    paths = [str(p) for p in fixtures]
    env = await import_files(paths=paths, apply_transforms=True)
    assert env.data["total_count"] == 3
    assert env.data["imported_count"] == 3
    assert env.data["transforms_applied"] is True
    assert len(env.data["files"]) == 3
    assert all(r["status"] == "imported" for r in env.data["files"])


async def test_import_files_continues_past_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bogus file in middle: 2 imported, 1 failed, transforms still apply."""
    _setup_db(tmp_path, monkeypatch)
    good_a = _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path)
    good_b = _copy_fixture(FIXTURES_DIR / "multi_account_sample.ofx", tmp_path)
    bogus = tmp_path / "bogus.ofx"
    bogus.write_text("not actually OFX content\n")

    env = await import_files(
        paths=[str(good_a), str(bogus), str(good_b)],
        apply_transforms=True,
    )
    assert env.data["imported_count"] == 2
    assert env.data["failed_count"] == 1
    assert any(r["status"] == "failed" for r in env.data["files"])


async def test_import_files_apply_transforms_false_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """apply_transforms=False suppresses transforms; action hints transform_apply."""
    _setup_db(tmp_path, monkeypatch)
    fixture = _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path)
    env = await import_files(paths=[str(fixture)], apply_transforms=False)
    assert env.data["transforms_applied"] is False
    assert any("transform_apply" in a for a in env.actions)


async def test_import_files_validates_path_under_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paths outside the user's home are rejected (path traversal guard).

    The decorator catches ``UserError`` and converts it to an error
    envelope with the validator's ``invalid_file_path`` code.
    """
    _setup_db(tmp_path, monkeypatch)
    env = await import_files(paths=["/etc/passwd"])
    assert env.error is not None
    assert env.error.code == "invalid_file_path"
