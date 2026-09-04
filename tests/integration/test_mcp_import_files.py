# ruff: noqa: S101
"""MCP import_files tool: list-shaped, end-of-batch apply (integration)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.tools.import_tools import import_files
from tests.integration.conftest import make_secret_store as _make_secret_store

pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "ofx"


def _setup_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Build an encrypted test DB and patch get_database()/Path.home()."""
    secret_store = _make_secret_store()
    db_path = tmp_path / "mcp_files.duckdb"
    Database(db_path, secret_store=secret_store, read_only=False).close()

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
    env = import_files(paths=paths, refresh=True)
    assert env.data.total_count == 3
    assert env.data.imported_count == 3
    assert env.data.transforms_applied is True
    assert len(env.data.files) == 3
    assert all(r.status == "imported" for r in env.data.files)


async def test_import_files_continues_past_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bogus file in middle: 2 imported, 1 failed, transforms still apply."""
    _setup_db(tmp_path, monkeypatch)
    good_a = _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path)
    good_b = _copy_fixture(FIXTURES_DIR / "multi_account_sample.ofx", tmp_path)
    bogus = tmp_path / "bogus.ofx"
    bogus.write_text("not actually OFX content\n")

    env = import_files(
        paths=[str(good_a), str(bogus), str(good_b)],
        refresh=True,
    )
    assert env.data.imported_count == 2
    assert env.data.failed_count == 1
    assert any(r.status == "failed" for r in env.data.files)


async def test_import_files_refresh_false_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """refresh=False suppresses transforms; action hints refresh_run."""
    _setup_db(tmp_path, monkeypatch)
    fixture = _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path)
    env = import_files(paths=[str(fixture)], refresh=False)
    assert env.data.transforms_applied is False
    assert any("refresh_run" in a for a in env.actions)


async def test_import_files_validates_path_under_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paths outside the user's home are rejected (path traversal guard).

    The decorator catches ``UserError`` and converts it to an error
    envelope with the validator's ``invalid_file_path`` code.
    """
    _setup_db(tmp_path, monkeypatch)
    with pytest.raises(UserError) as exc_info:
        import_files(paths=["/etc/passwd"])
    assert exc_info.value.code == "import_invalid_file_path"


async def test_failed_file_raises_the_envelope_sensitivity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed row's `error`/`hint` are DESCRIPTION-tier, so the batch is medium.

    `summary.sensitivity` is what an agent reads to drive its consent prompt.
    Deriving it from `confirmation_required` rows alone under-declared every
    batch that merely failed — the CLI had the identical gap.
    """
    _setup_db(tmp_path, monkeypatch)
    bogus = tmp_path / "bogus.ofx"
    bogus.write_text("not actually OFX content\n")

    env = import_files(paths=[str(bogus)], refresh=False)

    assert env.data.failed_count == 1
    assert any(r.error for r in env.data.files)
    assert env.summary.sensitivity == "medium"


async def test_permission_failure_row_carries_structured_details(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MCP gets the same `details` the CLI row does, from the same classifier.

    `data-recovery-contract.md` promises `errno` and `platform` on every
    permission failure. Without them an agent has to grep the `hint` prose to
    recover what the classifier already knew — the pattern `error_code` and
    `details` exist to replace.

    The errno is asserted rather than the macOS branch: `protected_root` needs
    Darwin plus a real protected path, and this test runs on Linux in CI.
    """
    _setup_db(tmp_path, monkeypatch)
    blocked = tmp_path / "statement.ofx"
    blocked.write_text("OFXHEADER:100\n")
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        MagicMock(side_effect=PermissionError(13, "Permission denied", str(blocked))),
    )

    env = import_files(paths=[str(blocked)], refresh=False)

    assert env.data.failed_count == 1
    row = env.data.files[0]
    assert row.error_code == "infra_permission_denied"
    assert row.details is not None
    assert row.details["errno"] == 13
    assert row.details["platform"]
    # One failed file is unanimous, so the batch error carries it too.
    assert env.error is not None
    assert env.error.details == row.details


async def test_hard_refresh_failure_is_not_reported_as_a_parse_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash after the rows landed is a refresh failure, not a parse failure.

    The file parsed and loaded; only the SQLMesh apply blew up. Falling back to
    `import_parse_error` at the batch level steers an agent toward "fix the
    file and re-import" when the correct move is retry-refresh or
    `import_revert` on the orphaned raw load — and `import_id` is deliberately
    preserved on the row for exactly that.
    """
    _setup_db(tmp_path, monkeypatch)
    fixture = _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path)
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.refresh",
        MagicMock(side_effect=RuntimeError("sqlmesh exploded")),
    )

    env = import_files(paths=[str(fixture)], refresh=True)

    assert env.data.failed_count == 1
    row = env.data.files[0]
    # The revert handle survives — that is the recovery this path protects.
    assert row.import_id is not None
    assert env.error is not None
    assert env.error.code == "refresh_model_failed"


async def test_clean_batch_stays_low_sensitivity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bump is conditional — an all-imported batch carries no prose."""
    _setup_db(tmp_path, monkeypatch)
    fixture = _copy_fixture(FIXTURES_DIR / "sample_minimal.ofx", tmp_path)

    env = import_files(paths=[str(fixture)], refresh=False)

    assert env.data.failed_count == 0
    assert env.summary.sensitivity == "low"
