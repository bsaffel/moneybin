"""Encrypted backups publish only verified, quiescent recovery evidence."""

import errno
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database


def test_backup_refuses_committed_wal_without_publishing(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_backup_service import backup_encrypted_database

    source, output = tmp_path / "source.duckdb", tmp_path / "backup.duckdb"
    with Database(
        source, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        db.execute(
            "INSERT INTO app.versions (component, version) VALUES ('baseline', '1')"
        )
        db.checkpoint("pre_backup")
        db.execute(
            "INSERT INTO app.versions (component, version) VALUES ('retained', '2')"
        )
        assert Path(str(source) + ".wal").exists()
        with pytest.raises(ValueError, match="checkpoint_required|active_access"):
            backup_encrypted_database(source, output, secret_store=mock_secret_store)
        assert not output.exists()


def test_backup_reopens_encrypted_state_and_preserves_existing_output(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_backup_service import backup_encrypted_database

    source, output = tmp_path / "source.duckdb", tmp_path / "backup.duckdb"
    with Database(
        source, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        db.execute(
            "INSERT INTO app.versions (component, version) VALUES ('retained', '2')"
        )
    backup_encrypted_database(source, output, secret_store=mock_secret_store)
    with Database(output, read_only=True, secret_store=mock_secret_store) as restored:
        assert restored.execute(
            "SELECT version FROM app.versions WHERE component = 'retained'"
        ).fetchone() == ("2",)
    original = output.read_bytes()
    with pytest.raises((FileExistsError, ValueError)):
        backup_encrypted_database(source, output, secret_store=mock_secret_store)
    assert output.read_bytes() == original


def test_backup_refuses_open_writer_even_after_checkpoint(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_backup_service import backup_encrypted_database

    source, output = tmp_path / "source.duckdb", tmp_path / "backup.duckdb"
    with Database(
        source, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ) as db:
        db.checkpoint("pre_backup")
        with pytest.raises(ValueError, match="active_access|checkpoint_required"):
            backup_encrypted_database(source, output, secret_store=mock_secret_store)
        assert not output.exists()


def test_invalid_database_never_publishes_backup(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_backup_service import backup_encrypted_database

    source, output = tmp_path / "invalid.duckdb", tmp_path / "backup.duckdb"
    source.write_bytes(b"invalid encrypted database")
    with pytest.raises(duckdb.Error):
        backup_encrypted_database(source, output, secret_store=mock_secret_store)
    assert not output.exists()
    assert not list(tmp_path.glob(".backup-*"))


def test_backup_refuses_destination_with_retained_wal(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_backup_service import backup_encrypted_database

    source, output = tmp_path / "source.duckdb", tmp_path / "backup.duckdb"
    with Database(
        source, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ):
        pass
    wal = Path(str(output) + ".wal")
    wal.write_bytes(b"retained recovery evidence")
    with pytest.raises(ValueError, match="checkpoint_required"):
        backup_encrypted_database(source, output, secret_store=mock_secret_store)
    assert not output.exists()
    assert wal.read_bytes() == b"retained recovery evidence"


@pytest.mark.parametrize("error_number", [errno.EXDEV, errno.ENOTSUP, errno.EOPNOTSUPP])
def test_backup_reports_unsupported_hard_link_without_publishing(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    error_number: int,
) -> None:
    from moneybin.services import database_backup_service as service

    source, output = tmp_path / "source.duckdb", tmp_path / "backup.duckdb"
    with Database(
        source, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ):
        pass
    original = source.read_bytes()

    def unsupported_link(*args: object, **kwargs: object) -> None:
        raise OSError(error_number, "private-marker")

    monkeypatch.setattr(service.os, "link", unsupported_link)
    with pytest.raises(ValueError, match="^hard_link_unavailable$"):
        service.backup_encrypted_database(
            source, output, secret_store=mock_secret_store
        )
    assert not output.exists()
    assert source.read_bytes() == original
    assert not list(tmp_path.glob(".backup-*"))
