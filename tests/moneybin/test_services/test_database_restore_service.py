"""Encrypted restore validates sources and retains recovery evidence."""

import hashlib
import os
from pathlib import Path
from typing import BinaryIO
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.secrets import SecretStore


@pytest.fixture
def restore_files(
    tmp_path: Path, mock_secret_store: MagicMock
) -> tuple[Path, Path, Path]:
    source, destination, backup = (
        tmp_path / name
        for name in ("source.duckdb", "destination.duckdb", "original.duckdb")
    )
    for path in (source, destination):
        with Database(
            path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
        ) as db:
            db.execute(
                "INSERT INTO app.versions (component, version) VALUES ('synthetic', ?)",
                [path.stem],
            )
    return source, destination, backup


@pytest.mark.parametrize("failure_stage", ["copy", "validation"])
@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_restore_prejournal_failure_cleans_candidate_and_allows_retry(
    restore_files: tuple[Path, Path, Path],
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    failure: type[BaseException],
) -> None:
    from moneybin.services import database_restore_service as service

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    journal = destination.with_name(destination.name + ".restore-promotion.json")

    def interrupt_copy(input_file: BinaryIO, output: BinaryIO) -> None:
        output.write(input_file.read(64))
        output.flush()
        raise failure("synthetic_failure")

    def interrupt_validation(
        path: Path, *, read_only: bool, secret_store: SecretStore
    ) -> Database:
        if ".restore-" in path.name:
            raise failure("synthetic_failure")
        return Database(path, read_only=read_only, secret_store=secret_store)

    with monkeypatch.context() as patcher:
        if failure_stage == "copy":
            patcher.setattr(service.shutil, "copyfileobj", interrupt_copy)
        else:
            patcher.setattr(service, "Database", interrupt_validation)
        with pytest.raises(failure, match="synthetic_failure"):
            service.restore_encrypted_database(
                source, destination, secret_store=mock_secret_store, backup_path=backup
            )

    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not list(destination.parent.glob("destination.restore-*.duckdb"))
    assert not journal.exists()
    assert not backup.exists()
    assert (
        service.restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )
        == backup
    )
    assert destination.read_bytes() == source_bytes
    assert backup.read_bytes() == destination_bytes
    assert source.read_bytes() == source_bytes
    assert not journal.exists()


def test_restore_preserves_preexisting_candidate(
    restore_files: tuple[Path, Path, Path], mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_restore_service import restore_encrypted_database

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    fingerprint = hashlib.sha256(source_bytes).hexdigest()
    candidate = destination.with_name(f"destination.restore-{fingerprint[:16]}.duckdb")
    candidate.write_bytes(b"existing recovery evidence")

    with pytest.raises(ValueError, match="recovery_required"):
        restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )

    assert candidate.read_bytes() == b"existing recovery evidence"
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not backup.exists()


def test_restore_preserves_replaced_candidate_on_validation_interrupt(
    restore_files: tuple[Path, Path, Path],
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.services import database_restore_service as service

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    replacement = destination.with_name("replacement.duckdb")
    replacement.write_bytes(b"replacement recovery evidence")

    def replace_candidate(
        path: Path, *, read_only: bool, secret_store: SecretStore
    ) -> Database:
        if ".restore-" in path.name:
            replacement.replace(path)
            raise KeyboardInterrupt
        return Database(path, read_only=read_only, secret_store=secret_store)

    monkeypatch.setattr(service, "Database", replace_candidate)
    with pytest.raises(KeyboardInterrupt):
        service.restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )

    candidates = list(destination.parent.glob("destination.restore-*.duckdb"))
    assert len(candidates) == 1
    assert candidates[0].read_bytes() == b"replacement recovery evidence"
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not backup.exists()


def test_restore_preserves_candidate_created_before_exclusive_open(
    restore_files: tuple[Path, Path, Path],
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.services import database_restore_service as service

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    original_copy = service._copy_exclusive  # pyright: ignore[reportPrivateUsage]  # inject the exclusive-create race

    def competing_copy(
        source_path: Path, target: Path, *, remove_on_failure: bool = False
    ) -> os.stat_result:
        target.write_bytes(b"concurrent recovery evidence")
        return original_copy(source_path, target, remove_on_failure=remove_on_failure)

    with monkeypatch.context() as patcher:
        patcher.setattr(service, "_copy_exclusive", competing_copy)
        with pytest.raises(FileExistsError):
            service.restore_encrypted_database(
                source, destination, secret_store=mock_secret_store, backup_path=backup
            )

    candidates = list(destination.parent.glob("destination.restore-*.duckdb"))
    assert len(candidates) == 1
    assert candidates[0].read_bytes() == b"concurrent recovery evidence"
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not backup.exists()


def test_restore_preserves_journaled_candidate_before_promotion(
    restore_files: tuple[Path, Path, Path],
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.services.database_restore_lock import SourceLease
    from moneybin.services.database_restore_service import (
        recover_restore,
        restore_encrypted_database,
    )

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()

    def interrupt(self: SourceLease, candidate: Path, *, fingerprint: str) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(SourceLease, "promote", interrupt)
    with pytest.raises(KeyboardInterrupt):
        restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )

    journal = destination.with_name(destination.name + ".restore-promotion.json")
    journal_bytes = journal.read_bytes()
    candidates = list(destination.parent.glob("destination.restore-*.duckdb"))
    assert len(candidates) == 1
    assert candidates[0].read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert backup.read_bytes() == destination_bytes
    with pytest.raises(ValueError, match="promotion_not_completed_original_retained"):
        recover_restore(destination)
    assert journal.read_bytes() == journal_bytes
    assert candidates[0].read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert backup.read_bytes() == destination_bytes
    assert source.read_bytes() == source_bytes


def test_restore_lease_refuses_same_process_database_reader(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_restore_lock import exclusive_source

    path = tmp_path / "current.duckdb"
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ):
        pass
    with Database(path, secret_store=mock_secret_store, read_only=True):
        with pytest.raises(ValueError, match="active_access"):
            with exclusive_source(path):
                pass


def test_restore_lease_refuses_wal_and_lost_helper(tmp_path: Path) -> None:
    from moneybin.services.database_restore_lock import exclusive_source

    path = tmp_path / "synthetic.duckdb"
    path.write_bytes(b"synthetic source bytes")
    wal = Path(str(path) + ".wal")
    wal.write_bytes(b"pending")
    with pytest.raises(ValueError, match="checkpoint_required"):
        with exclusive_source(path):
            pass
    wal.unlink()
    with exclusive_source(path) as lease:
        lease.process.kill()
        lease.process.wait(timeout=5)
        with pytest.raises(ValueError, match="lease_lost"):
            lease.check()
    assert path.read_bytes() == b"synthetic source bytes"


def test_restore_lease_refuses_external_holder_and_closed_ipc(tmp_path: Path) -> None:
    from moneybin.services.database_restore_lock import exclusive_source

    path = tmp_path / "synthetic.duckdb"
    path.write_bytes(b"synthetic bytes")
    with exclusive_source(path) as lease:
        with pytest.raises(ValueError, match="active_access"):
            with exclusive_source(path):
                pass
        assert lease.process.stdin is not None
        lease.process.stdin.close()
        with pytest.raises(ValueError, match="lease_lost"):
            lease.check()


@pytest.mark.parametrize("held", ["source", "destination"])
def test_restore_preserves_existing_reader_locks(
    tmp_path: Path, mock_secret_store: MagicMock, held: str
) -> None:
    from moneybin.services.database_restore_service import restore_encrypted_database

    source, destination = (
        tmp_path / name for name in ("source.duckdb", "destination.duckdb")
    )
    for path in (source, destination):
        with Database(
            path, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
        ):
            pass
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    backup = tmp_path / "recovery.duckdb"
    journal = destination.with_name(destination.name + ".restore-promotion.json")
    with Database(
        source if held == "source" else destination,
        read_only=True,
        secret_store=mock_secret_store,
    ):
        with pytest.raises(ValueError, match="active_access"):
            restore_encrypted_database(
                source,
                destination,
                secret_store=mock_secret_store,
                backup_path=backup,
            )
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not list(tmp_path.glob("destination.restore-*.duckdb"))
    assert not journal.exists()
    assert not backup.exists()

    assert (
        restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )
        == backup
    )
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == source_bytes
    assert backup.read_bytes() == destination_bytes
    assert not journal.exists()


@pytest.mark.parametrize("fail_validation", [False, True])
def test_encrypted_inspection_snapshot_preserves_reader_and_cleans_scratch(
    tmp_path: Path, mock_secret_store: MagicMock, fail_validation: bool
) -> None:
    from moneybin.services.database_restore_lock import (
        encrypted_inspection_snapshot,
        exclusive_source,
    )

    path = tmp_path / "snapshot-source.duckdb"
    with Database(
        path, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ):
        pass
    original = path.read_bytes()
    scratch: Path | None = None
    with Database(path, read_only=True, secret_store=mock_secret_store):
        try:
            with encrypted_inspection_snapshot(path) as (snapshot, lease):
                scratch = snapshot.parent
                assert scratch.stat().st_mode & 0o777 == 0o700
                assert snapshot.stat().st_mode & 0o777 == 0o600
                assert snapshot.read_bytes() == original
                with Database(
                    snapshot, read_only=True, secret_store=mock_secret_store
                ) as db:
                    assert db.execute("SELECT 1").fetchone() == (1,)
                lease.check()
                if fail_validation:
                    raise RuntimeError("synthetic_validation_failure")
        except RuntimeError as exc:
            assert fail_validation and str(exc) == "synthetic_validation_failure"
        else:
            assert not fail_validation
        assert scratch is not None and not scratch.exists()
        with pytest.raises(ValueError, match="active_access"):
            with exclusive_source(path):
                pytest.fail("Inspection released the original reader lock")
    assert path.read_bytes() == original


def test_encrypted_inspection_snapshot_refuses_active_writer(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_restore_lock import encrypted_inspection_snapshot

    path = tmp_path / "writer-source.duckdb"
    with Database(
        path, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ):
        pass
    original = path.read_bytes()
    with Database(
        path, read_only=False, secret_store=mock_secret_store, assume_initialized=True
    ):
        assert not Path(str(path) + ".wal").exists()
        with pytest.raises(ValueError, match="active_access"):
            with encrypted_inspection_snapshot(path):
                pytest.fail("Inspection admitted an active writer")
    assert path.read_bytes() == original


@pytest.mark.parametrize("mismatched_backup", [False, True])
def test_interrupted_restore_recovery_requires_matching_original_backup(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    mismatched_backup: bool,
) -> None:
    from moneybin.services.database_restore_lock import SourceLease
    from moneybin.services.database_restore_service import (
        recover_restore,
        restore_encrypted_database,
    )

    source, destination, backup = (
        tmp_path / name
        for name in ("source.duckdb", "destination.duckdb", "original.duckdb")
    )
    for path in (source, destination):
        with Database(
            path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
        ) as db:
            db.execute(
                "INSERT INTO app.versions (component, version) VALUES ('synthetic', ?)",
                [path.stem],
            )
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    promote = SourceLease.promote

    def interrupt(self: SourceLease, candidate: Path, *, fingerprint: str) -> None:
        promote(self, candidate, fingerprint=fingerprint)
        raise KeyboardInterrupt

    with monkeypatch.context() as patcher:
        patcher.setattr(SourceLease, "promote", interrupt)
        with pytest.raises(KeyboardInterrupt):
            restore_encrypted_database(
                source, destination, secret_store=mock_secret_store, backup_path=backup
            )
    journal = destination.with_name(destination.name + ".restore-promotion.json")
    assert destination.read_bytes() == source_bytes
    assert backup.read_bytes() == destination_bytes
    assert journal.exists()
    if mismatched_backup:
        backup.write_bytes(source_bytes)
    journal_bytes, backup_bytes = journal.read_bytes(), backup.read_bytes()

    if mismatched_backup:
        with pytest.raises(ValueError, match="recovery_evidence_mismatch"):
            recover_restore(destination)
        assert journal.read_bytes() == journal_bytes
    else:
        recover_restore(destination)
        assert not journal.exists()
        with Database(
            destination, secret_store=mock_secret_store, read_only=True
        ) as db:
            assert db.execute(
                "SELECT version FROM app.versions WHERE component = 'synthetic'"
            ).fetchone() == ("source",)
    assert destination.read_bytes() == source_bytes
    assert backup.read_bytes() == backup_bytes
    assert source.read_bytes() == source_bytes
