"""Encrypted restore validates sources and retains recovery evidence."""

import hashlib
import json
import os
import stat
import subprocess  # noqa: S404  # abrupt-exit test needs a controlled child process
import sys
import textwrap
from pathlib import Path
from typing import IO, Any, BinaryIO, TextIO
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


@pytest.mark.parametrize("failure_stage", ["copy", "backup_copy", "validation"])
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
    original_copy = service.shutil.copyfileobj

    def interrupt_copy(input_file: BinaryIO, output: BinaryIO) -> None:
        if failure_stage == "backup_copy" and Path(input_file.name) != destination:
            original_copy(input_file, output)
            return
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
        if failure_stage in {"copy", "backup_copy"}:
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


def test_restore_preserves_preexisting_backup(
    restore_files: tuple[Path, Path, Path], mock_secret_store: MagicMock
) -> None:
    from moneybin.services.database_restore_service import restore_encrypted_database

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    backup.write_bytes(b"other actor")

    with pytest.raises(FileExistsError):
        restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )

    assert backup.read_bytes() == b"other actor"
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not list(destination.parent.glob("destination.restore-*.duckdb"))
    assert not destination.with_name(
        destination.name + ".restore-promotion.json"
    ).exists()


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_restore_preserves_replaced_backup_on_copy_failure(
    restore_files: tuple[Path, Path, Path],
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    from moneybin.services import database_restore_service as service

    source, destination, backup = restore_files
    source_bytes, destination_bytes = source.read_bytes(), destination.read_bytes()
    replacement = backup.with_name("replacement.duckdb")
    replacement.write_bytes(b"other actor")
    original_copy = service.shutil.copyfileobj

    def replace_backup(input_file: BinaryIO, output: BinaryIO) -> None:
        if Path(input_file.name) != destination:
            original_copy(input_file, output)
            return
        output.write(input_file.read(64))
        output.flush()
        replacement.replace(backup)
        raise failure("synthetic_backup_failure")

    monkeypatch.setattr(service.shutil, "copyfileobj", replace_backup)
    with pytest.raises(failure, match="synthetic_backup_failure"):
        service.restore_encrypted_database(
            source, destination, secret_store=mock_secret_store, backup_path=backup
        )

    assert backup.read_bytes() == b"other actor"
    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert not list(destination.parent.glob("destination.restore-*.duckdb"))
    assert not destination.with_name(
        destination.name + ".restore-promotion.json"
    ).exists()


@pytest.mark.parametrize("failure_stage", ["write", "flush", "fsync"])
@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_restore_journal_failure_cleans_owned_files_and_allows_retry(
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
    original_fdopen, original_fsync = os.fdopen, os.fsync
    journal_fds: set[int] = set()

    def failing_fdopen(fd: int, mode: str) -> IO[Any] | MagicMock:
        stream = original_fdopen(fd, mode)
        if mode != "w":
            return stream
        journal_fds.add(fd)
        output = MagicMock(wraps=stream)
        output.__enter__.return_value = output
        output.__exit__.side_effect = stream.__exit__

        def interrupt_write(text: str) -> None:
            stream.write(text)
            raise failure("synthetic_journal_failure")

        if failure_stage == "write":
            output.write.side_effect = interrupt_write
        elif failure_stage == "flush":
            output.flush.side_effect = failure("synthetic_journal_failure")
        return output

    def failing_fsync(fd: int) -> None:
        if fd in journal_fds and stat.S_ISREG(os.fstat(fd).st_mode):
            raise failure("synthetic_journal_failure")
        original_fsync(fd)

    with monkeypatch.context() as patcher:
        patcher.setattr(service.os, "fdopen", failing_fdopen)
        if failure_stage == "fsync":
            patcher.setattr(service.os, "fsync", failing_fsync)
        with pytest.raises(failure, match="synthetic_journal_failure"):
            service.restore_encrypted_database(
                source, destination, secret_store=mock_secret_store, backup_path=backup
            )

    assert source.read_bytes() == source_bytes
    assert destination.read_bytes() == destination_bytes
    assert backup.read_bytes() == destination_bytes
    assert not journal.exists()
    assert not list(destination.parent.glob(".*.tmp"))
    assert not list(destination.parent.glob("destination.restore-*.duckdb"))
    retry_backup = backup.with_name("retry-original.duckdb")
    assert (
        service.restore_encrypted_database(
            source,
            destination,
            secret_store=mock_secret_store,
            backup_path=retry_backup,
        )
        == retry_backup
    )
    assert destination.read_bytes() == source_bytes
    assert retry_backup.read_bytes() == destination_bytes
    assert backup.read_bytes() == destination_bytes
    assert not journal.exists()


def test_write_journal_preserves_preexisting_journal(tmp_path: Path) -> None:
    from moneybin.services.database_restore_service import _write_journal  # pyright: ignore[reportPrivateUsage]  # exercise exclusive journal ownership

    journal = tmp_path / "restore-promotion.json"
    journal.write_bytes(b"existing recovery evidence")
    with pytest.raises(FileExistsError):
        _write_journal(journal, {})
    assert journal.read_bytes() == b"existing recovery evidence"
    assert list(tmp_path.iterdir()) == [journal]


def test_write_journal_publishes_only_complete_synced_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.services import database_restore_service as service

    journal = tmp_path / "restore-promotion.json"
    payload: dict[str, object] = {"candidate_fingerprint": "synthetic"}
    original_fsync, original_link = os.fsync, os.link
    synced_files: set[int] = set()

    def observe_fsync(fd: int) -> None:
        identity = os.fstat(fd)
        if stat.S_ISREG(identity.st_mode):
            assert not journal.exists()
            original_fsync(fd)
            synced_files.add(identity.st_ino)
        else:
            original_fsync(fd)

    def observe_link(source: Path, target: Path) -> None:
        assert source.parent == journal.parent
        assert json.loads(source.read_text()) == payload
        assert source.stat().st_ino in synced_files
        assert source.stat().st_mode & 0o777 == 0o600
        original_link(source, target)
        assert json.loads(journal.read_text()) == payload

    monkeypatch.setattr(service.os, "fsync", observe_fsync)
    monkeypatch.setattr(service.os, "link", observe_link)
    service._write_journal(journal, payload)  # pyright: ignore[reportPrivateUsage]  # observe publication boundaries
    assert json.loads(journal.read_text()) == payload
    assert list(tmp_path.iterdir()) == [journal]


def test_write_journal_abrupt_exit_never_publishes_partial_payload(
    tmp_path: Path,
) -> None:
    from moneybin.services.database_restore_service import recover_restore

    destination = tmp_path / "destination.duckdb"
    destination.write_bytes(b"original database")
    journal = destination.with_name(destination.name + ".restore-promotion.json")
    script = textwrap.dedent("""\
        import os
        import sys
        from pathlib import Path
        from moneybin.services import database_restore_service as service

        def exit_during_write(payload, output):
            output.write('{')
            output.flush()
            os._exit(73)

        service.json.dump = exit_during_write
        service._write_journal(Path(sys.argv[1]), {})
        """)
    result = subprocess.run(  # noqa: S603  # fixed synthetic script and test-owned path
        [sys.executable, "-c", script, str(journal)], check=False
    )
    assert result.returncode == 73
    assert not journal.exists()
    temporary = list(tmp_path.glob(".*.tmp"))
    assert len(temporary) == 1
    assert temporary[0].read_bytes() == b"{"
    with pytest.raises(FileNotFoundError):
        recover_restore(destination)
    assert destination.read_bytes() == b"original database"


@pytest.mark.parametrize("competing_journal", [False, True])
def test_write_journal_link_failure_preserves_other_actor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, competing_journal: bool
) -> None:
    from moneybin.services import database_restore_service as service

    journal = tmp_path / "restore-promotion.json"
    original_link = os.link

    def fail_link(source: Path, target: Path) -> None:
        if competing_journal:
            target.write_bytes(b"other actor")
            original_link(source, target)
        raise OSError("synthetic_link_failure")

    monkeypatch.setattr(service.os, "link", fail_link)
    with pytest.raises(OSError):
        service._write_journal(journal, {})  # pyright: ignore[reportPrivateUsage]  # inject publication failure
    assert not list(tmp_path.glob(".*.tmp"))
    if competing_journal:
        assert journal.read_bytes() == b"other actor"
    else:
        assert not journal.exists()


def test_write_journal_directory_sync_failure_retains_complete_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.services import database_restore_service as service

    journal = tmp_path / "restore-promotion.json"
    payload: dict[str, object] = {"candidate_fingerprint": "synthetic"}

    def fail_directory_sync(path: Path) -> None:
        raise OSError("synthetic_directory_sync_failure")

    monkeypatch.setattr(service, "_sync_directory", fail_directory_sync)
    with pytest.raises(OSError, match="synthetic_directory_sync_failure"):
        service._write_journal(journal, payload)  # pyright: ignore[reportPrivateUsage]  # preserve published recovery evidence
    assert json.loads(journal.read_text()) == payload
    assert list(tmp_path.iterdir()) == [journal]


def test_write_journal_preserves_replacement_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.services import database_restore_service as service

    journal = tmp_path / "restore-promotion.json"
    replacement = tmp_path / "replacement.json"
    replacement.write_bytes(b"replacement recovery evidence")

    def replace_journal(payload: dict[str, object], output: TextIO) -> None:
        output.write("{")
        replacement.replace(journal)
        raise KeyboardInterrupt

    monkeypatch.setattr(service.json, "dump", replace_journal)
    with pytest.raises(KeyboardInterrupt):
        service._write_journal(journal, {})  # pyright: ignore[reportPrivateUsage]  # exercise interrupted journal ownership
    assert journal.read_bytes() == b"replacement recovery evidence"


def test_write_journal_preserves_replaced_temporary_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.services import database_restore_service as service

    journal = tmp_path / "restore-promotion.json"
    replacement = tmp_path / "replacement.json"
    replacement.write_bytes(b"other actor")

    def replace_temporary(payload: dict[str, object], output: TextIO) -> None:
        output.write("{")
        identity = os.fstat(output.fileno())
        temporary = next(
            path for path in tmp_path.iterdir() if path.stat().st_ino == identity.st_ino
        )
        assert temporary != journal
        replacement.replace(temporary)
        raise KeyboardInterrupt

    monkeypatch.setattr(service.json, "dump", replace_temporary)
    with pytest.raises(KeyboardInterrupt):
        service._write_journal(journal, {})  # pyright: ignore[reportPrivateUsage]  # exercise temporary ownership
    assert not journal.exists()
    remaining = list(tmp_path.iterdir())
    assert len(remaining) == 1
    assert remaining[0].read_bytes() == b"other actor"


@pytest.mark.parametrize("journal_bytes", [None, b"{", b"{}"])
def test_recover_restore_refuses_missing_or_unverifiable_journal(
    tmp_path: Path, journal_bytes: bytes | None
) -> None:
    from moneybin.services.database_restore_service import recover_restore

    destination = tmp_path / "destination.duckdb"
    destination.write_bytes(b"original database")
    journal = destination.with_name(destination.name + ".restore-promotion.json")
    if journal_bytes is not None:
        journal.write_bytes(journal_bytes)
    with pytest.raises((FileNotFoundError, ValueError)):
        recover_restore(destination)
    assert destination.read_bytes() == b"original database"
    if journal_bytes is not None:
        assert journal.read_bytes() == journal_bytes
    else:
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
