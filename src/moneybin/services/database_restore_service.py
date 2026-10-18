"""Validate and atomically restore encrypted databases with retained recovery evidence."""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

from moneybin.config import DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS
from moneybin.database import Database
from moneybin.db_lock import write_lock
from moneybin.secrets import SecretStore


def _fingerprint(path: Path) -> str:
    from moneybin.services.database_restore_lock import fingerprint_file

    return fingerprint_file(path)


def _require_snapshot(path: Path) -> None:
    if Path(str(path) + ".wal").exists():
        raise ValueError(
            "checkpoint_required: close the profile in a compatible MoneyBin "
            "version after a successful checkpoint, then inspect again"
        )


def _copy_exclusive(source: Path, target: Path) -> None:
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output, source.open("rb") as input_file:
        shutil.copyfileobj(input_file, output)
        output.flush()
        os.fsync(output.fileno())
    _sync_directory(target.parent)


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_journal(path: Path, payload: dict[str, object]) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(payload, output)
        output.flush()
        os.fsync(output.fileno())
    _sync_directory(path.parent)


def restore_encrypted_database(
    source: Path,
    destination: Path,
    *,
    secret_store: SecretStore,
    backup_path: Path,
) -> Path | None:
    """Validate the backup before retaining/replacing an exclusively held target."""
    from contextlib import ExitStack

    from moneybin.services.database_restore_lock import (
        encrypted_inspection_snapshot,
        exclusive_source,
    )

    source, destination = source.resolve(), destination.resolve()
    if source == destination:
        raise ValueError("restore_source_is_destination")
    _require_snapshot(source)
    with encrypted_inspection_snapshot(source) as (inspection_path, inspection_lease):
        source_fingerprint = inspection_lease.fingerprint
        with Database(inspection_path, read_only=True, secret_store=secret_store):
            pass
    candidate = destination.with_name(
        f"{destination.stem}.restore-{source_fingerprint[:16]}.duckdb"
    )
    journal = destination.with_name(destination.name + ".restore-promotion.json")
    with ExitStack() as stack:
        # Consistent ordering avoids inverted cross-profile writer locks.
        for path in sorted({source, destination}):
            stack.enter_context(
                write_lock(
                    path,
                    deadline=time.monotonic() + DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS,
                    operation_type="backup",
                )
            )
        source_lease = stack.enter_context(exclusive_source(source))
        if source_lease.fingerprint != source_fingerprint:
            raise ValueError("stale_preflight")
        if candidate.exists() or journal.exists():
            raise ValueError("recovery_required")
        _copy_exclusive(source, candidate)
        with Database(candidate, read_only=True, secret_store=secret_store):
            pass
        if _fingerprint(candidate) != source_fingerprint:
            raise ValueError("copy_verification_failed")
        if not destination.exists():
            # link is an atomic create-if-absent: a concurrent creator wins
            # without its bytes being overwritten.
            with exclusive_source(candidate) as candidate_lease:
                candidate_lease.publish(destination)
            return None
        destination_lease = stack.enter_context(exclusive_source(destination))
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        _copy_exclusive(destination, backup_path)
        if _fingerprint(backup_path) != destination_lease.fingerprint:
            raise ValueError("copy_verification_failed")
        _write_journal(
            journal,
            {
                "source_fingerprint": destination_lease.fingerprint,
                "candidate_fingerprint": source_fingerprint,
                "backup_path": str(backup_path.resolve()),
            },
        )
        destination_lease.promote(candidate, fingerprint=source_fingerprint)
        journal.unlink()
        _sync_directory(destination.parent)
        return backup_path


def recover_restore(destination: Path) -> None:
    """Acknowledge an interrupted completed restore only with matching evidence."""
    from moneybin.services.database_restore_lock import exclusive_source

    destination = destination.resolve()
    journal = destination.with_name(destination.name + ".restore-promotion.json")
    payload = json.loads(journal.read_text())
    with (
        write_lock(
            destination,
            deadline=time.monotonic() + DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS,
            operation_type="backup",
        ),
        exclusive_source(destination) as lease,
    ):
        if lease.fingerprint != payload.get("candidate_fingerprint"):
            raise ValueError("promotion_not_completed_original_retained")
        backup = Path(payload["backup_path"])
        if _fingerprint(backup) != payload.get("source_fingerprint"):
            raise ValueError("recovery_evidence_mismatch")
        journal.unlink()
        _sync_directory(destination.parent)
