"""Publish verified encrypted backups from quiescent database snapshots."""

from __future__ import annotations

import errno
import os
import tempfile
from pathlib import Path

from moneybin.database import Database
from moneybin.secrets import SecretStore
from moneybin.services.database_restore_lock import (
    encrypted_inspection_snapshot,
    fingerprint_file,
)
from moneybin.services.database_restore_service import (
    _copy_exclusive,  # pyright: ignore[reportPrivateUsage]  # reuse verified recovery copy primitive
    _require_snapshot,  # pyright: ignore[reportPrivateUsage]  # preserve pending WAL recovery evidence
    _sync_directory,  # pyright: ignore[reportPrivateUsage]  # reuse verified recovery durability primitive
)


def backup_encrypted_database(
    source: Path, destination: Path, *, secret_store: SecretStore
) -> None:
    """Refuse live writers/WAL and publish a validated copy without overwriting."""
    source, destination = source.resolve(), destination.resolve()
    if source == destination or destination.exists():
        raise FileExistsError("backup_destination_exists")
    _require_snapshot(destination)
    with encrypted_inspection_snapshot(source) as (snapshot, source_lease):
        with Database(snapshot, read_only=True, secret_store=secret_store):
            pass
        with tempfile.TemporaryDirectory(
            dir=destination.parent, prefix=".backup-"
        ) as directory:
            candidate = Path(directory) / "candidate.duckdb"
            _copy_exclusive(snapshot, candidate, remove_on_failure=True)
            with Database(candidate, read_only=True, secret_store=secret_store):
                pass
            if fingerprint_file(candidate) != source_lease.fingerprint:
                raise ValueError("copy_verification_failed")
            source_lease.check()
            # Hard linking publishes complete bytes atomically and refuses an
            # output created by another actor during validation.
            _require_snapshot(destination)
            try:
                os.link(candidate, destination)
            except OSError as exc:
                if exc.errno in {errno.EXDEV, errno.ENOTSUP, errno.EOPNOTSUPP}:
                    raise ValueError("hard_link_unavailable") from None
                raise
            _sync_directory(destination.parent)
