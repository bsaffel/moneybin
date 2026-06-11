"""Write-then-checkpoint worker: insert a row, checkpoint, close.

Used by the durability scenario to verify ``Database.checkpoint(reason)`` flushes
the row to disk so a separate process reopening read-only sees it.

Usage:
    python write_then_checkpoint.py <db_path> <reason>

Output:
    WRITE_CHECKPOINT_DONE
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

from moneybin.db_lock._types import CheckpointReason


def main() -> None:
    """Open write, INSERT, checkpoint(reason), close."""
    db_path = Path(sys.argv[1])
    reason: CheckpointReason = cast(CheckpointReason, sys.argv[2])

    mock_store = MagicMock()
    mock_store.get_key.return_value = "scenario-ephemeral-key-tmpdir-only"
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = True

    from moneybin.database import get_database

    with (
        patch("moneybin.database.get_settings", lambda: mock_settings),
        patch("moneybin.database.SecretStore", lambda: mock_store),
    ):
        with get_database(read_only=False) as db:
            db.execute("INSERT INTO t VALUES (6)")
            db.checkpoint(reason)
    print("WRITE_CHECKPOINT_DONE", flush=True)  # noqa: T201  # IPC result marker


if __name__ == "__main__":
    main()
