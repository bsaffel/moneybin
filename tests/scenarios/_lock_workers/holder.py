"""Holder worker: open the DB write-mode, hold for N seconds, INSERT, then close.

Used by scenarios that need a write holder while another process attempts to
acquire. The lifetime of the file lock + DuckDB write attach equals this
script's lifetime.

Usage:
    python holder.py <db_path> <hold_seconds> [<operation_type>]

Output:
    HOLDER_OPEN          (after write lock + attach succeed)
    HOLDER_CLOSED        (after sleep, INSERT, and close)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

from moneybin.db_lock._types import OperationType


def main() -> None:
    """Open write, hold, INSERT, close — prints sync markers throughout."""
    db_path = Path(sys.argv[1])
    hold_seconds = float(sys.argv[2])
    operation_type: OperationType = cast(
        OperationType, sys.argv[3] if len(sys.argv) > 3 else "interactive"
    )

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
        with get_database(read_only=False, operation_type=operation_type) as db:
            print("HOLDER_OPEN", flush=True)  # noqa: T201  # IPC sync marker
            time.sleep(hold_seconds)
            db.execute("INSERT INTO t VALUES (99)")
    print("HOLDER_CLOSED", flush=True)  # noqa: T201  # IPC result marker


if __name__ == "__main__":
    main()
