"""Verify worker: read-only open, print all rows from the test table.

Used by the checkpoint-durability scenario to confirm a write + checkpoint +
close is visible to a fresh subprocess re-opening the DB.

Usage:
    python verify.py <db_path>

Output:
    VERIFY:<comma-separated-x-values>
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


def main() -> None:
    """Open read-only and print the rows from table t."""
    db_path = Path(sys.argv[1])

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
        with get_database(read_only=True) as db:
            rows = db.execute("SELECT x FROM t ORDER BY x").fetchall()
    values = ",".join(str(r[0]) for r in rows)
    print(f"VERIFY:{values}", flush=True)  # noqa: T201  # IPC result marker


if __name__ == "__main__":
    main()
