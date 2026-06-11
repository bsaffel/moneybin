"""Reader worker: open the DB read-only, hold for N seconds, then SELECT and close.

Used by scenarios that need a concurrent reader to verify read-read coexistence
or to hold a read attach while a writer attempts to acquire.

Usage:
    python reader.py <db_path> <hold_seconds>

Output:
    READER_OPEN          (immediately after attach)
    READER_CLOSED:<n>    (after sleep + SELECT COUNT(*) FROM t = n)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch


def main() -> None:
    """Open read-only, hold, SELECT COUNT(*), close — prints sync markers throughout."""
    db_path = Path(sys.argv[1])
    hold_seconds = float(sys.argv[2])

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
            print("READER_OPEN", flush=True)  # noqa: T201  # IPC sync marker
            time.sleep(hold_seconds)
            row = db.execute("SELECT COUNT(*) FROM t").fetchone()
            count = row[0] if row is not None else 0
    print(f"READER_CLOSED:{count}", flush=True)  # noqa: T201  # IPC result marker


if __name__ == "__main__":
    main()
