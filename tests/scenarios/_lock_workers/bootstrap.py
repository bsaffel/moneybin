"""Bootstrap worker: initialize an encrypted DuckDB with a tiny test table.

Run as a stand-alone script (not imported as part of a package). Used by the
``bootstrapped_db`` fixture in ``test_writer_coordination.py`` so each scenario
has a fresh encrypted file to contend over without inheriting the test
process's DuckDB module state.

Usage:
    python bootstrap.py <db_path>
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock


def main() -> None:
    """Create the encrypted DB plus a ``t (x INTEGER)`` table seeded with one row."""
    db_path = Path(sys.argv[1])

    mock_store = MagicMock()
    mock_store.get_key.return_value = "scenario-ephemeral-key-tmpdir-only"

    from moneybin.database import Database

    db = Database(
        db_path,
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    try:
        db.execute("CREATE TABLE t (x INTEGER)")
        db.execute("INSERT INTO t VALUES (1)")
        db.checkpoint("post_migration")
    finally:
        db.close()


if __name__ == "__main__":
    main()
