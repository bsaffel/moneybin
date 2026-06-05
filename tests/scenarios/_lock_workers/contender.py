"""Contender worker: attempt a write-mode open; classify the lock error if any.

Exits 0 on both success and on a clean DatabaseLockError so the test reads the
stdout marker (and JSON payload) instead of branching on the exit code. Used
by the writer-timeout scenario to verify the timeout produces a MoneyBin
envelope rather than a raw duckdb.IOException string.

Usage:
    python contender.py <db_path> <max_wait>

Output:
    CONTENDER_ACQUIRED                 (write open succeeded)
    CONTENDER_TIMEOUT:<json_payload>   (DatabaseLockError, classified)

The JSON payload carries the classify_user_error() output:
    {
        "message": str,
        "hint": str | None,
        "recovery_actions": [
            {"tool": str, "arguments": {...}, "confidence": str, "idempotent": bool}
        ]
    }
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


def main() -> None:
    """Attempt a contended write; on DatabaseLockError, emit the classified envelope."""
    db_path = Path(sys.argv[1])
    max_wait = float(sys.argv[2])

    mock_store = MagicMock()
    mock_store.get_key.return_value = "scenario-ephemeral-key-tmpdir-only"
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = True

    from moneybin.database import DatabaseLockError, get_database
    from moneybin.errors import classify_user_error

    with (
        patch("moneybin.database.get_settings", lambda: mock_settings),
        patch("moneybin.database.SecretStore", lambda: mock_store),
    ):
        try:
            with get_database(read_only=False, max_wait=max_wait) as _db:
                pass
            print("CONTENDER_ACQUIRED", flush=True)  # noqa: T201  # IPC result marker
        except DatabaseLockError as e:
            classified = classify_user_error(e)
            if classified is None:
                # classify_user_error must recognize DatabaseLockError — if it
                # doesn't, the envelope contract is broken. Surface the failure
                # via stderr so the test sees an explicit signal rather than an
                # empty JSON payload.
                print(  # noqa: T201  # IPC error marker (stderr)
                    f"CONTENDER_UNCLASSIFIED:{str(e)[:200]}",
                    file=sys.stderr,
                    flush=True,
                )
                sys.exit(2)
            payload = classified.to_dict()
            print(  # noqa: T201  # IPC result marker
                f"CONTENDER_TIMEOUT:{json.dumps(payload)}", flush=True
            )


if __name__ == "__main__":
    main()
