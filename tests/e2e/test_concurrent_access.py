"""Multi-process database lock contention tests.

Spawns real subprocesses to test cross-process DuckDB write coordination.
Uses a module-scoped initialized database (created once, shared across tests).

Marked @pytest.mark.e2e — run via:
  uv run pytest tests/e2e/test_concurrent_access.py -m e2e -v
"""

from __future__ import annotations

import os
import subprocess  # noqa: S404 — subprocess is intentional; we test real cross-process DuckDB locking
import sys
import textwrap
import time
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_env(db_path: Path, encryption_key: str) -> dict[str, str]:
    """Build subprocess env with DB path and encryption key injected.

    Subprocesses use Database(db_path, secret_store=...) directly, passing
    the key via a custom env var rather than relying on get_database() /
    get_settings() — MoneyBinSettings.__init__ always builds the DB path from
    the profile directory and ignores MONEYBIN_DATABASE__PATH.
    """
    env = os.environ.copy()
    env["_MB_TEST_DB_PATH"] = str(db_path)
    env["_MB_TEST_KEY"] = encryption_key
    # Prevent subprocess from inheriting profile-based config that might conflict.
    env.pop("MONEYBIN_HOME", None)
    env.pop("MONEYBIN_PROFILE", None)
    return env


def _run_script(
    script: str,
    env: dict[str, str],
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    """Run a Python script in a subprocess using the current venv's python."""
    return subprocess.run(  # noqa: S603 — input is controlled test script, not user input
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# The encryption key used for all tests in this module.
_TEST_KEY = "concurrent-access-test-key-abc123"  # noqa: S105 — test-only key


@pytest.fixture(scope="module")
def concurrent_db(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    """Initialize a real encrypted database for concurrent-access tests.

    Returns (db_path, encryption_key). Tests share the file; each test
    opens and closes its own connections via subprocess.
    """
    from unittest.mock import MagicMock

    from moneybin.database import Database

    base = tmp_path_factory.mktemp("concurrent_access")
    db_path = base / "concurrent.duckdb"

    mock_store = MagicMock()
    mock_store.get_key.return_value = _TEST_KEY

    db = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    db.execute("CREATE TABLE IF NOT EXISTS ping (x INTEGER)")
    db.close()

    return (db_path, _TEST_KEY)


# ---------------------------------------------------------------------------
# Scenario 1: Two read-only connections coexist
# ---------------------------------------------------------------------------

_READ_ONLY_WORKER = """
import os, sys
from pathlib import Path
from unittest.mock import MagicMock
from moneybin.database import Database

db_path = Path(os.environ["_MB_TEST_DB_PATH"])
key = os.environ["_MB_TEST_KEY"]
mock_store = MagicMock()
mock_store.get_key.return_value = key

with Database(db_path, read_only=True, secret_store=mock_store) as db:
    db.execute("SELECT 1").fetchone()
sys.exit(0)
"""


@pytest.mark.e2e
def test_two_read_only_connections_coexist(
    concurrent_db: tuple[Path, str],
) -> None:
    """Two processes can open read_only=True simultaneously without contention."""
    db_path, key = concurrent_db
    env = _make_env(db_path, key)

    script = textwrap.dedent(_READ_ONLY_WORKER)

    p1 = subprocess.Popen(  # noqa: S603 — controlled test script
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    p2 = subprocess.Popen(  # noqa: S603 — controlled test script
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    _, e1 = p1.communicate(timeout=15)
    _, e2 = p2.communicate(timeout=15)
    assert p1.returncode == 0, f"Process 1 failed:\n{e1}"
    assert p2.returncode == 0, f"Process 2 failed:\n{e2}"


# ---------------------------------------------------------------------------
# Scenario 2: Write-write contention — second writer retries and succeeds
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_write_write_contention_retries(
    concurrent_db: tuple[Path, str],
    tmp_path: Path,
) -> None:
    """Process A holds a write connection; Process B retries and succeeds after A exits."""
    db_path, key = concurrent_db
    env = _make_env(db_path, key)
    signal_path = tmp_path / "a_write_ready.flag"

    worker_a = f"""
    import time, sys, os
    from pathlib import Path
    from unittest.mock import MagicMock
    from moneybin.database import Database

    db_path = Path(os.environ["_MB_TEST_DB_PATH"])
    key = os.environ["_MB_TEST_KEY"]
    mock_store = MagicMock()
    mock_store.get_key.return_value = key

    with Database(db_path, secret_store=mock_store, no_auto_upgrade=True) as db:
        open("{signal_path}", "w").close()
        time.sleep(3)
    sys.exit(0)
    """

    worker_b = f"""
    import time, sys, os
    from pathlib import Path
    from unittest.mock import MagicMock
    from moneybin.database import Database, DatabaseLockError

    db_path = Path(os.environ["_MB_TEST_DB_PATH"])
    key = os.environ["_MB_TEST_KEY"]
    mock_store = MagicMock()
    mock_store.get_key.return_value = key
    signal_path = "{signal_path}"

    for _ in range(50):
        if os.path.exists(signal_path):
            break
        time.sleep(0.1)

    # Manual retry loop — same pattern as get_database() internally.
    retry_count = 0
    deadline = time.monotonic() + 8.0
    delay = 0.05
    db = None
    last_exc = None

    while time.monotonic() < deadline:
        try:
            db = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
            break
        except DatabaseLockError as e:
            last_exc = e
            retry_count += 1
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)

    if db is None:
        print(f"FAILED after {{retry_count}} retries. Last: {{last_exc}}", file=sys.stderr)
        sys.exit(2)

    db.close()
    print(f"SUCCESS after {{retry_count}} retries")
    sys.exit(0)
    """

    pa = subprocess.Popen(  # noqa: S603 — controlled test script
        [sys.executable, "-c", textwrap.dedent(worker_a)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    time.sleep(0.3)
    pb = subprocess.Popen(  # noqa: S603 — controlled test script
        [sys.executable, "-c", textwrap.dedent(worker_b)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    _, a_err = pa.communicate(timeout=15)
    b_out, b_err = pb.communicate(timeout=20)

    assert pa.returncode == 0, f"Process A failed: {a_err}"
    assert pb.returncode == 0, f"Process B failed.\nstdout: {b_out}\nstderr: {b_err}"
    assert "SUCCESS" in b_out, f"B output doesn't mention SUCCESS: {b_out}"


# ---------------------------------------------------------------------------
# Scenario 3: Read-only holder blocks write — IOException → DatabaseLockError
# → retry → success
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_read_only_holder_blocks_write_then_succeeds(
    concurrent_db: tuple[Path, str],
    tmp_path: Path,
) -> None:
    """Process A holds read_only=True; Process B retries and succeeds after A exits.

    B's write attempt hits IOException 'Conflicting lock', classified as
    DatabaseLockError. This pins the IOException → DatabaseLockError path.
    """
    db_path, key = concurrent_db
    env = _make_env(db_path, key)
    signal_path = tmp_path / "a_ro_ready.flag"

    worker_a = f"""
    import time, sys, os
    from pathlib import Path
    from unittest.mock import MagicMock
    from moneybin.database import Database

    db_path = Path(os.environ["_MB_TEST_DB_PATH"])
    key = os.environ["_MB_TEST_KEY"]
    mock_store = MagicMock()
    mock_store.get_key.return_value = key

    with Database(db_path, read_only=True, secret_store=mock_store) as db:
        open("{signal_path}", "w").close()
        time.sleep(4)
    sys.exit(0)
    """

    worker_b = f"""
    import time, sys, os
    from pathlib import Path
    from unittest.mock import MagicMock
    from moneybin.database import Database, DatabaseLockError

    db_path = Path(os.environ["_MB_TEST_DB_PATH"])
    key = os.environ["_MB_TEST_KEY"]
    mock_store = MagicMock()
    mock_store.get_key.return_value = key
    signal_path = "{signal_path}"

    for _ in range(50):
        if os.path.exists(signal_path):
            break
        time.sleep(0.1)

    retry_count = 0
    deadline = time.monotonic() + 9.0
    delay = 0.05
    db = None
    last_exc = None

    while time.monotonic() < deadline:
        try:
            db = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
            break
        except DatabaseLockError as e:
            last_exc = e
            retry_count += 1
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)
        except Exception as e:
            print(f"UNEXPECTED: {{type(e).__name__}}: {{e}}", file=sys.stderr)
            sys.exit(1)

    if db is None:
        print(f"FAILED after {{retry_count}} retries. Last: {{last_exc}}", file=sys.stderr)
        sys.exit(2)

    db.close()
    print(f"SUCCESS after {{retry_count}} retries")
    sys.exit(0)
    """

    pa = subprocess.Popen(  # noqa: S603 — controlled test script
        [sys.executable, "-c", textwrap.dedent(worker_a)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    time.sleep(0.3)
    pb = subprocess.Popen(  # noqa: S603 — controlled test script
        [sys.executable, "-c", textwrap.dedent(worker_b)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    _, a_err = pa.communicate(timeout=15)
    b_out, b_err = pb.communicate(timeout=20)

    assert pa.returncode == 0, f"Process A (reader) failed: {a_err}"
    assert pb.returncode == 0, (
        f"Process B (writer) failed (expected retry success).\n"
        f"stdout: {b_out}\nstderr: {b_err}\n"
        f"If exit 2: DatabaseLockError was raised but deadline exhausted.\n"
        f"If exit 1: unexpected exception — see stderr."
    )
    assert "SUCCESS" in b_out, f"B output doesn't mention SUCCESS: {b_out}"
    # Verify B actually had to retry (not a false pass where A exited before B started).
    assert "after 0 retries" not in b_out, (
        f"B succeeded on first attempt — A may have released before B started: {b_out}"
    )
