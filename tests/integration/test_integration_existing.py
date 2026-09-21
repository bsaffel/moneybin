"""Integration tests for cross-subsystem interactions.

These tests exercise real encrypted databases, real loaders, and real
SQLMesh transforms — the boundaries where unit-test mocks hide bugs.
They are excluded from ``make test`` (fast feedback) and included in
``make test-all``.

Marker: ``@pytest.mark.integration``
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pexpect
import pytest
from typer.testing import CliRunner

from moneybin.database import Database
from tests.pty_support import spawn_python_pty

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture()
def encryption_key() -> str:
    """Provide a test encryption key."""
    return "integration-test-key-0123456789abcdef"


@pytest.fixture()
def mock_store(encryption_key: str) -> MagicMock:
    """Mock SecretStore that returns a test encryption key."""
    store = MagicMock()
    store.get_key.return_value = encryption_key
    return store


@pytest.fixture()
def encrypted_db(tmp_path: Path, mock_store: MagicMock) -> Database:
    """Real encrypted database with base schemas initialized."""
    db_path = tmp_path / "integration.duckdb"
    db = Database(db_path, secret_store=mock_store, read_only=False)
    return db


# ---------------------------------------------------------------------------
# 1. Passphrase round-trip: init → lock → unlock → verify DB opens
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPassphraseRoundTrip:
    """init --passphrase → lock → unlock → verify data is still there."""

    def test_passphrase_init_lock_unlock_preserves_data(
        self,
        tmp_path: Path,
    ) -> None:
        """Data survives a lock/unlock cycle via passphrase derivation."""
        db_path = tmp_path / "pp_test.duckdb"
        program = """
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from moneybin.cli.commands.db import app
from moneybin.database import Database
from moneybin.secrets import SecretNotFoundError

db_path = Path(os.environ["MONEYBIN_TEST_DB_PATH"])
settings = SimpleNamespace(
    database=SimpleNamespace(
        path=db_path,
        encryption_key_mode="passphrase",
        backup_path=db_path.parent / "backups",
        argon2_time_cost=1,
        argon2_memory_cost=1024,
        argon2_parallelism=1,
        argon2_hash_len=32,
    )
)

class Store:
    values = {}
    def get_key(self, name):
        if name not in self.values:
            raise SecretNotFoundError(name)
        return self.values[name]
    def set_key(self, name, value):
        self.values[name] = value
    def delete_key(self, name):
        if name not in self.values:
            raise SecretNotFoundError(name)
        del self.values[name]

def invoke(args):
    app(args=args, prog_name="moneybin db", standalone_mode=False)

with patch("moneybin.config.get_settings", return_value=settings), patch(
    "moneybin.database.get_settings", return_value=settings
), patch("moneybin.secrets.SecretStore", Store):
    invoke(["init", "--passphrase", "--yes"])
    with Database(
        db_path, secret_store=Store(), read_only=False, no_auto_upgrade=True
    ) as db:
        db.execute("CREATE TABLE raw.test_data (id INTEGER, val VARCHAR)")
        db.execute("INSERT INTO raw.test_data VALUES (1, 'hello')")
    invoke(["lock"])
    assert "DATABASE__ENCRYPTION_KEY" not in Store.values
    invoke(["unlock"])
    with Database(
        db_path, secret_store=Store(), read_only=False, no_auto_upgrade=True
    ) as db:
        row = db.execute("SELECT val FROM raw.test_data WHERE id = 1").fetchone()
    assert row == ("hello",)
print("roundtrip sentinel: hello")
"""
        process = spawn_python_pty(program, env={"MONEYBIN_TEST_DB_PATH": str(db_path)})
        process.child.expect("Enter passphrase")
        process.child.sendline("testpass123")
        process.child.expect("Confirm passphrase")
        process.child.sendline("testpass123")
        process.child.expect("Enter passphrase")
        process.child.sendline("testpass123")
        process.child.expect("roundtrip sentinel: hello")
        process.child.expect(pexpect.EOF)
        process.child.close()

        assert process.child.exitstatus == 0
        assert "testpass123" not in process.transcript.getvalue()


# ---------------------------------------------------------------------------
# 3. Key rotation: create DB with data → rotate → verify data accessible
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestKeyRotation:
    """rotate-key re-encrypts and data remains accessible with new key."""

    def test_rotate_key_preserves_data(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Data is accessible after key rotation."""
        from moneybin.cli.commands.db import app

        runner = CliRunner()
        db_path = tmp_path / "rotate_test.duckdb"

        # Clear any cached key from a prior xdist worker test to prevent it
        # being used when opening this test's fresh DB.
        import moneybin.database as _db_mod

        monkeypatch.setattr(_db_mod, "_cached_encryption_key", None)

        mock_settings = MagicMock()
        mock_settings.database.path = db_path
        mock_settings.database.backup_path = tmp_path / "backups"
        monkeypatch.setattr("moneybin.config.get_settings", lambda: mock_settings)

        # Create DB with initial key
        keychain: dict[str, str] = {}
        initial_key = "initial-test-key-for-rotation"

        def mock_set_key(name: str, value: str) -> None:
            keychain[name] = value

        def mock_get_key(name: str) -> str:
            if name in keychain:
                return keychain[name]
            from moneybin.secrets import SecretNotFoundError

            raise SecretNotFoundError(f"not found: {name}")

        fake_store = MagicMock()
        fake_store.get_key = mock_get_key
        fake_store.set_key = mock_set_key
        monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: fake_store)

        keychain["DATABASE__ENCRYPTION_KEY"] = initial_key

        # Create database and insert data
        db = Database(db_path, secret_store=fake_store, read_only=False)
        db.execute("CREATE TABLE raw.test_data (id INTEGER, val VARCHAR)")
        db.execute("INSERT INTO raw.test_data VALUES (42, 'pre-rotation')")
        db.close()

        # Rotate key
        result = runner.invoke(app, ["key", "rotate", "--yes"])
        assert result.exit_code == 0, result.output

        # Key should have changed
        new_key = keychain["DATABASE__ENCRYPTION_KEY"]
        assert new_key != initial_key
        assert len(new_key) == 64  # 32 bytes hex

        # Verify data is accessible with new key
        db2 = Database(db_path, secret_store=fake_store, read_only=False)
        try:
            row = db2.execute("SELECT val FROM raw.test_data WHERE id = 42").fetchone()
            assert row is not None
            assert row[0] == "pre-rotation"
        finally:
            db2.close()


# ---------------------------------------------------------------------------
# 4. DuckDB CLI format flag ordering
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestQueryFormatFlag:
    """db query --format flag is placed before -c so it actually takes effect."""

    def test_format_flag_precedes_c_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The -json (etc.) flag must appear before -c sql in the subprocess command."""
        from moneybin.cli.commands.db import app

        runner = CliRunner()
        db_path = tmp_path / "fmt_test.duckdb"
        db_path.touch()

        mock_settings = MagicMock()
        mock_settings.database.path = db_path
        monkeypatch.setattr("moneybin.config.get_settings", lambda: mock_settings)

        # Mock _create_init_script to avoid SecretStore
        init_script = tmp_path / "init.sql"
        init_script.touch()
        monkeypatch.setattr(
            "moneybin.cli.commands.db._create_init_script",
            lambda _path: init_script,  # type: ignore[reportUnknownLambdaType]
        )
        monkeypatch.setattr(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            lambda: os.environ.copy(),  # type: ignore[reportUnknownLambdaType]
        )

        captured_cmd: list[str] = []

        def capture_run(cmd: list[str], **kwargs: object) -> None:
            captured_cmd.extend(cmd)

        monkeypatch.setattr("moneybin.cli.commands.db.subprocess.run", capture_run)

        # Also need duckdb CLI to be "found"
        monkeypatch.setattr(
            "moneybin.cli.commands.db.shutil.which",
            lambda _name: "/usr/local/bin/duckdb",  # type: ignore[reportUnknownLambdaType]
        )

        result = runner.invoke(app, ["query", "SELECT 1", "--output", "json"])
        assert result.exit_code == 0, result.output

        # Verify ordering: -json must come before -c
        assert "-json" in captured_cmd
        assert "-c" in captured_cmd
        json_idx = captured_cmd.index("-json")
        c_idx = captured_cmd.index("-c")
        assert json_idx < c_idx, (
            f"-json at index {json_idx} must precede -c at index {c_idx}; "
            f"got: {captured_cmd}"
        )
