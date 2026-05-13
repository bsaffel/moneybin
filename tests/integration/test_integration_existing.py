# ruff: noqa: S101,S106
"""Integration tests for cross-subsystem interactions.

These tests exercise real encrypted databases, real loaders, and real
SQLMesh transforms — the boundaries where unit-test mocks hide bugs.
They are excluded from ``make test`` (fast feedback) and included in
``make test-all``.

Marker: ``@pytest.mark.integration``
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.database import Database

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
    db = Database(db_path, secret_store=mock_store)
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Data survives a lock/unlock cycle via passphrase derivation."""
        from moneybin.cli.commands.db import app

        runner = CliRunner()
        db_path = tmp_path / "pp_test.duckdb"

        # Mock settings to use our tmp_path
        mock_settings = MagicMock()
        mock_settings.database.path = db_path
        mock_settings.database.encryption_key_mode = "passphrase"
        mock_settings.database.backup_path = tmp_path / "backups"
        mock_settings.database.argon2_time_cost = 1  # fast for tests
        mock_settings.database.argon2_memory_cost = 1024  # small for tests
        mock_settings.database.argon2_parallelism = 1
        mock_settings.database.argon2_hash_len = 32
        monkeypatch.setattr("moneybin.config.get_settings", lambda: mock_settings)

        # Step 1: Init with passphrase
        # Use a real SecretStore backed by a dict (avoid hitting system keychain)
        keychain: dict[str, str] = {}

        def mock_set_key(name: str, value: str) -> None:
            keychain[name] = value

        def mock_get_key(name: str) -> str:
            if name in keychain:
                return keychain[name]
            from moneybin.secrets import SecretNotFoundError

            raise SecretNotFoundError(f"not found: {name}")

        def mock_delete_key(name: str) -> None:
            if name in keychain:
                del keychain[name]
            else:
                from moneybin.secrets import SecretNotFoundError

                raise SecretNotFoundError(f"not found: {name}")

        fake_store = MagicMock()
        fake_store.get_key = mock_get_key
        fake_store.set_key = mock_set_key
        fake_store.delete_key = mock_delete_key
        monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: fake_store)

        result = runner.invoke(
            app,
            ["init", "--passphrase", "--yes"],
            input="testpass123\ntestpass123\n",
        )
        assert result.exit_code == 0, result.output
        assert db_path.exists()
        assert "DATABASE__ENCRYPTION_KEY" in keychain

        # Write some test data
        db = Database(db_path, secret_store=fake_store)
        db.execute("CREATE TABLE raw.test_data (id INTEGER, val VARCHAR)")
        db.execute("INSERT INTO raw.test_data VALUES (1, 'hello')")
        db.close()

        # Step 2: Lock — clears key from keychain
        result = runner.invoke(app, ["lock"])
        assert result.exit_code == 0
        assert "DATABASE__ENCRYPTION_KEY" not in keychain

        # Step 3: Unlock with same passphrase
        result = runner.invoke(app, ["unlock"], input="testpass123\n")
        assert result.exit_code == 0, result.output
        assert "DATABASE__ENCRYPTION_KEY" in keychain

        # Step 4: Verify data is accessible
        db2 = Database(db_path, secret_store=fake_store)
        try:
            row = db2.execute("SELECT val FROM raw.test_data WHERE id = 1").fetchone()
            assert row is not None
            assert row[0] == "hello"
        finally:
            db2.close()


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
        import moneybin.database as _db_mod  # noqa: PLC0415

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
        db = Database(db_path, secret_store=fake_store)
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
        db2 = Database(db_path, secret_store=fake_store)
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
