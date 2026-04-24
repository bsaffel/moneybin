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
from moneybin.loaders.ofx_loader import OFXLoader

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
# 1. Full import pipeline: encrypted DB → OFX load → SQLMesh → core tables
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestImportPipeline:
    """Load OFX data into an encrypted DB, run transforms, verify core tables."""

    def test_ofx_load_and_transform_produces_core_tables(
        self,
        tmp_path: Path,
        encrypted_db: Database,
        mock_store: MagicMock,
    ) -> None:
        """End-to-end: OFX extract → raw load → SQLMesh → core.fct_transactions."""
        from moneybin.extractors.ofx_extractor import OFXExtractor

        # Extract from fixture
        qfx_path = FIXTURES_DIR / "sample_statement.qfx"
        extractor = OFXExtractor()
        data = extractor.extract_from_file(qfx_path)

        # Load into raw tables
        loader = OFXLoader(encrypted_db)
        counts = loader.load_data(data)
        assert counts["transactions"] == 3
        assert counts["accounts"] >= 1

        # Verify raw data is in the encrypted database
        raw_count = encrypted_db.execute(
            "SELECT COUNT(*) FROM raw.ofx_transactions"
        ).fetchone()
        assert raw_count is not None
        assert raw_count[0] == 3

        # Close DB before SQLMesh (it manages its own connection)
        db_path = encrypted_db.path
        encrypted_db.close()

        # Run SQLMesh transforms against the encrypted database.
        # This is the exact code path that was broken before the fix —
        # SQLMesh needs the encryption key passed via adapter cache.
        from moneybin.services.import_service import run_transforms

        with pytest.MonkeyPatch.context() as mp:
            # Patch SecretStore in both modules that import it
            mp.setattr("moneybin.secrets.SecretStore", lambda: mock_store)
            mp.setattr("moneybin.database.SecretStore", lambda: mock_store)
            # Point get_settings().database.path at the test database
            mock_settings = MagicMock()
            mock_settings.database.path = db_path
            mp.setattr("moneybin.database.get_settings", lambda: mock_settings)
            result = run_transforms()

        assert result is True

        # Reopen and verify core tables have data
        db2 = Database(db_path, secret_store=mock_store)
        try:
            core_txns = db2.execute(
                "SELECT COUNT(*) FROM core.fct_transactions"
            ).fetchone()
            assert core_txns is not None
            assert core_txns[0] >= 3  # at least the 3 OFX transactions

            core_accts = db2.execute(
                "SELECT COUNT(*) FROM core.dim_accounts"
            ).fetchone()
            assert core_accts is not None
            assert core_accts[0] >= 1

            # Verify data fidelity — amounts survived the pipeline
            amounts = db2.execute(
                "SELECT amount FROM core.fct_transactions ORDER BY amount"
            ).fetchall()
            amount_values = [float(r[0]) for r in amounts]
            assert -100.50 in amount_values
            assert -50.00 in amount_values
            assert 1000.00 in amount_values
        finally:
            db2.close()


# ---------------------------------------------------------------------------
# 2. Passphrase round-trip: init → lock → unlock → verify DB opens
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
        result = runner.invoke(app, ["rotate-key", "--yes"])
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

        result = runner.invoke(app, ["query", "SELECT 1", "--format", "json"])
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
