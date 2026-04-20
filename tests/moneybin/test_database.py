"""Tests for Database class — centralized encrypted connection management."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database, DatabaseKeyError, get_database


@pytest.fixture()
def db_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for test databases."""
    return tmp_path / "data" / "test"


@pytest.fixture()
def encryption_key() -> str:
    """Provide a test encryption key string."""
    return "test-encryption-key-256bit-placeholder"


@pytest.fixture()
def mock_secret_store(encryption_key: str) -> MagicMock:
    """Mock SecretStore that returns a test encryption key."""
    store = MagicMock()
    store.get_key.return_value = encryption_key
    return store


class TestDatabaseInit:
    """Database initialization and encrypted attachment."""

    def test_creates_encrypted_database(
        self, db_dir: Path, mock_secret_store: MagicMock, encryption_key: str
    ) -> None:
        """New database file is created and encrypted."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        try:
            assert db_path.exists()
            mock_secret_store.get_key.assert_called_once_with(
                "DATABASE__ENCRYPTION_KEY"
            )
        finally:
            db.close()

    def test_sets_file_permissions_0600(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """Database file is created with owner-only permissions."""
        if sys.platform == "win32":
            pytest.skip("File permissions not enforced on Windows")
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        try:
            mode = db_path.stat().st_mode & 0o777
            assert mode == 0o600
        finally:
            db.close()

    def test_encrypted_file_unreadable_without_key(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """Database file cannot be opened without the encryption key."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        db.execute("CREATE TABLE test_data (id INTEGER, name VARCHAR)")
        db.execute("INSERT INTO test_data VALUES (1, 'Alice')")
        db.close()

        # Try to open without key — should fail.
        # DuckDB raises CatalogException when opening an encrypted file without a key.
        with pytest.raises(duckdb.CatalogException):
            bad_conn = duckdb.connect(str(db_path))
            bad_conn.execute("SELECT * FROM test_data")

    def test_runs_init_schemas(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """Schema initialization runs on first open."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        try:
            # init_schemas creates the raw, core, and app schemas
            result = db.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('raw', 'core', 'app') ORDER BY schema_name"
            ).fetchall()
            schemas = [r[0] for r in result]
            assert "app" in schemas
            assert "raw" in schemas
        finally:
            db.close()

    def test_raises_database_key_error_when_no_key(self, db_dir: Path) -> None:
        """DatabaseKeyError raised when SecretStore cannot find the key."""
        store = MagicMock()
        from moneybin.secrets import SecretNotFoundError

        store.get_key.side_effect = SecretNotFoundError("not found")
        db_path = db_dir / "moneybin.duckdb"
        with pytest.raises(DatabaseKeyError, match="encryption key"):
            Database(db_path, secret_store=store)


class TestDatabaseOperations:
    """Database.execute(), .sql(), .conn property."""

    @pytest.fixture()
    def db(self, db_dir: Path, mock_secret_store: MagicMock) -> Database:
        db_path = db_dir / "moneybin.duckdb"
        database = Database(db_path, secret_store=mock_secret_store)
        yield database  # type: ignore[misc]
        database.close()

    def test_execute_with_params(self, db: Database) -> None:
        """Parameterized query works on attached encrypted database."""
        db.execute("CREATE TABLE test (id INTEGER, val VARCHAR)")
        db.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        result = db.execute("SELECT val FROM test WHERE id = ?", [1]).fetchone()
        assert result is not None
        assert result[0] == "hello"

    def test_sql_convenience(self, db: Database) -> None:
        """sql() method works for parameter-free queries."""
        db.execute("CREATE TABLE test2 (id INTEGER)")
        db.execute("INSERT INTO test2 VALUES (42)")
        result = db.sql("SELECT * FROM test2").fetchone()
        assert result is not None
        assert result[0] == 42

    def test_conn_property(self, db: Database) -> None:
        """Conn property exposes the underlying DuckDB connection."""
        conn = db.conn
        assert isinstance(conn, duckdb.DuckDBPyConnection)

    def test_close_releases_resources(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """After close(), conn access raises."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        db.close()
        with pytest.raises(RuntimeError, match="closed"):
            _ = db.conn


class TestGetDatabase:
    """get_database() singleton behavior."""

    def test_returns_same_instance(
        self,
        db_dir: Path,
        mock_secret_store: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Repeated calls return the same Database instance."""
        from moneybin import database as db_module

        db_path = db_dir / "moneybin.duckdb"

        # Patch get_settings to return our test path
        mock_settings = MagicMock()
        mock_settings.database.path = db_path
        mock_settings.database.temp_directory = db_dir / "temp"
        mock_settings.database.create_dirs = True
        monkeypatch.setattr(db_module, "_database_instance", None)
        monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
        monkeypatch.setattr("moneybin.database.SecretStore", lambda: mock_secret_store)

        db1 = get_database()
        db2 = get_database()
        assert db1 is db2
        db1.close()
        monkeypatch.setattr(db_module, "_database_instance", None)
