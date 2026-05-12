"""Tests for Database class — centralized encrypted connection management."""

import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database, DatabaseKeyError, get_database


@pytest.fixture()
def db_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for test databases."""
    d = tmp_path / "data" / "test"
    d.mkdir(parents=True)
    return d


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


class TestBuildAttachSql:
    """build_attach_sql() — ATTACH statement construction."""

    def test_default_has_no_read_only(self) -> None:
        from moneybin.database import build_attach_sql

        sql = build_attach_sql(Path("/tmp/db.duckdb"), "key123")  # noqa: S108 — hardcoded path for SQL string test, not an actual file
        assert "READ_ONLY" not in sql

    def test_read_only_true_appends_flag(self) -> None:
        from moneybin.database import build_attach_sql

        sql = build_attach_sql(Path("/tmp/db.duckdb"), "key123", read_only=True)  # noqa: S108 — hardcoded path for SQL string test, not an actual file
        assert "READ_ONLY" in sql
        # Verify it's inside the options parens, after ENCRYPTION_KEY
        assert "(TYPE DUCKDB, ENCRYPTION_KEY 'key123', READ_ONLY)" in sql


class TestNewExceptions:
    """New exception classes introduced for writer coordination."""

    def test_database_lock_error_is_exception(self) -> None:
        from moneybin.database import DatabaseLockError

        err = DatabaseLockError("lock held")
        assert isinstance(err, Exception)
        assert str(err) == "lock held"

    def test_database_not_initialized_error_is_exception(self) -> None:
        from moneybin.database import DatabaseNotInitializedError

        err = DatabaseNotInitializedError("db missing")
        assert isinstance(err, Exception)
        assert str(err) == "db missing"


class TestDatabaseInit:
    """Database initialization and encrypted attachment."""

    def test_creates_encrypted_database(
        self, db_dir: Path, mock_secret_store: MagicMock, encryption_key: str
    ) -> None:
        """New database file is created and encrypted."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
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
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
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
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
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
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
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

    def test_raises_database_key_error_when_no_key(
        self, db_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DatabaseKeyError raised when SecretStore cannot find the key."""
        import moneybin.database as db_module

        # Clear the key cache so the store is actually consulted.
        monkeypatch.setattr(db_module, "_cached_encryption_key", None)
        store = MagicMock()
        from moneybin.secrets import SecretNotFoundError

        store.get_key.side_effect = SecretNotFoundError("not found")
        db_path = db_dir / "moneybin.duckdb"
        with pytest.raises(DatabaseKeyError, match="encryption key"):
            Database(db_path, secret_store=store)


class TestDatabaseOperations:
    """Database.execute(), .sql(), .conn property."""

    @pytest.fixture()
    def db(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> Generator[Database, None, None]:
        db_path = db_dir / "moneybin.duckdb"
        database = Database(
            db_path, secret_store=mock_secret_store, no_auto_upgrade=True
        )
        yield database
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
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        db.close()
        with pytest.raises(RuntimeError, match="closed"):
            _ = db.conn


class TestIngestDataframe:
    """Database.ingest_dataframe() — Arrow-based batch loading."""

    @pytest.fixture()
    def db(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> Generator[Database, None, None]:
        db_path = db_dir / "moneybin.duckdb"
        database = Database(
            db_path, secret_store=mock_secret_store, no_auto_upgrade=True
        )
        database.execute(
            "CREATE TABLE test_items (id INTEGER, name VARCHAR, score DECIMAL(5,2))"
        )
        yield database
        database.close()

    def test_insert_mode_loads_rows(self, db: Database) -> None:
        """Insert mode appends rows to the target table."""
        import polars as pl

        df = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"], "score": [9.5, 8.0]})
        db.ingest_dataframe("test_items", df, on_conflict="insert")

        result = db.execute("SELECT COUNT(*) FROM test_items").fetchone()
        assert result is not None
        assert result[0] == 2

    def test_replace_mode_recreates_table(self, db: Database) -> None:
        """Replace mode drops and recreates the table from the DataFrame."""
        import polars as pl

        db.execute("INSERT INTO test_items VALUES (1, 'old', 1.0)")
        df = pl.DataFrame({
            "id": [2, 3],
            "name": ["new_a", "new_b"],
            "score": [5.0, 6.0],
        })
        db.ingest_dataframe("test_items", df, on_conflict="replace")

        result = db.execute("SELECT COUNT(*) FROM test_items").fetchone()
        assert result is not None
        assert result[0] == 2
        ids = [
            r[0] for r in db.execute("SELECT id FROM test_items ORDER BY id").fetchall()
        ]
        assert ids == [2, 3]

    def test_upsert_mode_replaces_conflicting_rows(self, db: Database) -> None:
        """Upsert mode replaces conflicting rows (INSERT OR REPLACE) and appends new ones."""
        import polars as pl

        db.execute("CREATE TABLE upsert_items (id INTEGER PRIMARY KEY, val VARCHAR)")
        db.execute("INSERT INTO upsert_items VALUES (1, 'original')")

        df = pl.DataFrame({"id": [1, 2], "val": ["updated", "new"]})
        db.ingest_dataframe("upsert_items", df, on_conflict="upsert")

        rows = db.execute("SELECT id, val FROM upsert_items ORDER BY id").fetchall()
        assert rows == [(1, "updated"), (2, "new")]

    def test_by_name_matching_ignores_column_order(self, db: Database) -> None:
        """Columns are matched by name, so DataFrame column order need not match table order."""
        import polars as pl

        # DataFrame has columns in reverse order
        df = pl.DataFrame({"score": [7.5], "name": ["carol"], "id": [3]})
        db.ingest_dataframe("test_items", df, on_conflict="insert")

        row = db.execute("SELECT id, name, score FROM test_items").fetchone()
        assert row == (3, "carol", 7.5)

    def test_default_columns_receive_defaults(self, db: Database) -> None:
        """Columns absent from the DataFrame receive their DEFAULT values."""
        import polars as pl

        db.execute(
            "CREATE TABLE timed_items "
            "(id INTEGER PRIMARY KEY, val VARCHAR, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        df = pl.DataFrame({"id": [1], "val": ["hello"]})
        db.ingest_dataframe("timed_items", df, on_conflict="insert")

        row = db.execute("SELECT id, val, ts FROM timed_items").fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "hello"
        assert row[2] is not None  # DEFAULT applied

    def test_invalid_on_conflict_raises(self, db: Database) -> None:
        """ValueError raised for unknown on_conflict value."""
        import polars as pl

        df = pl.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="on_conflict"):
            db.ingest_dataframe("test_items", df, on_conflict="bad")  # type: ignore[arg-type]  # negative test: intentionally invalid value to verify the runtime ValueError


class TestRunSqlmeshMigrate:
    """Database._run_sqlmesh_migrate() — in-process SQLMesh state migration."""

    @pytest.fixture()
    def db(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> Generator[Database, None, None]:
        db_path = db_dir / "moneybin.duckdb"
        database = Database(
            db_path, secret_store=mock_secret_store, no_auto_upgrade=True
        )
        yield database
        database.close()

    def test_skips_when_no_sqlmesh_dir(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns True immediately when sqlmesh project dir doesn't exist."""
        monkeypatch.setattr("moneybin.database.Path.is_dir", lambda self: False)  # type: ignore[reportUnknownLambdaType]
        assert db._run_sqlmesh_migrate() is True  # type: ignore[reportPrivateUsage]

    def test_skips_when_sqlmesh_not_installed(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns True and skips when sqlmesh is not importable."""
        # Ensure sqlmesh_root exists so we get past the dir check
        sqlmesh_root = Path(__file__).resolve().parents[2] / "sqlmesh"
        assert sqlmesh_root.is_dir()  # project has sqlmesh dir

        # Evict sqlmesh from module cache so __import__ is actually called.
        # Without this, cached modules bypass fake_import entirely and the
        # real migrate path runs — passing for the wrong reason.
        import builtins

        for key in [k for k in sys.modules if k.startswith("sqlmesh")]:
            monkeypatch.delitem(sys.modules, key)

        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name.startswith("sqlmesh"):
                raise ImportError("mocked")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        assert db._run_sqlmesh_migrate() is True  # type: ignore[reportPrivateUsage]

    def test_success_calls_migrate_and_cleans_cache(
        self, db: Database, mocker: MockerFixture
    ) -> None:
        """Successful path: adapter injected, ctx.migrate() called, cache cleaned."""
        mock_ctx_class = mocker.patch("sqlmesh.Context")
        mock_ctx = mock_ctx_class.return_value
        from sqlmesh.core.config.connection import BaseDuckDBConnectionConfig

        cache = BaseDuckDBConnectionConfig._data_file_to_adapter  # type: ignore[reportPrivateUsage]
        cache_key = str(db.path)

        # Verify adapter is in cache when migrate() is called
        injected_during_migrate: list[bool] = []
        mock_ctx.migrate.side_effect = lambda: injected_during_migrate.append(
            cache_key in cache
        )

        result = db._run_sqlmesh_migrate()  # type: ignore[reportPrivateUsage]

        assert result is True
        mock_ctx.migrate.assert_called_once()
        assert injected_during_migrate == [True]  # adapter was present during migrate
        # Cache should be cleaned up in finally
        assert cache_key not in cache

    def test_failure_logs_warning_and_returns_false(
        self, db: Database, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Failure path: exception caught, warning logged with traceback, False returned."""
        mock_ctx_class = mocker.patch("sqlmesh.Context")
        mock_ctx_class.return_value.migrate.side_effect = RuntimeError("boom")
        from sqlmesh.core.config.connection import BaseDuckDBConnectionConfig

        cache = BaseDuckDBConnectionConfig._data_file_to_adapter  # type: ignore[reportPrivateUsage]
        cache_key = str(db.path)

        import logging

        with caplog.at_level(logging.WARNING):
            result = db._run_sqlmesh_migrate()  # type: ignore[reportPrivateUsage]

        assert result is False
        assert "sqlmesh migrate failed" in caplog.text
        # Cache should still be cleaned up in finally
        assert cache_key not in cache


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
        monkeypatch.setattr(db_module, "_database_instance", None)
        monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
        monkeypatch.setattr("moneybin.database.SecretStore", lambda: mock_secret_store)

        db1 = get_database()
        db2 = get_database()
        assert db1 is db2
        db1.close()
        monkeypatch.setattr(db_module, "_database_instance", None)

    def test_close_database_resets_singleton(
        self,
        db_dir: Path,
        mock_secret_store: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """close_database() closes the connection and resets the singleton."""
        from moneybin import database as db_module
        from moneybin.database import close_database

        db_path = db_dir / "moneybin.duckdb"

        mock_settings = MagicMock()
        mock_settings.database.path = db_path
        monkeypatch.setattr(db_module, "_database_instance", None)
        monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
        monkeypatch.setattr("moneybin.database.SecretStore", lambda: mock_secret_store)

        db = get_database()
        assert db_module._database_instance is db  # type: ignore[reportPrivateUsage]  # test-only: verify singleton state
        close_database()
        assert db_module._database_instance is None  # type: ignore[reportPrivateUsage]  # test-only: verify singleton reset


class TestDatabaseReadOnly:
    """Database read_only=True path — missing file detection and schema skip."""

    def test_raises_not_initialized_when_path_missing(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        from moneybin.database import DatabaseNotInitializedError

        db_path = tmp_path / "nonexistent.duckdb"
        assert not db_path.exists()
        with pytest.raises(DatabaseNotInitializedError, match="db init"):
            Database(db_path, read_only=True, secret_store=mock_secret_store)

    def test_skips_init_schemas_on_read_only(
        self, tmp_path: Path, mock_secret_store: MagicMock, mocker: MockerFixture
    ) -> None:
        db_path = tmp_path / "ro.duckdb"
        # Create a real DB first (write mode)
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        db.close()
        # Open read-only; init_schemas should NOT be called
        mock_init = mocker.patch("moneybin.schema.init_schemas")
        with Database(db_path, read_only=True, secret_store=mock_secret_store):
            pass
        mock_init.assert_not_called()

    def test_read_only_can_query_existing_table(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        db_path = tmp_path / "rw.duckdb"
        with Database(
            db_path, secret_store=mock_secret_store, no_auto_upgrade=True
        ) as db:
            db.execute("CREATE TABLE test_ro (id INTEGER)")
            db.execute("INSERT INTO test_ro VALUES (42)")
        with Database(db_path, read_only=True, secret_store=mock_secret_store) as ro_db:
            result = ro_db.execute("SELECT id FROM test_ro").fetchone()
        assert result == (42,)


class TestEncryptionKeyCache:
    """_cached_encryption_key module-level cache avoids repeated SecretStore lookups."""

    def test_second_init_skips_store_get_key(
        self,
        tmp_path: Path,
        mock_secret_store: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import moneybin.database as db_module

        monkeypatch.setattr(db_module, "_cached_encryption_key", None)
        db_path = tmp_path / "cached.duckdb"
        # First open — calls store.get_key once
        db1 = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        db1.close()
        call_count_after_first = mock_secret_store.get_key.call_count
        # Second open — key is cached; store.get_key NOT called again
        db2 = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        db2.close()
        assert mock_secret_store.get_key.call_count == call_count_after_first
        # Cleanup cache
        monkeypatch.setattr(db_module, "_cached_encryption_key", None)


class TestActiveWriteSlot:
    """Database.close() deregistration from the _active_write_conn slot."""

    def test_close_deregisters_active_write_conn(
        self,
        tmp_path: Path,
        mock_secret_store: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import moneybin.database as db_module

        monkeypatch.setattr(db_module, "_active_write_conn", None)
        db_path = tmp_path / "wslot.duckdb"
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        # Manually register (get_database() will do this; testing close() cleanup)
        with db_module._active_write_lock:  # pyright: ignore[reportPrivateUsage]  # test-only: verify deregistration
            db_module._active_write_conn = db  # type: ignore[reportPrivateUsage]  # test-only
        assert db_module._active_write_conn is db  # type: ignore[reportPrivateUsage]  # test-only
        db.close()
        assert db_module._active_write_conn is None  # type: ignore[reportPrivateUsage]  # test-only

    def test_close_does_not_deregister_different_conn(
        self,
        tmp_path: Path,
        mock_secret_store: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import moneybin.database as db_module

        other = MagicMock(name="other_conn")
        monkeypatch.setattr(db_module, "_active_write_conn", other)
        db_path = tmp_path / "wslot2.duckdb"
        db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        db.close()
        # Some other conn was registered — our close() shouldn't clear it
        assert db_module._active_write_conn is other  # type: ignore[reportPrivateUsage]  # test-only
        # Cleanup
        monkeypatch.setattr(db_module, "_active_write_conn", None)


class TestTemporarySingleton:
    """_temporary_singleton must restore prior state on both clean exit and exception."""

    def test_restores_prior_singleton_on_clean_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin import database as db_module
        from moneybin.database import (
            _temporary_singleton,  # pyright: ignore[reportPrivateUsage]  # test-only: verify state-restoration contract
        )

        prior = MagicMock(name="prior_singleton")
        monkeypatch.setattr(db_module, "_database_instance", prior)

        local = MagicMock(name="local_db")
        with _temporary_singleton(local):
            assert db_module._database_instance is local  # type: ignore[reportPrivateUsage]  # test-only

        assert db_module._database_instance is prior  # type: ignore[reportPrivateUsage]  # test-only

    def test_restores_prior_singleton_on_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin import database as db_module
        from moneybin.database import (
            _temporary_singleton,  # pyright: ignore[reportPrivateUsage]  # test-only: verify state-restoration contract
        )

        prior = MagicMock(name="prior_singleton")
        monkeypatch.setattr(db_module, "_database_instance", prior)

        local = MagicMock(name="local_db")
        with pytest.raises(RuntimeError, match="boom"):
            with _temporary_singleton(local):
                raise RuntimeError("boom")

        assert db_module._database_instance is prior  # type: ignore[reportPrivateUsage]  # test-only

    def test_restores_none_when_no_prior_singleton(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin import database as db_module
        from moneybin.database import (
            _temporary_singleton,  # pyright: ignore[reportPrivateUsage]  # test-only: verify state-restoration contract
        )

        monkeypatch.setattr(db_module, "_database_instance", None)

        local = MagicMock(name="local_db")
        with _temporary_singleton(local):
            assert db_module._database_instance is local  # type: ignore[reportPrivateUsage]  # test-only

        assert db_module._database_instance is None  # type: ignore[reportPrivateUsage]  # test-only
