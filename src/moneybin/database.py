"""Centralized encrypted database connection management.

The Database class is the sole entry point for all database access in
MoneyBin. It handles encryption key retrieval, encrypted file attachment,
extension loading, schema initialization, and migrations.

Usage::

    from moneybin.database import get_database

    db = get_database()
    db.execute("SELECT * FROM core.fct_transactions WHERE account_id = ?", [acct_id])

Never call ``duckdb.connect()`` directly. See the data-protection spec
(``docs/specs/privacy-data-protection.md``) for the full design.
"""

import importlib.metadata
import logging
import stat
import sys
from pathlib import Path
from typing import Any

import duckdb

from moneybin.config import get_settings
from moneybin.secrets import SecretNotFoundError, SecretStore

logger = logging.getLogger(__name__)

_KEY_NAME = "DATABASE__ENCRYPTION_KEY"
_DATABASE_ALIAS = "moneybin"


def build_attach_sql(
    db_path: Path, encryption_key: str, *, alias: str = _DATABASE_ALIAS
) -> str:
    """Build a DuckDB ATTACH statement for an encrypted database.

    Single-quote escapes the path and key to prevent injection. The alias
    is double-quoted via sqlglot as defense in depth. All three parameters
    must come from trusted sources (config, SecretStore, hardcoded literals)
    — never user input.

    Args:
        db_path: Path to the DuckDB database file.
        encryption_key: AES-256-GCM encryption key.
        alias: Database alias in DuckDB (default "moneybin"). Must be a
            simple identifier — callers should only pass hardcoded literals.

    Returns:
        ATTACH SQL string ready for execution.
    """
    from sqlglot import exp

    safe_path = str(db_path).replace("'", "''")
    safe_key = encryption_key.replace("'", "''")
    safe_alias = exp.to_identifier(alias, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
    return (
        f"ATTACH '{safe_path}' AS {safe_alias} "  # noqa: S608 — trusted internal values, single-quote escaped, alias sqlglot-quoted
        f"(TYPE DUCKDB, ENCRYPTION_KEY '{safe_key}')"
    )


class DatabaseKeyError(Exception):
    """Raised when the database encryption key cannot be retrieved."""


class Database:
    """Encrypted DuckDB connection manager.

    One long-lived read-write connection per process. The initialization
    sequence:
        a. Retrieve encryption key via SecretStore
        b. Open in-memory DuckDB connection
        c. Attach encrypted database file
        d. USE <attached_db>
        e. Run init_schemas() (idempotent baseline DDL)
        f. Run pending schema migrations via MigrationRunner
        g. Run sqlmesh migrate if SQLMesh version changed
        h. Record component versions in app.versions

    The Database class does NOT own query logic, transaction boundaries,
    domain rules, or data access patterns. It is infrastructure.

    Args:
        db_path: Path to the DuckDB database file.
        secret_store: SecretStore instance for key retrieval. If None,
            creates a new one.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        secret_store: SecretStore | None = None,
        no_auto_upgrade: bool | None = None,
    ) -> None:
        """Initialize and open the encrypted database connection.

        Args:
            db_path: Path to the DuckDB database file.
            secret_store: SecretStore instance for key retrieval. If None,
                creates a new one.
            no_auto_upgrade: If True, skip versioned migrations and SQLMesh
                migrate on startup. If None, reads from config.

        Raises:
            DatabaseKeyError: If the encryption key cannot be retrieved.
        """
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._closed = False

        store = secret_store or SecretStore()

        try:
            encryption_key = store.get_key(_KEY_NAME)
        except SecretNotFoundError as e:
            raise DatabaseKeyError(
                f"Cannot open database — encryption key not found. "
                f"Run 'moneybin db init' to create a new database, or set "
                f"MONEYBIN_{_KEY_NAME} for CI/headless environments."
            ) from e

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        is_new = not db_path.exists()

        self._conn = duckdb.connect()

        self._conn.execute(build_attach_sql(db_path, encryption_key))
        self._conn.execute(f"USE {_DATABASE_ALIAS}")

        # Set file permissions on new databases (macOS/Linux)
        if is_new and sys.platform != "win32":
            try:
                db_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600
            except OSError:
                logger.warning(f"Could not set file permissions on {db_path}")

        # Validate permissions on existing databases
        if not is_new and sys.platform != "win32":
            self._check_permissions(db_path)

        from moneybin.schema import init_schemas

        init_schemas(self._conn)

        from moneybin.migrations import (
            MigrationError,
            MigrationRunner,
            get_current_versions,
            record_version,
        )

        skip_upgrade = no_auto_upgrade
        if skip_upgrade is None:
            settings = get_settings()
            skip_upgrade = settings.database.no_auto_upgrade
        if not skip_upgrade:
            current_pkg_version = importlib.metadata.version("moneybin")
            stored_versions = get_current_versions(self)
            stored_pkg_version = stored_versions.get("moneybin")

            # Check MoneyBin schema migrations
            if stored_pkg_version != current_pkg_version:
                if stored_pkg_version is not None:
                    logger.info(
                        f"⚙️  MoneyBin upgraded ({stored_pkg_version} → {current_pkg_version}). "
                        f"Applying updates..."
                    )

                # Run pending schema migrations
                runner = MigrationRunner(self)
                result = runner.apply_all()

                if result.failed:
                    result.log_failure()
                    raise MigrationError(
                        f"Migration failed: {result.failed_migration or 'stuck migration'}. "
                        f"See logs for details."
                    )

                if result.applied_count > 0:
                    logger.info(f"  ✅ {result.applied_count} migration(s) applied")

                # Record MoneyBin version
                record_version(self, "moneybin", current_pkg_version)

            # Check SQLMesh version independently — a SQLMesh upgrade
            # without a MoneyBin upgrade still needs `sqlmesh migrate`.
            try:
                sqlmesh_version = importlib.metadata.version("sqlmesh")
                stored_sqlmesh = stored_versions.get("sqlmesh")
                if stored_sqlmesh != sqlmesh_version:
                    if self._run_sqlmesh_migrate():
                        record_version(self, "sqlmesh", sqlmesh_version)
                        if stored_sqlmesh is not None:
                            logger.info("  ✅ SQLMesh state updated")
            except importlib.metadata.PackageNotFoundError:
                pass  # SQLMesh not installed — skip

        logger.info(f"Database connection established: {db_path}")

    def _check_permissions(self, db_path: Path) -> None:
        """Warn if database file has overly permissive permissions.

        Args:
            db_path: Path to the database file.
        """
        try:
            mode = db_path.stat().st_mode & 0o777
            if mode & 0o077:  # group or world readable/writable
                logger.warning(
                    f"⚠️  Database file {db_path} has permissive permissions ({mode:04o}). "
                    f"Run: chmod 600 {db_path}"
                )
        except OSError:
            pass

    def _run_sqlmesh_migrate(self) -> bool:
        """Run sqlmesh migrate to update SQLMesh internal state.

        Called when the installed SQLMesh version differs from the recorded
        version. Uses the SQLMesh Python API in-process so it inherits the
        current profile's encrypted connection — no subprocess needed.

        Returns:
            True if migration succeeded or was skipped (no sqlmesh dir
                or sqlmesh not installed),
            False if migration failed.
        """
        sqlmesh_root = Path(__file__).resolve().parents[2] / "sqlmesh"
        if not sqlmesh_root.is_dir():
            logger.debug("sqlmesh project dir not found, skipping migrate")
            return True

        try:
            from sqlmesh.core.config import Config, GatewayConfig
            from sqlmesh.core.config.connection import (
                BaseDuckDBConnectionConfig,
                DuckDBConnectionConfig,
            )
            from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter

            from sqlmesh import Context  # type: ignore[import-untyped]
        except ImportError:
            logger.debug("sqlmesh not installed, skipping migrate")
            return True

        # Adapter construction and cache injection are inside the try block
        # so that failures degrade gracefully instead of breaking DB init.
        # Note: _data_file_to_adapter is a class-level dict — not thread-safe
        # for concurrent init with the same db_path. Acceptable for single-user.
        cache_key = str(self._db_path)
        try:
            conn = self._conn
            if conn is None:
                raise RuntimeError(
                    "_run_sqlmesh_migrate called before connection is established"
                )
            adapter = DuckDBEngineAdapter(
                lambda: conn,
                default_catalog=_DATABASE_ALIAS,
                register_comments=True,
            )
            BaseDuckDBConnectionConfig._data_file_to_adapter[cache_key] = adapter  # type: ignore[reportPrivateUsage]  # no public API for encrypted DB injection

            config = Config(
                default_gateway="moneybin",
                gateways={
                    "moneybin": GatewayConfig(
                        connection=DuckDBConnectionConfig(database=str(self._db_path)),
                    ),
                },
            )
            ctx = Context(
                paths=str(sqlmesh_root),
                config=config,
                gateway="moneybin",
            )
            ctx.migrate()
            logger.debug("sqlmesh migrate completed successfully")
            return True
        except Exception:  # noqa: BLE001 — sqlmesh migration failures are non-fatal
            logger.warning(
                "⚠️  sqlmesh migrate failed — see logs for details",
                exc_info=True,
            )
            return False
        finally:
            BaseDuckDBConnectionConfig._data_file_to_adapter.pop(cache_key, None)  # type: ignore[reportPrivateUsage]  # cleanup matches injection above

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection.

        Prefer execute() for normal queries. Use this property only when
        you need access to the raw DuckDB API (e.g., registering Arrow tables).

        Raises:
            RuntimeError: If the database has been closed.
        """
        if self._closed or self._conn is None:
            raise RuntimeError(
                "Database connection is closed. "
                "Call get_database() to get a new instance."
            )
        return self._conn

    def ingest_dataframe(
        self,
        table: str,
        df: Any,
        *,
        on_conflict: str = "insert",
    ) -> None:
        """Load a Polars (or Arrow-compatible) DataFrame into the database.

        Converts the DataFrame to Arrow (zero-copy for Polars) and writes
        via the encrypted connection using a registered temporary view.

        Columns are matched by name (``BY NAME``), so column order in the
        DataFrame does not need to match the table definition.  Columns
        present in the table but absent from the DataFrame (e.g. ``loaded_at
        DEFAULT CURRENT_TIMESTAMP``) receive their declared defaults.

        Args:
            table: Fully-qualified table name (e.g. "raw.tabular_transactions").
                Schema and table parts are sqlglot-quoted before interpolation.
            df: Polars DataFrame (or any object with a .to_arrow() method).
            on_conflict: How to handle existing rows:
                - ``"insert"`` — plain INSERT; fails on primary-key conflict.
                - ``"replace"`` — DROP and recreate the table from the DataFrame
                  (CREATE OR REPLACE TABLE).
                - ``"upsert"`` — INSERT OR REPLACE; conflicting rows are deleted
                  then re-inserted (idempotent reload pattern).

        Raises:
            ValueError: If on_conflict is not "insert", "replace", or "upsert".
        """
        from sqlglot import exp

        if on_conflict not in ("insert", "replace", "upsert"):
            raise ValueError(
                f"on_conflict must be 'insert', 'replace', or 'upsert', "
                f"got {on_conflict!r}"
            )

        parts = table.split(".", 1)
        if len(parts) == 2:
            safe_ref = (
                f"{exp.to_identifier(parts[0], quoted=True).sql('duckdb')}"
                f".{exp.to_identifier(parts[1], quoted=True).sql('duckdb')}"
            )
        else:
            safe_ref = exp.to_identifier(table, quoted=True).sql("duckdb")

        arrow_table = df.to_arrow()
        self.conn.register("_ingest_tmp", arrow_table)
        try:
            if on_conflict == "replace":
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {safe_ref} AS SELECT * FROM _ingest_tmp"  # noqa: S608 — sqlglot-quoted identifier from trusted caller
                )
            elif on_conflict == "upsert":
                self.conn.execute(
                    f"INSERT OR REPLACE INTO {safe_ref} BY NAME SELECT * FROM _ingest_tmp"  # noqa: S608 — sqlglot-quoted identifier from trusted caller
                )
            else:
                self.conn.execute(
                    f"INSERT INTO {safe_ref} BY NAME SELECT * FROM _ingest_tmp"  # noqa: S608 — sqlglot-quoted identifier from trusted caller
                )
        finally:
            self.conn.unregister("_ingest_tmp")

    @property
    def path(self) -> Path:
        """Path to the database file."""
        return self._db_path

    def execute(
        self, query: str, params: list[Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Execute a parameterized SQL query.

        Args:
            query: SQL query string with ? placeholders.
            params: Parameter values for placeholders.

        Returns:
            DuckDB connection with query results (call .fetchone(), .fetchall(), etc.).
        """
        if params is not None:
            return self.conn.execute(query, params)
        return self.conn.execute(query)

    def executemany(
        self, query: str, params: list[list[Any]]
    ) -> duckdb.DuckDBPyConnection:
        """Execute a parameterized SQL query for each parameter set.

        More efficient than calling execute() in a loop — DuckDB batches
        the parameter binding internally.

        Args:
            query: SQL query string with ? placeholders.
            params: List of parameter lists, one per row.

        Returns:
            DuckDB connection (typically no result set for INSERT/UPDATE).
        """
        return self.conn.executemany(query, params)

    def begin(self) -> None:
        """Begin an explicit transaction."""
        self.conn.begin()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.conn.commit()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self.conn.rollback()

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a parameter-free SQL query.

        Convenience method for queries that don't need parameters.

        Args:
            query: SQL query string.

        Returns:
            DuckDB relation with query results.
        """
        return self.conn.sql(query)

    def close(self) -> None:
        """Close the database connection and release resources."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 S110  # intentional broad catch on close; pass is correct here
                pass
            self._conn = None
        self._closed = True
        logger.debug(f"Database connection closed: {self._db_path}")


def database_key_error_hint() -> str:
    """Return the appropriate hint for a DatabaseKeyError.

    Checks whether the database file exists to distinguish first-run
    (need ``db init``) from locked (need ``db unlock``).

    Returns:
        A hint string with the correct recovery command.
    """
    try:
        db_path = get_settings().database.path
        if db_path.exists():
            return "💡 Run 'moneybin db unlock' to unlock the database first"
        return "💡 Run 'moneybin db init' to create the database first"
    except Exception:  # noqa: BLE001 — fallback if settings can't load
        return "💡 Run 'moneybin db init' to create the database"


# Singleton instance
_database_instance: Database | None = None


def get_database() -> Database:
    """Get the singleton Database instance for the current profile.

    Creates the Database on first call, reuses on subsequent calls.
    The database path comes from get_settings().database.path.

    Returns:
        The Database singleton instance.
    """
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        return _database_instance

    settings = get_settings()
    db = Database(settings.database.path)
    _database_instance = db
    return db


def close_database() -> None:
    """Close and clear the singleton Database instance."""
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        _database_instance.close()
        _database_instance = None
