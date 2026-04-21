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


class DatabaseKeyError(Exception):
    """Raised when the database encryption key cannot be retrieved."""


class Database:
    """Encrypted DuckDB connection manager.

    One long-lived read-write connection per process. The initialization
    sequence:
        a. Retrieve encryption key via SecretStore
        b. Open in-memory DuckDB connection
        c. Load required extensions (httpfs)
        d. Attach encrypted database file
        e. USE <attached_db>
        f. Run init_schemas() (idempotent baseline DDL)
        g. Run pending schema migrations via MigrationRunner
        h. Run sqlmesh migrate if SQLMesh version changed
        i. Record component versions in app.versions

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
    ) -> None:
        """Initialize and open the encrypted database connection.

        Args:
            db_path: Path to the DuckDB database file.
            secret_store: SecretStore instance for key retrieval. If None,
                creates a new one.

        Raises:
            DatabaseKeyError: If the encryption key cannot be retrieved.
        """
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._closed = False

        store = secret_store or SecretStore()

        # Step a: Retrieve encryption key
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

        # Step b: Open in-memory connection
        self._conn = duckdb.connect()

        # Step c: Load required extensions
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Step d: Attach encrypted database file.
        # ATTACH does not support parameterized queries in DuckDB — the path
        # and key must be interpolated as string literals. The path comes from
        # our own config (trusted), and the key is from SecretStore (trusted).
        # Both are escaped by replacing single-quotes to prevent injection.
        safe_path = str(db_path).replace("'", "''")
        safe_key = encryption_key.replace("'", "''")
        self._conn.execute(  # noqa: S608  # identifiers from trusted config, not user input
            f"ATTACH '{safe_path}' AS moneybin (TYPE DUCKDB, ENCRYPTION_KEY '{safe_key}')"
        )

        # Step e: USE attached database
        self._conn.execute("USE moneybin")

        # Set file permissions on new databases (macOS/Linux)
        if is_new and sys.platform != "win32":
            try:
                db_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600
            except OSError:
                logger.warning(f"Could not set file permissions on {db_path}")

        # Validate permissions on existing databases
        if not is_new and sys.platform != "win32":
            self._check_permissions(db_path)

        # Step f: Run init_schemas (idempotent)
        from moneybin.schema import init_schemas

        init_schemas(self._conn)

        # Steps g-i: Auto-upgrade (migrations + version tracking)
        from moneybin.migrations import (
            MigrationError,
            MigrationRunner,
            get_current_versions,
            record_version,
        )

        settings = get_settings()
        if not settings.database.no_auto_upgrade:
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
                    logger.error(
                        f"❌ Migration {result.failed_migration} failed. "
                        f"Database rolled back."
                    )
                    logger.info("💡 See logs for details")
                    logger.error(
                        "🐛 Report issues at https://github.com/bsaffel/moneybin/issues"
                    )
                    raise MigrationError(
                        f"Migration failed: {result.failed_migration}. "
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
                    self._run_sqlmesh_migrate()
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

    def _run_sqlmesh_migrate(self) -> None:
        """Run sqlmesh migrate to update SQLMesh internal state.

        Called when the installed SQLMesh version differs from the recorded
        version. Uses subprocess to invoke the sqlmesh CLI.
        """
        import subprocess  # noqa: S404  # subprocess is required to invoke the sqlmesh CLI

        # Assumes editable install — __file__ resolves to the project tree.
        # Non-editable installs silently skip (CalledProcessError caught below).
        sqlmesh_root = Path(__file__).resolve().parents[2] / "sqlmesh"
        try:
            subprocess.run(  # noqa: S603  # fixed args from trusted internal config, not user input
                ["uv", "run", "sqlmesh", "-p", str(sqlmesh_root), "migrate"],  # noqa: S607  # uv is a trusted internal tool
                check=True,
                capture_output=True,
                text=True,
            )
            logger.debug("sqlmesh migrate completed successfully")
        except subprocess.CalledProcessError as exc:
            logger.warning(
                f"⚠️  sqlmesh migrate failed (exit {exc.returncode}): {exc.stderr.strip()}"
            )
        except FileNotFoundError:
            logger.debug("sqlmesh CLI not found, skipping migrate")

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
