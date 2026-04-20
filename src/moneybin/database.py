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

import logging
import os
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
        g-i. Migration steps (not yet implemented — see database-migration.md)

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
                logger.warning("Could not set file permissions on %s", db_path)

        # Validate permissions on existing databases
        if not is_new and sys.platform != "win32":
            self._check_permissions(db_path)

        # Step f: Run init_schemas (idempotent)
        from moneybin.schema import init_schemas

        init_schemas(self._conn)

        # Steps g-i: Migration (stub — see database-migration.md spec)
        if not os.environ.get("MONEYBIN_NO_AUTO_UPGRADE"):
            logger.debug(
                "Migration steps skipped — MigrationRunner not yet implemented"
            )

        logger.info("Database connection established: %s", db_path)

    def _check_permissions(self, db_path: Path) -> None:
        """Warn if database file has overly permissive permissions.

        Args:
            db_path: Path to the database file.
        """
        try:
            mode = db_path.stat().st_mode & 0o777
            if mode & 0o077:  # group or world readable/writable
                logger.warning(
                    "⚠️  Database file %s has permissive permissions (%04o). "
                    "Run: chmod 600 %s",
                    db_path,
                    mode,
                    db_path,
                )
        except OSError:
            pass

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection.

        Use this for bulk operations that require the raw connection
        (e.g., ingest_dataframe). For normal queries, prefer execute().

        Raises:
            RuntimeError: If the database has been closed.
        """
        if self._closed or self._conn is None:
            raise RuntimeError(
                "Database connection is closed. "
                "Call get_database() to get a new instance."
            )
        return self._conn

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
        logger.debug("Database connection closed: %s", self._db_path)


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
