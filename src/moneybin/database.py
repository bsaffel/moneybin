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
import os
import stat
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

import duckdb

# SQLMesh resolves MAX_FORK_WORKERS at import via os.sched_getaffinity, which
# does not exist on macOS — falling through to ProcessPoolExecutor's default
# (os.cpu_count(), e.g. 12). Each forked worker inherits our encrypted DuckDB
# FD and the `BaseDuckDBConnectionConfig._data_file_to_adapter` injection,
# competing with the parent for the file's single-writer lock and leaking as
# orphans (re-parented to PID 1, still holding FDs) when a sync is interrupted
# mid-load. Forcing MAX_FORK_WORKERS=1 selects sqlmesh's SynchronousPoolExecutor
# (no fork). Sequential load of the project's ~14 models is faster than fork
# overhead anyway. Must be set before sqlmesh is first imported.
os.environ.setdefault("MAX_FORK_WORKERS", "1")

from moneybin.config import get_settings
from moneybin.secrets import (
    SecretNotFoundError,
    SecretStorageUnavailableError,
    SecretStore,
)

logger = logging.getLogger(__name__)

_KEY_NAME = "DATABASE__ENCRYPTION_KEY"
SALT_NAME = "DATABASE__PASSPHRASE_SALT"
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

    safe_path = escape_sql_literal(str(db_path))
    safe_key = escape_sql_literal(encryption_key)
    safe_alias = exp.to_identifier(alias, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
    return (
        f"ATTACH '{safe_path}' AS {safe_alias} "  # noqa: S608 — trusted internal values, single-quote escaped, alias sqlglot-quoted
        f"(TYPE DUCKDB, ENCRYPTION_KEY '{safe_key}')"
    )


def escape_sql_literal(value: str) -> str:
    """Escape single quotes for safe interpolation into a SQL string literal.

    DuckDB statements like ``ATTACH 'path'`` and ``COMMENT ON … IS 'text'``
    require an inline string literal — they cannot be parameterized with ``?``.
    Use this helper for those cases; prefer parameterized queries everywhere
    else.
    """
    return value.replace("'", "''")


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

        # Ensure parent directory exists — parents=False so we don't
        # recreate a deleted profile's directory tree. The profile root
        # must already exist (created by ProfileService.create).
        db_path.parent.mkdir(parents=False, exist_ok=True)

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

        # Assemble app.categories and app.merchants views on every open.
        # _ensure_seed_tables_exist creates empty seeds.* tables if SQLMesh
        # hasn't run yet (tests, fresh installs) — the CREATE TABLE IF NOT
        # EXISTS calls are no-ops in production where SQLMesh has already
        # populated them.
        from moneybin.seeds import refresh_views

        refresh_views(self)

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

            # Gate on pending migrations, not pkg version. The version string in
            # pyproject.toml is bumped by hand and was previously the only
            # trigger — so a DB opened pre-V003 stayed pre-V003 forever if the
            # version hadn't moved between releases. Any unapplied migration
            # (or a version mismatch) drives the runner.
            runner = MigrationRunner(self)
            pending = runner.pending()
            if pending or stored_pkg_version != current_pkg_version:
                if stored_pkg_version is None:
                    # First-ever open of this DB — schema initialization.
                    logger.info("⚙️  Initializing MoneyBin schema...")
                elif stored_pkg_version != current_pkg_version:
                    logger.info(
                        f"⚙️  MoneyBin upgraded ({stored_pkg_version} → {current_pkg_version}). "
                        f"Applying updates..."
                    )
                else:
                    logger.info(
                        f"⚙️  {len(pending)} pending migration(s) detected. Applying..."
                    )

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

        logger.debug(f"Database connection established: {db_path}")

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
        sqlmesh_root = _SQLMESH_ROOT
        if not sqlmesh_root.is_dir():
            logger.debug("sqlmesh project dir not found, skipping migrate")
            return True

        try:
            from sqlmesh.core.config import Config, GatewayConfig
            from sqlmesh.core.config.connection import (
                BaseDuckDBConnectionConfig,
                DuckDBConnectionConfig,
            )
            from sqlmesh.core.console import NoopConsole, set_console
            from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter

            from sqlmesh import Context  # type: ignore[import-untyped]
        except ImportError:
            logger.debug("sqlmesh not installed, skipping migrate")
            return True

        # See sqlmesh_context() — silence SQLMesh's rich console; logs still
        # flow to the sqlmesh log file.
        set_console(NoopConsole())

        # Adapter construction and cache injection are inside the try block
        # so that failures degrade gracefully instead of breaking DB init.
        # Note: _data_file_to_adapter is a class-level dict — not thread-safe
        # for concurrent init with the same db_path. Acceptable for single-user.
        adapter_key = str(self._db_path)
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
            BaseDuckDBConnectionConfig._data_file_to_adapter[adapter_key] = adapter  # type: ignore[reportPrivateUsage]  # no public API for encrypted DB injection

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
            logger.debug(
                "sqlmesh migrate failed",
                exc_info=True,
            )
            logger.warning("⚠️  sqlmesh migrate failed — see logs for details")
            return False
        finally:
            BaseDuckDBConnectionConfig._data_file_to_adapter.pop(adapter_key, None)  # type: ignore[reportPrivateUsage]  # cleanup matches injection above

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
        on_conflict: Literal["insert", "replace", "upsert"] = "insert",
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

    def __enter__(self) -> "Database":  # noqa: D105
        return self

    def __exit__(self, *_: object) -> None:  # noqa: D105
        self.close()

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

    def interrupt_and_reset(self) -> None:
        """Interrupt any active statement and force-close the connection.

        Called from the MCP timeout path so a stuck tool releases its
        DuckDB write lock before the dispatcher returns. Best-effort:
        DuckDB's interrupt() is a no-op for some statement types (e.g.,
        mid-COPY), so we always follow with close() to guarantee the
        lock drops.
        """
        if self._conn is not None:
            try:
                self._conn.interrupt()
            except Exception:  # noqa: BLE001, S110 — interrupt is best-effort; pass is correct here
                pass
        self.close()


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


def get_database_if_initialized() -> Database | None:
    """Return the singleton Database if it exists, otherwise None.

    Unlike ``get_database()``, this never creates a new connection.
    Used by the atexit handler to flush metrics only when a database
    was actually used during the session.
    """
    return _database_instance


def close_database() -> None:
    """Close and clear the singleton Database instance."""
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        _database_instance.close()
        _database_instance = None


def interrupt_and_reset_database() -> None:
    """Interrupt and clear the singleton Database, if one exists.

    The next ``get_database()`` call will reopen a fresh connection. No-op
    if no Database has been initialized yet (e.g., timeout before any
    tool actually touched the DB).

    Known limitation — thread-survivor race: when this is called from the
    MCP timeout path, ``asyncio.timeout()`` cancels the awaited task but
    the underlying ``asyncio.to_thread`` worker keeps running until the
    sync tool body returns. If that surviving thread later touches the DB,
    it will see the closed connection and raise — that exception surfaces
    via ``ThreadPoolExecutor``'s unhandled-exception path (stderr), not
    the MCP envelope. Occasional stderr noise after a timeout is expected.
    Forcing termination would require cooperative cancellation in tool
    bodies; out of scope for this iteration.
    """
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        _database_instance.interrupt_and_reset()
        _database_instance = None


@contextmanager
def _temporary_singleton(db: Database) -> Generator[None, None, None]:
    """Register ``db`` as the singleton for the duration of the block.

    Used by ``init_db`` to let ``sqlmesh_context()`` find the locally-opened
    Database. Restores the prior state on exit.
    """
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional
    prior = _database_instance
    _database_instance = db
    try:
        yield
    finally:
        _database_instance = prior


# ---------------------------------------------------------------------------
# SQLMesh encrypted-context helper
# ---------------------------------------------------------------------------

_SQLMESH_ROOT = Path(__file__).resolve().parents[2] / "sqlmesh"


@contextmanager
def sqlmesh_context(
    sqlmesh_root: Path | None = None,
) -> Generator[Any, None, None]:
    """Create a SQLMesh Context that can open the encrypted database.

    SQLMesh's DuckDBConnectionConfig does not support encryption_key,
    so we create a properly-connected DuckDB adapter and pre-populate
    SQLMesh's internal adapter cache. SQLMesh then reuses our encrypted
    connection instead of opening its own unencrypted one.

    Requires the Database singleton to be initialized via
    ``get_database()`` before calling.  Callers should NOT close the
    database before invoking this — ``sqlmesh_context()`` borrows the
    singleton's connection.

    Usage::

        db = get_database()          # ensure singleton is open
        with sqlmesh_context() as ctx:
            ctx.plan(auto_apply=True, no_prompts=True)

    Args:
        sqlmesh_root: Path to the sqlmesh/ directory. Defaults to the
            project's ``sqlmesh/`` directory.

    Yields:
        A ``sqlmesh.Context`` connected to the encrypted database.

    Raises:
        DatabaseKeyError: If the Database singleton is not initialized.
    """
    from sqlmesh.core.config import Config, GatewayConfig
    from sqlmesh.core.config.connection import (
        BaseDuckDBConnectionConfig,
        DuckDBConnectionConfig,
    )
    from sqlmesh.core.console import NoopConsole, set_console
    from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter

    from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
        Context,
    )

    # SQLMesh's rich-based TerminalConsole writes plan/progress directly to
    # stdout, bypassing stdlib logging. Swap in NoopConsole so import/transform
    # commands don't drown the user in SQLMesh chatter — diagnostic output
    # still reaches the sqlmesh_*.log file via the stdlib loggers.
    set_console(NoopConsole())

    root = sqlmesh_root or _SQLMESH_ROOT

    # Reuse the singleton's connection — DuckDB only allows one
    # connection per file.  Callers must call get_database() first.
    # httpfs is NOT loaded — no SQLMesh models use remote file access.
    # If a future model needs read_parquet over HTTP or s3://, add
    # conn.execute("INSTALL httpfs; LOAD httpfs;") to Database.__init__.
    if _database_instance is None or _database_instance._conn is None:  # type: ignore[reportPrivateUsage]  # must check singleton's connection state
        raise DatabaseKeyError(
            "Database not initialized — call get_database() before "
            "sqlmesh_context(). If the database was explicitly closed, "
            "re-open it first."
        )
    conn = _database_instance._conn  # type: ignore[reportPrivateUsage]
    # Use the singleton's actual path, not settings — during `profile create`
    # the new profile isn't yet the active one, so get_settings() would fail.
    db_path = _database_instance._db_path  # type: ignore[reportPrivateUsage]

    cache_key = str(db_path)
    try:
        # Each new DuckDB cursor defaults to the `memory` catalog regardless of
        # the parent connection's USE — without this, SQLMesh writes its state
        # tables (_environments, _snapshots, _versions) into memory.sqlmesh.*
        # and they evaporate at process exit.
        def _pin_cursor_to_moneybin(cur: Any) -> None:
            cur.execute(f"USE {_DATABASE_ALIAS}")

        adapter = DuckDBEngineAdapter(
            lambda: conn,
            default_catalog=_DATABASE_ALIAS,
            register_comments=True,
            cursor_init=_pin_cursor_to_moneybin,
        )
        BaseDuckDBConnectionConfig._data_file_to_adapter[cache_key] = adapter  # type: ignore[reportPrivateUsage]  # no public API for encrypted DB injection

        config = Config(
            default_gateway=_DATABASE_ALIAS,
            gateways={
                _DATABASE_ALIAS: GatewayConfig(
                    connection=DuckDBConnectionConfig(database=str(db_path)),
                ),
            },
        )
        ctx = Context(
            paths=str(root),
            config=config,
            gateway=_DATABASE_ALIAS,
        )
        yield ctx
    finally:
        BaseDuckDBConnectionConfig._data_file_to_adapter.pop(cache_key, None)  # type: ignore[reportPrivateUsage]  # cleanup matches injection above


def derive_key_from_passphrase(
    passphrase: str,
    salt: bytes,
    *,
    time_cost: int = 3,
    memory_cost: int = 65536,
    parallelism: int = 4,
    hash_len: int = 32,
) -> str:
    """Derive a hex encryption key from a passphrase using Argon2id.

    Used by both init_db (at creation) and db_unlock (at re-derivation).
    Both callers must pass the same parameters — defaults match
    ``DatabaseConfig`` so callers with access to settings can forward
    them explicitly.

    Args:
        passphrase: User-supplied passphrase string.
        salt: Random 16-byte salt (stored at init, retrieved at unlock).
        time_cost: Argon2id time cost (iterations).
        memory_cost: Argon2id memory cost in KiB.
        parallelism: Argon2id degree of parallelism.
        hash_len: Argon2id output hash length in bytes.

    Returns:
        64-character hex string (256-bit key).
    """
    import argon2.low_level

    raw_key = argon2.low_level.hash_secret_raw(
        secret=passphrase.encode(),
        salt=salt,
        time_cost=time_cost,
        memory_cost=memory_cost,
        parallelism=parallelism,
        hash_len=hash_len,
        type=argon2.low_level.Type.ID,
    )
    return raw_key.hex()


def init_db(
    db_path: Path,
    *,
    passphrase: str | None = None,
    secret_store: SecretStore | None = None,
    profile: str | None = None,
    argon2_time_cost: int = 3,
    argon2_memory_cost: int = 65536,
    argon2_parallelism: int = 4,
    argon2_hash_len: int = 32,
) -> None:
    """Create a new encrypted database with all schemas initialized.

    Two modes:
    - **Auto-key** (default): uses an existing encryption key if available
      (e.g., from env var), otherwise generates a random 256-bit key and
      stores it in the OS keychain.
    - **Passphrase**: derives a key via Argon2id from the supplied
      passphrase, stores the derived key and salt in the keychain.

    Args:
        db_path: Path to the DuckDB database file to create.
        passphrase: If provided, use passphrase-based key derivation
            instead of auto-generated key.
        secret_store: SecretStore instance for key storage. If None,
            creates a new one (uses OS keychain by default).
        profile: Profile name used to scope the keychain service when
            ``secret_store`` is None. Ignored if ``secret_store`` is provided.
        argon2_time_cost: Argon2id time cost (only used with passphrase).
        argon2_memory_cost: Argon2id memory cost in KiB (only used with passphrase).
        argon2_parallelism: Argon2id parallelism (only used with passphrase).
        argon2_hash_len: Argon2id hash length in bytes (only used with passphrase).
    """
    import secrets as secrets_mod

    # init_db is the explicit "create a database" entry point, so it's safe
    # to create parent directories. This supports custom --database paths
    # (e.g., /tmp/new/path/moneybin.duckdb) where ancestors may not exist.
    # The Database constructor itself uses parents=False to avoid silently
    # recreating deleted profile trees during normal operation.
    db_path.parent.mkdir(parents=True, exist_ok=True)

    store = secret_store or SecretStore(profile=profile)

    if passphrase is not None:
        import base64

        salt = secrets_mod.token_bytes(16)
        encryption_key = derive_key_from_passphrase(
            passphrase,
            salt,
            time_cost=argon2_time_cost,
            memory_cost=argon2_memory_cost,
            parallelism=argon2_parallelism,
            hash_len=argon2_hash_len,
        )
        # Save previous keys so we can roll back if DB open fails
        # (e.g., db_path already encrypted with a different key).
        prev_key: str | None = None
        prev_salt: str | None = None
        try:
            prev_key = store.get_key(_KEY_NAME)
        except SecretNotFoundError:
            pass
        try:
            prev_salt = store.get_key(SALT_NAME)
        except SecretNotFoundError:
            pass

        db_existed = db_path.exists()
        try:
            # set_key calls inside the try block so a failure on SALT_NAME
            # after _KEY_NAME succeeded still triggers the rollback below.
            store.set_key(_KEY_NAME, encryption_key)
            store.set_key(SALT_NAME, base64.b64encode(salt).decode())
            with Database(db_path, secret_store=store, no_auto_upgrade=False) as db:
                from moneybin.seeds import materialize_seeds

                with _temporary_singleton(db):
                    materialize_seeds(db)
        except Exception:
            # Roll back keychain to previous state so the existing DB
            # remains accessible with its original key.
            if prev_key is not None:
                store.set_key(_KEY_NAME, prev_key)
            else:
                try:
                    store.delete_key(_KEY_NAME)
                except Exception:  # noqa: BLE001, S110 — best-effort rollback
                    pass  # noqa: S110
            if prev_salt is not None:
                store.set_key(SALT_NAME, prev_salt)
            else:
                try:
                    store.delete_key(SALT_NAME)
                except Exception:  # noqa: BLE001, S110 — best-effort rollback
                    pass  # noqa: S110
            # If we just created an encrypted DB file but Database() then
            # raised (e.g., during schema/migration), remove the orphan so
            # retries aren't locked out by a file with no matching key.
            if not db_existed and db_path.exists():
                try:
                    db_path.unlink()
                except OSError:
                    pass
            raise
        logger.debug("Passphrase-derived key stored in OS keychain")
    else:
        # Auto-key mode: prefer existing keychain entry; if absent, persist
        # an env-provided key (so the DB stays openable after the env var
        # is unset) or generate a fresh one.
        db_existed = db_path.exists()
        key_was_persisted_now = False
        if store.has_keychain_entry(_KEY_NAME):
            logger.debug("Using existing encryption key")
        else:
            key_from_env = False
            try:
                encryption_key = store.get_key(_KEY_NAME)
                key_from_env = True
                logger.debug("Persisting env-provided encryption key to keychain")
            except SecretNotFoundError:
                encryption_key = secrets_mod.token_hex(32)
                logger.debug("Auto-generated encryption key stored in OS keychain")
            try:
                store.set_key(_KEY_NAME, encryption_key)
                key_was_persisted_now = True
            except SecretStorageUnavailableError:
                # Headless environment with no keyring backend. The key must
                # be supplied via env var on every run — refuse to mint a
                # fresh random key (it would be lost on the next process).
                if not key_from_env:
                    raise
                logger.debug(
                    "No keyring backend; relying on env var for encryption key"
                )

        try:
            with Database(db_path, secret_store=store, no_auto_upgrade=False) as db:
                from moneybin.seeds import materialize_seeds

                with _temporary_singleton(db):
                    materialize_seeds(db)
        except Exception:
            # Roll back the freshly persisted key and any orphan DB file.
            # We only undo persistence we just performed — a pre-existing
            # keychain entry belongs to the user and stays put.
            if key_was_persisted_now:
                try:
                    store.delete_key(_KEY_NAME)
                except Exception:  # noqa: BLE001, S110 — best-effort rollback
                    pass  # noqa: S110
                if not db_existed and db_path.exists():
                    try:
                        db_path.unlink()
                    except OSError:
                        pass
            raise
    logger.debug(f"Initialized encrypted database: {db_path}")
