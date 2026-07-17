"""Centralized encrypted database connection management.

The Database class is the sole entry point for all database access in
MoneyBin. It handles encryption key retrieval, encrypted file attachment,
extension loading, schema initialization, and migrations.

Usage::

    from moneybin.database import get_database

    with get_database(read_only=True) as db:
        db.execute(
            "SELECT * FROM core.fct_transactions WHERE account_id = ?", [acct_id]
        )

Never call ``duckdb.connect()`` directly. See the data-protection spec
(``docs/specs/privacy-data-protection.md``) for the full design.
"""

import importlib.metadata
import logging
import os
import stat
import sys
import threading
import time
from collections.abc import Callable, Generator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Literal

import duckdb

from moneybin.db_lock._types import CheckpointReason, OperationType

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

from moneybin.config import DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS, get_settings
from moneybin.secrets import (
    SecretNotFoundError,
    SecretStorageUnavailableError,
    SecretStore,
)

logger = logging.getLogger(__name__)

_KEY_NAME = "DATABASE__ENCRYPTION_KEY"
SALT_NAME = "DATABASE__PASSPHRASE_SALT"
_DATABASE_ALIAS = "moneybin"

_cached_encryption_key: str | None = None

_active_write_conn: "Database | None" = None
_active_write_lock: threading.Lock = threading.Lock()
# Per-call holder populated by the MCP decorator so the timeout handler can
# interrupt the specific connection opened for *this* tool call rather than
# whatever is currently in the process-global slot.
#
# A ContextVar, not a threading.local: an async tool body dispatches its write
# through its own asyncio.to_thread, which runs on a fresh worker thread. A
# thread-local set by the decorator is invisible there, so the connection never
# registers and a timeout cannot interrupt the write — it commits after the
# caller already got a timed_out envelope. asyncio.to_thread copies the calling
# context, so a ContextVar reaches the worker thread (and any thread nested
# below it), which is what makes the async write tools interruptible at all.
_write_conn_holder: ContextVar[list[Any] | None] = ContextVar(
    "_write_conn_holder", default=None
)

_migration_check_done: set[Path] = set()
_database_accessed: bool = False
_database_written: bool = False

# --- Extension seal ---------------------------------------------------------
# DuckDB 1.4.1 disabled encrypted *writes* through its built-in mbedtls crypto
# (RNG vulnerability, GHSA-vmp8-hg63-v2hp). mbedtls is now a read-only crypto
# module: it still decrypts, but any write to an encrypted database fails with
# "DuckDB currently has a read-only crypto module loaded". The only supported
# encrypted-write path is the OpenSSL crypto that ships inside `httpfs` — so
# every MoneyBin *write* connection must load httpfs, and every MoneyBin
# database is encrypted.
#
# httpfs brings the http/s3 filesystems with it, on the very connection an MCP
# agent runs SQL against. The seal keeps the crypto and revokes the filesystems.
#
# `disabled_filesystems` is one-way by DuckDB's own semantics — it refuses to be
# shrunk ("has been disabled previously, it cannot be re-enabled"), locked or
# not. `lock_configuration` closes the *other* door: without it an agent can
# re-enable `autoinstall_known_extensions` and load an extension whose
# filesystem ISN'T in our disable list (azure), then read `az://`. That is
# reachable from `sql_query`, whose validator permits `PRAGMA` — and `PRAGMA
# autoinstall_known_extensions=true` is a config write. So the lock is what
# makes the agent-facing handle a boundary rather than a suggestion.
#
# The lock therefore goes on READ-ONLY connections only — the ones that execute
# agent-supplied SQL. It cannot go on write connections: DuckDB itself issues
# `SET current_transaction_invalidation_policy` when running DDL that carries a
# DEFAULT inside an explicit transaction (exactly what MigrationRunner does), and
# a locked configuration refuses that SET — even to the value it already holds.
# Write connections never execute agent SQL; they run our migrations and SQLMesh
# transforms. They still get the filesystem disable and the extension lockdown,
# just not the lock.
#
# Note `LOAD` itself is NOT gated by lock_configuration — a session can still
# load httpfs. That buys it nothing: `disabled_filesystems` is enforced at
# filesystem-lookup time, so the remote schemes stay refused.
#
# `enable_external_access=false` was considered as a blunter seal and rejected:
# it blocks the local encrypted DB file itself (the ATTACH can't open it), so it
# is unusable here. `disabled_filesystems` is the right tool — it revokes the
# remote filesystems by name while leaving LocalFileSystem intact.
#
# Watch item: if a future DuckDB restores built-in write crypto without httpfs,
# all of this — the LOAD, the disable, the lock, and test_extension_seal.py —
# can be deleted.

# The extension whose OpenSSL crypto module makes encrypted writes possible.
_CRYPTO_EXTENSION = "httpfs"

# Every remote filesystem httpfs registers, kept in lockstep with what the
# extension actually exposes: HTTPFileSystem (http/https), S3FileSystem (which
# also serves gcs://, gs:// and r2://), and HuggingFaceFileSystem (hf://).
# Other remote backends (azure://) live in extensions that autoinstall=false +
# allow_community=false + the lock prevent loading at all. This list is a hand-
# maintained allowlist-of-denies precisely because it must NOT include
# LocalFileSystem — disabling that would block the encrypted DB file itself. Its
# completeness is guarded by test_disabled_filesystems_covers_every_registered_fs
# in test_extension_seal.py, which loads httpfs, enumerates conn.list_filesystems()
# (which returns only extension-registered remote filesystems, never the local
# one), and fails CI the moment DuckDB registers a fourth we haven't disabled —
# so a silently-missed filesystem is a red build, not open egress.
_DISABLED_FILESYSTEMS = "HTTPFileSystem,S3FileSystem,HuggingFaceFileSystem"

# Set before anything else so nothing can be silently fetched or loaded, and
# so an agent's SQL can't pull in an extension by merely referencing it.
_EXTENSION_LOCKDOWN_SQL = (
    "SET autoinstall_known_extensions=false",
    "SET autoload_known_extensions=false",
    "SET allow_community_extensions=false",
)


def _seal_connection(conn: duckdb.DuckDBPyConnection, *, writable: bool) -> None:
    """Lock down a fresh connection's extension and filesystem surface.

    Must run BEFORE the encrypted ATTACH — the crypto module is selected as the
    database is attached, so a later ``LOAD httpfs`` is too late for a write.
    Every statement must also precede ``lock_configuration``, which freezes all
    of them.

    Args:
        conn: Freshly-opened DuckDB connection, not yet attached.
        writable: True for read-write opens, which need OpenSSL crypto and so
            must load httpfs. Read-only opens decrypt fine with the built-in
            mbedtls module and skip the load — though DuckDB still pulls in a
            locally-installed httpfs itself during ATTACH, which is why the
            filesystem disable is applied to both paths rather than only to the
            write one. Read-only opens additionally lock the configuration; see
            the "Extension seal" block above for why write opens cannot.
    """
    for stmt in _EXTENSION_LOCKDOWN_SQL:
        conn.execute(stmt)

    if writable:
        _load_crypto_extension(conn)

    conn.execute(f"SET disabled_filesystems='{_DISABLED_FILESYSTEMS}'")

    if not writable:
        conn.execute("SET lock_configuration=true")


def _load_crypto_extension(conn: duckdb.DuckDBPyConnection) -> None:
    """Install and load httpfs for its OpenSSL crypto module.

    INSTALL is a local no-op once the extension is cached in the DuckDB
    extension directory; it only reaches the network on a machine that has
    never fetched httpfs for this DuckDB version. Explicit INSTALL still works
    with ``autoinstall_known_extensions=false`` — that setting governs only the
    *implicit* installs DuckDB would otherwise trigger from a query.
    """
    try:
        conn.execute(f"INSTALL {_CRYPTO_EXTENSION}")
        conn.execute(f"LOAD {_CRYPTO_EXTENSION}")
    except duckdb.Error as e:
        conn.close()
        raise DatabaseCryptoError(
            f"Cannot open the database for writing — MoneyBin needs DuckDB's "
            f"'{_CRYPTO_EXTENSION}' extension for the OpenSSL crypto that "
            f"encrypted writes require, and it could not be installed.\n\n"
            f"DuckDB downloads it once per version; this machine has no cached "
            f"copy, so the first write needs network access to "
            f"extensions.duckdb.org.\n\n"
            f"Underlying error: {e}"
        ) from e


def _pin_cursor_to_moneybin(cur: Any) -> None:
    """Pin a freshly-opened SQLMesh cursor to the persistent ``moneybin`` catalog.

    Each new DuckDB cursor defaults to the ``memory`` catalog regardless of the
    parent connection's ``USE``. Without this, SQLMesh writes its state tables
    (``_environments`` / ``_snapshots`` / ``_versions``) into ``memory.sqlmesh.*``
    and they evaporate at process exit. Every ``DuckDBEngineAdapter`` MoneyBin
    builds against the encrypted DB MUST pass this as ``cursor_init`` — it is the
    single shared definition so the two SQLMesh entry points cannot drift.
    """
    cur.execute(f"USE {_DATABASE_ALIAS}")


def build_attach_sql(
    db_path: Path,
    encryption_key: str,
    *,
    alias: str = _DATABASE_ALIAS,
    read_only: bool = False,
) -> str:
    """Build a DuckDB ATTACH statement for an encrypted database.

    SQL-building helper, not a connection opener — the ``read_only`` default
    here is intentional and matches DuckDB's own ATTACH default. Connection
    openers (``Database.__init__``, ``get_database``) require ``read_only``.

    Single-quote escapes the path and key to prevent injection. The alias
    is double-quoted via sqlglot as defense in depth. All three parameters
    must come from trusted sources (config, SecretStore, hardcoded literals)
    — never user input.

    Args:
        db_path: Path to the DuckDB database file.
        encryption_key: AES-256-GCM encryption key.
        alias: Database alias in DuckDB (default "moneybin"). Must be a
            simple identifier — callers should only pass hardcoded literals.
        read_only: If True, append READ_ONLY to the ATTACH options so the
            connection cannot write to the database file.

    Returns:
        ATTACH SQL string ready for execution.
    """
    from sqlglot import exp

    safe_path = escape_sql_literal(str(db_path))
    safe_key = escape_sql_literal(encryption_key)
    safe_alias = exp.to_identifier(alias, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
    options = f"TYPE DUCKDB, ENCRYPTION_KEY '{safe_key}'"
    if read_only:
        options += ", READ_ONLY"
    return (
        f"ATTACH '{safe_path}' AS {safe_alias} "  # noqa: S608 — trusted internal values, single-quote escaped, alias sqlglot-quoted
        f"({options})"
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


class DatabaseCryptoError(Exception):
    """DuckDB's OpenSSL crypto module (httpfs) is unavailable.

    Encrypted writes require it since DuckDB 1.4.1. Practically this means a
    machine with no cached httpfs and no network access on its first write.
    """


class DatabaseLockError(Exception):
    """DuckDB file lock held by another process; caller may retry."""


class DatabaseNotInitializedError(Exception):
    """Database file missing or incomplete; run 'moneybin db init'."""


class SchemaDriftError(Exception):
    """Raised when materialized core.* tables lack expected columns.

    Indicates a stale SQLMesh snapshot vs. the current model definition.
    Remediation: ``moneybin transform apply`` to rebuild affected models.
    """


# Captured from the final SELECT of each FULL-materialized core SQLMesh
# model. Views (kind VIEW) cannot drift — they always reflect the current
# model. NOT parsed at runtime; keep in sync via the parity test in
# tests/moneybin/test_db_helpers_parity.py (Task 6C).
EXPECTED_CORE_COLUMNS: dict[str, frozenset[str]] = {
    # src/moneybin/sqlmesh/models/core/dim_accounts.sql — kind FULL
    "core.dim_accounts": frozenset({
        "account_id",
        "routing_number",
        "account_type",
        "institution_name",
        "institution_fid",
        "source_type",
        "source_file",
        "extracted_at",
        "loaded_at",
        "updated_at",
        "display_name",
        "official_name",
        "last_four",
        "account_subtype",
        "holder_category",
        "currency_code",
        "credit_limit",
        "archived",
        "include_in_net_worth",
    }),
    # src/moneybin/sqlmesh/models/core/fct_balances_daily.py — kind FULL
    "core.fct_balances_daily": frozenset({
        "account_id",
        "balance_date",
        "balance",
        "is_observed",
        "observation_source",
        "reconciliation_delta",
    }),
}


def check_core_schema_drift(db: "Database") -> dict[str, list[str]]:
    """Return missing-column map per table, or empty dict for no drift.

    Reads ``duckdb_columns()`` once and compares each expected table's column
    set to the observed set. Cheap (< 5 ms warm). Used at FastMCP boot and
    re-runnable from system_status.

    Tables that don't exist yet (e.g. on a pre-first-transform database) are
    not drift — the boot check must not block a freshly initialized profile
    from starting the MCP server. Only tables that exist but lack expected
    columns are reported.
    """
    rows = db.execute(
        "SELECT table_name, column_name "
        "FROM duckdb_columns() "
        "WHERE schema_name = 'core'"
    ).fetchall()
    observed: dict[str, set[str]] = {}
    for table, column in rows:
        observed.setdefault(f"core.{table}", set()).add(column)

    drift: dict[str, list[str]] = {}
    for qualified_name, expected in EXPECTED_CORE_COLUMNS.items():
        if qualified_name not in observed:
            # Table not materialized yet — not drift.
            continue
        missing = sorted(expected - observed[qualified_name])
        if missing:
            drift[qualified_name] = missing
    return drift


def _attach_encrypted(conn: "duckdb.DuckDBPyConnection", sql: str) -> None:
    """Execute an ATTACH statement, mapping lock/config errors to DatabaseLockError.

    Closes ``conn`` and raises ``DatabaseLockError`` if DuckDB reports a
    lock-contention condition. Re-raises other DuckDB exceptions unchanged.

    DuckDB 1.5.3 unified previously-distinct messages (``"Conflicting lock"``
    IO error + ``"different configuration"`` catalog error in 1.5.2) into a
    single ``"Could not set lock on file"`` IO error. Both phrasings are
    matched for belt-and-suspenders coverage across environments.
    """
    try:
        conn.execute(sql)
    except duckdb.CatalogException as e:
        conn.close()
        if "different configuration" in str(e):
            raise DatabaseLockError(str(e)) from e
        raise
    except duckdb.IOException as e:
        conn.close()
        msg = str(e)
        if "Could not set lock on file" in msg or "Conflicting lock" in msg:
            raise DatabaseLockError(msg) from e
        raise


class Database:
    """Encrypted DuckDB connection manager.

    Short-lived encrypted connection. Acquire via ``get_database()``, use,
    and release via the context manager — there is no process-level
    singleton (per ADR-010). Write-mode opens run the initialization
    sequence below; read-only opens (``read_only=True``) skip steps e–h.

    Initialization sequence (write mode):
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
        read_only: bool,
        secret_store: SecretStore | None = None,
        no_auto_upgrade: bool | None = None,
        assume_initialized: bool = False,
    ) -> None:
        """Initialize and open the encrypted database connection.

        Args:
            db_path: Path to the DuckDB database file.
            read_only: If True, open the database in read-only mode. The
                database must already exist. Skips schema init, migrations,
                and view refresh.
            secret_store: SecretStore instance for key retrieval. If None,
                creates a new one.
            no_auto_upgrade: If True, skip versioned migrations and SQLMesh
                migrate on startup. If None, reads from config.
            assume_initialized: If True, skip ``init_schemas``, the migration
                block, and ``refresh_views`` entirely. The caller asserts the
                file is already schema-current — i.e. a copy of a
                fully-initialized template DB. Default ``False``; production
                opens always leave this ``False`` and run the full sequence.
                Intended only for the test template-copy fixture.

        Raises:
            DatabaseKeyError: If the encryption key cannot be retrieved.
            DatabaseNotInitializedError: If read_only=True and db_path does
                not exist.
            DatabaseLockError: If DuckDB reports a conflicting lock or
                configuration mismatch on ATTACH.
            ValueError: If assume_initialized=True combined with read_only=True,
                or if assume_initialized=True but db_path does not exist.
        """
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._closed = False
        self._read_only = read_only
        # Populated by get_database() for write-mode opens — releases the
        # process file lock acquired in front of the connection. None on
        # read-mode opens and on direct Database() construction in tests.
        self._lock_release: Callable[[], None] | None = None

        if read_only and not db_path.exists():
            raise DatabaseNotInitializedError(
                f"Database not found at {db_path}.\n"
                f"Run 'moneybin db init' to initialize it first."
            )

        if assume_initialized and read_only:
            raise ValueError(
                "assume_initialized is incompatible with read_only "
                "(read-only opens never initialize the schema)."
            )
        if assume_initialized and not db_path.exists():
            raise ValueError(
                "assume_initialized requires an existing, already-initialized "
                "database file (e.g. a copy of a built template)."
            )

        store = secret_store or SecretStore()

        global _cached_encryption_key  # noqa: PLW0603
        # An explicitly-passed secret_store is authoritative: read its key and
        # neither consult nor populate the process cache. The cache only spares
        # the default (secret_store=None) path repeat keyring lookups; letting it
        # override an explicit store would let a key cached in one context
        # decrypt-fail a DB opened with a different explicit key in another.
        if secret_store is None and _cached_encryption_key is not None:
            encryption_key = _cached_encryption_key
        else:
            try:
                encryption_key = store.get_key(_KEY_NAME)
            except SecretNotFoundError as e:
                raise DatabaseKeyError(
                    f"Cannot open database — encryption key not found. "
                    f"Run 'moneybin db init' to create a new database, or set "
                    f"MONEYBIN_{_KEY_NAME} for CI/headless environments."
                ) from e
            if secret_store is None:
                _cached_encryption_key = encryption_key

        if read_only:
            self._conn = duckdb.connect()
            # Construct-or-rollback: if ATTACH or USE raises, close the conn we
            # just opened — __init__ never returns a Database, so nobody else
            # can. pop_all() disarms the cleanup once construction succeeds.
            with ExitStack() as stack:
                stack.callback(self._conn.close)
                _seal_connection(self._conn, writable=False)
                _attach_encrypted(
                    self._conn,
                    build_attach_sql(db_path, encryption_key, read_only=True),
                )
                self._conn.execute(f"USE {_DATABASE_ALIAS}")
                stack.pop_all()
            return

        # Ensure parent directory exists — parents=False so we don't
        # recreate a deleted profile's directory tree. The profile root
        # must already exist (created by ProfileService.create).
        #
        # Not redundant with get_database()'s identical mkdir: that one runs
        # ahead of write_lock for the get_database() path, but direct
        # Database() construction (tests, embedded callers) reaches here
        # without it. Keep both — exist_ok=True makes the second a no-op.
        db_path.parent.mkdir(parents=False, exist_ok=True)

        is_new = not db_path.exists()

        self._conn = duckdb.connect()
        # Construct-or-rollback: arm a cleanup that closes the conn we just
        # opened if any step below raises. __init__ returns no Database on
        # failure, so nobody else could close it. pop_all() disarms the
        # cleanup once construction succeeds and the conn becomes owned by
        # this instance (released later via close()). Same ExitStack idiom
        # get_database() uses to manage the write-lock lifetime.
        with ExitStack() as stack:
            stack.callback(self._conn.close)
            _seal_connection(self._conn, writable=True)
            _attach_encrypted(self._conn, build_attach_sql(db_path, encryption_key))
            self._conn.execute(f"USE {_DATABASE_ALIAS}")

            if is_new and sys.platform != "win32":
                try:
                    db_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600
                except OSError:
                    logger.warning(f"Could not set file permissions on {db_path}")

            if not is_new and sys.platform != "win32":
                self._check_permissions(db_path)

            if assume_initialized:
                # Caller asserts the attached file is already schema-current — a
                # copy of a fully-initialized template DB. Skip the idempotent but
                # expensive schema build (init_schemas + migration check +
                # refresh_views) so per-test fixtures don't re-pay it ~1,600×.
                # Test-only contract: production opens leave this False and always
                # run the full sequence below. The construct-or-rollback ExitStack
                # still guards the attach/USE above.
                stack.pop_all()
                logger.debug(
                    f"Database connection established (assume_initialized): {db_path}"
                )
                return

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

                # Gate on pending migrations, not pkg version. The version
                # string in pyproject.toml is bumped by hand and was previously
                # the only trigger — so a DB opened pre-V003 stayed pre-V003
                # forever if the version hadn't moved between releases. Any
                # unapplied migration (or a version mismatch) drives the runner.
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

                    # Record the MoneyBin version BEFORE the post-migration
                    # checkpoint so the single durable boundary flushes the schema
                    # change AND the version record together. (Previously the
                    # checkpoint ran first; a crash between it and record_version
                    # left migrations applied but unversioned, so the next open
                    # re-ran the runner and emitted a redundant checkpoint before
                    # recording the version.) This runs whenever the upgrade path
                    # runs (pending migrations or a version change), independent of
                    # applied_count — so a version-only bump still records the new
                    # version even when no migration was applied.
                    record_version(self, "moneybin", current_pkg_version)

                    if result.applied_count > 0:
                        # Checkpoint last, at the durable boundary, so it covers
                        # the version record too. Only on actually-applied
                        # migrations — a no-op open must not increment the
                        # counter. A checkpoint failure is a durability hint, not
                        # a correctness signal (same contract as
                        # TransformService.apply): the migrations already
                        # committed, so log and continue rather than letting the
                        # ExitStack tear down the connection and surface a
                        # spurious migration-failed error to the caller.
                        try:
                            self.checkpoint("post_migration")
                        except Exception as e:  # noqa: BLE001 — checkpoint is best-effort durability, not correctness
                            logger.warning(
                                f"post_migration checkpoint failed "
                                f"(migrations applied): {type(e).__name__}"
                            )

                # Check SQLMesh version independently — a SQLMesh upgrade
                # without a MoneyBin upgrade still needs `sqlmesh migrate`.
                try:
                    sqlmesh_version = importlib.metadata.version("sqlmesh")
                    stored_sqlmesh = stored_versions.get("sqlmesh")
                    if stored_sqlmesh != sqlmesh_version:
                        if self.migrate_sqlmesh_state():
                            record_version(self, "sqlmesh", sqlmesh_version)
                            if stored_sqlmesh is not None:
                                logger.info("  ✅ SQLMesh state updated")
                except importlib.metadata.PackageNotFoundError:
                    pass  # SQLMesh not installed — skip

            # Build core.dim_* views AFTER migrations. Order matters on the
            # upgrade path: V006 must drop the legacy `app.merchants` TABLE
            # before refresh_views can create the replacement view structure.
            # _ensure_seed_tables_exist creates empty seeds.* tables if SQLMesh
            # hasn't populated them yet (tests, fresh installs) so the dim
            # views can resolve.
            from moneybin.seeds import refresh_views

            refresh_views(self)

            # Construction succeeded — transfer conn ownership to this instance.
            stack.pop_all()

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

    def migrate_sqlmesh_state(self) -> bool:
        """Run sqlmesh migrate to update SQLMesh internal state.

        Called when the installed SQLMesh version differs from the recorded
        version. Uses the SQLMesh Python API in-process so it inherits the
        current profile's encrypted connection — no subprocess needed.

        Returns:
            True if migration succeeded (durable state verified current) or
                was skipped (no sqlmesh dir or sqlmesh not installed),
            False if migration failed or the durable state did not advance.
        """
        sqlmesh_root = SQLMESH_ROOT
        if not sqlmesh_root.is_dir():
            logger.debug("sqlmesh project dir not found, skipping migrate")
            return True

        try:
            from sqlmesh import Context  # type: ignore[import-untyped]
            from sqlmesh.core.config import Config, GatewayConfig
            from sqlmesh.core.config.connection import (
                BaseDuckDBConnectionConfig,
                DuckDBConnectionConfig,
            )
            from sqlmesh.core.console import NoopConsole, set_console
            from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
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
                    "migrate_sqlmesh_state called before connection is established"
                )
            # cursor_init pins state writes to the persistent moneybin.sqlmesh.*
            # catalog. Without it, ctx.migrate() below writes _versions/_snapshots
            # to the throwaway memory.sqlmesh.* catalog, which evaporates at
            # process exit — the migration silently no-ops while reporting success.
            adapter = DuckDBEngineAdapter(
                lambda: conn,
                default_catalog=_DATABASE_ALIAS,
                register_comments=True,
                cursor_init=_pin_cursor_to_moneybin,
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
            # Verify the DURABLE state actually advanced. get_versions(validate=True)
            # raises if the state SQLMesh will read on the next plan is still
            # behind the installed package — so a migrate that silently
            # under-persisted is reported as failure (caller does NOT record it as
            # done and retries on the next open), not cached as a false success.
            ctx.state_sync.get_versions(validate=True)
            logger.debug("sqlmesh migrate completed and durable state verified")
            return True
        except Exception:  # noqa: BLE001 — sqlmesh migration failures are non-fatal
            logger.debug(
                "sqlmesh migrate failed or durable state did not advance",
                exc_info=True,
            )
            logger.warning("⚠️  sqlmesh migrate failed — see logs for details")
            return False
        finally:
            BaseDuckDBConnectionConfig._data_file_to_adapter.pop(adapter_key, None)  # type: ignore[reportPrivateUsage]  # cleanup matches injection above

    def repair_sqlmesh_state(self) -> bool:
        """Advance SQLMesh's durable state to the installed package and confirm it.

        The explicit recovery path behind ``moneybin db migrate apply``. Runs the
        in-process migrate, then re-reads the durable state to confirm it actually
        advanced: ``migrate_sqlmesh_state()`` returns True even on its no-op skip
        paths (no sqlmesh project dir), so a bare True is not proof of repair.
        Records the version proxy — so the next auto-open skips the check — only
        when the durable state is verifiably current afterward.

        Returns:
            True if the durable state is current afterward; False if it could not
                be advanced (skip path, or state ahead of the installed package).
        """
        from moneybin.migrations import record_version, sqlmesh_state_assessment

        self.migrate_sqlmesh_state()
        drift, _needs_migration = sqlmesh_state_assessment(self)
        if drift is not None:
            # Still drifting after the migrate (skip path, or state ahead of the
            # package) — not repaired, so don't record a false success.
            return False
        record_version(self, "sqlmesh", importlib.metadata.version("sqlmesh"))
        return True

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
        on_conflict: Literal["insert", "replace", "upsert", "ignore"] = "insert",
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
                - ``"ignore"`` — INSERT OR IGNORE; conflicting rows are silently
                  skipped (preserves the original row and its import_id).

        Raises:
            ValueError: If on_conflict is not a recognised value.
        """
        from sqlglot import exp

        if on_conflict not in ("insert", "replace", "upsert", "ignore"):
            raise ValueError(
                f"on_conflict must be 'insert', 'replace', 'upsert', or 'ignore', "
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
            elif on_conflict == "ignore":
                self.conn.execute(
                    f"INSERT OR IGNORE INTO {safe_ref} BY NAME SELECT * FROM _ingest_tmp"  # noqa: S608 — sqlglot-quoted identifier from trusted caller
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

    def checkpoint(self, reason: "CheckpointReason") -> None:
        """Execute CHECKPOINT at a durable boundary and record observability.

        Per ``docs/specs/database-writer-coordination.md`` § "PR B hardening
        pass", CHECKPOINT calls are emitted intentionally at named boundaries
        (post-migration, post-transform-apply, etc.) — not on every app-state
        mutation. The ``reason`` argument identifies the boundary for the
        ``moneybin_db_checkpoint_total{reason=...}`` counter and the
        structured log line.
        """
        from moneybin.metrics.registry import DB_CHECKPOINT_TOTAL

        self.conn.execute("CHECKPOINT")
        DB_CHECKPOINT_TOTAL.labels(reason=reason).inc()
        logger.debug(f"checkpoint: reason={reason}")

    def __enter__(self) -> "Database":  # noqa: D105
        return self

    def __exit__(self, *_: object) -> None:  # noqa: D105
        self.close()

    def close(self) -> None:
        """Close the database connection and release resources.

        Closes the DuckDB connection FIRST, then releases the process file
        lock (acquired by ``get_database()`` on write-mode opens). Order is
        load-bearing: our DuckDB connection holds DuckDB's own OS-level lock
        on the database file until it closes. If we released the file lock
        first, a peer could acquire the file lock and attempt its ATTACH
        while our connection still owns that OS lock — surfacing a raw
        IOException. Releasing in dependency order (conn, then file lock)
        closes that window. The release runs in a ``finally`` so a DuckDB
        close failure still drops the file lock. The release callable is
        nulled before invocation so a re-entrant close raised from an
        exception handler cannot double-release.
        """
        global _active_write_conn  # noqa: PLW0603

        release = self._lock_release
        self._lock_release = None
        try:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:  # noqa: BLE001 S110  # intentional broad catch on close; pass is correct here
                    pass
                self._conn = None
            # Clear the global write-conn slot AFTER closing the connection.
            # Clearing it first leaves a window where the connection is still
            # open yet interrupt_and_reset_database()'s global-slot fallback
            # finds None and no-ops, missing a live connection. Clearing after
            # close means that fallback either interrupts the still-open conn
            # (before this point) or finds _conn already None and safely no-ops.
            with _active_write_lock:
                if _active_write_conn is self:
                    _active_write_conn = None
            self._closed = True
        finally:
            if release is not None:
                release()
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
            # Explicit DETACH so DuckDB's process-level file registry releases
            # the path entry before close(). USE memory first: DuckDB prohibits
            # detaching the active catalog, and connection setup calls USE moneybin.
            # These execute() calls survive interrupt() when no DuckDB statement
            # was running (the common case for Python-level sleeps); even if they
            # fail, the subsequent close() releases the handle.
            try:
                self._conn.execute("USE memory")  # noqa: S608 — hardcoded literal, not user input
            except Exception:  # noqa: BLE001, S110 — best-effort; pass is correct here
                pass
            try:
                self._conn.execute(f'DETACH "{_DATABASE_ALIAS}"')  # noqa: S608 — alias is a hardcoded internal literal
            except Exception:  # noqa: BLE001, S110 — DETACH is best-effort; pass is correct here
                pass
        self.close()


def invalidate_encryption_key_cache() -> None:
    """Clear the in-process encryption key cache.

    Called by key rotation so subsequent Database() calls fetch the new key
    from the keychain instead of reusing the pre-rotation cached value.
    """
    global _cached_encryption_key  # noqa: PLW0603
    _cached_encryption_key = None


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


def _lock_error_message(db_path: "Path", max_wait: float) -> str:
    from moneybin.utils.db_processes import describe_process, find_blocking_processes

    blockers = find_blocking_processes(db_path)
    if blockers:
        names = ", ".join(describe_process(str(p["cmdline"])) for p in blockers)
        return (
            f"Could not acquire write lock after {max_wait:.0f}s "
            f"(held by: {names}). "
            f"Run 'moneybin db ps' for details."
        )
    return (
        f"Could not acquire write lock after {max_wait:.0f}s. "
        f"Another process may be writing to the database. "
        f"Run 'moneybin db ps' for details."
    )


def database_was_accessed() -> bool:
    """Return True if any Database connection was opened in this process."""
    return _database_accessed


def database_was_written() -> bool:
    """Return True if any write (non-read-only) Database connection was opened."""
    return _database_written


def get_database(
    *,
    read_only: bool,
    max_wait: float = DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS,
    operation_type: OperationType = "interactive",
) -> "Database":
    """Create and return a new short-lived Database connection.

    Each call opens a fresh connection; callers must close it when done
    (``with get_database(read_only=...) as db: ...`` closes automatically).

    Write-mode opens acquire a process file lock (``write_lock``) that is
    held for the **lifetime of the returned Database**, not just during
    ATTACH. ``Database.close()`` releases the file lock alongside the
    DuckDB connection. Read-mode opens never touch the file lock — DuckDB's
    own arbitration handles read-write contention at the ATTACH layer.

    A single shared ``deadline = monotonic() + max_wait`` drives both
    file-lock acquisition AND the existing ATTACH retry, so end-to-end
    writer wait stays under ``max_wait`` (default 10 s, per the
    writer-coordination policy ceiling).

    Both read-only and write connections retry on DatabaseLockError with
    exponential backoff (start 50 ms, ×1.5, cap 500 ms) until the deadline
    is reached — DuckDB's exclusive/shared-lock matrix means a read-only
    open can also fail (and retry) when another process holds a write
    lock, and a write open can fail (and retry) when another process
    holds a read-only attach.
    """
    global _database_accessed, _database_written, _active_write_conn  # noqa: PLW0603

    # Lazy import: db_lock.lock imports DatabaseLockError from this module,
    # so deferring the import past module-load time breaks the cycle.
    from moneybin.db_lock import write_lock

    settings = get_settings()
    db_path = settings.database.path
    deadline = time.monotonic() + max_wait
    skip_upgrade = (
        read_only
        or settings.database.no_auto_upgrade
        or (db_path in _migration_check_done)
    )

    if read_only:
        return _open_with_attach_retry(
            db_path=db_path,
            read_only=True,
            skip_upgrade=skip_upgrade,
            deadline=deadline,
            max_wait=max_wait,
        )

    # write_lock places its lock file at <db_path>.write.lock inside the
    # profile directory, so that directory must exist before it runs. Pre-PR-B
    # Database.__init__ created it (mkdir parents=False) as the first
    # filesystem touch; write_lock now runs first, so the creation moves ahead
    # of it — otherwise os.open raises FileNotFoundError on a write open that
    # is the first thing to touch a fresh profile (e.g. `synthetic generate`).
    # parents=False preserves the invariant that the profile root already
    # exists (created by ProfileService.create): we create only the leaf
    # profile directory, never a deleted tree.
    db_path.parent.mkdir(parents=False, exist_ok=True)

    # Write path: enter the write_lock context manager into an ExitStack and
    # stash stack.close on the returned Database. The lock outlives this
    # function — Database.close() invokes stack.close() to exit the context
    # and release the file lock. Holding the lock for the full Database
    # lifetime (not just during ATTACH) is what prevents a second writer
    # from slipping in between get_database() returning and
    # Database.close() running and surfacing a raw IOException at ATTACH.
    db: Database | None = None
    stack = ExitStack()
    try:
        stack.enter_context(
            write_lock(db_path, deadline=deadline, operation_type=operation_type)
        )
        db = _open_with_attach_retry(
            db_path=db_path,
            read_only=False,
            skip_upgrade=skip_upgrade,
            deadline=deadline,
            max_wait=max_wait,
        )
        # get_database() is the sole site that supplies the lock-release
        # callable; the field is "private" only in that no external caller
        # should set it.
        db._lock_release = stack.close  # pyright: ignore[reportPrivateUsage]
        _database_written = True
        _migration_check_done.add(db_path)
        with _active_write_lock:
            _active_write_conn = db
        # If the MCP decorator registered a per-call holder for this call,
        # store the connection there too. The timeout handler reads from the
        # holder to interrupt the *specific* connection it dispatched rather
        # than whatever is currently in the global slot (which may belong to a
        # different concurrent tool call).
        _holder = _write_conn_holder.get()
        if _holder is not None:
            _holder[0] = db
        return db
    except BaseException:
        # Failure after the file lock was acquired (ATTACH retry exhausted,
        # Database init raised, bookkeeping interrupted). If a Database was
        # bound, close it so its DuckDB connection is torn down too —
        # closing only the lock stack would leak the open connection. The
        # trailing stack.close() is idempotent: it covers the narrow window
        # where db is bound but _lock_release was not yet wired, and is a
        # no-op once db.close() has already exited the context.
        if db is not None:
            db.close()
        stack.close()
        raise


def _open_with_attach_retry(
    *,
    db_path: Path,
    read_only: bool,
    skip_upgrade: bool,
    deadline: float,
    max_wait: float,
) -> "Database":
    """Open a ``Database`` with the existing ATTACH-retry loop.

    Factored out of ``get_database()`` so the write path can run the loop
    inside the ``write_lock`` context. Bookkeeping that depends on the
    open being a write (``_database_written``, ``_migration_check_done``,
    ``_active_write_conn``, MCP per-call holder) stays in
    ``get_database()`` so it runs only after a successful write open.
    """
    global _database_accessed  # noqa: PLW0603
    delay = 0.05
    while True:
        try:
            db = Database(
                db_path,
                read_only=read_only,
                no_auto_upgrade=skip_upgrade,
            )
            _database_accessed = True
            return db
        except DatabaseLockError:
            if time.monotonic() >= deadline:
                raise DatabaseLockError(
                    _lock_error_message(db_path, max_wait)
                ) from None
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)


def interrupt_and_reset_database(conn: "Database | None" = None) -> None:
    """Interrupt and close the active write connection, if any.

    If *conn* is provided (captured by the MCP decorator's per-call holder),
    interrupt that specific connection. Otherwise fall back to the
    process-global slot. No-op if no write connection is active.
    """
    if conn is not None:
        conn.interrupt_and_reset()
        return
    with _active_write_lock:
        slot_conn = _active_write_conn
    if slot_conn is not None:
        slot_conn.interrupt_and_reset()


# ---------------------------------------------------------------------------
# SQLMesh encrypted-context helper
# ---------------------------------------------------------------------------

# Inside the package, not above it: an installed wheel has no repo root to walk
# up to, so a package-relative path is the only rule that resolves in both a
# source checkout and site-packages.
SQLMESH_ROOT = Path(__file__).resolve().parent / "sqlmesh"


@contextmanager
def sqlmesh_context(
    db: "Database",
    sqlmesh_root: Path | None = None,
) -> Generator[Any, None, None]:
    """Create a SQLMesh Context that can open the encrypted database.

    SQLMesh's DuckDBConnectionConfig does not support encryption_key,
    so we create a properly-connected DuckDB adapter and pre-populate
    SQLMesh's internal adapter cache. SQLMesh then reuses our encrypted
    connection instead of opening its own unencrypted one.

    Usage::

        with get_database(read_only=False) as db:
            with sqlmesh_context(db) as ctx:
                ctx.plan(auto_apply=True, no_prompts=True)

    Args:
        db: Open Database instance whose connection SQLMesh will borrow.
        sqlmesh_root: Root path for SQLMesh models; defaults to ``SQLMESH_ROOT``.

    Yields:
        A ``sqlmesh.Context`` connected to the encrypted database.

    Raises:
        DatabaseKeyError: If the database connection is closed.
    """
    from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
        Context,
    )
    from sqlmesh.core.config import Config, GatewayConfig
    from sqlmesh.core.config.connection import (
        BaseDuckDBConnectionConfig,
        DuckDBConnectionConfig,
    )
    from sqlmesh.core.console import NoopConsole, set_console
    from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter

    # SQLMesh's rich-based TerminalConsole writes plan/progress directly to
    # stdout, bypassing stdlib logging. Swap in NoopConsole so import/transform
    # commands don't drown the user in SQLMesh chatter — diagnostic output
    # still reaches the sqlmesh_*.log file via the stdlib loggers.
    set_console(NoopConsole())

    # sqlglot emits WARNING-level dialect-fidelity notes while generating model
    # SQL (e.g. "REGEXP_REPLACE with non-literal position" from dim_accounts),
    # spamming stderr several times per transform. We only ever target DuckDB —
    # there is no cross-dialect transpile — so these are non-actionable noise.
    # Quiet to ERROR; genuine sqlglot failures still surface.
    logging.getLogger("sqlglot").setLevel(logging.ERROR)

    root = sqlmesh_root or SQLMESH_ROOT

    # Reuse the caller-supplied connection — DuckDB only allows one
    # connection per file.
    #
    # That connection HAS httpfs loaded (on write opens `_seal_connection` loads
    # it explicitly; on read opens DuckDB pulls in a locally-cached copy itself
    # during ATTACH). It is loaded for its OpenSSL crypto module, not for remote
    # file access — since DuckDB 1.4.1 the built-in mbedtls crypto is read-only,
    # so an encrypted write is impossible without it. No SQLMesh model reads
    # remote files, and none can: `_seal_connection` disables the http/s3
    # filesystems httpfs registers and locks the configuration, so a model (or
    # an agent) that reaches for s3:// gets a PermissionException. See the
    # "Extension seal" block at the top of this module.
    if db._conn is None:  # pyright: ignore[reportPrivateUsage]
        raise DatabaseKeyError(
            "Database connection is closed — cannot create SQLMesh context."
        )
    conn = db._conn  # pyright: ignore[reportPrivateUsage]
    # Use the supplied db's actual path, not settings — during `profile create`
    # the new profile isn't yet the active one, so get_settings() would fail.
    db_path = db._db_path  # pyright: ignore[reportPrivateUsage]
    # Contract guard: a real Database always has a Path here. A test that drives
    # the real sqlmesh_context with a bare mock db (and forgets to patch
    # sqlmesh.Context) reaches this line with an auto-mock _db_path, then
    # silently mkdir's src/moneybin/sqlmesh/<MagicMock ...>/ from the stringified cache_dir
    # below. Fail loudly here instead — the traceback names the offending test.
    if not isinstance(db_path, Path):  # pyright: ignore[reportUnnecessaryIsInstance]  # static type is Path; the guard exists for tests that pass a mock that violates it at runtime
        raise TypeError(
            "sqlmesh_context requires an open Database; got "
            f"{type(db).__name__} whose _db_path is {type(db_path).__name__}, "
            "not a Path. A test is driving the real sqlmesh_context with a mock "
            "db — patch sqlmesh_context (or pass a real Database) in that test."
        )

    cache_key = str(db_path)
    try:
        # _pin_cursor_to_moneybin (module-level) routes SQLMesh's state writes to
        # the persistent moneybin.sqlmesh.* catalog instead of the throwaway
        # memory.sqlmesh.* — see its docstring.
        adapter = DuckDBEngineAdapter(
            lambda: conn,
            default_catalog=_DATABASE_ALIAS,
            register_comments=True,
            cursor_init=_pin_cursor_to_moneybin,
        )
        BaseDuckDBConnectionConfig._data_file_to_adapter[cache_key] = adapter  # type: ignore[reportPrivateUsage]  # no public API for encrypted DB injection

        config = Config(
            default_gateway=_DATABASE_ALIAS,
            # Pin the SQLMesh cache beside this DB instead of the shared
            # src/moneybin/sqlmesh/.cache. The cache is keyed by model fingerprint, not by
            # environment, so one cache shared across concurrent restate plans
            # on different DBs (parallel scenario-test workers) poisons each
            # other's snapshots and raises ConflictingPlanError. Per-DB scopes
            # it to one profile (prod) / one test tmpdir (tests).
            cache_dir=str(db_path.parent / ".sqlmesh-cache"),
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
        # After the caller's SQLMesh plan/run has written
        # register_comments output to core.*, append the privacy
        # classification sigils. Idempotent — short-circuits when the
        # catalog already matches the registry. Skip on read-only DBs
        # (COMMENT ON COLUMN is an ALTER and would fail). Wrapped in a
        # broad except so any other privacy-sync failure never breaks
        # the SQLMesh workflow; logged at debug to avoid noising stderr.
        if not db._read_only:  # pyright: ignore[reportPrivateUsage]  # internal flag
            from moneybin.privacy.comment_sync import sync_classification_comments

            try:
                sync_classification_comments(conn)
            except Exception:  # noqa: BLE001 — sync errors must not break sqlmesh flows
                logger.debug(
                    "Privacy classification sync after sqlmesh_context failed",
                    exc_info=True,
                )
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
            with Database(
                db_path, read_only=False, secret_store=store, no_auto_upgrade=False
            ) as db:
                from moneybin.seeds import materialize_seeds

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
            with Database(
                db_path, read_only=False, secret_store=store, no_auto_upgrade=False
            ) as db:
                from moneybin.seeds import materialize_seeds

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
