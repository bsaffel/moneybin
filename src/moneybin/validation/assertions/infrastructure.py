"""Infrastructure assertions — verify wiring invariants, not data shape.

These primitives bind to a ``Database`` instance (not just a raw connection)
because they must read ``db.path`` and verify SQLMesh's adapter binding.

Assertions are read-only: they observe state, never mutate it. Mutating
work (subprocess invocations, schema changes) belongs in pipeline steps,
which separate "what happened" from "did it land correctly".

Failure modes these assertions catch:

- SQLMesh writing to an unencrypted ``memory.*`` catalog instead of the
  encrypted profile DB.
- Stray plaintext ``.duckdb`` files left in temp dirs (sign of an
  unencrypted leak).
- A migrated DB that's behind the latest on-disk migration.
- A populated table failing to meet an expected minimum row count.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from moneybin.database import Database, sqlmesh_context
from moneybin.migrations import MigrationRunner
from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult

logger = logging.getLogger(__name__)


def assert_sqlmesh_catalog_matches(db: Database) -> AssertionResult:
    """Assert SQLMesh's bound DuckDB adapter targets ``db.path``.

    ``sqlmesh_context()`` reads the module-level ``_database_instance``
    singleton in ``moneybin.database``. Callers must ensure that
    singleton is set to ``db`` (e.g., via ``monkeypatch`` in tests, or
    by going through ``get_database()`` in production) before invoking
    this assertion.

    Catches the regression where SQLMesh opens its own unencrypted
    connection and silently writes state to ``memory.sqlmesh.*``.
    """
    db_path = str(Path(db.path).resolve())
    try:
        with sqlmesh_context() as ctx:
            adapter_path = _resolve_adapter_path(ctx)
    except Exception as exc:  # noqa: BLE001  — sqlmesh raises untyped errors during context setup
        return AssertionResult(
            name="sqlmesh_catalog_matches",
            passed=False,
            details={"db_path": db_path, "adapter_path": "<error>"},
            error=f"{type(exc).__name__}: {exc}",
        )
    matches = adapter_path == db_path
    return AssertionResult(
        name="sqlmesh_catalog_matches",
        passed=matches,
        details={"db_path": db_path, "adapter_path": adapter_path},
    )


def _resolve_adapter_path(ctx: Any) -> str:
    """Best-effort introspection of SQLMesh's bound DuckDB path.

    Uses the public ``ctx.engine_adapter.fetchall(...)`` surface against
    DuckDB's ``duckdb_databases()`` catalog rather than reaching into
    SQLMesh internals — this stays stable across SQLMesh versions.
    """
    try:
        rows = ctx.engine_adapter.fetchall(
            "SELECT path FROM duckdb_databases() "
            "WHERE database_name = current_database()"
        )
    except Exception as exc:  # noqa: BLE001  — sqlmesh adapter raises untyped errors
        logger.debug(f"adapter introspection failed: {exc}")
        return "<unknown>"
    if not rows:
        return "<unknown>"
    file_value = rows[0][0]
    if not file_value:
        return "<unknown>"
    return str(Path(file_value).resolve())


def assert_min_rows(db: Database, *, table_min_rows: dict[str, int]) -> AssertionResult:
    """Assert each table has at least the specified number of rows.

    Read-only: counts rows, never mutates. A missing table contributes 0
    rows — callers asserting against not-yet-materialized tables get a
    deterministic failure rather than a query error.
    """
    counts = {table: _count(db, table) for table in table_min_rows}
    failures = {
        table: {"min_required": table_min_rows[table], "actual": counts[table]}
        for table in table_min_rows
        if counts[table] < table_min_rows[table]
    }
    return AssertionResult(
        name="min_rows",
        passed=not failures,
        details={"counts": counts, "failures": failures},
    )


def _count(db: Database, table: str) -> int:
    """Return ``COUNT(*)`` for a fully-qualified table name.

    Identifier validated against DuckDB's catalog before interpolation.
    Tables that don't yet exist return 0 so callers can assert against
    expected post-pipeline state without query errors.
    """
    # Include both tables and views — core.fct_transactions is a VIEW.
    catalog_rows = db.execute(
        """
        SELECT schema_name || '.' || table_name FROM duckdb_tables()
        UNION ALL
        SELECT schema_name || '.' || view_name FROM duckdb_views()
        """
    ).fetchall()
    valid = {row[0] for row in catalog_rows}
    if table not in valid:
        return 0
    row = db.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()  # noqa: S608  — identifier validated against catalog above
    return int(row[0]) if row else 0


def assert_no_unencrypted_db_files(
    db: Database,  # noqa: ARG001 — not used; Database first-arg is the standard signature
    *,
    tmpdir: Path,
) -> AssertionResult:
    """Assert no *unencrypted* ``*.duckdb`` files exist anywhere under ``tmpdir``.

    A ``.duckdb`` file may be either encrypted or plaintext — the file
    extension alone doesn't tell. Probe each candidate by opening without
    an encryption key: an unencrypted file opens successfully, an
    encrypted file raises. A plaintext file in a profile data directory
    means a code path bypassed the ``Database`` abstraction (which always
    attaches via ``ENCRYPTION_KEY``).

    Note: this is the rare case where the failure mode of an external
    library *is* the success path. The scenario's own encrypted DB lives
    under ``tmpdir`` and is correctly skipped because opening without a
    key raises — that's exactly what "encrypted, not leaked" looks like.
    """
    import duckdb

    leaks: list[str] = []
    for p in Path(tmpdir).rglob("*.duckdb"):
        try:
            with duckdb.connect(str(p), read_only=True) as conn:
                conn.execute("SELECT 1").fetchone()
        except Exception:  # noqa: BLE001, S112 — encrypted DBs raise untyped errors; that's the success path
            continue
        leaks.append(str(p.relative_to(tmpdir)))

    leaks.sort()
    return AssertionResult(
        name="no_unencrypted_db_files",
        passed=not leaks,
        details={"files": leaks},
    )


def assert_migrations_at_head(db: Database) -> AssertionResult:
    """Assert no migrations on disk are pending against ``db``.

    Uses ``MigrationRunner.pending()`` as the source of truth — the
    runner reconciles applied versions in ``app.schema_migrations``
    against discovered files on disk.
    """
    runner = MigrationRunner(db)
    pending = runner.pending()
    pending_filenames = [getattr(m, "filename", str(m)) for m in pending]
    return AssertionResult(
        name="migrations_at_head",
        passed=not pending,
        details={
            "pending_count": len(pending),
            "pending": pending_filenames,
        },
    )
