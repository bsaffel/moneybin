"""Infrastructure assertions — verify wiring invariants, not data shape.

These primitives bind to a ``Database`` instance (not just a raw connection)
because they must read ``db.path``, inherit env vars into subprocess children,
and verify SQLMesh's adapter binding.

Failure modes these assertions catch:

- SQLMesh writing to an unencrypted ``memory.*`` catalog instead of the
  encrypted profile DB.
- Subprocess invocations that drop the encryption key (no rows land).
- Stray ``.duckdb`` files left in temp dirs after a test (sign of an
  unencrypted leak).
- A migrated DB that's behind the latest on-disk migration.
"""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: S404  — explicit command lists, never shell=True
from pathlib import Path
from typing import Any

from moneybin.database import Database, sqlmesh_context
from moneybin.migrations import MigrationRunner
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


def assert_encryption_key_propagated_to_subprocess(
    db: Database,
    *,
    command: list[str],
    expected_min_rows: dict[str, int],
    env: dict[str, str] | None = None,
    timeout: int = 300,
) -> AssertionResult:
    """Run ``command`` as a subprocess and verify rows landed in ``db``.

    The subprocess inherits the parent environment (so the encryption-key
    env var or profile selection propagates), then we re-query the same
    encrypted database and confirm row deltas meet the expected minimums.

    A passing result proves both that the subprocess could open the
    encrypted database and that it wrote the expected rows.
    """
    pre_counts = {table: _count(db, table) for table in expected_min_rows}
    proc = subprocess.run(  # noqa: S603  — explicit command list, not shell
        command,
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    post_counts = {table: _count(db, table) for table in expected_min_rows}
    deltas = {
        table: post_counts[table] - pre_counts[table] for table in expected_min_rows
    }
    failures = {
        table: {"min_required": expected_min_rows[table], "delta": deltas[table]}
        for table in expected_min_rows
        if deltas[table] < expected_min_rows[table]
    }
    return AssertionResult(
        name="encryption_key_propagated_to_subprocess",
        passed=proc.returncode == 0 and not failures,
        details={
            "returncode": proc.returncode,
            "deltas": deltas,
            "failures": failures,
            "stderr_tail": proc.stderr[-500:] if proc.stderr else "",
        },
    )


def _count(db: Database, table: str) -> int:
    """Return ``COUNT(*)`` for a fully-qualified table name.

    Identifier validated against DuckDB's catalog before interpolation.
    """
    catalog_rows = db.execute(
        "SELECT schema_name || '.' || table_name FROM duckdb_tables()"
    ).fetchall()
    valid = {row[0] for row in catalog_rows}
    if table not in valid:
        raise ValueError(f"unknown table: {table!r}")
    safe = ".".join(f'"{seg}"' for seg in table.split("."))
    row = db.execute(f"SELECT COUNT(*) FROM {safe}").fetchone()  # noqa: S608  — identifier validated against catalog above
    return int(row[0]) if row else 0


def assert_no_unencrypted_db_files(*, tmpdir: Path) -> AssertionResult:
    """Assert no bare ``*.duckdb`` files exist anywhere under ``tmpdir``.

    Unencrypted DuckDB files in a profile's data directory mean a code
    path bypassed the ``Database`` abstraction (which always attaches
    via ``ENCRYPTION_KEY``). The presence of such a file is itself the
    failure signal — we don't open it.
    """
    leaks = sorted(str(p.relative_to(tmpdir)) for p in Path(tmpdir).rglob("*.duckdb"))
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
