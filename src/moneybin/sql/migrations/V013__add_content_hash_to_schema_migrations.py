"""Add content_hash column to app.schema_migrations and backfill from on-disk files.

Adds the column the self-heal path in MigrationRunner uses to detect whether a
previously-failed migration's body has changed since the failure. Backfill is
best-effort: rows whose migration file is still on disk get the current hash;
rows whose file was deleted (purges, renames) stay NULL and are treated as
"unknown" by the self-heal guard (manual intervention required).

Purely additive — nullable VARCHAR, no DEFAULT, no NOT NULL — so there are no
"outstanding writes" interactions with downstream DDL. Safe inside the runner's
enclosing transaction.

Idempotent: skips the ALTER when the column already exists (fresh installs get
the end-state from app_schema_migrations.sql via init_schemas).
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import cast

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent


def migrate(conn: object) -> None:
    """Add content_hash column and backfill from on-disk migration files."""
    existing = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name FROM duckdb_columns()
        WHERE schema_name = 'app'
          AND table_name = 'schema_migrations'
          AND column_name = 'content_hash'
        """
    ).fetchone()
    if existing is None:
        logger.info("Adding content_hash to app.schema_migrations")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.schema_migrations ADD COLUMN content_hash VARCHAR"
        )

    rows = cast(
        list[tuple[int, str]],
        conn.execute(  # type: ignore[union-attr]
            "SELECT version, filename FROM app.schema_migrations "
            "WHERE content_hash IS NULL"
        ).fetchall(),
    )

    backfilled = 0
    for version, filename in rows:
        file_path = MIGRATIONS_DIR / filename
        if not file_path.exists():
            continue
        # Deliberately hardcoded — migrations are frozen historical artifacts
        # and must not import live module constants whose meaning could shift.
        content_hash = hashlib.sha256(file_path.read_bytes()).hexdigest()[:16]
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.schema_migrations SET content_hash = ? WHERE version = ?",
            [content_hash, version],
        )
        backfilled += 1

    logger.info(
        f"V013 complete: backfilled content_hash for {backfilled} row(s); "
        f"{len(rows) - backfilled} row(s) had no on-disk file and remain NULL"
    )
