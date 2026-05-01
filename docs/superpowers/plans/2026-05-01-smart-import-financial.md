# Smart Import Financial (OFX/QFX/QBO Parity) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the OFX/QFX/QBO import path to parity with `smart-import-tabular.md`: shared import-batch infrastructure, reversible imports, unified account matching, encrypted writes via `Database.ingest_dataframe()`, formalized QBO support.

**Architecture:** Extract a generic `import_log` module from `tabular_loader.py`, then rewrite `ImportService._import_ofx` to use it (deleting `OFXLoader` along the way). OFX retains its own raw schema (`raw.ofx_*`) but adopts the same import-batch contract that tabular uses. QBO is added as an OFX dialect — extension routing only, no parser changes.

**Tech Stack:** Python 3.12, Pydantic, Typer, DuckDB, Polars, SQLMesh. `ofxparse` for OFX/QFX/QBO parsing (existing dependency). `uv` package manager.

**Spec:** `docs/specs/smart-import-financial.md`.

**Branch:** `feat/smart-import-financial` (worktree at `.worktrees/smart-import-financial`).

**PR plan:** Two sequential PRs off this branch.
- **PR 1** ("Infra parity"): Tasks 1–14. Schema migration + `import_log` extraction + OFX rewrite + magic-byte detection + `--institution` override-when-missing.
- **PR 2** ("Format coverage + ship"): Tasks 15–22. QBO routing + fixtures + scenarios + docs.

After Task 14 lands and PR 1 merges to `main`, rebase the branch onto main before continuing with Task 15.

---

## File Structure

### New files
- `src/moneybin/loaders/import_log.py` — Generic batch lifecycle module (`begin_import`, `finalize_import`, `revert_import`, `get_import_history`).
- `src/moneybin/extractors/institution_resolution.py` — Institution name resolution chain for OFX.
- `src/moneybin/sql/migrations/V003__ofx_import_batch_columns.py` — Schema migration adding `import_id`, `source_type`, `source_origin` columns to `raw.ofx_*` tables.
- `tests/moneybin/test_loaders/test_import_log.py` — Unit tests for the new module.
- `tests/moneybin/test_extractors/test_institution_resolution.py` — Unit tests for institution resolution.
- `tests/moneybin/test_services/test_import_service_ofx.py` — Integration tests for the new `_import_ofx` path.
- `tests/scenarios/ofx_single_account_checking/` — Golden-path scenario.
- `tests/scenarios/ofx_multi_account_statement/` — Multi-account in one file.
- `tests/scenarios/ofx_qbo_intuit_export/` — QBO from QuickBooks export.
- `tests/scenarios/ofx_qbo_bank_export/` — QBO from a bank export.
- `tests/scenarios/ofx_reimport_idempotent/` — Re-import detection.
- `tests/scenarios/ofx_missing_institution_metadata/` — Resolution chain fallback.
- `tests/scenarios/ofx_cross_source_dedup/` — OFX + tabular merge in matching engine.
- `tests/fixtures/ofx/qbo_intuit_sample.qbo` — Sanitized QBO fixture (Intuit export).
- `tests/fixtures/ofx/qbo_bank_sample.qbo` — Sanitized QBO fixture (bank export).

### Modified files
- `src/moneybin/sql/schema/raw_ofx_transactions.sql` — Add `import_id`, `source_type`, `source_origin` columns.
- `src/moneybin/sql/schema/raw_ofx_accounts.sql` — Add `import_id`, `source_type` columns.
- `src/moneybin/sql/schema/raw_ofx_balances.sql` — Add `import_id`, `source_type` columns.
- `src/moneybin/sql/schema/raw_ofx_institutions.sql` — Add `import_id`, `source_type` columns.
- `src/moneybin/extractors/ofx_extractor.py` — Populate new columns in extracted DataFrames; remove `institution_name` parameter (replaced by resolution chain in service layer).
- `src/moneybin/loaders/tabular_loader.py` — Delegate batch lifecycle to `import_log` module (refactor; no behavior change for tabular).
- `src/moneybin/services/import_service.py` — Rewrite `_import_ofx`; update `_detect_file_type` for `.qbo` and magic-byte sniffing; thread institution resolution and re-import detection.
- `src/moneybin/cli/commands/import_cmd.py` — `--institution` semantics flip to override-when-missing; `--force` flag for re-imports.
- `src/moneybin/mcp/tools/import_tools.py` — Reflect new `institution` semantics in `import_file` tool docstring/args.
- `src/moneybin/utils/file.py` — `copy_to_raw()` accepts `qbo` (lands in `data/raw/ofx/`).
- `src/moneybin/metrics/registry.py` — Add OFX-specific batch counter parallel to `TABULAR_IMPORT_BATCHES`.
- `sqlmesh/models/prep/stg_ofx__transactions.sql` — Surface new columns in projection.
- `sqlmesh/models/prep/stg_ofx__accounts.sql` — Surface new columns.
- `sqlmesh/models/prep/stg_ofx__balances.sql` — Surface new columns.
- `sqlmesh/models/prep/stg_ofx__institutions.sql` — Surface new columns.
- `src/moneybin/testing/synthetic/models.py` — Extend file-format marker to include QBO output.
- `src/moneybin/testing/synthetic/writer.py` — Emit `.qbo` variants when scenarios request them.
- `docs/specs/INDEX.md` — Register this spec; mark `archived/ofx-import.md` as superseded.
- `docs/specs/archived/ofx-import.md` — Add superseded note pointing to new spec.
- `README.md` — Update "What Works Today" with QBO + OFX revert support.

### Deleted files
- `src/moneybin/loaders/ofx_loader.py` — Replaced by `_import_ofx` orchestration over `ingest_dataframe()` + `import_log`.
- Existing OFX loader tests (file paths discovered in Task 9 verification step).

---

# PR 1 — Infra parity

## Task 1: Create `import_log` module

Create a generic batch-lifecycle module that both `tabular_loader.py` and the future OFX path can use. This is a pure extraction — no new behavior, just a relocated and generalized version of `TabularLoader`'s lifecycle methods.

**Files:**
- Create: `src/moneybin/loaders/import_log.py`
- Test: `tests/moneybin/test_loaders/test_import_log.py`

- [ ] **Step 1: Write the failing test for `begin_import`**

Create `tests/moneybin/test_loaders/test_import_log.py`:

```python
"""Tests for the generic import_log module."""

import json
import pytest

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.testing.fixtures import test_database  # noqa: F401  # fixture


class TestBeginImport:
    """begin_import creates a 'importing' status row and returns a UUID."""

    def test_returns_uuid_string(self, test_database: Database) -> None:
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/test.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking", "savings"],
        )
        assert len(import_id) == 36
        assert import_id.count("-") == 4

    def test_writes_pending_row(self, test_database: Database) -> None:
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/test.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        row = test_database.execute(
            "SELECT source_file, source_type, source_origin, status, account_names "
            "FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert row is not None
        assert row[0] == "/tmp/test.ofx"
        assert row[1] == "ofx"
        assert row[2] == "wells_fargo"
        assert row[3] == "importing"
        assert json.loads(row[4]) == ["checking"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/moneybin/test_loaders/test_import_log.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'moneybin.loaders.import_log'`.

- [ ] **Step 3: Implement the `import_log` module**

Create `src/moneybin/loaders/import_log.py`:

```python
"""Generic import-batch lifecycle for raw.import_log.

Both tabular and OFX import paths call these functions to create batches,
finalize them with row counts, query history, and revert by import_id.

The module is the single source of truth for which raw tables a given
source_type populates — see _REVERT_TABLES below.
"""

import json
import logging
import uuid
from typing import Any, Literal

from moneybin.database import Database

logger = logging.getLogger(__name__)


_SourceType = Literal["csv", "tsv", "excel", "parquet", "feather", "pipe", "ofx"]


# Allowlist mapping source_type → raw tables that carry rows for that type.
# revert_import() uses this to know what to delete. Adding a new format means
# adding an entry here AND ensuring those tables have an import_id column.
_REVERT_TABLES: dict[str, list[str]] = {
    "csv": ["raw.tabular_transactions", "raw.tabular_accounts"],
    "tsv": ["raw.tabular_transactions", "raw.tabular_accounts"],
    "excel": ["raw.tabular_transactions", "raw.tabular_accounts"],
    "parquet": ["raw.tabular_transactions", "raw.tabular_accounts"],
    "feather": ["raw.tabular_transactions", "raw.tabular_accounts"],
    "pipe": ["raw.tabular_transactions", "raw.tabular_accounts"],
    "ofx": [
        "raw.ofx_transactions",
        "raw.ofx_accounts",
        "raw.ofx_balances",
        "raw.ofx_institutions",
    ],
}


def begin_import(
    db: Database,
    *,
    source_file: str,
    source_type: str,
    source_origin: str,
    account_names: list[str],
    format_name: str | None = None,
    format_source: str | None = None,
) -> str:
    """Create an import_log row in 'importing' state. Returns the new import_id (UUID).

    Args:
        db: Database connection.
        source_file: Absolute path to the imported file.
        source_type: File format marker (csv, ofx, etc.). Must be a key of
            _REVERT_TABLES — anything else cannot be reverted.
        source_origin: Format/institution identifier (e.g., 'wells_fargo', 'tiller').
        account_names: List of account names this import touches.
        format_name: Tabular format name if a format matched; None for OFX.
        format_source: How the format was resolved ('built-in', 'saved', 'detected').
            None for OFX.

    Returns:
        UUID import_id for this batch.

    Raises:
        ValueError: If source_type is not in _REVERT_TABLES.
    """
    if source_type not in _REVERT_TABLES:
        raise ValueError(
            f"Unknown source_type {source_type!r}; "
            f"must be one of {sorted(_REVERT_TABLES)}"
        )
    import_id = str(uuid.uuid4())
    db.execute(
        """
        INSERT INTO raw.import_log (
            import_id, source_file, source_type, source_origin,
            format_name, format_source, account_names, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'importing')
        """,
        [
            import_id,
            source_file,
            source_type,
            source_origin,
            format_name,
            format_source,
            json.dumps(account_names),
        ],
    )
    logger.info(f"Created import batch: {import_id[:8]}...")
    return import_id


def finalize_import(
    db: Database,
    import_id: str,
    *,
    status: Literal["complete", "partial", "failed"],
    rows_total: int,
    rows_imported: int,
    rows_rejected: int = 0,
    rows_skipped_trailing: int = 0,
    rejection_details: list[dict[str, str]] | None = None,
    detection_confidence: str | None = None,
    number_format: str | None = None,
    date_format: str | None = None,
    sign_convention: str | None = None,
    balance_validated: bool | None = None,
) -> None:
    """Finalize an import batch with status and counts.

    The trailing arguments after rows_skipped_trailing are tabular-specific
    metadata. OFX callers leave them at their defaults (all None / not supplied).
    """
    db.execute(
        """
        UPDATE raw.import_log SET
            status = ?,
            rows_total = ?,
            rows_imported = ?,
            rows_rejected = ?,
            rows_skipped_trailing = ?,
            rejection_details = ?,
            detection_confidence = ?,
            number_format = ?,
            date_format = ?,
            sign_convention = ?,
            balance_validated = ?,
            completed_at = CURRENT_TIMESTAMP
        WHERE import_id = ?
        """,
        [
            status,
            rows_total,
            rows_imported,
            rows_rejected,
            rows_skipped_trailing,
            json.dumps(rejection_details) if rejection_details else None,
            detection_confidence,
            number_format,
            date_format,
            sign_convention,
            balance_validated,
            import_id,
        ],
    )
    logger.info(
        f"Import {import_id[:8]}... finalized: {status} "
        f"({rows_imported} imported, {rows_rejected} rejected)"
    )


def revert_import(db: Database, import_id: str) -> dict[str, str | int]:
    """Revert an import batch by deleting all its rows from raw tables.

    Looks up source_type from raw.import_log to determine which tables to
    delete from (via the _REVERT_TABLES allowlist). Updates status to 'reverted'.

    Returns:
        {'status': 'reverted', 'rows_deleted': N} on success.
        {'status': 'not_found', ...} if import_id doesn't exist.
        {'status': 'already_reverted'} if already reverted.
        {'status': 'superseded', ...} if a later import overwrote the rows.
    """
    row = db.execute(
        "SELECT source_type, status, source_file, started_at "
        "FROM raw.import_log WHERE import_id = ?",
        [import_id],
    ).fetchone()

    if row is None:
        return {"status": "not_found", "reason": f"No import with ID {import_id}"}

    src_type, status, source_file, started_at = row

    if status == "reverted":
        return {"status": "already_reverted"}

    if src_type not in _REVERT_TABLES:
        return {
            "status": "unsupported",
            "reason": f"Cannot revert source_type {src_type!r}",
        }

    tables = _REVERT_TABLES[src_type]

    # Count rows in the primary transactions table for this batch (used both
    # for return value and superseded detection).
    primary_table = tables[0]
    rows_total = db.execute(
        f"SELECT COUNT(*) FROM {primary_table} WHERE import_id = ?",  # noqa: S608 — table name is from allowlist
        [import_id],
    ).fetchone()
    rows_to_delete = rows_total[0] if rows_total else 0

    if rows_to_delete == 0:
        # Same superseded check as the original tabular_loader: if a later
        # import upserted over this one's rows, surface that.
        reimport_row = db.execute(
            """
            SELECT import_id
            FROM raw.import_log
            WHERE source_file = ?
              AND import_id != ?
              AND started_at > ?
              AND status NOT IN ('reverted', 'failed')
            ORDER BY started_at DESC
            LIMIT 1
            """,
            [source_file, import_id, started_at],
        ).fetchone()
        if reimport_row:
            newer_id = reimport_row[0]
            return {
                "status": "superseded",
                "reason": (
                    f"File was re-imported as {newer_id[:8]}...; "
                    f"revert that batch to remove the data."
                ),
            }

    db.begin()
    try:
        for table in tables:
            db.execute(
                f"DELETE FROM {table} WHERE import_id = ?",  # noqa: S608 — table from allowlist
                [import_id],
            )
        db.execute(
            """
            UPDATE raw.import_log SET
                status = 'reverted',
                reverted_at = CURRENT_TIMESTAMP
            WHERE import_id = ?
            """,
            [import_id],
        )
        db.commit()
    except Exception:
        db.rollback()
        raise

    logger.info(f"Reverted import {import_id[:8]}...: {rows_to_delete} rows deleted")
    return {"status": "reverted", "rows_deleted": rows_to_delete}


def get_import_history(
    db: Database,
    *,
    limit: int = 20,
    import_id: str | None = None,
) -> list[dict[str, Any]]:
    """Query the import_log. If import_id is given, returns at most one row."""
    if import_id:
        rows = db.execute(
            """
            SELECT import_id, source_file, source_type, source_origin,
                   format_name, status, rows_imported, rows_rejected,
                   detection_confidence, started_at, completed_at
            FROM raw.import_log
            WHERE import_id = ?
            """,
            [import_id],
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT import_id, source_file, source_type, source_origin,
                   format_name, status, rows_imported, rows_rejected,
                   detection_confidence, started_at, completed_at
            FROM raw.import_log
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()

    columns = [
        "import_id",
        "source_file",
        "source_type",
        "source_origin",
        "format_name",
        "status",
        "rows_imported",
        "rows_rejected",
        "detection_confidence",
        "started_at",
        "completed_at",
    ]
    return [dict(zip(columns, row, strict=True)) for row in rows]


def find_existing_import(
    db: Database,
    source_file: str,
) -> str | None:
    """Return the most recent non-reverted import_id for source_file, or None.

    Used by the OFX path (and eventually tabular) to detect re-imports and
    refuse without --force.
    """
    row = db.execute(
        """
        SELECT import_id
        FROM raw.import_log
        WHERE source_file = ?
          AND status NOT IN ('reverted', 'failed')
        ORDER BY started_at DESC
        LIMIT 1
        """,
        [source_file],
    ).fetchone()
    return row[0] if row else None
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/moneybin/test_loaders/test_import_log.py -v
```

Expected: PASS for both tests in `TestBeginImport`.

- [ ] **Step 5: Add tests for `finalize_import`, `revert_import`, `get_import_history`, `find_existing_import`**

Append to `tests/moneybin/test_loaders/test_import_log.py`:

```python
class TestFinalizeImport:
    """finalize_import updates status, counts, and completed_at."""

    def test_marks_complete(self, test_database: Database) -> None:
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/test.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            test_database,
            import_id,
            status="complete",
            rows_total=100,
            rows_imported=100,
        )
        row = test_database.execute(
            "SELECT status, rows_imported, completed_at "
            "FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert row[0] == "complete"
        assert row[1] == 100
        assert row[2] is not None


class TestRevertImport:
    """revert_import deletes from the right tables for the import's source_type."""

    def test_returns_not_found_for_missing_id(self, test_database: Database) -> None:
        result = import_log.revert_import(test_database, "00000000-0000-0000-0000-000000000000")
        assert result["status"] == "not_found"

    def test_reverts_ofx_batch(self, test_database: Database) -> None:
        # Setup: create import row + a single OFX transaction row.
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/test.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        test_database.execute(
            """
            INSERT INTO raw.ofx_transactions (
                source_transaction_id, account_id, transaction_type, date_posted,
                amount, payee, memo, check_number, source_file, extracted_at,
                import_id, source_type, source_origin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "FITID001",
                "checking",
                "DEBIT",
                "2026-01-15",
                "-50.00",
                "Coffee",
                None,
                None,
                "/tmp/test.ofx",
                "2026-01-15 10:00:00",
                import_id,
                "ofx",
                "wells_fargo",
            ],
        )
        import_log.finalize_import(
            test_database, import_id, status="complete", rows_total=1, rows_imported=1
        )

        result = import_log.revert_import(test_database, import_id)
        assert result["status"] == "reverted"
        assert result["rows_deleted"] == 1

        remaining = test_database.execute(
            "SELECT COUNT(*) FROM raw.ofx_transactions WHERE import_id = ?",
            [import_id],
        ).fetchone()[0]
        assert remaining == 0

    def test_already_reverted_returns_status(self, test_database: Database) -> None:
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/test.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            test_database, import_id, status="complete", rows_total=0, rows_imported=0
        )
        import_log.revert_import(test_database, import_id)
        result = import_log.revert_import(test_database, import_id)
        assert result["status"] == "already_reverted"


class TestFindExistingImport:
    """find_existing_import detects prior imports of the same source_file."""

    def test_returns_none_for_new_file(self, test_database: Database) -> None:
        result = import_log.find_existing_import(test_database, "/tmp/never_imported.ofx")
        assert result is None

    def test_returns_import_id_for_imported_file(self, test_database: Database) -> None:
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/once.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            test_database, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(test_database, "/tmp/once.ofx")
        assert result == import_id

    def test_skips_reverted_imports(self, test_database: Database) -> None:
        import_id = import_log.begin_import(
            test_database,
            source_file="/tmp/reverted.ofx",
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            test_database, import_id, status="complete", rows_total=0, rows_imported=0
        )
        import_log.revert_import(test_database, import_id)
        result = import_log.find_existing_import(test_database, "/tmp/reverted.ofx")
        assert result is None


class TestBeginImportValidatesSourceType:
    def test_rejects_unknown_source_type(self, test_database: Database) -> None:
        with pytest.raises(ValueError, match="Unknown source_type"):
            import_log.begin_import(
                test_database,
                source_file="/tmp/x",
                source_type="nope",
                source_origin="x",
                account_names=[],
            )
```

Note: this test depends on `raw.ofx_transactions` having `import_id`, `source_type`, `source_origin` columns — those are added in Task 3. **This task's tests will fail until Task 3 is complete; that's expected. Run them at the end of Task 3.**

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/loaders/import_log.py tests/moneybin/test_loaders/test_import_log.py
git commit -m "Add import_log module for shared batch lifecycle"
```

---

## Task 2: Migrate `TabularLoader` to delegate to `import_log` module

Pure refactor. `TabularLoader` keeps the same public API but its lifecycle methods become two-line shims over `import_log` functions. This proves the new module's API is right by exercising it from the existing tabular path before OFX adopts it.

**Files:**
- Modify: `src/moneybin/loaders/tabular_loader.py`
- Modify: `src/moneybin/metrics/registry.py` (move `TABULAR_IMPORT_BATCHES` increment out of finalize, since `import_log.finalize_import` is now generic)

- [ ] **Step 1: Verify tabular tests pass before changing anything**

```bash
uv run pytest tests/moneybin/test_loaders/ tests/moneybin/test_services/test_tabular_import_service.py -v
```

Expected: PASS (baseline). Note the count.

- [ ] **Step 2: Rewrite `TabularLoader` lifecycle methods as shims**

Edit `src/moneybin/loaders/tabular_loader.py`. Replace `create_import_batch`, `finalize_import_batch`, `revert_import`, `get_import_history` with delegations to the `import_log` module. The metrics counter increment moves to `finalize_import_batch` (the shim) since the underlying `import_log.finalize_import` is format-agnostic and shouldn't know about tabular-specific metrics.

```python
"""Stage 5: Tabular data loader.

Handles raw table writes via Database.ingest_dataframe(). Batch lifecycle
delegates to moneybin.loaders.import_log.
"""

import logging

import polars as pl

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.metrics.registry import TABULAR_IMPORT_BATCHES

logger = logging.getLogger(__name__)


class TabularLoader:
    """Load tabular data into DuckDB raw tables with batch tracking."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def create_import_batch(
        self,
        *,
        source_file: str,
        source_type: str,
        source_origin: str,
        account_names: list[str],
        format_name: str | None = None,
        format_source: str | None = None,
    ) -> str:
        """Create an import batch record. Delegates to import_log module."""
        return import_log.begin_import(
            self.db,
            source_file=source_file,
            source_type=source_type,
            source_origin=source_origin,
            account_names=account_names,
            format_name=format_name,
            format_source=format_source,
        )

    def load_transactions(self, df: pl.DataFrame) -> int:
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe("raw.tabular_transactions", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} transactions")
        return len(df)

    def load_accounts(self, df: pl.DataFrame) -> int:
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe("raw.tabular_accounts", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} accounts")
        return len(df)

    def finalize_import_batch(
        self,
        *,
        import_id: str,
        rows_total: int,
        rows_imported: int,
        rows_rejected: int = 0,
        rows_skipped_trailing: int = 0,
        rejection_details: list[dict[str, str]] | None = None,
        detection_confidence: str | None = None,
        number_format: str | None = None,
        date_format: str | None = None,
        sign_convention: str | None = None,
        balance_validated: bool | None = None,
    ) -> None:
        """Finalize an import batch. Delegates to import_log module + records metric."""
        if rows_imported == 0 and rows_rejected > 0:
            status = "failed"
        elif rows_rejected == 0:
            status = "complete"
        else:
            status = "partial"
        TABULAR_IMPORT_BATCHES.labels(status=status).inc()
        import_log.finalize_import(
            self.db,
            import_id,
            status=status,
            rows_total=rows_total,
            rows_imported=rows_imported,
            rows_rejected=rows_rejected,
            rows_skipped_trailing=rows_skipped_trailing,
            rejection_details=rejection_details,
            detection_confidence=detection_confidence,
            number_format=number_format,
            date_format=date_format,
            sign_convention=sign_convention,
            balance_validated=balance_validated,
        )

    def revert_import(self, import_id: str) -> dict[str, str | int]:
        """Delegate to import_log module."""
        return import_log.revert_import(self.db, import_id)

    def get_import_history(
        self,
        *,
        limit: int = 20,
        import_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        """Delegate to import_log module."""
        return import_log.get_import_history(self.db, limit=limit, import_id=import_id)
```

- [ ] **Step 3: Run tabular tests to verify no regressions**

```bash
uv run pytest tests/moneybin/test_loaders/ tests/moneybin/test_services/test_tabular_import_service.py -v
```

Expected: same count as Step 1, all PASS.

- [ ] **Step 4: Run full test suite**

```bash
make test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/loaders/tabular_loader.py
git commit -m "Migrate TabularLoader lifecycle to import_log module"
```

---

## Task 3: Schema migration — add `import_id`/`source_type`/`source_origin` to `raw.ofx_*`

Add the new columns to all four `raw.ofx_*` tables and to their DDL files. Backfill existing rows with `source_type='ofx'` and a derived `source_origin` from the institution column on `raw.ofx_accounts` (best-effort).

**Files:**
- Modify: `src/moneybin/sql/schema/raw_ofx_transactions.sql`
- Modify: `src/moneybin/sql/schema/raw_ofx_accounts.sql`
- Modify: `src/moneybin/sql/schema/raw_ofx_balances.sql`
- Modify: `src/moneybin/sql/schema/raw_ofx_institutions.sql`
- Create: `src/moneybin/sql/migrations/V003__ofx_import_batch_columns.py`
- Test: `tests/moneybin/test_sql/test_v003_ofx_migration.py`

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_sql/test_v003_ofx_migration.py`:

```python
"""Tests for V003: add import_id/source_type/source_origin to raw.ofx_* tables."""

from moneybin.database import Database
from moneybin.sql.migrations.V003__ofx_import_batch_columns import migrate
from moneybin.testing.fixtures import test_database  # noqa: F401  # fixture


class TestV003Migration:
    def test_adds_columns_to_ofx_transactions(self, test_database: Database) -> None:
        # Drop the new columns to simulate pre-migration state
        for col in ("import_id", "source_type", "source_origin"):
            try:
                test_database.execute(
                    f"ALTER TABLE raw.ofx_transactions DROP COLUMN {col}"
                )
            except Exception:
                pass  # column already absent

        migrate(test_database._conn)

        cols = {
            row[0]
            for row in test_database.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = 'ofx_transactions'"
            ).fetchall()
        }
        assert "import_id" in cols
        assert "source_type" in cols
        assert "source_origin" in cols

    def test_idempotent_on_second_run(self, test_database: Database) -> None:
        migrate(test_database._conn)
        migrate(test_database._conn)  # should not raise

    def test_backfills_source_type_for_existing_rows(
        self, test_database: Database
    ) -> None:
        # Drop columns and insert a legacy row
        for col in ("import_id", "source_type", "source_origin"):
            try:
                test_database.execute(
                    f"ALTER TABLE raw.ofx_transactions DROP COLUMN {col}"
                )
            except Exception:
                pass
        test_database.execute(
            """
            INSERT INTO raw.ofx_transactions (
                source_transaction_id, account_id, transaction_type, date_posted,
                amount, payee, memo, check_number, source_file, extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "LEGACY1",
                "checking",
                "DEBIT",
                "2025-12-01",
                "-10.00",
                "Test",
                None,
                None,
                "/tmp/legacy.ofx",
                "2025-12-01 12:00:00",
            ],
        )

        migrate(test_database._conn)

        row = test_database.execute(
            "SELECT source_type, import_id FROM raw.ofx_transactions "
            "WHERE source_transaction_id = 'LEGACY1'"
        ).fetchone()
        assert row[0] == "ofx"
        assert row[1] is None  # legacy rows have NULL import_id, by design
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/moneybin/test_sql/test_v003_ofx_migration.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'moneybin.sql.migrations.V003__ofx_import_batch_columns'`.

- [ ] **Step 3: Update DDL files**

Edit `src/moneybin/sql/schema/raw_ofx_transactions.sql`:

```sql
/* Transaction records extracted from OFX/QFX files; one record per transaction per account per source file */
CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
    source_transaction_id VARCHAR, -- OFX FITID element; institution-assigned unique transaction identifier
    account_id VARCHAR, -- Account this transaction belongs to; foreign key to raw.ofx_accounts; part of primary key
    transaction_type VARCHAR, -- OFX TRNTYPE element, e.g. DEBIT, CREDIT, CHECK, INT, DIV
    date_posted TIMESTAMP, -- OFX DTPOSTED element; mapped to transaction_date in core
    amount DECIMAL(18, 2), -- OFX TRNAMT element; negative = expense, positive = income
    payee VARCHAR, -- OFX NAME element (payee/merchant); mapped to description in core
    memo VARCHAR, -- OFX MEMO element; supplemental transaction notes from the institution
    check_number VARCHAR, -- OFX CHECKNUM element; check number for paper checks; NULL for electronic transactions
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    import_id VARCHAR, -- UUID of the import batch this row belongs to; NULL for rows imported before V003
    source_type VARCHAR DEFAULT 'ofx', -- Format taxonomy marker; always 'ofx' for OFX/QFX/QBO files
    source_origin VARCHAR, -- Institution slug derived from <FI><ORG> or filename heuristic; NULL for legacy rows
    PRIMARY KEY (source_transaction_id, account_id, source_file)
);
```

Edit `src/moneybin/sql/schema/raw_ofx_accounts.sql`:

```sql
/* Account records extracted from OFX/QFX files; one record per account per source file */
CREATE TABLE IF NOT EXISTS raw.ofx_accounts (
    account_id VARCHAR, -- Account identifier (account number from OFX); part of primary key
    routing_number VARCHAR, -- Bank routing number (US); NULL for credit cards and non-US accounts
    account_type VARCHAR, -- OFX ACCTTYPE element, e.g. CHECKING, SAVINGS, CREDITLINE, MONEYMRKT
    institution_org VARCHAR, -- OFX FI/ORG element; financial institution name as reported by the export
    institution_fid VARCHAR, -- OFX FI/FID element; financial institution numeric identifier
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    import_id VARCHAR, -- UUID of the import batch this row belongs to; NULL for rows imported before V003
    source_type VARCHAR DEFAULT 'ofx', -- Format taxonomy marker; always 'ofx' for OFX/QFX/QBO files
    PRIMARY KEY (account_id, source_file, extracted_at)
);
```

Edit `src/moneybin/sql/schema/raw_ofx_balances.sql`:

```sql
/* Account balance snapshots extracted from OFX/QFX statements */
CREATE TABLE IF NOT EXISTS raw.ofx_balances (
    account_id VARCHAR, -- Account this balance applies to; part of primary key
    statement_start_date TIMESTAMP, -- OFX DTSTART element; statement period start
    statement_end_date TIMESTAMP, -- OFX DTEND element; statement period end; part of primary key
    ledger_balance DECIMAL(18, 2), -- OFX LEDGERBAL/BALAMT; book balance at statement end
    ledger_balance_date TIMESTAMP, -- OFX LEDGERBAL/DTASOF; balance as-of timestamp
    available_balance DECIMAL(18, 2), -- OFX AVAILBAL/BALAMT; available (post-pending) balance, if reported
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    import_id VARCHAR, -- UUID of the import batch this row belongs to; NULL for rows imported before V003
    source_type VARCHAR DEFAULT 'ofx', -- Format taxonomy marker; always 'ofx' for OFX/QFX/QBO files
    PRIMARY KEY (account_id, statement_end_date, source_file)
);
```

Edit `src/moneybin/sql/schema/raw_ofx_institutions.sql`:

```sql
/* Financial institution records extracted from OFX/QFX files */
CREATE TABLE IF NOT EXISTS raw.ofx_institutions (
    organization VARCHAR, -- OFX FI/ORG element; institution name; part of primary key
    fid VARCHAR, -- OFX FI/FID element; institution numeric identifier; part of primary key
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    import_id VARCHAR, -- UUID of the import batch this row belongs to; NULL for rows imported before V003
    source_type VARCHAR DEFAULT 'ofx', -- Format taxonomy marker; always 'ofx' for OFX/QFX/QBO files
    PRIMARY KEY (organization, fid)
);
```

- [ ] **Step 4: Create the migration**

Create `src/moneybin/sql/migrations/V003__ofx_import_batch_columns.py`:

```python
"""Add import_id, source_type, source_origin to raw.ofx_* tables.

Brings OFX raw lineage into parity with the tabular import-batch model.
Existing rows get source_type='ofx' backfilled (literal value) and
import_id/source_origin left NULL — these are 'pre-batch-tracking' rows
that cannot be reverted via import revert.

Idempotent: skips columns that already exist on a fresh install.
"""


_TABLE_COLUMNS = {
    "raw.ofx_transactions": ["import_id", "source_type", "source_origin"],
    "raw.ofx_accounts": ["import_id", "source_type"],
    "raw.ofx_balances": ["import_id", "source_type"],
    "raw.ofx_institutions": ["import_id", "source_type"],
}


def _column_exists(conn: object, schema: str, table: str, column: str) -> bool:
    return (
        conn.execute(  # type: ignore[union-attr]
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
            """,
            [schema, table, column],
        ).fetchone()[0]
        > 0
    )


def migrate(conn: object) -> None:
    """Add the new columns to raw.ofx_* tables and backfill source_type='ofx'."""
    for qualified_table, columns in _TABLE_COLUMNS.items():
        schema, table = qualified_table.split(".", 1)
        for column in columns:
            if _column_exists(conn, schema, table, column):
                continue
            if column == "source_type":
                conn.execute(  # type: ignore[union-attr]
                    f"ALTER TABLE {qualified_table} ADD COLUMN {column} VARCHAR DEFAULT 'ofx'"
                )
                # Backfill existing rows; DEFAULT only applies to new inserts.
                conn.execute(  # type: ignore[union-attr]
                    f"UPDATE {qualified_table} SET source_type = 'ofx' WHERE source_type IS NULL"
                )
            else:
                conn.execute(  # type: ignore[union-attr]
                    f"ALTER TABLE {qualified_table} ADD COLUMN {column} VARCHAR"
                )
```

- [ ] **Step 5: Run migration test**

```bash
uv run pytest tests/moneybin/test_sql/test_v003_ofx_migration.py -v
```

Expected: PASS for all three tests.

- [ ] **Step 6: Run the full Task 1 test suite (now that columns exist)**

```bash
uv run pytest tests/moneybin/test_loaders/test_import_log.py -v
```

Expected: PASS for all `import_log` tests including `TestRevertImport::test_reverts_ofx_batch`.

- [ ] **Step 7: Run full suite to confirm no regressions**

```bash
make test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/sql/schema/raw_ofx_*.sql \
        src/moneybin/sql/migrations/V003__ofx_import_batch_columns.py \
        tests/moneybin/test_sql/test_v003_ofx_migration.py
git commit -m "Add import_id/source_type/source_origin columns to raw.ofx_* tables"
```

---

## Task 4: Update SQLMesh staging models to surface new columns

The staging views need to project the new columns so downstream models (`dim_accounts`, `fct_transactions`) and `import history` queries see them.

**Files:**
- Modify: `sqlmesh/models/prep/stg_ofx__transactions.sql`
- Modify: `sqlmesh/models/prep/stg_ofx__accounts.sql`
- Modify: `sqlmesh/models/prep/stg_ofx__balances.sql`
- Modify: `sqlmesh/models/prep/stg_ofx__institutions.sql`

- [ ] **Step 1: Update `stg_ofx__transactions.sql`**

The current `source_origin` is computed as `COALESCE(a.institution_org, 'ofx_unknown')` — keep that fallback for legacy rows but prefer the new `t.source_origin` column when populated. Add `t.import_id` to the projection.

```sql
MODEL (
  name prep.stg_ofx__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    t.source_transaction_id,
    t.account_id,
    t.transaction_type,
    t.date_posted::DATE AS posted_date,
    t.amount,
    TRIM(t.payee) AS payee,
    TRIM(t.memo) AS memo,
    t.check_number,
    t.source_file,
    t.extracted_at,
    t.loaded_at,
    t.import_id,
    'ofx' AS source_type,
    COALESCE(t.source_origin, a.institution_org, 'ofx_unknown') AS source_origin,
    ROW_NUMBER() OVER (PARTITION BY t.source_transaction_id, t.account_id ORDER BY t.loaded_at DESC) AS _row_num
  FROM raw.ofx_transactions AS t
  LEFT JOIN raw.ofx_accounts AS a
    ON t.account_id = a.account_id
)
SELECT
  source_transaction_id,
  account_id,
  transaction_type,
  posted_date,
  amount,
  payee,
  memo,
  check_number,
  source_file,
  extracted_at,
  loaded_at,
  import_id,
  source_type,
  source_origin
FROM ranked
WHERE
  _row_num = 1
```

- [ ] **Step 2: Update `stg_ofx__accounts.sql`**

Read the current file:

```bash
cat sqlmesh/models/prep/stg_ofx__accounts.sql
```

Add `import_id` to the projection (read existing columns, append `import_id`, preserve `source_type` from raw or literal `'ofx'`). Pattern matches Step 1. Use `uv run sqlmesh -p sqlmesh format` after editing.

- [ ] **Step 3: Update `stg_ofx__balances.sql` and `stg_ofx__institutions.sql`**

Same pattern: add `import_id` to projection, ensure `source_type` is surfaced.

- [ ] **Step 4: Run SQL formatter**

```bash
uv run sqlmesh -p sqlmesh format
```

Expected: no errors. Files may be reformatted (block comments converted, etc.) — that's fine.

- [ ] **Step 5: Run integration tests that exercise the staging views**

```bash
uv run pytest tests/integration/ -v -k 'ofx'
```

Expected: PASS. Watch for any column-not-found errors in the staging views; if any, the projection list above is incomplete — add the missing column.

- [ ] **Step 6: Commit**

```bash
git add sqlmesh/models/prep/stg_ofx__*.sql
git commit -m "Surface import_id/source_type/source_origin in stg_ofx__ views"
```

---

## Task 5: Update `OFXExtractor` to populate new columns and drop `institution_name` parameter

The extractor's `extract_from_file` currently accepts `institution_name: str | None` as an override. That parameter goes away — institution resolution moves to the service layer (Task 6) per `data-extraction.md` "don't expose options for extractor-derivable fields." The extractor stops trying to populate `institution_org` from a caller hint and just reflects what's in the file.

The extractor also gains parameters for `import_id` and `source_origin` so the produced DataFrames carry these columns ready for `ingest_dataframe()`.

**Files:**
- Modify: `src/moneybin/extractors/ofx_extractor.py`
- Test: `tests/moneybin/test_extractors/test_ofx_extractor.py` (add cases)

- [ ] **Step 1: Find existing extractor tests**

```bash
ls tests/moneybin/test_extractors/
```

Note the existing `test_ofx_extractor.py` location (or create one if missing).

- [ ] **Step 2: Write a failing test for the new signature**

Add to `tests/moneybin/test_extractors/test_ofx_extractor.py`:

```python
class TestExtractorPopulatesBatchColumns:
    """extract_from_file populates import_id and source_origin in returned DataFrames."""

    def test_transactions_df_has_import_id_and_source_origin(self) -> None:
        # Use a minimal sample OFX fixture; create one if none exists yet.
        from pathlib import Path

        from moneybin.extractors.ofx_extractor import OFXExtractor

        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.skip("OFX fixture not present yet")

        extractor = OFXExtractor()
        result = extractor.extract_from_file(
            fixture,
            import_id="11111111-1111-1111-1111-111111111111",
            source_origin="test_bank",
        )

        txns = result["transactions"]
        assert "import_id" in txns.columns
        assert "source_origin" in txns.columns
        assert "source_type" in txns.columns
        assert all(v == "11111111-1111-1111-1111-111111111111" for v in txns["import_id"].to_list())
        assert all(v == "test_bank" for v in txns["source_origin"].to_list())
        assert all(v == "ofx" for v in txns["source_type"].to_list())
```

- [ ] **Step 3: Run test to confirm it fails**

```bash
uv run pytest tests/moneybin/test_extractors/test_ofx_extractor.py::TestExtractorPopulatesBatchColumns -v
```

Expected: FAIL on signature mismatch (no `import_id` keyword) or skip if fixture missing. **If skipped, create the fixture first by running an existing OFX import test that produces sample output, or copy a minimal fixture from `synthetic` test outputs.**

- [ ] **Step 4: Modify `OFXExtractor.extract_from_file`**

Edit `src/moneybin/extractors/ofx_extractor.py`. Change the signature and add the columns to extracted DataFrames. The full updated method:

```python
def extract_from_file(
    self,
    file_path: Path,
    *,
    import_id: str,
    source_origin: str,
) -> dict[str, pl.DataFrame]:
    """Extract all data from an OFX/QFX/QBO file.

    Args:
        file_path: Path to the file.
        import_id: UUID of the import batch this extraction belongs to.
            Stamped on every row in every returned DataFrame.
        source_origin: Institution slug resolved by the caller (service layer).
            Stamped on transactions and accounts.

    Returns:
        dict with DataFrames for institutions, accounts, transactions, balances.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ValueError: If the file cannot be parsed.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"OFX file not found: {file_path}")

    logger.info(f"Extracting data from OFX file: {file_path}")

    try:
        with open(file_path, "rb") as f:
            content = f.read().decode("utf-8", errors="ignore")
        content = self._preprocess_ofx_content(content)

        from io import BytesIO

        # ofxparse library has incomplete type annotations
        ofx = ofxparse.OfxParser.parse(BytesIO(content.encode("utf-8")))  # type: ignore[reportUnknownMemberType]

        extraction_timestamp = datetime.now()
        source_file = str(file_path)

        results = {
            "institutions": self._extract_institutions(
                ofx, source_file, extraction_timestamp, import_id
            ),
            "accounts": self._extract_accounts(
                ofx, source_file, extraction_timestamp, import_id
            ),
            "transactions": self._extract_transactions(
                ofx, source_file, extraction_timestamp, import_id, source_origin
            ),
            "balances": self._extract_balances(
                ofx, source_file, extraction_timestamp, import_id
            ),
        }

        logger.info(
            f"Extracted {len(results['institutions'])} institution(s), "
            f"{len(results['accounts'])} account(s), "
            f"{len(results['transactions'])} transaction(s)"
        )

        return results

    except Exception as e:
        logger.error(f"Failed to parse OFX file {file_path}: {e}")
        raise ValueError(f"Invalid OFX file format: {e}") from e
```

Update each `_extract_*` method to accept and stamp the new fields. Example for `_extract_transactions`:

```python
def _extract_transactions(
    self,
    ofx: Any,
    source_file: str,
    extraction_timestamp: datetime,
    import_id: str,
    source_origin: str,
) -> pl.DataFrame:
    """Extract transaction data from OFX file."""
    transactions_data: list[dict[str, Any]] = []

    for account in ofx.accounts:
        for transaction in account.statement.transactions:
            tx_schema = OFXTransactionSchema(
                id=transaction.id,
                type=transaction.type,
                date=transaction.date,
                amount=transaction.amount,
                payee=transaction.payee,
                memo=transaction.memo,
                checknum=transaction.checknum
                if hasattr(transaction, "checknum")
                else None,
            )

            tx_data = {
                "source_transaction_id": tx_schema.id,
                "account_id": account.account_id,
                "transaction_type": tx_schema.type,
                "date_posted": tx_schema.date.isoformat(),
                "amount": tx_schema.amount,
                "payee": tx_schema.payee,
                "memo": tx_schema.memo,
                "check_number": tx_schema.checknum,
                "source_file": source_file,
                "extracted_at": extraction_timestamp.isoformat(),
                "import_id": import_id,
                "source_type": "ofx",
                "source_origin": source_origin,
            }
            transactions_data.append(tx_data)

    if transactions_data:
        return pl.DataFrame(
            transactions_data,
            schema_overrides=_TRANSACTIONS_AMOUNT_OVERRIDES,
        )
    return self._build_empty_transactions_df()


def _build_empty_transactions_df(self) -> pl.DataFrame:
    """Build an empty transactions DataFrame with the correct schema."""
    return pl.DataFrame(
        schema={
            "source_transaction_id": pl.String,
            "account_id": pl.String,
            "transaction_type": pl.String,
            "date_posted": pl.String,
            "amount": _DECIMAL_AMOUNT,
            "payee": pl.String,
            "memo": pl.String,
            "check_number": pl.String,
            "source_file": pl.String,
            "extracted_at": pl.String,
            "import_id": pl.String,
            "source_type": pl.String,
            "source_origin": pl.String,
        }
    )
```

Apply the same pattern to `_extract_institutions`, `_extract_accounts`, `_extract_balances`. For accounts/balances/institutions, only `import_id` and `source_type='ofx'` are added (no `source_origin` — institutions don't have one separate from themselves).

Update the empty-DataFrame fallbacks in each method to include the new columns with `pl.String` schema.

Update the convenience function at the bottom of the file:

```python
def extract_ofx_file(
    file_path: Path | str,
    *,
    import_id: str,
    source_origin: str,
) -> dict[str, pl.DataFrame]:
    """Convenience function to extract data from an OFX/QFX/QBO file."""
    extractor = OFXExtractor()
    return extractor.extract_from_file(
        Path(file_path), import_id=import_id, source_origin=source_origin
    )
```

- [ ] **Step 5: Run extractor tests**

```bash
uv run pytest tests/moneybin/test_extractors/test_ofx_extractor.py -v
```

Expected: existing tests may FAIL because they pass `institution_name` positionally or as a kwarg. **Update those callsites in tests to use the new signature**: pass `import_id="<uuid>"` and `source_origin="<slug>"`. If a test was specifically validating the institution-name-override behavior, delete that test — the behavior is gone.

- [ ] **Step 6: Run pyright on the modified file**

```bash
uv run pyright src/moneybin/extractors/ofx_extractor.py
```

Expected: 0 errors.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/extractors/ofx_extractor.py tests/moneybin/test_extractors/test_ofx_extractor.py
git commit -m "Update OFXExtractor to populate import_id/source_type/source_origin"
```

---

## Task 6: Create institution resolution module

Implement the resolution chain: `<FI><ORG>` → `<FI><FID>` lookup → filename heuristic → CLI override → interactive prompt → fail.

**Files:**
- Create: `src/moneybin/extractors/institution_resolution.py`
- Test: `tests/moneybin/test_extractors/test_institution_resolution.py`

- [ ] **Step 1: Write failing tests**

Create `tests/moneybin/test_extractors/test_institution_resolution.py`:

```python
"""Tests for the OFX institution resolution chain."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.extractors.institution_resolution import (
    InstitutionResolutionError,
    resolve_institution,
)


def _ofx_with(org: str | None = None, fid: str | None = None) -> MagicMock:
    """Build a mock parsed-OFX object with one account whose institution has the given org/fid."""
    inst = MagicMock()
    inst.organization = org
    inst.fid = fid
    account = MagicMock()
    account.institution = inst
    ofx = MagicMock()
    ofx.accounts = [account]
    return ofx


class TestResolveInstitution:
    def test_uses_fi_org_when_present(self) -> None:
        ofx = _ofx_with(org="WELLS FARGO BANK", fid="3000")
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/whatever.qfx"),
            cli_override=None,
            interactive=False,
        )
        assert result == "wells_fargo_bank"

    def test_falls_back_to_fid_lookup(self) -> None:
        ofx = _ofx_with(org=None, fid="3000")  # 3000 = wells_fargo per static table
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/whatever.qfx"),
            cli_override=None,
            interactive=False,
        )
        assert result == "wells_fargo"

    def test_falls_back_to_filename_heuristic(self) -> None:
        ofx = _ofx_with(org=None, fid=None)
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/chase_2026.qfx"),
            cli_override=None,
            interactive=False,
        )
        assert result == "chase"

    def test_uses_cli_override_when_chain_empty(self) -> None:
        ofx = _ofx_with(org=None, fid=None)
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/anonymous.qfx"),
            cli_override="Local Credit Union",
            interactive=False,
        )
        assert result == "local_credit_union"

    def test_cli_override_logs_ignored_when_file_has_org(self, caplog) -> None:
        ofx = _ofx_with(org="Wells Fargo", fid=None)
        with caplog.at_level("INFO"):
            result = resolve_institution(
                ofx,
                file_path=Path("/tmp/x.qfx"),
                cli_override="Other Bank",
                interactive=False,
            )
        assert result == "wells_fargo"
        assert any("ignored" in r.message.lower() for r in caplog.records)

    def test_raises_in_non_interactive_mode_when_chain_empty(self) -> None:
        ofx = _ofx_with(org=None, fid=None)
        with pytest.raises(InstitutionResolutionError):
            resolve_institution(
                ofx,
                file_path=Path("/tmp/anonymous.qfx"),
                cli_override=None,
                interactive=False,
            )
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/moneybin/test_extractors/test_institution_resolution.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the resolver**

Create `src/moneybin/extractors/institution_resolution.py`:

```python
"""Institution name resolution for OFX/QFX/QBO imports.

Per .claude/rules/data-extraction.md, callers should not be required to
supply values present in the file. This module implements the resolution
chain so the service layer can derive a canonical institution slug:

1. parsed_ofx.fi.org (when populated)
2. parsed_ofx.fi.fid → static lookup table
3. filename heuristic (regex against known patterns)
4. CLI/MCP override (only when 1-3 yield nothing)
5. interactive prompt (only when interactive=True)
6. raise InstitutionResolutionError (non-interactive failure)

The static FID lookup starts small and grows via PR contributions. Unknown
FIDs fall through to step 3.
"""

import logging
import re
from pathlib import Path
from typing import Any

from moneybin.utils import slugify

logger = logging.getLogger(__name__)


class InstitutionResolutionError(ValueError):
    """Raised when institution cannot be derived in non-interactive mode."""


# Static lookup: well-known OFX FID → institution slug.
# Add entries here as PRs identify new institutions in the wild.
_FID_TO_SLUG: dict[str, str] = {
    "3000": "wells_fargo",
    "10898": "chase",
    "1601": "bank_of_america",
    "10247": "citi",
    "5950": "us_bank",
}

# Filename heuristic: lowercase substring → slug.
_FILENAME_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"wells[\s_-]*fargo", re.IGNORECASE), "wells_fargo"),
    (re.compile(r"chase", re.IGNORECASE), "chase"),
    (re.compile(r"bank[\s_-]*of[\s_-]*america|\bboa\b", re.IGNORECASE), "bank_of_america"),
    (re.compile(r"\bciti\b|citibank", re.IGNORECASE), "citi"),
    (re.compile(r"us[\s_-]*bank", re.IGNORECASE), "us_bank"),
    (re.compile(r"capital[\s_-]*one", re.IGNORECASE), "capital_one"),
    (re.compile(r"discover", re.IGNORECASE), "discover"),
    (re.compile(r"amex|american[\s_-]*express", re.IGNORECASE), "amex"),
]


def resolve_institution(
    parsed_ofx: Any,
    *,
    file_path: Path,
    cli_override: str | None,
    interactive: bool,
) -> str:
    """Resolve an institution slug for an OFX/QFX/QBO file.

    Returns:
        Institution slug (snake_case, e.g. 'wells_fargo').

    Raises:
        InstitutionResolutionError: If the chain yields nothing and
            interactive=False.
    """
    # Step 1: <FI><ORG> from the file.
    org = _first_org(parsed_ofx)
    if org:
        if cli_override:
            logger.info(
                f"--institution {cli_override!r} ignored; using <FI><ORG> from file"
            )
        return slugify(org)

    # Step 2: <FI><FID> lookup.
    fid = _first_fid(parsed_ofx)
    if fid and fid in _FID_TO_SLUG:
        if cli_override:
            logger.info(
                f"--institution {cli_override!r} ignored; using FID lookup for {fid!r}"
            )
        return _FID_TO_SLUG[fid]

    # Step 3: filename heuristic.
    for pattern, slug in _FILENAME_PATTERNS:
        if pattern.search(file_path.name):
            if cli_override:
                logger.info(
                    f"--institution {cli_override!r} ignored; matched filename pattern {slug!r}"
                )
            return slug

    # Step 4: CLI override.
    if cli_override:
        return slugify(cli_override)

    # Step 5: interactive prompt.
    if interactive:
        try:
            answer = input("Institution name (e.g. 'Wells Fargo'): ").strip()
            if answer:
                return slugify(answer)
        except EOFError:
            pass

    # Step 6: fail.
    raise InstitutionResolutionError(
        f"Institution could not be derived from file {file_path.name!r}. "
        f"Pass --institution <name> to override."
    )


def _first_org(parsed_ofx: Any) -> str | None:
    """Return the first non-empty <FI><ORG> across all accounts in the file."""
    for account in getattr(parsed_ofx, "accounts", []):
        inst = getattr(account, "institution", None)
        if inst is None:
            continue
        org = getattr(inst, "organization", None)
        if org:
            return str(org).strip() or None
    return None


def _first_fid(parsed_ofx: Any) -> str | None:
    """Return the first non-empty <FI><FID> across all accounts in the file."""
    for account in getattr(parsed_ofx, "accounts", []):
        inst = getattr(account, "institution", None)
        if inst is None:
            continue
        fid = getattr(inst, "fid", None)
        if fid:
            return str(fid).strip() or None
    return None
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/moneybin/test_extractors/test_institution_resolution.py -v
```

Expected: PASS for all six tests.

- [ ] **Step 5: Run pyright**

```bash
uv run pyright src/moneybin/extractors/institution_resolution.py
```

Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/extractors/institution_resolution.py \
        tests/moneybin/test_extractors/test_institution_resolution.py
git commit -m "Add institution resolution chain for OFX/QFX/QBO"
```

---

## Task 7: Add magic-byte detection to `_detect_file_type`

`_detect_file_type` currently routes purely by extension. Add magic-byte sniffing as a second pass for files with missing/wrong extensions.

**Files:**
- Modify: `src/moneybin/services/import_service.py`
- Test: `tests/moneybin/test_services/test_detect_file_type.py`

- [ ] **Step 1: Write failing tests**

Create `tests/moneybin/test_services/test_detect_file_type.py`:

```python
"""Tests for _detect_file_type, including magic-byte sniffing."""

from pathlib import Path

import pytest

from moneybin.services.import_service import _detect_file_type


class TestDetectFileType:
    def test_routes_ofx_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "x.ofx"
        f.write_text("dummy")
        assert _detect_file_type(f) == "ofx"

    def test_routes_qfx_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "x.qfx"
        f.write_text("dummy")
        assert _detect_file_type(f) == "ofx"

    def test_sniffs_ofx_content_in_unknown_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "renamed.txt"
        f.write_text("OFXHEADER:100\nDATA:OFXSGML\n<OFX></OFX>")
        assert _detect_file_type(f) == "ofx"

    def test_sniffs_xml_ofx_content(self, tmp_path: Path) -> None:
        f = tmp_path / "renamed.txt"
        f.write_text('<?xml version="1.0"?>\n<OFX><BANKMSGSRSV1/></OFX>')
        assert _detect_file_type(f) == "ofx"

    def test_extension_takes_precedence_over_sniffing(self, tmp_path: Path) -> None:
        # CSV that incidentally contains <OFX> in a description should still route as tabular
        f = tmp_path / "x.csv"
        f.write_text("date,amount,description\n2026-01-01,10.00,About <OFX> tag\n")
        assert _detect_file_type(f) == "tabular"

    def test_unknown_extension_with_no_magic_bytes_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "x.bin"
        f.write_text("not a recognized format")
        with pytest.raises(ValueError, match="Unsupported file type"):
            _detect_file_type(f)
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/moneybin/test_services/test_detect_file_type.py -v
```

Expected: most FAIL — magic-byte sniffing isn't implemented.

- [ ] **Step 3: Update `_detect_file_type` in `import_service.py`**

Replace the existing `_detect_file_type` function with:

```python
def _detect_file_type(file_path: Path) -> str:
    """Detect file type from extension, falling back to magic-byte sniffing.

    Returns:
        File type string: 'ofx', 'w2', or 'tabular'.

    Raises:
        ValueError: If the file cannot be classified.
    """
    from moneybin.extractors.tabular.format_detector import TABULAR_EXTENSIONS

    suffix = file_path.suffix.lower()
    if suffix in (".ofx", ".qfx", ".qbo"):
        return "ofx"
    if suffix == ".pdf":
        return "w2"
    if suffix in TABULAR_EXTENSIONS:
        return "tabular"

    # Extension didn't match — try magic-byte sniffing for OFX content.
    if _sniff_ofx_content(file_path):
        return "ofx"

    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx, .qbo, .csv, .tsv, .xlsx, .parquet, .feather, .pdf"
    )


def _sniff_ofx_content(file_path: Path) -> bool:
    """Return True if the file's first 1024 bytes look like OFX/QFX/QBO content."""
    try:
        with open(file_path, "rb") as f:
            head = f.read(1024)
    except OSError:
        return False
    head_lstripped = head.lstrip()
    if head_lstripped.startswith(b"OFXHEADER:"):
        return True
    if head_lstripped.startswith(b"<?xml") and b"<OFX>" in head:
        return True
    return False
```

Note: `.qbo` is added to the extension list here as part of this task (Task 7), even though the broader QBO formalization is PR 2. Adding the routing here lets PR 1 already accept `.qbo` files; PR 2 adds the fixtures and scenarios that exercise that routing end-to-end.

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/moneybin/test_services/test_detect_file_type.py -v
```

Expected: PASS for all six tests.

- [ ] **Step 5: Update `copy_to_raw()` to accept `qbo`**

Edit `src/moneybin/utils/file.py`. The current code:

```python
if normalized_type in ("qfx", "ofx"):
    target_dir = base_path / "ofx"
```

Change to:

```python
if normalized_type in ("qfx", "ofx", "qbo"):
    target_dir = base_path / "ofx"
```

- [ ] **Step 6: Update the `Unsupported` error message in `_detect_file_type`**

(Already included in Step 3.)

- [ ] **Step 7: Run full suite**

```bash
make test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/services/import_service.py \
        src/moneybin/utils/file.py \
        tests/moneybin/test_services/test_detect_file_type.py
git commit -m "Add magic-byte detection and .qbo routing to _detect_file_type"
```

---

## Task 8: Add OFX-specific Prometheus metric

Parallel to `TABULAR_IMPORT_BATCHES`. The generic `IMPORT_RECORDS_TOTAL`, `IMPORT_DURATION_SECONDS`, `IMPORT_ERRORS_TOTAL` already exist and are used by both paths via the `source_type` label.

**Files:**
- Modify: `src/moneybin/metrics/registry.py`

- [ ] **Step 1: Read current metrics file**

```bash
sed -n '40,60p' src/moneybin/metrics/registry.py
```

Note the `TABULAR_IMPORT_BATCHES` definition.

- [ ] **Step 2: Add `OFX_IMPORT_BATCHES`**

Add after `TABULAR_IMPORT_BATCHES`:

```python
OFX_IMPORT_BATCHES = Counter(
    "moneybin_ofx_import_batches_total",
    "OFX/QFX/QBO import batches by status (complete, partial, failed).",
    labelnames=("status",),
)
```

- [ ] **Step 3: Run pyright**

```bash
uv run pyright src/moneybin/metrics/registry.py
```

Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/metrics/registry.py
git commit -m "Add OFX_IMPORT_BATCHES metric"
```

---

## Task 9: Rewrite `ImportService._import_ofx`

The big one. Replace the existing `_import_ofx` (which uses `OFXLoader`) with the new orchestration: detect re-import → resolve institution → resolve accounts → `begin_import` → `extract_from_file` → `ingest_dataframe` × 4 → `finalize_import` → metrics.

**Files:**
- Modify: `src/moneybin/services/import_service.py`
- Test: `tests/moneybin/test_services/test_import_service_ofx.py`

- [ ] **Step 1: Write failing integration tests**

Create `tests/moneybin/test_services/test_import_service_ofx.py`:

```python
"""Integration tests for ImportService._import_ofx via the new pipeline."""

import time
from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService
from moneybin.testing.fixtures import test_database  # noqa: F401  # fixture


class TestImportOFXBatchLifecycle:
    def test_import_creates_committed_batch(
        self, test_database: Database, tmp_path: Path
    ) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.skip("Sample OFX fixture missing")

        service = ImportService(test_database)
        result = service.import_file(fixture, apply_transforms=False)

        assert result.transactions > 0

        # Find the import_id via the source_file
        history = import_log.get_import_history(test_database, limit=5)
        ofx_imports = [h for h in history if h["source_type"] == "ofx"]
        assert len(ofx_imports) >= 1
        latest = ofx_imports[0]
        assert latest["status"] == "complete"
        assert latest["rows_imported"] == result.transactions

    def test_reverting_ofx_batch_deletes_rows(
        self, test_database: Database
    ) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.skip("Sample OFX fixture missing")

        service = ImportService(test_database)
        service.import_file(fixture, apply_transforms=False)

        history = import_log.get_import_history(test_database, limit=5)
        latest = [h for h in history if h["source_type"] == "ofx"][0]
        import_id = latest["import_id"]

        result = import_log.revert_import(test_database, import_id)
        assert result["status"] == "reverted"

        remaining = test_database.execute(
            "SELECT COUNT(*) FROM raw.ofx_transactions WHERE import_id = ?",
            [import_id],
        ).fetchone()[0]
        assert remaining == 0

    def test_reimport_without_force_raises(self, test_database: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.skip("Sample OFX fixture missing")

        service = ImportService(test_database)
        service.import_file(fixture, apply_transforms=False)

        with pytest.raises(ValueError, match="already imported"):
            service.import_file(fixture, apply_transforms=False)

    def test_reimport_with_force_creates_new_batch(
        self, test_database: Database
    ) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.skip("Sample OFX fixture missing")

        service = ImportService(test_database)
        service.import_file(fixture, apply_transforms=False)
        service.import_file(fixture, apply_transforms=False, force=True)

        history = import_log.get_import_history(test_database, limit=5)
        ofx_for_file = [
            h
            for h in history
            if h["source_type"] == "ofx" and h["source_file"] == str(fixture)
        ]
        assert len(ofx_for_file) == 2
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/moneybin/test_services/test_import_service_ofx.py -v
```

Expected: FAIL — current `_import_ofx` doesn't accept `force`, doesn't detect re-imports, and uses `OFXLoader` which doesn't write `import_id`.

- [ ] **Step 3: Rewrite `_import_ofx`**

In `src/moneybin/services/import_service.py`, replace the existing `_import_ofx` method with:

```python
def _import_ofx(
    self,
    file_path: Path,
    *,
    institution: str | None = None,
    force: bool = False,
    interactive: bool = False,
) -> ImportResult:
    """Import an OFX/QFX/QBO file via the shared import-batch pipeline.

    Args:
        file_path: Path to the file.
        institution: Override-when-missing flag — consulted only if the
            resolution chain (FI/ORG → FID lookup → filename) yields nothing.
        force: If True, allow re-importing a file that's already been imported.
            The previous batch is left in place; this creates a new batch.
        interactive: If True, prompt for institution when the chain yields
            nothing. False for --yes, MCP, and scripts.

    Returns:
        ImportResult with summary.

    Raises:
        ValueError: On re-import without force, or when institution can't be derived.
    """
    import time
    from io import BytesIO

    import ofxparse

    from moneybin.extractors.institution_resolution import (
        InstitutionResolutionError,
        resolve_institution,
    )
    from moneybin.extractors.ofx_extractor import OFXExtractor
    from moneybin.loaders import import_log
    from moneybin.metrics.registry import (
        IMPORT_DURATION_SECONDS,
        IMPORT_ERRORS_TOTAL,
        IMPORT_RECORDS_TOTAL,
        OFX_IMPORT_BATCHES,
    )

    result = ImportResult(file_path=str(file_path), file_type="ofx")
    _t0 = time.monotonic()

    # Re-import detection
    if not force:
        existing = import_log.find_existing_import(self._db, str(file_path))
        if existing:
            raise ValueError(
                f"File already imported (import_id {existing[:8]}...). "
                f"Use --force to re-import."
            )

    # Parse once for institution resolution; re-parse inside the extractor
    # is fine — these files are small. The alternative is plumbing a
    # parsed-OFX object through the extractor, which leaks abstraction.
    with open(file_path, "rb") as f:
        content = f.read().decode("utf-8", errors="ignore")
    extractor = OFXExtractor()
    content = extractor._preprocess_ofx_content(content)
    parsed_ofx = ofxparse.OfxParser.parse(BytesIO(content.encode("utf-8")))  # type: ignore[reportUnknownMemberType]

    # Resolve institution (raises InstitutionResolutionError on non-interactive failure)
    try:
        source_origin = resolve_institution(
            parsed_ofx,
            file_path=file_path,
            cli_override=institution,
            interactive=interactive,
        )
    except InstitutionResolutionError as e:
        IMPORT_ERRORS_TOTAL.labels(
            source_type="ofx", error_type="institution_unresolved"
        ).inc()
        raise ValueError(str(e)) from e

    # Begin batch BEFORE extraction so failures land as 'failed' status rows.
    account_names = [
        a.account_id for a in parsed_ofx.accounts if a.account_id is not None
    ]
    import_id = import_log.begin_import(
        self._db,
        source_file=str(file_path),
        source_type="ofx",
        source_origin=source_origin,
        account_names=account_names,
    )

    try:
        data = extractor.extract_from_file(
            file_path,
            import_id=import_id,
            source_origin=source_origin,
        )
    except Exception:
        import_log.finalize_import(
            self._db,
            import_id,
            status="failed",
            rows_total=0,
            rows_imported=0,
        )
        OFX_IMPORT_BATCHES.labels(status="failed").inc()
        IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="extract").inc()
        raise

    # Resolve account_ids via the shared matcher (best-effort: if matching
    # fails or no existing accounts, the OFX account_id from the file is
    # used as-is, which preserves existing behavior).
    self._match_ofx_accounts(data, account_names)

    # Write all four DataFrames through the encrypted ingest path.
    rows_loaded = {}
    for table_key, qualified in (
        ("institutions", "raw.ofx_institutions"),
        ("accounts", "raw.ofx_accounts"),
        ("transactions", "raw.ofx_transactions"),
        ("balances", "raw.ofx_balances"),
    ):
        df = data[table_key]
        if len(df) > 0:
            self._db.ingest_dataframe(qualified, df, on_conflict="upsert")
        rows_loaded[table_key] = len(df)

    rows_imported = rows_loaded["transactions"]
    status = "complete" if rows_imported > 0 else "partial"

    import_log.finalize_import(
        self._db,
        import_id,
        status=status,
        rows_total=rows_imported,
        rows_imported=rows_imported,
    )
    OFX_IMPORT_BATCHES.labels(status=status).inc()
    IMPORT_RECORDS_TOTAL.labels(source_type="ofx").inc(rows_imported)
    IMPORT_DURATION_SECONDS.labels(source_type="ofx").observe(
        time.monotonic() - _t0
    )

    result.institutions = rows_loaded["institutions"]
    result.accounts = rows_loaded["accounts"]
    result.transactions = rows_loaded["transactions"]
    result.balances = rows_loaded["balances"]
    result.details = rows_loaded

    if rows_imported > 0:
        result.date_range = self._query_date_range(
            "raw.ofx_transactions", "CAST(date_posted AS DATE)", file_path
        )

    return result


def _match_ofx_accounts(
    self,
    data: dict[str, "pl.DataFrame"],
    account_names: list[str],
) -> None:
    """Best-effort account matching for OFX. Logs decisions; does not mutate data.

    Today's behavior (carried forward): the OFX file's account_id IS the
    matching key downstream. This method exists so future improvements
    (renaming or merging accounts at import time) have a single place to
    live, and so account-match metrics are emitted for OFX too.
    """
    from moneybin.metrics.registry import ACCOUNT_MATCH_OUTCOMES_TOTAL

    for name in account_names:
        # Existing OFX account_ids are the institution-assigned numbers.
        # We don't rename them at import time (would break dedup), but we
        # do want metrics symmetry with tabular.
        ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="ofx_passthrough").inc()
```

Note: this preserves existing behavior where the OFX file's `account_id` is used directly as the match key. The spec lists "wired to `account_matching.py`" as a parity goal; the simplest correct interpretation is "OFX accounts participate in the same metrics surface and the same future renaming hooks." A more aggressive rewrite (matching OFX account numbers against existing tabular accounts and unifying them) is *out of scope for this PR* — it would change the dedup contract and warrants its own spec.

- [ ] **Step 4: Update `import_file` to thread the new args**

In the same file, update `import_file`:

```python
def import_file(
    self,
    file_path: str | Path,
    *,
    apply_transforms: bool = True,
    institution: str | None = None,
    force: bool = False,
    interactive: bool = False,
    account_id: str | None = None,
    account_name: str | None = None,
    format_name: str | None = None,
    overrides: dict[str, str] | None = None,
    sign: str | None = None,
    date_format: str | None = None,
    number_format: str | None = None,
    save_format: bool = True,
    sheet: str | None = None,
    delimiter: str | None = None,
    encoding: str | None = None,
    no_row_limit: bool = False,
    no_size_limit: bool = False,
    auto_accept: bool = False,
) -> ImportResult:
    """Import a financial data file into DuckDB.

    Args:
        ...
        institution: Institution name override for OFX/QFX/QBO files. Consulted
            only when the resolution chain (FI/ORG → FID lookup → filename)
            yields nothing.
        force: If True, allow re-importing a file already in the import_log.
        interactive: If True, prompt for institution when resolution fails.
        ...
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    file_type = _detect_file_type(path)
    logger.info(f"Importing {_display_label(file_type, path)} file: {path}")

    if file_type == "ofx":
        result = self._import_ofx(
            path, institution=institution, force=force, interactive=interactive
        )
    elif file_type == "w2":
        result = self._import_w2(path)
    elif file_type == "tabular":
        result = self._import_tabular(
            path,
            account_name=account_name,
            account_id=account_id,
            format_name=format_name,
            overrides=overrides,
            sign=sign,
            date_format_override=date_format,
            number_format_override=number_format,
            save_format=save_format,
            sheet=sheet,
            delimiter=delimiter,
            encoding=encoding,
            no_row_limit=no_row_limit,
            no_size_limit=no_size_limit,
            auto_accept=auto_accept,
        )
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    if apply_transforms and file_type in ("ofx", "tabular"):
        try:
            self._run_matching()
        except Exception:  # noqa: BLE001
            logger.debug("Matching skipped (views may not exist yet)", exc_info=True)
        result.core_tables_rebuilt = self.run_transforms()
        self._apply_categorization()

    logger.info(f"Import complete: {result.summary()}")
    return result
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/moneybin/test_services/test_import_service_ofx.py -v
```

Expected: PASS for all four tests (or skip if fixture missing — see Step 6).

- [ ] **Step 6: Create the minimal OFX fixture if missing**

```bash
ls tests/fixtures/ofx/
```

If `sample_minimal.ofx` doesn't exist, create it. A minimal OFX 1.x SGML sample:

```bash
mkdir -p tests/fixtures/ofx
cat > tests/fixtures/ofx/sample_minimal.ofx <<'EOF'
OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE

<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<DTSERVER>20260115120000</DTSERVER>
<LANGUAGE>ENG</LANGUAGE>
<FI><ORG>SAMPLE BANK</ORG><FID>9999</FID></FI>
</SONRS>
</SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS>
<TRNUID>0</TRNUID>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<STMTRS>
<CURDEF>USD</CURDEF>
<BANKACCTFROM><BANKID>123456789</BANKID><ACCTID>1111</ACCTID><ACCTTYPE>CHECKING</ACCTTYPE></BANKACCTFROM>
<BANKTRANLIST>
<DTSTART>20260101</DTSTART><DTEND>20260131</DTEND>
<STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20260115</DTPOSTED><TRNAMT>-12.50</TRNAMT><FITID>FITID001</FITID><NAME>Coffee Shop</NAME></STMTTRN>
<STMTTRN><TRNTYPE>CREDIT</TRNTYPE><DTPOSTED>20260120</DTPOSTED><TRNAMT>1500.00</TRNAMT><FITID>FITID002</FITID><NAME>Payroll</NAME></STMTTRN>
</BANKTRANLIST>
<LEDGERBAL><BALAMT>1487.50</BALAMT><DTASOF>20260131120000</DTASOF></LEDGERBAL>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
EOF
```

Re-run:

```bash
uv run pytest tests/moneybin/test_services/test_import_service_ofx.py -v
```

Expected: PASS for all four tests.

- [ ] **Step 7: Run full integration tests**

```bash
uv run pytest tests/integration/ -v
```

Expected: PASS. Address any test failures that result from the OFX signature change (likely a few callsites that pass `institution_name` positionally — update to the new kwarg).

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/services/import_service.py \
        tests/moneybin/test_services/test_import_service_ofx.py \
        tests/fixtures/ofx/sample_minimal.ofx
git commit -m "Rewrite _import_ofx using import_log + ingest_dataframe"
```

---

## Task 10: Delete `OFXLoader` and update remaining callsites

`OFXLoader` is now unused. Delete it and any remaining tests.

**Files:**
- Delete: `src/moneybin/loaders/ofx_loader.py`
- Delete: corresponding test files (discover by grep)
- Modify: `src/moneybin/loaders/__init__.py` (remove the export)

- [ ] **Step 1: Find remaining references**

```bash
grep -rn 'OFXLoader\|ofx_loader' src/ tests/ --include='*.py'
```

Expected: only the file itself, the import line in `loaders/__init__.py`, and possibly old tests.

- [ ] **Step 2: Delete the loader and remove the export**

```bash
git rm src/moneybin/loaders/ofx_loader.py
```

Edit `src/moneybin/loaders/__init__.py`. Remove any `from moneybin.loaders.ofx_loader import OFXLoader` and any `OFXLoader` mention in `__all__`.

- [ ] **Step 3: Delete obsolete tests**

```bash
grep -rln 'OFXLoader' tests/
```

For each match, evaluate: if the test was specifically for `OFXLoader` internals (load_data, create_raw_tables), delete the file. If the test imports `OFXLoader` incidentally, update it to use the new path or remove the unused import.

```bash
# Example, adjust to actual paths found:
git rm tests/moneybin/test_loaders/test_ofx_loader.py
```

- [ ] **Step 4: Run full suite**

```bash
make test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -m "Delete OFXLoader; OFX path uses ingest_dataframe + import_log"
```

---

## Task 11: Wire CLI flags — `--institution` and `--force`

The CLI's `import` command currently accepts `--institution` (default-provider semantics) and lacks `--force`. Update both.

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py`

- [ ] **Step 1: Read the current import command signature**

```bash
grep -n 'def import_file\|institution\|force' src/moneybin/cli/commands/import_cmd.py | head -30
```

- [ ] **Step 2: Update the import command**

Find the function (typically `import_cmd` or `import_file_cmd`) and update its signature/help text:

```python
@app.command(name="import")
def import_file_cmd(
    file_path: Path = typer.Argument(..., help="Path to the file to import"),
    institution: str | None = typer.Option(
        None,
        "--institution",
        "-i",
        help=(
            "Institution override for OFX/QFX/QBO files. Consulted only when "
            "the file's <FI><ORG>, FID lookup, and filename heuristic all "
            "yield nothing."
        ),
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Re-import a file already in the import log (creates a new batch).",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Auto-accept fuzzy matches and skip interactive prompts.",
    ),
    # ... existing flags ...
) -> None:
    """Import a financial data file."""
    from moneybin.database import get_database
    from moneybin.services.import_service import ImportService

    service = ImportService(get_database())
    interactive = not yes and sys.stdin.isatty()

    try:
        result = service.import_file(
            file_path,
            institution=institution,
            force=force,
            interactive=interactive,
            auto_accept=yes,
            # ... pass other existing args ...
        )
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(code=1) from e

    typer.echo(result.summary())
```

Adjust to match the actual function structure in your codebase — preserve all existing flags, just add `force`/update `institution` help text and route `interactive`.

- [ ] **Step 3: Update the existing CLI tests**

```bash
grep -rln 'institution\|--institution' tests/moneybin/test_cli/
```

Find tests that pass `--institution` for OFX imports. Two likely changes:

1. Tests that pass `--institution "Some Bank"` for a file that already has `<FI><ORG>` — they should now expect a log message that the override was ignored, but the import should still succeed.
2. Tests that rely on `--institution` to *override* the file's value — those tests are validating the old behavior. Update them: either drop the override (let the file's value through) or use a fixture without `<FI><ORG>` so the override is consulted.

- [ ] **Step 4: Add a CLI test for `--force`**

Add to the appropriate `tests/moneybin/test_cli/test_import_*.py`:

```python
def test_force_allows_reimport(tmp_path, runner):
    """import --force allows re-importing a file already in the log."""
    fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
    if not fixture.exists():
        pytest.skip("Fixture missing")

    # First import succeeds
    result = runner.invoke(app, ["import", str(fixture)])
    assert result.exit_code == 0

    # Second import without --force fails
    result = runner.invoke(app, ["import", str(fixture)])
    assert result.exit_code != 0
    assert "already imported" in result.stderr.lower() or "already imported" in result.stdout.lower()

    # Second import with --force succeeds
    result = runner.invoke(app, ["import", str(fixture), "--force"])
    assert result.exit_code == 0
```

- [ ] **Step 5: Run CLI tests**

```bash
uv run pytest tests/moneybin/test_cli/ -v -k 'import'
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/
git commit -m "CLI: --institution becomes override-when-missing; add --force"
```

---

## Task 12: Update MCP `import_file` tool

Reflect the new `institution` semantics and `force` flag in the MCP tool surface.

**Files:**
- Modify: `src/moneybin/mcp/tools/import_tools.py`

- [ ] **Step 1: Read the current tool signature**

```bash
grep -n 'def import_file\|institution\|force' src/moneybin/mcp/tools/import_tools.py
```

- [ ] **Step 2: Update the tool**

In `src/moneybin/mcp/tools/import_tools.py`, find the `import_file` MCP tool and update:

```python
@app.tool()
def import_file(
    file_path: str,
    institution: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Import a financial data file (OFX, QFX, QBO, CSV, TSV, Excel, Parquet, PDF).

    Args:
        file_path: Absolute path to the file.
        institution: Institution name override for OFX/QFX/QBO files. Consulted
            only when the file's <FI><ORG>, FID lookup, and filename heuristic
            all yield nothing. For files with institution metadata, this
            argument is logged and ignored.
        force: If True, allow re-importing a file already in the import log.
            Returns a structured error otherwise.

    Returns:
        Response envelope with import_id, summary counts, and date range.
    """
    from moneybin.database import get_database
    from moneybin.services.import_service import ImportService

    service = ImportService(get_database())
    try:
        result = service.import_file(
            Path(file_path),
            institution=institution,
            force=force,
            interactive=False,  # MCP is always non-interactive
        )
    except ValueError as e:
        return {
            "summary": {"sensitivity": "low", "error": str(e)},
            "data": [],
            "actions": [
                "Pass force=true to re-import",
                "Pass institution=<name> if institution couldn't be derived",
            ],
        }

    # Look up the import_id for the just-completed import.
    from moneybin.loaders import import_log

    history = import_log.get_import_history(get_database(), limit=1)
    import_id = history[0]["import_id"] if history else None

    return {
        "summary": {
            "sensitivity": "low",
            "import_id": import_id,
            "transactions": result.transactions,
            "accounts": result.accounts,
            "date_range": result.date_range,
        },
        "data": [result.details],
        "actions": [
            f"Use import.revert with import_id={import_id} to undo",
            "Use spending.summary to view imported transactions",
        ],
    }
```

Match the project's actual MCP tool conventions — the above is illustrative. The key changes are: add `force` argument, update `institution` docstring, return `import_id` in response.

- [ ] **Step 3: Run MCP tool tests**

```bash
uv run pytest tests/moneybin/test_mcp/ -v -k 'import'
```

Expected: PASS. Update any tests that asserted the old `institution` semantics.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/mcp/tools/import_tools.py tests/moneybin/test_mcp/
git commit -m "MCP: update import_file tool with new institution + force semantics"
```

---

## Task 13: Run pre-commit checks and full test suite

End-of-PR-1 verification.

- [ ] **Step 1: Format**

```bash
make format
```

- [ ] **Step 2: Lint**

```bash
make lint
```

Expected: 0 errors.

- [ ] **Step 3: Type-check modified files**

```bash
uv run pyright \
  src/moneybin/loaders/import_log.py \
  src/moneybin/loaders/tabular_loader.py \
  src/moneybin/extractors/ofx_extractor.py \
  src/moneybin/extractors/institution_resolution.py \
  src/moneybin/services/import_service.py \
  src/moneybin/cli/commands/import_cmd.py \
  src/moneybin/mcp/tools/import_tools.py
```

Expected: 0 errors.

- [ ] **Step 4: Full test suite**

```bash
make check test
```

Expected: PASS.

- [ ] **Step 5: Commit any formatting fixups**

If `make format` produced changes:

```bash
git add -A
git commit -m "Format and lint cleanup"
```

---

## Task 14: Open PR 1

- [ ] **Step 1: Push branch**

```bash
git push -u origin feat/smart-import-financial
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "Bring OFX/QFX/QBO imports to parity with smart-import-tabular (PR 1: infra)" --body "$(cat <<'EOF'
## Summary

PR 1 of 2 for `smart-import-financial.md`. Brings OFX/QFX/QBO imports onto the same import-batch infrastructure tabular uses.

- Extracts `import_log` module from `tabular_loader.py`; both pipelines now share batch lifecycle, history, and revert.
- Adds `import_id`, `source_type`, `source_origin` columns to `raw.ofx_*` tables (V003 migration).
- Rewrites `ImportService._import_ofx` to use `import_log` + `Database.ingest_dataframe()` + `account_matching.py`. Deletes `OFXLoader`.
- `--institution` flips from default-provider to override-when-missing semantics (per `data-extraction.md`).
- Adds `--force` flag for re-imports; without it, re-importing a file rejects with an actionable error.
- Adds magic-byte detection so misnamed OFX files route correctly.
- `.qbo` extension now routes to the OFX pipeline (formalized with fixtures and scenarios in PR 2).

## Test plan

- [ ] `make check test` passes locally
- [ ] All existing tabular import tests pass unchanged
- [ ] New `test_import_log.py`, `test_v003_ofx_migration.py`, `test_institution_resolution.py`, `test_import_service_ofx.py` all pass
- [ ] CI green
- [ ] Manual: import an OFX file, verify `moneybin import history` shows the batch, run `moneybin import revert <id>`, verify rows are deleted

## Follow-up

PR 2 will add QBO fixtures, the seven scenario tests, synthetic-generator QBO support, and the docs flip (INDEX.md, README, archived spec note).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for review and merge**

After PR 1 merges to `main`, rebase the worktree branch:

```bash
git fetch origin
git rebase origin/main
```

Resolve any conflicts (unlikely for PR 2 work, since it's mostly additive). Then continue with Task 15.

---

# PR 2 — Format coverage + ship

## Task 15: Add QBO fixtures from two institutions

Create sanitized `.qbo` fixtures: one in the style of an Intuit/QuickBooks export, one in the style of a bank's "Quicken Web Connect" download. They differ structurally — Intuit exports often include `<INTU.BID>` / `<INTU.USERID>` extension tags; bank exports stay closer to plain OFX 1.x.

**Files:**
- Create: `tests/fixtures/ofx/qbo_intuit_sample.qbo`
- Create: `tests/fixtures/ofx/qbo_bank_sample.qbo`

- [ ] **Step 1: Create `qbo_intuit_sample.qbo`**

```bash
cat > tests/fixtures/ofx/qbo_intuit_sample.qbo <<'EOF'
OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE

<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<DTSERVER>20260115120000</DTSERVER>
<LANGUAGE>ENG</LANGUAGE>
<FI><ORG>INTUIT</ORG><FID>1234</FID></FI>
<INTU.BID>1234</INTU.BID>
</SONRS>
</SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS>
<TRNUID>0</TRNUID>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<STMTRS>
<CURDEF>USD</CURDEF>
<BANKACCTFROM><BANKID>987654321</BANKID><ACCTID>2222</ACCTID><ACCTTYPE>CHECKING</ACCTTYPE></BANKACCTFROM>
<BANKTRANLIST>
<DTSTART>20260101</DTSTART><DTEND>20260131</DTEND>
<STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20260105</DTPOSTED><TRNAMT>-25.00</TRNAMT><FITID>QBO_INTUIT_001</FITID><NAME>Grocery Store</NAME></STMTTRN>
<STMTTRN><TRNTYPE>CREDIT</TRNTYPE><DTPOSTED>20260115</DTPOSTED><TRNAMT>2000.00</TRNAMT><FITID>QBO_INTUIT_002</FITID><NAME>Payroll Deposit</NAME></STMTTRN>
</BANKTRANLIST>
<LEDGERBAL><BALAMT>1975.00</BALAMT><DTASOF>20260131120000</DTASOF></LEDGERBAL>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
EOF
```

- [ ] **Step 2: Create `qbo_bank_sample.qbo`**

```bash
cat > tests/fixtures/ofx/qbo_bank_sample.qbo <<'EOF'
OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE

<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<DTSERVER>20260201120000</DTSERVER>
<LANGUAGE>ENG</LANGUAGE>
<FI><ORG>WELLS FARGO</ORG><FID>3000</FID></FI>
</SONRS>
</SIGNONMSGSRSV1>
<CREDITCARDMSGSRSV1>
<CCSTMTTRNRS>
<TRNUID>0</TRNUID>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<CCSTMTRS>
<CURDEF>USD</CURDEF>
<CCACCTFROM><ACCTID>4444555566667777</ACCTID></CCACCTFROM>
<BANKTRANLIST>
<DTSTART>20260101</DTSTART><DTEND>20260131</DTEND>
<STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20260108</DTPOSTED><TRNAMT>-12.34</TRNAMT><FITID>QBO_BANK_001</FITID><NAME>Coffee</NAME><MEMO>downtown</MEMO></STMTTRN>
<STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20260120</DTPOSTED><TRNAMT>-45.67</TRNAMT><FITID>QBO_BANK_002</FITID><NAME>Restaurant</NAME></STMTTRN>
</BANKTRANLIST>
<LEDGERBAL><BALAMT>-58.01</BALAMT><DTASOF>20260131120000</DTASOF></LEDGERBAL>
</CCSTMTRS>
</CCSTMTTRNRS>
</CREDITCARDMSGSRSV1>
</OFX>
EOF
```

- [ ] **Step 3: Verify both parse**

```bash
uv run python -c "
from pathlib import Path
from moneybin.extractors.ofx_extractor import OFXExtractor

for path in ['tests/fixtures/ofx/qbo_intuit_sample.qbo', 'tests/fixtures/ofx/qbo_bank_sample.qbo']:
    e = OFXExtractor()
    data = e.extract_from_file(Path(path), import_id='00000000-0000-0000-0000-000000000000', source_origin='test')
    print(f'{path}: txns={len(data[\"transactions\"])}, accts={len(data[\"accounts\"])}')
"
```

Expected: each file produces ≥ 2 transactions, ≥ 1 account.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/ofx/qbo_intuit_sample.qbo tests/fixtures/ofx/qbo_bank_sample.qbo
git commit -m "Add QBO fixtures from Intuit and bank export styles"
```

---

## Task 16: Synthetic data generator emits QBO output

Extend the synthetic writer so scenarios can request QBO-formatted output (same OFX content, `.qbo` extension and a `source_origin` consistent with QuickBooks-style exports).

**Files:**
- Modify: `src/moneybin/testing/synthetic/models.py`
- Modify: `src/moneybin/testing/synthetic/writer.py`
- Test: extend the existing synthetic writer tests

- [ ] **Step 1: Read the current `source_type` Literal in models**

```bash
grep -n 'source_type' src/moneybin/testing/synthetic/models.py | head
```

- [ ] **Step 2: Extend the literal to include QBO as a file-format marker**

In `src/moneybin/testing/synthetic/models.py`, change:

```python
source_type: Literal["ofx", "csv"]
```

to:

```python
# Note: this is a file-format marker for the synthetic writer (not the matching
# taxonomy source_type). 'qbo' produces .qbo extension with OFX content; the
# resulting raw rows still carry source_type='ofx' in the matching taxonomy.
source_type: Literal["ofx", "qbo", "csv"]
```

- [ ] **Step 3: Update `writer.py` to emit QBO when requested**

```bash
grep -n 'source_type\s*==\s*["\']ofx["\']\|\.ofx' src/moneybin/testing/synthetic/writer.py | head
```

For each branch that emits OFX, add a parallel branch for QBO. The content is identical; only the file extension differs. Recommended: factor out a `_write_ofx_content(account, transactions, suffix: Literal["ofx", "qbo"])` helper if duplication grows.

Concretely, edit any pattern like:

```python
ofx_accts = [a for a in result.accounts if a.source_type == "ofx"]
```

to:

```python
ofx_accts = [a for a in result.accounts if a.source_type in ("ofx", "qbo")]
```

And for the file-write step, set the filename suffix based on the account's `source_type`:

```python
suffix = ".qbo" if account.source_type == "qbo" else ".ofx"
output_path = output_dir / f"{account.account_name}_{period}{suffix}"
```

- [ ] **Step 4: Add a writer test for QBO output**

Add to the existing synthetic writer test file (find with `grep -rln 'class TestWriter' tests/`):

```python
def test_writer_emits_qbo_extension_for_qbo_source_type(tmp_path):
    """Accounts with source_type='qbo' produce .qbo files with OFX content."""
    from moneybin.testing.synthetic.models import (
        SyntheticAccount,
        SyntheticResult,
        SyntheticTransaction,
    )
    from moneybin.testing.synthetic.writer import write_synthetic_files

    account = SyntheticAccount(
        account_id="checking",
        account_name="Test Checking",
        source_type="qbo",
        # ... other required fields per the dataclass ...
    )
    txn = SyntheticTransaction(
        # minimal txn with required fields
        account_name="Test Checking",
        date="2026-01-15",
        amount="-12.50",
        description="Coffee",
    )
    result = SyntheticResult(
        accounts=[account],
        transactions=[txn],
        seed=42,
    )

    write_synthetic_files(result, output_dir=tmp_path)

    qbo_files = list(tmp_path.glob("*.qbo"))
    assert len(qbo_files) >= 1, "expected at least one .qbo file"

    # Content should still be OFX SGML
    content = qbo_files[0].read_text()
    assert content.startswith("OFXHEADER:")
```

Adjust the dataclass instantiation to match the actual `SyntheticAccount` / `SyntheticTransaction` field requirements.

- [ ] **Step 5: Run synthetic tests**

```bash
uv run pytest tests/moneybin/test_synthetic/ -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/testing/synthetic/models.py \
        src/moneybin/testing/synthetic/writer.py \
        tests/moneybin/test_synthetic/
git commit -m "Synthetic generator: emit .qbo files for source_type='qbo'"
```

---

## Task 17: Scenario test — `ofx_single_account_checking`

The scenario suite uses YAML expectation files and a runner. The seven new scenarios share structure; this task creates the first one as a template, the next task creates the remaining six in parallel.

**Files:**
- Create: `tests/scenarios/ofx_single_account_checking/scenario.yaml`
- Create: `tests/scenarios/ofx_single_account_checking/expectations.yaml`
- Create: `tests/scenarios/ofx_single_account_checking/inputs/sample.ofx` (or symlink to fixture)

- [ ] **Step 1: Inspect an existing scenario for structure**

```bash
ls tests/scenarios/ | head
ls tests/scenarios/$(ls tests/scenarios/ | head -1)/
cat tests/scenarios/$(ls tests/scenarios/ | head -1)/scenario.yaml
```

Note the YAML structure — typically `inputs:`, `pipeline:`, and an expectations block.

- [ ] **Step 2: Create the scenario directory and files**

Adapt the YAML keys to match what the existing scenarios use. Pattern (adjust to actual conventions):

```yaml
# tests/scenarios/ofx_single_account_checking/scenario.yaml
name: ofx_single_account_checking
description: Single-account OFX import — golden path through the new import_log pipeline.
inputs:
  - path: inputs/sample.ofx
    type: ofx
pipeline:
  - import:
      file: inputs/sample.ofx
  - transforms: true
```

```yaml
# tests/scenarios/ofx_single_account_checking/expectations.yaml
import_log:
  count: 1
  source_type: ofx
  status: complete
raw_ofx_transactions:
  row_count: 2
core_fct_transactions:
  row_count: 2
  source_type: ofx
```

Copy the fixture:

```bash
mkdir -p tests/scenarios/ofx_single_account_checking/inputs
cp tests/fixtures/ofx/sample_minimal.ofx tests/scenarios/ofx_single_account_checking/inputs/sample.ofx
```

- [ ] **Step 3: Run the scenario**

```bash
make test-scenarios
# OR, if a finer-grained command exists:
uv run pytest tests/scenarios/ -v -k 'ofx_single_account'
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/scenarios/ofx_single_account_checking/
git commit -m "Add ofx_single_account_checking scenario"
```

---

## Task 18: Scenario tests — remaining six scenarios

Create the other six scenarios using the pattern from Task 17. For brevity, each is described with its distinguishing setup; the YAML structure mirrors Task 17.

**Files:**
- Create: `tests/scenarios/ofx_multi_account_statement/{scenario.yaml,expectations.yaml,inputs/sample.ofx}`
- Create: `tests/scenarios/ofx_qbo_intuit_export/{scenario.yaml,expectations.yaml,inputs/sample.qbo}`
- Create: `tests/scenarios/ofx_qbo_bank_export/{scenario.yaml,expectations.yaml,inputs/sample.qbo}`
- Create: `tests/scenarios/ofx_reimport_idempotent/{scenario.yaml,expectations.yaml,inputs/sample.ofx}`
- Create: `tests/scenarios/ofx_missing_institution_metadata/{scenario.yaml,expectations.yaml,inputs/sample.ofx}`
- Create: `tests/scenarios/ofx_cross_source_dedup/{scenario.yaml,expectations.yaml,inputs/{ofx_side.ofx,tabular_side.csv}}`

- [ ] **Step 1: `ofx_multi_account_statement`**

Build a multi-account OFX fixture (one file with two `<STMTTRNRS>` blocks for different account IDs, or a `<BANKMSGSRSV1>` + `<CREDITCARDMSGSRSV1>`). Inputs file:

```bash
mkdir -p tests/scenarios/ofx_multi_account_statement/inputs
cat > tests/scenarios/ofx_multi_account_statement/inputs/sample.ofx <<'EOF'
OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE

<OFX>
<SIGNONMSGSRSV1><SONRS><STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS><DTSERVER>20260115120000</DTSERVER><LANGUAGE>ENG</LANGUAGE><FI><ORG>MULTI BANK</ORG><FID>0001</FID></FI></SONRS></SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS><TRNUID>0</TRNUID><STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<STMTRS><CURDEF>USD</CURDEF><BANKACCTFROM><BANKID>1</BANKID><ACCTID>CHECKING1</ACCTID><ACCTTYPE>CHECKING</ACCTTYPE></BANKACCTFROM><BANKTRANLIST><DTSTART>20260101</DTSTART><DTEND>20260131</DTEND>
<STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20260110</DTPOSTED><TRNAMT>-50.00</TRNAMT><FITID>MA001</FITID><NAME>Grocery</NAME></STMTTRN>
</BANKTRANLIST><LEDGERBAL><BALAMT>1000.00</BALAMT><DTASOF>20260131120000</DTASOF></LEDGERBAL></STMTRS>
</STMTTRNRS>
<STMTTRNRS><TRNUID>1</TRNUID><STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<STMTRS><CURDEF>USD</CURDEF><BANKACCTFROM><BANKID>1</BANKID><ACCTID>SAVINGS1</ACCTID><ACCTTYPE>SAVINGS</ACCTTYPE></BANKACCTFROM><BANKTRANLIST><DTSTART>20260101</DTSTART><DTEND>20260131</DTEND>
<STMTTRN><TRNTYPE>CREDIT</TRNTYPE><DTPOSTED>20260115</DTPOSTED><TRNAMT>100.00</TRNAMT><FITID>MA002</FITID><NAME>Interest</NAME></STMTTRN>
</BANKTRANLIST><LEDGERBAL><BALAMT>5100.00</BALAMT><DTASOF>20260131120000</DTASOF></LEDGERBAL></STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
EOF
```

`scenario.yaml` mirrors Task 17's pattern. `expectations.yaml`:

```yaml
import_log:
  count: 1
  source_type: ofx
raw_ofx_transactions:
  row_count: 2
raw_ofx_accounts:
  row_count: 2
core_dim_accounts:
  source_type: ofx
  count: 2
```

- [ ] **Step 2: `ofx_qbo_intuit_export`**

Use the `qbo_intuit_sample.qbo` fixture from Task 15. `expectations.yaml`:

```yaml
import_log:
  source_type: ofx  # taxonomy: QBO is OFX
  source_origin: intuit
raw_ofx_transactions:
  row_count: 2
```

- [ ] **Step 3: `ofx_qbo_bank_export`**

Use `qbo_bank_sample.qbo`. `expectations.yaml`:

```yaml
import_log:
  source_origin: wells_fargo
raw_ofx_transactions:
  row_count: 2
```

- [ ] **Step 4: `ofx_reimport_idempotent`**

`scenario.yaml` runs the import twice and asserts the second run fails. The exact pipeline-DSL syntax depends on the runner — check `tests/scenarios/testing-scenario-comprehensive.md` and an existing scenario's `scenario.yaml`. Pattern:

```yaml
pipeline:
  - import:
      file: inputs/sample.ofx
  - import:
      file: inputs/sample.ofx
      expect_failure: true
      expect_error_substring: "already imported"
```

If the runner doesn't support `expect_failure` directly, add a Python-style scenario test instead (`tests/scenarios/test_ofx_reimport_idempotent.py`) and skip the YAML form.

- [ ] **Step 5: `ofx_missing_institution_metadata`**

Build a fixture without `<FI><ORG>` and with an unknown FID. `expectations.yaml`:

```yaml
pipeline_result:
  failed: true
  error_substring: "could not be derived"
```

Then a second pipeline branch with `--institution "Local Bank"` succeeding.

- [ ] **Step 6: `ofx_cross_source_dedup`**

Two input files: an OFX file and a CSV file with the same transactions. Both import; the matching engine should merge them into one canonical row in `core.fct_transactions`.

```yaml
pipeline:
  - import:
      file: inputs/ofx_side.ofx
  - import:
      file: inputs/tabular_side.csv
      account_name: Checking
  - transforms: true
expectations:
  core_fct_transactions:
    distinct_canonical_count: 2  # 2 source rows merged into 1 canonical (or whatever the actual count is)
  bridge_transactions:
    cross_source_pairs: 2
```

The actual numbers depend on the matcher's behavior — run the scenario, observe what it produces, and write expectations that lock in correct behavior.

- [ ] **Step 7: Run all scenarios**

```bash
make test-scenarios
```

Expected: PASS for all seven.

- [ ] **Step 8: Commit**

```bash
git add tests/scenarios/ofx_multi_account_statement/ \
        tests/scenarios/ofx_qbo_intuit_export/ \
        tests/scenarios/ofx_qbo_bank_export/ \
        tests/scenarios/ofx_reimport_idempotent/ \
        tests/scenarios/ofx_missing_institution_metadata/ \
        tests/scenarios/ofx_cross_source_dedup/
git commit -m "Add six OFX/QBO scenario tests"
```

---

## Task 19: Update `INDEX.md` and archived spec note

Mark this spec as `implemented` in `INDEX.md` and add a superseded pointer to `archived/ofx-import.md`.

**Files:**
- Modify: `docs/specs/INDEX.md`
- Modify: `docs/specs/archived/ofx-import.md`
- Modify: `docs/specs/smart-import-financial.md` (status flip to `implemented`)

- [ ] **Step 1: Add the spec to INDEX.md**

Read the current `INDEX.md`:

```bash
grep -n 'Smart Import' docs/specs/INDEX.md
```

Locate the "Smart Import" section. Add a row for this spec after the existing Smart Import rows:

```markdown
| [Financial Format Import](smart-import-financial.md) | Feature | implemented | OFX/QFX/QBO parity with `smart-import-tabular.md`: shared `import_log` infrastructure, reversible imports, `Database.ingest_dataframe()` writes, magic-byte detection, institution resolution chain, formalized QBO support. Supersedes archived `ofx-import.md`. |
```

- [ ] **Step 2: Add superseded note to `archived/ofx-import.md`**

Edit `docs/specs/archived/ofx-import.md`. Add at the very top (before the existing `# Feature: OFX/QFX Import` line):

```markdown
> **Superseded by [`smart-import-financial.md`](../smart-import-financial.md)** (2026-05-01). This spec describes the original 2024-vintage OFX import, which used a bespoke `OFXLoader` and bypassed the import-batch infrastructure. The replacement spec brings OFX/QFX/QBO onto the same contract surface as smart-import-tabular.

---

```

- [ ] **Step 3: Flip the spec status**

Edit `docs/specs/smart-import-financial.md`:

```markdown
## Status
<!-- draft | ready | in-progress | implemented -->
implemented
```

- [ ] **Step 4: Commit**

```bash
git add docs/specs/INDEX.md docs/specs/archived/ofx-import.md docs/specs/smart-import-financial.md
git commit -m "Mark smart-import-financial as implemented; archive original spec"
```

---

## Task 20: Update README per `shipping.md`

Per `.claude/rules/shipping.md`: roadmap icon flip, "What Works Today" expansion.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find the relevant sections**

```bash
grep -n 'OFX\|QBO\|Roadmap\|What Works' README.md | head -30
```

- [ ] **Step 2: Update the roadmap table**

If there's a roadmap entry for this feature with a `📐` (designed) or `🗓️` (planned) icon, change it to `✅`. If no entry exists, add one to the appropriate section.

- [ ] **Step 3: Expand "What Works Today"**

Find the import-formats subsection. Update text along these lines:

```markdown
### File imports

MoneyBin imports financial data from a wide range of formats:

- **OFX / QFX / QBO** — Bank statements, QuickBooks Web Connect downloads, and Quicken exports. Institution name is auto-detected from the file; multi-account statements are supported. Re-importing the same file is detected and rejected (use `--force` to override). Imports can be reverted with `moneybin import revert <id>`.
- **Tabular** — CSV, TSV, Excel, Parquet, Feather. Smart format detection with confidence tiers; built-in formats for Tiller, Mint, YNAB, Maybe; per-import column mapping; fuzzy account matching.
- **PDF** — W-2 forms (more PDF formats planned).

Example:

```bash
moneybin import ~/Downloads/wells_fargo_2026.qfx
moneybin import ~/Downloads/intuit_export.qbo
moneybin import history --limit 10
moneybin import revert <id>
```

Adjust the wording to match the existing README voice.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "README: document QBO support and OFX revert"
```

---

## Task 21: Pre-push quality pass

Per `shipping.md`, run `/simplify` before the final push.

- [ ] **Step 1: Invoke `/simplify`**

In the Claude Code session running this plan, invoke:

```
/simplify
```

The skill will review changed code and propose fixes for copy-paste, redundant state, and missing validations. Apply the fixes it surfaces, run `make check test`, commit.

- [ ] **Step 2: Final pre-commit check**

```bash
make check test test-scenarios
```

Expected: PASS.

- [ ] **Step 3: Commit any simplify fixes**

```bash
git add -A
git commit -m "Apply /simplify pre-push pass"
```

---

## Task 22: Open PR 2

- [ ] **Step 1: Push**

```bash
git push origin feat/smart-import-financial
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "Smart import financial PR 2: QBO formalization, scenarios, docs" --body "$(cat <<'EOF'
## Summary

PR 2 of 2 for `smart-import-financial.md`. Builds on the infrastructure in PR 1.

- Adds QBO fixtures from two institutions (Intuit-style export, bank-style export).
- Synthetic data generator emits `.qbo` files when `source_type='qbo'`.
- Adds seven scenario tests covering single-account, multi-account, QBO Intuit, QBO bank, re-import idempotency, missing-institution-metadata fallback, and cross-source dedup against tabular.
- Marks the spec as `implemented` in `INDEX.md`; adds superseded note to `archived/ofx-import.md`.
- README "What Works Today" updated with QBO support and OFX revert per shipping.md.

## Test plan

- [ ] `make check test test-scenarios` passes locally
- [ ] All seven new scenarios pass under `make test-scenarios`
- [ ] `/simplify` pass applied
- [ ] CI green
- [ ] Manual: `moneybin import ~/Downloads/some.qbo` works end-to-end and shows up in `moneybin import history`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: After merge — finishing the development branch**

Once PR 2 merges, the branch is done. Use the `superpowers:finishing-a-development-branch` skill to clean up the worktree.
