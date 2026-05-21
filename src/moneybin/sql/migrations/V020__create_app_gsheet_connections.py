"""V020: create app.gsheet_connections.

The central state table for the gsheet connector. One row per connected
(Google Sheets tab, adapter) — identity, adapter choice, pinned column
mapping, drift signature, and health status all live here. Subsequent
phases (Task 19 repository, Task 20 connection service, Task 21 pull
service) all write to / read from this table.

The same DDL also ships in ``src/moneybin/sql/schema/app_gsheet_connections.sql``
which ``init_schemas`` runs on every Database open. Fresh installs get
the table at open time; pre-existing databases get it via this
migration. ``CREATE TABLE IF NOT EXISTS`` keeps both paths idempotent.

Pure additive DDL — no backfill, no reshape. The migration is a no-op
when the schema file has already created the table.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.gsheet_connections (
    connection_id VARCHAR PRIMARY KEY,
    spreadsheet_id VARCHAR NOT NULL,
    sheet_gid INTEGER NOT NULL,
    sheet_name VARCHAR NOT NULL,
    workbook_name VARCHAR NOT NULL,
    adapter VARCHAR NOT NULL CHECK (adapter IN ('transactions', 'seed')),
    account_id VARCHAR,
    account_name VARCHAR,
    column_mapping JSON NOT NULL,
    header_signature JSON NOT NULL,
    date_format VARCHAR,
    sign_convention VARCHAR,
    number_format VARCHAR,
    skip_rows INTEGER NOT NULL DEFAULT 0,
    skip_trailing_patterns JSON,
    status VARCHAR NOT NULL DEFAULT 'healthy'
        CHECK (status IN ('healthy', 'auth_expired', 'unreachable',
                          'drift_detected', 'rate_limited', 'disconnected')),
    last_pull_at TIMESTAMP,
    last_pull_import_id VARCHAR,
    last_success_at TIMESTAMP,
    last_drift_reason TEXT,
    consecutive_failure_count INTEGER NOT NULL DEFAULT 0,
    alias VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (spreadsheet_id, sheet_gid),
    UNIQUE (alias)
)
"""

# (column_name, comment_text) — applied as COMMENT ON COLUMN after CREATE.
# COMMENT ON COLUMN replaces existing comments, so this is safe to re-run.
_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "connection_id",
        "Truncated UUID (uuid4().hex[:12]) per identifiers.md strategy 3; "
        "stable across renames",
    ),
    (
        "spreadsheet_id",
        "Google Sheets workbook id (from URL); shared across tabs in the same workbook",
    ),
    (
        "sheet_gid",
        "Numeric tab id (URL fragment gid=); stable across sheet renames",
    ),
    (
        "sheet_name",
        "Tab name at last successful pull; informational only — may drift, "
        "not used for joins",
    ),
    (
        "workbook_name",
        "Workbook title at last successful pull; informational",
    ),
    (
        "adapter",
        "Adapter target: 'transactions' (Tiller-style ledger -> "
        "raw.tabular_transactions) or 'seed' (catch-all -> raw.gsheet_seeds). "
        "v1 values; future values add as new adapters ship.",
    ),
    (
        "account_id",
        "FK to dim_accounts (transactions adapter only); NULL for seed "
        "adapter (multi-account by design)",
    ),
    (
        "account_name",
        "As provided via --account-name at connect time; denormalized for display",
    ),
    (
        "column_mapping",
        "{source_header: dest_field} pinned at connect/reconnect time; the "
        "contract for subsequent pulls",
    ),
    (
        "header_signature",
        "Ordered list of source headers at connect/reconnect time; drift "
        "detection baseline",
    ),
    (
        "date_format",
        "Pinned strftime format (e.g. '%m/%d/%Y'); NULL means auto-detect on each pull",
    ),
    (
        "sign_convention",
        "One of: negative_is_expense, negative_is_income, "
        "split_debit_credit, all_positive; NULL = auto",
    ),
    (
        "number_format",
        "One of: us, european, swiss, zero_decimal; NULL = auto",
    ),
    (
        "skip_rows",
        "Rows to skip before the header row (Tiller sheets often have a banner row)",
    ),
    (
        "skip_trailing_patterns",
        "Optional list of regex strings flagging trailing junk rows; NULL = "
        "use default patterns, [] = none, ['^Total'] = custom",
    ),
    (
        "status",
        "Connection health; drives pre-refresh hook decisions and the "
        "system_status gsheet block",
    ),
    (
        "last_pull_at",
        "Timestamp of most recent pull attempt (success or failure)",
    ),
    (
        "last_pull_import_id",
        "FK to raw.import_log.import_id for the most recent attempt; NULL "
        "before first pull",
    ),
    (
        "last_success_at",
        "Timestamp of most recent pull that ingested cleanly; NULL before "
        "first successful pull",
    ),
    (
        "last_drift_reason",
        "Human-readable explanation populated when status='drift_detected'",
    ),
    (
        "consecutive_failure_count",
        "Number of consecutive failed pulls since the last success; resets "
        "to 0 on success",
    ),
    (
        "alias",
        "User-supplied slug; required for adapter='seed' (becomes view name "
        "raw.gsheet_<alias>); NULL for adapter='transactions'. Globally "
        "unique across all connections.",
    ),
    (
        "created_at",
        "Connection-row creation timestamp",
    ),
    (
        "updated_at",
        "Last mutation timestamp (refreshed by GSheetConnectionsRepo on "
        "every audited write)",
    ),
]


def migrate(conn: object) -> None:
    """Create app.gsheet_connections + apply column comments. Idempotent."""
    logger.info("V020: CREATE TABLE IF NOT EXISTS app.gsheet_connections")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        # COMMENT ON COLUMN does not accept parameterized values; inline a
        # single-quoted literal with standard SQL escaping (double the
        # single quote). column names come from the static _COLUMN_COMMENTS
        # list, not user input.
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.gsheet_connections.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.info("V020: app.gsheet_connections ready")
