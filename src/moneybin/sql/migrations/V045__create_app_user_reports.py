"""V045: create app.user_reports.

A saved report is user state, not a derivable artifact: it carries the SQL its
author wrote plus the privacy map derivation produced for it. The class map and
its fingerprint are stored together so a run can tell whether the frozen
classification still describes the schema it was derived from.

Numbered V045 rather than the V041 its spec prescribed — V041 landed as
app.export_destinations three days after that spec merged, and V042-V044 are
claimed by branches in flight. Migration discovery globs and sorts by version
and rejects only duplicates, so a gap is safe.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.user_reports (
    report_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    description VARCHAR,
    query_sql VARCHAR NOT NULL,
    params JSON NOT NULL DEFAULT '[]',
    classes JSON NOT NULL,
    semantics JSON NOT NULL,
    class_downgrades JSON NOT NULL DEFAULT '{}',
    class_fingerprint VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "report_id",
        "Namespaced letter-led saved-report identity ('user:r' + uuid4().hex[:12]); "
        "the audit target and the identity that survives a rename",
    ),
    (
        "name",
        "User-facing report name; the handle every lifecycle operation takes, "
        "resolved to report_id at the service boundary",
    ),
    ("description", "Agent-visible summary of what the report answers"),
    (
        "query_sql",
        "User-authored read-only SELECT carrying $name placeholders bound at run time",
    ),
    (
        "params",
        "Declared ParamSpec list; data_class is derived at save, never declared "
        "by the author",
    ),
    (
        "classes",
        "Derived output-column privacy map, keyed by DuckDB result column name",
    ),
    (
        "semantics",
        "ReportSemantics fields, explicitly unknown for an arbitrary user query",
    ),
    (
        "class_downgrades",
        "Approved downgrades as {column: {from, to, reason}}; 'from' is the derived "
        "class the approval was granted against",
    ),
    (
        "class_fingerprint",
        "Hash over the derivation inputs; a mismatch forces re-resolution before "
        "the run rather than trusting the stored map",
    ),
    (
        "is_active",
        "False archives the report: hidden from the default catalog, still runnable "
        "by name",
    ),
    ("created_at", "Saved-report creation timestamp"),
    ("updated_at", "Last saved-report mutation timestamp"),
]


def migrate(conn: object) -> None:
    """Create app.user_reports and apply catalog comments."""
    logger.debug("V045: CREATE TABLE IF NOT EXISTS app.user_reports")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.user_reports.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.debug("V045: app.user_reports ready")
