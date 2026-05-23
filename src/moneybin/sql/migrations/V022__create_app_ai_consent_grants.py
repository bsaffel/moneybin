"""V022: create app.ai_consent_grants.

The consent ledger table. One row per granted (feature_category,
backend) authorization. The same DDL ships in
``src/moneybin/sql/schema/app_ai_consent_grants.sql`` which
``init_schemas`` runs on every Database open — fresh installs get the
table at open time; pre-existing databases get it via this migration.
``CREATE TABLE IF NOT EXISTS`` keeps both paths idempotent.

Pure additive DDL — no backfill, no reshape.
"""

from __future__ import annotations

import logging

from sqlglot import exp

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.ai_consent_grants (
    grant_id VARCHAR PRIMARY KEY,
    feature_category VARCHAR NOT NULL,
    backend VARCHAR NOT NULL,
    consent_mode VARCHAR NOT NULL CHECK (consent_mode IN ('persistent', 'one-time')),
    granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at TIMESTAMP,
    grant_prompt TEXT NOT NULL
)
"""

# (column_name, comment_text) — applied as COMMENT ON COLUMN after CREATE.
# COMMENT ON COLUMN replaces existing comments, so this is safe to re-run.
_COLUMN_COMMENTS: list[tuple[str, str]] = [
    ("grant_id", "Truncated UUID4 (uuid4().hex[:12]) per identifiers.md strategy 3"),
    (
        "feature_category",
        "AI flow category: mcp-data-sharing, smart-import-parsing, "
        "ml-categorization, matching-overview (free string; per-tool granularity deferred)",
    ),
    (
        "backend",
        "AI backend this consent applies to (e.g. anthropic, openai, ollama); "
        "consent is per (feature_category, backend)",
    ),
    (
        "consent_mode",
        "persistent: survives sessions until revoked; one-time: single authorized use",
    ),
    ("granted_at", "When the user granted this consent"),
    (
        "revoked_at",
        "When revoked; NULL while active. Revoked rows retained for audit history.",
    ),
    (
        "grant_prompt",
        "Exact consent text the user saw and agreed to; not surfaced in read payloads",
    ),
]


def migrate(conn: object) -> None:
    """Create app.ai_consent_grants + apply column comments. Idempotent."""
    logger.info("V022: CREATE TABLE IF NOT EXISTS app.ai_consent_grants")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        # COMMENT ON COLUMN does not accept parameterized values; inline a
        # single-quoted literal with standard SQL escaping (double the single
        # quote). Column names are from the static list above, but quote the
        # identifier regardless — security.md requires double-quoting all
        # interpolated identifiers as defense in depth.
        escaped = comment.replace("'", "''")
        safe_column = exp.to_identifier(column, quoted=True).sql("duckdb")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.ai_consent_grants.{safe_column} "  # noqa: S608  # quoted identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.info("V022: app.ai_consent_grants ready")
