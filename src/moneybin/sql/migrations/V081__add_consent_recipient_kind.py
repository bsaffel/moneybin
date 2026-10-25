"""V081: record AI host recipients, terms version and approval route on consent grants.

Appended last because DuckDB's ALTER TABLE ADD COLUMN always appends, and
init_schemas() must produce the same column order as an upgraded database.
Existing rows are AI backend grants and take the 'ai_backend' default. DuckDB cannot ADD COLUMN with NOT NULL or CHECK, so ConsentRepo validates the value.
"""

from __future__ import annotations

_COLUMNS = (
    (
        "recipient_kind",
        "VARCHAR DEFAULT 'ai_backend'",
        "Kind of recipient the backend column names: ai_backend (a model vendor, set by privacy grant --backend) or ai_host (the AI app reading the output, keyed transport-class:name such as local:claude-code); consent is per (feature_category, backend, recipient_kind)",
    ),
    (
        "terms_version",
        "VARCHAR",
        "Vendor terms version the user consented under (ai_host rows only; NULL for ai_backend); a release that changes the shipped terms stops honoring the grant",
    ),
    (
        "approved_via",
        "VARCHAR",
        "Surface that recorded the consent, e.g. cli_prompt (ai_host rows only; NULL for ai_backend)",
    ),
)


def migrate(conn: object) -> None:
    """Add the three columns with comments. Idempotent."""
    for column, ddl, comment in _COLUMNS:
        conn.execute(  # type: ignore[attr-defined]
            f"ALTER TABLE app.ai_consent_grants ADD COLUMN IF NOT EXISTS {column} {ddl}"
        )
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[attr-defined]
            f"COMMENT ON COLUMN app.ai_consent_grants.{column} IS '{escaped}'"
        )
