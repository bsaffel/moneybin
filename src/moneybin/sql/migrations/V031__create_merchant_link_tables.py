"""V031: create app.merchant_links and app.merchant_link_decisions.

The same DDL also ships in ``src/moneybin/sql/schema/app_merchant_links.sql``
and ``src/moneybin/sql/schema/app_merchant_link_decisions.sql`` which
``init_schemas`` runs on every Database open. Fresh installs get both tables
at open time; pre-existing databases get them via this migration.
``CREATE TABLE IF NOT EXISTS`` keeps both paths idempotent.

Pure additive DDL — no backfill, no reshape.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_MERCHANT_LINKS_SQL = """
CREATE TABLE IF NOT EXISTS app.merchant_links (
    link_id VARCHAR NOT NULL,
    merchant_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL
        CHECK (ref_kind IN ('merchant_entity_id')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL
        CHECK (status IN ('accepted', 'reversed')),
    decided_by VARCHAR NOT NULL
        CHECK (decided_by IN ('auto', 'user', 'system')),
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user', 'system')),
    PRIMARY KEY (link_id)
)
"""

_CREATE_MERCHANT_LINK_DECISIONS_SQL = """
CREATE TABLE IF NOT EXISTS app.merchant_link_decisions (
    decision_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL
        CHECK (ref_kind IN ('merchant_entity_id')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    provider_merchant_name VARCHAR,
    candidate_merchant_id VARCHAR NOT NULL,
    confidence_score DECIMAL(5, 4),
    match_signals VARCHAR,
    status VARCHAR NOT NULL
        CHECK (status IN ('pending', 'accepted', 'rejected', 'reversed')),
    decided_by VARCHAR NOT NULL
        CHECK (decided_by IN ('auto', 'user')),
    match_reason VARCHAR,
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user')),
    PRIMARY KEY (decision_id)
)
"""

# (column_name, comment_text) — applied as COMMENT ON COLUMN after CREATE.
# COMMENT ON COLUMN replaces existing comments, so this is safe to re-run.
_MERCHANT_LINKS_COLUMN_COMMENTS: list[tuple[str, str]] = [
    ("link_id", "uuid4[:12] primary key for this binding"),
    ("merchant_id", "canonical merchant this provider id maps to"),
    ("ref_kind", "which kind of provider reference this row carries"),
    ("ref_value", "the provider's stable merchant id (opaque, non-PII)"),
    ("source_type", "issuing provider: plaid (future: mx, simplefin, ...)"),
    ("status", "accepted (live) or reversed (undone)"),
    (
        "decided_by",
        "domain actor: auto, user (human OR agent ratification), or system",
    ),
    ("decided_at", "when this binding was decided"),
    ("reversed_at", "when reversed; NULL while accepted"),
    ("reversed_by", "domain actor who reversed; NULL while accepted"),
]

_MERCHANT_LINK_DECISIONS_COLUMN_COMMENTS: list[tuple[str, str]] = [
    ("decision_id", "uuid4[:12] primary key"),
    ("ref_kind", "'merchant_entity_id'"),
    ("ref_value", "the unbound provider id under review"),
    ("source_type", "issuing provider"),
    (
        "provider_merchant_name",
        "provider's merchant_name (reviewer display + match basis)",
    ),
    ("candidate_merchant_id", "existing merchant proposed as the binding target"),
    ("confidence_score", "informational; fuzzy matches always go to review"),
    (
        "match_signals",
        "JSON: which signal fired + value (per match_decisions convention)",
    ),
    ("status", "pending | accepted | rejected | reversed"),
    ("decided_by", "auto | user"),
    ("match_reason", "short human reason (e.g. signal name)"),
    ("decided_at", "when the decision was made"),
    ("reversed_at", "when reversed; NULL while pending/accepted/rejected"),
    ("reversed_by", "domain actor who reversed; NULL until reversed"),
]


def migrate(conn: object) -> None:
    """Create app.merchant_links + app.merchant_link_decisions. Idempotent."""
    logger.info("V031: CREATE TABLE IF NOT EXISTS app.merchant_links")
    conn.execute(_CREATE_MERCHANT_LINKS_SQL)  # type: ignore[union-attr]

    for column, comment in _MERCHANT_LINKS_COLUMN_COMMENTS:
        # COMMENT ON COLUMN does not accept parameterized values; inline a
        # single-quoted literal with standard SQL escaping (double the
        # single quote). column names come from the static _*_COLUMN_COMMENTS
        # list, not user input.
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.merchant_links.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.info("V031: CREATE TABLE IF NOT EXISTS app.merchant_link_decisions")
    conn.execute(_CREATE_MERCHANT_LINK_DECISIONS_SQL)  # type: ignore[union-attr]

    for column, comment in _MERCHANT_LINK_DECISIONS_COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.merchant_link_decisions.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.info("V031: app.merchant_links + app.merchant_link_decisions ready")
