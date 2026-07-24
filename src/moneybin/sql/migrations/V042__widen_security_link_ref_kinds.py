"""V042: widen the ref_kind CHECK on both security-link tables for feed keys.

Phase C.2 of ``investments-price-feeds.md`` binds a Tiingo ticker and a
CoinGecko slug through the existing provider-neutral ``app.security_links``
instead of a text ``ticker`` join, so every provider resolves by one audited
path. That needs two new ``ref_kind`` values on the binding table.

Because a ticker is not a unique identifier (BHP on NYSE vs ASX; GOOG vs GOOGL;
recycled symbols), deriving that feed key from the catalog can be ambiguous. The
same two ``ref_kind`` values are therefore admitted on the review queue
``app.security_link_decisions`` as well: a near-certain derivation binds
silently, but an ambiguous one is queued there for review rather than acted on
("Magic stays visible", design-principles.md). Widening only the binding table
would leave the queue unable to hold the very rows the resolver needs to defer.

DuckDB cannot alter a CHECK constraint in place, so each table is rebuilt on the
V034/V035 idiom: copy -> drop -> recreate with the widened CHECK -> restore ->
drop copy. Fresh installs get the widened CHECKs from the schema DDL
(``app_security_links.sql`` / ``app_security_link_decisions.sql``); this
migration is the existing-DB path (database-migration.md dual-path). Each table
is guarded independently, so the migration is idempotent and safe on a database
where only one of the two has already been widened.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_NEW_SECURITY_LINKS_SQL = """
CREATE TABLE app.security_links (
    link_id VARCHAR NOT NULL,
    security_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id', 'tiingo_ticker', 'coingecko_slug')),
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

_RESTORE_LINKS_SQL = """
INSERT INTO app.security_links (
    link_id, security_id, ref_kind, ref_value, source_type, status,
    decided_by, decided_at, reversed_at, reversed_by
)
SELECT
    link_id, security_id, ref_kind, ref_value, source_type, status,
    decided_by, decided_at, reversed_at, reversed_by
FROM app.security_links__v042_tmp
"""

_LINKS_REF_KIND_COMMENT = (
    "COMMENT ON COLUMN app.security_links.ref_kind IS "
    "'Which kind of provider reference this row carries; market-feed keys "
    "(tiingo_ticker, coingecko_slug) bind here so every provider resolves by "
    "one audited path'"
)

_NEW_SECURITY_LINK_DECISIONS_SQL = """
CREATE TABLE app.security_link_decisions (
    decision_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id', 'tiingo_ticker', 'coingecko_slug')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    provider_ticker VARCHAR,
    provider_name VARCHAR,
    candidate_security_id VARCHAR NOT NULL,
    confidence_score DECIMAL(5, 4),
    match_signals JSON,
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

_RESTORE_DECISIONS_SQL = """
INSERT INTO app.security_link_decisions (
    decision_id, ref_kind, ref_value, source_type, provider_ticker,
    provider_name, candidate_security_id, confidence_score, match_signals,
    status, decided_by, match_reason, decided_at, reversed_at, reversed_by
)
SELECT
    decision_id, ref_kind, ref_value, source_type, provider_ticker,
    provider_name, candidate_security_id, confidence_score, match_signals,
    status, decided_by, match_reason, decided_at, reversed_at, reversed_by
FROM app.security_link_decisions__v042_tmp
"""

_DECISIONS_REF_KIND_COMMENT = (
    "COMMENT ON COLUMN app.security_link_decisions.ref_kind IS "
    "'Which kind of provider reference is under review; an ambiguous market-feed "
    "key derivation (tiingo_ticker, coingecko_slug) is queued here rather than "
    "bound silently, so every uncertain inference surfaces for review'"
)


def _ref_kind_admits_feed_keys(conn: object, table: str) -> bool:
    """True once ``app.<table>``'s ref_kind CHECK admits the market-feed keys."""
    rows: list[tuple[str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT constraint_text FROM duckdb_constraints()
        WHERE schema_name = 'app' AND table_name = ?
          AND constraint_type = 'CHECK'
        """,
        [table],
    ).fetchall()
    return any("tiingo_ticker" in text for (text,) in rows)


def _widen_security_links(conn: object) -> None:
    if _ref_kind_admits_feed_keys(conn, "security_links"):
        logger.debug("V042: security_links.ref_kind already widened; skipping")
        return
    logger.debug("V042: rebuilding app.security_links with widened ref_kind CHECK")
    conn.execute(  # type: ignore[union-attr]
        "CREATE TABLE app.security_links__v042_tmp AS SELECT * FROM app.security_links"
    )
    conn.execute("DROP TABLE app.security_links")  # type: ignore[union-attr]
    conn.execute(_NEW_SECURITY_LINKS_SQL)  # type: ignore[union-attr]
    conn.execute(_RESTORE_LINKS_SQL)  # type: ignore[union-attr]
    conn.execute("DROP TABLE app.security_links__v042_tmp")  # type: ignore[union-attr]
    conn.execute(_LINKS_REF_KIND_COMMENT)  # type: ignore[union-attr]


def _widen_security_link_decisions(conn: object) -> None:
    if _ref_kind_admits_feed_keys(conn, "security_link_decisions"):
        logger.debug("V042: security_link_decisions.ref_kind already widened; skipping")
        return
    logger.debug(
        "V042: rebuilding app.security_link_decisions with widened ref_kind CHECK"
    )
    conn.execute(  # type: ignore[union-attr]
        "CREATE TABLE app.security_link_decisions__v042_tmp AS "
        "SELECT * FROM app.security_link_decisions"
    )
    conn.execute("DROP TABLE app.security_link_decisions")  # type: ignore[union-attr]
    conn.execute(_NEW_SECURITY_LINK_DECISIONS_SQL)  # type: ignore[union-attr]
    conn.execute(_RESTORE_DECISIONS_SQL)  # type: ignore[union-attr]
    conn.execute(  # type: ignore[union-attr]
        "DROP TABLE app.security_link_decisions__v042_tmp"
    )
    conn.execute(_DECISIONS_REF_KIND_COMMENT)  # type: ignore[union-attr]


def migrate(conn: object) -> None:
    """Widen ref_kind on both security-link tables for market-feed keys. Idempotent."""
    _widen_security_links(conn)
    _widen_security_link_decisions(conn)
