"""V042: widen app.security_links.ref_kind for market-feed provider keys.

Phase C.2 of ``investments-price-feeds.md`` binds a Tiingo ticker and a
CoinGecko slug through the existing provider-neutral ``app.security_links``
instead of a text ``ticker`` join, so every provider resolves by one audited
path. That needs two new ``ref_kind`` values.

DuckDB cannot alter a CHECK constraint in place, so rebuild the table (the
V034/V035 idiom): copy -> drop -> recreate with the widened CHECK -> restore ->
drop copy. Fresh installs get the widened CHECK from ``app_security_links.sql``;
this migration is the existing-DB path (database-migration.md dual-path).

The sibling ``app.security_link_decisions.ref_kind`` is deliberately left
narrow. That table is the provider-initiated identity queue — Plaid reports a
security MoneyBin does not recognize and the resolver proposes a merge candidate
for review. A market-feed key runs the other way: MoneyBin mints it from a
security already in its own catalog, so there is no provider claim to
adjudicate and nothing would ever write a decision row for one. Widening it too
would advertise a review path with no producer.
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

_RESTORE_SQL = """
INSERT INTO app.security_links (
    link_id, security_id, ref_kind, ref_value, source_type, status,
    decided_by, decided_at, reversed_at, reversed_by
)
SELECT
    link_id, security_id, ref_kind, ref_value, source_type, status,
    decided_by, decided_at, reversed_at, reversed_by
FROM app.security_links__v042_tmp
"""

_REF_KIND_COMMENT = (
    "COMMENT ON COLUMN app.security_links.ref_kind IS "
    "'Which kind of provider reference this row carries; market-feed keys "
    "(tiingo_ticker, coingecko_slug) bind here so every provider resolves by "
    "one audited path'"
)


def migrate(conn: object) -> None:
    """Rebuild app.security_links with the widened ref_kind CHECK. Idempotent."""
    rows: list[tuple[str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT constraint_text FROM duckdb_constraints()
        WHERE schema_name = 'app' AND table_name = 'security_links'
          AND constraint_type = 'CHECK'
        """
    ).fetchall()
    if any("tiingo_ticker" in text for (text,) in rows):
        logger.debug("V042: ref_kind already admits market-feed keys; skipping")
        return
    logger.debug("V042: rebuilding app.security_links with widened ref_kind CHECK")
    conn.execute(  # type: ignore[union-attr]
        "CREATE TABLE app.security_links__v042_tmp AS SELECT * FROM app.security_links"
    )
    conn.execute("DROP TABLE app.security_links")  # type: ignore[union-attr]
    conn.execute(_NEW_SECURITY_LINKS_SQL)  # type: ignore[union-attr]
    conn.execute(_RESTORE_SQL)  # type: ignore[union-attr]
    conn.execute("DROP TABLE app.security_links__v042_tmp")  # type: ignore[union-attr]
    conn.execute(_REF_KIND_COMMENT)  # type: ignore[union-attr]
