"""V035: add created_by provenance to app.securities.

DuckDB cannot ADD COLUMN with a CHECK constraint, so rebuild the table
(V034 idiom): copy -> drop -> recreate with created_by -> restore -> drop copy.
Fresh installs get the column from app_securities.sql; this migration is the
existing-DB path (database-migration.md dual-path).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_NEW_SECURITIES_SQL = """
CREATE TABLE app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    security_type VARCHAR NOT NULL CHECK (security_type IN ('equity', 'etf', 'mutual_fund', 'bond', 'crypto', 'cash', 'other')),
    ticker VARCHAR,
    exchange VARCHAR,
    cusip VARCHAR,
    isin VARCHAR,
    figi VARCHAR,
    coingecko_id VARCHAR,
    is_cash_equivalent BOOLEAN,
    cost_basis_method VARCHAR CHECK (cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')),
    currency_code VARCHAR NOT NULL DEFAULT 'USD',
    created_by VARCHAR NOT NULL DEFAULT 'user' CHECK (created_by IN ('user', 'plaid')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_RESTORE_SQL = """
INSERT INTO app.securities (
    security_id, name, security_type, ticker, exchange, cusip, isin, figi,
    coingecko_id, is_cash_equivalent, cost_basis_method, currency_code,
    created_by, created_at, updated_at
)
SELECT
    security_id, name, security_type, ticker, exchange, cusip, isin, figi,
    coingecko_id, is_cash_equivalent, cost_basis_method, currency_code,
    'user', created_at, updated_at
FROM app.__v035_securities_tmp
"""

_CREATED_BY_COMMENT = (
    "COMMENT ON COLUMN app.securities.created_by IS "
    "'Catalog provenance: user-authored vs provider-minted (plaid); "
    "gates resolver attribute refresh'"
)


def migrate(conn: object) -> None:
    """Rebuild app.securities with created_by. Idempotent."""
    cols: list[tuple[str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'securities'
        """
    ).fetchall()
    existing = {c[0] for c in cols}
    if "created_by" in existing:
        logger.debug("V035: created_by already present; skipping")
        return
    logger.debug("V035: rebuilding app.securities with created_by")
    conn.execute(  # type: ignore[union-attr]
        "CREATE TABLE app.__v035_securities_tmp AS SELECT * FROM app.securities"
    )
    conn.execute("DROP TABLE app.securities")  # type: ignore[union-attr]
    conn.execute(_NEW_SECURITIES_SQL)  # type: ignore[union-attr]
    conn.execute(_RESTORE_SQL)  # type: ignore[union-attr]
    conn.execute("DROP TABLE app.__v035_securities_tmp")  # type: ignore[union-attr]
    conn.execute(_CREATED_BY_COMMENT)  # type: ignore[union-attr]
