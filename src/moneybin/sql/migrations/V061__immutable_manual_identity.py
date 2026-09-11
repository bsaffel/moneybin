"""V061: freeze manual observations and admit their audited Security routes."""

from __future__ import annotations

import logging

import duckdb

logger = logging.getLogger(__name__)

_CREATE_LINKS = """
CREATE TABLE app.security_links (
    link_id VARCHAR NOT NULL PRIMARY KEY,
    security_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL CHECK (ref_kind IN (
        'plaid_security_id', 'institution_security_id', 'tiingo_ticker',
        'coingecko_slug', 'manual_investment_transaction_id')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('accepted', 'reversed')),
    decided_by VARCHAR NOT NULL CHECK (decided_by IN ('auto', 'user', 'system')),
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user', 'system'))
)
"""


def _preflight_account_activation(conn: object) -> None:
    historical = conn.execute(  # type: ignore[attr-defined]
        """
        SELECT DISTINCT t.account_id
        FROM raw.manual_investment_transactions AS t
        JOIN app.account_link_decisions AS d
          ON d.provisional_account_id = t.account_id
        WHERE d.status = 'accepted' AND d.reversed_at IS NULL
        """
    ).fetchall()
    if (
        not historical
        or not conn.execute(  # type: ignore[attr-defined]
            "SELECT COUNT(*) FROM app.lot_selections"
        ).fetchone()[0]
    ):
        return
    prerequisite = (
        "Identity upgrade refused: historical Account routing may change stored "
        "lot selections. Before retrying, use the prior version to reconcile "
        "the affected selections and Account merges; this upgrade cannot prove "
        "a complete selection mapping and has changed no observations or routes."
    )
    try:
        selections = conn.execute(  # type: ignore[attr-defined]
            """
            SELECT t.account_id, l.account_id
            FROM app.lot_selections AS ls
            LEFT JOIN core.fct_investment_transactions AS t
              ON t.investment_transaction_id = ls.investment_transaction_id
            LEFT JOIN core.fct_investment_lots AS l ON l.lot_id = ls.lot_id
            """
        ).fetchall()
    except (duckdb.CatalogException, duckdb.BinderException):
        raise RuntimeError(prerequisite) from None
    affected: set[str] = {row[0] for row in historical}
    edges = conn.execute(  # type: ignore[attr-defined]
        """
        SELECT provisional_account_id, candidate_account_id
        FROM app.account_link_decisions
        WHERE status = 'accepted' AND reversed_at IS NULL
        """
    ).fetchall()
    # Activation pools positions at every surviving node along a merge chain.
    while True:
        reachable: set[str] = {target for source, target in edges if source in affected}
        if reachable <= affected:
            break
        affected.update(reachable)
    if any(
        account is None
        or lot_account is None
        or account in affected
        or lot_account in affected
        for account, lot_account in selections
    ):
        raise RuntimeError(prerequisite)


def migrate(conn: object) -> None:
    """Refuse unsafe historical activation, then widen Links in the runner txn."""
    _preflight_account_activation(conn)
    constraints = conn.execute(  # type: ignore[attr-defined]
        """
        SELECT constraint_text FROM duckdb_constraints()
        WHERE schema_name = 'app' AND table_name = 'security_links'
          AND constraint_type = 'CHECK'
        """
    ).fetchall()
    if any("manual_investment_transaction_id" in text for (text,) in constraints):
        return
    logger.debug("V061: widen Security Links for immutable manual routing")
    conn.execute(  # type: ignore[attr-defined]
        "CREATE TABLE app.security_links__v061_tmp AS SELECT * FROM app.security_links"
    )
    conn.execute("DROP TABLE app.security_links")  # type: ignore[attr-defined]
    conn.execute(_CREATE_LINKS)  # type: ignore[attr-defined]
    conn.execute(  # type: ignore[attr-defined]
        "INSERT INTO app.security_links SELECT * FROM app.security_links__v061_tmp"
    )
    conn.execute("DROP TABLE app.security_links__v061_tmp")  # type: ignore[attr-defined]
