"""Live overlap evidence for sync warnings and doctor findings."""

from moneybin.database import Database
from moneybin.investments.identity import manual_identity_sql
from moneybin.tables import (
    ACCOUNT_LINKS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_TRANSACTIONS,
)


def investment_source_overlap(db: Database) -> list[str]:
    """Accounts with manual history and Plaid transaction or holdings evidence."""
    rows = db.execute(
        f"""
        WITH manual_identity AS ({manual_identity_sql()}), plaid_evidence AS (
            SELECT account_id, source_origin
            FROM {PLAID_INVESTMENT_TRANSACTIONS.full_name}
            UNION
            SELECT account_id, source_origin
            FROM {PLAID_INVESTMENT_HOLDINGS.full_name}
        )
        SELECT DISTINCT COALESCE(al.account_id, p.account_id) AS account_id
        FROM plaid_evidence AS p
        LEFT JOIN {ACCOUNT_LINKS.full_name} AS al
          ON al.status = 'accepted' AND al.ref_kind = 'source_native'
          AND al.source_type = 'plaid' AND al.source_origin = p.source_origin
          AND al.ref_value = p.account_id
        WHERE EXISTS (
            SELECT 1 FROM manual_identity AS m
            WHERE COALESCE(m.account_id, m.frozen_account_id)
                = COALESCE(al.account_id, p.account_id)
        )
        ORDER BY account_id
        """,  # noqa: S608  # TableRef constants
    ).fetchall()
    return [str(row[0]) for row in rows]
