"""Overlap follows the latest received transaction revision before transforms."""

import pytest

from moneybin.database import Database
from moneybin.investments.source_overlap import investment_source_overlap


@pytest.mark.parametrize("returned_to_original", [False, True])
def test_overlap_uses_current_receipt_after_account_correction(
    db: Database, returned_to_original: bool
) -> None:
    for account in ("old", "corrected"):
        db.execute(
            """
            INSERT INTO raw.manual_investment_transactions (
                source_transaction_id, import_id, account_id, type, trade_date,
                created_by
            ) VALUES (?, 'manual_import', ?, 'buy', '2026-01-01', 'cli')
            """,
            [f"manual_{account}", f"canonical_{account}"],
        )
        db.execute(
            """
            INSERT INTO app.account_links (
                link_id, account_id, ref_kind, ref_value, source_type,
                source_origin, status, decided_by, decided_at
            ) VALUES (?, ?, 'source_native', ?, 'plaid', 'item', 'accepted',
                      'user', CURRENT_TIMESTAMP)
            """,
            [f"link_{account}", f"canonical_{account}", account],
        )
        db.execute(
            """
            INSERT INTO raw.plaid_investment_transactions (
                investment_transaction_id, account_id, transaction_date,
                amount, observation_version, source_origin
            ) VALUES ('same_transaction', ?, '2026-01-01', 100, ?, 'item')
            """,
            [account, f"version_{account}"],
        )
    versions = ["old", "corrected"]
    if returned_to_original:
        versions.append("old")
    for sequence, version in enumerate(versions, start=1):
        db.execute(
            """
            INSERT INTO raw.plaid_investment_transaction_receipts (
                investment_transaction_id, source_origin, source_file,
                observation_version, ingestion_sequence, extracted_at
            ) VALUES ('same_transaction', 'item', ?, ?, ?, '2026-01-02')
            """,
            [f"sync_{sequence}", f"version_{version}", sequence],
        )

    assert investment_source_overlap(db) == [f"canonical_{versions[-1]}"]
    assert db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone() == (2,)
