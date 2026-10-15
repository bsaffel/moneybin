"""First holdings evidence follows per-account receipt ingestion order."""

from pathlib import Path

import yaml

from moneybin.connectors.sync_models import PlaidSyncDataResponse
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from tests.moneybin.db_helpers import install_prep_view


def test_first_snapshot_is_account_scoped_and_stable_when_timestamps_tie(
    db: Database,
) -> None:
    fixture = Path(
        "tests/moneybin/test_extractors/fixtures/plaid_investments_sync_response.yaml"
    )
    payload = PlaidSyncDataResponse.model_validate(yaml.safe_load(fixture.read_text()))
    provider = PlaidExtractor(db)
    first = payload.model_copy(deep=True)
    first.investment_holdings = [
        h for h in first.investment_holdings if h.account_id == "acc_1"
    ]
    provider.load(first, job_id="z-first")
    provider.load(payload, job_id="a-second")
    provider.load(first, job_id="z-first")
    install_prep_view(db, "stg_plaid__investment_transactions")
    install_prep_view(db, "stg_plaid__investment_holdings_snapshots")
    install_prep_view(db, "int_plaid__opening_positions")
    anchors = db.execute("""
        SELECT DISTINCT source_account_key, source_file
        FROM prep.int_plaid__opening_positions ORDER BY source_account_key
    """).fetchall()
    assert ("acc_1", "sync_z-first") in anchors
    later_accounts = {h.account_id for h in payload.investment_holdings} - {"acc_1"}
    assert later_accounts
    assert all((account, "sync_a-second") in anchors for account in later_accounts)


def test_receipt_staging_exposes_ingestion_sequence(db: Database) -> None:
    install_prep_view(db, "stg_plaid__investment_holdings_snapshots")
    columns = db.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'prep' AND table_name = 'stg_plaid__investment_holdings_snapshots'
    """).fetchall()
    assert ("ingestion_sequence",) in columns
