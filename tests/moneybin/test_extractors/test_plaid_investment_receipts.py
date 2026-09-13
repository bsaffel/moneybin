"""Immutable transaction evidence and replay-stable delivery ordering."""

from pathlib import Path
from typing import Literal
from unittest.mock import patch

import polars as pl
import pytest
import yaml
from sqlglot import exp

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor


@pytest.fixture
def receipt_payload() -> SyncDataResponse:
    fixture = Path(__file__).parent / "fixtures/plaid_investments_sync_response.yaml"
    return SyncDataResponse.model_validate(yaml.safe_load(fixture.read_text()))


def test_transaction_delivery_keeps_immutable_versions_and_replay_order(
    db: Database, receipt_payload: SyncDataResponse
) -> None:
    columns = db.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema = 'raw'
             AND table_name = 'plaid_investment_transactions'"""
    ).fetchall()
    assert ("observation_version",) in columns
    assert ("source_file",) not in columns
    provider = PlaidExtractor(db)
    changed = receipt_payload.model_copy(deep=True)
    changed.investment_transactions[0].transaction_name = "Corrected description"
    provider.load(receipt_payload, job_id="z-first")
    provider.load(changed, job_id="a-second")
    provider.load(receipt_payload, job_id="m-third")
    native_id = receipt_payload.investment_transactions[0].investment_transaction_id
    revisions = db.execute(
        """SELECT observation_version FROM raw.plaid_investment_transactions
           WHERE investment_transaction_id = ?""",
        [native_id],
    ).fetchall()
    assert len(revisions) == 2
    assert all(
        str(row[0]).startswith("plaid_") and len(str(row[0])) == 22 for row in revisions
    )
    receipts = db.execute(
        """SELECT source_file, observation_version, ingestion_sequence
           FROM raw.plaid_investment_transaction_receipts
           WHERE investment_transaction_id = ?
           ORDER BY extracted_at, ingestion_sequence""",
        [native_id],
    ).fetchall()
    assert [row[0] for row in receipts] == [
        "sync_z-first",
        "sync_a-second",
        "sync_m-third",
    ]
    assert receipts[0][1] == receipts[2][1] != receipts[1][1]
    provider.load(changed, job_id="a-second")
    assert (
        db.execute(
            """SELECT source_file, observation_version, ingestion_sequence
           FROM raw.plaid_investment_transaction_receipts
           WHERE investment_transaction_id = ?
           ORDER BY extracted_at, ingestion_sequence""",
            [native_id],
        ).fetchall()
        == receipts
    )


def test_holdings_receipt_sequence_survives_replay(
    db: Database, receipt_payload: SyncDataResponse
) -> None:
    columns = db.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema = 'raw'
             AND table_name = 'plaid_investment_holdings_snapshots'"""
    ).fetchall()
    assert ("ingestion_sequence",) in columns
    provider = PlaidExtractor(db)
    provider.load(receipt_payload, job_id="z-first")
    provider.load(receipt_payload, job_id="a-second")
    before = db.execute(
        """SELECT source_origin, source_file, ingestion_sequence
           FROM raw.plaid_investment_holdings_snapshots
           ORDER BY source_origin, ingestion_sequence"""
    ).fetchall()
    provider.load(receipt_payload, job_id="z-first")
    assert (
        db.execute(
            """SELECT source_origin, source_file, ingestion_sequence
           FROM raw.plaid_investment_holdings_snapshots
           ORDER BY source_origin, ingestion_sequence"""
        ).fetchall()
        == before
    )
    for origin in {row[0] for row in before}:
        assert [row[1] for row in before if row[0] == origin] == [
            "sync_z-first",
            "sync_a-second",
        ]


def test_current_staging_uses_final_delivery_at_equal_extraction_times(
    db: Database, receipt_payload: SyncDataResponse
) -> None:
    provider = PlaidExtractor(db)
    changed = receipt_payload.model_copy(deep=True)
    changed.investment_transactions[0].transaction_name = "Corrected description"
    for payload, job in [
        (receipt_payload, "z"),
        (changed, "a"),
        (receipt_payload, "m"),
    ]:
        provider.load(payload, job_id=job)
    path = Path(
        "src/moneybin/sqlmesh/models/prep/stg_plaid__investment_transactions.sql"
    )
    # The standard SQLMesh parser handles MODEL metadata before rendering DuckDB SQL.
    from sqlmesh.core.dialect import parse as parse_model

    query = next(
        node for node in parse_model(path.read_text()) if isinstance(node, exp.Select)
    )
    sql = query.sql(dialect="duckdb")
    rows = db.execute(sql).fetchall()
    assert len(rows) == len(receipt_payload.investment_transactions)
    columns = [column[0] for column in db.execute(sql).description]
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    selected = next(
        row
        for row in records
        if row["investment_transaction_id"]
        == receipt_payload.investment_transactions[0].investment_transaction_id
    )
    assert selected["source_file"] == "sync_m"
    assert (
        selected["description"]
        == receipt_payload.investment_transactions[0].transaction_name
    )
    assert selected["observation_version"].startswith("plaid_")
    assert selected["trade_date_basis"] in {"explicit", "posting_fallback"}


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_failed_receipt_write_rolls_back_new_revision(
    db: Database, receipt_payload: SyncDataResponse, failure: type[BaseException]
) -> None:
    provider = PlaidExtractor(db)
    provider.load(receipt_payload, job_id="first")
    changed = receipt_payload.model_copy(deep=True)
    changed.investment_transactions[0].transaction_name = "Corrected description"
    before = db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall()
    original = db.ingest_dataframe

    def fail_receipt(
        table: str,
        df: pl.DataFrame,
        *,
        on_conflict: Literal["insert", "replace", "upsert", "ignore"] = "insert",
    ) -> int:
        if table == "raw.plaid_investment_transaction_receipts":
            raise failure("injected receipt failure")
        return original(table, df, on_conflict=on_conflict)

    with patch.object(db, "ingest_dataframe", side_effect=fail_receipt):
        with pytest.raises(failure, match="injected receipt failure"):
            provider.load(changed, job_id="failed")
    assert (
        db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall()
        == before
    )
    assert db.execute(
        """SELECT COUNT(*) FROM raw.plaid_investment_transaction_receipts
           WHERE source_file = 'sync_failed'"""
    ).fetchone() == (0,)
