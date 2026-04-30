"""Loads hand-authored fixtures into raw schema tables for scenarios.

Supports CSV → ``raw.tabular_transactions`` and OFX-shaped CSV →
``raw.ofx_transactions`` so dedup scenarios can drive both sources from
deterministic fixtures without invoking the synthetic generator. A minimal
account row is upserted into the corresponding raw account table so
downstream views (dim_accounts, prep.int_transactions__merged) have the
foreign-key target they need.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import polars as pl

from moneybin.database import Database
from moneybin.tables import (
    OFX_ACCOUNTS,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
)
from moneybin.testing.scenarios.loader import REPO_ROOT, FixtureSpec


def load_fixture_into_db(db: Database, spec: FixtureSpec) -> None:
    """Load a fixture file described by ``spec`` into the matching raw table."""
    path = (REPO_ROOT / spec.path).resolve()
    df = pl.read_csv(path)
    source_file = str(path)

    if spec.source_type == "csv":
        _seed_tabular_account(db, spec.account, source_file)
        enriched = _enrich_for_tabular_raw(
            df, account=spec.account, source_file=source_file
        )
        db.ingest_dataframe(
            TABULAR_TRANSACTIONS.full_name, enriched, on_conflict="insert"
        )
    elif spec.source_type == "ofx":
        _seed_ofx_account(db, spec.account, source_file)
        enriched = _enrich_for_ofx_raw(
            df, account=spec.account, source_file=source_file
        )
        db.ingest_dataframe(OFX_TRANSACTIONS.full_name, enriched, on_conflict="insert")
    else:
        raise NotImplementedError(
            f"fixture loader does not support source_type={spec.source_type!r}"
        )


def _seed_tabular_account(db: Database, account_id: str, source_file: str) -> None:
    df = pl.DataFrame([
        {
            "account_id": account_id,
            "account_name": account_id,
            "account_type": "credit_card",
            "institution_name": "fixture",
            "source_file": source_file,
            "source_type": "csv",
            "source_origin": "fixture",
            "import_id": uuid.uuid4().hex[:12],
            "extracted_at": datetime.now(UTC),
        }
    ])
    db.ingest_dataframe(TABULAR_ACCOUNTS.full_name, df, on_conflict="upsert")


def _seed_ofx_account(db: Database, account_id: str, source_file: str) -> None:
    df = pl.DataFrame([
        {
            "account_id": account_id,
            "routing_number": None,
            "account_type": "CREDITCARD",
            "institution_org": "fixture",
            "institution_fid": None,
            "source_file": source_file,
            "extracted_at": datetime.now(UTC),
        }
    ])
    db.ingest_dataframe(OFX_ACCOUNTS.full_name, df, on_conflict="upsert")


def _enrich_for_tabular_raw(
    df: pl.DataFrame, *, account: str, source_file: str
) -> pl.DataFrame:
    import_id = uuid.uuid4().hex[:12]
    return df.select(
        pl.col("source_transaction_id").alias("transaction_id"),
        pl.lit(account).alias("account_id"),
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("transaction_date"),
        pl.col("amount").cast(pl.Decimal(18, 2)),
        pl.col("description"),
        pl.col("source_transaction_id"),
        pl.lit(source_file).alias("source_file"),
        pl.lit("csv").alias("source_type"),
        pl.lit("fixture").alias("source_origin"),
        pl.lit(import_id).alias("import_id"),
        pl.int_range(1, df.height + 1, eager=True).alias("row_number"),
    )


def _enrich_for_ofx_raw(
    df: pl.DataFrame, *, account: str, source_file: str
) -> pl.DataFrame:
    return df.select(
        pl.col("source_transaction_id"),
        pl.lit(account).alias("account_id"),
        pl.col("transaction_type"),
        pl
        .col("date")
        .str.strptime(pl.Date, "%Y-%m-%d")
        .cast(pl.Datetime)
        .alias("date_posted"),
        pl.col("amount").cast(pl.Decimal(18, 2)),
        pl.col("payee"),
        pl.lit(None, dtype=pl.Utf8).alias("memo"),
        pl.lit(None, dtype=pl.Utf8).alias("check_number"),
        pl.lit(source_file).alias("source_file"),
        pl.lit(datetime.now(UTC)).alias("extracted_at"),
    )
