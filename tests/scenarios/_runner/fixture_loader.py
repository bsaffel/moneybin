"""Loads hand-authored fixtures into raw schema tables for scenarios.

Supports CSV → ``raw.tabular_transactions`` and OFX-shaped CSV →
``raw.ofx_transactions`` so dedup scenarios can drive both sources from
deterministic fixtures without invoking the synthetic generator. A minimal
account row is upserted into the corresponding raw account table so
downstream views (dim_accounts, prep.int_transactions__merged) have the
foreign-key target they need.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

import polars as pl

from moneybin.database import Database
from moneybin.tables import (
    OFX_ACCOUNTS,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)
from tests.scenarios._runner.loader import FIXTURES_ROOT, FixtureSpec


def load_fixture_into_db(db: Database, spec: FixtureSpec) -> None:
    """Load a fixture file described by ``spec`` into the matching raw table."""
    path = (FIXTURES_ROOT / spec.path).resolve()
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

    if spec.categories:
        _seed_category_overrides(db, spec)


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


def _seed_category_overrides(db: Database, spec: FixtureSpec) -> None:
    """Write FixtureSpec.categories into app.transaction_categories.

    Computes the gold transaction_id (SHA256(source_type|source_transaction_id|account_id)[:16])
    to match int_transactions__matched.sql — must align with the gold key so the
    categorize step's LEFT JOIN on transaction_categories actually finds the override.

    Runs after raw rows are loaded but before transform, so the categorize
    step (which skips rows already present in transaction_categories) sees
    the override and leaves it untouched.
    """
    _upsert_sql = (
        f"INSERT INTO {TRANSACTION_CATEGORIES.full_name}"  # noqa: S608 — TableRef constant; values parameterized
        " (transaction_id, category, subcategory, categorized_by)"
        " VALUES (?, ?, ?, ?)"
        " ON CONFLICT (transaction_id) DO UPDATE SET"
        " category = EXCLUDED.category,"
        " subcategory = EXCLUDED.subcategory,"
        " categorized_by = EXCLUDED.categorized_by"
    )
    for override in spec.categories:
        raw = f"{spec.source_type}|{override.source_transaction_id}|{spec.account}"
        gold_id = hashlib.sha256(raw.encode()).hexdigest()[:16]
        db.execute(
            _upsert_sql,
            [gold_id, override.category, override.subcategory, override.categorized_by],
        )
