"""Loads hand-authored CSV fixtures into ``raw.tabular_transactions`` for scenarios."""

from __future__ import annotations

import uuid

import polars as pl

from moneybin.database import Database
from moneybin.testing.scenarios.loader import REPO_ROOT, FixtureSpec


def load_fixture_into_db(db: Database, spec: FixtureSpec) -> None:
    """Load a fixture CSV described by ``spec`` into ``raw.tabular_transactions``."""
    path = (REPO_ROOT / spec.path).resolve()
    if spec.source_type != "csv":
        raise NotImplementedError(
            f"fixture loader only supports source_type='csv', got {spec.source_type!r}"
        )

    df = pl.read_csv(path)
    enriched = _enrich_for_tabular_raw(df, account=spec.account, source_file=str(path))
    db.ingest_dataframe("raw.tabular_transactions", enriched, on_conflict="insert")


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
