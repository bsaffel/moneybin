"""SQLMesh Python model for realized foreign-exchange gains and losses."""

from __future__ import annotations

import typing as t
from collections.abc import Iterator
from dataclasses import asdict
from datetime import datetime

import pandas as pd
import pyarrow as pa
from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    ExecutionContext,
    model,
)

from moneybin.currency_lots.sqlmesh_loader import load_currency_accounting
from moneybin.metrics.registry import set_fx_accounting_rows


@model(
    "core.fct_realized_fx_gains",
    kind="FULL",
    depends_on={
        "core.bridge_currency_conversions",
        "core.bridge_transfers",
        "core.fct_investment_transactions",
        "core.fct_transactions",
        "prep.int_transactions__matched",
    },
    columns={
        "realized_fx_gain_id": "VARCHAR",
        "account_id": "VARCHAR",
        "conversion_id": "VARCHAR",
        "currency_lot_id": "VARCHAR",
        "currency_code": "VARCHAR",
        "home_currency": "VARCHAR",
        "cost_basis_method": "VARCHAR",
        "valuation_source_type": "VARCHAR",
        "coverage_status": "VARCHAR",
        "coverage_reason": "VARCHAR",
        "disposed_amount": "DECIMAL(18, 2)",
        "proceeds": "DECIMAL(18, 2)",
        "cost_basis": "DECIMAL(18, 2)",
        "gain_loss": "DECIMAL(18, 2)",
        "fee_amount": "DECIMAL(18, 2)",
        "valuation_rate": "DECIMAL(18, 8)",
        "acquisition_date": "DATE",
        "disposal_date": "DATE",
        "valuation_rate_date": "DATE",
        "updated_at": "TIMESTAMP",
    },
    column_descriptions={
        "realized_fx_gain_id": "Stable content hash of the disposal and consumed lot",
        "account_id": "Account holding the disposed Currency",
        "conversion_id": "Conversion that disposed the Currency",
        "currency_lot_id": "Consumed Currency lot; NULL for unmatched inventory",
        "currency_code": "Currency disposed",
        "home_currency": "Profile Home currency used for valuation",
        "acquisition_date": "Date the consumed Currency was acquired",
        "disposal_date": "Date the Currency was disposed",
        "disposed_amount": "Positive magnitude of Currency disposed",
        "proceeds": "Home-currency disposal proceeds",
        "cost_basis": "Home-currency basis of the disposed Currency",
        "gain_loss": "Home-currency proceeds less cost basis",
        "fee_amount": "Home-currency fee allocated to the disposal",
        "cost_basis_method": "fifo | average",
        "valuation_rate": "Actual or stored rate used for proceeds",
        "valuation_rate_date": "Date of the actual terms or stored rate",
        "valuation_source_type": "actual | override | provider Source type",
        "coverage_status": "complete | incomplete",
        "coverage_reason": "Closed reason when coverage is incomplete",
        "updated_at": "Latest contributing input timestamp",
    },
    description=(
        "Realized Home-currency FX gain or loss at the disposal and consumed-Currency-"
        "lot grain."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # noqa: ARG001 — FULL kind ignores start/end
    end: datetime,  # noqa: ARG001
    execution_time: datetime,  # noqa: ARG001
    **kwargs: t.Any,  # noqa: ARG001
) -> Iterator[pd.DataFrame]:
    """Emit one explicitly typed row per realized FX gain or loss."""
    rows = load_currency_accounting(context).gains
    set_fx_accounting_rows("realized_fx_gain", (row.coverage_reason for row in rows))
    if not rows:
        yield from ()
        return

    schema = pa.schema([
        pa.field("realized_fx_gain_id", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("conversion_id", pa.string()),
        pa.field("currency_lot_id", pa.string()),
        pa.field("currency_code", pa.string()),
        pa.field("home_currency", pa.string()),
        pa.field("cost_basis_method", pa.string()),
        pa.field("valuation_source_type", pa.string()),
        pa.field("coverage_status", pa.string()),
        pa.field("coverage_reason", pa.string()),
        pa.field("disposed_amount", pa.decimal128(18, 2)),
        pa.field("proceeds", pa.decimal128(18, 2)),
        pa.field("cost_basis", pa.decimal128(18, 2)),
        pa.field("gain_loss", pa.decimal128(18, 2)),
        pa.field("fee_amount", pa.decimal128(18, 2)),
        pa.field("valuation_rate", pa.decimal128(18, 8)),
        pa.field("acquisition_date", pa.date32()),
        pa.field("disposal_date", pa.date32()),
        pa.field("valuation_rate_date", pa.date32()),
        pa.field("updated_at", pa.timestamp("us")),
    ])
    yield pa.Table.from_pylist([asdict(row) for row in rows], schema=schema).to_pandas(
        types_mapper=pd.ArrowDtype
    )
