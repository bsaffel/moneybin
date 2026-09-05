"""SQLMesh Python model for Currency lots valued in Home currency."""

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
    "core.fct_currency_lots",
    kind="FULL",
    depends_on={
        "core.bridge_currency_conversions",
        "core.bridge_transfers",
        "core.fct_investment_transactions",
        "core.fct_transactions",
        "prep.int_transactions__matched",
    },
    columns={
        "currency_lot_id": "VARCHAR",
        "account_id": "VARCHAR",
        "source_conversion_id": "VARCHAR",
        "source_investment_transaction_id": "VARCHAR",
        "source_transfer_id": "VARCHAR",
        "currency_code": "VARCHAR",
        "acquisition_type": "VARCHAR",
        "cost_basis_method": "VARCHAR",
        "home_currency": "VARCHAR",
        "coverage_status": "VARCHAR",
        "coverage_reason": "VARCHAR",
        "original_quantity": "DECIMAL(18, 2)",
        "remaining_quantity": "DECIMAL(18, 2)",
        "cost_basis_total": "DECIMAL(18, 2)",
        "cost_basis_remaining": "DECIMAL(18, 2)",
        "basis_incomplete": "BOOLEAN",
        "acquisition_date": "DATE",
        "updated_at": "TIMESTAMP",
    },
    column_descriptions={
        "currency_lot_id": "Stable content hash of the Currency acquisition",
        "account_id": "Account holding the Currency lot",
        "currency_code": "Currency held by this lot",
        "acquisition_date": "Date the Currency was acquired",
        "acquisition_type": "conversion | security_sale | transfer",
        "original_quantity": "Currency units when the lot opened",
        "remaining_quantity": "Currency units remaining after disposals",
        "cost_basis_total": "Home-currency basis of the original quantity",
        "cost_basis_remaining": "Home-currency basis of the remaining quantity",
        "cost_basis_method": "fifo | average",
        "home_currency": "Profile Home currency used for basis",
        "source_conversion_id": "Opening conversion, when applicable",
        "source_investment_transaction_id": "Opening Security sale, when applicable",
        "source_transfer_id": "Accepted Transfer that placed the lot in this Account",
        "basis_incomplete": "Whether the lot lacks complete Home-currency basis",
        "coverage_status": "complete | incomplete",
        "coverage_reason": "Closed reason when coverage is incomplete",
        "updated_at": "Latest contributing input timestamp",
    },
    description=(
        "Currency lots derived from trusted conversions and foreign-Security sale "
        "proceeds, valued in Home currency."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # noqa: ARG001 — FULL kind ignores start/end
    end: datetime,  # noqa: ARG001
    execution_time: datetime,  # noqa: ARG001
    **kwargs: t.Any,  # noqa: ARG001
) -> Iterator[pd.DataFrame]:
    """Emit one explicitly typed row per derived Currency lot."""
    rows = load_currency_accounting(context).lots
    set_fx_accounting_rows("currency_lot", (row.coverage_reason for row in rows))
    if not rows:
        yield from ()
        return

    schema = pa.schema([
        pa.field("currency_lot_id", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("source_conversion_id", pa.string()),
        pa.field("source_investment_transaction_id", pa.string()),
        pa.field("source_transfer_id", pa.string()),
        pa.field("currency_code", pa.string()),
        pa.field("acquisition_type", pa.string()),
        pa.field("cost_basis_method", pa.string()),
        pa.field("home_currency", pa.string()),
        pa.field("coverage_status", pa.string()),
        pa.field("coverage_reason", pa.string()),
        pa.field("original_quantity", pa.decimal128(18, 2)),
        pa.field("remaining_quantity", pa.decimal128(18, 2)),
        pa.field("cost_basis_total", pa.decimal128(18, 2)),
        pa.field("cost_basis_remaining", pa.decimal128(18, 2)),
        pa.field("basis_incomplete", pa.bool_()),
        pa.field("acquisition_date", pa.date32()),
        pa.field("updated_at", pa.timestamp("us")),
    ])
    yield pa.Table.from_pylist([asdict(row) for row in rows], schema=schema).to_pandas(
        types_mapper=pd.ArrowDtype
    )
