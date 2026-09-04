"""SQLMesh Python model for trusted Currency conversions."""

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

from moneybin.currency_lots.sqlmesh_loader import load_conversion_rows
from moneybin.metrics.registry import set_fx_accounting_rows


@model(
    "core.bridge_currency_conversions",
    kind="FULL",
    depends_on={
        "core.bridge_transfers",
        "core.dim_accounts",
        "core.fct_transactions",
        "prep.int_transactions__merged",
        "raw.exchange_rates",
    },
    columns={
        "conversion_id": "VARCHAR",
        "transfer_pair_id": "VARCHAR",
        "from_transaction_id": "VARCHAR",
        "to_transaction_id": "VARCHAR",
        "from_account_id": "VARCHAR",
        "to_account_id": "VARCHAR",
        "from_source_transaction_id": "VARCHAR",
        "to_source_transaction_id": "VARCHAR",
        "source_shape": "VARCHAR",
        "from_currency": "VARCHAR",
        "to_currency": "VARCHAR",
        "home_currency": "VARCHAR",
        "valuation_source_type": "VARCHAR",
        "from_source_type": "VARCHAR",
        "from_source_origin": "VARCHAR",
        "to_source_type": "VARCHAR",
        "to_source_origin": "VARCHAR",
        "coverage_status": "VARCHAR",
        "coverage_reason": "VARCHAR",
        "from_amount": "DECIMAL(18, 2)",
        "to_amount": "DECIMAL(18, 2)",
        "executed_rate": "DECIMAL(18, 8)",
        "home_value": "DECIMAL(18, 2)",
        "valuation_rate": "DECIMAL(18, 8)",
        "from_date": "DATE",
        "to_date": "DATE",
        "valuation_rate_date": "DATE",
        "updated_at": "TIMESTAMP",
    },
    column_descriptions={
        "conversion_id": "Content hash of the trusted evidence identity",
        "source_shape": "linked_two_row | single_row",
        "transfer_pair_id": "Accepted Transfer Decision; NULL for single-row evidence",
        "from_transaction_id": "Canonical sent Transaction",
        "to_transaction_id": "Canonical received Transaction; NULL for single-row evidence",
        "from_account_id": "Account holding the sent leg",
        "to_account_id": "Account holding the received leg",
        "from_date": "Date of the sent leg",
        "to_date": "Date of the received leg",
        "from_amount": "Positive magnitude actually sent",
        "from_currency": "ISO 4217 currency actually sent",
        "to_amount": "Positive magnitude actually received",
        "to_currency": "ISO 4217 currency actually received",
        "executed_rate": "Actual to_amount / from_amount, never a reference rate",
        "home_currency": "Profile Home currency used for valuation",
        "home_value": "Actual Home leg, else stored valuation of the received leg",
        "valuation_rate": "Actual or stored rate used for Home value",
        "valuation_rate_date": "Date of the actual terms or stored rate",
        "valuation_source_type": "actual | override | provider Source type",
        "from_source_type": "Source type supplying the sent leg",
        "from_source_origin": "Source origin supplying the sent leg",
        "from_source_transaction_id": "Native reference supplying the sent leg",
        "to_source_type": "Source type supplying the received leg",
        "to_source_origin": "Source origin supplying the received leg",
        "to_source_transaction_id": "Native reference supplying the received leg",
        "coverage_status": "complete | incomplete",
        "coverage_reason": "Closed reason when coverage is incomplete",
        "updated_at": "Latest contributing input timestamp",
    },
    description=(
        "Trusted cross-currency events derived from accepted Transfer Decisions "
        "or source-provided single-row evidence, with actual executed terms and "
        "cache-only Home valuation."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # noqa: ARG001 — FULL kind ignores start/end
    end: datetime,  # noqa: ARG001
    execution_time: datetime,  # noqa: ARG001
    **kwargs: t.Any,  # noqa: ARG001
) -> Iterator[pd.DataFrame]:
    """Emit one explicitly typed row per trusted Currency conversion."""
    rows = load_conversion_rows(context)
    set_fx_accounting_rows("conversion", (row.coverage_reason for row in rows))
    if not rows:
        yield from ()
        return

    schema = pa.schema([
        pa.field("conversion_id", pa.string()),
        pa.field("transfer_pair_id", pa.string()),
        pa.field("from_transaction_id", pa.string()),
        pa.field("to_transaction_id", pa.string()),
        pa.field("from_account_id", pa.string()),
        pa.field("to_account_id", pa.string()),
        pa.field("from_source_transaction_id", pa.string()),
        pa.field("to_source_transaction_id", pa.string()),
        pa.field("source_shape", pa.string()),
        pa.field("from_currency", pa.string()),
        pa.field("to_currency", pa.string()),
        pa.field("home_currency", pa.string()),
        pa.field("valuation_source_type", pa.string()),
        pa.field("from_source_type", pa.string()),
        pa.field("from_source_origin", pa.string()),
        pa.field("to_source_type", pa.string()),
        pa.field("to_source_origin", pa.string()),
        pa.field("coverage_status", pa.string()),
        pa.field("coverage_reason", pa.string()),
        pa.field("from_amount", pa.decimal128(18, 2)),
        pa.field("to_amount", pa.decimal128(18, 2)),
        pa.field("executed_rate", pa.decimal128(18, 8)),
        pa.field("home_value", pa.decimal128(18, 2)),
        pa.field("valuation_rate", pa.decimal128(18, 8)),
        pa.field("from_date", pa.date32()),
        pa.field("to_date", pa.date32()),
        pa.field("valuation_rate_date", pa.date32()),
        pa.field("updated_at", pa.timestamp("us")),
    ])
    yield pa.Table.from_pylist([asdict(row) for row in rows], schema=schema).to_pandas(
        types_mapper=pd.ArrowDtype
    )
