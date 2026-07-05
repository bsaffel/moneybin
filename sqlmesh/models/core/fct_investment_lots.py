"""SQLMesh Python model for core.fct_investment_lots.

Each acquisition opens a tax lot; disposals consume open lots per the resolved
cost-basis method. The consumption logic (FIFO cursor, HIFO sort, average-cost
running pool, specific-ID override) lives in the pure engine
``moneybin.investments.cost_basis`` and is fed by ``sqlmesh_loader`` — this model
is the thin SQLMesh wrapper that runs the engine and types the output.

Decimal exactness is load-bearing (1099-B reconciliation): the loader parses
inputs without a float64 round-trip and the output is emitted through an explicit
PyArrow schema so DuckDB receives DECIMAL(28,10) quantities and DECIMAL(18,2)
money end-to-end, never inferring a too-narrow precision from sample values.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator
from datetime import datetime

import pandas as pd
import pyarrow as pa

from moneybin.investments.cost_basis import compute_lots_and_gains
from moneybin.investments.sqlmesh_loader import load_engine_inputs
from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    ExecutionContext,
    model,
)


@model(
    "core.fct_investment_lots",
    kind="FULL",
    # fetchdf() SQL is opaque to SQLMesh's dependency scanner; declare the one
    # SQLMesh-built input so it materialises before this model runs. The app.*
    # inputs (securities, account_settings, lot_selections) are external tables
    # populated outside SQLMesh — always present, so they need no ordering edge.
    depends_on={"core.fct_investment_transactions"},
    columns={
        "lot_id": "VARCHAR",
        "account_id": "VARCHAR",
        "security_id": "VARCHAR",
        "acquisition_date": "DATE",
        "acquisition_type": "VARCHAR",
        "original_quantity": "DECIMAL(28, 10)",
        "remaining_quantity": "DECIMAL(28, 10)",
        "cost_basis_total": "DECIMAL(18, 2)",
        "cost_basis_remaining": "DECIMAL(18, 2)",
        "cost_basis_method": "VARCHAR",
        "currency_code": "VARCHAR",
        "is_open": "BOOLEAN",
        "source_transaction_id": "VARCHAR",
        "updated_at": "TIMESTAMP",
    },
    column_descriptions={
        "lot_id": "Content hash of (account_id, security_id, acquisition_date, source acquisition txn id); prefix 'lot_'",
        "account_id": "FK to core.dim_accounts",
        "security_id": "FK to core.dim_securities",
        "acquisition_date": "Trade date of the opening event; drives ST/LT",
        "acquisition_type": "buy | reinvest | transfer_in",
        "original_quantity": "Units when the lot opened",
        "remaining_quantity": "Open units after disposals consumed (0 when fully closed)",
        "cost_basis_total": "Total basis of original_quantity, including fees",
        "cost_basis_remaining": "Basis attributable to remaining_quantity",
        "cost_basis_method": "Resolved method that governed this lot's consumption (fifo | hifo | specific | average)",
        "currency_code": "Denominating currency",
        "is_open": "remaining_quantity > 0",
        "source_transaction_id": "FK to the opening core.fct_investment_transactions row",
        "updated_at": "Latest of all per-row input timestamps contributing to this row's current values (MAX over the position's ledger rows). Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md.",
    },
    description=(
        "Tax lots derived from core.fct_investment_transactions: each acquisition "
        "opens a lot, each disposal consumes open lots per the resolved cost-basis "
        "method (fifo | hifo | specific | average). Rebuilt in full on every run."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # noqa: ARG001 — FULL kind ignores start/end
    end: datetime,  # noqa: ARG001
    execution_time: datetime,  # noqa: ARG001
    **kwargs: t.Any,  # noqa: ARG001
) -> Iterator[pd.DataFrame]:
    """Run the cost-basis engine and emit one row per derived tax lot."""
    events, method_for, selections_for, group_updated_at = load_engine_inputs(context)
    if not events:
        # SQLMesh rejects empty DataFrames from Python models; the generator
        # protocol signals "no rows" without the "Cannot construct source query"
        # error (see core.fct_balances_daily).
        yield from ()
        return

    lots, _gains = compute_lots_and_gains(
        events, method_for=method_for, selections_for=selections_for
    )
    if not lots:
        yield from ()
        return

    rows = [
        {
            "lot_id": lot.lot_id,
            "account_id": lot.account_id,
            "security_id": lot.security_id,
            "acquisition_date": lot.acquisition_date,
            "acquisition_type": lot.acquisition_type,
            "original_quantity": lot.original_quantity,
            "remaining_quantity": lot.remaining_quantity,
            "cost_basis_total": lot.cost_basis_total,
            "cost_basis_remaining": lot.cost_basis_remaining,
            "cost_basis_method": lot.cost_basis_method,
            "currency_code": lot.currency_code,
            "is_open": lot.remaining_quantity > 0,
            "source_transaction_id": lot.source_transaction_id,
            "updated_at": group_updated_at.get((lot.account_id, lot.security_id)),
        }
        for lot in lots
    ]
    # Build the schema here (not at module scope): SQLMesh serializes module-level
    # objects when loading the model and cannot serialize a pyarrow Schema.
    # Explicit types keep DECIMAL precision end-to-end instead of letting DuckDB
    # infer a too-narrow precision from sample values.
    schema = pa.schema([
        pa.field("lot_id", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("security_id", pa.string()),
        pa.field("acquisition_date", pa.date32()),
        pa.field("acquisition_type", pa.string()),
        pa.field("original_quantity", pa.decimal128(28, 10)),
        pa.field("remaining_quantity", pa.decimal128(28, 10)),
        pa.field("cost_basis_total", pa.decimal128(18, 2)),
        pa.field("cost_basis_remaining", pa.decimal128(18, 2)),
        pa.field("cost_basis_method", pa.string()),
        pa.field("currency_code", pa.string()),
        pa.field("is_open", pa.bool_()),
        pa.field("source_transaction_id", pa.string()),
        pa.field("updated_at", pa.timestamp("us")),
    ])
    yield pa.Table.from_pylist(rows, schema=schema).to_pandas(
        types_mapper=pd.ArrowDtype
    )
