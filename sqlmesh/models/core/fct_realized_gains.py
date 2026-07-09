"""SQLMesh Python model for core.fct_realized_gains.

The 1099-B grain: one row per (disposal, consumed lot) pair — the realized
gain/loss reconciliation surface. Shares the cost-basis engine and inputs with
``core.fct_investment_lots``; this model takes the engine's ``gains`` output.

Running the engine here as well as in the lots model is deliberate: the engine
is deterministic and cheap at personal-finance scale, so re-running it keeps each
model self-contained rather than threading shared state across two models.

Decimal exactness is load-bearing: the loader parses inputs without a float64
round-trip and the output is emitted through an explicit PyArrow schema so DuckDB
receives DECIMAL(28,10) quantities and DECIMAL(18,2) money end-to-end.
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
    "core.fct_realized_gains",
    kind="FULL",
    # See core.fct_investment_lots: only the SQLMesh-built ledger needs an
    # ordering edge; the app.* inputs are always-present external tables.
    depends_on={"core.fct_investment_transactions"},
    columns={
        "realized_gain_id": "VARCHAR",
        "account_id": "VARCHAR",
        "security_id": "VARCHAR",
        "disposal_txn_id": "VARCHAR",
        "lot_id": "VARCHAR",
        "quantity": "DECIMAL(28, 10)",
        "acquisition_date": "DATE",
        "disposal_date": "DATE",
        "proceeds": "DECIMAL(18, 2)",
        "cost_basis": "DECIMAL(18, 2)",
        "gain_loss": "DECIMAL(18, 2)",
        "term": "VARCHAR",
        "cost_basis_method": "VARCHAR",
        "basis_incomplete": "BOOLEAN",
        "currency_code": "VARCHAR",
        "updated_at": "TIMESTAMP",
    },
    column_descriptions={
        "realized_gain_id": "Content hash of (disposal txn id, lot_id)",
        "account_id": "FK to core.dim_accounts",
        "security_id": "FK to core.dim_securities",
        "disposal_txn_id": "FK to the disposing core.fct_investment_transactions row",
        "lot_id": "FK to core.fct_investment_lots consumed",
        "quantity": "Units drawn from this lot for this disposal",
        "acquisition_date": "Lot acquisition date (holding-period start)",
        "disposal_date": "Disposal trade date (holding-period end)",
        "proceeds": "Sale proceeds attributable to this quantity (net of fees)",
        "cost_basis": "Cost basis attributable to this quantity (method-dependent)",
        "gain_loss": "proceeds - cost_basis (signed; - is a loss)",
        "term": "'short' (held <= 1 year) | 'long' (held > 1 year)",
        "cost_basis_method": "Method that produced this basis",
        "basis_incomplete": "TRUE when part of this disposal matched no tracked lot (zero-basis slice)",
        "currency_code": "Denominating currency",
        "updated_at": "Latest of all per-row input timestamps contributing to this row's current values (MAX over the position's ledger rows). Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md.",
    },
    description=(
        "Realized gains at the 1099-B grain: one row per (disposal, consumed lot) "
        "pair derived from core.fct_investment_transactions via the cost-basis "
        "engine. Rebuilt in full on every run."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # noqa: ARG001 — FULL kind ignores start/end
    end: datetime,  # noqa: ARG001
    execution_time: datetime,  # noqa: ARG001
    **kwargs: t.Any,  # noqa: ARG001
) -> Iterator[pd.DataFrame]:
    """Run the cost-basis engine and emit one row per realized-gain slice."""
    events, method_for, selections_for, group_updated_at = load_engine_inputs(context)
    if not events:
        yield from ()
        return

    _lots, gains = compute_lots_and_gains(
        events, method_for=method_for, selections_for=selections_for
    )
    if not gains:
        yield from ()
        return

    rows = [
        {
            "realized_gain_id": gain.realized_gain_id,
            "account_id": gain.account_id,
            "security_id": gain.security_id,
            "disposal_txn_id": gain.disposal_txn_id,
            "lot_id": gain.lot_id,
            "quantity": gain.quantity,
            "acquisition_date": gain.acquisition_date,
            "disposal_date": gain.disposal_date,
            "proceeds": gain.proceeds,
            "cost_basis": gain.cost_basis,
            "gain_loss": gain.gain_loss,
            "term": gain.term,
            "cost_basis_method": gain.cost_basis_method,
            "basis_incomplete": gain.basis_incomplete,
            "currency_code": gain.currency_code,
            "updated_at": group_updated_at.get((gain.account_id, gain.security_id)),
        }
        for gain in gains
    ]
    # Build the schema here (not at module scope): SQLMesh serializes module-level
    # objects when loading the model and cannot serialize a pyarrow Schema.
    schema = pa.schema([
        pa.field("realized_gain_id", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("security_id", pa.string()),
        pa.field("disposal_txn_id", pa.string()),
        pa.field("lot_id", pa.string()),
        pa.field("quantity", pa.decimal128(28, 10)),
        pa.field("acquisition_date", pa.date32()),
        pa.field("disposal_date", pa.date32()),
        pa.field("proceeds", pa.decimal128(18, 2)),
        pa.field("cost_basis", pa.decimal128(18, 2)),
        pa.field("gain_loss", pa.decimal128(18, 2)),
        pa.field("term", pa.string()),
        pa.field("cost_basis_method", pa.string()),
        pa.field("basis_incomplete", pa.bool_()),
        pa.field("currency_code", pa.string()),
        pa.field("updated_at", pa.timestamp("us")),
    ])
    yield pa.Table.from_pylist(rows, schema=schema).to_pandas(
        types_mapper=pd.ArrowDtype
    )
