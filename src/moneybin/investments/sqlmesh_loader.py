"""Shared input loader for the investment cost-basis SQLMesh models.

Lives OUTSIDE ``sqlmesh/models/`` on purpose so SQLMesh's model scanner never
imports it as a model. Both ``core.fct_investment_lots`` and
``core.fct_realized_gains`` run the same pure engine
(``moneybin.investments.cost_basis``) over the same three inputs; this module is
the single place those inputs are fetched and shaped, so the two Python models
stay thin and the fetch/closure logic is unit-testable without a SQLMesh Context.

Precision (1099-B to the cent): ``context.fetchdf`` returns DECIMAL columns as
float64, and ``Decimal(str(float64))`` is lossy for the ``DECIMAL(28,10)``
quantities/prices the cost-basis math depends on. Every decimal column is
therefore CAST to VARCHAR in-SQL and parsed back with ``Decimal(str)`` — the
exact value, never routed through a float. Dates and the freshness timestamp are
CAST to VARCHAR and parsed for the same robustness reason (it also sidesteps
pandas ``NaT``/Timestamp ambiguity on nullable columns).
"""

from __future__ import annotations

import typing as t
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from moneybin.investments.cost_basis import LedgerEvent

if t.TYPE_CHECKING:
    from sqlmesh import ExecutionContext  # type: ignore[import-untyped]

# Global fallback when neither the security nor the account elects a method.
_DEFAULT_METHOD = "fifo"

MethodFor = Callable[[str, str], str]
SelectionsFor = Callable[[str], list[tuple[str, Decimal]]]


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    """DataFrame -> row dicts with typed cells.

    ``fetchdf`` yields a DataFrame whose cell types pyright cannot infer; this
    single cast localizes the Unknown so every downstream cell access is typed
    ``object`` and flows cleanly into the ``_opt_*`` helpers.
    """
    return t.cast(
        "list[dict[str, object]]",
        frame.to_dict(orient="records"),  # pyright: ignore[reportUnknownMemberType]
    )


def _opt_str(value: object) -> str | None:
    """A VARCHAR-cast cell -> ``str`` or ``None``.

    Guards float ``NaN`` (which can surface when an all-NULL column comes back as
    float64 rather than object dtype) via the not-equal-to-itself test — a
    pandas-stub-free check that never trips on a real string.
    """
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    return str(value)


def _opt_decimal(value: object) -> Decimal | None:
    """Exact Decimal from a VARCHAR-cast cell; ``None`` passes through."""
    text = _opt_str(value)
    return Decimal(text) if text is not None else None


def _opt_date(value: object) -> date | None:
    """Python ``date`` from a VARCHAR-cast DATE cell; ``None`` passes through."""
    text = _opt_str(value)
    return date.fromisoformat(text) if text is not None else None


def _req_date(value: object) -> date:
    """A non-NULL DATE cell -> ``date``; raise on unexpected NULL.

    ``trade_date`` is ``NOT NULL`` in the ledger; a NULL here is a real data
    violation the engine must not silently absorb.
    """
    parsed = _opt_date(value)
    if parsed is None:
        raise ValueError(
            "trade_date unexpectedly NULL in core.fct_investment_transactions"
        )
    return parsed


def _opt_timestamp(value: object) -> datetime | None:
    """Python ``datetime`` from a VARCHAR-cast TIMESTAMP cell; ``None`` passes."""
    text = _opt_str(value)
    return datetime.fromisoformat(text) if text is not None else None


def _load_ledger(
    context: ExecutionContext,
) -> tuple[list[LedgerEvent], dict[tuple[str, str], datetime]]:
    """Fetch the canonical ledger and the per-group freshness map.

    Returns the ``LedgerEvent`` list (engine input) plus
    ``{(account_id, security_id): MAX(updated_at)}`` — the row-freshness the two
    fact models stamp on every lot/gain in that group (core-updated-at
    convention: MAX of the contributing input rows, never CURRENT_TIMESTAMP).
    """
    # resolve_table() gives the versioned physical name valid during backfill;
    # the plain "core.fct_investment_transactions" only exists after promotion.
    ledger_table = context.resolve_table("core.fct_investment_transactions")
    frame: pd.DataFrame = context.fetchdf(
        f"""
        SELECT
          investment_transaction_id,
          account_id,
          security_id,
          trade_date::VARCHAR AS trade_date,
          original_acquisition_date::VARCHAR AS original_acquisition_date,
          type,
          quantity::VARCHAR AS quantity,
          price::VARCHAR AS price,
          amount::VARCHAR AS amount,
          fees::VARCHAR AS fees,
          currency_code,
          updated_at::VARCHAR AS updated_at
        FROM {ledger_table}
        """  # noqa: S608  # table name from context.resolve_table(), not user input
    )

    events: list[LedgerEvent] = []
    group_updated_at: dict[tuple[str, str], datetime] = {}
    for record in _records(frame):
        account_id = _opt_str(record["account_id"])
        if account_id is None:
            # account_id is NOT NULL in the ledger; skip defensively.
            continue
        security_id = _opt_str(record["security_id"])
        events.append(
            LedgerEvent(
                investment_transaction_id=str(record["investment_transaction_id"]),
                account_id=account_id,
                security_id=security_id,
                trade_date=_req_date(record["trade_date"]),
                original_acquisition_date=_opt_date(
                    record["original_acquisition_date"]
                ),
                type=str(record["type"]),
                quantity=_opt_decimal(record["quantity"]),
                price=_opt_decimal(record["price"]),
                amount=_opt_decimal(record["amount"]),
                fees=_opt_decimal(record["fees"]),
                currency_code=_opt_str(record["currency_code"]),
            )
        )
        # Only security-bearing events open lots (engine skips security_id NULL),
        # so the freshness map is keyed the same way the engine groups.
        updated_at = _opt_timestamp(record["updated_at"])
        if security_id is not None and updated_at is not None:
            key = (account_id, security_id)
            current = group_updated_at.get(key)
            if current is None or updated_at > current:
                group_updated_at[key] = updated_at
    return events, group_updated_at


def _load_method_for(context: ExecutionContext) -> MethodFor:
    """Build the elected-method resolver: per-security -> per-account -> fifo."""
    securities: pd.DataFrame = context.fetchdf(
        """
        SELECT security_id, cost_basis_method
        FROM app.securities
        WHERE NOT cost_basis_method IS NULL
        """
    )
    security_method = {
        str(record["security_id"]): str(record["cost_basis_method"])
        for record in _records(securities)
    }
    accounts: pd.DataFrame = context.fetchdf(
        """
        SELECT account_id, default_cost_basis_method
        FROM app.account_settings
        WHERE NOT default_cost_basis_method IS NULL
        """
    )
    account_default = {
        str(record["account_id"]): str(record["default_cost_basis_method"])
        for record in _records(accounts)
    }

    def method_for(account_id: str, security_id: str) -> str:
        elected = security_method.get(security_id)
        if elected is not None:
            return elected
        elected = account_default.get(account_id)
        if elected is not None:
            return elected
        return _DEFAULT_METHOD

    return method_for


def _load_selections_for(context: ExecutionContext) -> SelectionsFor:
    """Build the specific-identification lot-selection resolver."""
    # ORDER BY is load-bearing, not cosmetic: selection order feeds specific-ID
    # consumption, and the engine assigns each disposal's penny-rounding residual
    # to the LAST slice. A non-deterministic row order could shift a one-cent
    # residual between lots across rebuilds, breaking content-hash-stable output.
    selections_frame: pd.DataFrame = context.fetchdf(
        """
        SELECT investment_transaction_id, lot_id, quantity::VARCHAR AS quantity
        FROM app.lot_selections
        ORDER BY investment_transaction_id, lot_id
        """
    )
    selections: dict[str, list[tuple[str, Decimal]]] = {}
    for record in _records(selections_frame):
        quantity = _opt_decimal(record["quantity"])
        if quantity is None:
            # quantity is NOT NULL in app.lot_selections; skip defensively.
            continue
        disposal_txn_id = str(record["investment_transaction_id"])
        selections.setdefault(disposal_txn_id, []).append((
            str(record["lot_id"]),
            quantity,
        ))

    def selections_for(disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        return selections.get(disposal_txn_id, [])

    return selections_for


def load_engine_inputs(
    context: ExecutionContext,
) -> tuple[
    list[LedgerEvent], MethodFor, SelectionsFor, dict[tuple[str, str], datetime]
]:
    """Fetch and shape every input the cost-basis engine needs.

    Returns ``(events, method_for, selections_for, group_updated_at)``:

    - ``events`` — the ledger as ``LedgerEvent`` objects, decimals/dates parsed
      exactly (no float64 round-trip).
    - ``method_for(account_id, security_id) -> method`` — per-security election,
      else per-account default, else ``"fifo"``.
    - ``selections_for(disposal_txn_id) -> [(lot_id, quantity), ...]`` — the
      specific-identification overrides for a disposal (empty when none).
    - ``group_updated_at`` — ``{(account_id, security_id): MAX(updated_at)}`` for
      stamping row freshness on the derived lots/gains.

    The fourth element extends the three engine arguments so both fact models
    share one ledger fetch (DRY) instead of each re-querying freshness.
    """
    events, group_updated_at = _load_ledger(context)
    return (
        events,
        _load_method_for(context),
        _load_selections_for(context),
        group_updated_at,
    )
