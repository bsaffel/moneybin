"""Load trusted Currency conversions for SQLMesh models.

Accepted, unreversed Transfer Decisions and source-provided single rows are the
only evidence admitted here. Reference-rate resolution is deliberately
cache-only: a transform must never turn into a provider call or an App write.
"""

from __future__ import annotations

import hashlib
import re
import typing as t
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

from moneybin.investments.cost_basis import LedgerEvent, compute_lots_and_gains
from moneybin.services.currency_service import last_publication_day

if t.TYPE_CHECKING:
    from sqlmesh import ExecutionContext  # type: ignore[import-untyped]

_RATE_QUANTUM = Decimal("0.00000001")
_MONEY_QUANTUM = Decimal("0.01")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True)
class CurrencyConversionRow:
    """One immutable row in ``core.bridge_currency_conversions``."""

    conversion_id: str
    source_shape: str
    transfer_pair_id: str | None
    from_transaction_id: str | None
    to_transaction_id: str | None
    from_account_id: str | None
    to_account_id: str | None
    from_date: date | None
    to_date: date | None
    from_amount: Decimal | None
    from_currency: str | None
    to_amount: Decimal | None
    to_currency: str | None
    executed_rate: Decimal | None
    home_currency: str | None
    home_value: Decimal | None
    valuation_rate: Decimal | None
    valuation_rate_date: date | None
    valuation_source_type: str | None
    from_source_type: str | None
    from_source_origin: str | None
    from_source_transaction_id: str | None
    to_source_type: str | None
    to_source_origin: str | None
    to_source_transaction_id: str | None
    coverage_status: str
    coverage_reason: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class ForeignSecuritySale:
    """One foreign-currency cash acquisition from a Security sale."""

    investment_transaction_id: str
    account_id: str
    trade_date: date
    net_proceeds: Decimal
    fees: Decimal | None
    currency_code: str
    home_currency: str
    home_value: Decimal | None
    valuation_rate: Decimal | None
    valuation_rate_date: date | None
    valuation_source_type: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class CurrencyLotRow:
    """One public Currency-lot row derived from the private engine key."""

    currency_lot_id: str
    account_id: str
    currency_code: str
    acquisition_date: date
    acquisition_type: str
    original_quantity: Decimal
    remaining_quantity: Decimal
    cost_basis_total: Decimal | None
    cost_basis_remaining: Decimal | None
    cost_basis_method: str
    home_currency: str
    source_conversion_id: str | None
    source_investment_transaction_id: str | None
    basis_incomplete: bool
    coverage_status: str
    coverage_reason: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class RealizedFXGainRow:
    """One public realized-FX row for a disposal and consumed Currency lot."""

    realized_fx_gain_id: str
    account_id: str
    conversion_id: str
    currency_lot_id: str
    currency_code: str
    home_currency: str
    acquisition_date: date
    disposal_date: date
    disposed_amount: Decimal
    proceeds: Decimal
    cost_basis: Decimal | None
    gain_loss: Decimal | None
    fee_amount: Decimal
    cost_basis_method: str
    valuation_rate: Decimal | None
    valuation_rate_date: date | None
    valuation_source_type: str | None
    coverage_status: str
    coverage_reason: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class CurrencyAccountingResult:
    """Immutable Currency lots and realized-FX output."""

    lots: Sequence[CurrencyLotRow]
    gains: Sequence[RealizedFXGainRow]


@dataclass(frozen=True)
class _EventMetadata:
    account_id: str
    currency_code: str
    home_currency: str
    source_conversion_id: str | None
    source_investment_transaction_id: str | None
    acquisition_type: str | None
    quantity: Decimal
    home_value: Decimal
    valuation_rate: Decimal | None
    valuation_rate_date: date | None
    valuation_source_type: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class _Candidate:
    source_shape: str
    transfer_pair_id: str | None
    from_transaction_id: str | None
    to_transaction_id: str | None
    from_account_id: str | None
    to_account_id: str | None
    from_date: date | None
    to_date: date | None
    from_amount: Decimal | None
    from_currency: str | None
    to_amount: Decimal | None
    to_currency: str | None
    from_source_type: str | None
    from_source_origin: str | None
    from_source_transaction_id: str | None
    to_source_type: str | None
    to_source_origin: str | None
    to_source_transaction_id: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class _StoredRate:
    rate: Decimal
    rate_date: date
    source_type: str
    updated_at: datetime | None


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return t.cast(
        "list[dict[str, object]]",
        frame.to_dict(orient="records"),  # pyright: ignore[reportUnknownMemberType]
    )


def _opt_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    return str(value)


def _opt_decimal(value: object) -> Decimal | None:
    text = _opt_str(value)
    return Decimal(text) if text is not None else None


def _opt_date(value: object) -> date | None:
    text = _opt_str(value)
    return date.fromisoformat(text) if text is not None else None


def _opt_timestamp(value: object) -> datetime | None:
    text = _opt_str(value)
    return datetime.fromisoformat(text) if text is not None else None


def _candidate(record: dict[str, object]) -> _Candidate:
    return _Candidate(
        source_shape=str(record["source_shape"]),
        transfer_pair_id=_opt_str(record["transfer_pair_id"]),
        from_transaction_id=_opt_str(record["from_transaction_id"]),
        to_transaction_id=_opt_str(record["to_transaction_id"]),
        from_account_id=_opt_str(record["from_account_id"]),
        to_account_id=_opt_str(record["to_account_id"]),
        from_date=_opt_date(record["from_date"]),
        to_date=_opt_date(record["to_date"]),
        from_amount=_opt_decimal(record["from_amount"]),
        from_currency=_opt_str(record["from_currency"]),
        to_amount=_opt_decimal(record["to_amount"]),
        to_currency=_opt_str(record["to_currency"]),
        from_source_type=_opt_str(record["from_source_type"]),
        from_source_origin=_opt_str(record["from_source_origin"]),
        from_source_transaction_id=_opt_str(record["from_source_transaction_id"]),
        to_source_type=_opt_str(record["to_source_type"]),
        to_source_origin=_opt_str(record["to_source_origin"]),
        to_source_transaction_id=_opt_str(record["to_source_transaction_id"]),
        updated_at=_opt_timestamp(record["candidate_updated_at"]),
    )


def _conversion_id(candidate: _Candidate) -> str:
    if candidate.source_shape == "linked_two_row":
        evidence_id = candidate.transfer_pair_id
        evidence_kind = "transfer"
    elif candidate.source_shape == "single_row":
        evidence_id = candidate.from_transaction_id
        evidence_kind = "transaction"
    else:
        raise ValueError(
            f"unsupported conversion source shape: {candidate.source_shape}"
        )
    if evidence_id is None:
        raise ValueError("trusted conversion evidence has no identity")
    digest = hashlib.sha256(f"{evidence_kind}|{evidence_id}".encode()).hexdigest()[:16]
    return f"fxc_{digest}"


def _latest(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _quantize_rate(value: Decimal) -> Decimal:
    return value.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _valid_currency(value: str | None) -> bool:
    return value is not None and _CURRENCY_RE.fullmatch(value) is not None


def _derive_conversion(
    candidate: _Candidate,
    *,
    home_currency: str | None,
    home_updated_at: datetime | None,
    stored_rate: t.Callable[[str, str, date], _StoredRate | None],
) -> CurrencyConversionRow | None:
    missing_leg = candidate.source_shape == "linked_two_row" and (
        candidate.from_transaction_id is None or candidate.to_transaction_id is None
    )
    valid_terms = (
        candidate.from_amount is not None
        and candidate.from_amount < 0
        and candidate.to_amount is not None
        and candidate.to_amount > 0
    )
    incomplete_shape = (
        any(
            value is None
            for value in (
                candidate.from_transaction_id,
                candidate.from_account_id,
                candidate.to_account_id,
                candidate.from_date,
                candidate.to_date,
                candidate.from_amount,
                candidate.to_amount,
            )
        )
        or (
            candidate.source_shape == "single_row"
            and (candidate.to_amount is None or candidate.to_currency is None)
        )
        or not valid_terms
    )
    unknown_currency = not _valid_currency(
        candidate.from_currency
    ) or not _valid_currency(candidate.to_currency)

    if (
        not missing_leg
        and not incomplete_shape
        and not unknown_currency
        and candidate.from_currency == candidate.to_currency
    ):
        return None

    reason: str | None = None
    if missing_leg:
        reason = "missing_leg"
    elif incomplete_shape:
        reason = "incomplete_shape"
    elif unknown_currency:
        reason = "unknown_currency"
    elif not _valid_currency(home_currency):
        reason = "missing_home_currency"

    from_amount = (
        abs(candidate.from_amount) if candidate.from_amount is not None else None
    )
    to_amount = abs(candidate.to_amount) if candidate.to_amount is not None else None
    executed_rate: Decimal | None = None
    if valid_terms:
        complete_from_amount = t.cast("Decimal", from_amount)
        complete_to_amount = t.cast("Decimal", to_amount)
        executed_rate = _quantize_rate(complete_to_amount / complete_from_amount)

    home_value: Decimal | None = None
    valuation_rate: Decimal | None = None
    valuation_rate_date: date | None = None
    valuation_source_type: str | None = None
    rate_updated_at: datetime | None = None

    if reason is None:
        from_currency = t.cast("str", candidate.from_currency)
        to_currency = t.cast("str", candidate.to_currency)
        to_date = t.cast("date", candidate.to_date)
        complete_from_amount = t.cast("Decimal", from_amount)
        complete_to_amount = t.cast("Decimal", to_amount)
        complete_executed_rate = t.cast("Decimal", executed_rate)
        complete_home_currency = t.cast("str", home_currency)
        if from_currency == complete_home_currency:
            home_value = _quantize_money(complete_from_amount)
            valuation_rate = _quantize_rate(complete_from_amount / complete_to_amount)
            valuation_rate_date = to_date
            valuation_source_type = "actual"
        elif to_currency == complete_home_currency:
            home_value = _quantize_money(complete_to_amount)
            valuation_rate = complete_executed_rate
            valuation_rate_date = to_date
            valuation_source_type = "actual"
        else:
            rate = stored_rate(to_currency, complete_home_currency, to_date)
            if rate is None:
                reason = "missing_valuation_rate"
            else:
                valuation_rate = _quantize_rate(rate.rate)
                valuation_rate_date = rate.rate_date
                valuation_source_type = rate.source_type
                home_value = _quantize_money(complete_to_amount * valuation_rate)
                rate_updated_at = rate.updated_at

    return CurrencyConversionRow(
        conversion_id=_conversion_id(candidate),
        source_shape=candidate.source_shape,
        transfer_pair_id=candidate.transfer_pair_id,
        from_transaction_id=candidate.from_transaction_id,
        to_transaction_id=candidate.to_transaction_id,
        from_account_id=candidate.from_account_id,
        to_account_id=candidate.to_account_id,
        from_date=candidate.from_date,
        to_date=candidate.to_date,
        from_amount=from_amount,
        from_currency=candidate.from_currency,
        to_amount=to_amount,
        to_currency=candidate.to_currency,
        executed_rate=executed_rate,
        home_currency=home_currency,
        home_value=home_value,
        valuation_rate=valuation_rate,
        valuation_rate_date=valuation_rate_date,
        valuation_source_type=valuation_source_type,
        from_source_type=candidate.from_source_type,
        from_source_origin=candidate.from_source_origin,
        from_source_transaction_id=candidate.from_source_transaction_id,
        to_source_type=candidate.to_source_type,
        to_source_origin=candidate.to_source_origin,
        to_source_transaction_id=candidate.to_source_transaction_id,
        coverage_status="complete" if reason is None else "incomplete",
        coverage_reason=reason,
        updated_at=_latest(candidate.updated_at, home_updated_at, rate_updated_at),
    )


def _load_candidates(context: ExecutionContext) -> list[_Candidate]:
    bridge = context.resolve_table("core.bridge_transfers")
    merged = context.resolve_table("prep.int_transactions__merged")
    linked = context.fetchdf(
        f"""
        SELECT
          'linked_two_row' AS source_shape,
          bt.transfer_id AS transfer_pair_id,
          debit.transaction_id AS from_transaction_id,
          credit.transaction_id AS to_transaction_id,
          debit.account_id AS from_account_id,
          credit.account_id AS to_account_id,
          debit.transaction_date::VARCHAR AS from_date,
          credit.transaction_date::VARCHAR AS to_date,
          debit.amount::VARCHAR AS from_amount,
          debit.currency_code AS from_currency,
          credit.amount::VARCHAR AS to_amount,
          credit.currency_code AS to_currency,
          md.source_type_a AS from_source_type,
          md.source_origin_a AS from_source_origin,
          md.source_transaction_id_a AS from_source_transaction_id,
          md.source_type_b AS to_source_type,
          md.source_origin_b AS to_source_origin,
          md.source_transaction_id_b AS to_source_transaction_id,
          GREATEST(debit.loaded_at, credit.loaded_at, md.decided_at)::VARCHAR
            AS candidate_updated_at
        FROM {bridge} AS bt
        JOIN app.match_decisions AS md
          ON bt.transfer_id = md.match_id
        LEFT JOIN {merged} AS debit
          ON bt.debit_transaction_id = debit.transaction_id
        LEFT JOIN {merged} AS credit
          ON bt.credit_transaction_id = credit.transaction_id
        WHERE md.match_type = 'transfer'
          AND md.match_status = 'accepted'
          AND md.reversed_at IS NULL
        """  # noqa: S608  # table names resolved by SQLMesh, not user input
    )
    missing = context.fetchdf(
        f"""
        SELECT
          'linked_two_row' AS source_shape,
          md.match_id AS transfer_pair_id,
          NULL::VARCHAR AS from_transaction_id,
          NULL::VARCHAR AS to_transaction_id,
          NULL::VARCHAR AS from_account_id,
          NULL::VARCHAR AS to_account_id,
          NULL::VARCHAR AS from_date,
          NULL::VARCHAR AS to_date,
          NULL::VARCHAR AS from_amount,
          NULL::VARCHAR AS from_currency,
          NULL::VARCHAR AS to_amount,
          NULL::VARCHAR AS to_currency,
          md.source_type_a AS from_source_type,
          md.source_origin_a AS from_source_origin,
          md.source_transaction_id_a AS from_source_transaction_id,
          md.source_type_b AS to_source_type,
          md.source_origin_b AS to_source_origin,
          md.source_transaction_id_b AS to_source_transaction_id,
          md.decided_at::VARCHAR AS candidate_updated_at
        FROM app.match_decisions AS md
        LEFT JOIN {bridge} AS bt
          ON md.match_id = bt.transfer_id
        WHERE md.match_type = 'transfer'
          AND md.match_status = 'accepted'
          AND md.reversed_at IS NULL
          AND bt.transfer_id IS NULL
        """  # noqa: S608  # table name resolved by SQLMesh, not user input
    )
    single = context.fetchdf(
        f"""
        SELECT
          'single_row' AS source_shape,
          NULL::VARCHAR AS transfer_pair_id,
          transaction_id AS from_transaction_id,
          NULL::VARCHAR AS to_transaction_id,
          account_id AS from_account_id,
          account_id AS to_account_id,
          transaction_date::VARCHAR AS from_date,
          transaction_date::VARCHAR AS to_date,
          amount::VARCHAR AS from_amount,
          currency_code AS from_currency,
          to_amount::VARCHAR AS to_amount,
          to_currency,
          conversion_source_type AS from_source_type,
          conversion_source_origin AS from_source_origin,
          conversion_source_transaction_id AS from_source_transaction_id,
          conversion_source_type AS to_source_type,
          conversion_source_origin AS to_source_origin,
          conversion_source_transaction_id AS to_source_transaction_id,
          loaded_at::VARCHAR AS candidate_updated_at
        FROM {merged}
        WHERE NOT to_amount IS NULL OR NOT to_currency IS NULL
        """  # noqa: S608  # table name resolved by SQLMesh, not user input
    )
    return [
        _candidate(record)
        for frame in (linked, missing, single)
        for record in _records(frame)
    ]


def _load_home_currency(
    context: ExecutionContext,
) -> tuple[str | None, datetime | None]:
    frame = context.fetchdf(
        """
        SELECT home_currency, updated_at::VARCHAR AS profile_updated_at
        FROM app.profile_settings
        ORDER BY scope
        LIMIT 1
        """
    )
    records = _records(frame)
    if not records:
        return None, None
    return (
        _opt_str(records[0]["home_currency"]),
        _opt_timestamp(records[0]["profile_updated_at"]),
    )


def _load_stored_rates(
    context: ExecutionContext,
) -> t.Callable[[str, str, date], _StoredRate | None]:
    override_frame = context.fetchdf(
        """
        SELECT from_currency, to_currency, rate_date::VARCHAR AS rate_date,
               rate::VARCHAR AS rate, updated_at::VARCHAR AS rate_updated_at
        FROM app.exchange_rate_overrides
        """
    )
    provider_frame = context.fetchdf(
        """
        SELECT from_currency, to_currency, rate_date::VARCHAR AS rate_date,
               rate::VARCHAR AS rate, source_type,
               loaded_at::VARCHAR AS loaded_at
        FROM raw.exchange_rates
        """
    )

    overrides: dict[tuple[str, str, date], _StoredRate] = {}
    for record in _records(override_frame):
        rate_date = _opt_date(record["rate_date"])
        rate = _opt_decimal(record["rate"])
        if rate_date is None or rate is None:
            continue
        key = (str(record["from_currency"]), str(record["to_currency"]), rate_date)
        overrides[key] = _StoredRate(
            rate,
            rate_date,
            "override",
            _opt_timestamp(record["rate_updated_at"]),
        )

    provider_rates: dict[tuple[str, str, date], _StoredRate] = {}
    for record in _records(provider_frame):
        rate_date = _opt_date(record["rate_date"])
        rate = _opt_decimal(record["rate"])
        source_type = _opt_str(record["source_type"])
        if rate_date is None or rate is None or source_type is None:
            continue
        key = (str(record["from_currency"]), str(record["to_currency"]), rate_date)
        candidate = _StoredRate(
            rate,
            rate_date,
            source_type,
            _opt_timestamp(record["loaded_at"]),
        )
        current = provider_rates.get(key)
        candidate_loaded_at = candidate.updated_at or datetime.min
        current_loaded_at = current.updated_at if current is not None else None
        if (
            current is None
            or candidate_loaded_at > (current_loaded_at or datetime.min)
            or (
                candidate_loaded_at == (current_loaded_at or datetime.min)
                and candidate.source_type < current.source_type
            )
        ):
            provider_rates[key] = candidate

    def stored_rate(base: str, quote: str, requested: date) -> _StoredRate | None:
        for rate_date in dict.fromkeys((requested, last_publication_day(requested))):
            if override := overrides.get((base, quote, rate_date)):
                return override
            if cached := provider_rates.get((base, quote, rate_date)):
                return cached
        return None

    return stored_rate


def load_conversion_rows(
    context: ExecutionContext,
) -> list[CurrencyConversionRow]:
    """Return the trusted, cache-only Currency conversion projection."""
    candidates = _load_candidates(context)
    home_currency, home_updated_at = _load_home_currency(context)
    stored_rate = _load_stored_rates(context)
    rows: list[CurrencyConversionRow] = []
    for candidate in candidates:
        row = _derive_conversion(
            candidate,
            home_currency=home_currency,
            home_updated_at=home_updated_at,
            stored_rate=stored_rate,
        )
        if row is not None:
            rows.append(row)
    return rows


def _public_lot_id(account_id: str, currency: str, source_event_id: str) -> str:
    raw = f"{account_id}|{currency}|{source_event_id}"
    return "clot_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _public_gain_id(disposal_id: str, currency_lot_id: str) -> str:
    raw = f"{disposal_id}|{currency_lot_id}"
    return "rfx_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _method(account_id: str, account_methods: Mapping[str, str | None]) -> str:
    return account_methods.get(account_id) or "fifo"


def _event(
    event_id: str,
    *,
    account_id: str,
    currency: str,
    home_currency: str,
    trade_date: date,
    event_type: str,
    quantity: Decimal,
    home_value: Decimal | None,
) -> LedgerEvent:
    return LedgerEvent(
        investment_transaction_id=event_id,
        account_id=account_id,
        security_id=f"currency:{currency}",
        trade_date=trade_date,
        original_acquisition_date=None,
        type=event_type,
        quantity=quantity,
        price=None,
        amount=(
            -home_value
            if event_type == "buy" and home_value is not None
            else home_value
        ),
        fees=Decimal("0.00"),
        currency_code=home_currency,
    )


def _conversion_events(
    conversion: CurrencyConversionRow,
) -> list[tuple[LedgerEvent, _EventMetadata]]:
    if conversion.coverage_status != "complete":
        return []
    required = (
        conversion.from_account_id,
        conversion.to_account_id,
        conversion.from_date,
        conversion.to_date,
        conversion.from_amount,
        conversion.from_currency,
        conversion.to_amount,
        conversion.to_currency,
        conversion.home_currency,
        conversion.home_value,
    )
    if any(value is None for value in required):
        return []

    from_account_id = t.cast("str", conversion.from_account_id)
    to_account_id = t.cast("str", conversion.to_account_id)
    from_date = t.cast("date", conversion.from_date)
    to_date = t.cast("date", conversion.to_date)
    from_amount = t.cast("Decimal", conversion.from_amount)
    from_currency = t.cast("str", conversion.from_currency)
    to_amount = t.cast("Decimal", conversion.to_amount)
    to_currency = t.cast("str", conversion.to_currency)
    home_currency = t.cast("str", conversion.home_currency)
    home_value = t.cast("Decimal", conversion.home_value)
    events: list[tuple[LedgerEvent, _EventMetadata]] = []

    if from_currency != home_currency:
        event = _event(
            f"{conversion.conversion_id}:dispose",
            account_id=from_account_id,
            currency=from_currency,
            home_currency=home_currency,
            trade_date=from_date,
            event_type="sell",
            quantity=-from_amount,
            home_value=home_value,
        )
        events.append((
            event,
            _EventMetadata(
                account_id=from_account_id,
                currency_code=from_currency,
                home_currency=home_currency,
                source_conversion_id=conversion.conversion_id,
                source_investment_transaction_id=None,
                acquisition_type=None,
                quantity=from_amount,
                home_value=home_value,
                valuation_rate=conversion.valuation_rate,
                valuation_rate_date=conversion.valuation_rate_date,
                valuation_source_type=conversion.valuation_source_type,
                updated_at=conversion.updated_at,
            ),
        ))

    if to_currency != home_currency:
        event = _event(
            f"{conversion.conversion_id}:acquire",
            account_id=to_account_id,
            currency=to_currency,
            home_currency=home_currency,
            trade_date=to_date,
            event_type="buy",
            quantity=to_amount,
            home_value=home_value,
        )
        events.append((
            event,
            _EventMetadata(
                account_id=to_account_id,
                currency_code=to_currency,
                home_currency=home_currency,
                source_conversion_id=conversion.conversion_id,
                source_investment_transaction_id=None,
                acquisition_type="conversion",
                quantity=to_amount,
                home_value=home_value,
                valuation_rate=conversion.valuation_rate,
                valuation_rate_date=conversion.valuation_rate_date,
                valuation_source_type=conversion.valuation_source_type,
                updated_at=conversion.updated_at,
            ),
        ))
    return events


def _sale_reason(sale: ForeignSecuritySale) -> str | None:
    if not _valid_currency(sale.currency_code):
        return "unknown_currency"
    if not _valid_currency(sale.home_currency):
        return "missing_home_currency"
    if sale.home_value is None:
        return "missing_valuation_rate"
    return None


def _sale_event(sale: ForeignSecuritySale) -> tuple[LedgerEvent, _EventMetadata]:
    event_id = f"{sale.investment_transaction_id}:currency_acquire"
    event = _event(
        event_id,
        account_id=sale.account_id,
        currency=sale.currency_code,
        home_currency=sale.home_currency,
        trade_date=sale.trade_date,
        event_type="buy",
        quantity=sale.net_proceeds,
        home_value=sale.home_value,
    )
    return (
        event,
        _EventMetadata(
            account_id=sale.account_id,
            currency_code=sale.currency_code,
            home_currency=sale.home_currency,
            source_conversion_id=None,
            source_investment_transaction_id=sale.investment_transaction_id,
            acquisition_type="security_sale",
            quantity=sale.net_proceeds,
            home_value=sale.home_value or Decimal("0.00"),
            valuation_rate=sale.valuation_rate,
            valuation_rate_date=sale.valuation_rate_date,
            valuation_source_type=sale.valuation_source_type,
            updated_at=sale.updated_at,
        ),
    )


def _incomplete_lot(
    event: LedgerEvent,
    metadata: _EventMetadata,
    method: str,
    reason: str,
    updated_at: datetime | None,
) -> CurrencyLotRow:
    return CurrencyLotRow(
        currency_lot_id=_public_lot_id(
            metadata.account_id,
            metadata.currency_code,
            event.investment_transaction_id,
        ),
        account_id=metadata.account_id,
        currency_code=metadata.currency_code,
        acquisition_date=event.trade_date,
        acquisition_type=t.cast("str", metadata.acquisition_type),
        original_quantity=metadata.quantity,
        remaining_quantity=metadata.quantity,
        cost_basis_total=None,
        cost_basis_remaining=None,
        cost_basis_method=method,
        home_currency=metadata.home_currency,
        source_conversion_id=metadata.source_conversion_id,
        source_investment_transaction_id=metadata.source_investment_transaction_id,
        basis_incomplete=True,
        coverage_status="incomplete",
        coverage_reason=reason,
        updated_at=updated_at,
    )


def _incomplete_gain(
    event: LedgerEvent,
    metadata: _EventMetadata,
    method: str,
    reason: str,
    updated_at: datetime | None,
) -> RealizedFXGainRow:
    conversion_id = t.cast("str", metadata.source_conversion_id)
    return RealizedFXGainRow(
        realized_fx_gain_id=_public_gain_id(conversion_id, ""),
        account_id=metadata.account_id,
        conversion_id=conversion_id,
        currency_lot_id="",
        currency_code=metadata.currency_code,
        home_currency=metadata.home_currency,
        acquisition_date=event.trade_date,
        disposal_date=event.trade_date,
        disposed_amount=metadata.quantity,
        proceeds=metadata.home_value,
        cost_basis=None,
        gain_loss=None,
        fee_amount=Decimal("0.00"),
        cost_basis_method=method,
        valuation_rate=metadata.valuation_rate,
        valuation_rate_date=metadata.valuation_rate_date,
        valuation_source_type=metadata.valuation_source_type,
        coverage_status="incomplete",
        coverage_reason=reason,
        updated_at=updated_at,
    )


def derive_currency_accounting(
    conversions: Sequence[CurrencyConversionRow],
    security_sales: Sequence[ForeignSecuritySale],
    account_methods: Mapping[str, str | None],
) -> CurrencyAccountingResult:
    """Adapt Currency events to the unchanged cost-basis engine."""
    event_rows: list[tuple[LedgerEvent, _EventMetadata]] = []
    incomplete_sales: list[tuple[LedgerEvent, _EventMetadata, str]] = []
    for conversion in conversions:
        event_rows.extend(_conversion_events(conversion))
    for sale in security_sales:
        if (
            _valid_currency(sale.currency_code)
            and _valid_currency(sale.home_currency)
            and sale.currency_code == sale.home_currency
        ):
            continue
        event, metadata = _sale_event(sale)
        if reason := _sale_reason(sale):
            incomplete_sales.append((event, metadata, reason))
        else:
            event_rows.append((event, metadata))

    group_updated_at: dict[tuple[str, str], datetime] = {}
    for _event_row, metadata in event_rows:
        if metadata.updated_at is None:
            continue
        key = (metadata.account_id, metadata.currency_code)
        current = group_updated_at.get(key)
        if current is None or metadata.updated_at > current:
            group_updated_at[key] = metadata.updated_at

    metadata_for = {
        event.investment_transaction_id: metadata for event, metadata in event_rows
    }
    supported_events: list[LedgerEvent] = []
    unsupported: list[tuple[LedgerEvent, _EventMetadata, str]] = []
    for event, metadata in event_rows:
        method = _method(event.account_id, account_methods)
        if method in {"fifo", "average"}:
            supported_events.append(event)
        else:
            unsupported.append((event, metadata, method))

    engine_lots, engine_gains = ([], [])
    if supported_events:
        engine_lots, engine_gains = compute_lots_and_gains(
            supported_events,
            method_for=lambda account_id, _security_id: _method(
                account_id, account_methods
            ),
            selections_for=lambda _disposal_id: [],
        )

    lots: list[CurrencyLotRow] = []
    engine_lot_ids: dict[str, str] = {}
    for lot in engine_lots:
        metadata = metadata_for[lot.source_transaction_id]
        public_lot_id = _public_lot_id(
            lot.account_id, metadata.currency_code, lot.source_transaction_id
        )
        engine_lot_ids[lot.lot_id] = public_lot_id
        incomplete = lot.basis_incomplete
        lots.append(
            CurrencyLotRow(
                currency_lot_id=public_lot_id,
                account_id=lot.account_id,
                currency_code=metadata.currency_code,
                acquisition_date=lot.acquisition_date,
                acquisition_type=t.cast("str", metadata.acquisition_type),
                original_quantity=lot.original_quantity,
                remaining_quantity=lot.remaining_quantity,
                cost_basis_total=None if incomplete else lot.cost_basis_total,
                cost_basis_remaining=(None if incomplete else lot.cost_basis_remaining),
                cost_basis_method=lot.cost_basis_method,
                home_currency=metadata.home_currency,
                source_conversion_id=metadata.source_conversion_id,
                source_investment_transaction_id=(
                    metadata.source_investment_transaction_id
                ),
                basis_incomplete=incomplete,
                coverage_status="incomplete" if incomplete else "complete",
                coverage_reason="incomplete_history" if incomplete else None,
                updated_at=group_updated_at.get((
                    metadata.account_id,
                    metadata.currency_code,
                )),
            )
        )

    gains: list[RealizedFXGainRow] = []
    for gain in engine_gains:
        metadata = metadata_for[gain.disposal_txn_id]
        conversion_id = t.cast("str", metadata.source_conversion_id)
        public_lot_id = engine_lot_ids.get(gain.lot_id, "")
        reason = None
        if gain.basis_incomplete:
            reason = "negative_inventory" if gain.lot_id == "" else "incomplete_history"
        gains.append(
            RealizedFXGainRow(
                realized_fx_gain_id=_public_gain_id(conversion_id, public_lot_id),
                account_id=gain.account_id,
                conversion_id=conversion_id,
                currency_lot_id=public_lot_id,
                currency_code=metadata.currency_code,
                home_currency=metadata.home_currency,
                acquisition_date=gain.acquisition_date,
                disposal_date=gain.disposal_date,
                disposed_amount=gain.quantity,
                proceeds=gain.proceeds,
                cost_basis=None if reason else gain.cost_basis,
                gain_loss=None if reason else gain.gain_loss,
                fee_amount=Decimal("0.00"),
                cost_basis_method=gain.cost_basis_method,
                valuation_rate=metadata.valuation_rate,
                valuation_rate_date=metadata.valuation_rate_date,
                valuation_source_type=metadata.valuation_source_type,
                coverage_status="incomplete" if reason else "complete",
                coverage_reason=reason,
                updated_at=group_updated_at.get((
                    metadata.account_id,
                    metadata.currency_code,
                )),
            )
        )

    for event, metadata, method in unsupported:
        updated_at = group_updated_at.get((metadata.account_id, metadata.currency_code))
        if event.type == "buy":
            lots.append(
                _incomplete_lot(
                    event, metadata, method, "unsupported_method", updated_at
                )
            )
        else:
            gains.append(
                _incomplete_gain(
                    event, metadata, method, "unsupported_method", updated_at
                )
            )
    for event, metadata, reason in incomplete_sales:
        method = _method(event.account_id, account_methods)
        lots.append(
            _incomplete_lot(event, metadata, method, reason, metadata.updated_at)
        )

    lots.sort(key=lambda row: (row.acquisition_date, row.currency_lot_id))
    gains.sort(
        key=lambda row: (
            row.disposal_date,
            row.acquisition_date,
            row.realized_fx_gain_id,
        )
    )
    return CurrencyAccountingResult(lots=tuple(lots), gains=tuple(gains))


def _load_security_sales(
    context: ExecutionContext,
    *,
    home_currency: str | None,
    home_updated_at: datetime | None,
    stored_rate: t.Callable[[str, str, date], _StoredRate | None],
) -> list[ForeignSecuritySale]:
    ledger = context.resolve_table("core.fct_investment_transactions")
    frame = context.fetchdf(
        f"""
        SELECT investment_transaction_id, account_id,
               trade_date::VARCHAR AS trade_date,
               amount::VARCHAR AS net_proceeds,
               fees::VARCHAR AS fees, currency_code,
               updated_at::VARCHAR AS event_updated_at
        FROM {ledger}
        WHERE type = 'sell' AND amount > 0
        """  # noqa: S608  # table name resolved by SQLMesh, not user input
    )
    sales: list[ForeignSecuritySale] = []
    for record in _records(frame):
        account_id = _opt_str(record["account_id"])
        trade_date = _opt_date(record["trade_date"])
        net_proceeds = _opt_decimal(record["net_proceeds"])
        if account_id is None or trade_date is None or net_proceeds is None:
            continue
        currency = _opt_str(record["currency_code"]) or ""
        resolved_home = home_currency or ""
        if (
            _valid_currency(currency)
            and _valid_currency(resolved_home)
            and currency == resolved_home
        ):
            continue
        rate = (
            stored_rate(currency, resolved_home, trade_date)
            if _valid_currency(currency) and _valid_currency(resolved_home)
            else None
        )
        home_value = _quantize_money(net_proceeds * rate.rate) if rate else None
        sales.append(
            ForeignSecuritySale(
                investment_transaction_id=str(record["investment_transaction_id"]),
                account_id=account_id,
                trade_date=trade_date,
                net_proceeds=net_proceeds,
                fees=_opt_decimal(record["fees"]),
                currency_code=currency,
                home_currency=resolved_home,
                home_value=home_value,
                valuation_rate=_quantize_rate(rate.rate) if rate else None,
                valuation_rate_date=rate.rate_date if rate else None,
                valuation_source_type=rate.source_type if rate else None,
                updated_at=_latest(
                    _opt_timestamp(record["event_updated_at"]),
                    home_updated_at,
                    rate.updated_at if rate else None,
                ),
            )
        )
    return sales


def _load_account_methods(context: ExecutionContext) -> dict[str, str | None]:
    frame = context.fetchdf(
        "SELECT account_id, default_cost_basis_method FROM app.account_settings"
    )
    return {
        str(record["account_id"]): _opt_str(record["default_cost_basis_method"])
        for record in _records(frame)
    }


def load_currency_accounting(
    context: ExecutionContext,
) -> CurrencyAccountingResult:
    """Load cache-only inputs and derive Currency lots and realized FX."""
    candidates = _load_candidates(context)
    home_currency, home_updated_at = _load_home_currency(context)
    stored_rate = _load_stored_rates(context)
    conversions = [
        row
        for candidate in candidates
        if (
            row := _derive_conversion(
                candidate,
                home_currency=home_currency,
                home_updated_at=home_updated_at,
                stored_rate=stored_rate,
            )
        )
        is not None
    ]
    sales = _load_security_sales(
        context,
        home_currency=home_currency,
        home_updated_at=home_updated_at,
        stored_rate=stored_rate,
    )
    return derive_currency_accounting(
        conversions,
        sales,
        _load_account_methods(context),
    )
