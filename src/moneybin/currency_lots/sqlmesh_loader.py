"""Load trusted Currency conversions for SQLMesh models.

Accepted, unreversed Transfer Decisions and source-provided single rows are the
only evidence admitted here. Reference-rate resolution is deliberately
cache-only: a transform must never turn into a provider call or an App write.
"""

from __future__ import annotations

import hashlib
import re
import typing as t
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd

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
    evidence_id = (
        candidate.transfer_pair_id
        if candidate.source_shape == "linked_two_row"
        else candidate.from_transaction_id
    )
    if evidence_id is None:
        raise ValueError("trusted conversion evidence has no identity")
    digest = hashlib.sha256(
        f"{candidate.source_shape}|{evidence_id}".encode()
    ).hexdigest()[:16]
    return f"conversion_{digest}"


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
    if (
        candidate.from_currency is not None
        and candidate.to_currency is not None
        and candidate.from_currency == candidate.to_currency
    ):
        return None

    missing_leg = candidate.source_shape == "linked_two_row" and (
        candidate.from_transaction_id is None or candidate.to_transaction_id is None
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
        or candidate.from_amount == 0
    )
    unknown_currency = not _valid_currency(
        candidate.from_currency
    ) or not _valid_currency(candidate.to_currency)

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
    if from_amount not in (None, 0) and to_amount is not None:
        executed_rate = _quantize_rate(to_amount / from_amount)

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
