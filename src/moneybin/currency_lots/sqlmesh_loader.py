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
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    BRIDGE_CURRENCY_CONVERSIONS,
    BRIDGE_TRANSFERS,
    DIM_ACCOUNTS,
    EXCHANGE_RATE_OVERRIDES,
    EXCHANGE_RATES,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
    INT_TRANSACTIONS_MERGED,
    MATCH_DECISIONS,
    PROFILE_SETTINGS,
)

if t.TYPE_CHECKING:
    from sqlmesh import ExecutionContext  # type: ignore[import-untyped]

_RATE_QUANTUM = Decimal("0.00000001")
_MONEY_QUANTUM = Decimal("0.01")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_UNATTRIBUTED_LOT_HASH_KEY = "<unattributed-currency-lot>"


@dataclass(frozen=True)
class CurrencyConversionRow:
    """One immutable row in ``core.bridge_currency_conversions``."""

    conversion_id: str
    transfer_pair_id: str | None
    from_transaction_id: str | None
    to_transaction_id: str | None
    from_account_id: str | None
    to_account_id: str | None
    from_source_transaction_id: str | None
    to_source_transaction_id: str | None
    source_shape: str
    from_currency: str | None
    to_currency: str | None
    home_currency: str | None
    valuation_source_type: str | None
    from_source_type: str | None
    from_source_origin: str | None
    to_source_type: str | None
    to_source_origin: str | None
    coverage_status: str
    coverage_reason: str | None
    from_amount: Decimal | None
    to_amount: Decimal | None
    executed_rate: Decimal | None
    home_value: Decimal | None
    valuation_rate: Decimal | None
    from_date: date | None
    to_date: date | None
    valuation_rate_date: date | None
    updated_at: datetime | None


@dataclass(frozen=True)
class ForeignSecuritySale:
    """One foreign-currency cash acquisition from a Security sale."""

    investment_transaction_id: str
    account_id: str
    trade_date: date
    net_proceeds: Decimal
    fees: Decimal | None
    currency_code: str | None
    home_currency: str | None
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
    source_conversion_id: str | None
    source_investment_transaction_id: str | None
    currency_code: str | None
    acquisition_type: str
    cost_basis_method: str
    home_currency: str | None
    coverage_status: str
    coverage_reason: str | None
    original_quantity: Decimal
    remaining_quantity: Decimal
    cost_basis_total: Decimal | None
    cost_basis_remaining: Decimal | None
    basis_incomplete: bool
    acquisition_date: date
    updated_at: datetime | None


@dataclass(frozen=True)
class RealizedFXGainRow:
    """One public realized-FX row for a disposal and consumed Currency lot."""

    realized_fx_gain_id: str
    account_id: str
    conversion_id: str
    currency_lot_id: str | None
    currency_code: str | None
    home_currency: str | None
    cost_basis_method: str
    valuation_source_type: str | None
    coverage_status: str
    coverage_reason: str | None
    disposed_amount: Decimal
    proceeds: Decimal | None
    cost_basis: Decimal | None
    gain_loss: Decimal | None
    fee_amount: Decimal
    valuation_rate: Decimal | None
    acquisition_date: date
    disposal_date: date
    valuation_rate_date: date | None
    updated_at: datetime | None


@dataclass(frozen=True)
class CurrencyAccountingResult:
    """Immutable Currency lots and realized-FX output."""

    lots: Sequence[CurrencyLotRow]
    gains: Sequence[RealizedFXGainRow]


@dataclass(frozen=True)
class _EventMetadata:
    account_id: str
    currency_code: str | None
    home_currency: str | None
    source_conversion_id: str | None
    source_investment_transaction_id: str | None
    acquisition_type: str | None
    quantity: Decimal
    home_value: Decimal | None
    coverage_reason: str | None
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


@dataclass(frozen=True)
class _RateResolution:
    rate: _StoredRate | None
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


def _valid_currency(value: str | None) -> t.TypeGuard[str]:
    return value is not None and _CURRENCY_RE.fullmatch(value) is not None


def _derive_conversion(
    candidate: _Candidate,
    *,
    home_currency: str | None,
    home_updated_at: datetime | None,
    stored_rate: t.Callable[[str, str, date], _RateResolution],
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
            valuation_rate_date = t.cast("date", candidate.from_date)
            valuation_source_type = "actual"
        elif to_currency == complete_home_currency:
            home_value = _quantize_money(complete_to_amount)
            valuation_rate = complete_executed_rate
            valuation_rate_date = to_date
            valuation_source_type = "actual"
        else:
            resolved_rate = stored_rate(to_currency, complete_home_currency, to_date)
            rate_updated_at = resolved_rate.updated_at
            if resolved_rate.rate is None:
                reason = "missing_valuation_rate"
            else:
                rate = resolved_rate.rate
                valuation_rate = _quantize_rate(rate.rate)
                valuation_rate_date = rate.rate_date
                valuation_source_type = rate.source_type
                home_value = _quantize_money(complete_to_amount * valuation_rate)

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
    bridge = context.resolve_table(BRIDGE_TRANSFERS.full_name)
    merged = context.resolve_table(INT_TRANSACTIONS_MERGED.full_name)
    match_decisions = MATCH_DECISIONS.full_name
    audit_log = AUDIT_LOG.full_name
    accounts = context.resolve_table(DIM_ACCOUNTS.full_name)
    transactions = context.resolve_table(FCT_TRANSACTIONS.full_name)
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
          canonical_debit.currency_code AS from_currency,
          credit.amount::VARCHAR AS to_amount,
          canonical_credit.currency_code AS to_currency,
          md.source_type_a AS from_source_type,
          md.source_origin_a AS from_source_origin,
          md.source_transaction_id_a AS from_source_transaction_id,
          md.source_type_b AS to_source_type,
          md.source_origin_b AS to_source_origin,
          md.source_transaction_id_b AS to_source_transaction_id,
          GREATEST(
            debit.loaded_at,
            credit.loaded_at,
            COALESCE(decision_audit.mutation_updated_at, md.decided_at),
            COALESCE(canonical_debit.updated_at, debit.loaded_at),
            COALESCE(canonical_credit.updated_at, credit.loaded_at)
          )::VARCHAR AS candidate_updated_at
        FROM {bridge} AS bt
        JOIN {match_decisions} AS md
          ON bt.transfer_id = md.match_id
        LEFT JOIN (
          SELECT target_id, MAX(occurred_at) AS mutation_updated_at
          FROM {audit_log}
          WHERE target_schema = 'app'
            AND target_table = 'match_decisions'
          GROUP BY target_id
        ) AS decision_audit
          ON decision_audit.target_id = md.match_id
        LEFT JOIN {merged} AS debit
          ON bt.debit_transaction_id = debit.transaction_id
        LEFT JOIN {merged} AS credit
          ON bt.credit_transaction_id = credit.transaction_id
        LEFT JOIN {transactions} AS canonical_debit
          ON debit.transaction_id = canonical_debit.transaction_id
        LEFT JOIN {transactions} AS canonical_credit
          ON credit.transaction_id = canonical_credit.transaction_id
        WHERE md.match_type = 'transfer'
          AND md.match_status = 'accepted'
          AND md.reversed_at IS NULL
        """  # noqa: S608  # registered/resolved table names, not user input
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
          COALESCE(
            decision_audit.mutation_updated_at,
            md.decided_at
          )::VARCHAR AS candidate_updated_at
        FROM {match_decisions} AS md
        LEFT JOIN {bridge} AS bt
          ON md.match_id = bt.transfer_id
        LEFT JOIN (
          SELECT target_id, MAX(occurred_at) AS mutation_updated_at
          FROM {audit_log}
          WHERE target_schema = 'app'
            AND target_table = 'match_decisions'
          GROUP BY target_id
        ) AS decision_audit
          ON decision_audit.target_id = md.match_id
        WHERE md.match_type = 'transfer'
          AND md.match_status = 'accepted'
          AND md.reversed_at IS NULL
          AND bt.transfer_id IS NULL
        """  # noqa: S608  # registered/resolved table names, not user input
    )
    single = context.fetchdf(
        f"""
        SELECT
          'single_row' AS source_shape,
          NULL::VARCHAR AS transfer_pair_id,
          single_row.transaction_id AS from_transaction_id,
          NULL::VARCHAR AS to_transaction_id,
          single_row.account_id AS from_account_id,
          single_row.account_id AS to_account_id,
          single_row.conversion_from_date::VARCHAR AS from_date,
          single_row.conversion_from_date::VARCHAR AS to_date,
          single_row.conversion_from_amount::VARCHAR AS from_amount,
          COALESCE(
            single_row.conversion_from_currency,
            sent_account.currency_code
          ) AS from_currency,
          single_row.to_amount::VARCHAR AS to_amount,
          single_row.to_currency,
          single_row.conversion_source_type AS from_source_type,
          single_row.conversion_source_origin AS from_source_origin,
          single_row.conversion_source_transaction_id AS from_source_transaction_id,
          single_row.conversion_source_type AS to_source_type,
          single_row.conversion_source_origin AS to_source_origin,
          single_row.conversion_source_transaction_id AS to_source_transaction_id,
          GREATEST(
            single_row.loaded_at,
            CASE
              WHEN single_row.conversion_from_currency IS NULL
              THEN COALESCE(sent_account.updated_at, single_row.loaded_at)
              ELSE single_row.loaded_at
            END
          )::VARCHAR AS candidate_updated_at
        FROM {merged} AS single_row
        LEFT JOIN {accounts} AS sent_account
          ON single_row.account_id = sent_account.account_id
        LEFT JOIN {bridge} AS linked_transfer
          ON single_row.transaction_id IN (
            linked_transfer.debit_transaction_id,
            linked_transfer.credit_transaction_id
          )
        WHERE (
          NOT single_row.to_amount IS NULL OR NOT single_row.to_currency IS NULL
        )
          AND linked_transfer.transfer_id IS NULL
        """  # noqa: S608  # table name resolved by SQLMesh, not user input
    )
    return [
        _candidate(record)
        for frame in (linked, missing, single)
        for record in _records(frame)
    ]


def _load_app_mutation_watermarks(
    context: ExecutionContext, target_table: str
) -> dict[str, datetime]:
    audit_log = AUDIT_LOG.full_name
    frame = context.fetchdf(
        f"""
        SELECT target_id, MAX(occurred_at)::VARCHAR AS mutation_updated_at
        FROM {audit_log}
        WHERE target_schema = 'app' AND target_table = '{target_table}'
        GROUP BY target_id
        """  # noqa: S608  # TableRef and target table are code-supplied
    )
    return {
        target_id: changed_at
        for record in _records(frame)
        if (target_id := _opt_str(record["target_id"])) is not None
        and (changed_at := _opt_timestamp(record["mutation_updated_at"])) is not None
    }


def _load_home_currency(
    context: ExecutionContext,
) -> tuple[str | None, datetime | None]:
    profile_settings = PROFILE_SETTINGS.full_name
    frame = context.fetchdf(
        f"""
        SELECT home_currency, updated_at::VARCHAR AS profile_updated_at
        FROM {profile_settings}
        ORDER BY scope
        LIMIT 1
        """  # noqa: S608  # registered physical table name, not user input
    )
    records = _records(frame)
    changed_at = _load_app_mutation_watermarks(context, "profile_settings").get(
        "profile"
    )
    if not records:
        return None, changed_at
    return (
        _opt_str(records[0]["home_currency"]),
        _latest(_opt_timestamp(records[0]["profile_updated_at"]), changed_at),
    )


def _load_stored_rates(
    context: ExecutionContext,
) -> t.Callable[[str, str, date], _RateResolution]:
    override_table = EXCHANGE_RATE_OVERRIDES.full_name
    rates_table = EXCHANGE_RATES.full_name
    override_frame = context.fetchdf(
        f"""
        SELECT from_currency, to_currency, rate_date::VARCHAR AS rate_date,
               rate::VARCHAR AS rate, updated_at::VARCHAR AS rate_updated_at
        FROM {override_table}
        """  # noqa: S608  # registered physical table name, not user input
    )
    provider_frame = context.fetchdf(
        f"""
        SELECT from_currency, to_currency, rate_date::VARCHAR AS rate_date,
               rate::VARCHAR AS rate, source_type,
               loaded_at::VARCHAR AS loaded_at
        FROM {rates_table}
        """  # noqa: S608  # registered physical table name, not user input
    )
    mutation_watermarks_by_id = _load_app_mutation_watermarks(
        context, "exchange_rate_overrides"
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

    mutation_watermarks: dict[tuple[str, str, date], datetime] = {}
    for target_id, changed_at in mutation_watermarks_by_id.items():
        parts = target_id.split("|")
        if len(parts) != 3 or not all(_valid_currency(part) for part in parts[:2]):
            continue
        rate_date = _opt_date(parts[2])
        if rate_date is not None:
            mutation_watermarks[(parts[0], parts[1], rate_date)] = changed_at

    def stored_rate(base: str, quote: str, requested: date) -> _RateResolution:
        changed_at: datetime | None = None
        for rate_date in dict.fromkeys((requested, last_publication_day(requested))):
            key = (base, quote, rate_date)
            changed_at = _latest(changed_at, mutation_watermarks.get(key))
            rate = overrides.get(key) or provider_rates.get(key)
            if rate is not None:
                return _RateResolution(rate, _latest(rate.updated_at, changed_at))
        return _RateResolution(None, changed_at)

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


def _load_materialized_conversions(
    context: ExecutionContext,
) -> list[CurrencyConversionRow]:
    table = context.resolve_table(BRIDGE_CURRENCY_CONVERSIONS.full_name)
    frame = context.fetchdf(
        f"""
        SELECT conversion_id, source_shape, transfer_pair_id,
               from_transaction_id, to_transaction_id,
               from_account_id, to_account_id,
               from_date::VARCHAR AS from_date,
               to_date::VARCHAR AS to_date,
               from_amount::VARCHAR AS from_amount, from_currency,
               to_amount::VARCHAR AS to_amount, to_currency,
               executed_rate::VARCHAR AS executed_rate, home_currency,
               home_value::VARCHAR AS home_value,
               valuation_rate::VARCHAR AS valuation_rate,
               valuation_rate_date::VARCHAR AS valuation_rate_date,
               valuation_source_type, from_source_type, from_source_origin,
               from_source_transaction_id, to_source_type, to_source_origin,
               to_source_transaction_id, coverage_status, coverage_reason,
               updated_at::VARCHAR AS conversion_updated_at
        FROM {table}
        """  # noqa: S608  # table name resolved by SQLMesh, not user input
    )
    return [
        CurrencyConversionRow(
            conversion_id=str(record["conversion_id"]),
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
            executed_rate=_opt_decimal(record["executed_rate"]),
            home_currency=_opt_str(record["home_currency"]),
            home_value=_opt_decimal(record["home_value"]),
            valuation_rate=_opt_decimal(record["valuation_rate"]),
            valuation_rate_date=_opt_date(record["valuation_rate_date"]),
            valuation_source_type=_opt_str(record["valuation_source_type"]),
            from_source_type=_opt_str(record["from_source_type"]),
            from_source_origin=_opt_str(record["from_source_origin"]),
            from_source_transaction_id=_opt_str(record["from_source_transaction_id"]),
            to_source_type=_opt_str(record["to_source_type"]),
            to_source_origin=_opt_str(record["to_source_origin"]),
            to_source_transaction_id=_opt_str(record["to_source_transaction_id"]),
            coverage_status=str(record["coverage_status"]),
            coverage_reason=_opt_str(record["coverage_reason"]),
            updated_at=_opt_timestamp(record["conversion_updated_at"]),
        )
        for record in _records(frame)
    ]


def _public_lot_id(account_id: str, currency: str | None, source_event_id: str) -> str:
    raw = f"{account_id}|{currency}|{source_event_id}"
    return "clot_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _public_gain_id(disposal_id: str, currency_lot_id: str | None) -> str:
    lot_hash_key = currency_lot_id or _UNATTRIBUTED_LOT_HASH_KEY
    raw = f"{disposal_id}|{lot_hash_key}"
    return "rfx_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _method(account_id: str, account_methods: Mapping[str, str | None]) -> str:
    return account_methods.get(account_id) or "fifo"


def _event(
    event_id: str,
    *,
    account_id: str,
    currency: str | None,
    home_currency: str | None,
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
    quantity_only_reasons = {"missing_home_currency", "missing_valuation_rate"}
    if (
        conversion.coverage_status != "complete"
        and conversion.coverage_reason not in quantity_only_reasons
    ):
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
    home_currency = conversion.home_currency
    home_value = conversion.home_value
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
                coverage_reason=conversion.coverage_reason,
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
                coverage_reason=conversion.coverage_reason,
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
            home_value=sale.home_value,
            coverage_reason=_sale_reason(sale),
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
        realized_fx_gain_id=_public_gain_id(conversion_id, None),
        account_id=metadata.account_id,
        conversion_id=conversion_id,
        currency_lot_id=None,
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
    return _derive_currency_accounting(
        conversions,
        security_sales,
        account_methods,
        account_method_updated_at={},
    )


def _derive_currency_accounting(
    conversions: Sequence[CurrencyConversionRow],
    security_sales: Sequence[ForeignSecuritySale],
    account_methods: Mapping[str, str | None],
    *,
    account_method_updated_at: Mapping[str, datetime],
) -> CurrencyAccountingResult:
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
        if (reason := _sale_reason(sale)) and reason not in {
            "missing_home_currency",
            "missing_valuation_rate",
        }:
            incomplete_sales.append((event, metadata, reason))
        else:
            event_rows.append((event, metadata))

    group_updated_at: dict[tuple[str, str | None], datetime] = {}
    for _event_row, metadata in event_rows:
        key = (metadata.account_id, metadata.currency_code)
        updated_at = _latest(
            metadata.updated_at,
            account_method_updated_at.get(metadata.account_id),
        )
        if updated_at is not None:
            current = group_updated_at.get(key)
            if current is None or updated_at > current:
                group_updated_at[key] = updated_at

    metadata_for = {
        event.investment_transaction_id: metadata for event, metadata in event_rows
    }
    engine_lots, engine_gains = ([], [])
    if event_rows:
        engine_lots, engine_gains = compute_lots_and_gains(
            [event for event, _metadata in event_rows],
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
        reason = (
            "unsupported_method"
            if lot.cost_basis_method not in {"fifo", "average"}
            else metadata.coverage_reason
        )
        if reason is None and lot.basis_incomplete:
            reason = "incomplete_history"
        incomplete = reason is not None
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
                coverage_reason=reason,
                updated_at=group_updated_at.get((
                    metadata.account_id,
                    metadata.currency_code,
                )),
            )
        )

    gains: list[RealizedFXGainRow] = []
    for gain in engine_gains:
        if gain.cost_basis_method not in {"fifo", "average"}:
            continue
        metadata = metadata_for[gain.disposal_txn_id]
        conversion_id = t.cast("str", metadata.source_conversion_id)
        public_lot_id = engine_lot_ids.get(gain.lot_id)
        reason = metadata.coverage_reason
        if reason is None and gain.basis_incomplete:
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
                proceeds=None if reason else gain.proceeds,
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

    for event, metadata in event_rows:
        method = _method(event.account_id, account_methods)
        if event.type == "buy" or method in {"fifo", "average"}:
            continue
        gains.append(
            _incomplete_gain(
                event,
                metadata,
                method,
                "unsupported_method",
                group_updated_at.get((metadata.account_id, metadata.currency_code)),
            )
        )
    for event, metadata, reason in incomplete_sales:
        method = _method(event.account_id, account_methods)
        lots.append(
            _incomplete_lot(
                event,
                metadata,
                method,
                reason,
                _latest(
                    metadata.updated_at,
                    account_method_updated_at.get(metadata.account_id),
                ),
            )
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
    stored_rate: t.Callable[[str, str, date], _RateResolution],
) -> list[ForeignSecuritySale]:
    ledger = context.resolve_table(FCT_INVESTMENT_TRANSACTIONS.full_name)
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
        currency = _opt_str(record["currency_code"])
        resolved_home = home_currency
        if (
            _valid_currency(currency)
            and _valid_currency(resolved_home)
            and currency == resolved_home
        ):
            continue
        resolved_rate = (
            stored_rate(currency, resolved_home, trade_date)
            if _valid_currency(currency) and _valid_currency(resolved_home)
            else _RateResolution(None, None)
        )
        rate = resolved_rate.rate
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
                    resolved_rate.updated_at,
                ),
            )
        )
    return sales


def _load_account_methods(
    context: ExecutionContext,
) -> tuple[dict[str, str | None], dict[str, datetime]]:
    account_settings = ACCOUNT_SETTINGS.full_name
    frame = context.fetchdf(
        f"""
        SELECT account_id, default_cost_basis_method,
               updated_at::VARCHAR AS method_updated_at
        FROM {account_settings}
        """  # noqa: S608  # registered physical table name, not user input
    )
    methods = {
        str(record["account_id"]): _opt_str(record["default_cost_basis_method"])
        for record in _records(frame)
    }
    updated_at = {
        str(record["account_id"]): timestamp
        for record in _records(frame)
        if (timestamp := _opt_timestamp(record["method_updated_at"])) is not None
    }
    for account_id, changed_at in _load_app_mutation_watermarks(
        context, "account_settings"
    ).items():
        latest = _latest(updated_at.get(account_id), changed_at)
        if latest is not None:
            updated_at[account_id] = latest
    return methods, updated_at


def load_currency_accounting(
    context: ExecutionContext,
) -> CurrencyAccountingResult:
    """Load cache-only inputs and derive Currency lots and realized FX."""
    conversions = _load_materialized_conversions(context)
    home_currency, home_updated_at = _load_home_currency(context)
    stored_rate = _load_stored_rates(context)
    sales = _load_security_sales(
        context,
        home_currency=home_currency,
        home_updated_at=home_updated_at,
        stored_rate=stored_rate,
    )
    account_methods, account_method_updated_at = _load_account_methods(context)
    return _derive_currency_accounting(
        conversions,
        sales,
        account_methods,
        account_method_updated_at=account_method_updated_at,
    )
