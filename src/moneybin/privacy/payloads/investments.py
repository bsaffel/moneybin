# src/moneybin/privacy/payloads/investments.py
"""Typed payload dataclasses for the investments surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly — mirroring ``payloads/accounts.py``.

Annotations mirror the Task-13b taxonomy classification of the
corresponding ``core.*`` columns exactly (``privacy/taxonomy.py``
``CLASSIFICATION`` entries for ``fct_investment_transactions``,
``fct_investment_lots``, ``fct_realized_gains``, ``dim_holdings``, and
``dim_securities``): ``cost_basis``/``proceeds``/``gain_loss``/``average_cost``
→ ``BALANCE``, ``quantity``/``price``/``amount``/``fees`` → ``TXN_AMOUNT``,
ids → ``RECORD_ID``, dates → ``TXN_DATE``, enums/tickers/names → ``TXN_TYPE``.

Sensitivity is DERIVED from these annotations, not hardcoded. Because
``BALANCE``/``TXN_AMOUNT`` are Tier.HIGH, ``investments`` /
``investments_holdings`` / ``investments_lots`` / ``investments_gains``
resolve to ``"high"`` sensitivity — not the ``"medium"`` shown in
``docs/specs/investments-data-model.md``'s illustrative envelope example.
That spec text predates the Task-13b classification decisions and is
approximate (see ``.superpowers/sdd/progress.md`` BASE-for-Task-16 note:
"plan's tiers are approximate"). ``investments_securities`` carries only
``TXN_TYPE``/``CURRENCY``/``RECORD_ID`` fields (Tier.LOW) and resolves to
``"low"``, matching the spec.

The service layer (``services/investment_service.py``) returns its own
internal read-result dataclasses (``EventRow``/``HoldingRow``/``LotRow``/
``RealizedGainRow``/``SecurityRow``, wrapped in ``*Result``) — those carry
no privacy metadata. The MCP tool boundary (``mcp/tools/investments.py``)
maps service results into the payloads below via each payload's
``from_result``/``from_row`` classmethod, the same boundary-mapping pattern
``accounts_set`` uses for ``AccountSettingsPayload`` (built from
``AccountSettings.to_dict()``, not by annotating the service dataclass).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# Runtime, not TYPE_CHECKING: Pydantic resolves the annotation when it builds
# the model. `payloads.currency` pulls in nothing heavy of its own.
from moneybin.privacy.payloads.currency import FxRatePayload
from moneybin.privacy.taxonomy import DataClass

# Every payload below classifies its `warnings` field AGGREGATE (Tier.LOW), not
# DESCRIPTION: these are canned system-generated diagnostic strings (the
# Pillar-C caveat, an oversold-row count) — never interpolated user-authored
# free text — so classifying them as DESCRIPTION (Tier.MEDIUM) would
# over-classify and, for InvestmentSecuritiesPayload (which has no other
# elevated field), wrongly push the derived tier to "medium" instead of the
# intended "low". (Each `warnings` field below points back to this comment
# rather than repeating it.)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from moneybin.services.investment_service import (
        EventRow,
        EventsResult,
        GainsResult,
        HoldingRow,
        HoldingsResult,
        LotRow,
        LotsResult,
        RealizedGainRow,
        SecuritiesResult,
        SecurityRow,
    )
    from moneybin.services.price_service import PricesResult
    from moneybin.services.security_links_service import (
        PendingSecurityLinkGroup,
    )

# ---------------------------------------------------------------------------
# investments — ledger events (core.fct_investment_transactions)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvestmentEventRow:
    """One row of core.fct_investment_transactions (the investments ledger read)."""

    investment_transaction_id: Annotated[str, DataClass.RECORD_ID]
    account_id: Annotated[str, DataClass.RECORD_ID]
    security_id: Annotated[str | None, DataClass.RECORD_ID]
    trade_date: Annotated[date, DataClass.TXN_DATE]
    settlement_date: Annotated[date | None, DataClass.TXN_DATE]
    original_acquisition_date: Annotated[date | None, DataClass.TXN_DATE]
    type: Annotated[str, DataClass.TXN_TYPE]
    subtype: Annotated[str | None, DataClass.TXN_TYPE]
    event_group_id: Annotated[str | None, DataClass.RECORD_ID]
    quantity: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    price: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    amount: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    fees: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    currency_code: Annotated[str, DataClass.CURRENCY]
    description: Annotated[str | None, DataClass.DESCRIPTION]

    @classmethod
    def from_row(cls, row: EventRow) -> InvestmentEventRow:
        """Map one service ``EventRow`` into the surfaced payload row."""
        return cls(
            investment_transaction_id=row.investment_transaction_id,
            account_id=row.account_id,
            security_id=row.security_id,
            trade_date=row.trade_date,
            settlement_date=row.settlement_date,
            original_acquisition_date=row.original_acquisition_date,
            type=row.type,
            subtype=row.subtype,
            event_group_id=row.event_group_id,
            quantity=row.quantity,
            price=row.price,
            amount=row.amount,
            fees=row.fees,
            currency_code=row.currency_code,
            description=row.description,
        )


@dataclass(frozen=True, slots=True)
class InvestmentEventsPayload:
    """Payload for the ``investments`` tool (ledger list)."""

    rows: list[InvestmentEventRow]
    # AGGREGATE (Tier.LOW) — rationale in the module-level comment above.
    warnings: Annotated[list[str], DataClass.AGGREGATE] = field(default_factory=list)

    @classmethod
    def from_result(cls, result: EventsResult) -> InvestmentEventsPayload:
        """Build the payload from ``InvestmentService.list_events()`` output."""
        return cls(
            rows=[InvestmentEventRow.from_row(r) for r in result.rows],
            warnings=result.warnings,
        )


# ---------------------------------------------------------------------------
# investments_holdings — current positions (core.dim_holdings)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvestmentHoldingRow:
    """One current position — cost basis plus the Pillar-C valuation.

    The six valuation fields mirror the ``core.dim_holdings`` classifications in
    ``privacy/taxonomy.py``: ``market_value``/``unrealized_gain`` are BALANCE
    (a held "stock" figure, like ``cost_basis``); ``price_date``/
    ``days_since_observed`` are TIMESTAMP_OBSERVABILITY (a public close's date,
    and a value bijective with it); ``price_source``/``valuation_status`` are
    TXN_TYPE (routing tags). None of them raises the payload's derived tier —
    ``cost_basis`` already carries BALANCE.
    """

    account_id: Annotated[str, DataClass.RECORD_ID]
    security_id: Annotated[str, DataClass.RECORD_ID]
    quantity: Annotated[Decimal, DataClass.TXN_AMOUNT]
    cost_basis: Annotated[Decimal, DataClass.BALANCE]
    average_cost: Annotated[Decimal | None, DataClass.BALANCE]
    currency_code: Annotated[str, DataClass.CURRENCY]
    market_value: Annotated[Decimal | None, DataClass.BALANCE]
    unrealized_gain: Annotated[Decimal | None, DataClass.BALANCE]
    price_date: Annotated[date | None, DataClass.TIMESTAMP_OBSERVABILITY]
    price_source: Annotated[str | None, DataClass.TXN_TYPE]
    days_since_observed: Annotated[int | None, DataClass.TIMESTAMP_OBSERVABILITY]
    valuation_status: Annotated[str, DataClass.TXN_TYPE]

    @classmethod
    def from_row(cls, row: HoldingRow) -> InvestmentHoldingRow:
        """Map one service ``HoldingRow`` into the surfaced payload row."""
        return cls(
            account_id=row.account_id,
            security_id=row.security_id,
            quantity=row.quantity,
            cost_basis=row.cost_basis,
            average_cost=row.average_cost,
            currency_code=row.currency_code,
            market_value=row.market_value,
            unrealized_gain=row.unrealized_gain,
            price_date=row.price_date,
            price_source=row.price_source,
            days_since_observed=row.days_since_observed,
            valuation_status=row.valuation_status,
        )


@dataclass(frozen=True, slots=True)
class InvestmentHoldingsPayload:
    """Payload for ``investments_holdings``.

    ``max_days_since_observed`` mirrors the row field's
    TIMESTAMP_OBSERVABILITY classification — it is the maximum of that column
    across the priced rows, so it carries no more than the rows already do.
    ``total_market_value`` and ``market_value_by_currency`` are BALANCE for the
    same reason: they are sums of the rows' BALANCE ``market_value``, and
    classifying a portfolio total AGGREGATE would under-classify real money.
    ``total_market_value_currency`` is CURRENCY like every other currency code —
    a converted total names its unit, and the unit is not the money.
    ``applied_rates`` reuses ``FxRatePayload``, so the rate behind a converted
    total is published with the same six fields and the same classes as the
    ``fx rate`` surface (Requirement 10); it is empty when nothing converted.
    """

    rows: list[InvestmentHoldingRow]
    # AGGREGATE (Tier.LOW) — rationale in the module-level comment above.
    warnings: Annotated[list[str], DataClass.AGGREGATE] = field(default_factory=list)
    max_days_since_observed: Annotated[
        int | None, DataClass.TIMESTAMP_OBSERVABILITY
    ] = None
    total_market_value: Annotated[Decimal | None, DataClass.BALANCE] = None
    total_market_value_currency: Annotated[str | None, DataClass.CURRENCY] = None
    market_value_by_currency: Annotated[dict[str, Decimal], DataClass.BALANCE] = field(
        default_factory=dict
    )
    applied_rates: list[FxRatePayload] = field(default_factory=list)

    @classmethod
    def from_result(cls, result: HoldingsResult) -> InvestmentHoldingsPayload:
        """Build the payload from ``InvestmentService.holdings()`` output."""
        return cls(
            rows=[InvestmentHoldingRow.from_row(r) for r in result.rows],
            warnings=result.warnings,
            max_days_since_observed=result.max_days_since_observed,
            total_market_value=result.total_market_value,
            total_market_value_currency=result.total_market_value_currency,
            market_value_by_currency=result.market_value_by_currency,
            applied_rates=[
                FxRatePayload.from_resolved(rate) for rate in result.applied_rates
            ],
        )


# ---------------------------------------------------------------------------
# investments_lots — tax lots (core.fct_investment_lots)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvestmentLotRow:
    """One tax lot (open or closed) from core.fct_investment_lots."""

    lot_id: Annotated[str, DataClass.RECORD_ID]
    account_id: Annotated[str, DataClass.RECORD_ID]
    security_id: Annotated[str, DataClass.RECORD_ID]
    acquisition_date: Annotated[date, DataClass.TXN_DATE]
    acquisition_type: Annotated[str, DataClass.TXN_TYPE]
    original_quantity: Annotated[Decimal, DataClass.TXN_AMOUNT]
    remaining_quantity: Annotated[Decimal, DataClass.TXN_AMOUNT]
    cost_basis_total: Annotated[Decimal, DataClass.BALANCE]
    cost_basis_remaining: Annotated[Decimal, DataClass.BALANCE]
    cost_basis_method: Annotated[str, DataClass.TXN_TYPE]
    currency_code: Annotated[str, DataClass.CURRENCY]
    is_open: Annotated[bool, DataClass.TXN_TYPE]
    basis_incomplete: Annotated[bool, DataClass.TXN_TYPE]

    @classmethod
    def from_row(cls, row: LotRow) -> InvestmentLotRow:
        """Map one service ``LotRow`` into the surfaced payload row."""
        return cls(
            lot_id=row.lot_id,
            account_id=row.account_id,
            security_id=row.security_id,
            acquisition_date=row.acquisition_date,
            acquisition_type=row.acquisition_type,
            original_quantity=row.original_quantity,
            remaining_quantity=row.remaining_quantity,
            cost_basis_total=row.cost_basis_total,
            cost_basis_remaining=row.cost_basis_remaining,
            cost_basis_method=row.cost_basis_method,
            currency_code=row.currency_code,
            is_open=row.is_open,
            basis_incomplete=row.basis_incomplete,
        )


@dataclass(frozen=True, slots=True)
class InvestmentLotsPayload:
    """Payload for ``investments_lots``."""

    rows: list[InvestmentLotRow]
    # AGGREGATE (Tier.LOW) — rationale in the module-level comment above.
    warnings: Annotated[list[str], DataClass.AGGREGATE] = field(default_factory=list)

    @classmethod
    def from_result(cls, result: LotsResult) -> InvestmentLotsPayload:
        """Build the payload from ``InvestmentService.lots()`` output."""
        return cls(
            rows=[InvestmentLotRow.from_row(r) for r in result.rows],
            warnings=result.warnings,
        )


# ---------------------------------------------------------------------------
# investments_gains — realized gain/loss (core.fct_realized_gains)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvestmentGainRow:
    """One (disposal, consumed lot) realized gain/loss slice — the 1099-B grain."""

    realized_gain_id: Annotated[str, DataClass.RECORD_ID]
    account_id: Annotated[str, DataClass.RECORD_ID]
    security_id: Annotated[str, DataClass.RECORD_ID]
    disposal_txn_id: Annotated[str, DataClass.RECORD_ID]
    lot_id: Annotated[str, DataClass.RECORD_ID]
    quantity: Annotated[Decimal, DataClass.TXN_AMOUNT]
    acquisition_date: Annotated[date, DataClass.TXN_DATE]
    disposal_date: Annotated[date, DataClass.TXN_DATE]
    proceeds: Annotated[Decimal, DataClass.BALANCE]
    cost_basis: Annotated[Decimal, DataClass.BALANCE]
    gain_loss: Annotated[Decimal, DataClass.BALANCE]
    term: Annotated[str, DataClass.TXN_TYPE]
    cost_basis_method: Annotated[str, DataClass.TXN_TYPE]
    basis_incomplete: Annotated[bool, DataClass.TXN_TYPE]
    currency_code: Annotated[str, DataClass.CURRENCY]

    @classmethod
    def from_row(cls, row: RealizedGainRow) -> InvestmentGainRow:
        """Map one service ``RealizedGainRow`` into the surfaced payload row."""
        return cls(
            realized_gain_id=row.realized_gain_id,
            account_id=row.account_id,
            security_id=row.security_id,
            disposal_txn_id=row.disposal_txn_id,
            lot_id=row.lot_id,
            quantity=row.quantity,
            acquisition_date=row.acquisition_date,
            disposal_date=row.disposal_date,
            proceeds=row.proceeds,
            cost_basis=row.cost_basis,
            gain_loss=row.gain_loss,
            term=row.term,
            cost_basis_method=row.cost_basis_method,
            basis_incomplete=row.basis_incomplete,
            currency_code=row.currency_code,
        )


@dataclass(frozen=True, slots=True)
class InvestmentGainsPayload:
    """Payload for ``investments_gains``."""

    rows: list[InvestmentGainRow]
    # AGGREGATE (Tier.LOW) — rationale in the module-level comment above.
    warnings: Annotated[list[str], DataClass.AGGREGATE] = field(default_factory=list)

    @classmethod
    def from_result(cls, result: GainsResult) -> InvestmentGainsPayload:
        """Build the payload from ``InvestmentService.gains()`` output."""
        return cls(
            rows=[InvestmentGainRow.from_row(r) for r in result.rows],
            warnings=result.warnings,
        )


# ---------------------------------------------------------------------------
# investments_securities — the catalog (core.dim_securities)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvestmentSecurityRow:
    """One catalog entry from core.dim_securities (projects app.securities)."""

    security_id: Annotated[str, DataClass.RECORD_ID]
    name: Annotated[str, DataClass.TXN_TYPE]
    security_type: Annotated[str, DataClass.TXN_TYPE]
    ticker: Annotated[str | None, DataClass.TXN_TYPE]
    exchange: Annotated[str | None, DataClass.TXN_TYPE]
    cusip: Annotated[str | None, DataClass.TXN_TYPE]
    isin: Annotated[str | None, DataClass.TXN_TYPE]
    figi: Annotated[str | None, DataClass.TXN_TYPE]
    coingecko_id: Annotated[str | None, DataClass.TXN_TYPE]
    is_cash_equivalent: Annotated[bool | None, DataClass.TXN_TYPE]
    currency_code: Annotated[str, DataClass.CURRENCY]

    @classmethod
    def from_row(cls, row: SecurityRow) -> InvestmentSecurityRow:
        """Map one service ``SecurityRow`` into the surfaced payload row."""
        return cls(
            security_id=row.security_id,
            name=row.name,
            security_type=row.security_type,
            ticker=row.ticker,
            exchange=row.exchange,
            cusip=row.cusip,
            isin=row.isin,
            figi=row.figi,
            coingecko_id=row.coingecko_id,
            is_cash_equivalent=row.is_cash_equivalent,
            currency_code=row.currency_code,
        )


@dataclass(frozen=True, slots=True)
class InvestmentSecuritiesPayload:
    """Payload for ``investments_securities``."""

    rows: list[InvestmentSecurityRow]
    # AGGREGATE (Tier.LOW) — rationale in the module-level comment above.
    warnings: Annotated[list[str], DataClass.AGGREGATE] = field(default_factory=list)

    @classmethod
    def from_result(cls, result: SecuritiesResult) -> InvestmentSecuritiesPayload:
        """Build the payload from ``InvestmentService.list_securities()`` output."""
        return cls(
            rows=[InvestmentSecurityRow.from_row(r) for r in result.rows],
            warnings=result.warnings,
        )


# ---------------------------------------------------------------------------
# Dormant coarse investment read
# ---------------------------------------------------------------------------


class InvestmentsEventsView(BaseModel):
    """Paginated investment-ledger events."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["events"] = "events"
    rows: list[InvestmentEventRow]
    warnings: Annotated[list[str], DataClass.AGGREGATE]


class InvestmentsHoldingsView(BaseModel):
    """Paginated current positions with portfolio-level valuation.

    The five portfolio fields mirror ``InvestmentHoldingsPayload``'s (same
    classifications, same rationale). They are computed over the full position
    set, so a paginated ``rows`` page does not change them — including
    ``applied_rates``, which names the rates behind the whole portfolio's
    total, not the ones behind the page.
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["holdings"] = "holdings"
    rows: list[InvestmentHoldingRow]
    warnings: Annotated[list[str], DataClass.AGGREGATE]
    max_days_since_observed: Annotated[
        int | None, DataClass.TIMESTAMP_OBSERVABILITY
    ] = None
    total_market_value: Annotated[Decimal | None, DataClass.BALANCE] = None
    total_market_value_currency: Annotated[str | None, DataClass.CURRENCY] = None
    market_value_by_currency: Annotated[dict[str, Decimal], DataClass.BALANCE] = Field(
        default_factory=dict
    )
    applied_rates: list[FxRatePayload] = Field(default_factory=list)


class InvestmentsLotsView(BaseModel):
    """Paginated open tax lots."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["lots"] = "lots"
    rows: list[InvestmentLotRow]
    warnings: Annotated[list[str], DataClass.AGGREGATE]


class InvestmentsGainsView(BaseModel):
    """Paginated realized gains."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["gains"] = "gains"
    rows: list[InvestmentGainRow]
    warnings: Annotated[list[str], DataClass.AGGREGATE]


class InvestmentsSecuritiesView(BaseModel):
    """Paginated securities catalog."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["securities"] = "securities"
    rows: list[InvestmentSecurityRow]
    warnings: Annotated[list[str], DataClass.AGGREGATE]


InvestmentsCoarsePayload = Annotated[
    InvestmentsEventsView
    | InvestmentsHoldingsView
    | InvestmentsLotsView
    | InvestmentsGainsView
    | InvestmentsSecuritiesView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Write-result payloads: investments_record, investments_securities_set,
# investments_lots_select
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvestmentRecordPayload:
    """Payload for ``investments_record`` — batch event-recording result.

    ``investment_transaction_ids`` carries RECORD_ID tier (opaque ids, safe
    to echo back). ``error_details`` uses ``list[dict[str, str]]`` (not a
    typed nested dataclass) so the introspection walker does not recurse
    into per-item content — same rationale/shape as
    ``payloads/categories.py``'s ``MerchantsCreatePayload``.
    """

    investment_transaction_ids: Annotated[list[str], DataClass.RECORD_ID]
    error_details: Annotated[list[dict[str, str]], DataClass.AGGREGATE] = field(
        default_factory=list
    )


@dataclass(frozen=True, slots=True)
class InvestmentSecuritySetPayload:
    """Payload for ``investments_securities_set`` — the created/updated id."""

    security_id: Annotated[str, DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class InvestmentLotSelectionEntry:
    """One ``(lot_id, quantity)`` pair in an ``investments_lots_select`` result."""

    lot_id: Annotated[str, DataClass.RECORD_ID]
    quantity: Annotated[Decimal, DataClass.TXN_AMOUNT]


@dataclass(frozen=True, slots=True)
class InvestmentLotsSelectPayload:
    """Payload for ``investments_lots_select`` — the selection set that was applied."""

    disposal_txn_id: Annotated[str, DataClass.RECORD_ID]
    selections: list[InvestmentLotSelectionEntry]


# ---------------------------------------------------------------------------
# investments_securities_links_pending / _history (M1G.4 Task 12)
#
# Field classifications mirror the name-based CLASSIFICATION registry already
# shipped for app.security_link_decisions (privacy/taxonomy.py, Task 7):
# ref_kind/source_type/status/decided_by -> TXN_TYPE, ref_value/decision_id/
# candidate_security_id -> RECORD_ID, confidence_score -> AGGREGATE,
# match_reason -> USER_NOTE, decided_at -> TIMESTAMP_OBSERVABILITY.
# candidate_ticker/candidate_name/provider_ticker/provider_name mirror
# app.securities.ticker/.name (TXN_TYPE), same as InvestmentSecurityRow above.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SecurityLinkCandidateRow:
    """One candidate merge-survivor proposal in a pending-review group."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    candidate_security_id: Annotated[str, DataClass.RECORD_ID]
    candidate_ticker: Annotated[str | None, DataClass.TXN_TYPE]
    candidate_name: Annotated[str | None, DataClass.TXN_TYPE]
    confidence: Annotated[float | None, DataClass.AGGREGATE]
    match_reason: Annotated[str | None, DataClass.USER_NOTE]


@dataclass(frozen=True, slots=True)
class SecurityLinkPendingGroup:
    """One provider ref awaiting review + its candidate merge-survivor proposals."""

    ref_kind: Annotated[str, DataClass.TXN_TYPE]
    ref_value: Annotated[str, DataClass.RECORD_ID]
    source_type: Annotated[str, DataClass.TXN_TYPE]
    provider_ticker: Annotated[str | None, DataClass.TXN_TYPE]
    provider_name: Annotated[str | None, DataClass.TXN_TYPE]
    candidates: list[SecurityLinkCandidateRow]

    @classmethod
    def from_domain(cls, g: PendingSecurityLinkGroup) -> SecurityLinkPendingGroup:
        """Map a service ``PendingSecurityLinkGroup`` into the payload group."""
        return cls(
            ref_kind=g.ref_kind,
            ref_value=g.ref_value,
            source_type=g.source_type,
            provider_ticker=g.provider_ticker,
            provider_name=g.provider_name,
            candidates=[
                SecurityLinkCandidateRow(
                    decision_id=c.decision_id,
                    candidate_security_id=c.candidate_security_id,
                    candidate_ticker=c.candidate_ticker,
                    candidate_name=c.candidate_name,
                    confidence=c.confidence,
                    match_reason=c.match_reason,
                )
                for c in g.candidates
            ],
        )


@dataclass(frozen=True, slots=True)
class SecurityLinksPendingPayload:
    """Payload for ``investments securities links pending`` — pending queue grouped by provider ref."""

    groups: list[SecurityLinkPendingGroup]
    n_pending: Annotated[int, DataClass.AGGREGATE]

    @classmethod
    def from_service(
        cls,
        groups: Iterable[PendingSecurityLinkGroup],
        n_pending: int,
    ) -> SecurityLinksPendingPayload:
        """Build the pending payload from ``SecurityLinksService.pending()`` output."""
        return cls(
            groups=[SecurityLinkPendingGroup.from_domain(g) for g in groups],
            n_pending=n_pending,
        )


@dataclass(frozen=True, slots=True)
class SecurityLinksSetPayload:
    """Payload for ``investments_securities_links_set`` — confirmation of the decision applied."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]  # "accepted" or "rejected"


@dataclass(frozen=True, slots=True)
class SecurityLinkHistoryRow:
    """One past security-link decision (``investments securities links history`` result)."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    ref_kind: Annotated[str, DataClass.TXN_TYPE]
    ref_value: Annotated[str, DataClass.RECORD_ID]
    source_type: Annotated[str, DataClass.TXN_TYPE]
    provider_ticker: Annotated[str | None, DataClass.TXN_TYPE]
    provider_name: Annotated[str | None, DataClass.TXN_TYPE]
    candidate_security_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    decided_by: Annotated[str, DataClass.TXN_TYPE]
    decided_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    confidence: Annotated[float | None, DataClass.AGGREGATE]
    match_reason: Annotated[str | None, DataClass.USER_NOTE]

    @classmethod
    def from_decision_row(cls, r: dict[str, Any]) -> SecurityLinkHistoryRow:
        """Map a decoded ``security_link_decisions`` row into the history payload."""
        return cls(
            decision_id=r["decision_id"],
            ref_kind=r["ref_kind"],
            ref_value=r["ref_value"],
            source_type=r["source_type"],
            provider_ticker=r.get("provider_ticker"),
            provider_name=r.get("provider_name"),
            candidate_security_id=r["candidate_security_id"],
            status=r["status"],
            decided_by=r["decided_by"],
            decided_at=str(r["decided_at"])
            if r.get("decided_at") is not None
            else None,
            confidence=(
                float(r["confidence_score"])
                if r.get("confidence_score") is not None
                else None
            ),
            match_reason=r.get("match_reason"),
        )


@dataclass(frozen=True, slots=True)
class SecurityLinksHistoryPayload:
    """Payload for ``investments securities links history`` — decision log, newest first."""

    decisions: list[SecurityLinkHistoryRow]

    @classmethod
    def from_rows(cls, rows: Iterable[dict[str, Any]]) -> SecurityLinksHistoryPayload:
        """Build the history payload from ``SecurityLinksService.history()`` rows."""
        return cls(
            decisions=[SecurityLinkHistoryRow.from_decision_row(r) for r in rows]
        )


@dataclass(frozen=True, slots=True)
class InvestmentPricePullPayload:
    """Payload for ``investments_prices_pull`` — counts plus what stayed unpriced.

    Deliberately carries no price and no security identifier beyond the id: a
    refresh summary is about coverage, not about what anything is worth.
    """

    rows_written: Annotated[int, DataClass.AGGREGATE]
    observations: Annotated[int, DataClass.AGGREGATE]
    securities_priced: Annotated[int, DataClass.AGGREGATE]
    queued_for_review: Annotated[int, DataClass.AGGREGATE]
    unpriced: list[InvestmentUnpricedEntry]
    failed_sources: list[InvestmentFailedSourceEntry]
    # Whether these rows reached core. A pull writes raw.security_prices; every
    # consumer reads core.fct_security_prices. Without this an agent cannot tell
    # a current valuation from one that predates the closes it just fetched.
    refreshed: Annotated[bool, DataClass.AGGREGATE] = False
    refresh_error: Annotated[str | None, DataClass.AGGREGATE] = None


@dataclass(frozen=True, slots=True)
class InvestmentUnpricedEntry:
    """One held security a refresh could not price, and why."""

    security_id: Annotated[str, DataClass.RECORD_ID]
    reason: Annotated[str, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class InvestmentFailedSourceEntry:
    """One price source that failed as a whole, and the message to act on.

    A whole-source failure no longer aborts the refresh, so it needs somewhere to
    be seen: without this the run reports every one of that source's securities
    unpriced and never says the token was missing. ``message`` is safe to carry —
    ``PriceFeedError`` is documented as whole-batch conditions only, so it names
    a provider and a condition, never a security or a holding.
    """

    source_type: Annotated[str, DataClass.TXN_TYPE]
    message: Annotated[str, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class InvestmentPriceMarkPayload:
    """Payload for ``investments_prices_set`` / ``_delete`` — the mark's identity.

    ``close`` is TXN_AMOUNT, matching the registry entries for
    ``app.security_price_overrides.close`` and ``core.fct_security_prices.close``.
    A mark is a number the user wrote — a 409A valuation, a private-company
    estimate — so it is a personal financial fact, not a market observation that
    happens to be stored locally. ``market_value`` (quantity x close) reveals
    position size on top of that and stays BALANCE on the holdings payload.
    """

    security_id: Annotated[str, DataClass.RECORD_ID]
    price_date: Annotated[date, DataClass.TIMESTAMP_OBSERVABILITY]
    quote_currency: Annotated[str, DataClass.CURRENCY]
    close: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    removed: Annotated[bool, DataClass.AGGREGATE]
    # Whether the mark reached core. Same reason as InvestmentPricePullPayload:
    # the write lands in app.security_price_overrides and every consumer reads
    # core.fct_security_prices, so without this an agent cannot tell a corrected
    # valuation from one still serving the price the user just overrode.
    refreshed: Annotated[bool, DataClass.AGGREGATE] = False
    refresh_error: Annotated[str | None, DataClass.AGGREGATE] = None


@dataclass(frozen=True, slots=True)
class InvestmentPriceRow:
    """One resolved price in an ``investments_prices_list`` result.

    ``close`` is the resolved winner across provider, override, and trade-implied
    sources, so it carries the tier of the strictest of those — see
    ``core.fct_security_prices.close`` in the registry.
    """

    price_date: Annotated[date, DataClass.TIMESTAMP_OBSERVABILITY]
    quote_currency: Annotated[str, DataClass.CURRENCY]
    close: Annotated[Decimal, DataClass.TXN_AMOUNT]
    source_type: Annotated[str, DataClass.TXN_TYPE]
    price_basis: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class InvestmentPricesPayload:
    """Payload for ``investments_prices_list`` — the resolved series, newest first."""

    security_id: Annotated[str, DataClass.RECORD_ID]
    rows: list[InvestmentPriceRow]

    @classmethod
    def from_result(cls, result: PricesResult) -> InvestmentPricesPayload:
        """Map the service result onto the classified payload."""
        return cls(
            security_id=result.security_id,
            rows=[
                InvestmentPriceRow(
                    price_date=row.price_date,
                    quote_currency=row.quote_currency,
                    close=row.close,
                    source_type=row.source_type,
                    price_basis=row.price_basis,
                )
                for row in result.rows
            ],
        )
