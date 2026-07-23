# src/moneybin/mcp/tools/investments.py
"""Investments namespace tools — functional parity with the CLI `investments` group.

Sensitivity is derived per tool from its payload's classified fields (never
declared here): ``investments``, ``investments_holdings``, ``investments_lots``,
and ``investments_gains`` carry BALANCE/TXN_AMOUNT fields (cost basis,
quantity, proceeds, gain/loss) and resolve to ``high``; ``investments_securities``
carries only TXN_TYPE/CURRENCY/RECORD_ID fields and resolves to ``low``.

Read tools:   investments, investments_holdings, investments_lots,
              investments_gains, investments_securities,
              investments_securities_links_pending,
              investments_securities_links_history
Write tools:  investments_record, investments_securities_set,
              investments_lots_select, investments_securities_links_set

All tools delegate to InvestmentService — no business logic here. Free-text
account/security references resolve inside the service (Guard 2,
identifiers.md); ambiguous or unresolved references raise a UserError-family
exception that the ``@mcp_tool`` decorator converts to the standard error
envelope. The investments_securities_links_* review tools delegate to
SecurityLinksService instead — the agent-facing peer to the
`investments securities links` CLI (mirrors merchants_links_* /
accounts_links_*).

The granular callbacks named in ``_LEGACY_INTERNAL_CALLBACKS`` are internal
helpers retained for standard-boundary composition and parity. They are never
individually registered, remain undecorated, and are pinned by the
surface-budget tests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import date as _date
from decimal import Decimal
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, StrictBool

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.pagination import (
    KeysetPosition,
    decode_keyset_cursor,
    encode_keyset_cursor,
)
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.investments import (
    InvestmentEventsPayload,
    InvestmentGainsPayload,
    InvestmentHoldingsPayload,
    InvestmentLotSelectionEntry,
    InvestmentLotsPayload,
    InvestmentLotsSelectPayload,
    InvestmentRecordPayload,
    InvestmentsCoarsePayload,
    InvestmentSecuritiesPayload,
    InvestmentSecuritySetPayload,
    InvestmentsEventsView,
    InvestmentsGainsView,
    InvestmentsHoldingsView,
    InvestmentsLotsView,
    InvestmentsSecuritiesView,
    SecurityLinksHistoryPayload,
    SecurityLinksPendingPayload,
    SecurityLinksSetPayload,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_service import AccountService
from moneybin.services.entity_reference import (
    AmbiguousEntity,
    EntityCandidate,
    MissingEntity,
    resolve_entity_reference,
)
from moneybin.services.investment_service import InvestmentService
from moneybin.services.security_links_service import (
    SecurityLinkAcceptImpact,
    SecurityLinksService,
)


def _parse_date(value: str | None) -> _date | None:
    return _date.fromisoformat(value) if value is not None else None


def _parse_decimal(value: object) -> Decimal | None:
    # str(...) first: a JSON number arriving as a Python float would inject
    # binary rounding noise straight into a money/quantity field. Every other
    # money parser in the codebase (accounts.py, _parse_selection below) does
    # the same. None passes through unchanged.
    return Decimal(str(value)) if value is not None else None


# ─── Read tools ─────────────────────────────────────────────────────────────


def investments(
    account: str | None = None,
    security: str | None = None,
    type_filter: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> ResponseEnvelope[InvestmentEventsPayload]:
    """List investment ledger events (buys, sells, dividends, corporate actions, ...).

    Args:
        account: Account ID or free-text reference (resolved to an id;
            ambiguous or unresolved references raise an error).
        security: Ticker, CUSIP, ISIN, or catalog name (resolved to a
            security_id the same way).
        type_filter: Restrict to one event type (buy, sell, reinvest,
            dividend, interest, capital_gain_distribution, transfer_in,
            transfer_out, deposit, withdrawal, split, fee,
            return_of_capital, other).
        from_date: ISO date (YYYY-MM-DD), inclusive lower bound on trade_date.
        to_date: ISO date (YYYY-MM-DD), inclusive upper bound on trade_date.

    Amounts (quantity/price/amount/fees) use the per-type sign convention
    recorded by investments_record: positive quantity = acquisition, negative
    = disposal; amount is negative for cash out (buy/reinvest), positive for
    cash in (sell/dividend/interest/etc). Amounts are in the currency named
    by `summary.display_currency`.
    """
    with get_database(read_only=True) as db:
        result = InvestmentService(db).list_events(
            account_ref=account,
            security_ref=security,
            type_filter=type_filter,
            date_from=_parse_date(from_date),
            date_to=_parse_date(to_date),
        )
    return build_envelope(
        data=InvestmentEventsPayload.from_result(result),
        actions=[
            "Use investments(view='holdings') for current positions",
            "Use investments(view='gains') for realized gain/loss",
        ],
    )


def investments_holdings(
    account: str | None = None,
) -> ResponseEnvelope[InvestmentHoldingsPayload]:
    """Current positions with market value per (account, security).

    Args:
        account: Account ID or free-text reference (resolved to an id).

    Each row carries quantity, cost basis, average cost, and — when a close
    resolved — `market_value`, `unrealized_gain` (signed: negative below
    cost), the `price_date` of the close used, and `days_since_observed`.
    `valuation_status` is one of `valued` (close is today's),
    `carried_forward` (the most recent close is older), `unpriced` (no close
    resolved), or `withheld` (the share count is known wrong). The last two
    report `market_value`/`unrealized_gain` as null, never zero, and
    `data.warnings` names how many rows those are.

    `data.max_days_since_observed` is the age in days of the stalest close any
    published figure rests on — the largest `days_since_observed` across the
    priced rows, null when no position priced. Read it before reporting a
    portfolio value: a large number means the figures come from an old close.

    Amounts are in each row's own `currency_code`, not converted to
    `summary.display_currency`. Do not sum `market_value` across rows: read
    `data.total_market_value`, which is populated only when every priced
    position shares one currency and null when they do not. When it is null,
    `data.market_value_by_currency` carries the per-currency split and no
    single portfolio figure exists.
    """
    with get_database(read_only=True) as db:
        result = InvestmentService(db).holdings(account_ref=account)
    return build_envelope(
        data=InvestmentHoldingsPayload.from_result(result),
        actions=[
            "Use investments(view='lots') for per-lot basis",
            "Use investments(view='gains') for realized gain/loss",
        ],
    )


def investments_lots(
    account: str | None = None,
    security: str | None = None,
    open_only: bool = True,
) -> ResponseEnvelope[InvestmentLotsPayload]:
    """Tax lots with remaining quantity and basis. Open lots only by default.

    Args:
        account: Account ID or free-text reference (resolved to an id).
        security: Ticker, CUSIP, ISIN, or catalog name (resolved to a
            security_id).
        open_only: Show only open lots (default) or the full open+closed
            history when False.

    A row with `basis_incomplete=true` means it opened with no supplied
    basis (e.g. a transfer_in recorded with unknown cost basis) — its
    cost_basis_total/remaining are 0.00, not a real zero. When any row is
    incomplete, `data.warnings` names the count. Amounts are in the
    currency named by `summary.display_currency`.
    """
    with get_database(read_only=True) as db:
        result = InvestmentService(db).lots(
            account_ref=account, security_ref=security, open_only=open_only
        )
    return build_envelope(
        data=InvestmentLotsPayload.from_result(result),
        actions=[
            "Use investments_lots_select to override FIFO for a disposal",
            "Use investments(view='gains') for realized gain/loss",
        ],
    )


def investments_gains(
    account: str | None = None,
    security: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    term: str | None = None,
) -> ResponseEnvelope[InvestmentGainsPayload]:
    """Realized gain/loss (the 1099-B surface) from the realized-gains fact table.

    Args:
        account: Account ID or free-text reference (resolved to an id).
        security: Ticker, CUSIP, ISIN, or catalog name (resolved to a
            security_id).
        from_date: ISO date (YYYY-MM-DD), inclusive lower bound on disposal_date.
        to_date: ISO date (YYYY-MM-DD), inclusive upper bound on disposal_date.
        term: Restrict to one holding term: "short" or "long".

    A row with `basis_incomplete=true` means the disposal exceeded tracked
    lots (oversold) or the acquisition lot is missing — its gain/loss is
    computed from zero cost basis and is conservative, not authoritative.
    When any row is incomplete, `data.warnings` names the count. Amounts are
    in the currency named by `summary.display_currency`.
    """
    with get_database(read_only=True) as db:
        result = InvestmentService(db).gains(
            account_ref=account,
            security_ref=security,
            date_from=_parse_date(from_date),
            date_to=_parse_date(to_date),
            term=term,
        )
    return build_envelope(
        data=InvestmentGainsPayload.from_result(result),
        actions=["Use investments(view='lots') for lot-level detail"],
    )


def investments_securities(
    security_type: str | None = None,
) -> ResponseEnvelope[InvestmentSecuritiesPayload]:
    """List the manually-maintained securities catalog.

    Args:
        security_type: Filter by instrument type (equity, etf, mutual_fund,
            bond, crypto, cash, other).

    Reference data only — no amounts, no per-user holdings.
    """
    with get_database(read_only=True) as db:
        result = InvestmentService(db).list_securities(security_type=security_type)
    return build_envelope(
        data=InvestmentSecuritiesPayload.from_result(result),
        actions=["Use investments_securities_set to add or update a catalog entry"],
    )


# ─── Write tools ────────────────────────────────────────────────────────────


def _opt_str(value: object) -> str | None:
    return str(value) if value is not None else None


def _require_event_fields(item: dict[str, Any], index: int) -> tuple[str, str, str]:
    """Return (account, type, date) or raise naming the missing field(s)."""
    account = item.get("account")
    type_ = item.get("type")
    date_str = item.get("date")
    if not account or not type_ or not date_str:
        raise UserError(
            f"events[{index}]: 'account', 'type', and 'date' are required.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    return str(account), str(type_), str(date_str)


@mcp_tool(read_only=False)
def investments_record(
    events: list[dict[str, Any]],
) -> ResponseEnvelope[InvestmentRecordPayload]:
    """Record one or more investment ledger events (Shape 3 batch).

    Each item in `events` is a dict with keys:
    - `account` (required): account ID or free-text reference.
    - `type` (required): buy, sell, reinvest, dividend, interest,
      capital_gain_distribution, transfer_in, transfer_out, deposit,
      withdrawal, split, fee, return_of_capital, or other.
    - `date` (required): trade date, ISO YYYY-MM-DD.
    - `security` (optional): ticker, CUSIP, ISIN, or catalog name — resolved
      to a security_id; required for buy/sell/reinvest/transfer_in/
      transfer_out/split/return_of_capital, forbidden for deposit/withdrawal.
    - `quantity`, `price`, `amount`, `fees`, `basis` (optional): decimal
      strings, e.g. "10.5" (never floats — pass as strings to avoid binary
      rounding).
    - `subtype`, `acquired` (ISO date), `event_group_id`, `description`
      (optional).
    - `currency` (optional, default "USD"): ISO-4217 code.

    Sign convention: `quantity` is positive for acquisitions (buy, reinvest,
    transfer_in), negative for disposals (sell, transfer_out), and must be
    absent for cash-only events (dividend, interest,
    capital_gain_distribution, deposit, withdrawal, fee, return_of_capital).
    `amount` is negative for cash leaving the account (buy, reinvest,
    withdrawal, fee) and positive for cash arriving (sell, deposit,
    dividend, interest, capital_gain_distribution, return_of_capital);
    buy/sell/reinvest require a non-null amount — `transfer_in` does not:
    when the transferred-in cost basis is unknown, omit `basis`/`amount` and
    the resulting lot opens at zero basis flagged `basis_incomplete=true`
    (see `investments_lots`) rather than a value being invented. A `split`
    event carries the
    split multiplier in `quantity` (e.g. 2 for a 2:1 split) with price/
    amount/fees left unset. A `reinvest` event atomically writes the
    acquisition leg AND a paired income row sharing one `event_group_id` —
    both ids are returned.

    Resolution behavior is asymmetric by ref kind:
    - A bad/ambiguous ACCOUNT ref is a HARD failure: it aborts the ENTIRE
      call with NOTHING written (standard error envelope), same as a sign
      violation or unknown type/subtype.
    - A bad/ambiguous SECURITY ref is a SOFT, per-item failure: that event
      is skipped and reported in `data.error_details`, and the rest of the
      batch is still written.

    All events are validated and their refs resolved BEFORE any row is
    written: if any HARD failure is found anywhere in the batch, nothing is
    written at all, so a retry after fixing the offending event cannot
    double-insert the events that would otherwise have committed before the
    abort. Events skipped for an unresolved security are simply omitted from
    the write.

    Mutation surface: writes raw.manual_investment_transactions (one row per
    event, two for reinvest). No revert tool; every write is recorded in
    app.audit_log under action="investment.record".
    """
    if not events:
        return build_envelope(
            data=InvestmentRecordPayload(investment_transaction_ids=[]),
        )

    # Parse each dict into the typed event shape record_events consumes. A
    # missing required field or unparseable date/decimal raises here, before any
    # write. Validation, ref resolution, the SOFT security-skip, and the atomic
    # single-transaction write all live in record_events (one pass over the
    # batch, so each ref resolves exactly once).
    with get_database(read_only=False) as db:
        typed: list[dict[str, Any]] = []
        for index, item in enumerate(events):
            account, type_, date_str = _require_event_fields(item, index)
            typed.append({
                "account_ref": account,
                "security_ref": _opt_str(item.get("security")),
                "type_": type_,
                "subtype": _opt_str(item.get("subtype")),
                "trade_date": _date.fromisoformat(date_str),
                "quantity": _parse_decimal(item.get("quantity")),
                "price": _parse_decimal(item.get("price")),
                "amount": _parse_decimal(item.get("amount")),
                "fees": _parse_decimal(item.get("fees")),
                "acquired": _parse_date(item.get("acquired")),
                "basis": _parse_decimal(item.get("basis")),
                "event_group_id": _opt_str(item.get("event_group_id")),
                "currency_code": str(item.get("currency") or "USD"),
                "description": _opt_str(item.get("description")),
            })
        result = InvestmentService(db).record_events(
            typed, actor="mcp", created_by="mcp"
        )

    return build_envelope(
        data=InvestmentRecordPayload(
            investment_transaction_ids=result.investment_transaction_ids,
            error_details=result.error_details,
        ),
        actions=[
            "Use refresh_run to materialize them into "
            "core.fct_investment_transactions (and derive holdings, lots, gains)",
            "Use investments to view recorded events",
            "Use investments(view='holdings') to see updated positions",
        ],
    )


@mcp_tool(read_only=False)
def investments_securities_set(
    security_id: str | None = None,
    name: str | None = None,
    security_type: str | None = None,
    ticker: str | None = None,
    exchange: str | None = None,
    cusip: str | None = None,
    isin: str | None = None,
    figi: str | None = None,
    coingecko_id: str | None = None,
    is_cash_equivalent: bool | None = None,
    cost_basis_method: str | None = None,
    currency_code: str | None = None,
) -> ResponseEnvelope[InvestmentSecuritySetPayload]:
    """Create-or-update one securities-catalog entry (Shape 1b entity upsert).

    Pass `security_id=None` to CREATE a new entry — `name` and
    `security_type` are then required (equity, etf, mutual_fund, bond,
    crypto, cash, or other); other fields default per the catalog schema
    (`currency_code` defaults to "USD"). Pass an existing `security_id` to
    PARTIALLY UPDATE that entry — unset (None) fields keep their current
    value; `security_type` cannot be changed after creation. Updating a
    `security_id` that doesn't exist raises mutation_not_found.

    `cost_basis_method` (fifo, hifo, specific, or average) is a per-security
    override of the account/global default; "average" is valid only for
    mutual_fund or etf securities and raises mutation_invalid_input on any
    other security_type. Changing it on an update applies retroactively:
    core.fct_investment_lots/fct_realized_gains re-derive the full history
    from the current method on every refresh_run, so a disposal already
    realized under the old method silently gets a different cost basis (v1
    does not enforce IRS election lock-in).

    Mutation surface: writes app.securities. No delete tool exists for
    catalog entries in v1; revert by calling again with the prior values.
    """
    with get_database(read_only=False) as db:
        svc = InvestmentService(db)
        if security_id is None:
            if not name or not security_type:
                raise UserError(
                    "Creating a new security requires 'name' and 'security_type'.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            new_id = svc.upsert_security(
                security_id=None,
                name=name,
                security_type=security_type,
                ticker=ticker,
                exchange=exchange,
                cusip=cusip,
                isin=isin,
                figi=figi,
                coingecko_id=coingecko_id,
                is_cash_equivalent=is_cash_equivalent,
                cost_basis_method=cost_basis_method,
                currency_code=currency_code or "USD",
                actor="mcp",
            )
        else:
            if security_type is not None:
                raise UserError(
                    "security_type cannot be changed after creation.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            new_id = svc.set_security(
                security_id,
                name=name,
                ticker=ticker,
                exchange=exchange,
                cusip=cusip,
                isin=isin,
                figi=figi,
                coingecko_id=coingecko_id,
                is_cash_equivalent=is_cash_equivalent,
                cost_basis_method=cost_basis_method,
                currency_code=currency_code,
                actor="mcp",
            )
    return build_envelope(data=InvestmentSecuritySetPayload(security_id=new_id))


def _parse_selection(entry: dict[str, Any], index: int) -> tuple[str, Decimal]:
    lot_id = entry.get("lot_id")
    quantity = entry.get("quantity")
    if not lot_id or quantity is None:
        raise UserError(
            f"selections[{index}] requires 'lot_id' and 'quantity'.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    return str(lot_id), Decimal(str(quantity))


@mcp_tool(read_only=False)
def investments_lots_select(
    disposal_txn_id: str,
    selections: list[dict[str, Any]],
) -> ResponseEnvelope[InvestmentLotsSelectPayload]:
    """Set (or clear) the full specific-identification lot selection for a disposal.

    Shape 1a (collection state-set): the listed `(lot_id, quantity)` pairs
    REPLACE any prior selection for this disposal in full — an omitted lot
    is dropped, not left in place. Pass `selections=[]` to clear all
    overrides and revert the disposal to FIFO.

    Args:
        disposal_txn_id: investment_transaction_id of the disposal (must be
            a "sell"; other event types raise mutation_invalid_input).
        selections: List of `{"lot_id": ..., "quantity": "..."}` dicts
            (quantity as a decimal string). The selected quantities must sum
            to no more than the disposal's magnitude.

    Mutation surface: writes app.lot_selections. No revert tool; call again
    with the prior selections (or `[]`) to undo.
    """
    parsed = [_parse_selection(s, i) for i, s in enumerate(selections)]
    with get_database(read_only=False) as db:
        InvestmentService(db).select_lots(disposal_txn_id, parsed, actor="mcp")
    return build_envelope(
        data=InvestmentLotsSelectPayload(
            disposal_txn_id=disposal_txn_id,
            selections=[
                InvestmentLotSelectionEntry(lot_id=lot_id, quantity=qty)
                for lot_id, qty in parsed
            ],
        ),
        actions=[
            "Use refresh_run to materialize the updated selection into "
            "core.fct_realized_gains",
            "Use investments(view='gains') to see the updated allocation",
        ],
    )


# ─── Review tools (security links) ─────────────────────────────────────────


def investments_securities_links_pending() -> ResponseEnvelope[
    SecurityLinksPendingPayload
]:
    """List pending security merge decisions, grouped by provider ref.

    Returns the review queue of provider refs (a Plaid `plaid_security_id`
    or `institution_security_id`) with candidate merge-survivor proposals.
    Each group's header carries BOTH sides of the proposed merge —
    `provider_ticker`/`provider_name` (what's being merged) alongside each
    candidate's `candidate_ticker`/`candidate_name` (what it would merge
    into) — this matters most for a `fuzzy_name` proposal, where name
    similarity is the entire basis. Each candidate also carries
    `match_reason` (`identifier_tie`, `exchange_contradiction`,
    `fuzzy_name`, ...): the field that conveys HOW risky accepting is — an
    `identifier_tie` is a much safer accept than an
    `exchange_contradiction`, which signals the two instruments are
    probably NOT the same.

    Decide each group via investments_securities_links_set.
    """
    with get_database(read_only=True) as db:
        svc = SecurityLinksService(db, actor="mcp")
        groups = svc.pending()
        n_pending = svc.count_pending()
    payload = SecurityLinksPendingPayload.from_service(groups, n_pending)
    return build_envelope(
        data=payload,
        total_count=n_pending,
        actions=[
            "Use identity_links_decide with kind='security_link', "
            "decision='accept', decision_id, and target_id to merge",
            "Use identity_links_decide with kind='security_link', "
            "decision='reject', and decision_id to keep it distinct",
        ],
    )


@dataclass(frozen=True, slots=True)
class _MergeProposal:
    """One pending merge decision, flattened for the confirmation prompt."""

    decision_id: str
    ref_kind: str
    ref_value: str
    provider_ticker: str | None
    provider_name: str | None
    candidate_security_id: str
    candidate_ticker: str | None
    candidate_name: str | None
    match_reason: str | None
    provisional_security_id: str
    blast_radius: dict[str, int]


def _load_pending_proposal(decision_id: str) -> _MergeProposal:
    """Read the decision out of the live review queue, or raise if it isn't there."""
    with get_database(read_only=True) as db:
        service = SecurityLinksService(db, actor="mcp")
        groups = service.pending()
        for group in groups:
            for candidate in group.candidates:
                if candidate.decision_id == decision_id:
                    impact = service.accept_impact(
                        decision_id,
                        into=candidate.candidate_security_id,
                    )
                    return _MergeProposal(
                        decision_id=decision_id,
                        ref_kind=group.ref_kind,
                        ref_value=group.ref_value,
                        provider_ticker=group.provider_ticker,
                        provider_name=group.provider_name,
                        candidate_security_id=candidate.candidate_security_id,
                        candidate_ticker=candidate.candidate_ticker,
                        candidate_name=candidate.candidate_name,
                        match_reason=candidate.match_reason,
                        provisional_security_id=impact.provisional_security_id,
                        blast_radius=impact.blast_radius,
                    )
    raise UserError(
        f"No pending security merge decision '{decision_id}'.",
        code=error_codes.MUTATION_NOTHING_TO_DO,
        hint="List open decisions with reviews(kind='security_links').",
    )


def _security_link_binding(
    *,
    decision_id: str,
    candidate_security_id: str,
    provisional_security_id: str,
    blast_radius: dict[str, int],
) -> ConfirmationBinding:
    """Bind approval without exposing the raw provider reference."""
    return ConfirmationBinding(
        arguments={
            "decision_id": decision_id,
            "action": "accept",
            "into": candidate_security_id,
        },
        resolved_ids=(
            provisional_security_id,
            candidate_security_id,
        ),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="security_identity_merge",
        blast_radius=blast_radius,
    )


def _confirm_message(p: _MergeProposal) -> str:
    """Prompt text a human reads before two instruments' tax lots are fused.

    Names BOTH sides — the provisional is identified by its provider ref (the
    catalog id it will be deleted under is an implementation detail the review
    queue does not surface) — plus the reason the resolver refused to decide.
    """
    return (
        "Confirm a security merge (this fuses two instruments' tax lots).\n\n"
        f"MERGE AWAY — provisional, provider ref {p.ref_kind}={p.ref_value}:\n"
        f"  ticker {p.provider_ticker or '(none)'} · "
        f"name {p.provider_name or '(none)'}\n\n"
        f"INTO — survivor, security_id {p.candidate_security_id}:\n"
        f"  ticker {p.candidate_ticker or '(none)'} · "
        f"name {p.candidate_name or '(none)'}\n\n"
        f"Proposed on: {p.match_reason or 'unspecified'}. The resolver proposes a "
        "merge ONLY when it cannot decide on its own — this is an ambiguous "
        "match, not a certain one.\n\n"
        "Accepting re-points every accepted provider ref, tax-lot selection, "
        "and manual investment ledger row onto the survivor, then deletes the "
        "provisional catalog row. If these are not the same instrument, cost "
        "basis and every later realized gain will be wrong. Reversible via "
        "system_audit_undo(operation_id).\n\n"
        "Accept this merge?"
    )


def _apply_accept(
    decision_id: str,
    into: str,
    grant: ConfirmationGrant,
) -> None:
    # decided_by="user" is truthful only on this path: a human just ratified the
    # merge through the elicitation gate above.
    def verify(impact: SecurityLinkAcceptImpact) -> None:
        grant.verify(
            _security_link_binding(
                decision_id=decision_id,
                candidate_security_id=into,
                provisional_security_id=impact.provisional_security_id,
                blast_radius=impact.blast_radius,
            )
        )

    with get_database(read_only=False) as db:
        SecurityLinksService(db, actor="mcp").accept_merge(
            decision_id,
            into=into,
            decided_by="user",
            verify_accept=verify,
        )


def _apply_reject(decision_id: str) -> None:
    # decided_by="auto": no human ratified this reject — the agent called it.
    # The column's CHECK admits only 'auto' | 'user', and recording 'user' for a
    # decision no human made is precisely the falsehood the accept gate exists to
    # prevent. The MCP channel itself is preserved in app.audit_log (actor='mcp').
    with get_database(read_only=False) as db:
        SecurityLinksService(db, actor="mcp").reject_merge(
            decision_id, decided_by="auto"
        )


async def investments_securities_links_set(
    decision_id: str,
    action: Literal["accept", "reject"],
    into: str | None = None,
    confirmation_token: str | None = None,
) -> ResponseEnvelope[SecurityLinksSetPayload]:
    """Accept (merge) or reject one pending security merge decision.

    `action` is explicit — accept vs reject is never inferred from whether
    `into` has a value:

    - `action="accept"` + `into=<the decision's own candidate_security_id>`
      MERGES. This REQUIRES explicit human confirmation: the tool prompts the
      user through an MCP elicitation naming both securities and the reason,
      and merges only if they agree. A client that cannot prompt receives
      mutation_confirmation_required with a short-lived, payload-bound token
      for an exact retry. `into` is also a confirming safety check: it must
      equal the decision's own candidate (on a tied group the resolver files
      one decision per candidate), so a mistyped or stale decision_id cannot
      merge into the wrong security.
      Mismatched, empty, or missing `into` raises mutation_invalid_input —
      it is never treated as a reject.
    - `action="reject"` (pass no `into`) REJECTS — keeps the provisional
      security as its own distinct instrument. Cheap and reversible, so no
      confirmation is required. Only THIS decision is rejected; sibling
      candidates for the same provider ref remain pending (rejecting one
      candidate answers only that pairing, not whether another candidate is
      the correct match).

    A merge fuses two instruments' tax lots: it re-points every accepted
    provider ref and lot selection onto the survivor and DELETES the
    provisional catalog row. If they are not the same instrument, cost basis
    and every later realized gain are wrong.

    Mutation surface: writes app.security_link_decisions + app.security_links
    + app.lot_selections + raw.manual_investment_transactions + app.securities
    (deletes the merged-away provisional row on accept). Revert with
    system_audit_undo(operation_id) — the whole cascade is one audited operation
    and reverses atomically; find the operation_id via system_audit. Find pending
    decisions with investments_securities_links_pending.

    Args:
        decision_id: The decision id to act on (from
            investments_securities_links_pending).
        action: "accept" (merge, requires `into` + human confirmation) or
            "reject" (keep the provisional security; pass no `into`).
        into: With action="accept", the candidate_security_id to merge into —
            must equal the decision's own candidate_security_id. Invalid with
            action="reject".
        confirmation_token: Opaque payload-bound token returned to clients that
            cannot elicit. Used only with action="accept".
    """
    if action not in ("accept", "reject"):
        raise UserError(
            f"action must be 'accept' or 'reject' (got {action!r}).",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if action == "reject":
        if into is not None:
            raise UserError(
                "'into' is only valid with action='accept'. To reject, pass "
                "action='reject' with no 'into'.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        # DB work off the event loop: this tool is a coroutine (it awaits the
        # elicitation), so a blocking DuckDB write here would stall the server.
        await asyncio.to_thread(_apply_reject, decision_id)
        status = "rejected"
    else:
        if not into:
            raise UserError(
                "action='accept' requires 'into' = the target_id shown by "
                "reviews(kind='security_links'). "
                "An empty 'into' is not a reject — pass action='reject' for that.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if confirmation_token is None:
            proposal = await asyncio.to_thread(_load_pending_proposal, decision_id)
            if into != proposal.candidate_security_id:
                # Refuse BEFORE prompting: a doomed merge must not cost the user a
                # confirmation. The service re-checks this; this is the boundary copy.
                raise UserError(
                    f"'into' does not match decision '{decision_id}' — it must be that "
                    "decision's own candidate_security_id.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                    hint=("Re-read the decision with reviews(kind='security_links')."),
                )
            binding = _security_link_binding(
                decision_id=decision_id,
                candidate_security_id=into,
                provisional_security_id=proposal.provisional_security_id,
                blast_radius=proposal.blast_radius,
            )
            message = _confirm_message(proposal)
        else:
            binding = None
            message = ""
        grant = await grant_confirmation_or_raise(
            binding=binding,
            message=message,
            confirmation_token=confirmation_token,
        )
        await asyncio.to_thread(
            _apply_accept,
            decision_id,
            into,
            grant,
        )
        status = "accepted"
    return build_envelope(
        data=SecurityLinksSetPayload(decision_id=decision_id, status=status),
        actions=[
            "Use reviews(kind='security_links') for remaining pending decisions",
            "Reverse this decision with system_audit_undo(operation_id) — find "
            "the operation_id with system_audit",
        ],
    )


def investments_securities_links_history(
    limit: int = 50,
) -> ResponseEnvelope[SecurityLinksHistoryPayload]:
    """Recent security-link decisions (all statuses), newest first.

    Args:
        limit: Maximum rows (default 50).
    """
    with get_database(read_only=True) as db:
        rows = SecurityLinksService(db, actor="mcp").history(limit=limit)
    payload = SecurityLinksHistoryPayload.from_rows(rows)
    return build_envelope(
        data=payload,
        actions=["Use reviews(kind='security_links') for the active review queue"],
    )


# ─── Standard coarse read ─────────────────────────────────────────────────


def _resolve_coarse_reference(
    reference: str,
    candidates: list[EntityCandidate],
    *,
    noun: Literal["account", "security"],
) -> str:
    """Resolve one filter reference without echoing it in errors."""
    resolution = resolve_entity_reference(reference, candidates)
    if isinstance(resolution, AmbiguousEntity):
        raise UserError(
            f"The {noun} reference matches multiple {noun}s.",
            code="ENTITY_REFERENCE_AMBIGUOUS",
            details={"candidate_ids": list(resolution.candidate_ids)},
        )
    if isinstance(resolution, MissingEntity):
        raise UserError(
            f"The {noun} reference did not match a {noun}.",
            code="ENTITY_REFERENCE_NOT_FOUND",
            details={"candidate_ids": []},
        )
    return resolution.entity_id


def _coarse_account_candidates(service: AccountService) -> list[EntityCandidate]:
    """Project active accounts into the shared resolver shape."""
    payload = service.list_accounts(include_archived=False, type_filter=None)
    return [
        EntityCandidate(
            entity_id=row.account_id,
            display_name=row.display_name or row.account_id,
            aliases=tuple(
                value
                for value in (
                    row.institution_name,
                    row.account_type,
                    row.account_subtype,
                )
                if value is not None
            ),
        )
        for row in payload.rows
    ]


def _coarse_security_candidates(
    service: InvestmentService,
) -> list[EntityCandidate]:
    """Project securities into the shared resolver shape."""
    return [
        EntityCandidate(
            entity_id=row.security_id,
            display_name=row.name,
            aliases=tuple(
                value
                for value in (
                    row.ticker,
                    (
                        f"{row.ticker}.{row.exchange}"
                        if row.ticker is not None and row.exchange is not None
                        else None
                    ),
                    row.cusip,
                    row.isin,
                    row.figi,
                    row.coingecko_id,
                )
                if value is not None
            ),
        )
        for row in service.list_securities().rows
    ]


def _investment_scope(
    view: Literal["events", "holdings", "lots", "gains", "securities"],
    filters: dict[str, object],
) -> dict[str, object]:
    """Return the complete canonical investment cursor scope."""
    return {"view": view, **filters}


def _investment_position(
    cursor: str | None,
    *,
    view: Literal["events", "holdings", "lots", "gains", "securities"],
    filters: dict[str, object],
) -> KeysetPosition | None:
    """Decode and type-check a stable-identity investment cursor."""
    if cursor is None:
        return None
    try:
        position = decode_keyset_cursor(
            cursor,
            namespace="investments",
            scope=_investment_scope(view, filters),
        )
        key_length = 2 if view == "holdings" else 1
        if (
            len(position.snapshot) != key_length
            or len(position.after) != key_length
            or not all(
                type(value) is str for value in (*position.snapshot, *position.after)
            )
        ):
            raise ValueError("invalid investment key")
        snapshot = cast(tuple[str, ...], position.snapshot)
        after = cast(tuple[str, ...], position.after)
        if after > snapshot:
            raise ValueError("invalid investment key order")
        return position
    except ValueError as exc:
        raise UserError(
            "Invalid pagination cursor.",
            code="INVESTMENT_CURSOR_INVALID",
        ) from exc


def _investment_row_key(
    view: Literal["events", "holdings", "lots", "gains", "securities"],
    row: Any,
) -> tuple[str, ...]:
    """Return immutable stable IDs for one investment projection row."""
    if view == "events":
        return (str(row.investment_transaction_id),)
    if view == "holdings":
        return (str(row.account_id), str(row.security_id))
    if view == "lots":
        return (str(row.lot_id),)
    if view == "gains":
        return (str(row.realized_gain_id),)
    return (str(row.security_id),)


def _investment_page[T](
    rows: list[T],
    *,
    view: Literal["events", "holdings", "lots", "gains", "securities"],
    filters: dict[str, object],
    limit: int,
    position: KeysetPosition | None,
) -> tuple[list[T], str | None, int]:
    """Page immutable identities within the initial high-water boundary."""
    ordered = sorted(rows, key=lambda row: _investment_row_key(view, row))
    if position is None:
        eligible = ordered
        total_count = len(ordered)
        snapshot = _investment_row_key(view, ordered[-1]) if ordered else None
    else:
        snapshot = cast(tuple[str, ...], position.snapshot)
        after = cast(tuple[str, ...], position.after)
        eligible = [
            row for row in ordered if after < _investment_row_key(view, row) <= snapshot
        ]
        total_count = position.total
    page = eligible[:limit]
    if len(eligible) <= limit or not page or snapshot is None:
        return page, None, total_count
    return (
        page,
        encode_keyset_cursor(
            namespace="investments",
            scope=_investment_scope(view, filters),
            snapshot=snapshot,
            after=_investment_row_key(view, page[-1]),
            total=total_count,
        ),
        total_count,
    )


def _investment_period(start: _date | None, end: _date | None) -> str | None:
    """Render the selected investment date window."""
    if start is not None and end is not None:
        return f"{start.isoformat()} to {end.isoformat()}"
    if start is not None:
        return f"from {start.isoformat()}"
    if end is not None:
        return f"through {end.isoformat()}"
    return None


def _investment_actions(
    view: Literal["events", "holdings", "lots", "gains", "securities"],
    *,
    account: str | None,
    security: str | None,
    start: _date | None,
    end: _date | None,
    open_only: bool | None,
    limit: int,
    next_cursor: str | None,
) -> list[str]:
    """Return replacement-only hints with an executable continuation."""
    by_view = {
        "events": [
            "Use investments(view='holdings') for current positions",
            "Use investments(view='gains') for realized gain/loss",
        ],
        "holdings": [
            "Use investments(view='lots') for per-lot basis",
            "Use investments(view='gains') for realized gain/loss",
        ],
        "lots": [
            "Use investments_lots_select to override FIFO for a disposal",
            "Use investments(view='gains') for realized gain/loss",
        ],
        "gains": ["Use investments(view='lots') for lot-level detail"],
        "securities": [
            "Use investments_securities_set to add or update a catalog entry"
        ],
    }
    actions = list(by_view[view])
    if next_cursor is not None:
        arguments = [f"view={view!r}"]
        if account is not None:
            arguments.append(f"account={account!r}")
        if security is not None:
            arguments.append(f"security={security!r}")
        if start is not None:
            arguments.append(f"start={start.isoformat()!r}")
        if end is not None:
            arguments.append(f"end={end.isoformat()!r}")
        if open_only is not None:
            arguments.append(f"open_only={open_only!r}")
        arguments.extend((f"limit={limit}", f"cursor={next_cursor!r}"))
        actions.append(f"Continue with investments({', '.join(arguments)})")
    return actions


def _investment_coarse_envelope(
    data: InvestmentsCoarsePayload,
    *,
    total_count: int,
    next_cursor: str | None,
    period: str | None,
    actions: list[str],
) -> ResponseEnvelope[InvestmentsCoarsePayload]:
    """Build a runtime-classified investment envelope."""
    contract_type = type(data)
    classes = extract_data_classes(contract_type)
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(InvestmentsCoarsePayload, redact_typed(data, None))
    envelope = cast(
        ResponseEnvelope[InvestmentsCoarsePayload],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=total_count,
            returned_count=len(data.rows),
            next_cursor=next_cursor,
            period=period,
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )
    return replace(
        envelope,
        summary=replace(envelope.summary, has_more=next_cursor is not None),
    )


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
def investments_coarse(
    view: Literal["events", "holdings", "lots", "gains", "securities"] = "holdings",
    account: str | None = None,
    security: str | None = None,
    start: _date | None = None,
    end: _date | None = None,
    open_only: StrictBool | None = None,
    limit: Annotated[int, Field(strict=True, ge=1)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[InvestmentsCoarsePayload]:
    """Return one paginated investment projection selected by a closed view."""
    if view not in ("events", "gains") and (start is not None or end is not None):
        raise UserError(
            "start and end are only valid for investment events and gains.",
            code="INVESTMENT_DATES_NOT_ALLOWED",
        )
    if view != "lots" and open_only is not None:
        raise UserError(
            "open_only is only valid for investment lots.",
            code="INVESTMENT_OPEN_ONLY_NOT_ALLOWED",
        )
    if view == "securities" and account is not None:
        raise UserError(
            "account is not valid for the securities catalog.",
            code="INVESTMENT_ACCOUNT_NOT_ALLOWED",
        )
    if start is not None and end is not None and start > end:
        raise UserError(
            "Investment start must not be after end.",
            code="INVESTMENT_DATE_RANGE_INVALID",
        )

    filters: dict[str, object] = {
        "account": account.casefold().strip() if account is not None else None,
        "end": end.isoformat() if end is not None else None,
        "open_only": (True if view == "lots" and open_only is None else open_only),
        "security": security.casefold().strip() if security is not None else None,
        "start": start.isoformat() if start is not None else None,
    }
    position = _investment_position(cursor, view=view, filters=filters)

    with get_database(read_only=True) as db:
        service = InvestmentService(db)
        account_id = (
            _resolve_coarse_reference(
                account,
                _coarse_account_candidates(AccountService(db)),
                noun="account",
            )
            if account is not None
            else None
        )
        security_id = (
            _resolve_coarse_reference(
                security,
                _coarse_security_candidates(service),
                noun="security",
            )
            if security is not None
            else None
        )

        if view == "events":
            result = service.list_events(
                account_ref=account_id,
                security_ref=security_id,
                date_from=start,
                date_to=end,
            )
            all_rows = InvestmentEventsPayload.from_result(result)
        elif view == "holdings":
            result = service.holdings(
                account_ref=account_id,
                security_ref=security_id,
            )
            all_rows = InvestmentHoldingsPayload.from_result(result)
        elif view == "lots":
            result = service.lots(
                account_ref=account_id,
                security_ref=security_id,
                open_only=True if open_only is None else bool(open_only),
            )
            all_rows = InvestmentLotsPayload.from_result(result)
        elif view == "gains":
            result = service.gains(
                account_ref=account_id,
                security_ref=security_id,
                date_from=start,
                date_to=end,
            )
            all_rows = InvestmentGainsPayload.from_result(result)
        else:
            result = service.list_securities()
            all_rows = InvestmentSecuritiesPayload.from_result(result)
            filtered_rows = (
                [row for row in all_rows.rows if row.security_id == security_id]
                if security_id is not None
                else all_rows.rows
            )
            all_rows = InvestmentSecuritiesPayload(
                rows=filtered_rows,
                warnings=all_rows.warnings,
            )
        page, next_cursor, total_count = _investment_page(
            cast(list[Any], all_rows.rows),
            view=view,
            filters=filters,
            limit=limit,
            position=position,
        )
        if view == "events":
            data: InvestmentsCoarsePayload = InvestmentsEventsView(
                rows=page,
                warnings=all_rows.warnings,
            )
        elif view == "holdings":
            holdings = cast(InvestmentHoldingsPayload, all_rows)
            data = InvestmentsHoldingsView(
                rows=page,
                warnings=holdings.warnings,
                max_days_since_observed=holdings.max_days_since_observed,
                total_market_value=holdings.total_market_value,
                market_value_by_currency=holdings.market_value_by_currency,
            )
        elif view == "lots":
            data = InvestmentsLotsView(
                rows=page,
                warnings=all_rows.warnings,
            )
        elif view == "gains":
            data = InvestmentsGainsView(
                rows=page,
                warnings=all_rows.warnings,
            )
        else:
            data = InvestmentsSecuritiesView(
                rows=page,
                warnings=all_rows.warnings,
            )

    return _investment_coarse_envelope(
        data,
        total_count=total_count,
        next_cursor=next_cursor,
        period=_investment_period(start, end),
        actions=_investment_actions(
            view,
            account=account,
            security=security,
            start=start,
            end=end,
            open_only=open_only,
            limit=limit,
            next_cursor=next_cursor,
        ),
    )


def register_investment_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard investment read."""
    register(
        mcp,
        investments_coarse,
        "investments",
        "Return investment events, holdings, open or full tax-lot history, "
        "realized gains, or securities through one typed view. Amounts use the "
        "investment ledger sign convention; currency is summary.display_currency "
        "for every view except holdings, whose market_value stays in each row's "
        "currency_code. For holdings, valuation_status marks each row valued, "
        "carried_forward, unpriced, or withheld; unpriced and withheld report "
        "market_value/unrealized_gain null (never zero) and data.warnings counts "
        "them, and withheld means the share count is known wrong. Do not sum "
        "market_value across rows: read data.total_market_value (null when priced "
        "rows span currencies, with data.market_value_by_currency giving the "
        "split) and data.max_days_since_observed for the stalest close behind any "
        "published figure.",
        privacy_actor="investments",
    )


# ─── Registration ──────────────────────────────────────────────────────────

_LEGACY_INTERNAL_CALLBACKS = (
    investments,
    investments_holdings,
    investments_lots,
    investments_gains,
    investments_securities,
    investments_securities_links_pending,
    investments_securities_links_set,
    investments_securities_links_history,
)


def register_investments_tools(mcp: FastMCP) -> None:
    """Register the standard investment read and write boundaries."""
    register_investment_coarse_reads(mcp)
    register(
        mcp,
        investments_record,
        "investments_record",
        "Record investment ledger events as one validated batch. Quantity and "
        "cash signs follow the investment event convention; no revert tool.",
    )
    register(
        mcp,
        investments_securities_set,
        "investments_securities_set",
        "Create or update securities in app.securities by stable ID or ticker. "
        "Call again with prior values to revert.",
    )
    register(
        mcp,
        investments_lots_select,
        "investments_lots_select",
        "Select specific acquisition lots for one disposal. Writes "
        "app.lot_selections; replace the selection to revert.",
    )
