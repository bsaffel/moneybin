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
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date as _date
from decimal import Decimal
from typing import Any

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.elicitation import confirm_or_raise
from moneybin.privacy.payloads.investments import (
    InvestmentEventsPayload,
    InvestmentGainsPayload,
    InvestmentHoldingsPayload,
    InvestmentLotSelectionEntry,
    InvestmentLotsPayload,
    InvestmentLotsSelectPayload,
    InvestmentRecordPayload,
    InvestmentSecuritiesPayload,
    InvestmentSecuritySetPayload,
    SecurityLinksHistoryPayload,
    SecurityLinksPendingPayload,
    SecurityLinksSetPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.investment_service import InvestmentService
from moneybin.services.security_links_service import SecurityLinksService


def _parse_date(value: str | None) -> _date | None:
    return _date.fromisoformat(value) if value is not None else None


def _parse_decimal(value: object) -> Decimal | None:
    # str(...) first: a JSON number arriving as a Python float would inject
    # binary rounding noise straight into a money/quantity field. Every other
    # money parser in the codebase (accounts.py, _parse_selection below) does
    # the same. None passes through unchanged.
    return Decimal(str(value)) if value is not None else None


# ─── Read tools ─────────────────────────────────────────────────────────────


@mcp_tool()
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
            "Use investments_holdings for current positions",
            "Use investments_gains for realized gain/loss",
        ],
    )


@mcp_tool()
def investments_holdings(
    account: str | None = None,
) -> ResponseEnvelope[InvestmentHoldingsPayload]:
    """Current positions: quantity, cost basis, average cost per (account, security).

    Args:
        account: Account ID or free-text reference (resolved to an id).

    Market value and unrealized gain/loss require price feeds (Pillar C, not
    yet shipped) — this always carries a warning that only cost basis is
    available. Amounts are in the currency named by `summary.display_currency`.
    """
    with get_database(read_only=True) as db:
        result = InvestmentService(db).holdings(account_ref=account)
    return build_envelope(
        data=InvestmentHoldingsPayload.from_result(result),
        actions=[
            "Use investments_lots for per-lot basis",
            "Use investments_gains for realized gain/loss",
        ],
    )


@mcp_tool()
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
            "Use investments_gains for realized gain/loss",
        ],
    )


@mcp_tool()
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
        actions=["Use investments_lots for lot-level detail"],
    )


@mcp_tool()
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
            "Use investments_holdings to see updated positions",
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
            "Use investments_gains to see the updated allocation",
        ],
    )


# ─── Review tools (security links) ─────────────────────────────────────────


@mcp_tool(domain="links")
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
            "Use investments_securities_links_set(decision_id, action='accept', "
            "into=<candidate_security_id>) to merge — the user is prompted to "
            "confirm the merge before anything is written",
            "Use investments_securities_links_set(decision_id, action='reject') "
            "to keep the provider's security as its own distinct instrument",
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


def _cli_equivalent(decision_id: str, into: str) -> str:
    return (
        f"moneybin investments securities links set {decision_id} "
        f"--accept --into {into}"
    )


def _load_pending_proposal(decision_id: str) -> _MergeProposal:
    """Read the decision out of the live review queue, or raise if it isn't there."""
    with get_database(read_only=True) as db:
        groups = SecurityLinksService(db, actor="mcp").pending()
    for group in groups:
        for candidate in group.candidates:
            if candidate.decision_id == decision_id:
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
                )
    raise UserError(
        f"No pending security merge decision '{decision_id}'.",
        code=error_codes.MUTATION_NOT_FOUND,
        hint="List open decisions with investments_securities_links_pending.",
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
        "Accepting re-points every accepted provider ref and tax-lot selection "
        "onto the survivor and deletes the provisional catalog row. If these are "
        "not the same instrument, cost basis and every later realized gain will "
        "be wrong. Reversible via system_audit_undo(operation_id).\n\n"
        "Accept this merge?"
    )


async def _confirm_merge_or_raise(p: _MergeProposal) -> None:
    """Obtain explicit human agreement for a merge, or raise — never fall through.

    Delegates the gate itself to the shared ``confirm_or_raise`` helper (one
    elicitation pattern across every accept gate); only the prompt and the CLI
    equivalent are security-specific.
    """
    await confirm_or_raise(
        _confirm_message(p),
        subject="This merge",
        unchanged=f"decision '{p.decision_id}' is still pending",
        cli_equivalent=_cli_equivalent(p.decision_id, p.candidate_security_id),
        details={"decision_id": p.decision_id},
    )


def _apply_accept(decision_id: str, into: str) -> None:
    # decided_by="user" is truthful only on this path: a human just ratified the
    # merge through the elicitation gate above.
    with get_database(read_only=False) as db:
        SecurityLinksService(db, actor="mcp").accept_merge(
            decision_id, into=into, decided_by="user"
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


@mcp_tool(
    domain="links",
    read_only=False,
    destructive=True,
    idempotent=False,
    # The accept path blocks on a human reading a merge confirmation (two
    # securities + the reason they're ambiguous). The 30s default would routinely
    # fire first — and a cap that expires mid-decision means the user "accepts"
    # into a coroutine that was already cancelled. Same headroom as gsheet_auth's
    # interactive OAuth wait. Timing out is still safe (nothing is written), just
    # confusing.
    timeout_seconds=180.0,
)
async def investments_securities_links_set(
    decision_id: str,
    action: str,
    into: str | None = None,
) -> ResponseEnvelope[SecurityLinksSetPayload]:
    """Accept (merge) or reject one pending security merge decision.

    `action` is explicit — accept vs reject is never inferred from whether
    `into` has a value:

    - `action="accept"` + `into=<the decision's own candidate_security_id>`
      MERGES. This REQUIRES explicit human confirmation: the tool prompts the
      user through an MCP elicitation naming both securities and the reason,
      and merges only if they agree. On a client that cannot prompt (no
      elicitation capability), accept HARD-FAILS with
      mutation_confirmation_required and points at the CLI — an agent cannot
      accept a merge on its own, at any confidence. `into` is also a
      confirming safety check: it must equal the decision's own candidate (on
      a tied group the resolver files one decision per candidate), so a
      mistyped or stale decision_id cannot merge into the wrong security.
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
    + app.lot_selections + app.securities (deletes the merged-away
    provisional row on accept). Revert with system_audit_undo(operation_id) —
    the whole cascade is one audited operation and reverses atomically; find
    the operation_id via system_audit. Find pending decisions with
    investments_securities_links_pending.

    Args:
        decision_id: The decision id to act on (from
            investments_securities_links_pending).
        action: "accept" (merge, requires `into` + human confirmation) or
            "reject" (keep the provisional security; pass no `into`).
        into: With action="accept", the candidate_security_id to merge into —
            must equal the decision's own candidate_security_id. Invalid with
            action="reject".
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
                "action='accept' requires 'into' = the decision's own "
                "candidate_security_id (see investments_securities_links_pending). "
                "An empty 'into' is not a reject — pass action='reject' for that.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        proposal = await asyncio.to_thread(_load_pending_proposal, decision_id)
        if into != proposal.candidate_security_id:
            # Refuse BEFORE prompting: a doomed merge must not cost the user a
            # confirmation. The service re-checks this; this is the boundary copy.
            raise UserError(
                f"'into' does not match decision '{decision_id}' — it must be that "
                "decision's own candidate_security_id.",
                code=error_codes.MUTATION_INVALID_INPUT,
                hint="Re-read the decision with investments_securities_links_pending.",
            )
        await _confirm_merge_or_raise(proposal)
        await asyncio.to_thread(_apply_accept, decision_id, into)
        status = "accepted"
    return build_envelope(
        data=SecurityLinksSetPayload(decision_id=decision_id, status=status),
        actions=[
            "Use investments_securities_links_pending to review remaining "
            "pending decisions",
            "Reverse this decision with system_audit_undo(operation_id) — find "
            "the operation_id with system_audit",
        ],
    )


@mcp_tool(domain="links")
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
        actions=[
            "Use investments_securities_links_pending for the active review queue"
        ],
    )


# ─── Registration ──────────────────────────────────────────────────────────


def register_investments_tools(mcp: FastMCP) -> None:
    """Register all investments namespace tools with the FastMCP server."""
    register(
        mcp,
        investments,
        "investments",
        "List investment ledger events (buys, sells, dividends, corporate "
        "actions, ...). Amounts use the per-type sign convention documented "
        "in investments_record; amounts are in the currency named by "
        "`summary.display_currency`.",
    )
    register(
        mcp,
        investments_holdings,
        "investments_holdings",
        "Current positions: quantity, cost basis, average cost per "
        "(account, security). Market value/unrealized gain require price "
        "feeds (not yet shipped) — always carries a warning that only cost "
        "basis is available. Amounts are in the currency named by "
        "`summary.display_currency`.",
    )
    register(
        mcp,
        investments_lots,
        "investments_lots",
        "Tax lots with remaining quantity and basis. Open lots only by "
        "default (open_only=False for full history). A row with "
        "basis_incomplete=true opened with no supplied basis (e.g. a "
        "transfer_in with unknown cost basis); data.warnings names the count "
        "when any row is incomplete. Amounts are in the currency named by "
        "`summary.display_currency`.",
    )
    register(
        mcp,
        investments_gains,
        "investments_gains",
        "Realized gain/loss (the 1099-B surface). A row with "
        "basis_incomplete=true means the disposal was oversold or the "
        "acquisition lot is missing; data.warnings names the count when any "
        "row is incomplete. Amounts are in the currency named by "
        "`summary.display_currency`.",
    )
    register(
        mcp,
        investments_securities,
        "investments_securities",
        "List the manually-maintained securities catalog. Reference data "
        "only — no amounts, no per-user holdings.",
    )
    register(
        mcp,
        investments_record,
        "investments_record",
        "Record one or more investment ledger events in one call. Sign "
        "convention: quantity positive for acquisitions / negative for "
        "disposals / absent for cash-only events; amount negative for cash "
        "out, positive for cash in. A reinvest event writes an acquisition + "
        "income row pair sharing one event_group_id. All events are validated "
        "and resolved before any write. A validation failure OR a bad/ambiguous "
        "ACCOUNT ref is a HARD failure that aborts the whole call with nothing "
        "written (standard error envelope); a bad/ambiguous SECURITY ref is a "
        "SOFT per-item failure reported in data.error_details while the rest of "
        "the batch is written. Writes raw.manual_investment_transactions; no "
        "revert tool.",
    )
    register(
        mcp,
        investments_securities_set,
        "investments_securities_set",
        "Create-or-update one securities-catalog entry. security_id=None "
        "creates (name + security_type required); an existing security_id "
        "partially updates (unset fields unchanged; security_type immutable "
        "post-creation). cost_basis_method='average' is valid only for "
        "mutual_fund/etf. Writes app.securities; no delete tool in v1.",
    )
    register(
        mcp,
        investments_lots_select,
        "investments_lots_select",
        "Set the full specific-identification lot selection for one "
        "disposal (a sell) — the listed (lot_id, quantity) pairs REPLACE any "
        "prior selection; selections=[] clears all overrides and reverts to "
        "FIFO. Writes app.lot_selections; no revert tool (call again to "
        "undo).",
    )
    register(
        mcp,
        investments_securities_links_pending,
        "investments_securities_links_pending",
        "List pending security merge decisions grouped by provider ref "
        "(plaid_security_id or institution_security_id). Returns the review "
        "queue with BOTH sides of each proposed merge: provider_ticker/"
        "provider_name alongside each candidate's candidate_ticker/"
        "candidate_name, plus match_reason (identifier_tie, "
        "exchange_contradiction, fuzzy_name, ...) — the field that conveys "
        "how risky accepting is. Use investments_securities_links_set to "
        "merge or reject each decision.",
    )
    register(
        mcp,
        investments_securities_links_set,
        "investments_securities_links_set",
        "Accept (merge) or reject one pending security merge decision. "
        "action='accept' + into=<the decision's own candidate_security_id> "
        "MERGES: it prompts the user to confirm (MCP elicitation naming both "
        "securities and the match reason) and merges only on their explicit "
        "agreement — a merge fuses two instruments' tax lots, so the agent "
        "cannot accept one on its own. On a client without elicitation, "
        "accept fails with mutation_confirmation_required and names the CLI "
        "equivalent. into must equal the decision's own candidate (mismatched, "
        "empty, or missing into raises mutation_invalid_input — it is NEVER "
        "read as a reject). action='reject' (no into) keeps the provider's "
        "security as its own instrument; only this decision is rejected, "
        "sibling candidates stay pending. Writes app.security_link_decisions "
        "+ app.security_links + app.lot_selections + app.securities; reverse "
        "the whole cascade with system_audit_undo(operation_id). Discover "
        "pending decisions with investments_securities_links_pending.",
    )
    register(
        mcp,
        investments_securities_links_history,
        "investments_securities_links_history",
        "Recent security-link decisions (all statuses), newest first. "
        "Read-only. Filter by limit. Use investments_securities_links_pending "
        "for the active review queue.",
    )
