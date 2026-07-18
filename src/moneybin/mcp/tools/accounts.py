# src/moneybin/mcp/tools/accounts.py
"""Accounts namespace tools — v2 per docs/specs/account-management.md + reports-net-worth.md.

Sensitivity is derived per tool from its payload's classified fields, not
declared here — tools returning ``account_id`` (ACCOUNT_IDENTIFIER) or
``routing_number`` (ROUTING_NUMBER) are CRITICAL; ``accounts_summary``
(counts only) is LOW.

Read tools (entity):       accounts, accounts_get, accounts_summary
Write tools (entity):      accounts_set
Read tools (balance):      accounts_balances, accounts_balance_history,
                           accounts_balance_reconcile, accounts_balance_assertions
Write tools (balance):     accounts_balance_assert, accounts_balance_assertion_delete
Read tools (links):        accounts_links_pending, accounts_links_history
Write tools (links):       accounts_links_set, accounts_links_run

All tools delegate to AccountService / BalanceService / AccountLinksService — no
business logic here. accounts links undo is deliberately NOT YET registered:
deferred to the M1L audit-undo consumer.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date as _date
from decimal import Decimal
from typing import Literal

from fastmcp import FastMCP

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
from moneybin.privacy.payloads.accounts import (
    AccountDetail,
    AccountLinksHistoryPayload,
    AccountLinksPendingPayload,
    AccountLinksRunPayload,
    AccountLinksSetPayload,
    AccountListPayload,
    AccountResolvePayload,
    AccountSettingsPayload,
    AccountSummaryStats,
)
from moneybin.privacy.payloads.balances import (
    BalanceAssertionDeletePayload,
    BalanceAssertionListPayload,
    BalanceAssertionPayload,
    BalanceObservationListPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_links_service import (
    AccountLinkAcceptImpact,
    AccountLinksService,
)
from moneybin.services.account_service import CLEAR, AccountService
from moneybin.services.balance_service import BalanceService

# ─── Read tools (entity) ──────────────────────────────────────────────────


@mcp_tool()
def accounts(
    include_archived: bool = False, type_filter: str | None = None
) -> ResponseEnvelope[AccountListPayload]:
    """List accounts in MoneyBin.

    Args:
        include_archived: Include archived accounts (default: hide them)
        type_filter: Match account_type or account_subtype (case-insensitive)

    Returns the resolved view from core.dim_accounts including display_name,
    institution_name, account_type, account_subtype, holder_category,
    iso_currency_code, archived, include_in_net_worth, last_four, and
    credit_limit. CRITICAL-tier fields (last_four) are masked by the
    middleware; the raw values never leave the service layer unredacted.
    """
    with get_database(read_only=True) as db:
        result = AccountService(db).list_accounts(
            include_archived=include_archived, type_filter=type_filter
        )
    return build_envelope(
        data=result,
        actions=[
            "Use accounts_balances for current balances",
            "Use reports_spending with a category filter to drill in by account",
        ],
    )


@mcp_tool()
def accounts_get(account_id: str) -> ResponseEnvelope[AccountDetail]:
    """Single account record with full settings + dim record.

    Returns full fields including CRITICAL-tier values (account_id, last_four,
    routing_number). The tool's sensitivity is derived from AccountDetail's
    classified fields (CRITICAL), and CRITICAL fields are masked by the
    middleware.

    Args:
        account_id: The account ID to look up

    Returns the account record if found, or raises not_found if not.
    """
    with get_database(read_only=True) as db:
        record = AccountService(db).get_account(account_id)
    if record is None:
        raise UserError(
            f"Account not found: {account_id}", code=error_codes.INFRA_NOT_FOUND
        )
    return build_envelope(data=record)


@mcp_tool()
def accounts_summary() -> ResponseEnvelope[AccountSummaryStats]:
    """Aggregate account snapshot: counts only, no per-account data, no PII.

    Useful as context for AI conversations about finances. Returns total counts,
    counts by type and subtype, count archived, count excluded from net worth,
    and count with recent activity (last 30 days).
    """
    with get_database(read_only=True) as db:
        stats = AccountService(db).summary()
    return build_envelope(data=stats)


# ─── Write tools (entity) ──────────────────────────────────────────────────


_CLEARABLE_FIELDS: frozenset[str] = frozenset({
    "official_name",
    "last_four",
    "account_subtype",
    "holder_category",
    "iso_currency_code",
    "credit_limit",
    "display_name",
    "default_cost_basis_method",
})


@mcp_tool(read_only=False)
def accounts_set(
    account_id: str,
    official_name: str | None = None,
    last_four: str | None = None,
    account_subtype: str | None = None,
    holder_category: str | None = None,
    iso_currency_code: str | None = None,
    credit_limit: float | None = None,
    display_name: str | None = None,
    default_cost_basis_method: str | None = None,
    include_in_net_worth: bool | None = None,
    is_archived: bool | None = None,
    clear_fields: list[str] | None = None,
) -> ResponseEnvelope[AccountSettingsPayload]:
    """Partial update of an account's settings (structural + behavioral fields).

    Replaces the formerly-separate ``accounts_rename``, ``accounts_include``,
    ``accounts_archive``, and ``accounts_unarchive`` tools — one entrypoint for
    every per-account settings mutation.

    Structural fields (Plaid-parity metadata):
      ``official_name``, ``last_four``, ``account_subtype``, ``holder_category``,
      ``iso_currency_code``, ``credit_limit``.

    Behavioral fields:
      ``display_name`` — text override for the account's resolved name.
      ``default_cost_basis_method`` — per-account cost-basis default for
        investment disposals: one of ``"fifo"``, ``"hifo"``, ``"specific"``,
        ``"average"``. ``None`` falls back to the global FIFO default. An
        unrecognized value raises ``mutation_invalid_input`` before the write.
      ``include_in_net_worth`` — toggle inclusion in net-worth aggregates.
      ``is_archived`` — archive / unarchive flag.

    Pass ``None`` to leave a field unchanged. To explicitly clear a text field
    back to NULL, include its name in ``clear_fields``. Valid clearable names:
    ``"official_name"``, ``"last_four"``, ``"account_subtype"``,
    ``"holder_category"``, ``"iso_currency_code"``, ``"credit_limit"``,
    ``"display_name"``, ``"default_cost_basis_method"``. Booleans
    (``include_in_net_worth``, ``is_archived``) are not clearable — pass the
    explicit value.

    Archive cascade: ``is_archived=True`` also sets ``include_in_net_worth=False``
    atomically in the same write. Unarchiving (``is_archived=False``) does NOT
    restore the prior ``include_in_net_worth`` value — pass
    ``include_in_net_worth=True`` explicitly to re-include. When the cascade
    fires, the response data includes ``cascaded_include_in_net_worth: false``
    to surface the side effect.

    Soft-validation warnings (for non-canonical ``account_subtype`` or
    ``holder_category`` values) are embedded in ``data['warnings']``.
    """
    kwargs: dict[str, object] = {
        "official_name": official_name,
        "last_four": last_four,
        "account_subtype": account_subtype,
        "holder_category": holder_category,
        "iso_currency_code": iso_currency_code,
        "credit_limit": Decimal(str(credit_limit))
        if credit_limit is not None
        else None,
        "display_name": display_name,
        "default_cost_basis_method": default_cost_basis_method,
        "include_in_net_worth": include_in_net_worth,
        # MCP param `is_archived` → service kwarg `archived`.
        "archived": is_archived,
    }
    if clear_fields:
        unknown = set(clear_fields) - _CLEARABLE_FIELDS
        if unknown:
            raise UserError(
                f"Unknown clearable fields: {sorted(unknown)}", code="invalid_field"
            )
        for field in clear_fields:
            kwargs[field] = CLEAR
    with get_database(read_only=False) as db:
        settings, warnings = AccountService(db).settings_update(
            account_id,
            actor="mcp",
            **kwargs,  # type: ignore[arg-type]  # CLEAR sentinel + Optional unioned for partial update
        )
    d = settings.to_dict()
    payload = AccountSettingsPayload(
        account_id=str(d["account_id"]),
        display_name=d.get("display_name"),  # type: ignore[arg-type]
        official_name=d.get("official_name"),  # type: ignore[arg-type]
        last_four=d.get("last_four"),  # type: ignore[arg-type]
        account_subtype=d.get("account_subtype"),  # type: ignore[arg-type]
        holder_category=d.get("holder_category"),  # type: ignore[arg-type]
        iso_currency_code=d.get("iso_currency_code"),  # type: ignore[arg-type]
        credit_limit=d.get("credit_limit"),  # type: ignore[arg-type]
        default_cost_basis_method=d.get("default_cost_basis_method"),  # type: ignore[arg-type]
        include_in_net_worth=bool(d["include_in_net_worth"]),
        archived=bool(d["archived"]),
        warnings=[w.get("message", str(w)) for w in warnings] if warnings else [],
        cascaded_include_in_net_worth=False if is_archived is True else None,
    )
    return build_envelope(data=payload)


# ─── Read tools (balance) ──────────────────────────────────────────────────


@mcp_tool()
def accounts_balances(
    account_ids: list[str] | None = None, as_of_date: str | None = None
) -> ResponseEnvelope[BalanceObservationListPayload]:
    """Most recent balance per account; optionally as-of an ISO date.

    Args:
        account_ids: Filter to specific accounts
        as_of_date: ISO date (YYYY-MM-DD) — shows balance on or before this date
    """
    parsed_date = _date.fromisoformat(as_of_date) if as_of_date else None
    with get_database(read_only=True) as db:
        result = BalanceService(db).current_balances(
            account_ids=account_ids, as_of_date=parsed_date
        )
    return build_envelope(data=result)


@mcp_tool()
def accounts_balance_history(
    account_id: str, from_date: str | None = None, to_date: str | None = None
) -> ResponseEnvelope[BalanceObservationListPayload]:
    """Per-account balance history (daily series with carry-forward + reconciliation deltas).

    Args:
        account_id: Required — the account to query
        from_date: ISO date (YYYY-MM-DD); inclusive
        to_date: ISO date (YYYY-MM-DD); inclusive
    """
    parsed_from = _date.fromisoformat(from_date) if from_date else None
    parsed_to = _date.fromisoformat(to_date) if to_date else None
    with get_database(read_only=True) as db:
        result = BalanceService(db).history(
            account_id, from_date=parsed_from, to_date=parsed_to
        )
    return build_envelope(data=result)


@mcp_tool()
def accounts_balance_reconcile(
    account_ids: list[str] | None = None, threshold: float = 0.01
) -> ResponseEnvelope[BalanceObservationListPayload]:
    """Show balance days with non-zero reconciliation delta above threshold.

    Args:
        account_ids: Filter to specific accounts
        threshold: Minimum absolute delta to include (default: 0.01 = 1 cent)
    """
    parsed_threshold = Decimal(str(threshold))
    with get_database(read_only=True) as db:
        result = BalanceService(db).reconcile(
            account_ids=account_ids, threshold=parsed_threshold
        )
    return build_envelope(data=result)


@mcp_tool()
def accounts_balance_assertions(
    account_id: str | None = None,
) -> ResponseEnvelope[BalanceAssertionListPayload]:
    """List user-entered balance assertions.

    Args:
        account_id: Optional filter to a single account
    """
    with get_database(read_only=True) as db:
        result = BalanceService(db).list_assertions(account_id)
    return build_envelope(data=result)


# ─── Write tools (balance) ──────────────────────────────────────────────────


@mcp_tool(read_only=False)
def accounts_balance_assert(
    account_id: str, assertion_date: str, balance: float, notes: str | None = None
) -> ResponseEnvelope[BalanceAssertionPayload]:
    """Insert or update a manual balance assertion.

    Args:
        account_id: The account ID
        assertion_date: ISO date (YYYY-MM-DD)
        balance: Balance amount as decimal value
        notes: Optional free-text notes
    """
    parsed_date = _date.fromisoformat(assertion_date)
    parsed_balance = Decimal(str(balance))
    with get_database(read_only=False) as db:
        result = BalanceService(db).assert_balance(
            account_id=account_id,
            assertion_date=parsed_date,
            balance=parsed_balance,
            notes=notes,
            actor="mcp",
        )
    return build_envelope(data=result)


@mcp_tool(read_only=False, destructive=True)
def accounts_balance_assertion_delete(
    account_id: str, assertion_date: str
) -> ResponseEnvelope[BalanceAssertionDeletePayload]:
    """Delete a manual balance assertion. Silent no-op if no row exists.

    Args:
        account_id: The account ID
        assertion_date: ISO date (YYYY-MM-DD)
    """
    parsed_date = _date.fromisoformat(assertion_date)
    with get_database(read_only=False) as db:
        BalanceService(db).delete_assertion(account_id, parsed_date, actor="mcp")
    return build_envelope(
        data=BalanceAssertionDeletePayload(
            account_id=account_id, assertion_date=parsed_date, deleted=True
        )
    )


# ─── Resolution (free-text → account_id) ───────────────────────────────────


@mcp_tool()
def accounts_resolve(
    query: str, limit: int = 5
) -> ResponseEnvelope[AccountResolvePayload]:
    """Resolve a free-text account reference to an account_id.

    Fuzzy-matches against display_name, account_subtype, and institution_name
    from core.dim_accounts. Use this to convert natural-language references
    ("my Chase account", "checking", "Schwab brokerage") into an account_id
    before calling tools that require one.

    Args:
        query: Free-text account reference.
        limit: Maximum number of candidates to return (default 5).

    Returns ranked candidates with confidence scores in [0, 1]. Empty result
    or a top-match confidence below
    ``TabularProviderConfig.account_match_threshold`` (the shared fuzzy-match
    cutoff used by the tabular importer) emits an action hint suggesting the
    agent verify with the user.
    """
    from moneybin.config import get_settings

    with get_database(read_only=True) as db:
        payload = AccountService(db).resolve(query=query, limit=limit)
    threshold = get_settings().providers.tabular.account_match_threshold
    actions: list[str] = []
    if not payload.matches:
        actions.append(
            "No accounts matched the query. Try a broader query or call the `accounts` tool."
        )
    elif payload.matches[0].confidence < threshold:
        actions.append(
            "Top match has low confidence; verify with the user before taking action."
        )
    return build_envelope(data=payload, actions=actions)


# ─── Review tools (links) ──────────────────────────────────────────────────


@mcp_tool(domain="links")
def accounts_links_pending() -> ResponseEnvelope[AccountLinksPendingPayload]:
    """List pending account-link decisions, grouped by provisional account.

    Returns the review queue of provisional accounts with candidate merge
    proposals. Each group represents one provisional account (recently
    imported but not yet confirmed as a canonical entity) and its candidate
    existing accounts that may represent the same real-world account.

    For each candidate: decision_id, candidate_account_id, display name,
    confidence score, and the matching signal that fired (institution_last4
    or name). ref_value (the raw native reference, which can be a full
    account number) is never included.

    Decide each group via accounts_links_set. accounts_links_run (backfill
    discovery) and accounts links undo are not yet registered — deferred to
    follow-up units M1S.5b and M1L respectively.
    """
    with get_database(read_only=True) as db:
        svc = AccountLinksService(db, actor="mcp")
        groups = svc.pending()
        n_pending = svc.count_pending()
    payload = AccountLinksPendingPayload.from_service(groups, n_pending)
    return build_envelope(
        data=payload,
        total_count=n_pending,
        actions=[
            "Use accounts_links_set(decision_id, action='accept', "
            "target_account_id=<candidate_account_id>) to merge — the user is "
            "prompted to confirm the merge before anything is written",
            "Use accounts_links_set(decision_id, action='reject') to keep the "
            "provisional account as its own canonical account",
        ],
    )


@dataclass(frozen=True, slots=True)
class _AccountMergeProposal:
    """One pending account-merge decision, flattened for the confirmation prompt."""

    decision_id: str
    provisional_account_id: str
    provisional_display_name: str
    candidate_account_id: str
    candidate_display_name: str
    confidence: float | None
    signal: str | None
    blast_radius: dict[str, int]


def _load_pending_account_proposal(decision_id: str) -> _AccountMergeProposal:
    """Read the decision out of the live review queue, or raise if it isn't there."""
    with get_database(read_only=True) as db:
        service = AccountLinksService(db, actor="mcp")
        groups = service.pending()
        for group in groups:
            for candidate in group.candidates:
                if candidate.decision_id == decision_id:
                    impact = service.accept_impact(
                        decision_id,
                        target_account_id=candidate.candidate_account_id,
                    )
                    return _AccountMergeProposal(
                        decision_id=decision_id,
                        provisional_account_id=group.provisional_account_id,
                        provisional_display_name=group.provisional_display_name,
                        candidate_account_id=candidate.candidate_account_id,
                        candidate_display_name=candidate.candidate_display_name,
                        confidence=candidate.confidence,
                        signal=candidate.signal,
                        blast_radius=impact.blast_radius,
                    )
    raise UserError(
        f"No pending account-link decision '{decision_id}'.",
        code=error_codes.MUTATION_NOTHING_TO_DO,
        hint="List open decisions with accounts_links_pending.",
    )


def _account_link_binding(
    *,
    decision_id: str,
    target_account_id: str,
    provisional_account_id: str,
    blast_radius: dict[str, int],
) -> ConfirmationBinding:
    """Bind approval to one exact live account merge."""
    return ConfirmationBinding(
        arguments={
            "decision_id": decision_id,
            "action": "accept",
            "target_account_id": target_account_id,
        },
        resolved_ids=(
            provisional_account_id,
            target_account_id,
        ),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="account_identity_merge",
        blast_radius=blast_radius,
    )


def _account_confirm_message(p: _AccountMergeProposal) -> str:
    """Prompt text a human reads before two accounts' histories are fused.

    Names BOTH accounts and the weak signal the resolver fired on — the human
    cannot judge the merge without seeing what merges into what, and why the
    resolver refused to decide on its own.
    """
    confidence = "unscored" if p.confidence is None else f"{p.confidence:.2f}"
    return (
        "Confirm an account merge (this fuses two accounts' transaction "
        "histories and balances).\n\n"
        f"MERGE AWAY — provisional, account_id {p.provisional_account_id}:\n"
        f"  name {p.provisional_display_name or '(none)'}\n\n"
        f"INTO — survivor, account_id {p.candidate_account_id}:\n"
        f"  name {p.candidate_display_name or '(none)'}\n\n"
        f"Proposed on: signal {p.signal or 'unspecified'}, confidence "
        f"{confidence}. The resolver proposes a merge ONLY when it cannot bind "
        "on its own — this is an ambiguous match, not a certain one.\n\n"
        "Accepting re-points every accepted source reference from the "
        "provisional onto the survivor, so both accounts' transactions and "
        "balances become one account, and every other pending proposal touching "
        "the provisional is rejected. If these are not the same real-world "
        "account, the merged history and net worth will be wrong. Reversible "
        "via system_audit_undo(operation_id).\n\n"
        "Accept this merge?"
    )


def _apply_account_accept(
    decision_id: str,
    target_account_id: str,
    grant: ConfirmationGrant,
) -> None:
    # decided_by="user" is truthful only on this path: a human just ratified the
    # merge through the elicitation gate above.
    def verify(impact: AccountLinkAcceptImpact) -> None:
        grant.verify(
            _account_link_binding(
                decision_id=decision_id,
                target_account_id=target_account_id,
                provisional_account_id=impact.provisional_account_id,
                blast_radius=impact.blast_radius,
            )
        )

    with get_database(read_only=False) as db:
        AccountLinksService(db, actor="mcp").set(
            decision_id,
            target_account_id=target_account_id,
            decided_by="user",
            verify_accept=verify,
        )


def _apply_account_reject(decision_id: str) -> None:
    # decided_by="auto": no human ratified this reject — the agent called it.
    # The column's CHECK admits only 'auto' | 'user', and recording 'user' for a
    # decision no human made is precisely the falsehood the accept gate exists to
    # prevent. The MCP channel itself is preserved in app.audit_log (actor='mcp').
    with get_database(read_only=False) as db:
        AccountLinksService(db, actor="mcp").set(
            decision_id, target_account_id=None, decided_by="auto"
        )


@mcp_tool(
    domain="links",
    read_only=False,
    destructive=True,
    idempotent=False,
    # The accept path blocks on a human reading a merge confirmation (two
    # accounts + the reason they're ambiguous). The 30s default would routinely
    # fire first — and a cap that expires mid-decision means the user "accepts"
    # into a coroutine that was already cancelled. Same headroom as
    # investments_securities_links_set. Timing out is still safe (nothing is
    # written), just confusing.
    timeout_seconds=180.0,
)
async def accounts_links_set(
    decision_id: str,
    action: Literal["accept", "reject"],
    target_account_id: str | None = None,
    confirmation_token: str | None = None,
) -> ResponseEnvelope[AccountLinksSetPayload]:
    """Accept (merge) or standalone-reject one pending account-link decision.

    `action` is explicit — accept vs reject is never inferred from whether
    `target_account_id` has a value:

    - `action="accept"` + `target_account_id=<the decision's own
      candidate_account_id>` MERGES. This REQUIRES explicit human confirmation:
      the tool prompts the user through an MCP elicitation naming both accounts
      and the matching signal, and merges only if they agree. A client that
      cannot prompt receives mutation_confirmation_required with a short-lived,
      payload-bound token for an exact retry. `target_account_id` is also a
      confirming safety check: it must equal the decision's own candidate, so a
      mistyped or stale decision_id cannot merge into the wrong account.
      Mismatched, empty, or missing `target_account_id` raises
      mutation_invalid_input — it is never treated as a reject.
    - `action="reject"` (pass no `target_account_id`) STANDALONE-REJECTS — the
      provisional stays its own canonical account. Cheap and reversible, so no
      confirmation is required. Rejects every pending decision for the
      provisional account.

    A merge fuses two accounts: it re-points every accepted source reference
    from the provisional onto the survivor, so their transactions and balances
    become one account. If they are not the same real-world account, the merged
    history and net worth are wrong.

    Mutation surface: writes app.account_link_decisions + app.account_links.
    Reverse with system_audit_undo(operation_id) — find the operation_id via
    system_audit. Find pending decisions with accounts_links_pending.

    Args:
        decision_id: The decision id to act on (from accounts_links_pending).
        action: "accept" (merge, requires `target_account_id` + human
            confirmation) or "reject" (keep the provisional account standalone;
            pass no `target_account_id`).
        target_account_id: With action="accept", the candidate account_id to
            merge into — must equal the decision's own candidate_account_id.
            Invalid with action="reject".
        confirmation_token: Opaque payload-bound token returned to clients that
            cannot elicit. Used only with action="accept".
    """
    if action not in ("accept", "reject"):
        raise UserError(
            f"action must be 'accept' or 'reject' (got {action!r}).",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if action == "reject":
        if target_account_id is not None:
            raise UserError(
                "'target_account_id' is only valid with action='accept'. To "
                "reject, pass action='reject' with no 'target_account_id'.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        # DB work off the event loop: this tool is a coroutine (it awaits the
        # elicitation), so a blocking DuckDB write here would stall the server.
        await asyncio.to_thread(_apply_account_reject, decision_id)
        status = "rejected"
    else:
        if not target_account_id:
            raise UserError(
                "action='accept' requires 'target_account_id' = the decision's "
                "own candidate_account_id (see accounts_links_pending). An empty "
                "'target_account_id' is not a reject — pass action='reject' for "
                "that.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if confirmation_token is None:
            proposal = await asyncio.to_thread(
                _load_pending_account_proposal, decision_id
            )
            if target_account_id != proposal.candidate_account_id:
                # Refuse BEFORE prompting: a doomed merge must not cost the user a
                # confirmation. The service re-checks this; this is the boundary copy.
                raise UserError(
                    f"'target_account_id' does not match decision '{decision_id}' — "
                    "it must be that decision's own candidate_account_id.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                    hint="Re-read the decision with accounts_links_pending.",
                )
            binding = _account_link_binding(
                decision_id=decision_id,
                target_account_id=target_account_id,
                provisional_account_id=proposal.provisional_account_id,
                blast_radius=proposal.blast_radius,
            )
            message = _account_confirm_message(proposal)
        else:
            binding = None
            message = ""
        grant = await grant_confirmation_or_raise(
            binding=binding,
            message=message,
            confirmation_token=confirmation_token,
        )
        await asyncio.to_thread(
            _apply_account_accept,
            decision_id,
            target_account_id,
            grant,
        )
        status = "accepted"
    return build_envelope(
        data=AccountLinksSetPayload(decision_id=decision_id, status=status),
        actions=[
            "Use accounts_links_pending to review remaining pending decisions",
            "Reverse this decision with system_audit_undo(operation_id) — find "
            "the operation_id with system_audit",
        ],
    )


@mcp_tool(domain="links")
def accounts_links_history(
    limit: int = 50,
) -> ResponseEnvelope[AccountLinksHistoryPayload]:
    """Recent account-link decisions (all statuses), newest first.

    Args:
        limit: Maximum rows (default 50).
    """
    with get_database(read_only=True) as db:
        rows = AccountLinksService(db, actor="mcp").history(limit=limit)
    payload = AccountLinksHistoryPayload.from_rows(rows)
    return build_envelope(
        data=payload,
        actions=["Use accounts_links_pending for the active review queue"],
    )


@mcp_tool(domain="links", read_only=False)
def accounts_links_run() -> ResponseEnvelope[AccountLinksRunPayload]:
    """Backfill account-link proposals for existing accounts in core.dim_accounts.

    Surfaces weak-candidate merge proposals for accounts that already exist but
    have no pending proposal yet (e.g. accounts imported before the resolver
    candidate-pass existed, or cross-source twins minted separately). Writes
    ``pending`` ``app.account_link_decisions`` rows — the same shape the
    import-time resolver writes.

    Skips pairs that already have a decision in either direction (any status)
    and avoids double-proposing the same unordered pair within one run.

    Mutation surface: writes ``app.account_link_decisions``. Revert is via the
    audit log in ``app.audit_log`` (no undo tool yet; deferred to M1L).
    Review new proposals with ``accounts_links_pending``.

    Returns:
        Envelope with ``data.new_proposals`` — count of new pending decisions written.
    """
    with get_database(read_only=False) as db:
        new_proposals = AccountLinksService(db, actor="mcp").run()
    return build_envelope(
        data=AccountLinksRunPayload(new_proposals=new_proposals),
        actions=["Use accounts_links_pending to review the proposed merges"],
    )


# ─── Registration ──────────────────────────────────────────────────────────


def register_accounts_tools(mcp: FastMCP) -> None:
    """Register all v2 accounts namespace tools with the FastMCP server."""
    register(
        mcp,
        accounts,
        "accounts",
        "List accounts (default hides archived; supports type filter and redacted mode). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_get,
        "accounts_get",
        "Get one account's full settings + dim record. Raises a not_found error "
        "(code infra_not_found) if the account doesn't exist. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_summary,
        "accounts_summary",
        "Aggregate account snapshot — counts by type/subtype, archived, excluded, recent activity.",
    )
    register(
        mcp,
        accounts_set,
        "accounts_set",
        "Partial update of an account's settings. Behavioral fields: "
        "display_name, default_cost_basis_method (fifo/hifo/specific/average; "
        "invalid values raise mutation_invalid_input), include_in_net_worth, "
        "is_archived. Structural fields: "
        "official_name, last_four, account_subtype, holder_category, "
        "iso_currency_code, credit_limit. Pass None to leave a field "
        "unchanged; include a text field's name in clear_fields to clear it "
        "(booleans are not clearable). Archiving (is_archived=True) cascades "
        "include_in_net_worth=False atomically; unarchive does NOT restore "
        "the prior include value. "
        "Writes app.account_settings; revert by calling again with the prior "
        "values (no built-in undo). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balances,
        "accounts_balances",
        "Latest balance per account from fct_balances_daily (or as-of a date). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balance_history,
        "accounts_balance_history",
        "Per-account balance history (daily series with carry-forward + reconciliation deltas). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balance_reconcile,
        "accounts_balance_reconcile",
        "Threshold-filtered list of days where computed balance differs from asserted by more than `threshold`. "
        "Returns one row per (account, day) with the magnitude of the delta. Use when you want to find magnitude-level mismatches. "
        "Amounts use the accounting convention; currency named by summary.display_currency.",
    )
    register(
        mcp,
        accounts_balance_assertions,
        "accounts_balance_assertions",
        "List user-entered balance assertions. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balance_assert,
        "accounts_balance_assert",
        "Record an asserted balance for an account on a specific date. "
        "Used to reconcile against external statements when sync data is "
        "incomplete or wrong. "
        "Upsert semantics by (account_id, assertion_date) natural key — "
        "calling twice for the same date updates the existing assertion. "
        "Writes app.balance_assertions; remove with accounts_balance_assertion_delete (permanent — no undo). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balance_assertion_delete,
        "accounts_balance_assertion_delete",
        "Delete a manual balance assertion. "
        "Hard-deletes from app.balance_assertions — permanent, no revert; re-create with accounts_balance_assert.",
    )
    register(
        mcp,
        accounts_resolve,
        "accounts_resolve",
        "Resolve a free-text account reference (e.g., 'my Chase account', "
        "'checking') to an account_id. Returns ranked candidates with "
        "confidence scores. Use this before tools that require an account_id "
        "when you only have a natural-language reference.",
    )
    register(
        mcp,
        accounts_links_pending,
        "accounts_links_pending",
        "List pending account-link decisions grouped by provisional account. "
        "Returns the review queue of provisional accounts (recently imported, "
        "not yet confirmed canonical) with candidate merge proposals. Each "
        "candidate carries decision_id, account_id, display name, confidence "
        "score, and the match signal (institution_last4 or name). ref_value "
        "(raw native reference, which can be a full account number) is never "
        "included. Use accounts_links_set to accept or standalone-reject each group. "
        "Run accounts_links_run first to backfill proposals for pre-existing accounts.",
    )
    register(
        mcp,
        accounts_links_run,
        "accounts_links_run",
        "Backfill account-link proposals for accounts already in core.dim_accounts "
        "that have no pending proposal yet (e.g. cross-source twins imported separately). "
        "Writes pending app.account_link_decisions rows; skips pairs already proposed "
        "or decided in either direction. Returns data.new_proposals (count of new pending "
        "decisions written). Mutation surface: writes app.account_link_decisions; "
        "revert via app.audit_log (no undo tool yet). "
        "Review results with accounts_links_pending.",
    )
    register(
        mcp,
        accounts_links_set,
        "accounts_links_set",
        "Accept (merge) or standalone-reject one pending account-link decision. "
        "action='accept' + target_account_id=<the decision's own "
        "candidate_account_id> MERGES: it prompts the user to confirm (MCP "
        "elicitation naming both accounts and the match signal) and merges only "
        "on their explicit agreement — a merge fuses two accounts' transaction "
        "histories and balances, so the agent cannot accept one on its own. On a "
        "client without elicitation, accept fails with "
        "mutation_confirmation_required and returns a short-lived opaque "
        "confirmation_token; repeat the exact retry with that token. "
        "target_account_id must equal the decision's own candidate (mismatched, "
        "empty, or missing target_account_id raises mutation_invalid_input — it "
        "is NEVER read as a reject). action='reject' (no target_account_id) keeps "
        "the provisional as its own canonical account and rejects every pending "
        "decision for it. Writes app.account_link_decisions + app.account_links; "
        "reverse with system_audit_undo(operation_id). Discover pending decisions "
        "with accounts_links_pending.",
    )
    register(
        mcp,
        accounts_links_history,
        "accounts_links_history",
        "Recent account-link decisions (all statuses), newest first. "
        "Read-only. Filter by limit. Use accounts_links_pending for the "
        "active review queue.",
    )
