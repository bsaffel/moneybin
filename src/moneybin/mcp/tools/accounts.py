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

The granular callbacks named in ``_LEGACY_INTERNAL_CALLBACKS`` are internal
helpers retained for standard-boundary composition and parity. They are never
individually registered and remain undecorated; ``test_tool_surface_budget``
guards against accidental publication.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date as _date
from decimal import Decimal
from functools import cmp_to_key
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, StrictBool

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import RecoveryAction, UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.pagination import (
    KeysetPosition,
    compare_keyset,
    decode_keyset_cursor,
    encode_keyset_cursor,
)
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.mcp.write_contracts import FiniteDecimal
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.accounts import (
    AccountDetail,
    AccountLinksHistoryPayload,
    AccountLinksPendingPayload,
    AccountLinksRunPayload,
    AccountLinksSetPayload,
    AccountListPayload,
    AccountResolvePayload,
    AccountsBalancesAssertionsView,
    AccountsBalancesCoarsePayload,
    AccountsBalancesHistoryView,
    AccountsBalancesLatestView,
    AccountsBalancesReconcileView,
    AccountsCoarsePayload,
    AccountsDetailView,
    AccountSettingsPayload,
    AccountsListView,
    AccountsResolveView,
    AccountsSummaryView,
    AccountSummaryStats,
    BalanceAssertionStatePayload,
)
from moneybin.privacy.payloads.balances import (
    BalanceAssertionDeletePayload,
    BalanceAssertionListPayload,
    BalanceAssertionPayload,
    BalanceObservationListPayload,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_links_service import (
    AccountLinkAcceptImpact,
    AccountLinksService,
)
from moneybin.services.account_service import CLEAR, AccountService
from moneybin.services.balance_service import BalanceAssertionSnapshot, BalanceService
from moneybin.services.entity_reference import (
    AmbiguousEntity,
    EntityCandidate,
    MissingEntity,
    resolve_entity_reference,
)
from moneybin.services.mutation_context import current_operation_id

# ─── Read tools (entity) ──────────────────────────────────────────────────


def accounts(
    include_archived: bool = False, type_filter: str | None = None
) -> ResponseEnvelope[AccountListPayload]:
    """List accounts in MoneyBin.

    Args:
        include_archived: Include archived accounts (default: hide them)
        type_filter: Match account_type (canonical: depository, credit, loan, investment, other) or account_subtype (source detail: checking, savings, credit card, ...), case-insensitive

    Returns the resolved view from core.dim_accounts including display_name,
    institution_name, account_type, account_subtype, holder_category,
    currency_code, archived, include_in_net_worth, last_four, and
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
            "Use reports(report_id='core:spending') to drill into spending",
        ],
    )


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
    "currency_code",
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
    currency_code: str | None = None,
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
      ``currency_code``, ``credit_limit``.

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
    ``"holder_category"``, ``"currency_code"``, ``"credit_limit"``,
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
        "currency_code": currency_code,
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
        currency_code=d.get("currency_code"),  # type: ignore[arg-type]
        credit_limit=d.get("credit_limit"),  # type: ignore[arg-type]
        default_cost_basis_method=d.get("default_cost_basis_method"),  # type: ignore[arg-type]
        include_in_net_worth=bool(d["include_in_net_worth"]),
        archived=bool(d["archived"]),
        warnings=[w.get("message", str(w)) for w in warnings] if warnings else [],
        cascaded_include_in_net_worth=False if is_archived is True else None,
    )
    return build_envelope(data=payload)


# ─── Read tools (balance) ──────────────────────────────────────────────────


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
    return _resolve_accounts(query=query, limit=limit)


def _resolve_accounts(
    query: str,
    limit: int | None,
) -> ResponseEnvelope[AccountResolvePayload]:
    """Resolve account candidates for both legacy and coarse read surfaces."""
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
            "Use identity_links_decide with kind='account_link', decision='accept', "
            "decision_id, and target_id to merge after confirmation",
            "Use identity_links_decide with kind='account_link', decision='reject', "
            "and decision_id to keep the provisional account standalone",
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
        hint="List open decisions with reviews(kind='account_links').",
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
                "action='accept' requires 'target_account_id' = the target_id "
                "shown by reviews(kind='account_links'). An empty "
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
                    "it must be the target_id shown by reviews(kind='account_links').",
                    code=error_codes.MUTATION_INVALID_INPUT,
                    hint="Re-read the decision with reviews(kind='account_links').",
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
            "Use reviews(kind='account_links') for remaining pending decisions",
            "Reverse this decision with system_audit_undo(operation_id) — find "
            "the operation_id with system_audit",
        ],
    )


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
        actions=["Use reviews(kind='account_links') for the active review queue"],
    )


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
        actions=["Use reviews(kind='account_links') to review proposed merges"],
    )


# ─── Standard coarse reads ────────────────────────────────────────────────


async def _run_account_read[T](
    callback: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Delegate to an existing read body without a second privacy audit."""
    body = cast(Callable[..., T], inspect.unwrap(callback))
    return await asyncio.to_thread(body, *args, **kwargs)


def _coarse_envelope[T](
    data: T,
    *,
    contract_type: type[Any],
    total_count: int,
    returned_count: int,
    next_cursor: str | None = None,
    period: str | None = None,
    display_currency: str = "USD",
    actions: list[str] | None = None,
    has_more: bool | None = None,
) -> ResponseEnvelope[T]:
    """Build and redact a dynamically classified account-read envelope."""
    classes = extract_data_classes(contract_type)
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(T, redact_typed(data, None))
    envelope = cast(
        ResponseEnvelope[T],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=total_count,
            returned_count=returned_count,
            next_cursor=next_cursor,
            period=period,
            display_currency=display_currency,
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )
    return replace(
        envelope,
        summary=replace(
            envelope.summary,
            has_more=next_cursor is not None if has_more is None else has_more,
        ),
    )


def _coarse_position(
    cursor: str | None,
    *,
    tool: Literal["accounts", "accounts_balances"],
    view: str,
    filters: dict[str, object],
    key_size: int,
) -> KeysetPosition | None:
    """Decode one scope-bound cursor and validate its string key shape."""
    if cursor is None:
        return None
    code = "ACCOUNT_CURSOR_INVALID" if tool == "accounts" else "BALANCE_CURSOR_INVALID"
    try:
        position = decode_keyset_cursor(
            cursor,
            namespace=tool,
            scope={"filters": filters, "view": view},
        )
    except ValueError as exc:
        raise UserError("Invalid pagination cursor.", code=code) from exc
    if (
        len(position.snapshot) != key_size
        or len(position.after) != key_size
        or not all(
            type(value) is str for value in (*position.snapshot, *position.after)
        )
    ):
        raise UserError("Invalid pagination cursor.", code=code)
    return position


def _keyset_page[T](
    rows: list[T],
    *,
    tool: Literal["accounts", "accounts_balances"],
    view: str,
    limit: int,
    position: KeysetPosition | None,
    filters: dict[str, object],
    key: Callable[[T], tuple[str, ...]],
    directions: tuple[Literal["asc", "desc"], ...],
) -> tuple[list[T], str | None, int]:
    """Page immutable keys behind the cursor's first-page prepend boundary."""
    code = "ACCOUNT_CURSOR_INVALID" if tool == "accounts" else "BALANCE_CURSOR_INVALID"

    def compare_rows(left: T, right: T) -> int:
        return compare_keyset(key(left), key(right), directions)

    ordered = sorted(rows, key=cmp_to_key(compare_rows))
    if position is None:
        if not ordered:
            return [], None, 0
        snapshot = key(ordered[0])
        eligible = ordered
        total_count = len(ordered)
    else:
        snapshot = cast(tuple[str, ...], position.snapshot)
        after = cast(tuple[str, ...], position.after)
        try:
            if compare_keyset(after, snapshot, directions) < 0:
                raise ValueError("continuation precedes snapshot")
            eligible = [
                row
                for row in ordered
                if compare_keyset(key(row), snapshot, directions) >= 0
                and compare_keyset(key(row), after, directions) > 0
            ]
        except ValueError as exc:
            raise UserError("Invalid pagination cursor.", code=code) from exc
        total_count = position.total

    page = eligible[:limit]
    if len(eligible) <= limit or not page:
        return page, None, total_count
    next_cursor = encode_keyset_cursor(
        namespace=tool,
        scope={"filters": filters, "view": view},
        snapshot=snapshot,
        after=key(page[-1]),
        total=total_count,
    )
    return page, next_cursor, total_count


def _account_candidates(payload: AccountListPayload) -> list[EntityCandidate]:
    """Project account rows into the shared deterministic resolver contract."""
    candidates: list[EntityCandidate] = []
    for account in payload.rows:
        display_name = account.display_name or account.account_id
        aliases = tuple(
            dict.fromkeys(
                value
                for value in (
                    account.institution_name,
                    account.account_type,
                    account.account_subtype,
                )
                if value is not None and value != display_name
            )
        )
        candidates.append(
            EntityCandidate(
                entity_id=account.account_id,
                display_name=display_name,
                aliases=aliases,
            )
        )
    return candidates


async def _resolve_account_reference(
    reference: str,
    *,
    include_closed: bool,
) -> str:
    """Resolve a user-facing account reference through the shared ladder."""
    response = await _run_account_read(
        accounts,
        include_archived=True,
        type_filter=None,
    )
    for account in response.data.rows:
        if account.account_id == reference:
            return account.account_id
    candidates = _account_candidates(
        AccountListPayload(
            rows=[
                account
                for account in response.data.rows
                if include_closed or not account.archived
            ]
        )
    )
    resolution = resolve_entity_reference(
        reference,
        candidates,
    )
    if isinstance(resolution, AmbiguousEntity):
        raise UserError(
            "The account reference matches multiple accounts.",
            code="ENTITY_REFERENCE_AMBIGUOUS",
            details={"candidate_ids": list(resolution.candidate_ids)},
        )
    if isinstance(resolution, MissingEntity):
        raise UserError(
            "The account reference did not match an account.",
            code="ENTITY_REFERENCE_NOT_FOUND",
            details={"candidate_ids": []},
        )
    return resolution.entity_id


def _account_actions(
    actions: list[str],
    *,
    limit: int,
    next_cursor: str | None,
    include_closed: bool = False,
) -> list[str]:
    """Preserve legacy hints and add an account-list continuation."""
    selected = list(actions)
    if next_cursor is not None:
        selected.append(
            f"Continue with accounts(view='list', "
            f"include_closed={include_closed!r}, limit={limit}, "
            f"cursor='{next_cursor}')"
        )
    return list(dict.fromkeys(selected))


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.CRITICAL)
async def accounts_coarse(
    view: Literal["list", "detail", "summary", "resolve"] = "list",
    reference: str | None = None,
    query: str | None = None,
    include_closed: StrictBool = False,
    limit: Annotated[int, Field(strict=True, ge=1)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[AccountsCoarsePayload]:
    """List, inspect, summarize, or resolve accounts through one read contract."""
    if view in ("list", "summary"):
        if reference is not None:
            raise UserError(
                "reference is not valid for this account view.",
                code="ACCOUNT_REFERENCE_NOT_ALLOWED",
            )
        if query is not None:
            raise UserError(
                "query is not valid for this account view.",
                code="ACCOUNT_QUERY_NOT_ALLOWED",
            )
    elif view == "detail":
        if reference is None:
            raise UserError(
                "Account detail requires a reference.",
                code="ACCOUNT_REFERENCE_REQUIRED",
            )
        if query is not None:
            raise UserError(
                "query is not valid for account detail.",
                code="ACCOUNT_QUERY_NOT_ALLOWED",
            )
    else:
        if query is None:
            raise UserError(
                "Account resolution requires a query.",
                code="ACCOUNT_QUERY_REQUIRED",
            )
        if reference is not None:
            raise UserError(
                "reference is not valid for account resolution.",
                code="ACCOUNT_REFERENCE_NOT_ALLOWED",
            )

    if view in ("detail", "summary", "resolve") and cursor is not None:
        raise UserError(
            "This account view does not accept a pagination cursor.",
            code="ACCOUNT_CURSOR_NOT_ALLOWED",
        )
    if view in ("summary", "resolve") and include_closed:
        raise UserError(
            "include_closed is not valid for this account view.",
            code="ACCOUNT_INCLUDE_CLOSED_NOT_ALLOWED",
        )
    if view in ("detail", "summary") and limit != 100:
        raise UserError(
            "limit is not valid for this account view.",
            code="ACCOUNT_LIMIT_NOT_ALLOWED",
        )

    if view == "list":
        filters: dict[str, object] = {"include_closed": bool(include_closed)}
        position = _coarse_position(
            cursor,
            tool="accounts",
            view="list",
            filters=filters,
            key_size=1,
        )
        response = await _run_account_read(
            accounts,
            include_archived=bool(include_closed),
            type_filter=None,
        )
        page, next_cursor, total_count = _keyset_page(
            response.data.rows,
            tool="accounts",
            view="list",
            limit=limit,
            position=position,
            filters=filters,
            key=lambda row: (row.account_id,),
            directions=("asc",),
        )
        payload = AccountsListView(rows=page)
        return _coarse_envelope(
            payload,
            contract_type=AccountsListView,
            total_count=total_count,
            returned_count=len(page),
            next_cursor=next_cursor,
            display_currency=response.summary.display_currency,
            actions=_account_actions(
                response.actions,
                limit=limit,
                next_cursor=next_cursor,
                include_closed=bool(include_closed),
            ),
        )

    if view == "detail":
        account_id = await _resolve_account_reference(
            cast(str, reference),
            include_closed=bool(include_closed),
        )
        response = await _run_account_read(accounts_get, account_id)
        payload = AccountsDetailView(account=response.data)
        return _coarse_envelope(
            payload,
            contract_type=AccountsDetailView,
            total_count=response.summary.total_count,
            returned_count=response.summary.returned_count,
            display_currency=response.summary.display_currency,
            actions=response.actions,
        )

    if view == "summary":
        response = await _run_account_read(accounts_summary)
        payload = AccountsSummaryView(summary=response.data)
        return _coarse_envelope(
            payload,
            contract_type=AccountsSummaryView,
            total_count=response.summary.total_count,
            returned_count=response.summary.returned_count,
            display_currency=response.summary.display_currency,
            actions=response.actions,
        )

    response = await _run_account_read(
        _resolve_accounts,
        query=cast(str, query),
        limit=None,
    )
    total_count = len(response.data.matches)
    page = response.data.matches[:limit]
    has_more = total_count > len(page)
    actions = list(response.actions)
    if has_more:
        actions.append(
            "Refine the account query or increase limit to inspect more candidates."
        )
    payload = AccountsResolveView(matches=page)
    return _coarse_envelope(
        payload,
        contract_type=AccountsResolveView,
        total_count=total_count,
        returned_count=len(page),
        has_more=has_more,
        display_currency=response.summary.display_currency,
        actions=actions,
    )


def _history_period(start: _date | None, end: _date | None) -> str | None:
    """Render the selected history window in envelope metadata."""
    if start is not None and end is not None:
        return f"{start.isoformat()} to {end.isoformat()}"
    if start is not None:
        return f"from {start.isoformat()}"
    if end is not None:
        return f"through {end.isoformat()}"
    return None


def _balance_actions(
    actions: list[str],
    *,
    view: Literal["latest", "history", "assertions", "reconcile"],
    limit: int,
    next_cursor: str | None,
    reference: str | None,
    start: _date | None,
    end: _date | None,
    as_of: _date | None,
    threshold: Decimal | None,
) -> list[str]:
    """Preserve balance hints and add a public continuation hint."""
    selected = list(actions)
    if next_cursor is not None:
        arguments = [f"view='{view}'"]
        if reference is not None:
            arguments.append(f"reference={reference!r}")
        if start is not None:
            arguments.append(f"start={start.isoformat()!r}")
        if end is not None:
            arguments.append(f"end={end.isoformat()!r}")
        if as_of is not None:
            arguments.append(f"as_of={as_of.isoformat()!r}")
        if threshold is not None:
            arguments.append(f"threshold={threshold}")
        arguments.extend((f"limit={limit}", f"cursor='{next_cursor}'"))
        selected.append(f"Continue with accounts_balances({', '.join(arguments)})")
    return list(dict.fromkeys(selected))


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
async def accounts_balances_coarse(
    view: Literal["latest", "history", "assertions", "reconcile"] = "latest",
    reference: str | None = None,
    start: _date | None = None,
    end: _date | None = None,
    as_of: _date | None = None,
    threshold: Annotated[FiniteDecimal, Field(ge=0)] | None = None,
    limit: Annotated[int, Field(strict=True, ge=1)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[AccountsBalancesCoarsePayload]:
    """Return balances, history, assertions, or reconciliation deltas."""
    if view != "history" and (start is not None or end is not None):
        raise UserError(
            "start and end are only valid for balance history.",
            code="BALANCE_DATES_NOT_ALLOWED",
        )
    if view != "latest" and as_of is not None:
        raise UserError(
            "as_of is only valid for latest balances.",
            code="BALANCE_AS_OF_NOT_ALLOWED",
        )
    if view != "reconcile" and threshold is not None:
        raise UserError(
            "threshold is only valid for balance reconciliation.",
            code="BALANCE_THRESHOLD_NOT_ALLOWED",
        )
    if view == "history" and reference is None:
        raise UserError(
            "Balance history requires an account reference.",
            code="ACCOUNT_REFERENCE_REQUIRED",
        )
    if start is not None and end is not None and start > end:
        raise UserError(
            "Balance history start must not be after end.",
            code="BALANCE_DATE_RANGE_INVALID",
        )

    filters: dict[str, object] = {
        "as_of": as_of.isoformat() if as_of is not None else None,
        "end": end.isoformat() if end is not None else None,
        "reference": reference.casefold().strip() if reference is not None else None,
        "start": start.isoformat() if start is not None else None,
        "threshold": str(threshold) if threshold is not None else None,
    }
    key_size = 1 if view == "latest" else 2
    position = _coarse_position(
        cursor,
        tool="accounts_balances",
        view=view,
        filters=filters,
        key_size=key_size,
    )
    account_id = (
        await _resolve_account_reference(reference, include_closed=True)
        if reference is not None
        else None
    )

    if view == "latest":
        response = await _run_account_read(
            accounts_balances,
            account_ids=[account_id] if account_id is not None else None,
            as_of_date=as_of.isoformat() if as_of is not None else None,
        )
        page, next_cursor, total_count = _keyset_page(
            response.data.observations,
            tool="accounts_balances",
            view="latest",
            limit=limit,
            position=position,
            filters=filters,
            key=lambda row: (row.account_id,),
            directions=("asc",),
        )
        payload = AccountsBalancesLatestView(observations=page)
        contract_type = AccountsBalancesLatestView
    elif view == "history":
        response = await _run_account_read(
            accounts_balance_history,
            cast(str, account_id),
            from_date=start.isoformat() if start is not None else None,
            to_date=end.isoformat() if end is not None else None,
        )
        page, next_cursor, total_count = _keyset_page(
            response.data.observations,
            tool="accounts_balances",
            view="history",
            limit=limit,
            position=position,
            filters=filters,
            key=lambda row: (row.balance_date.isoformat(), row.account_id),
            directions=("asc", "asc"),
        )
        payload = AccountsBalancesHistoryView(observations=page)
        contract_type = AccountsBalancesHistoryView
    elif view == "assertions":
        response = await _run_account_read(
            accounts_balance_assertions,
            account_id,
        )
        page, next_cursor, total_count = _keyset_page(
            response.data.assertions,
            tool="accounts_balances",
            view="assertions",
            limit=limit,
            position=position,
            filters=filters,
            key=lambda row: (row.account_id, row.assertion_date.isoformat()),
            directions=("asc", "desc"),
        )
        payload = AccountsBalancesAssertionsView(assertions=page)
        contract_type = AccountsBalancesAssertionsView
    else:
        response = await _run_account_read(
            accounts_balance_reconcile,
            account_ids=[account_id] if account_id is not None else None,
            threshold=threshold if threshold is not None else Decimal("0.01"),
        )
        page, next_cursor, total_count = _keyset_page(
            response.data.observations,
            tool="accounts_balances",
            view="reconcile",
            limit=limit,
            position=position,
            filters=filters,
            key=lambda row: (row.account_id, row.balance_date.isoformat()),
            directions=("asc", "desc"),
        )
        payload = AccountsBalancesReconcileView(observations=page)
        contract_type = AccountsBalancesReconcileView

    return _coarse_envelope(
        payload,
        contract_type=contract_type,
        total_count=total_count,
        returned_count=len(page),
        next_cursor=next_cursor,
        period=_history_period(start, end) if view == "history" else None,
        display_currency=response.summary.display_currency,
        actions=_balance_actions(
            response.actions,
            view=view,
            limit=limit,
            next_cursor=next_cursor,
            reference=reference,
            start=start,
            end=end,
            as_of=as_of,
            threshold=threshold,
        ),
    )


def register_accounts_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard account reads."""
    register(
        mcp,
        accounts_coarse,
        "accounts",
        "List accounts, inspect one deterministic reference, summarize the "
        "portfolio, or return resolution candidates. Amounts are in "
        "the currency named by summary.display_currency.",
        privacy_actor="accounts",
    )
    register(
        mcp,
        accounts_balances_coarse,
        "accounts_balances",
        "Return balances by date, history, assertions, or reconciliation deltas. "
        "Resolve one account by reference; amounts are positions in "
        "summary.display_currency.",
        privacy_actor="accounts_balances",
    )


# ─── Standard coarse writes ───────────────────────────────────────────────


_BALANCE_ASSERTION_MAX = Decimal("9999999999999999.99")

BalanceAmount = Annotated[
    FiniteDecimal,
    Field(
        ge=-_BALANCE_ASSERTION_MAX,
        le=_BALANCE_ASSERTION_MAX,
        max_digits=18,
        decimal_places=2,
    ),
]


def _load_balance_assertion_snapshot(
    account_id: str,
    as_of: _date,
) -> BalanceAssertionSnapshot | None:
    """Load one assertion without retaining a read connection."""
    with get_database(read_only=True) as db:
        return BalanceService(db).get_assertion_snapshot(account_id, as_of)


def _balance_assertion_remove_binding(
    snapshot: BalanceAssertionSnapshot,
) -> ConfirmationBinding:
    """Bind approval to the exact live assertion about to be removed."""
    return ConfirmationBinding(
        arguments={
            "account_id": snapshot.account_id,
            "as_of": snapshot.assertion_date.isoformat(),
            "state": "absent",
            "assertion": {
                "amount": str(snapshot.balance),
                "notes": snapshot.notes,
                "created_at": snapshot.created_at,
                "updated_at": snapshot.updated_at,
            },
        },
        resolved_ids=(snapshot.account_id, snapshot.assertion_date.isoformat()),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="balance_assertion_remove",
        blast_radius={"assertions": 1},
    )


def _assert_balance_present(
    account_id: str,
    as_of: _date,
    amount: Decimal,
) -> Literal["present", "absent"]:
    """Upsert an assertion unless its amount already matches the target."""
    with get_database(read_only=False) as db:
        service = BalanceService(db)
        prior = service.get_assertion(account_id, as_of)
        if prior is not None and prior.balance == amount:
            raise UserError(
                "The balance assertion already matches the requested state.",
                code=error_codes.MUTATION_NOTHING_TO_DO,
            )
        service.assert_balance(
            account_id=account_id,
            assertion_date=as_of,
            balance=amount,
            notes=prior.notes if prior is not None else None,
            actor="mcp",
        )
    return "present" if prior is not None else "absent"


def _remove_balance_assertion(
    account_id: str,
    as_of: _date,
    grant: ConfirmationGrant,
) -> None:
    """Recompute, verify, and remove one assertion in one transaction."""

    def verify(assertion: BalanceAssertionSnapshot) -> None:
        grant.verify(_balance_assertion_remove_binding(assertion))

    with get_database(read_only=False) as db:
        removed = BalanceService(db).delete_assertion(
            account_id,
            as_of,
            actor="mcp",
            verify=verify,
        )
    if not removed:
        raise UserError(
            "The balance assertion is already absent.",
            code=error_codes.MUTATION_NOTHING_TO_DO,
        )


@mcp_tool(read_only=False, destructive=True, idempotent=True)
async def accounts_balance_assert_coarse(
    account: str,
    as_of: _date,
    state: Literal["present", "absent"] = "present",
    amount: BalanceAmount | None = None,
    confirmation_token: str | None = None,
) -> ResponseEnvelope[BalanceAssertionStatePayload]:
    """Declare one resolved account's balance assertion present or absent."""
    if state == "present" and amount is None:
        raise UserError(
            "state='present' requires amount.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if state == "absent" and amount is not None:
        raise UserError(
            "amount is only valid with state='present'.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if state == "present" and confirmation_token is not None:
        raise UserError(
            "confirmation_token is only valid with state='absent'.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )

    account_id = await _resolve_account_reference(account, include_closed=True)
    if state == "present":
        prior_state = await asyncio.to_thread(
            _assert_balance_present,
            account_id,
            as_of,
            cast(Decimal, amount),
        )
    else:
        if confirmation_token is None:
            snapshot = await asyncio.to_thread(
                _load_balance_assertion_snapshot,
                account_id,
                as_of,
            )
            if snapshot is None:
                raise UserError(
                    "The balance assertion is already absent.",
                    code=error_codes.MUTATION_NOTHING_TO_DO,
                )
            binding = _balance_assertion_remove_binding(snapshot)
            message = (
                "Remove the manual balance assertion for account "
                f"{account_id} on {as_of.isoformat()}? This hard-deletes the "
                "assertion; recover it with system_audit_undo(operation_id)."
            )
        else:
            binding = None
            message = ""
        grant = await grant_confirmation_or_raise(
            binding=binding,
            message=message,
            confirmation_token=confirmation_token,
        )
        await asyncio.to_thread(
            _remove_balance_assertion,
            account_id,
            as_of,
            grant,
        )
        prior_state = "present"

    operation_id = current_operation_id()
    return build_envelope(
        data=BalanceAssertionStatePayload(
            account_id=account_id,
            as_of=as_of,
            prior_state=prior_state,
            state=state,
            operation_id=operation_id,
        ),
        recovery_actions=[
            RecoveryAction(
                tool="system_audit",
                arguments={"view": "detail", "operation_id": operation_id},
                rationale="Inspect the exact mutation and its undoability.",
                confidence="suggested",
                idempotent=True,
            ),
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": operation_id},
                rationale="Reverse this balance assertion mutation.",
                confidence="certain",
                idempotent=False,
            ),
        ],
    )


def register_accounts_coarse_writes(mcp: FastMCP) -> None:
    """Register the standard declarative balance assertion write."""
    register(
        mcp,
        accounts_balance_assert_coarse,
        "accounts_balance_assert",
        "Declare a manual balance assertion present or absent for one resolved "
        "account and date. Present requires amount and upserts "
        "app.balance_assertions; absent forbids amount, requires exact "
        "payload-bound confirmation, and hard-deletes the assertion. Repeat an "
        "unchanged target state returns mutation_nothing_to_do. Reverse a "
        "mutation with system_audit_undo(operation_id). Amounts are positions "
        "in the currency named by summary.display_currency.",
        privacy_actor="accounts_balance_assert",
        input_schema_extra={
            "allOf": [
                {
                    "if": {
                        "properties": {"state": {"const": "absent"}},
                        "required": ["state"],
                    },
                    "then": {
                        "not": {"anyOf": [{"required": ["amount"]}]},
                    },
                    "else": {
                        "required": ["amount"],
                        "properties": {
                            "amount": {"not": {"type": "null"}},
                        },
                        "not": {
                            "anyOf": [{"required": ["confirmation_token"]}],
                        },
                    },
                }
            ]
        },
    )


# ─── Registration ──────────────────────────────────────────────────────────

_LEGACY_INTERNAL_CALLBACKS = (
    accounts,
    accounts_get,
    accounts_summary,
    accounts_balances,
    accounts_balance_history,
    accounts_balance_reconcile,
    accounts_balance_assertions,
    accounts_balance_assert,
    accounts_balance_assertion_delete,
    accounts_resolve,
    accounts_links_pending,
    accounts_links_set,
    accounts_links_history,
)


def register_accounts_tools(mcp: FastMCP) -> None:
    """Register the standard account read and write boundaries."""
    register_accounts_coarse_reads(mcp)
    register(
        mcp,
        accounts_set,
        "accounts_set",
        "Partial update of an account's settings. Behavioral fields: "
        "display_name, default_cost_basis_method (fifo/hifo/specific/average; "
        "invalid values raise mutation_invalid_input), include_in_net_worth, "
        "is_archived. Structural fields: "
        "official_name, last_four, account_subtype, holder_category, "
        "currency_code, credit_limit. Pass None to leave a field "
        "unchanged; include a text field's name in clear_fields to clear it "
        "(booleans are not clearable). Archiving (is_archived=True) cascades "
        "include_in_net_worth=False atomically; unarchive does NOT restore "
        "the prior include value. "
        "Writes app.account_settings; revert by calling again with the prior "
        "values (no built-in undo). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register_accounts_coarse_writes(mcp)
