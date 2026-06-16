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

from datetime import date as _date
from decimal import Decimal

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
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
    LinkCandidateRow,
    LinkHistoryRow,
    LinkPendingGroup,
)
from moneybin.privacy.payloads.balances import (
    BalanceAssertionDeletePayload,
    BalanceAssertionListPayload,
    BalanceAssertionPayload,
    BalanceObservationListPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_links_service import AccountLinksService
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
      ``include_in_net_worth`` — toggle inclusion in net-worth aggregates.
      ``is_archived`` — archive / unarchive flag.

    Pass ``None`` to leave a field unchanged. To explicitly clear a text field
    back to NULL, include its name in ``clear_fields``. Valid clearable names:
    ``"official_name"``, ``"last_four"``, ``"account_subtype"``,
    ``"holder_category"``, ``"iso_currency_code"``, ``"credit_limit"``,
    ``"display_name"``. Booleans (``include_in_net_worth``, ``is_archived``) are
    not clearable — pass the explicit value.

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


def _link_signal(match_signals: object) -> str:
    """Extract 'signal' from an already-decoded match_signals dict."""
    try:
        return str(match_signals["signal"])  # type: ignore[index]  # Any-typed dict
    except (KeyError, TypeError):
        return ""


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
    payload = AccountLinksPendingPayload(
        groups=[
            LinkPendingGroup(
                provisional_account_id=g.provisional_account_id,
                provisional_display_name=g.provisional_display_name,
                candidates=[
                    LinkCandidateRow(
                        decision_id=c.decision_id,
                        candidate_account_id=c.candidate_account_id,
                        candidate_display_name=c.candidate_display_name,
                        confidence=float(c.confidence)
                        if c.confidence is not None
                        else None,
                        signal=c.signal,
                    )
                    for c in g.candidates
                ],
            )
            for g in groups
        ],
        n_pending=n_pending,
    )
    return build_envelope(
        data=payload,
        total_count=n_pending,
        actions=[
            "Use accounts_links_set to merge (pass candidate_account_id as "
            "target_account_id) or standalone-reject (pass null) each decision",
        ],
    )


@mcp_tool(domain="links", read_only=False)
def accounts_links_set(
    decision_id: str,
    target_account_id: str | None,
) -> ResponseEnvelope[AccountLinksSetPayload]:
    """Accept (merge) or standalone-reject one pending account-link decision.

    Mutates app.account_link_decisions (sets status) and, on accept,
    re-points app.account_links source_native entries from the provisional
    account onto target_account_id. On standalone-reject (target_account_id
    = null), rejects every pending decision for the provisional account —
    it remains its own canonical account.

    target_account_id MUST be passed explicitly — there is no default:
    - Pass the candidate_account_id from accounts_links_pending to MERGE.
    - Pass null to STANDALONE-REJECT (keep the provisional as its own entity).
    Omitting it would default the merge, which may cause unintended binding.

    Mutation surface: writes app.account_link_decisions + app.account_links.
    Revert is via the audit log in app.audit_log (no undo tool yet; undo
    is deferred to the M1L audit-undo consumer). Find pending decisions
    with accounts_links_pending.

    Args:
        decision_id: The decision id to act on (from accounts_links_pending).
        target_account_id: The candidate account_id to merge into, or null
            to standalone-reject (keep provisional as its own canonical account).
    """
    with get_database(read_only=False) as db:
        AccountLinksService(db, actor="mcp").set(
            decision_id, target_account_id=target_account_id, decided_by="user"
        )
    status = "accepted" if target_account_id is not None else "rejected"
    return build_envelope(
        data=AccountLinksSetPayload(decision_id=decision_id, status=status),
        actions=[
            "Use accounts_links_pending to review remaining pending decisions",
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
    payload = AccountLinksHistoryPayload(
        decisions=[
            LinkHistoryRow(
                decision_id=r["decision_id"],
                provisional_account_id=r["provisional_account_id"],
                candidate_account_id=r["candidate_account_id"],
                status=r["status"],
                decided_by=r["decided_by"],
                decided_at=str(r["decided_at"]) if r.get("decided_at") else None,
                confidence=(
                    float(r["confidence_score"])
                    if r.get("confidence_score") is not None
                    else None
                ),
                signal=_link_signal(r.get("match_signals")),
            )
            for r in rows
        ]
    )
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
        "display_name, include_in_net_worth, is_archived. Structural fields: "
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
        "Pass target_account_id = candidate_account_id to MERGE the provisional "
        "account into the candidate. Pass null to STANDALONE-REJECT — the "
        "provisional stays its own canonical account. target_account_id has no "
        "default: pass it explicitly to avoid accidental standalone rejection. "
        "Writes app.account_link_decisions + app.account_links; revert via "
        "app.audit_log (no undo tool yet). Discover pending decisions with "
        "accounts_links_pending.",
    )
    register(
        mcp,
        accounts_links_history,
        "accounts_links_history",
        "Recent account-link decisions (all statuses), newest first. "
        "Read-only. Filter by limit. Use accounts_links_pending for the "
        "active review queue.",
    )
