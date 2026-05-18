# src/moneybin/mcp/tools/accounts.py
"""Accounts namespace tools — v2 per docs/specs/account-management.md + net-worth.md.

Read tools (entity):
  - accounts (medium / low with redacted=True)
  - accounts_get (medium)
  - accounts_summary (low)

Write tools (entity, all medium):
  - accounts_set

Read tools (balance, contributed by net-worth.md):
  - accounts_balances (medium)
  - accounts_balance_history (medium)
  - accounts_balance_reconcile (medium)
  - accounts_balance_assertions (medium)

Write tools (balance, all medium):
  - accounts_balance_assert
  - accounts_balance_assertion_delete

All tools delegate to AccountService / BalanceService — no business logic here.
"""

from __future__ import annotations

from datetime import date as _date
from decimal import Decimal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.accounts import (
    AccountDetail,
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
from moneybin.services.account_service import CLEAR, AccountService
from moneybin.services.balance_service import BalanceService

# ─── Read tools (entity) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium")
def accounts(
    include_archived: bool = False,
    type_filter: str | None = None,
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
            include_archived=include_archived,
            type_filter=type_filter,
        )
    return build_envelope(
        data=result,
        sensitivity="medium",
        actions=[
            "Use accounts_balances for current balances",
            "Use reports_spending with a category filter to drill in by account",
        ],
    )


@mcp_tool(sensitivity="medium")
def accounts_get(account_id: str) -> ResponseEnvelope[AccountDetail]:
    """Single account record with full settings + dim record.

    Returns full fields including CRITICAL-tier values (last_four,
    credit_limit, routing_number). CRITICAL fields are masked by the
    middleware; sensitivity is always medium at the tool level.

    Args:
        account_id: The account ID to look up

    Returns the account record if found, or raises not_found if not.
    """
    with get_database(read_only=True) as db:
        record = AccountService(db).get_account(account_id)
    if record is None:
        raise UserError(f"Account not found: {account_id}", code="not_found")
    return build_envelope(data=record, sensitivity="medium")


@mcp_tool(sensitivity="low")
def accounts_summary() -> ResponseEnvelope[AccountSummaryStats]:
    """Aggregate account snapshot: counts only, no per-account data, no PII.

    Useful as context for AI conversations about finances. Returns total counts,
    counts by type and subtype, count archived, count excluded from net worth,
    and count with recent activity (last 30 days).
    """
    with get_database(read_only=True) as db:
        stats = AccountService(db).summary()
    return build_envelope(data=stats, sensitivity="low")


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


@mcp_tool(sensitivity="medium", read_only=False)
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
                f"Unknown clearable fields: {sorted(unknown)}",
                code="invalid_field",
            )
        for field in clear_fields:
            kwargs[field] = CLEAR
    with get_database() as db:
        settings, warnings = AccountService(db).settings_update(
            account_id,
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
    return build_envelope(data=payload, sensitivity="medium")


# ─── Read tools (balance) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium")
def accounts_balances(
    account_ids: list[str] | None = None,
    as_of_date: str | None = None,
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
    return build_envelope(data=result, sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_balance_history(
    account_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
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
    return build_envelope(data=result, sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_balance_reconcile(
    account_ids: list[str] | None = None,
    threshold: float = 0.01,
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
    return build_envelope(data=result, sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_balance_assertions(
    account_id: str | None = None,
) -> ResponseEnvelope[BalanceAssertionListPayload]:
    """List user-entered balance assertions.

    Args:
        account_id: Optional filter to a single account
    """
    with get_database(read_only=True) as db:
        result = BalanceService(db).list_assertions(account_id)
    return build_envelope(data=result, sensitivity="medium")


# ─── Write tools (balance) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium", read_only=False)
def accounts_balance_assert(
    account_id: str,
    assertion_date: str,
    balance: float,
    notes: str | None = None,
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
    with get_database() as db:
        result = BalanceService(db).assert_balance(
            account_id=account_id,
            assertion_date=parsed_date,
            balance=parsed_balance,
            notes=notes,
        )
    return build_envelope(data=result, sensitivity="medium")


@mcp_tool(sensitivity="medium", read_only=False, destructive=True)
def accounts_balance_assertion_delete(
    account_id: str,
    assertion_date: str,
) -> ResponseEnvelope[BalanceAssertionDeletePayload]:
    """Delete a manual balance assertion. Silent no-op if no row exists.

    Args:
        account_id: The account ID
        assertion_date: ISO date (YYYY-MM-DD)
    """
    parsed_date = _date.fromisoformat(assertion_date)
    with get_database() as db:
        BalanceService(db).delete_assertion(account_id, parsed_date)
    return build_envelope(
        data=BalanceAssertionDeletePayload(
            account_id=account_id,
            assertion_date=parsed_date,
            deleted=True,
        ),
        sensitivity="medium",
    )


# ─── Resolution (free-text → account_id) ───────────────────────────────────


@mcp_tool(sensitivity="low")
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
    or a top-match confidence below ``TabularConfig.account_match_threshold``
    (the shared fuzzy-match cutoff used by the tabular importer) emits an
    action hint suggesting the agent verify with the user.
    """
    from moneybin.config import get_settings

    with get_database(read_only=True) as db:
        payload = AccountService(db).resolve(query=query, limit=limit)
    threshold = get_settings().data.tabular.account_match_threshold
    actions: list[str] = []
    if not payload.matches:
        actions.append(
            "No accounts matched the query. Try a broader query or call the `accounts` tool."
        )
    elif payload.matches[0].confidence < threshold:
        actions.append(
            "Top match has low confidence; verify with the user before taking action."
        )
    return build_envelope(
        data=payload,
        sensitivity="low",
        actions=actions,
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
        "Get one account's full settings + dim record. Returns {found: false} if not found. "
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
        "Show daily balance rows whose precomputed reconciliation_delta "
        "exceeds the threshold (per-account, point-in-time). Reads "
        "fct_balances_daily. For a per-assertion-date asserted-vs-computed "
        "series with categorical drift status (drift / warning / clean / "
        "no-data), use reports_balance_drift instead. "
        "Amounts are in the currency named by `summary.display_currency`.",
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
