# src/moneybin/mcp/tools/accounts.py
"""Accounts namespace tools — v2 per docs/specs/account-management.md + net-worth.md.

Read tools (entity):
  - accounts_list (medium / low with redacted=True)
  - accounts_get (medium)
  - accounts_summary (low)

Write tools (entity, all medium):
  - accounts_rename
  - accounts_include
  - accounts_archive
  - accounts_unarchive
  - accounts_settings_update

Read tools (balance, contributed by net-worth.md):
  - accounts_balance_list (medium)
  - accounts_balance_history (medium)
  - accounts_balance_reconcile (medium)
  - accounts_balance_assertions_list (medium)

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
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_service import AccountService, AccountSettings
from moneybin.services.balance_service import BalanceService

# ─── Read tools (entity) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium")
def accounts_list(
    include_archived: bool = False,
    type_filter: str | None = None,
    redacted: bool = False,
) -> ResponseEnvelope:
    """List accounts in MoneyBin.

    Args:
        include_archived: Include archived accounts (default: hide them)
        type_filter: Match account_type or account_subtype (case-insensitive)
        redacted: Omit last_four and credit_limit; downgrades sensitivity to low

    Returns the resolved view from core.dim_accounts including display_name,
    institution_name, account_type, account_subtype, holder_category,
    iso_currency_code, archived, include_in_net_worth, plus last_four and
    credit_limit unless redacted.
    """
    result = AccountService(get_database()).list_accounts(
        include_archived=include_archived,
        type_filter=type_filter,
        redacted=redacted,
    )
    return result.to_envelope()


@mcp_tool(sensitivity="medium")
def accounts_get(account_id: str) -> ResponseEnvelope:
    """Single account record with full settings + dim record.

    Always returns full fields including PII-adjacent values (last_four,
    credit_limit, routing_number). Sensitivity is always medium.

    Args:
        account_id: The account ID to look up

    Returns None in the data field if the account is not found.
    """
    record = AccountService(get_database()).get_account(account_id)
    return build_envelope(data=record or {}, sensitivity="medium")


@mcp_tool(sensitivity="low")
def accounts_summary() -> ResponseEnvelope:
    """Aggregate account snapshot: counts only, no per-account data, no PII.

    Useful as context for AI conversations about finances. Returns total counts,
    counts by type and subtype, count archived, count excluded from net worth,
    and count with recent activity (last 30 days).
    """
    summary = AccountService(get_database()).summary()
    return build_envelope(data=summary, sensitivity="low")


# ─── Write tools (entity) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium")
def accounts_rename(account_id: str, display_name: str) -> ResponseEnvelope:
    """Rename an account by setting app.account_settings.display_name.

    Args:
        account_id: The account ID
        display_name: New display name; empty string clears the override

    Returns the updated settings record.
    """
    settings = AccountService(get_database()).rename(account_id, display_name)
    return build_envelope(data=_settings_to_dict(settings), sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_include(account_id: str, include: bool = True) -> ResponseEnvelope:
    """Toggle account inclusion in net worth.

    Args:
        account_id: The account ID
        include: True to include, False to exclude

    Returns the updated settings record.
    """
    settings = AccountService(get_database()).set_include_in_net_worth(
        account_id, include
    )
    return build_envelope(data=_settings_to_dict(settings), sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_archive(account_id: str) -> ResponseEnvelope:
    """Archive an account. Cascades include_in_net_worth=False in the same write.

    Args:
        account_id: The account ID

    Returns the updated settings record. The data field includes
    cascaded_include_in_net_worth: false to surface the cascade.
    """
    settings = AccountService(get_database()).archive(account_id)
    data = _settings_to_dict(settings)
    data["cascaded_include_in_net_worth"] = False
    return build_envelope(data=data, sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_unarchive(account_id: str) -> ResponseEnvelope:
    """Unarchive an account. Does NOT restore include_in_net_worth.

    Args:
        account_id: The account ID

    Returns the updated settings record.
    """
    settings = AccountService(get_database()).unarchive(account_id)
    return build_envelope(data=_settings_to_dict(settings), sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_settings_update(
    account_id: str,
    official_name: str | None = None,
    last_four: str | None = None,
    account_subtype: str | None = None,
    holder_category: str | None = None,
    iso_currency_code: str | None = None,
    credit_limit: float | None = None,
) -> ResponseEnvelope:
    """Partial update of structural metadata fields.

    Pass None for any field to leave it unchanged. All fields are upserted
    into app.account_settings. Soft-validation warnings (for non-canonical
    account_subtype or holder_category values) are embedded in data.warnings.

    Args:
        account_id: The account ID
        official_name: Institution's formal name
        last_four: Last 4 digits (must match ^[0-9]{4}$)
        account_subtype: Plaid-style subtype (open vocabulary)
        holder_category: 'personal' / 'business' / 'joint' (open vocabulary)
        iso_currency_code: ISO-4217 (e.g., USD)
        credit_limit: Decimal value as float (converted to Decimal internally)
    """
    diff: dict[str, object] = {}
    if official_name is not None:
        diff["official_name"] = official_name
    if last_four is not None:
        diff["last_four"] = last_four
    if account_subtype is not None:
        diff["account_subtype"] = account_subtype
    if holder_category is not None:
        diff["holder_category"] = holder_category
    if iso_currency_code is not None:
        diff["iso_currency_code"] = iso_currency_code
    if credit_limit is not None:
        diff["credit_limit"] = Decimal(str(credit_limit))

    settings, warnings = AccountService(get_database()).settings_update(
        account_id,
        **diff,  # type: ignore[arg-type]  # dynamic partial-update kwargs
    )
    data = _settings_to_dict(settings)
    if warnings:
        data["warnings"] = warnings
    return build_envelope(data=data, sensitivity="medium")


# ─── Read tools (balance) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium")
def accounts_balance_list(
    account_ids: list[str] | None = None,
    as_of_date: str | None = None,
) -> ResponseEnvelope:
    """Most recent balance per account; optionally as-of an ISO date.

    Args:
        account_ids: Filter to specific accounts
        as_of_date: ISO date (YYYY-MM-DD) — shows balance on or before this date
    """
    parsed_date = _date.fromisoformat(as_of_date) if as_of_date else None
    observations = BalanceService(get_database()).current_balances(
        account_ids=account_ids, as_of_date=parsed_date
    )
    return build_envelope(
        data=[o.to_dict() for o in observations], sensitivity="medium"
    )


@mcp_tool(sensitivity="medium")
def accounts_balance_history(
    account_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> ResponseEnvelope:
    """Per-account balance history (daily series with carry-forward + reconciliation deltas).

    Args:
        account_id: Required — the account to query
        from_date: ISO date (YYYY-MM-DD); inclusive
        to_date: ISO date (YYYY-MM-DD); inclusive
    """
    parsed_from = _date.fromisoformat(from_date) if from_date else None
    parsed_to = _date.fromisoformat(to_date) if to_date else None
    observations = BalanceService(get_database()).history(
        account_id, from_date=parsed_from, to_date=parsed_to
    )
    return build_envelope(
        data=[o.to_dict() for o in observations], sensitivity="medium"
    )


@mcp_tool(sensitivity="medium")
def accounts_balance_reconcile(
    account_ids: list[str] | None = None,
    threshold: float = 0.01,
) -> ResponseEnvelope:
    """Show balance days with non-zero reconciliation delta above threshold.

    Args:
        account_ids: Filter to specific accounts
        threshold: Minimum absolute delta to include (default: 0.01 = 1 cent)
    """
    parsed_threshold = Decimal(str(threshold))
    observations = BalanceService(get_database()).reconcile(
        account_ids=account_ids, threshold=parsed_threshold
    )
    return build_envelope(
        data=[o.to_dict() for o in observations], sensitivity="medium"
    )


@mcp_tool(sensitivity="medium")
def accounts_balance_assertions_list(
    account_id: str | None = None,
) -> ResponseEnvelope:
    """List user-entered balance assertions.

    Args:
        account_id: Optional filter to a single account
    """
    assertions = BalanceService(get_database()).list_assertions(account_id)
    return build_envelope(data=[a.to_dict() for a in assertions], sensitivity="medium")


# ─── Write tools (balance) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium")
def accounts_balance_assert(
    account_id: str,
    assertion_date: str,
    balance: float,
    notes: str | None = None,
) -> ResponseEnvelope:
    """Insert or update a manual balance assertion.

    Args:
        account_id: The account ID
        assertion_date: ISO date (YYYY-MM-DD)
        balance: Balance amount as decimal value
        notes: Optional free-text notes
    """
    parsed_date = _date.fromisoformat(assertion_date)
    parsed_balance = Decimal(str(balance))
    result = BalanceService(get_database()).assert_balance(
        account_id=account_id,
        assertion_date=parsed_date,
        balance=parsed_balance,
        notes=notes,
    )
    return build_envelope(data=result.to_dict(), sensitivity="medium")


@mcp_tool(sensitivity="medium")
def accounts_balance_assertion_delete(
    account_id: str,
    assertion_date: str,
) -> ResponseEnvelope:
    """Delete a manual balance assertion. Silent no-op if no row exists.

    Args:
        account_id: The account ID
        assertion_date: ISO date (YYYY-MM-DD)
    """
    parsed_date = _date.fromisoformat(assertion_date)
    BalanceService(get_database()).delete_assertion(account_id, parsed_date)
    return build_envelope(
        data={"account_id": account_id, "assertion_date": parsed_date.isoformat()},
        sensitivity="medium",
    )


# ─── Helpers ──────────────────────────────────────────────────────────────


def _settings_to_dict(settings: AccountSettings) -> dict[str, object]:
    """Serialize an AccountSettings dataclass to a plain dict for envelope data."""
    return {
        "account_id": settings.account_id,
        "display_name": settings.display_name,
        "official_name": settings.official_name,
        "last_four": settings.last_four,
        "account_subtype": settings.account_subtype,
        "holder_category": settings.holder_category,
        "iso_currency_code": settings.iso_currency_code,
        "credit_limit": settings.credit_limit,
        "archived": settings.archived,
        "include_in_net_worth": settings.include_in_net_worth,
    }


# ─── Registration ──────────────────────────────────────────────────────────


def register_accounts_tools(mcp: FastMCP) -> None:
    """Register all v2 accounts namespace tools with the FastMCP server."""
    register(
        mcp,
        accounts_list,
        "accounts_list",
        "List accounts (default hides archived; supports type filter and redacted mode).",
    )
    register(
        mcp,
        accounts_get,
        "accounts_get",
        "Get one account's full settings + dim record. Returns empty dict if not found.",
    )
    register(
        mcp,
        accounts_summary,
        "accounts_summary",
        "Aggregate account snapshot — counts by type/subtype, archived, excluded, recent activity.",
    )
    register(
        mcp,
        accounts_rename,
        "accounts_rename",
        "Rename an account (writes app.account_settings.display_name; empty clears).",
    )
    register(
        mcp,
        accounts_include,
        "accounts_include",
        "Toggle include_in_net_worth on an account.",
    )
    register(
        mcp,
        accounts_archive,
        "accounts_archive",
        "Archive an account; cascades include_in_net_worth=False in the same write.",
    )
    register(
        mcp,
        accounts_unarchive,
        "accounts_unarchive",
        "Unarchive an account. Does NOT auto-restore include_in_net_worth.",
    )
    register(
        mcp,
        accounts_settings_update,
        "accounts_settings_update",
        "Partial update of Plaid-parity metadata (subtype, holder_category, currency, credit_limit, etc.).",
    )
    register(
        mcp,
        accounts_balance_list,
        "accounts_balance_list",
        "Latest balance per account from fct_balances_daily (or as-of a date).",
    )
    register(
        mcp,
        accounts_balance_history,
        "accounts_balance_history",
        "Per-account balance history (daily series with carry-forward + reconciliation deltas).",
    )
    register(
        mcp,
        accounts_balance_reconcile,
        "accounts_balance_reconcile",
        "Days with non-zero reconciliation delta above threshold.",
    )
    register(
        mcp,
        accounts_balance_assertions_list,
        "accounts_balance_assertions_list",
        "List user-entered balance assertions.",
    )
    register(
        mcp,
        accounts_balance_assert,
        "accounts_balance_assert",
        "Upsert a manual balance assertion.",
    )
    register(
        mcp,
        accounts_balance_assertion_delete,
        "accounts_balance_assertion_delete",
        "Delete a manual balance assertion.",
    )
