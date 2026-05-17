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
  - accounts_set

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
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_service import CLEAR, AccountService
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
        redacted: Omit last_four and credit_limit; downgrades response envelope
            sensitivity to ``low``. NOTE: the tool-level ``@mcp_tool`` decorator
            tier is a conservative upper bound (``medium``) for consent gates;
            the actual per-call sensitivity is reported via
            ``ResponseEnvelope.summary.sensitivity`` and varies with ``redacted``.

    Returns the resolved view from core.dim_accounts including display_name,
    institution_name, account_type, account_subtype, holder_category,
    iso_currency_code, archived, include_in_net_worth, plus last_four and
    credit_limit unless redacted.
    """
    with get_database(read_only=True) as db:
        result = AccountService(db).list_accounts(
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

    Returns {"found": true, ...fields} if found, {"found": false, "account_id": ...} if not.
    """
    with get_database(read_only=True) as db:
        record = AccountService(db).get_account(account_id)
    if record is None:
        return build_envelope(
            data={"found": False, "account_id": account_id},
            sensitivity="medium",
        )
    return build_envelope(data={"found": True, **record}, sensitivity="medium")


@mcp_tool(sensitivity="low")
def accounts_summary() -> ResponseEnvelope:
    """Aggregate account snapshot: counts only, no per-account data, no PII.

    Useful as context for AI conversations about finances. Returns total counts,
    counts by type and subtype, count archived, count excluded from net worth,
    and count with recent activity (last 30 days).
    """
    with get_database(read_only=True) as db:
        summary = AccountService(db).summary()
    return build_envelope(data=summary, sensitivity="low")


# ─── Write tools (entity) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium", read_only=False)
def accounts_rename(account_id: str, display_name: str) -> ResponseEnvelope:
    """Rename an account by setting app.account_settings.display_name.

    Args:
        account_id: The account ID
        display_name: New display name; empty string clears the override

    Returns the updated settings record.
    """
    with get_database() as db:
        settings = AccountService(db).rename(account_id, display_name)
    return build_envelope(data=settings.to_dict(), sensitivity="medium")


@mcp_tool(sensitivity="medium", read_only=False)
def accounts_include(account_id: str, include: bool = True) -> ResponseEnvelope:
    """Toggle account inclusion in net worth.

    Args:
        account_id: The account ID
        include: True to include, False to exclude

    Returns the updated settings record.
    """
    with get_database() as db:
        settings = AccountService(db).set_include_in_net_worth(account_id, include)
    return build_envelope(data=settings.to_dict(), sensitivity="medium")


@mcp_tool(sensitivity="medium", read_only=False)
def accounts_archive(account_id: str) -> ResponseEnvelope:
    """Archive an account. Cascades include_in_net_worth=False in the same write.

    Args:
        account_id: The account ID

    Returns the updated settings record. The data field includes
    cascaded_include_in_net_worth: false to surface the cascade.
    """
    with get_database() as db:
        settings = AccountService(db).archive(account_id)
    data = settings.to_dict()
    data["cascaded_include_in_net_worth"] = False
    return build_envelope(data=data, sensitivity="medium")


@mcp_tool(sensitivity="medium", read_only=False)
def accounts_unarchive(account_id: str) -> ResponseEnvelope:
    """Unarchive an account. Does NOT restore include_in_net_worth.

    Args:
        account_id: The account ID

    Returns the updated settings record.
    """
    with get_database() as db:
        settings = AccountService(db).unarchive(account_id)
    return build_envelope(data=settings.to_dict(), sensitivity="medium")


_CLEARABLE_FIELDS: frozenset[str] = frozenset({
    "official_name",
    "last_four",
    "account_subtype",
    "holder_category",
    "iso_currency_code",
    "credit_limit",
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
    clear_fields: list[str] | None = None,
) -> ResponseEnvelope:
    """Partial update of structural metadata fields.

    Pass None for any field to leave it unchanged. To explicitly clear a field,
    include its name in the `clear_fields` list. Valid clearable field names:
    "official_name", "last_four", "account_subtype", "holder_category",
    "iso_currency_code", "credit_limit".

    Soft-validation warnings (for non-canonical account_subtype or holder_category
    values) are embedded in the response data['warnings'] field.
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
    data = settings.to_dict()
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
    with get_database(read_only=True) as db:
        observations = BalanceService(db).current_balances(
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
    with get_database(read_only=True) as db:
        observations = BalanceService(db).history(
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
    with get_database(read_only=True) as db:
        observations = BalanceService(db).reconcile(
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
    with get_database(read_only=True) as db:
        assertions = BalanceService(db).list_assertions(account_id)
    return build_envelope(data=[a.to_dict() for a in assertions], sensitivity="medium")


# ─── Write tools (balance) ──────────────────────────────────────────────────


@mcp_tool(sensitivity="medium", read_only=False)
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
    with get_database() as db:
        result = BalanceService(db).assert_balance(
            account_id=account_id,
            assertion_date=parsed_date,
            balance=parsed_balance,
            notes=notes,
        )
    return build_envelope(data=result.to_dict(), sensitivity="medium")


@mcp_tool(sensitivity="medium", read_only=False, destructive=True)
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
    with get_database() as db:
        BalanceService(db).delete_assertion(account_id, parsed_date)
    return build_envelope(
        data={"account_id": account_id, "assertion_date": parsed_date.isoformat()},
        sensitivity="medium",
    )


# ─── Resolution (free-text → account_id) ───────────────────────────────────


@mcp_tool(sensitivity="low")
def accounts_resolve(query: str, limit: int = 5) -> ResponseEnvelope:
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
        matches = AccountService(db).resolve(query=query, limit=limit)
    threshold = get_settings().data.tabular.account_match_threshold
    actions: list[str] = []
    if not matches:
        actions.append(
            "No accounts matched the query. Try a broader query or use accounts_list."
        )
    elif matches[0].confidence < threshold:
        actions.append(
            "Top match has low confidence; verify with the user before taking action."
        )
    return build_envelope(
        data=[m.to_dict() for m in matches],
        sensitivity="low",
        actions=actions,
    )


# ─── Registration ──────────────────────────────────────────────────────────


def register_accounts_tools(mcp: FastMCP) -> None:
    """Register all v2 accounts namespace tools with the FastMCP server."""
    register(
        mcp,
        accounts_list,
        "accounts_list",
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
        accounts_rename,
        "accounts_rename",
        "Rename an account (writes app.account_settings.display_name; empty clears). "
        "Writes app.account_settings; revert by calling accounts_rename again with the prior value (or empty string to clear).",
    )
    register(
        mcp,
        accounts_include,
        "accounts_include",
        "Toggle include_in_net_worth on an account. "
        "Writes app.account_settings.include_in_net_worth; revert by calling with the inverse `include` value.",
    )
    register(
        mcp,
        accounts_archive,
        "accounts_archive",
        "Archive an account; cascades include_in_net_worth=False in the same write. "
        "Writes app.account_settings (archived, include_in_net_worth); revert with accounts_unarchive (does NOT auto-restore include_in_net_worth).",
    )
    register(
        mcp,
        accounts_unarchive,
        "accounts_unarchive",
        "Unarchive an account. Does NOT auto-restore include_in_net_worth. "
        "Writes app.account_settings.archived=False; revert with accounts_archive.",
    )
    register(
        mcp,
        accounts_set,
        "accounts_set",
        "Partial update of Plaid-parity metadata (subtype, holder_category, currency, credit_limit, etc.). "
        "Writes app.account_settings; revert by calling again with the prior values (no built-in undo). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balance_list,
        "accounts_balance_list",
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
        "Days with non-zero reconciliation delta above threshold. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        accounts_balance_assertions_list,
        "accounts_balance_assertions_list",
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
