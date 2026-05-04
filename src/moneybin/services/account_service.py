# src/moneybin/services/account_service.py
"""Account and balance service.

Business logic for account listing and balance retrieval. Consumed by
both MCP tools and CLI commands.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from difflib import get_close_matches
from typing import Any, Literal, cast

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

# Sentinel for explicit field-clearing in settings_update.
# Distinct from None ("no change") and from a real value ("write this value").
CLEAR: object = object()


# Plaid's documented account subtype list (https://plaid.com/docs/api/accounts/).
# Open vocabulary in this project — soft-validated, never blocking.
PLAID_CANONICAL_SUBTYPES: frozenset[str] = frozenset({
    # depository
    "checking",
    "savings",
    "hsa",
    "cd",
    "money market",
    "paypal",
    "prepaid",
    "cash management",
    "ebt",
    # credit
    "credit card",
    "paypal credit",
    # loan
    "auto",
    "business",
    "commercial",
    "construction",
    "consumer",
    "home equity",
    "loan",
    "mortgage",
    "overdraft",
    "line of credit",
    "student",
    # investment
    "401a",
    "401k",
    "403b",
    "457b",
    "529",
    "brokerage",
    "cash isa",
    "education savings account",
    "fixed annuity",
    "gic",
    "health reimbursement arrangement",
    "ira",
    "isa",
    "keogh",
    "lif",
    "life insurance",
    "lira",
    "lrif",
    "lrsp",
    "mutual fund",
    "non-taxable brokerage account",
    "other",
    "other annuity",
    "other insurance",
    "pension",
    "plan",
    "prif",
    "profit sharing plan",
    "qshr",
    "rdsp",
    "resp",
    "retirement",
    "rlif",
    "roth",
    "roth 401k",
    "rrif",
    "rrsp",
    "sarsep",
    "sep ira",
    "simple ira",
    "sipp",
    "stock plan",
    "tfsa",
    "trust",
    "ugma",
    "utma",
    "variable annuity",
})

# "business" also appears in PLAID_CANONICAL_SUBTYPES (loan category) — overlap is intentional.
PLAID_CANONICAL_HOLDER_CATEGORIES: frozenset[str] = frozenset({
    "personal",
    "business",
    "joint",
})


def is_canonical_subtype(value: str) -> bool:
    """Whether the value matches Plaid's documented subtype list (case-insensitive)."""
    return value.lower() in PLAID_CANONICAL_SUBTYPES


def is_canonical_holder_category(value: str) -> bool:
    """Whether the value matches the canonical holder-category set."""
    return value.lower() in PLAID_CANONICAL_HOLDER_CATEGORIES


def suggest_subtype(value: str) -> str | None:
    """Suggest a canonical subtype near-match; None if no close match."""
    matches = get_close_matches(
        value.lower(), PLAID_CANONICAL_SUBTYPES, n=1, cutoff=0.75
    )
    return matches[0] if matches else None


def suggest_holder_category(value: str) -> str | None:
    """Suggest a canonical holder-category near-match; None if no close match."""
    matches = get_close_matches(
        value.lower(), PLAID_CANONICAL_HOLDER_CATEGORIES, n=1, cutoff=0.75
    )
    return matches[0] if matches else None


_LAST_FOUR_RE = re.compile(r"^[0-9]{4}$")
_ISO_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Sentinel used in summary() count_by_subtype to represent accounts with
# NULL account_subtype. MCP/CLI consumers see this string in the dict keys.
_UNSET_SUBTYPE_LABEL = "<unset>"


@dataclass(frozen=True, slots=True)
class AccountSettings:
    """Per-account settings record. Validated at construction.

    Validation lives here (not in SQL CHECK constraints) so historical rows
    written before tighter rules can still be read.
    """

    account_id: str
    display_name: str | None = None
    official_name: str | None = None
    last_four: str | None = None
    account_subtype: str | None = None
    holder_category: str | None = None
    iso_currency_code: str | None = None
    credit_limit: Decimal | None = None
    archived: bool = False
    include_in_net_worth: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict for JSON / envelope transport."""
        return {
            "account_id": self.account_id,
            "display_name": self.display_name,
            "official_name": self.official_name,
            "last_four": self.last_four,
            "account_subtype": self.account_subtype,
            "holder_category": self.holder_category,
            "iso_currency_code": self.iso_currency_code,
            "credit_limit": self.credit_limit,
            "archived": self.archived,
            "include_in_net_worth": self.include_in_net_worth,
        }

    def __post_init__(self) -> None:
        """Validate string lengths and formats at construction."""
        if not self.account_id:
            raise ValueError("account_id is required")
        if self.display_name is not None:
            if not 1 <= len(self.display_name) <= 80:
                raise ValueError("display_name must be 1-80 characters")
        if self.official_name is not None:
            if not 1 <= len(self.official_name) <= 200:
                raise ValueError("official_name must be 1-200 characters")
        if self.last_four is not None and not _LAST_FOUR_RE.match(self.last_four):
            raise ValueError("last_four must be exactly 4 digits")
        if self.account_subtype is not None:
            if not 1 <= len(self.account_subtype) <= 32:
                raise ValueError("account_subtype must be 1-32 characters")
        if self.holder_category is not None:
            if not 1 <= len(self.holder_category) <= 32:
                raise ValueError("holder_category must be 1-32 characters")
        if self.iso_currency_code is not None and not _ISO_CURRENCY_RE.match(
            self.iso_currency_code
        ):
            raise ValueError("iso_currency_code must be exactly 3 uppercase letters")
        if self.credit_limit is not None and self.credit_limit < Decimal("0"):
            raise ValueError("credit_limit must be non-negative")


class AccountSettingsRepository:
    """SQL-layer access to app.account_settings.

    All methods use parameterized queries — no string interpolation.
    Per .claude/rules/database.md, this is the only place that touches
    the app.account_settings table directly.
    """

    def __init__(self, db: Database) -> None:
        """Initialize with an open Database connection."""
        self._db = db

    def load(self, account_id: str) -> AccountSettings | None:
        """Load settings for an account; None if no row exists."""
        row = self._db.execute(
            f"""
            SELECT account_id, display_name, official_name, last_four,
                   account_subtype, holder_category, iso_currency_code,
                   credit_limit, archived, include_in_net_worth
            FROM {ACCOUNT_SETTINGS.full_name}
            WHERE account_id = ?
            """,
            [account_id],
        ).fetchone()
        if row is None:
            return None
        return AccountSettings(
            account_id=row[0],
            display_name=row[1],
            official_name=row[2],
            last_four=row[3],
            account_subtype=row[4],
            holder_category=row[5],
            iso_currency_code=row[6],
            credit_limit=row[7],
            archived=row[8],
            include_in_net_worth=row[9],
        )

    def upsert(self, settings: AccountSettings) -> None:
        """Insert or update by account_id; refreshes updated_at.

        Uses NOW() instead of CURRENT_TIMESTAMP — DuckDB treats CURRENT_TIMESTAMP
        as an identifier (not a function call) inside ON CONFLICT DO UPDATE clauses.
        """
        self._db.execute(
            f"""
            INSERT INTO {ACCOUNT_SETTINGS.full_name} (
                account_id, display_name, official_name, last_four,
                account_subtype, holder_category, iso_currency_code,
                credit_limit, archived, include_in_net_worth
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (account_id) DO UPDATE SET
                display_name         = excluded.display_name,
                official_name        = excluded.official_name,
                last_four            = excluded.last_four,
                account_subtype      = excluded.account_subtype,
                holder_category      = excluded.holder_category,
                iso_currency_code    = excluded.iso_currency_code,
                credit_limit         = excluded.credit_limit,
                archived             = excluded.archived,
                include_in_net_worth = excluded.include_in_net_worth,
                updated_at           = NOW()
            """,
            [
                settings.account_id,
                settings.display_name,
                settings.official_name,
                settings.last_four,
                settings.account_subtype,
                settings.holder_category,
                settings.iso_currency_code,
                settings.credit_limit,
                settings.archived,
                settings.include_in_net_worth,
            ],
        )

    def delete(self, account_id: str) -> None:
        """Delete the settings row for an account."""
        self._db.execute(
            f"DELETE FROM {ACCOUNT_SETTINGS.full_name} WHERE account_id = ?",
            [account_id],
        )


@dataclass(frozen=True, slots=True)
class AccountListResult:
    """Result of account listing query.

    ``accounts`` is a list of plain dicts matching the wire format the
    MCP envelope expects — one key per queried column. ``sensitivity``
    is ``"medium"`` by default; ``"low"`` when the caller requested
    ``redacted=True`` (last_four and credit_limit omitted).
    """

    accounts: list[dict[str, object]]
    sensitivity: Literal["low", "medium", "high"] = "medium"

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=list(self.accounts),
            sensitivity=self.sensitivity,
            actions=[
                "Use accounts_balance_list for current balances",
                "Use reports_spending_summary with account_id to filter by account",
            ],
        )


class AccountService:
    """Account and balance operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    """

    def __init__(self, db: Database) -> None:
        """Initialize AccountService with an open Database connection."""
        self._db = db
        self._settings_repo = AccountSettingsRepository(db)

    def _assert_account_exists(self, account_id: str) -> None:
        """Raise UserError if account_id is not in core.dim_accounts."""
        row = self._db.execute(
            f"SELECT 1 FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ? LIMIT 1",
            [account_id],
        ).fetchone()
        if row is None:
            raise UserError(f"Account not found: {account_id}", code="not_found")

    def list_accounts(
        self,
        *,
        include_archived: bool = False,
        type_filter: str | None = None,
        redacted: bool = False,
    ) -> AccountListResult:
        """List accounts. Hides archived by default. Redacted mode omits PII-adjacent fields."""
        where_clauses: list[str] = []
        params: list[object] = []
        if not include_archived:
            where_clauses.append("archived = FALSE")
        if type_filter is not None:
            where_clauses.append(
                "(UPPER(account_type) = UPPER(?) OR LOWER(account_subtype) = LOWER(?))"
            )
            params.extend([type_filter, type_filter])
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Field list is constructed from literal strings (not user input).
        fields = [
            "account_id",
            "display_name",
            "institution_name",
            "account_type",
            "account_subtype",
            "holder_category",
            "iso_currency_code",
            "archived",
            "include_in_net_worth",
        ]
        if not redacted:
            fields.extend(["last_four", "credit_limit"])

        field_list = ", ".join(fields)
        sql = f"""
            SELECT {field_list}
            FROM {DIM_ACCOUNTS.full_name}
            {where_sql}
            ORDER BY institution_name, account_type, account_id
        """  # noqa: S608  # field list is allowlisted above (literal strings)
        rows = self._db.execute(sql, params).fetchall()
        accounts: list[dict[str, object]] = [
            dict(zip(fields, row, strict=True)) for row in rows
        ]
        sensitivity: Literal["low", "medium"] = "low" if redacted else "medium"
        logger.info(f"Listed {len(accounts)} accounts")
        return AccountListResult(accounts=accounts, sensitivity=sensitivity)

    def get_account(self, account_id: str) -> dict[str, object] | None:
        """Single account record with full settings + dim record. None if not found.

        Always returns full fields including PII-adjacent values (last_four,
        credit_limit, routing_number). Sensitivity is always medium; there is no
        redacted variant. Callers requiring a redacted view should use
        list_accounts(redacted=True) instead and filter to the desired account.
        """
        fields = [
            "account_id",
            "display_name",
            "institution_name",
            "account_type",
            "account_subtype",
            "holder_category",
            "iso_currency_code",
            "last_four",
            "credit_limit",
            "archived",
            "include_in_net_worth",
            "source_type",
            "routing_number",
            "official_name",
        ]
        field_list = ", ".join(fields)
        row = self._db.execute(
            f"""
            SELECT {field_list}
            FROM {DIM_ACCOUNTS.full_name}
            WHERE account_id = ?
            """,  # noqa: S608  # field list is allowlisted above (literal strings)
            [account_id],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(fields, row, strict=True))

    def summary(self) -> dict[str, object]:
        """Aggregate snapshot for accounts_summary tool / accounts://summary resource."""
        row = self._db.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE archived) AS archived,
                COUNT(*) FILTER (WHERE NOT include_in_net_worth) AS excluded
            FROM {DIM_ACCOUNTS.full_name}
            """
        ).fetchone()
        # COUNT(*) always returns one row; guard is for type narrowing only.
        if row is None:  # pragma: no cover
            total, archived, excluded = 0, 0, 0
        else:
            total, archived, excluded = row[0], row[1], row[2]

        by_type: dict[str, int] = dict(
            self._db.execute(
                f"""
                SELECT account_type, COUNT(*)
                FROM {DIM_ACCOUNTS.full_name}
                WHERE NOT archived
                GROUP BY account_type
                """
            ).fetchall()
        )
        by_subtype: dict[str, int] = dict(
            self._db.execute(
                f"""
                SELECT COALESCE(account_subtype, ?), COUNT(*)
                FROM {DIM_ACCOUNTS.full_name}
                WHERE NOT archived
                GROUP BY 1
                """,
                [_UNSET_SUBTYPE_LABEL],
            ).fetchall()
        )
        recent_row = self._db.execute(
            f"""
            SELECT COUNT(DISTINCT account_id)
            FROM {FCT_TRANSACTIONS.full_name}
            WHERE transaction_date >= CURRENT_DATE - INTERVAL '30' DAY
            """
        ).fetchone()
        recent = recent_row[0] if recent_row is not None else 0  # pragma: no cover

        logger.info(f"Summary: {total} total accounts, {archived} archived")
        return {
            "total_accounts": total,
            "count_by_type": by_type,
            "count_by_subtype": by_subtype,
            "count_archived": archived,
            "count_excluded_from_net_worth": excluded,
            "count_with_recent_activity": recent,
        }

    def _load_or_default(self, account_id: str) -> AccountSettings:
        """Load existing settings or construct a default for the given account."""
        return self._settings_repo.load(account_id) or AccountSettings(
            account_id=account_id
        )

    def rename(self, account_id: str, display_name: str) -> AccountSettings:
        """Set or clear display_name. Empty string clears the override."""
        self._assert_account_exists(account_id)
        current = self._load_or_default(account_id)
        new_name: str | None = display_name if display_name else None
        updated = dataclasses.replace(current, display_name=new_name)
        self._settings_repo.upsert(updated)
        logger.info(
            f"Renamed account {account_id}: display_name "
            f"{'cleared' if new_name is None else 'set'}"
        )
        return updated

    def set_include_in_net_worth(
        self, account_id: str, include: bool
    ) -> AccountSettings:
        """Toggle include_in_net_worth flag. Idempotent."""
        self._assert_account_exists(account_id)
        current = self._load_or_default(account_id)
        updated = dataclasses.replace(current, include_in_net_worth=include)
        self._settings_repo.upsert(updated)
        logger.info(f"Updated account {account_id}: include_in_net_worth={include}")
        return updated

    def archive(self, account_id: str) -> AccountSettings:
        """Set archived=TRUE; cascades include_in_net_worth=FALSE in the same write."""
        self._assert_account_exists(account_id)
        current = self._load_or_default(account_id)
        updated = dataclasses.replace(
            current, archived=True, include_in_net_worth=False
        )
        self._settings_repo.upsert(updated)
        logger.info(
            f"Archived account {account_id} (cascaded include_in_net_worth=False)"
        )
        return updated

    def unarchive(self, account_id: str) -> AccountSettings:
        """Set archived=FALSE; does NOT restore include_in_net_worth (per spec)."""
        self._assert_account_exists(account_id)
        current = self._load_or_default(account_id)
        updated = dataclasses.replace(current, archived=False)
        self._settings_repo.upsert(updated)
        logger.info(f"Unarchived account {account_id}")
        return updated

    def settings_update(
        self,
        account_id: str,
        *,
        official_name: str | None | object = None,
        last_four: str | None | object = None,
        account_subtype: str | None | object = None,
        holder_category: str | None | object = None,
        iso_currency_code: str | None | object = None,
        credit_limit: Decimal | None | object = None,
    ) -> tuple[AccountSettings, list[dict[str, str]]]:
        """Partial update of structural metadata.

        None means "no change", CLEAR sentinel means "set to NULL", any other
        value writes that value. Returns the updated settings and a list of
        soft-validation warnings (empty if all values are canonical).
        """
        self._assert_account_exists(account_id)
        current = self._load_or_default(account_id)
        diff: dict[str, object] = {}
        warnings: list[dict[str, str]] = []

        def _resolve(field_name: str, new: object) -> None:
            if new is None:
                return
            if new is CLEAR:
                diff[field_name] = None
                return
            diff[field_name] = new

        _resolve("official_name", official_name)
        _resolve("last_four", last_four)
        _resolve("account_subtype", account_subtype)
        _resolve("holder_category", holder_category)
        _resolve("iso_currency_code", iso_currency_code)
        _resolve("credit_limit", credit_limit)

        subtype = diff.get("account_subtype")
        if isinstance(subtype, str) and not is_canonical_subtype(subtype):
            warnings.append({
                "field": "account_subtype",
                "message": f"'{subtype}' is not a known Plaid subtype",
                "suggestion": suggest_subtype(subtype) or "",
            })
        holder = diff.get("holder_category")
        if isinstance(holder, str) and not is_canonical_holder_category(holder):
            warnings.append({
                "field": "holder_category",
                "message": f"'{holder}' is not a known holder category",
                "suggestion": suggest_holder_category(holder) or "",
            })

        updated = dataclasses.replace(current, **cast(dict[str, Any], diff))
        self._settings_repo.upsert(updated)
        logger.info(
            f"Updated settings for account {account_id}: fields={sorted(diff.keys())}"
        )
        return updated, warnings
