# src/moneybin/services/account_service.py
"""Account and balance service.

Business logic for account listing and balance retrieval. Consumed by
both MCP tools and CLI commands.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from difflib import get_close_matches
from typing import Any

from moneybin.database import Database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import ACCOUNT_SETTINGS, DIM_ACCOUNTS, OFX_BALANCES

logger = logging.getLogger(__name__)

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
class Account:
    """Single account record."""

    account_id: str
    account_type: str
    institution_name: str
    source_type: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "account_id": self.account_id,
            "account_type": self.account_type,
            "institution_name": self.institution_name,
            "source_type": self.source_type,
        }


@dataclass(slots=True)
class AccountListResult:
    """Result of account listing query."""

    accounts: list[Account]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[a.to_dict() for a in self.accounts],
            sensitivity="low",
            actions=[
                "Use accounts_balances for current balances",
                "Use spending_summary with account_id to filter by account",
            ],
        )


@dataclass(frozen=True, slots=True)
class AccountBalance:
    """Balance snapshot for a single account."""

    account_id: str
    institution_name: str | None
    account_type: str | None
    ledger_balance: Decimal
    available_balance: Decimal | None
    as_of_date: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        d: dict[str, Any] = {
            "account_id": self.account_id,
            "institution_name": self.institution_name,
            "account_type": self.account_type,
            "ledger_balance": self.ledger_balance,
            "available_balance": self.available_balance,
            "as_of_date": self.as_of_date,
        }
        return d


@dataclass(slots=True)
class BalanceListResult:
    """Result of balance query."""

    balances: list[AccountBalance]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[b.to_dict() for b in self.balances],
            sensitivity="medium",
            actions=[
                "Use spending_summary for spending trends",
                "Use transactions_search with account_id for recent activity",
            ],
        )


class AccountService:
    """Account and balance operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    """

    def __init__(self, db: Database) -> None:
        """Initialize AccountService with an open Database connection."""
        self._db = db

    def list_accounts(self) -> AccountListResult:
        """List all accounts.

        Returns:
            AccountListResult with all accounts ordered by institution
            and type.
        """
        sql = f"""
            SELECT
                account_id,
                account_type,
                institution_name,
                source_type
            FROM {DIM_ACCOUNTS.full_name}
            ORDER BY institution_name, account_type
        """

        result = self._db.execute(sql)
        rows = result.fetchall()

        accounts = [
            Account(
                account_id=str(row[0]),
                account_type=str(row[1]),
                institution_name=str(row[2]),
                source_type=str(row[3]),
            )
            for row in rows
        ]

        logger.info(f"Listed {len(accounts)} accounts")
        return AccountListResult(accounts=accounts)

    def balances(self, account_id: str | None = None) -> BalanceListResult:
        """Get latest balance for each account.

        Uses ROW_NUMBER() to pick the most recent balance snapshot per
        account, then LEFT JOINs dim_accounts for institution info.

        Args:
            account_id: Filter to a specific account.

        Returns:
            BalanceListResult with latest balances.
        """
        conditions: list[str] = ["b.rn = 1"]
        params: list[object] = []

        if account_id:
            conditions.append("b.account_id = ?")
            params.append(account_id)

        where = "WHERE " + " AND ".join(conditions)

        sql = f"""
            WITH latest AS (
                SELECT
                    account_id,
                    ledger_balance,
                    available_balance,
                    ledger_balance_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY account_id
                        ORDER BY ledger_balance_date DESC
                    ) AS rn
                FROM {OFX_BALANCES.full_name}
            )
            SELECT
                b.account_id,
                a.institution_name,
                a.account_type,
                b.ledger_balance,
                b.available_balance,
                b.ledger_balance_date
            FROM latest b
            LEFT JOIN {DIM_ACCOUNTS.full_name} a
                ON b.account_id = a.account_id
            {where}
            ORDER BY a.institution_name, a.account_type
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        balances = [
            AccountBalance(
                account_id=str(row[0]),
                institution_name=str(row[1]) if row[1] else None,
                account_type=str(row[2]) if row[2] else None,
                ledger_balance=Decimal(str(row[3])),
                available_balance=Decimal(str(row[4])) if row[4] is not None else None,
                as_of_date=str(row[5]),
            )
            for row in rows
        ]

        logger.info(f"Retrieved balances for {len(balances)} accounts")
        return BalanceListResult(balances=balances)
