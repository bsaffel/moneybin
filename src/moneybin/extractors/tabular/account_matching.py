"""Account matching across source types.

Matches imported accounts against the full account registry using:
1. Account number (strongest signal)
2. Exact slug match on account name
3. Fuzzy name matching (difflib.SequenceMatcher)
4. Explicit --account-id bypass
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from difflib import SequenceMatcher


@dataclass
class AccountMatch:
    """Result of account matching."""

    matched: bool
    """Whether a match was found."""

    account_id: str | None = None
    """Matched or generated account ID."""

    candidates: list[dict[str, str]] = field(default_factory=list)
    """Fuzzy match candidates for "did you mean?" prompt."""


def _slugify(name: str) -> str:
    """Generate deterministic slug from account name.

    Args:
        name: Human-readable account name.

    Returns:
        Lowercase, hyphen-separated slug.
    """
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def match_account(
    account_name: str,
    *,
    account_number: str | None = None,
    explicit_account_id: str | None = None,
    existing_accounts: Sequence[Mapping[str, str | None]] | None = None,
) -> AccountMatch:
    """Match an account against the existing account registry.

    Args:
        account_name: Account name to match.
        account_number: Account number for strongest match.
        explicit_account_id: Explicit ID (bypasses matching).
        existing_accounts: List of existing account dicts with
            account_id, account_name, and optionally account_number.

    Returns:
        AccountMatch with match result and candidates.
    """
    if explicit_account_id:
        return AccountMatch(matched=True, account_id=explicit_account_id)

    existing = existing_accounts or []

    # 1. Account number match (strongest)
    if account_number:
        for acct in existing:
            if acct.get("account_number") == account_number:
                acct_id = acct.get("account_id") or ""
                return AccountMatch(matched=True, account_id=acct_id)

    # 2. Exact slug match
    target_slug = _slugify(account_name)
    for acct in existing:
        if acct.get("account_id") == target_slug:
            return AccountMatch(matched=True, account_id=target_slug)
        acct_name = acct.get("account_name") or ""
        if _slugify(acct_name) == target_slug:
            acct_id = acct.get("account_id") or ""
            return AccountMatch(matched=True, account_id=acct_id)

    # 3. Fuzzy matching
    candidates: list[tuple[float, dict[str, str]]] = []
    for acct in existing:
        name = acct.get("account_name") or ""
        ratio = SequenceMatcher(None, account_name.lower(), name.lower()).ratio()
        if ratio >= 0.6:
            candidates.append((
                ratio,
                {
                    "account_id": acct.get("account_id") or "",
                    "account_name": name,
                },
            ))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return AccountMatch(
            matched=False,
            candidates=[c[1] for c in candidates[:5]],
        )

    # No match — caller should create new account
    return AccountMatch(matched=False, account_id=None)
