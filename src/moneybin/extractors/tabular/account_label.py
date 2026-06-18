"""Parse a ``(name, last_four)`` pair from an account-label string.

Aggregator exports embed the last 4 of the account number in the account
*display name* (Monarch ``Daily Expense (...1789)``, ``Checking ····1789``,
``Savings x1789``, ``Card ending in 1789``). Per Decision 8 of
``account-identity-resolution.md`` this last4 is a **Tier-B suggestion** — it
corroborates a candidate and makes it recognizable, never an auto-merge key.

Distinct from ``import_service._to_account_number_mask``, which takes the
trailing 4 of *any* digit run: this only yields a last4 when a recognized
last-4 *pattern* matches, so a name like ``365 Savings`` (a stray 3-digit
token, no last4) yields ``None`` rather than a false ``365``. A bare trailing
4-digit group (``WF CHECKING 9940``) is accepted as a last4 — a 4-digit year
can false-positive, but a wrong last4 only produces a review *suggestion* the
user rejects, never a silent merge, so the lenient match is acceptable.
"""

from __future__ import annotations

import re

# Ordered most-specific first; each captures exactly the 4 last-4 digits.
_LAST4_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"\(\s*[.…*x#·\-\s]*?(\d{4})\s*\)", re.IGNORECASE
    ),  # (...1789) (xxxx1789) (·1789)
    re.compile(
        r"(?:ending(?:\s+in)?|ends?\s+in)\s+(\d{4})\b", re.IGNORECASE
    ),  # ending in 1789
    re.compile(
        r"[*x#·….\-]{2,}\s*(\d{4})\b", re.IGNORECASE
    ),  # ····1789 / xxxx1789 / ...1789
    re.compile(r"\bx(\d{4})\b", re.IGNORECASE),  # x1789
    re.compile(
        r"(?<!\d)(\d{4})\s*$"
    ),  # bare trailing 4-digit group (lenient; review-only)
)

# Strip a recognized trailing last4 token (and its mask/paren ornamentation)
# from the display name.
_TRAILING_TOKEN = re.compile(
    r"\s*[\(\[]?\s*(?:ending(?:\s+in)?\s+)?[.…*x#·\-\s]*\d{4}\s*[\)\]]?\s*$",
    re.IGNORECASE,
)


def parse_account_label(label: str | None) -> tuple[str, str | None]:
    """Return ``(clean_name, last_four|None)`` parsed from an account label.

    ``last_four`` is the 4 digits when a recognized last-4 pattern matches,
    else ``None``. ``clean_name`` strips a matched trailing last4 token and
    collapses whitespace; with no match it is the original label trimmed.
    """
    if not label:
        return ("", None)
    text = label.strip()
    last4: str | None = None
    for pattern in _LAST4_PATTERNS:
        match = pattern.search(text)
        if match:
            last4 = match.group(1)
            break
    if last4 is None:
        return (text, None)
    name = _TRAILING_TOKEN.sub("", text).strip()
    name = re.sub(r"\s{2,}", " ", name)
    return (name or text, last4)
