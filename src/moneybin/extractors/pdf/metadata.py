"""Statement-level metadata capture for PDF import (Req 7a).

Pure function: ``capture_metadata(document_text, anchors) -> StatementMetadata``.
No logging — silent None on no-match is the contract.  Regex errors and parse
failures both produce None for the affected field; they never propagate.

Anchor shape
------------
``dict[str, list[str]]`` — field name → ordered list of single-capture-group
patterns.  The first pattern whose first capture group matches wins; None if no
pattern matches.  All five fields (account_id, period_start, period_end,
opening_balance, closing_balance) are single-group anchors.

period_start / period_end use independent patterns rather than a shared
"Statement Period: <start> - <end>" two-group anchor.  This keeps the anchor
dict shape uniform (each value is list[str] of single-capture patterns) at the
cost of the two passes needing to find their own independent anchors.  The
Task 7 auto-derive pass freezes whatever matches into the saved recipe's
metadata_anchors, so replay is deterministic regardless of the split.

Security
--------
Uses ``regex`` (not stdlib ``re``) for the ``timeout=`` parameter — same
posture as recipe.py (Req 9b).  Timeout constant defined once here;
modules are independent.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import regex as _re

# Same timeout shape as recipe._PATTERN_TIMEOUT_SEC; defined independently
# so this module has no import-time dependency on recipe.py.
_TIMEOUT_SEC = 0.1

# Default anchors handle common US bank statement formats.
# period_start / period_end are independent single-capture patterns rather than
# a two-group "Statement Period: start - end" anchor.  Rationale: uniform
# dict[str, list[str]] shape throughout; two-group anchors would require a
# special-case code path for one field.
#: Public alias used by auto_derive to freeze metadata anchors into a Recipe.
DEFAULT_ANCHORS: dict[str, list[str]] = {
    "account_id": [
        r"Account\s+Number[:\s]+(\S+)",
        r"Account\s+ending\s+in\s+(\d+)",
    ],
    "period_start": [
        r"Statement\s+Period:\s+(\d{2}/\d{2}/\d{4})",
        r"From:\s+(\d{2}/\d{2}/\d{4})",
        # Credit-card "Opening/Closing Date  MM/DD/YY - MM/DD/YY" (2-digit year).
        # This line is also the year source for MM/DD transaction rows that
        # print no year of their own — see auto_derive / execute_recipe.
        r"Opening/Closing\s+Date\s+(\d{2}/\d{2}/\d{2})\b",
    ],
    "period_end": [
        r"(?:through|to|–|-)\s+(\d{2}/\d{2}/\d{4})\s*$",
        r"To:\s+(\d{2}/\d{2}/\d{4})",
        r"Opening/Closing\s+Date\s+\d{2}/\d{2}/\d{2}\s*-\s*(\d{2}/\d{2}/\d{2})\b",
    ],
    "opening_balance": [
        r"Beginning\s+Balance[:\s]+\$?([\d,]+\.\d{2})",
        # Chase credit-card summary label.
        r"Previous\s+Balance[:\s]+\$?([\d,]+\.\d{2})",
    ],
    "closing_balance": [
        r"Ending\s+Balance[:\s]+\$?([\d,]+\.\d{2})",
        r"New\s+Balance[:\s]+\$?([\d,]+\.\d{2})",
    ],
}


@dataclass(frozen=True)
class StatementMetadata:
    """Statement-level fields captured from raw PDF text."""

    account_id: str | None
    period_start: date | None
    period_end: date | None
    opening_balance: Decimal | None
    closing_balance: Decimal | None

    def is_complete_for_reconciliation(self) -> bool:
        """Return True when both balances are present (Req 7a reconciliation gate)."""
        return self.opening_balance is not None and self.closing_balance is not None


def capture_metadata(
    document_text: str,
    anchors: dict[str, list[str]] | None = None,
) -> StatementMetadata:
    """Scan *document_text* for statement-level fields using labelled-anchor regexes.

    Args:
        document_text: Full extracted PDF text.
        anchors: Override the default anchor dict.  Each key is a field name;
            each value is an ordered list of single-capture-group patterns.
            The first match wins; None when no pattern matches.

    Returns:
        ``StatementMetadata`` with None for any field that could not be captured.
    """
    resolved = anchors if anchors is not None else DEFAULT_ANCHORS
    raw: dict[str, str | None] = {}
    for field_name, patterns in resolved.items():
        raw[field_name] = _first_match(document_text, patterns)

    return StatementMetadata(
        account_id=raw.get("account_id"),
        period_start=_parse_date(raw.get("period_start")),
        period_end=_parse_date(raw.get("period_end")),
        opening_balance=_parse_decimal(raw.get("opening_balance")),
        closing_balance=_parse_decimal(raw.get("closing_balance")),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_match(text: str, patterns: list[str]) -> str | None:
    """Return the first capture group from the first matching pattern, or None."""
    for pattern in patterns:
        try:
            m = _re.search(pattern, text, _re.MULTILINE, timeout=_TIMEOUT_SEC)
        except (TimeoutError, _re.error):
            continue
        if m is not None:
            try:
                return m.group(1)
            except IndexError:
                continue
    return None


def _parse_date(raw: str | None) -> date | None:
    """Parse a MM/DD/YYYY or MM/DD/YY statement date; None on failure/None input.

    The 2-digit-year form is what credit-card "Opening/Closing Date" lines print
    (``12/23/24``); strptime's ``%y`` maps it to 2000-2068 per POSIX, which is
    correct for every statement this parser will ever see.
    """
    if raw is None:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(raw: str | None) -> Decimal | None:
    """Strip $ and , then parse as Decimal; return None on failure or None input."""
    if raw is None:
        return None
    cleaned = raw.replace("$", "").replace(",", "")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None
