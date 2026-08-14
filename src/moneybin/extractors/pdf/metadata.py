"""Statement-level metadata capture for PDF import (Req 7a).

Pure function: ``capture_metadata(document_text, anchors) -> StatementMetadata``.
No logging — silent None on no-match is the contract.  Regex errors and parse
failures both produce None for the affected field; they never propagate.

Anchor shape
------------
``dict[str, list[str]]`` — field name → ordered list of single-capture-group
patterns.  The first pattern whose first capture group matches wins; None if no
pattern matches. All fields are single-group anchors.

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
_COMPLETE_ACCOUNT_ID_PATTERN = (
    r"Account\s+Number[:\s]+([\dXx*]{3,}(?:[ -][\dXx*]{3,})*)(?![-\w*])"
)
ACCOUNT_ID_MASK_CHARACTERS = frozenset("*Xx•●")

DEFAULT_ANCHORS: dict[str, list[str]] = {
    "account_id": [
        # Card numbers print as space- or hyphen-separated groups
        # ("XXXX XXXX XXXX 1234"). Capture every group so the TRAILING one
        # survives: on a masked layout those are the only account-discriminating
        # digits the statement discloses. Stopping at the first whitespace token
        # returns the bare mask ("XXXX" → a digit-free account key) for a masked
        # layout, and the *leading* four — an authoritative-looking wrong last4
        # that then feeds the institution+last4 merge signal — for an unmasked
        # one. Each group needs 3+ chars so a trailing date ("XXXX1234 01/31/24")
        # can't extend the match. The trailing guard makes this anchor
        # all-or-nothing: without it a token like "123-ABC-456" matches only
        # its leading "123", and because _first_match is first-match-wins that
        # partial hit permanently retires the (\S+) anchor below.
        _COMPLETE_ACCOUNT_ID_PATTERN,
        r"Account\s+ending\s+in\s+(\d+)",
        # Institution-specific tokens that aren't digit/mask runs (e.g. "ACCT-9Z").
        r"Account\s+Number[:\s]+(\S+)",
    ],
    "account_label": [
        r"^Account\s+(?:Name|Nickname):[ \t]*([^\r\n]+?)[ \t]*$",
    ],
    "account_type": [
        r"^Account\s+Type:[ \t]*([^\r\n]+?)[ \t]*$",
    ],
    "product_name": [
        r"^Product(?:\s+Name)?:[ \t]*([^\r\n]+?)[ \t]*$",
    ],
    "routing_number": [
        r"^Routing\s+Number:[ \t]*(\d{9})[ \t]*$",
    ],
    "currency_code": [
        r"^Currency:[ \t]*([A-Za-z]{3})[ \t]*$",
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
    account_label: str | None = None
    account_type: str | None = None
    product_name: str | None = None
    routing_number: str | None = None
    currency_code: str | None = None
    account_id_complete: bool = False

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
    account_id_pattern: str | None = None
    for field_name, patterns in resolved.items():
        raw[field_name], matched_pattern = _first_match_with_pattern(
            document_text, patterns
        )
        if field_name == "account_id":
            account_id_pattern = matched_pattern

    account_id = raw.get("account_id")

    return StatementMetadata(
        account_id=account_id,
        period_start=_parse_date(raw.get("period_start")),
        period_end=_parse_date(raw.get("period_end")),
        opening_balance=_parse_decimal(raw.get("opening_balance")),
        closing_balance=_parse_decimal(raw.get("closing_balance")),
        account_label=raw.get("account_label"),
        account_type=raw.get("account_type"),
        product_name=raw.get("product_name"),
        routing_number=raw.get("routing_number"),
        currency_code=_normalize_currency_code(raw.get("currency_code")),
        account_id_complete=(
            account_id_pattern == _COMPLETE_ACCOUNT_ID_PATTERN
            and account_id is not None
            and not any(char in ACCOUNT_ID_MASK_CHARACTERS for char in account_id)
        ),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_match_with_pattern(
    text: str, patterns: list[str]
) -> tuple[str | None, str | None]:
    """Return the first capture and the pattern that produced it."""
    for pattern in patterns:
        try:
            m = _re.search(pattern, text, _re.MULTILINE, timeout=_TIMEOUT_SEC)
        except (TimeoutError, _re.error):
            continue
        if m is not None:
            try:
                return m.group(1), pattern
            except IndexError:
                continue
    return None, None


def _parse_date(raw: str | None) -> date | None:
    """Parse a MM/DD/YYYY, MM/DD/YY, or ISO YYYY-MM-DD statement date.

    None on failure or None input. The 2-digit-year form is what credit-card
    "Opening/Closing Date" lines print (``12/23/24``); strptime's ``%y`` maps it to
    2000-2068 per POSIX, which is correct for every statement this parser will ever
    see. ISO is accepted so a bridge-authored period anchor that captures an
    ISO-shaped date still resolves the year for a year-less statement (the year-less
    executor brackets each MM/DD row against this period).
    """
    if raw is None:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
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


def _normalize_currency_code(raw: str | None) -> str | None:
    """Normalize a captured three-letter currency code."""
    return raw.upper() if raw is not None else None
