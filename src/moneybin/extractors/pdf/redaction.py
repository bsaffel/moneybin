"""Shape-preserving redaction of parsed Critical fields before AI disclosure."""

import re
from collections.abc import Sequence

from moneybin.extractors.pdf.identity import is_aba_checksum_valid

_KEEP_LAST = 4
_SSN = re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)")
_ROUTING = re.compile(r"(?<!\d)\d{9}(?!\d)")


def _mask_digits(value: str) -> str:
    """Replace every digit except the last four with X, keeping separators."""
    total = sum(c.isdigit() for c in value)
    seen = 0
    out: list[str] = []
    for char in value:
        if char.isdigit():
            seen += 1
            out.append(char if seen > total - _KEEP_LAST else "X")
        else:
            out.append(char)
    return "".join(out)


def _identifier_pattern(identifier: str) -> re.Pattern[str] | None:
    digits = re.sub(r"\D", "", identifier)
    if len(digits) <= _KEEP_LAST:
        return None
    body = r"[\s./-]{0,2}".join(digits)
    return re.compile(rf"(?<!\d)0*{body}(?!\d)")


def redact_critical(text: str, *, account_identifiers: Sequence[str]) -> str:
    """Redact identified account numbers, strict SSNs and ABA routing numbers."""
    for identifier in account_identifiers:
        pattern = _identifier_pattern(identifier)
        if pattern:
            text = pattern.sub(lambda m: _mask_digits(m.group()), text)
    text = _SSN.sub(lambda m: _mask_digits(m.group()), text)
    return _ROUTING.sub(
        lambda m: (
            _mask_digits(m.group()) if is_aba_checksum_valid(m.group()) else m.group()
        ),
        text,
    )


def redact_tables(
    tables: list[list[list[str | None]]], *, account_identifiers: Sequence[str]
) -> list[list[list[str | None]]]:
    """Apply `redact_critical` to every non-empty cell."""
    return [
        [
            [
                None
                if cell is None
                else redact_critical(cell, account_identifiers=account_identifiers)
                for cell in row
            ]
            for row in table
        ]
        for table in tables
    ]
