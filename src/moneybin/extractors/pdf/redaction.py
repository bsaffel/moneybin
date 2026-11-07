"""Shape-preserving redaction of parsed Critical fields before AI disclosure."""

import re
from collections.abc import Sequence

from moneybin.extractors.pdf.identity import is_aba_checksum_valid

_KEEP_LAST = 4
_SSN = re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)")
_ROUTING = re.compile(r"(?<!\d)\d{9}(?!\d)")
_LEADING_MASK = re.compile(r"^[Xx*\u2022\u25cf]+")


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


def _mask_identifier(value: str) -> str:
    """Mask every letter, and every digit except the last four, keeping separators."""
    return "".join("X" if c.isalpha() else c for c in _mask_digits(value))


def _pattern(chars: str) -> re.Pattern[str]:
    body = r"[\s./-]{0,3}".join(re.escape(c) for c in chars)
    return re.compile(rf"(?<!\d)0*{body}(?!\d)", re.IGNORECASE)


def _identifier_patterns(identifier: str) -> list[re.Pattern[str]]:
    """Digits-only always; letters only add a pattern, never replace it.

    Label words (``Acct``, ``CHK``) and a leading mask (``XXXX``, ``****``)
    are captured context, not the number, so they never decide the match.
    """
    patterns: list[re.Pattern[str]] = []
    digits = re.sub(r"\D", "", identifier)
    if len(digits) > _KEEP_LAST:
        patterns.append(_pattern(digits))
    words = identifier.split()
    while words and not any(c.isdigit() for c in words[0]):
        words.pop(0)
    while words and not any(c.isdigit() for c in words[-1]):
        words.pop()
    core = _LEADING_MASK.sub("", " ".join(words))
    chars = re.sub(r"[^0-9A-Za-z]", "", core)
    if len(chars) > _KEEP_LAST and any(c.isalpha() for c in chars):
        patterns.insert(0, _pattern(chars))
    return patterns


def redact_critical(text: str, *, account_identifiers: Sequence[str]) -> str:
    """Redact identified account numbers, strict SSNs and ABA routing numbers."""
    for identifier in account_identifiers:
        for pattern in _identifier_patterns(identifier):
            text = pattern.sub(lambda m: _mask_identifier(m.group()), text)
    text = _SSN.sub(lambda m: _mask_digits(m.group()), text)
    return _ROUTING.sub(
        lambda m: (
            _mask_digits(m.group()) if is_aba_checksum_valid(m.group()) else m.group()
        ),
        text,
    )
