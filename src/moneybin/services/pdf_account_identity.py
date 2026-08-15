"""Privacy-preserving account identity derivation for PDF statements."""

from __future__ import annotations

import string
from dataclasses import dataclass

from moneybin.extractors.pdf.metadata import ACCOUNT_ID_MASK_CHARACTERS
from moneybin.utils import slugify

_DOCUMENT_KEY_HEX_LENGTH = 16
_ASCII_ALPHANUMERIC = frozenset(string.ascii_letters + string.digits)


def _is_valid_aba_routing_number(value: str) -> bool:
    """Return whether a nine-digit routing number passes the ABA checksum."""
    if len(value) != 9 or not value.isdigit():
        return False
    digits = [int(character) for character in value]
    checksum = (
        3 * (digits[0] + digits[3] + digits[6])
        + 7 * (digits[1] + digits[4] + digits[7])
        + digits[2]
        + digits[5]
        + digits[8]
    )
    return checksum % 10 == 0


@dataclass(frozen=True)
class PdfAccountKey:
    """Account keys derived from one PDF without exposing its full identifier."""

    source_account_key: str
    last_four: str | None
    scoped_full_number: str | None
    legacy_source_account_key: str | None


def derive_pdf_account_identity(
    *,
    issuer: str,
    identifier: str | None,
    document_sha256: str,
    identifier_is_complete: bool,
    routing_number: str | None = None,
) -> PdfAccountKey:
    """Separate document identity from complete and partial account evidence."""
    issuer_slug = slugify(issuer) or "unknown"
    source_account_key = f"pdf_doc_{document_sha256.lower()[:_DOCUMENT_KEY_HEX_LENGTH]}"
    stripped = identifier.strip() if identifier is not None else ""
    if not stripped:
        return PdfAccountKey(
            source_account_key=source_account_key,
            last_four=None,
            scoped_full_number=None,
            legacy_source_account_key=None,
        )

    digits = "".join(character for character in stripped if character.isdigit())
    last_four = digits[-4:] if len(digits) >= 4 else None
    legacy_value = f"****{last_four}" if last_four is not None else stripped
    legacy_slug = slugify(legacy_value)
    legacy_source_account_key = f"{issuer_slug}_{legacy_slug}" if legacy_slug else None

    normalized = "".join(
        character.upper() for character in stripped if character in _ASCII_ALPHANUMERIC
    )
    is_partial = (
        any(character in ACCOUNT_ID_MASK_CHARACTERS for character in stripped)
        or len(normalized) <= 4
    )
    strong_scope: str | None = None
    if issuer_slug != "unknown":
        strong_scope = issuer_slug
    elif routing_number and _is_valid_aba_routing_number(routing_number):
        strong_scope = routing_number
    scoped_full_number = (
        f"{strong_scope}:{normalized}"
        if identifier_is_complete and not is_partial and strong_scope
        else None
    )

    return PdfAccountKey(
        source_account_key=source_account_key,
        last_four=last_four,
        scoped_full_number=scoped_full_number,
        legacy_source_account_key=legacy_source_account_key,
    )
