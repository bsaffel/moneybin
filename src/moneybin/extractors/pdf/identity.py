"""Privacy-preserving account identity derivation for PDF statements.

Relocated by MB-52 slice 3 from ``services/pdf_account_identity.py`` — the
module held no ``Database``, repo, or config dependency, so it was an
``extractors/`` module filed under ``services/``. Named ``identity.py``
rather than mirroring the parent package's ``account_identity.py`` verbatim:
both modules concern account identity, but this one derives it from one PDF
document's bytes and captured evidence, while
``moneybin.extractors.account_identity`` holds the source-agnostic value
types every channel shares. A same-named pair one directory apart would read
as a copy-paste accident rather than a deliberate split.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

from moneybin.extractors.account_identity import (
    AccountNameFacts,
    SourceAccount,
    account_category,
    derived_last_four,
    mask_embedded_account_number,
    normalize_account_identifier,
)
from moneybin.extractors.pdf.metadata import (
    ACCOUNT_ID_MASK_CHARACTERS,
    to_account_number_mask,
)
from moneybin.extractors.pdf.routing import RouteDecision, pdf_account_type
from moneybin.utils import slugify

_DOCUMENT_KEY_HEX_LENGTH = 16
_DOCUMENT_SOURCE_ORIGIN = "document"
# ABA prefix 80 instruments are not personal deposit accounts, so stay review-only.
_ACCOUNT_ROUTING_PREFIX_RANGES = ((1, 12), (21, 32), (61, 72))


def _is_valid_aba_routing_number(value: str) -> bool:
    """Return whether a routing number has an account-safe prefix and checksum."""
    if len(value) != 9 or not value.isdigit():
        return False
    prefix = int(value[:2])
    if not any(
        lower <= prefix <= upper for lower, upper in _ACCOUNT_ROUTING_PREFIX_RANGES
    ):
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
    source_origin: str
    has_usable_identifier: bool
    last_four: str | None
    scoped_full_number: str | None
    legacy_source_account_key: str | None
    legacy_source_origin: str | None


def legacy_pdf_identifier_key(*, issuer: str, identifier: str | None) -> str | None:
    """Return the pre-document PDF key only for usable account evidence."""
    stripped = identifier.strip() if identifier is not None else ""
    if not stripped or not any(
        character.isalnum() and character not in ACCOUNT_ID_MASK_CHARACTERS
        for character in stripped
    ):
        return None
    digits = "".join(character for character in stripped if character.isdigit())
    legacy_value = f"****{digits[-4:]}" if len(digits) >= 4 else stripped
    legacy_slug = slugify(legacy_value)
    return f"{slugify(issuer)}_{legacy_slug}" if legacy_slug else None


def derive_pdf_account_identity(
    *,
    issuer: str,
    identifier: str | None,
    document_sha256: str,
    identifier_is_complete: bool,
    routing_number: str | None = None,
) -> PdfAccountKey:
    """Separate document identity from complete and partial account evidence."""
    issuer_slug = slugify(issuer)
    source_account_key = f"pdf_doc_{document_sha256.lower()[:_DOCUMENT_KEY_HEX_LENGTH]}"
    stripped = identifier.strip() if identifier is not None else ""
    if not stripped:
        return PdfAccountKey(
            source_account_key=source_account_key,
            source_origin=_DOCUMENT_SOURCE_ORIGIN,
            has_usable_identifier=False,
            last_four=None,
            scoped_full_number=None,
            legacy_source_account_key=None,
            legacy_source_origin=None,
        )

    has_usable_identifier = any(
        character.isalnum() and character not in ACCOUNT_ID_MASK_CHARACTERS
        for character in stripped
    )
    if not has_usable_identifier:
        return PdfAccountKey(
            source_account_key=source_account_key,
            source_origin=_DOCUMENT_SOURCE_ORIGIN,
            has_usable_identifier=False,
            last_four=None,
            scoped_full_number=None,
            legacy_source_account_key=None,
            legacy_source_origin=None,
        )

    digits = "".join(character for character in stripped if character.isdigit())
    last_four = digits[-4:] if len(digits) >= 4 else None
    legacy_source_account_key = legacy_pdf_identifier_key(
        issuer=issuer, identifier=stripped
    )

    normalized = normalize_account_identifier(stripped)
    is_partial = (
        any(character in ACCOUNT_ID_MASK_CHARACTERS for character in stripped)
        or len(normalized) <= 4
    )
    strong_scope = (
        routing_number
        if routing_number and _is_valid_aba_routing_number(routing_number)
        else None
    )
    scoped_full_number = (
        f"{strong_scope}:{normalized}"
        if identifier_is_complete and not is_partial and strong_scope
        else None
    )

    return PdfAccountKey(
        source_account_key=source_account_key,
        source_origin=_DOCUMENT_SOURCE_ORIGIN,
        has_usable_identifier=True,
        last_four=last_four,
        scoped_full_number=scoped_full_number,
        legacy_source_account_key=legacy_source_account_key,
        legacy_source_origin=issuer_slug,
    )


class PdfAccountIdentity(NamedTuple):
    """What a PDF statement says about its account, and whether it said anything.

    ``identity_unknown`` is returned beside the account rather than derived again
    at each call site: the gate and the resolve pass have to agree about whether
    the file stated an identity, and re-testing the anchor separately is exactly
    the drift ``derive_pdf_source_account``'s own contract rules out.
    """

    source: SourceAccount
    identity_unknown: bool

    @property
    def fallback_keys(self) -> tuple[str, ...]:
        """The gate's ``fallback_keys`` argument for this identity."""
        return (self.source.source_account_key,) if self.identity_unknown else ()


def derive_pdf_source_account(
    decision: RouteDecision,
    *,
    resolved_alias: str,
    account_id_override: str | None,
    document_sha256: str,
    source_file: str | None = None,
) -> PdfAccountIdentity:
    """Derive the account identity a PDF statement presents, without resolving.

    Shared by the confirm gate (which runs before ``begin_import``) and the
    resolve pass in ``_import_pdf_transactions``, so the identity the user
    ratifies is exactly the one bound.

    Every PDF gets a document-content ``source_native`` key. A complete captured
    identifier separately becomes a validated-routing-scoped ``full_number``
    strong ref inside the encrypted database; a masked, last-four-only, or
    issuer-only value remains weak evidence. This prevents two
    same-issuer/same-last-four accounts from sharing a native key while
    preserving exact-file re-import idempotency.

    A statement with no readable account number has no account identity of its
    own. Its document key still makes the file idempotent, while
    ``identity_unknown`` sends it through the gate's fallback pick-list.

    Pure — this is only the identity a document *presents*. The pin's
    key-borrowing (which reads ``AccountResolver`` state) is a second, DB-touching
    step the caller applies afterward; see
    ``ImportService._pdf_source_account`` in ``services/import_service.py``.
    """
    if decision.fp is None:
        # Defensive: route_pdf_import attaches fp on every outcome that reaches
        # the transactions path; this guards a hand-built RouteDecision.
        raise ValueError("PDF routing returned outcome='transactions' but fp is None")
    issuer = decision.fp.get("issuer", "unknown")
    derived = derive_pdf_account_identity(
        issuer=issuer,
        identifier=decision.metadata.account_id,
        document_sha256=document_sha256,
        identifier_is_complete=decision.metadata.account_id_complete,
        routing_number=decision.metadata.routing_number,
    )
    # Whether the document named an account, independent of its idempotency key.
    anchored = derived.has_usable_identifier
    derived_key = derived.source_account_key
    source = SourceAccount(
        source_type="pdf",
        source_origin=derived.source_origin,
        source_account_key=derived_key,
        account_name=(
            decision.metadata.account_label
            or decision.metadata.product_name
            or resolved_alias
        ),
        # account_label is captured from a printed "Account Name:"/"Account
        # Nickname:" line -- a label the account holder set, the PDF analogue
        # of Plaid's acc.name and a tabular --account-name. product_name is
        # the card/product's marketing name (identical across every holder of
        # that product) and resolved_alias is the filename slug; neither is
        # authored, so the flag must follow account_label specifically, not
        # merely "account_name is non-empty".
        account_name_is_user_set=decision.metadata.account_label is not None,
        account_number=derived.scoped_full_number,
        institution=issuer or None,
        # Before document keys, an anchorless PDF used its filename alias.
        # Preserve that accepted binding as review-only migration evidence.
        legacy_source_account_key=(
            derived.legacy_source_account_key
            or (resolved_alias if not anchored else None)
        ),
        legacy_source_origin=(
            derived.legacy_source_origin or (slugify(issuer) if not anchored else None)
        ),
        legacy_source_account_key_is_filename_alias=(
            derived.legacy_source_account_key is None and not anchored
        ),
        source_file=source_file,
        # None for a digits-free token ("xxxx"), which correctly denies the
        # institution+last4 signal and routes to name review rather than
        # inventing a strong match.
        last_four=derived.last_four,
        # What core.dim_accounts will name this account, built from the three
        # values _import_pdf_transactions writes to raw.tabular_accounts for it:
        # the issuer, the recipe-implied account type, and the last-4 display
        # mask. Not `derived.last_four`, which answers a different question (it
        # is None for a digits-free token so the institution+last4 match cannot
        # fire); the model reads the masked column and strips it to digits.
        name_facts=AccountNameFacts(
            institution_name=issuer or None,
            category=account_category(pdf_account_type(decision)),
            last_four=derived_last_four(
                to_account_number_mask(decision.metadata.account_id)
            ),
            # Same value and same condition as account_name_is_user_set below
            # -- a captured "Account Name:"/"Account Nickname:" line is the
            # only PDF-side source that counts as authored. Masked the way
            # every other display-safe label site is (mask_embedded_account_
            # number), never the raw captured text.
            source_label=(
                mask_embedded_account_number(decision.metadata.account_label)
                if decision.metadata.account_label
                else None
            ),
        ),
        explicit_account_id=account_id_override,
        # Set even when no key is borrowed by the resolver-touching wrapper;
        # _teach_unpinned_key ignores it once it equals source_account_key.
        unpinned_account_key=derived_key if account_id_override else None,
    )
    return PdfAccountIdentity(source=source, identity_unknown=not anchored)
