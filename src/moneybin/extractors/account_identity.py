"""Account-identity value types shared by extractors and services.

Consolidates two things that travel together: the raw identity a source
presents for one account (``SourceAccount``) and how ``core.dim_accounts``
will name it (``AccountNameFacts`` and its derivation, mirroring
``dim_accounts.sql``'s ``COALESCE`` chain -- see ``derive_display_name``).
Both are pure value types and pure functions -- no DB, no config -- so they
live in ``extractors/``, a layer both the extractor channels (OFX, tabular,
PDF) and ``services/`` (``AccountResolver`` et al.) can import.

Relocated by MB-246 from ``services/account_display_name.py`` and a slice of
``services/account_resolution_types.py``, closing the upward layering
inversion those two modules left in ``extractors/ofx/extractor.py`` (MB-52
slice 1, PR #585): an extractor may not import from ``services/``, so the
value objects both layers need had to move to one either can reach. The
resolver-verdict types that also lived in ``account_resolution_types.py``
(``AccountCandidate``, ``AccountProposal``, ``ResolvedAccount``, the
pending-link types, plus ``is_a_name`` / ``matchable_account_name`` /
``is_reserved_account_name``) stay there -- they depend on
``services.ledger_overlap`` (a DB-touching module) or exist only to serve
``AccountResolver``'s own service-layer contract, and no extractor needs them.
"""

from __future__ import annotations

import csv
import io
import re
import string
from dataclasses import dataclass, replace
from functools import lru_cache
from importlib import resources

_ACCOUNT_IDENTIFIER_CHARACTERS = frozenset(string.ascii_letters + string.digits)


def normalize_account_identifier(value: str) -> str:
    """Canonical cross-source form for a complete account identifier."""
    return "".join(
        character.upper()
        for character in value
        if character in _ACCOUNT_IDENTIFIER_CHARACTERS
    )


UNNAMED_ACCOUNT_LABEL = "Unnamed account"
"""What every surface calls an account nothing can name.

Duplicated as a literal in the terminal COALESCE arm of
``core.dim_accounts.display_name``, because SQL cannot import it. The two are
pinned together by ``test_dim_accounts_merge.py``, which asserts the model's
output against this constant after a real SQLMesh run -- so a drift in either
copy fails there rather than in front of a user.

One constant rather than a per-call-site literal because both spellings render
in the same table: ``core`` supplies this string for a row it could not name,
while the CLI and MCP substitute it for a name that is absent or was frozen as
``""``. Those are different states with one honest answer, and rendering them
as ``Unnamed account`` beside ``unnamed account`` reads as a bug.

Lives in ``extractors/`` rather than beside either consumer: the naming ladder
below (``derive_display_name``) and the free-text resolver's matching
(``services.account_resolution_types.is_a_name`` et al.) both have to agree on
it, and neither consumer may import the other's module for it.
"""

#: The shared account-type registry, relative to the installed ``moneybin``
#: package. Same CSV that backs ``seeds.account_type_map``, which
#: ``prep.stg_ofx__accounts`` and ``prep.stg_tabular__accounts`` join.
_TYPE_MAP_RESOURCE = "sqlmesh/models/seeds/account_type_map.csv"

#: The separator the model puts before a last four. Not "****": the dim column
#: is the display label, and the mask belongs to the raw account-number columns.
_LAST_FOUR_PREFIX = "…"

_NON_DIGITS = re.compile(r"[^0-9]")


def _has_letter(text: str) -> bool:
    r"""Whether the label holds a letter in any script.

    Mirrors the model's ``REGEXP_MATCHES(account_label, '\p{L}')``. Both sides
    were ``[A-Za-z]``, which agreed with each other and was wrong together: a
    label written in any non-Latin script -- ``储蓄账户``, ``Сбережения`` --
    held no "letter", so the rung dropped a name a person actually chose and
    named the account by an assembled label instead.

    ``str.isalpha`` is the exact Python spelling of ``\p{L}``: both are the
    Unicode letter categories and nothing else, so ``²`` and ``Ⅳ`` fail on
    both sides. A ``[^\W\d_]`` regex would have accepted those two and
    reopened the drift this mirror exists to prevent.
    """
    return any(character.isalpha() for character in text)


#: A label already carrying a four-digit group does not also take a last four.
#: Four digits is the last-four unit, so such a label is either stating the
#: account's own already or is what the masker left of a longer number --
#: ``Checking ****5678`` joined with ``…9012`` publishes eight digits of a
#: twelve-digit number, well past what the last-four convention allows. A year
#: inside a name is indistinguishable from a number's tail, so neither one is
#: joined. Python's ``\d`` is the Unicode decimal category, so a run written in
#: another script counts as one: the masker builds its token out of whatever
#: ``\d`` matched, and ``[0-9]{4}`` read the resulting ``****٦٧٨٩`` as carrying
#: no digits at all and joined a second four onto it. Mirrors the model's
#: ``NOT REGEXP_MATCHES(account_label, '\p{Nd}{4}')``, which cannot be spelled
#: ``\d{4}`` there: DuckDB's RE2 reads that as ASCII ``[0-9]``, so identical
#: source would have left the two sides disagreeing.
_HAS_FOUR_DIGIT_RUN = re.compile(r"\d{4}")


@lru_cache(maxsize=1)
def _alias_to_category() -> dict[str, str]:
    """Source account-type spelling → the word the model names an account by.

    ``dim_accounts`` reads ``COALESCE(s.account_subtype, w.account_subtype,
    w.account_type)``, and staging fills the subtype from the registry when the
    registry has a finer distinction than the canonical type. Collapsing both
    into one lookup keeps that precedence in one place; a row whose subtype is
    blank (``DEPOSITORY``) resolves to its canonical type, exactly as the
    model's COALESCE does.
    """
    raw = resources.files("moneybin").joinpath(_TYPE_MAP_RESOURCE).read_text()
    return {
        alias: category
        for row in csv.DictReader(io.StringIO(raw))
        if (alias := (row["alias"] or "").strip())
        and (category := (row["account_subtype"] or "").strip() or row["account_type"])
    }


def _stated(value: str | None) -> str | None:
    """``NULLIF(TRIM(value), '')`` — whitespace is silence, not a value."""
    return (value or "").strip() or None


def account_category(source_account_type: str | None) -> str | None:
    """The subtype-or-type ``core.dim_accounts`` names an account by.

    A registered spelling resolves through the shared type map. An unregistered
    one keeps its own word, lowercased — the staging models' ``ELSE
    LOWER(NULLIF(TRIM(...), ''))`` branch. Guessing a canonical type for it
    would invent a classification the sources never stated.
    """
    stated = _stated(source_account_type)
    if stated is None:
        return None
    return _alias_to_category().get(stated.upper(), stated.lower())


def derived_last_four(value: str | None) -> str | None:
    """Last four *digits* of an account number or mask, or None if fewer survive.

    Mirrors the ``REGEXP_REPLACE(..., '[^0-9]', '', 'g')`` guard every source arm
    of ``dim_accounts`` applies. The four-digit floor is what keeps an
    alphanumeric PDF identifier ("ACCT-9Z") from being reported as a last four
    that reads like a bank's.
    """
    digits = _NON_DIGITS.sub("", value or "")
    return digits[-4:] if len(digits) >= 4 else None


def usable_source_label(label: str | None) -> str | None:
    """The account label if it names the account, else None.

    A label holding only digits is the account number under another column
    heading — ordinary in a hand-rolled export — and masking makes it safe to
    show without making it a name: ``****1098`` identifies the account strictly
    worse than ``Test Bank …1098`` does. Requiring one letter is what keeps the
    top rung for labels a person wrote.

    ``UNNAMED_ACCOUNT_LABEL`` holds letters and is still not a name: it is this
    ladder's own terminal arm, the one string that says nothing could name the
    account. It reaches a source label by an ordinary route, because
    ``reports.*`` publish it as ``account_name`` and a MoneyBin export can be
    re-imported. Promoting it would hand ``is_a_name`` a label it must discard,
    leaving the account unresolvable by what it displays — strictly worse than
    the institution-derived name the fallthrough gives it.

    The letter test is the whole gate, and its reach is worth stating rather
    than assuming. The channel's masker fires at five digits counted across the
    run, so a label carrying four or fewer — ``ACCT-XY9Z``, ``AB1234C`` —
    arrives here whole and becomes the account's name everywhere it is shown.
    Accepted on the same terms as Plaid's unmasked ``name``
    (``stg_plaid__accounts.sql``): four digits is what every masked surface
    already prints as ``****1234``, and refusing identifier-shaped labels takes
    ``CD-2024`` and the rest of the real names this rung exists to surface with
    it. ``.claude/rules/identifiers.md`` carries the accepted list and that
    bound; a third surface is a fresh decision, not an inference from this one.
    """
    stated = _stated(label)
    if stated is None or not _has_letter(stated):
        return None
    if stated == UNNAMED_ACCOUNT_LABEL:
        return None
    return stated


def derive_display_name(
    *,
    source_label: str | None = None,
    institution_name: str | None,
    category: str | None,
    last_four: str | None,
) -> str:
    """The label ``core.dim_accounts.display_name`` will carry for this account.

    One arm per arm of the model's ``COALESCE``, in its order. SQL ``||`` yields
    NULL when any operand is NULL, which is what makes that chain a precedence
    ladder rather than a set of independent fragments: an arm fires only when
    every fact it names is present.

    ``source_label`` outranks every assembled name because it is the only one a
    person wrote, and because ``moneybin accounts`` already prints the
    institution and the type in their own columns beside it — spending the name
    on "Test Bank depository" restates what is on screen and discards what is
    not. It must arrive display-ready: the account column is free text and does
    carry whole account numbers, so the channel masks it upstream, where the
    one masking rule lives.

    A label with no digits of its own still takes the last four, like every
    other rung that can. The label is the one fact a person chose, but it is not
    by itself unique: Plaid sends the institution's own account name, and a
    household's two checking accounts routinely carry the same product name from
    their bank. Naming both of them that collides two distinct accounts onto one string —
    the very defect this module replaces — and
    ``AccountService.resolve_strict`` then refuses a name reference that
    resolved before. A label that already carries four digits keeps them and
    takes nothing more; see ``_HAS_FOUR_DIGIT_RUN``.
    """
    label = usable_source_label(source_label)
    institution = _stated(institution_name)
    kind = _stated(category)
    four = _stated(last_four)
    if label and four and not _HAS_FOUR_DIGIT_RUN.search(label):
        return f"{label} {_LAST_FOUR_PREFIX}{four}"
    if label:
        return label
    if institution and kind and four:
        return f"{institution} {kind} {_LAST_FOUR_PREFIX}{four}"
    if institution and four:
        return f"{institution} {_LAST_FOUR_PREFIX}{four}"
    if institution and kind:
        return f"{institution} {kind}"
    if institution:
        return institution
    if kind and four:
        return f"{kind} {_LAST_FOUR_PREFIX}{four}"
    if kind:
        return kind
    if four:
        return f"{_LAST_FOUR_PREFIX}{four}"
    return UNNAMED_ACCOUNT_LABEL


@dataclass(frozen=True, slots=True)
class AccountNameFacts:
    """The facts ``core.dim_accounts`` builds a display name from.

    Carried on a :class:`SourceAccount` so each channel can state them where
    its own raw account row is written — the only place that knows which
    spelling of the institution and which account-number column the model
    will read — while the mint report, built much later and elsewhere, stays
    a single derivation.
    """

    source_label: str | None = None
    """The display-ready account label this channel will write to raw, if any.

    Set only from a name a person authored — the file's account column, or
    ``--account-name``. Never a placeholder the importer synthesized from the
    filename: that names the upload, and promoting it would let renaming a file
    rename the account.
    """

    institution_name: str | None = None
    category: str | None = None
    last_four: str | None = None

    def with_settings(self, settings: dict[str, str] | None) -> AccountNameFacts:
        """Fold in the account settings this import is about to capture.

        ``dim_accounts`` reads ``COALESCE(s.account_subtype, w.account_subtype,
        ...)`` and ``COALESCE(s.last_four, w.last_four_derived)``, so a caller
        who supplies either in ``account_metadata`` renames the account without
        naming it. Ignoring them here would report the pre-override label and
        reintroduce the disagreement in a quieter form.

        Taken verbatim, not re-derived: the model reads these columns straight,
        and ``AccountSettings`` has already validated them.
        """
        if not settings:
            return self
        return replace(
            self,
            category=_stated(settings.get("account_subtype")) or self.category,
            last_four=_stated(settings.get("last_four")) or self.last_four,
        )

    def display_name(self) -> str:
        """The label ``core.dim_accounts`` will store for this account."""
        return derive_display_name(
            source_label=self.source_label,
            institution_name=self.institution_name,
            category=self.category,
            last_four=self.last_four,
        )


@dataclass(frozen=True)
class SourceAccount:
    """One source account presented to the resolver.

    ``source_account_key`` is the source's native key (OFX number, CSV slug,
    Plaid token, or PDF document digest) — the ``source_native`` ref_value
    staging joins on.
    PII fields (``account_number``) are used as scoped confirmers and never logged.
    """

    source_type: str
    source_origin: str
    source_account_key: str
    account_name: str
    account_name_is_user_set: bool = False
    """Whether ``account_name`` is a person- or source-authored label rather
    than a generated fallback (institution + type, a bare filename, a raw
    token). Mirrors ``core.dim_accounts.display_name_is_user_set`` on the
    candidate side: the resolver's name rung requires this on the SOURCE side
    too, so a channel that has no authored name field (OFX has none at all)
    can't have its generated placeholder read back as name evidence. Default
    False is the safe reading for a channel that never sets it."""
    account_number: str | None = None
    last_four: str | None = None
    institution: str | None = None
    persistent_token: str | None = None
    legacy_source_account_key: str | None = None
    """A superseded source key that may nominate a review candidate, never adopt."""
    legacy_source_origin: str | None = None
    """The origin that scoped ``legacy_source_account_key`` before replacement."""
    legacy_source_account_key_is_filename_alias: bool = False
    """Whether the legacy key came from an anchorless PDF filename alias."""
    source_file: str | None = None
    """Canonical source path used only to recover a proven historical PDF tuple."""
    unpinned_account_key: str | None = None
    """The key this source derives on its own, when a pin made it use another.

    A pinned import borrows the key its account already answers to so the rows
    dedup, which leaves nothing on record identifying THIS file. Carried here so
    the resolver can also link the derived key, and an unpinned re-import of the
    same file still recognises the account instead of asking or minting."""

    name_facts: AccountNameFacts | None = None
    """What ``core.dim_accounts`` will name this account by, if it mints one.

    Never a resolution signal — the resolver ignores it. It rides here because
    the mint report (``accounts_created``) is built long after the channel that
    knows which institution spelling and which account-number column the model
    will read. Distinct from ``account_name`` beside it, which is the file's raw
    free-text label and feeds fuzzy matching: ``name_facts.source_label`` is the
    display-safe form of that label, and is the top rung the model names by.
    Left None only by callers that never report a mint (the sync path, the
    resolver's own probes)."""

    explicit_account_id: str | None = None
    force_standalone: bool = False
    """User declared this a NEW standalone account: mint fresh, skip the
    weak-candidate merge pass. Set by an import-time ``account_bindings`` entry
    of ``"new"``. Still idempotent on re-import (adopts an existing
    source_native above)."""

    def __post_init__(self) -> None:
        """Canonicalize a blank last four to None — they mean the same thing.

        ``SyncAccount.mask`` declares only a maximum length, so the sync server
        can send ``""`` or ``"  "``; a source that writes an empty column
        produces the same. All answer the last4 rung with silence, but the
        resolver asks whether that answer is missing in two conventions — ``is
        None`` at the quarantine gates, falsy at the lookup and reissue passes —
        and neither ``"" is None`` nor ``bool("  ")`` agrees. Canonicalizing here
        is what keeps the two from disagreeing, rather than requiring every
        present and future consumer to pick the right one.

        Stripping, not just an empty-string test: a whitespace-only mask is
        truthy and non-None, so it would clear the quarantine gate that ``""``
        cannot. Padding around real digits is the same defect one step along —
        the last4 lookup matches exactly, so ``" 1234 "`` would mint a second
        account for a ledger that already has one.
        """
        if self.last_four is not None:
            stripped = self.last_four.strip()
            object.__setattr__(self, "last_four", stripped or None)
