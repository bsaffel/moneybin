"""Python mirror of ``core.dim_accounts``'s display-name derivation.

An import announces the accounts it minted (``accounts_created``) before any
transform has run — and ``import_confirm`` never refreshes at all — so the
label cannot be read back from ``core``. It is derived here instead, from the
same two seed CSVs the SQL model joins (``institutions``, ``account_type_map``),
so the two cannot disagree about a bank's name or an account type's spelling.

The top rung is the exception and needs no registry: it is the account label the
importer is about to write to ``raw.tabular_accounts.account_label`` (or that
Plaid already sent), display-ready before it gets here. Both sides read the same
string, so agreement there is structural rather than mirrored.

What *can* drift is the shape of the ladder below against
``dim_accounts.sql``'s ``COALESCE`` chain. That is pinned by
``tests/integration/test_mint_report_names.py``, which imports on every channel
with a real refresh and asserts the mint report equals the stored name.

Reporting a name derived some other way is the defect this replaces (#446): the
old label was the OFX ``<ORG>`` routing code plus the file's raw type spelling,
which named no account the user could later find and, having no per-account
discriminator, collided across distinct accounts.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, replace
from functools import lru_cache
from importlib import resources

from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL

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
    label written in any non-Latin script — ``储蓄账户``, ``Сбережения`` — held
    no "letter", so the rung dropped a name a person actually chose and named
    the account by an assembled label instead.

    ``str.isalpha`` is the exact Python spelling of ``\p{L}``: both are the
    Unicode letter categories and nothing else, so ``²`` and ``Ⅳ`` fail on
    both sides. A ``[^\W\d_]`` regex would have accepted those two and
    reopened the drift this mirror exists to prevent.
    """
    return any(character.isalpha() for character in text)


#: A label already carrying a four-digit group does not also take a last four.
#: Four digits is the last-four unit, so such a label is either stating the
#: account's own already or is what the masker left of a longer number —
#: ``Checking ****5678`` joined with ``…9012`` publishes eight digits of a
#: twelve-digit number, well past what the last-four convention allows. A year
#: inside a name is indistinguishable from a number's tail, so neither one is
#: joined. Mirrors the model's ``NOT REGEXP_MATCHES(account_label, '[0-9]{4}')``.
_HAS_FOUR_DIGIT_RUN = re.compile(r"[0-9]{4}")


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

    Carried on a :class:`~moneybin.services.account_resolution_types.SourceAccount`
    so each channel can state them where its own raw account row is written —
    the only place that knows which spelling of the institution and which
    account-number column the model will read — while the mint report, built
    much later and elsewhere, stays a single derivation.
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
