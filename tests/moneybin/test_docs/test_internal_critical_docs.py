"""Ties every public statement of the `raw`/`prep` declaration count to the registry.

`INTERNAL_CRITICAL` is the only thing standing between an undeclared
`raw`/`prep` column and the value-shape scan, and how many columns it holds is
stated in prose across the docs, the CHANGELOG, the design system, two MCP
strings, and the CLI help. Every one of those is a privacy claim a reader acts
on. None was mechanically bound to the registry, so the next declaration added
would have falsified all of them at once and silently.

Two things are derived here rather than written down, because writing either one
down reintroduces the failure:

- **The number**, from `INTERNAL_CRITICAL`. A guard that hard-codes the count it
  checks only proves the docs agree with the guard.
- **The corpus**, from `git ls-files`. The first version of this guard
  enumerated its corpus as `docs/**/*.md` and missed six sites — four of them in
  `src/`, including the registered `sql_query` `description=` an agent reads at
  tool-selection time and the `moneybin://schema` footer note. A corpus kept by
  hand is the same hazard as a count kept by hand, one level up.

What stays written down is `_CLAIM_SITES`: which files carry the claim and how
many each carries. That is the set-equality half (`.claude/references/guard-design.md`).
It fails on a file that gains a claim, on a file that loses one, and — the case
that matters most — on a rewording that slips out of `_CLAIM_PATTERNS`, so the
guard can never quietly pass over nothing.

The patterns were fitted to the prose, never the reverse. The MCP and CLI
description strings were settled over five review rounds and carry a masking
qualification that was hard to get right; binding them to a guard is not a
licence to edit them.
"""

from __future__ import annotations

import re
import subprocess  # noqa: S404  # git invoked with static args only
from pathlib import Path

from moneybin.privacy.taxonomy import INTERNAL_CRITICAL

ROOT = Path(__file__).parents[3]

#: Blank-line separated blocks. The claim and the `raw`/`prep` subject it
#: belongs to routinely sit in different sentences of one paragraph — a bullet
#: whose bolded lead names the schemas and whose body states the count — so
#: sentence scoping drops real sites.
_PARAGRAPH = re.compile(r"\n\s*\n")

#: A paragraph is a candidate only when it is about both schemas and about
#: declaring. Requiring *both* schema names is what keeps counts over the
#: neighbouring `CLASSIFICATION` registry out.
_DECLARES = re.compile(r"declar", re.IGNORECASE)
_RAW = re.compile(r"\braw\b", re.IGNORECASE)
_PREP = re.compile(r"\bprep\b", re.IGNORECASE)

#: A bare integer, rejecting the shapes that are identifiers rather than counts:
#: `ADR-013`, `M1K.2`, `D4`, `#372`, and a `1.` list marker.
_COUNT = re.compile(r"(?<![A-Za-z#.\-\d])(\d+)\b(?!\.)")

#: Plural only. `12. Every money column declares …` and `the Phase 1 catch-all
#: seed table` bind a number to a singular noun; no real claim does.
_COUNTED_NOUN = re.compile(r"^(columns|tables|declarations)$", re.IGNORECASE)
_WORD = re.compile(r"[\w'`/*\-]+")

#: `raw and prep declare 33 and scan every other value` states the count with no
#: noun after it at all, so the verb has to anchor that one.
_DECLARING_VERB = re.compile(r"\bdeclar(?:e|es|ing)\s+$", re.IGNORECASE)

#: How many modifier words may sit between the number and its noun.
#: `33 hand-written CRITICAL declarations` is the longest real one.
_MODIFIER_BUDGET = 3

#: Files carrying the claim, and how many each carries. Set equality, both
#: directions — see the module docstring.
_CLAIM_SITES = {
    "CHANGELOG.md": 1,
    "design-system/ai-surface.md": 1,
    "docs/features.md": 2,
    "docs/guides/database-security.md": 1,
    "docs/guides/sql-access.md": 4,
    "docs/guides/threat-model.md": 1,
    "docs/guides/what-the-ai-sees.md": 2,
    # Generated copies of the `sql` help and the `sql_query` description below;
    # the drift test in test_documentation_policy.py pins them to the source.
    "docs/reference/cli/sql.md": 1,
    "docs/reference/data-model.md": 1,
    "docs/reference/mcp-tools.md": 1,
    "docs/reference/system-overview.md": 1,
    "docs/roadmap.md": 1,
    "docs/specs/INDEX.md": 1,
    "docs/specs/queryable-internal-schemas.md": 2,
    "src/moneybin/cli/commands/sql.py": 1,
    "src/moneybin/mcp/tools/sql.py": 2,
    "src/moneybin/services/schema_catalog.py": 1,
}

_NUMBER_WORDS = (
    "zero one two three four five six seven eight nine ten eleven twelve "
    "thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty"
).split()

SQL_ACCESS_GUIDE = ROOT / "docs/guides/sql-access.md"
_ENUMERATION_LEAD = "names and no others:"
_IDENTIFIER = re.compile(r"`([a-z][a-z0-9_]*)`")


def _tracked_corpus() -> list[Path]:
    """Every tracked markdown file, plus every shipped Python source file.

    `git ls-files` rather than a glob over roots: it needs no exclusion list for
    `.venv`, build output, or untracked scratch, and "tracked" is exactly the
    surface that ships.

    This makes the guard depend on running inside a git checkout — `check=True`
    raises anywhere else. That is not a gap: the wheel ships no tests, so every
    context that can run this file is a checkout.
    """
    listed = subprocess.run(  # noqa: S603  # fixed argv, no user input
        ["git", "ls-files"],  # noqa: S607  # git resolved from PATH, as everywhere in CI
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return [
        ROOT / name
        for name in listed
        if name.endswith(".md") or (name.startswith("src/") and name.endswith(".py"))
    ]


def _counted_noun_after(paragraph: str, end: int) -> str | None:
    """The plural noun a number counts, within `_MODIFIER_BUDGET` words of it."""
    for index, word in enumerate(_WORD.finditer(paragraph[end : end + 70])):
        if index >= _MODIFIER_BUDGET:
            return None
        if _COUNTED_NOUN.match(word.group(0)):
            return word.group(0).lower()
    return None


def _claims(path: Path) -> list[tuple[int, str]]:
    """Every declaration-count claim in one file, as `(stated number, subject)`."""
    found: list[tuple[int, str]] = []
    for block in _PARAGRAPH.split(path.read_text()):
        paragraph = " ".join(block.split())
        if not (
            _DECLARES.search(paragraph)
            and _RAW.search(paragraph)
            and _PREP.search(paragraph)
        ):
            continue
        for number in _COUNT.finditer(paragraph):
            noun = _counted_noun_after(paragraph, number.end())
            if noun is None and not _DECLARING_VERB.search(paragraph[: number.start()]):
                continue
            # A bare `declare 33` counts columns; so does `33 … declarations`.
            found.append((int(number.group(1)), noun or "columns"))
    return found


def test_public_docs_state_the_live_internal_critical_counts() -> None:
    declared_tables = len(INTERNAL_CRITICAL)
    declared_columns = sum(len(columns) for columns in INTERNAL_CRITICAL.values())
    expected = {
        "columns": declared_columns,
        "declarations": declared_columns,
        "tables": declared_tables,
    }

    found = {
        path.relative_to(ROOT).as_posix(): claims
        for path in _tracked_corpus()
        if (claims := _claims(path))
    }

    assert {name: len(claims) for name, claims in found.items()} == _CLAIM_SITES, (
        "A file gained or lost a raw/prep declaration-count claim, or a "
        "rewording no longer matches. Check the sentence against "
        "INTERNAL_CRITICAL, then register it in _CLAIM_SITES — and if the "
        "prose is right but unmatched, widen _COUNTED_NOUN / _DECLARING_VERB "
        "to fit the prose rather than rewriting the prose to fit them."
    )

    for name, claims in sorted(found.items()):
        for stated, subject in claims:
            assert stated == expected[subject], (
                f"{name} states {stated} declared raw/prep {subject}; "
                f"INTERNAL_CRITICAL holds {expected[subject]}."
            )


def test_the_sql_access_guide_enumerates_the_declared_column_names() -> None:
    """The narrower claim over the same registry, and the more falsifiable one.

    `sql-access.md` does not only count the declarations — it names them and
    says "and no others". A declaration added under a tenth name leaves the
    count claim wrong *and* this list incomplete, and a reader checking the list
    is the one who would act on it.
    """
    declared_names = {
        column for columns in INTERNAL_CRITICAL.values() for column in columns
    }

    text = " ".join(SQL_ACCESS_GUIDE.read_text().split())
    lead = text.index(_ENUMERATION_LEAD)
    sentence = text[lead + len(_ENUMERATION_LEAD) :].split(". ", 1)[0]

    assert set(_IDENTIFIER.findall(sentence)) == declared_names
    assert f"{_NUMBER_WORDS[len(declared_names)]} {_ENUMERATION_LEAD}" in text
