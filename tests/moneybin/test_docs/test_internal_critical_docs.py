"""Ties the public `raw`/`prep` declaration counts to the live registry.

Nine sentences across eight user-facing files state how many `raw`/`prep`
columns carry a CRITICAL declaration and how many the value-shape scan is
therefore responsible for. Every one of them is a privacy claim a reader can
act on, and none of them was mechanically bound to `INTERNAL_CRITICAL` — so the
next declaration added would have falsified all nine silently.

Both halves are derived from the constant rather than written as literals, per
`.claude/references/guard-design.md`: a guard that hard-codes the number it is
checking only proves the docs agree with the guard.
"""

from __future__ import annotations

import re
from pathlib import Path

from moneybin.privacy.taxonomy import INTERNAL_CRITICAL

ROOT = Path(__file__).parents[3]

#: Every phrasing the docs use for the declaration count, each capturing the
#: number. Kept as alternatives rather than one loose `\d+` because a bare
#: number match would fire on unrelated counts elsewhere in the same files.
_COUNT_CLAIM = re.compile(
    r"(\d+) columns across (\d+) tables"
    r"|(\d+) (?:hand-)?declared columns"
    r"|(\d+) CRITICAL-column declarations"
    r"|(\d+) `raw`/`prep` columns"
)

#: The files carrying the claim, as a set rather than a floor: a new site must
#: be noticed so it can be checked, and a site that loses its sentence must be
#: noticed too — otherwise every phrasing could drift out of the regex and this
#: test would pass over zero matches.
_CLAIM_FILES = frozenset({
    "CHANGELOG.md",
    "docs/features.md",
    "docs/guides/database-security.md",
    "docs/guides/sql-access.md",
    "docs/guides/threat-model.md",
    "docs/guides/what-the-ai-sees.md",
    "docs/roadmap.md",
    "docs/specs/INDEX.md",
    "docs/specs/queryable-internal-schemas.md",
})


def _doc_paths() -> list[Path]:
    return [
        ROOT / "CHANGELOG.md",
        ROOT / "README.md",
        *sorted(ROOT.glob("docs/**/*.md")),
    ]


def test_public_docs_state_the_live_internal_critical_counts() -> None:
    declared_tables = len(INTERNAL_CRITICAL)
    declared_columns = sum(len(columns) for columns in INTERNAL_CRITICAL.values())

    found: dict[str, list[tuple[int, ...]]] = {}
    for path in _doc_paths():
        # Whitespace-flattened first: markdown wraps these sentences, and a
        # claim split across two lines is the same claim.
        matches = _COUNT_CLAIM.findall(" ".join(path.read_text().split()))
        if not matches:
            continue
        rel = path.relative_to(ROOT).as_posix()
        found[rel] = [tuple(int(g) for g in match if g) for match in matches]

    assert set(found) == _CLAIM_FILES, (
        "A file gained or lost the raw/prep declaration-count claim. Check the "
        "new sentence against INTERNAL_CRITICAL and update _CLAIM_FILES."
    )

    for rel, claims in sorted(found.items()):
        for claim in claims:
            assert claim[0] == declared_columns, (
                f"{rel} states {claim[0]} declared raw/prep columns; "
                f"INTERNAL_CRITICAL holds {declared_columns}."
            )
            if len(claim) == 2:
                assert claim[1] == declared_tables, (
                    f"{rel} states {claim[1]} declared raw/prep tables; "
                    f"INTERNAL_CRITICAL covers {declared_tables}."
                )
