"""Regression checks for public documentation authority.

Technical specs and ADRs explain MoneyBin decisions with its own constraints
and evidence. Comparative research belongs in private working material unless
the document's purpose is compatibility, migration, or a user-facing
comparison.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_AUTHORITY_DOCS = (_REPO_ROOT / "docs" / "specs", _REPO_ROOT / "docs" / "decisions")
_COMPETITOR_DERIVATION = re.compile(
    r"\b(?:"
    r"competitor(?:s|'s|’s)?|"
    r"competitive(?:\s+(?:bar|context|landscape))?|"
    r"adopted\s+(?:verbatim\s+)?from|"
    r"inspired\s+by|"
    r"learned\s+from|"
    r"model(?:l)?ed\s+on|"
    r"best-in-class|"
    r"(?:cross-project|cross-aggregator)\s+survey"
    r")\b",
    re.IGNORECASE,
)


def _authority_documents() -> list[Path]:
    return sorted(
        document
        for root in _AUTHORITY_DOCS
        for document in root.rglob("*.md")
        if "archived" not in document.parts
    )


def test_public_design_authority_is_not_competitor_derived() -> None:
    """Specs and ADRs name MoneyBin's constraints, not a competitor's behavior."""
    documents = _authority_documents()
    assert documents, "documentation policy check found no authority documents"

    violations: list[str] = []
    for document in documents:
        for line_number, line in enumerate(document.read_text().splitlines(), start=1):
            if match := _COMPETITOR_DERIVATION.search(line):
                path = document.relative_to(_REPO_ROOT)
                violations.append(f"{path}:{line_number}: {match.group()!r}")

    assert not violations, (
        "Public specs and ADRs must state MoneyBin's own rationale. Keep "
        "comparative research private; see .claude/rules/documentation.md.\n"
        + "\n".join(violations)
    )
