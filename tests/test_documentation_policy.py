"""Regression checks for public documentation policy.

Guards two rules from .claude/rules/documentation.md:

1. Specs and ADRs explain MoneyBin decisions with the project's own
   constraints and evidence, not an external product's behavior. The lexicon
   scan below is a backstop for the manual audit, not the enforcement
   mechanism: it matches paragraph-joined text so a phrase split across a
   markdown line wrap cannot hide, but synonyms outside the lexicon still
   require reviewer judgment.
2. Public documents never link into ``private/``.

A paragraph that must legitimately name an external product (a compatibility
matrix, a migration note) declares it inline with
``<!-- external-products-ok: <reason> -->`` in that paragraph; undeclared
matches fail.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_AUTHORITY_DOCS = (_REPO_ROOT / "docs" / "specs", _REPO_ROOT / "docs" / "decisions")
_PUBLIC_DOC_FILES = (
    _REPO_ROOT / "README.md",
    _REPO_ROOT / "CONTRIBUTING.md",
    _REPO_ROOT / "CHANGELOG.md",
    _REPO_ROOT / "SECURITY.md",
)
_PUBLIC_DOC_ROOT = _REPO_ROOT / "docs"
_ALLOW_MARKER = "external-products-ok"
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
_PRIVATE_LINK = re.compile(r"\]\((?:\.{1,2}/)*private/")


def _authority_documents() -> list[Path]:
    return sorted(
        document
        for root in _AUTHORITY_DOCS
        for document in root.rglob("*.md")
        if "archived" not in document.parts
    )


def _paragraphs(text: str) -> list[tuple[int, str]]:
    """Split into (first_line_number, space-joined-text) paragraphs.

    Joining wrapped lines is the point: a trigger phrase split across a
    markdown line wrap must still match.
    """
    paragraphs: list[tuple[int, str]] = []
    current: list[str] = []
    start_line = 1
    for line_number, line in enumerate(text.splitlines(), start=1):
        if line.strip():
            if not current:
                start_line = line_number
            current.append(line.strip())
        elif current:
            paragraphs.append((start_line, " ".join(current)))
            current = []
    if current:
        paragraphs.append((start_line, " ".join(current)))
    return paragraphs


def test_public_design_authority_is_not_competitor_derived() -> None:
    """Specs and ADRs name MoneyBin's constraints, not a competitor's behavior."""
    documents = _authority_documents()
    assert documents, "documentation policy check found no authority documents"

    violations: list[str] = []
    for document in documents:
        for start_line, paragraph in _paragraphs(document.read_text()):
            if _ALLOW_MARKER in paragraph:
                continue
            if match := _COMPETITOR_DERIVATION.search(paragraph):
                path = document.relative_to(_REPO_ROOT)
                violations.append(f"{path}:{start_line}: {match.group()!r}")

    assert not violations, (
        "Public specs and ADRs must state MoneyBin's own rationale. Keep "
        "comparative research private, or declare a legitimate external-product "
        f"mention inline with `<!-- {_ALLOW_MARKER}: <reason> -->`; see "
        ".claude/rules/documentation.md.\n" + "\n".join(violations)
    )


def test_public_docs_do_not_link_private() -> None:
    """Public documents never link into private/ (rule: Visibility)."""
    documents = [f for f in _PUBLIC_DOC_FILES if f.exists()]
    documents += sorted(_PUBLIC_DOC_ROOT.rglob("*.md"))
    assert documents, "documentation policy check found no public documents"

    violations: list[str] = []
    for document in documents:
        for line_number, line in enumerate(document.read_text().splitlines(), start=1):
            if _PRIVATE_LINK.search(line):
                path = document.relative_to(_REPO_ROOT)
                violations.append(f"{path}:{line_number}: {line.strip()[:120]}")

    assert not violations, (
        "Public documents must not link into private/. Replace the reference "
        "with a public issue, roadmap item, or an honest statement that the "
        "work is planned; see .claude/rules/documentation.md.\n" + "\n".join(violations)
    )
