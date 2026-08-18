"""Regression checks for public documentation policy.

Guards three documentation and agent-routing rules:

1. Specs and ADRs explain MoneyBin decisions with the project's own
   constraints and evidence, not an external product's behavior. The lexicon
   scan below is a backstop for the manual audit, not the enforcement
   mechanism: it matches paragraph-joined text so a phrase split across a
   markdown line wrap cannot hide, but synonyms outside the lexicon still
   require reviewer judgment.
2. Public documents never link into ``private/``.
3. Active agent instructions never route work to retired local trackers or the
   retired ``update-specs`` skill.

A paragraph that must legitimately name an external product (a compatibility
matrix, a migration note) declares it inline with
``<!-- external-products-ok: <reason> -->`` in that paragraph; undeclared
matches fail.
"""

from __future__ import annotations

import re
import shutil
import subprocess  # noqa: S404 -- the policy test queries local Git metadata
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_GIT = shutil.which("git")
assert _GIT is not None, "documentation policy check requires Git"
_AUTHORITY_DOCS = (_REPO_ROOT / "docs" / "specs", _REPO_ROOT / "docs" / "decisions")
_PUBLIC_DOC_FILES = (
    _REPO_ROOT / "README.md",
    _REPO_ROOT / "CONTRIBUTING.md",
    _REPO_ROOT / "CHANGELOG.md",
    _REPO_ROOT / "SECURITY.md",
)
_PUBLIC_DOC_ROOT = _REPO_ROOT / "docs"
_ALLOW_MARKER = "external-products-ok"
_RETIRED_ROUTE_DESCRIPTION_MARKER = "retired-route-description-ok"
_AUTHORIZED_PRIVATE_REPOSITORY_ROUTE = re.compile(
    r"(?<![\w./-])(?:https://github\.com/)?bsaffel/moneybin-private/"
)
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
# Inline links, reference-style definitions, and HTML hrefs — all three
# Markdown link destinations a public doc could use to reach private/.
_PRIVATE_LINK = re.compile(
    r"\]\(/?(?:\.{1,2}/)*private/"
    r"|^\s*\[[^\]]+\]:\s*/?(?:\.{1,2}/)*private/"
    r"|href=[\"']/?(?:\.{1,2}/)*private/"
)
_RETIRED_AGENT_ROUTE = re.compile(
    r"private/"
    r"|docs/followups\.md"
    r"|/update-(?:progress|specs)\b"
)


def _authority_documents() -> list[Path]:
    return sorted(
        document
        for root in _AUTHORITY_DOCS
        for document in root.rglob("*.md")
        if "archived" not in document.parts
    )


def _active_agent_instruction_files(repo_root: Path = _REPO_ROOT) -> list[Path]:
    tracked = subprocess.run(  # noqa: S603 -- fixed Git binary and arguments
        [_GIT, "ls-files", "-z"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split("\0")
    files: list[Path] = []
    for raw_path in tracked:
        if not raw_path:
            continue
        relative = Path(raw_path)
        posix = relative.as_posix()
        if (
            relative.name in {"AGENTS.md", "CLAUDE.md"}
            or (posix.startswith(".claude/") and relative.suffix == ".md")
            or (posix.startswith(".cursor/rules/") and relative.suffix == ".mdc")
            or posix
            in {
                ".github/ai-review-protocol.md",
                ".github/workflows/ai-review.yml",
            }
            or relative.name.endswith(".prompt.md")
        ):
            files.append(repo_root / relative)
    return sorted(files)


def _has_retired_agent_route(line: str) -> bool:
    without_authorized_repository = _AUTHORIZED_PRIVATE_REPOSITORY_ROUTE.sub("", line)
    return bool(_RETIRED_AGENT_ROUTE.search(without_authorized_repository))


def test_retired_route_guard_covers_any_local_private_destination() -> None:
    """The blanket local-private ban is not limited to historical folder names."""
    assert _has_retired_agent_route("save durable evidence in private/")
    assert _has_retired_agent_route("save durable evidence in `private/`")
    assert _has_retired_agent_route("save durable evidence in private/evidence/")
    assert _has_retired_agent_route("save durable evidence in ./moneybin-private/")
    assert _has_retired_agent_route("save durable evidence in evil-moneybin-private/")
    assert _has_retired_agent_route(
        "save evidence in https://evil.example/bsaffel/moneybin-private/"
    )
    assert not _has_retired_agent_route(
        "save durable evidence in bsaffel/moneybin-private/evidence/"
    )
    assert not _has_retired_agent_route(
        "save evidence in https://github.com/bsaffel/moneybin-private/evidence/"
    )


def test_active_agent_instruction_files_include_all_harness_surfaces() -> None:
    """Every repository agent-instruction surface participates in the guard."""
    expected = {_REPO_ROOT / "CLAUDE.md", _REPO_ROOT / "design-system/CLAUDE.md"}
    expected.update((_REPO_ROOT / ".cursor" / "rules").rglob("*.mdc"))
    expected.update((_REPO_ROOT / "design-system" / "components").rglob("*.prompt.md"))
    expected.add(_REPO_ROOT / ".github" / "ai-review-protocol.md")
    expected.add(_REPO_ROOT / ".github" / "workflows" / "ai-review.yml")
    assert expected <= set(_active_agent_instruction_files())


def test_active_agent_instruction_files_ignore_untracked_nested_checkout(
    tmp_path: Path,
) -> None:
    """Ignored worktrees do not contribute stale instructions to this checkout."""
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(  # noqa: S603 -- isolated test repository
        [_GIT, "init", "-q"], cwd=repo, check=True
    )
    tracked = repo / "CLAUDE.md"
    tracked.write_text("current\n")
    stale = repo / ".worktrees" / "stale" / "CLAUDE.md"
    stale.parent.mkdir(parents=True)
    stale.write_text("save work in private/\n")
    subprocess.run(  # noqa: S603 -- isolated test repository
        [_GIT, "add", "CLAUDE.md"], cwd=repo, check=True
    )

    assert _active_agent_instruction_files(repo) == [tracked]


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


def test_active_agent_instructions_do_not_route_to_retired_trackers_or_skills() -> None:
    """Active instructions use GitHub, Linear, and session scratch surfaces."""
    documents = _active_agent_instruction_files()
    assert documents, "agent routing policy check found no active instructions"

    violations: list[str] = []
    for document in documents:
        for line_number, line in enumerate(document.read_text().splitlines(), start=1):
            if (
                _RETIRED_ROUTE_DESCRIPTION_MARKER not in line
                and _has_retired_agent_route(line)
            ):
                path = document.relative_to(_REPO_ROOT)
                violations.append(f"{path}:{line_number}: {line.strip()[:120]}")

    retired_skill = _REPO_ROOT / ".claude" / "skills" / "update-specs" / "SKILL.md"
    if retired_skill.exists():
        violations.append(
            f"{retired_skill.relative_to(_REPO_ROOT)}: retired skill exists"
        )

    assert not violations, (
        "Active agent instructions must route private knowledge to "
        "bsaffel/moneybin-private, coordination to Linear, public delivery to "
        "GitHub, and disposable plans to session scratch. Remove retired local "
        "tracker and update-specs routes. Descriptive mentions of the retired "
        "path must declare `<!-- retired-route-description-ok -->`.\n"
        + "\n".join(violations)
    )
