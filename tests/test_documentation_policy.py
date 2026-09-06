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

import json
import re
import shlex
import shutil
import subprocess  # noqa: S404 -- the policy test queries local Git metadata
from pathlib import Path
from typing import NamedTuple

import click
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _git_executable() -> str:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("documentation policy check requires Git")
    return executable


_GIT = _git_executable()
_AUTHORITY_DOCS = (_REPO_ROOT / "docs" / "specs", _REPO_ROOT / "docs" / "decisions")
_PUBLIC_DOC_FILES = (
    _REPO_ROOT / "README.md",
    _REPO_ROOT / "CONTRIBUTING.md",
    _REPO_ROOT / "CONTEXT.md",
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
    r"|`private`"
    r"|\bprivate\s+(?:directory|folder|tracker|tree)\b"
    r"|docs/followups\.md"
    r"|update-(?:progress|specs)\b"
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
            relative.name in {"AGENTS.md", "CLAUDE.md", "CONTEXT.md"}
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
    assert _has_retired_agent_route("save the plan in `private`")
    assert _has_retired_agent_route("save the plan in the private directory")
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
    assert not _has_retired_agent_route("keep private strategy in the canonical repo")


def test_retired_route_guard_covers_unslashed_skill_names() -> None:
    assert _has_retired_agent_route("the update-specs skill helps here")
    assert _has_retired_agent_route("route work to update-progress")


def test_active_agent_instruction_files_include_all_harness_surfaces() -> None:
    """Every repository agent-instruction surface participates in the guard."""
    expected = {
        _REPO_ROOT / "AGENTS.md",
        _REPO_ROOT / "CLAUDE.md",
        _REPO_ROOT / "CONTEXT.md",
        _REPO_ROOT / "design-system/CLAUDE.md",
    }
    for root in (
        _REPO_ROOT / ".claude" / "commands",
        _REPO_ROOT / ".claude" / "references",
        _REPO_ROOT / ".claude" / "rules",
        _REPO_ROOT / ".claude" / "skills",
    ):
        expected.update(root.rglob("*.md"))
    expected.update((_REPO_ROOT / ".cursor" / "rules").rglob("*.mdc"))
    expected.update((_REPO_ROOT / "design-system" / "components").rglob("*.prompt.md"))
    expected.add(_REPO_ROOT / ".github" / "ai-review-protocol.md")
    expected.add(_REPO_ROOT / ".github" / "workflows" / "ai-review.yml")
    assert expected == set(_active_agent_instruction_files())


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


def test_project_tracking_locator_routes_to_canonical_private_declaration() -> None:
    declaration = json.loads(
        (_REPO_ROOT / ".agents/project-tracking.json").read_text(encoding="utf-8")
    )

    assert declaration == {
        "schema_version": 1,
        "project_id": "moneybin",
        "declaration_mode": "locator",
        "delivery_repository": "bsaffel/moneybin",
        "canonical_repository": "bsaffel/moneybin-private",
    }


# ---------------------------------------------------------------------------
# CLI invocations in public docs resolve to registered commands
# ---------------------------------------------------------------------------

# The user-facing set only: specs and ADRs describe planned or superseded
# commands by design, and CHANGELOG entries are historical.
_USER_FACING_DOC_DIRS = ("guides", "reference")
_CLI_INVOCATION_MARKER = "cli-invocation-ok"
_FENCE = re.compile(r"^(?P<fence>`{3,}|~{3,})\s*(?P<lang>[\w-]*)")
# Fenced blocks in these languages show shell input; every other language
# (mermaid, yaml, sql, python, …) mentions `moneybin` as data, not as a command.
_SHELL_LANGS = {"", "bash", "sh", "shell", "zsh", "console", "shell-session", "text"}
# A span may soft-wrap; a blank line ends the paragraph and with it the span.
_INLINE_CODE = re.compile(r"`((?:[^`\n]|\n(?!\n))+)`")
_INVOCATION_START = re.compile(r"(?<![\w./:@-])moneybin(?=\s+[A-Za-z<*{-])")
_TERMINATORS = {"|", "||", "&&", ";", ">", ">>", "2>", "<"}
# `2>&1`, `>/dev/null`, a trailing `&`. Not `<`: that opens a placeholder.
_REDIRECTION = re.compile(r"^(\d*>|&>|&$)")
_ELISIONS = {"...", "…"}
_COMMAND_SUBSTITUTION = re.compile(r"\$\((?P<inner>[^()]*)\)")
_SUBSTITUTED_INVOCATION = re.compile(r"^\s*moneybin(?:\s|$)")
_ANGLE_PLACEHOLDER = re.compile(r"<[^<>]*>")
_OPTIONAL_SEGMENT = re.compile(r"\[([^\[\]]*)\]")
_TRAILING_COMMENT = re.compile(r"(^|\s)#.*$")
_BRACE_ALTERNATIVES = re.compile(r"^\{([^{}]+)\}$")
_PATH_LIKE = re.compile(r"^[~./]|\.\w+$")


class _CodeLine(NamedTuple):
    """One scanned line and where it came from.

    A fenced shell block is a transcript that must run as written
    (``runnable=True``); an inline span or table row names a command
    (``runnable=False``) and is held to a looser standard.
    """

    number: int
    code: str
    runnable: bool


def _user_facing_documents() -> list[Path]:
    documents = [
        _REPO_ROOT / "README.md",
        _REPO_ROOT / "CONTRIBUTING.md",
        _REPO_ROOT / "CONTEXT.md",
    ]
    documents += sorted(_PUBLIC_DOC_ROOT.glob("*.md"))
    for directory in _USER_FACING_DOC_DIRS:
        documents += sorted((_PUBLIC_DOC_ROOT / directory).rglob("*.md"))
    return [document for document in documents if document.exists()]


def _inline_spans(prose: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """Inline code spans in one run of prose, each on the line it opens."""
    if not prose:
        return []
    first, joined = prose[0][0], "\n".join(line for _, line in prose)
    spans: list[tuple[int, str]] = []
    for match in _INLINE_CODE.finditer(joined):
        number = first + joined.count("\n", 0, match.start())
        span = " ".join(match.group(1).split())
        spans.append((number, span))
    return spans


def _code_lines(text: str) -> list[_CodeLine]:
    """Return one `_CodeLine` per fenced-block line and inline code span."""
    lines: list[_CodeLine] = []
    prose: list[tuple[int, str]] = []
    fence: str | None = None
    shell_block = False
    for number, line in enumerate(text.splitlines(), start=1):
        match = _FENCE.match(line.strip())
        if match and (fence is None or line.strip().startswith(fence)):
            lines += (_CodeLine(n, c, runnable=False) for n, c in _inline_spans(prose))
            prose = []
            if fence:
                fence = None
            else:
                fence = match.group("fence")
                shell_block = match.group("lang").lower() in _SHELL_LANGS
            continue
        if fence:
            if shell_block:
                lines.append(_CodeLine(number, line, runnable=True))
        else:
            prose.append((number, line))
    lines += (_CodeLine(n, c, runnable=False) for n, c in _inline_spans(prose))
    return lines


def _join_continuations(lines: list[_CodeLine]) -> list[_CodeLine]:
    joined: list[_CodeLine] = []
    for entry in lines:
        if joined and joined[-1].code.rstrip().endswith("\\"):
            previous = joined[-1]
            joined[-1] = _CodeLine(
                previous.number,
                previous.code.rstrip()[:-1] + " " + entry.code.strip(),
                previous.runnable,
            )
        else:
            joined.append(entry)
    return joined


def _unmask_or_hide_substitution(match: re.Match[str]) -> str:
    """Unwrap `$(moneybin …)` so its invocation is checked.

    Any other substitution (`$(date +%F)`) stays an opaque `SUBST` so it
    can't split an outer invocation's arguments apart.
    """
    inner = match.group("inner")
    if _SUBSTITUTED_INVOCATION.match(inner):
        return f" {inner.strip()} ; "
    return "SUBST"


def _optional_segment(match: re.Match[str]) -> str:
    """`[--yes]` keeps its option; any other bracket is an optional positional.

    Click's own usage line (`moneybin profile create [OPTIONS] NAME`, as the
    generated reference prints it) spells its options as `[OPTIONS]`, which
    names no positional at all.
    """
    inner = match.group(1)
    if inner.startswith("-"):
        return inner
    return "" if inner == "OPTIONS" else "<optional>"


def _invocations(code: str) -> list[list[str]]:
    """Every `moneybin …` token list found in one line of code."""
    code = _TRAILING_COMMENT.sub(r"\1", code)
    code = _COMMAND_SUBSTITUTION.sub(_unmask_or_hide_substitution, code)
    code = _ANGLE_PLACEHOLDER.sub(lambda m: m.group(0).replace(" ", "_"), code)
    code = _OPTIONAL_SEGMENT.sub(_optional_segment, code)
    found: list[list[str]] = []
    for match in _INVOCATION_START.finditer(code):
        rest = code[match.end() :]
        opener = code[match.start() - 1] if match.start() else ""
        if opener in {"'", '"'} and opener in rest:
            rest = rest[: rest.index(opener)]
        elif opener == "(" and ")" in rest:
            rest = rest[: rest.index(")")]
        for quote in ('"', "'"):  # a multi-line string: stop where it opens
            if rest.count(quote) % 2:
                # The cut string may have been an option's value (`--sql
                # "SELECT …`): an elision keeps that option from reading as
                # the last token in the invocation with nothing following.
                rest = rest[: rest.index(quote)] + " ..."
        try:
            tokens = shlex.split(rest, posix=True)
        except ValueError:
            tokens = rest.replace("'", "").replace('"', "").split()
        command: list[str] = []
        for token in tokens:
            if (
                token in _TERMINATORS
                or token.startswith("#")
                or _REDIRECTION.match(token)
            ):
                break
            command.append(token if token in _ELISIONS else token.rstrip(";,.)"))
            if token.endswith(";"):  # `db lock; moneybin db unlock`
                break
        found.append(command)
    return found


def _is_placeholder(token: str) -> bool:
    return (
        token.startswith(("<", "…", "...", "*"))
        or token in {"ARGS", "COMMAND", "SUBCOMMAND", "SUBST"}
        or (token.isupper() and len(token) > 1)
    )


def _names_a_command_slot(placeholder: str) -> bool:
    return any(word in placeholder.lower() for word in ("command", "group", "verb"))


def _expand_alternatives(token: str) -> list[str]:
    brace = _BRACE_ALTERNATIVES.match(token)
    if brace:
        return [part.strip() for part in brace.group(1).split(",")]
    if "|" in token:
        return token.split("|")
    if "/" in token and not _PATH_LIKE.search(token):
        return token.split("/")
    return [token]


def _resolve_invocation(
    tokens: list[str], root: click.Command, *, runnable: bool
) -> str | None:
    """Return why the invocation does not resolve, or None when it does.

    A runnable invocation (from a fenced shell block — a transcript that must
    run as written) is held to four additional checks a mention (an inline
    span or table row naming a command) is not: a group requiring a
    subcommand must get one, a value-taking option must get a value (and gets
    the very next token regardless of its shape, matching Click's own greedy
    consumption), a required positional argument must actually be present
    (a required variadic counts as needing one), and every required option
    must be seen.
    """
    node = root
    path = ["moneybin"]
    parents: list[tuple[click.Command, list[str]]] = []
    positionals = 0
    options_done = False
    saw_help = False
    seen_options: set[click.Option] = set()
    index = 0
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if token in {"--help", ""}:  # noqa: S105  # CLI argument, not a secret
            saw_help = saw_help or token == "--help"  # noqa: S105  # ditto
            continue
        if token == "--":  # noqa: S105  # end of options: the rest are positionals
            options_done = True
            continue
        if token == "/":  # noqa: S105  # `db init / lock / unlock`: leaf siblings
            if parents:
                node, path = parents.pop()
                positionals = 0
                seen_options = set()
            continue
        if token in {"*", "…", "..."}:  # noqa: S105  # CLI argument, not a secret
            return None  # a wildcard or elision: nothing checkable past it
        if (
            token.startswith("-")
            and len(token) > 1
            and not token[1].isdigit()  # `-12.50` is a value, not an option
            and not options_done
        ):
            name, has_inline_value = token.split("=", 1)[0], "=" in token
            aliases = name.split("/")  # `--refresh/--no-refresh`: each must exist
            if any("<" in alias for alias in aliases):
                continue  # `--clear-<field>`: a placeholder for an option family
            options = [p for p in node.params if isinstance(p, click.Option)]
            declared = {
                alias for p in options for alias in (*p.opts, *p.secondary_opts)
            }
            unknown = [alias for alias in aliases if alias not in declared]
            if unknown:
                return f"`{' '.join(path)}` does not accept option `{unknown[0]}`"
            option = next(
                p for p in options if aliases[0] in (*p.opts, *p.secondary_opts)
            )
            seen_options.add(option)
            if (
                runnable
                and not option.is_flag
                and not has_inline_value
                and index >= len(tokens)
            ):
                return f"`{' '.join(path)}` option `{name}` requires a value"
            following = tokens[index] if index < len(tokens) else ""
            # A bare value-taking option in an inline mention (`--pattern`,
            # `--match-type {exact,contains,regex}`) must not swallow the next
            # flag; a negative number (`--amount -12.50`) is still a value. A
            # *runnable* transcript gets no such benefit of the doubt: Click
            # itself always consumes the very next token as the value,
            # whatever it looks like.
            looks_like_option = (
                following.startswith("-") and not following[1:2].isdigit()
            )
            takes_next_token = following != "" and (runnable or not looks_like_option)
            if not option.is_flag and not has_inline_value and takes_next_token:
                index += 1  # the option's value
            continue
        if _is_placeholder(token):
            if isinstance(node, click.Group):
                if node is root or _names_a_command_slot(token):
                    return None  # `moneybin <command>`: nothing checkable
                return f"`{' '.join(path)}` takes a subcommand, not `{token}`"
            positionals += 1
        elif isinstance(node, click.Group):
            alternatives = _expand_alternatives(token)
            unknown = [alt for alt in alternatives if alt not in node.commands]
            if unknown:
                return f"`{' '.join(path)}` has no subcommand `{unknown[0]}`"
            if len(alternatives) > 1:
                return None  # `{a,b}` at a command position: each exists
            parents.append((node, list(path)))
            node = node.commands[token]
            path.append(token)
            positionals = 0
            seen_options = set()
            continue
        else:
            positionals += 1
        arguments = [p for p in node.params if isinstance(p, click.Argument)]
        capacity = sum(
            float("inf") if argument.nargs < 0 else argument.nargs
            for argument in arguments
        )
        if positionals > capacity:
            return f"`{' '.join(path)}` takes no positional `{token}`"
    # A concrete group with no callback of its own (`invoke_without_command`
    # unset) errors when invoked bare, e.g. `db key` — only `db key show`
    # works. `moneybin <command>` at the root is a generic reference, not an
    # invocation, so root is exempt; `--help` anywhere satisfies the
    # subcommand requirement (`moneybin db --help` is valid).
    if (
        runnable
        and not saw_help
        and isinstance(node, click.Group)
        and node is not root
        and not getattr(node, "invoke_without_command", False)
    ):
        return f"`{' '.join(path)}` takes a subcommand"
    # A runnable invocation that stops before a required positional arrives
    # (`moneybin db key import` with no path) exits non-zero, same as the
    # subcommand and option-value checks above. A required variadic
    # (`FILE_PATHS...` `[required]`) still needs at least one.
    if runnable and not saw_help and not isinstance(node, click.Group):
        arguments = [p for p in node.params if isinstance(p, click.Argument)]
        minimum = sum(
            (1 if argument.required else 0)
            if argument.nargs < 0
            else (argument.nargs if argument.required else 0)
            for argument in arguments
        )
        if positionals < minimum:
            return (
                f"`{' '.join(path)}` requires {minimum} positional argument(s), "
                f"got {positionals}"
            )
        missing_options = [
            option
            for option in node.params
            if isinstance(option, click.Option)
            and option.required
            and option not in seen_options
        ]
        if missing_options:
            return (
                f"`{' '.join(path)}` is missing required option "
                f"`{missing_options[0].opts[0]}`"
            )
    return None


def test_public_docs_cli_invocations_resolve() -> None:
    """Every `moneybin …` in a user-facing doc names a registered command.

    Registered means the group and subcommand exist and every option is one
    the command declares; placeholders are not checked. A fenced shell block
    is a transcript that must run as written, so it is additionally held to a
    subcommand, a required option, and a required positional (including a
    required variadic) actually being present, and a value-taking option
    greedily consuming the next token exactly as Click does; an inline span
    or table row names a command and is exempt from those four. A line that
    must show a wrong invocation on purpose (an error example) carries
    ``<!-- cli-invocation-ok: <reason> -->``.
    """
    from typer.main import get_command

    from moneybin.cli.main import app

    root = get_command(app)
    assert isinstance(root, click.Group)
    violations: list[str] = []
    for document in _user_facing_documents():
        text = document.read_text()
        allowed_lines = {
            number
            for number, line in enumerate(text.splitlines(), start=1)
            if _CLI_INVOCATION_MARKER in line
        }
        for entry in _join_continuations(_code_lines(text)):
            if entry.number in allowed_lines:
                continue
            for tokens in _invocations(entry.code):
                problem = _resolve_invocation(tokens, root, runnable=entry.runnable)
                if problem:
                    relative = document.relative_to(_REPO_ROOT)
                    violations.append(f"{relative}:{entry.number}: {problem}")

    assert not violations, (
        "Public docs cite CLI invocations that do not resolve against the "
        "registered command tree (run `uv run moneybin <group> --help`). Fix the "
        "doc, or mark a deliberate error example with "
        "`<!-- cli-invocation-ok: reason -->`.\n" + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# CLI-invocation guard parser: unit tests against a synthetic fixture
# ---------------------------------------------------------------------------
#
# Independent of the real MoneyBin CLI and the current doc corpus: correctness
# here does not depend on the corpus happening to exercise a given code path.


def _fixture_callback() -> None:
    """Shared no-op callback: these commands are inspected, never invoked."""


@pytest.fixture
def _fixture_cli() -> click.Group:  # pyright: ignore[reportUnusedFunction]  # pytest fixture
    """A small synthetic command tree exercising the resolver's checks.

    Built from plain Click objects (not Typer's decorator sugar, and not
    nested `def`s pyright can't see used) — a fixture only ever inspected,
    never invoked.
    """
    db = click.Group(
        "db",
        commands={"show": click.Command("show", callback=_fixture_callback)},
        # No `invoke_without_command`: a bare invocation requires a subcommand.
    )
    inbox = click.Group(
        "inbox",
        commands={"list": click.Command("list", callback=_fixture_callback)},
        invoke_without_command=True,  # Runs its own callback bare.
        callback=_fixture_callback,
    )
    create = click.Command(
        "create",
        callback=_fixture_callback,
        params=[
            click.Argument(["name"]),
            click.Argument(["amount"], required=False),
        ],
    )
    commit = click.Command(
        "commit",
        callback=_fixture_callback,
        params=[click.Option(["-y", "--yes"], is_flag=True)],
    )
    rules = click.Command(
        "rules", callback=_fixture_callback, params=[click.Option(["--pattern"])]
    )
    secret = click.Command("secret", callback=_fixture_callback, hidden=True)
    deploy = click.Command(
        "deploy",
        callback=_fixture_callback,
        params=[click.Option(["--target"], required=True)],
    )
    files = click.Command(
        "files",
        callback=_fixture_callback,
        params=[click.Argument(["file_paths"], nargs=-1, required=True)],
    )
    return click.Group(
        "cli",
        commands={
            "db": db,
            "inbox": inbox,
            "create": create,
            "commit": commit,
            "rules": rules,
            "secret": secret,
            "deploy": deploy,
            "files": files,
        },
    )


@pytest.mark.parametrize(
    ("tokens", "runnable", "expected_ok"),
    [
        # Bare group requiring a subcommand: strict only when runnable.
        pytest.param(["db"], True, False, id="bare-group-runnable-violation"),
        pytest.param(["db"], False, True, id="bare-group-inline-ok"),
        # `--help` anywhere satisfies the subcommand requirement, either way.
        pytest.param(["db", "--help"], True, True, id="group-help-runnable-ok"),
        pytest.param(["db", "--help"], False, True, id="group-help-inline-ok"),
        # A group with its own callback (`invoke_without_command=True`) never
        # needs a subcommand.
        pytest.param(["inbox"], True, True, id="invoke-without-command-ok"),
        # A value-taking option at the very end: strict only when runnable.
        pytest.param(
            ["rules", "--pattern"], True, False, id="option-value-runnable-violation"
        ),
        pytest.param(["rules", "--pattern"], False, True, id="option-value-inline-ok"),
        # A missing required positional: strict only when runnable.
        pytest.param(
            ["create"], True, False, id="missing-positional-runnable-violation"
        ),
        pytest.param(["create"], False, True, id="missing-positional-inline-ok"),
        # The optional positional may be omitted either way.
        pytest.param(
            ["create", "foo"], True, True, id="optional-positional-omitted-ok"
        ),
        # A flag never needs a value, regardless of position.
        pytest.param(["commit", "-y"], True, True, id="flag-no-value-needed"),
        # Negative number is a value, not an option.
        pytest.param(
            ["create", "foo", "-12.50"], True, True, id="negative-number-is-value"
        ),
        # A missing required option: strict only when runnable.
        pytest.param(["deploy"], True, False, id="missing-required-option-runnable"),
        pytest.param(["deploy"], False, True, id="missing-required-option-inline-ok"),
        pytest.param(
            ["deploy", "--target", "prod"], True, True, id="required-option-present"
        ),
        # A runnable transcript's value-taking option greedily takes the very
        # next token, exactly like Click — even one shaped like another
        # option; a mention still catches the apparent typo.
        pytest.param(
            ["rules", "--pattern", "--bogus"],
            True,
            True,
            id="option-value-greedy-runnable-ok",
        ),
        pytest.param(
            ["rules", "--pattern", "--bogus"],
            False,
            False,
            id="option-value-not-greedy-inline-violation",
        ),
        # A required variadic still needs at least one value.
        pytest.param(["files"], True, False, id="required-variadic-empty-violation"),
        pytest.param(["files"], False, True, id="required-variadic-inline-ok"),
        pytest.param(
            ["files", "a.csv"], True, True, id="required-variadic-one-value-ok"
        ),
    ],
)
def test_resolve_invocation_runnable_scoping(
    _fixture_cli: click.Group,
    tokens: list[str],
    runnable: bool,
    expected_ok: bool,
) -> None:
    """A fenced shell block is strict; an inline mention or table row is not."""
    problem = _resolve_invocation(tokens, _fixture_cli, runnable=runnable)
    assert (problem is None) == expected_ok, problem


def test_invocations_redirection_terminates() -> None:
    """`2>&1 | tee log` ends the invocation right after the command."""
    (command,) = _invocations("moneybin db show 2>&1 | tee log")
    assert command == ["db", "show"]


def test_invocations_semicolon_chain_finds_both() -> None:
    """A `;`-chained pair yields two separate, independently checkable commands."""
    commands = _invocations("moneybin db show; moneybin create x")
    assert commands == [["db", "show"], ["create", "x"]]


def test_resolve_invocation_elision_short_circuits(_fixture_cli: click.Group) -> None:
    """An elision (`...`) stops the check before a trailing bogus flag."""
    (command,) = _invocations("moneybin commit ... --bogus")
    assert command == ["commit", "...", "--bogus"]
    assert _resolve_invocation(command, _fixture_cli, runnable=True) is None


def test_invocations_unwraps_moneybin_substitution() -> None:
    """`$(moneybin …)` is unwrapped so the inner invocation is checked."""
    (command,) = _invocations("KEY=$(moneybin db show)")
    assert command == ["db", "show"]


def test_invocations_masks_non_moneybin_substitution() -> None:
    """`$(date …)` stays an opaque token — it never splits the outer command."""
    (command,) = _invocations("moneybin db show -o backup-$(date +%F).db")
    assert command == ["db", "show", "-o", "backup-SUBST.db"]


def test_invocations_elides_unterminated_quote_value(_fixture_cli: click.Group) -> None:
    """An unterminated quote (a multi-line string) elides rather than vanishing.

    Cutting the value away entirely would leave `--pattern` as the last token
    with nothing following it, which reads as a truncated invocation.
    """
    (command,) = _invocations('moneybin rules --pattern "SELECT * FROM')
    assert command == ["rules", "--pattern", "..."]
    assert _resolve_invocation(command, _fixture_cli, runnable=True) is None


def test_resolve_invocation_placeholder_then_bogus_flag(
    _fixture_cli: click.Group,
) -> None:
    (command,) = _invocations("moneybin create <name> --bogus")
    assert _resolve_invocation(command, _fixture_cli, runnable=False) is not None


def test_resolve_invocation_bracketed_bogus_option(_fixture_cli: click.Group) -> None:
    (command,) = _invocations("moneybin create <name> [--bogus]")
    assert _resolve_invocation(command, _fixture_cli, runnable=False) is not None


def test_invocations_drop_click_usage_options_placeholder(
    _fixture_cli: click.Group,
) -> None:
    """Click's `[OPTIONS]` names no positional; `[--yes]` still keeps its option."""
    (command,) = _invocations("moneybin create [OPTIONS] NAME [AMOUNT]")
    assert command == ["create", "NAME", "<optional>"]
    assert _resolve_invocation(command, _fixture_cli, runnable=False) is None
    (flagged,) = _invocations("moneybin commit [--yes]")
    assert flagged == ["commit", "--yes"]


def test_resolve_invocation_root_dash_h_unregistered(_fixture_cli: click.Group) -> None:
    (command,) = _invocations("moneybin -h")
    assert _resolve_invocation(command, _fixture_cli, runnable=True) is not None


def test_code_lines_reads_soft_wrapped_span(_fixture_cli: click.Group) -> None:
    """A code span that soft-wraps across a line is still read as one span."""
    text = "run `moneybin\ncommit --bogus` now"
    (entry,) = _code_lines(text)
    assert not entry.runnable
    (command,) = _invocations(entry.code)
    assert (
        _resolve_invocation(command, _fixture_cli, runnable=entry.runnable) is not None
    )


def test_full_pipeline_scopes_fenced_block_vs_inline_mention(
    _fixture_cli: click.Group,
) -> None:
    """End to end: the same bare-group text is strict fenced, lenient inline."""
    text = (
        "The `db` group manages the database; bare `moneybin db` is shown here "
        "only as a name, not something to run.\n"
        "\n"
        "```bash\n"
        "moneybin db\n"
        "```\n"
    )
    entries = _join_continuations(_code_lines(text))
    inline_problems = [
        _resolve_invocation(tokens, _fixture_cli, runnable=False)
        for entry in entries
        if not entry.runnable
        for tokens in _invocations(entry.code)
    ]
    fenced_problems = [
        _resolve_invocation(tokens, _fixture_cli, runnable=True)
        for entry in entries
        if entry.runnable
        for tokens in _invocations(entry.code)
    ]
    assert inline_problems == [None]
    assert fenced_problems and all(problem is not None for problem in fenced_problems)


# ---------------------------------------------------------------------------
# Generated reference pages match the code they are rendered from
# ---------------------------------------------------------------------------


def test_generated_references_are_current() -> None:
    """`docs/reference/cli/`, `mcp-tools.md`, and `configuration.md` are current.

    scripts/generate_reference_docs.py renders them from the command tree,
    the MCP server's tool list, and the settings model; a change to any of those
    that leaves the pages stale fails here. Run `make generate-docs` and
    commit the result.
    """
    from scripts.generate_reference_docs import stale_pages

    stale = stale_pages()
    assert not stale, (
        "Generated reference pages are stale; run `make generate-docs` and "
        "commit the result:\n" + "\n".join(str(page) for page in stale)
    )


# ---------------------------------------------------------------------------
# Refresh-cascade spellings in public docs match the runtime order
# ---------------------------------------------------------------------------


def test_public_docs_refresh_cascades_match_runtime() -> None:
    """Every `a → b → c` chain of refresh steps in a user-facing doc is canonical.

    A chain must list steps in the runtime's order; one that starts at the
    first step, or spans four or more, must run to the last step. A chain of a
    few middle steps stays free to describe a partial run. The derived literals
    in test_mcp_surface_docs.py pin the pipeline guide's own summary; this scans
    every other spelling, so a step appended to the cascade cannot leave a guide
    stale and green.
    """
    from moneybin.orchestration.refresh import CANONICAL_STEPS

    step = "|".join(CANONICAL_STEPS)
    chain = re.compile(rf"\b(?:{step})(?: → (?:{step}))+\b")
    violations: list[str] = []
    for document in _user_facing_documents():
        for number, line in enumerate(document.read_text().splitlines(), start=1):
            for found in chain.findall(line):
                names = found.split(" → ")
                order = [CANONICAL_STEPS.index(name) for name in names]
                in_order = order == sorted(set(order))
                must_finish = names[0] == CANONICAL_STEPS[0] or len(names) >= 4
                finished = names[-1] == CANONICAL_STEPS[-1]
                if not in_order or (must_finish and not finished):
                    relative = document.relative_to(_REPO_ROOT)
                    violations.append(f"{relative}:{number}: `{found}`")
    assert not violations, (
        "Public docs spell a refresh cascade that does not match "
        f"`{' → '.join(CANONICAL_STEPS)}`:\n" + "\n".join(violations)
    )
