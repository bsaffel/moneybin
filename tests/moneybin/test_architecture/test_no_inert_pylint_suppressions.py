"""Structural guardrail: no `# noqa: PLC…` marker, because none of them suppress.

Ruff's `select` in `pyproject.toml` does not list `PL`, so every Pylint-family
code is disabled and a `noqa` citing one never silenced a diagnostic. MB-168
removed 363 of them. They were not harmless: because no linter ever evaluated
the justifications riding on them, 17 comments claiming a deferred import
avoided a circular import were wrong, and an audit took one of those claims at
face value and had to be retracted. This guard stops the next one appearing.

**Scope.** A plain text scan for the marker across every tracked Python file,
which is the surface CI lints with `ruff check .`. It deliberately does not
reason about whether a deferred import is justified — only that it is not
justified by citing a rule that is switched off. Correcting a wrong
justification is a review question, not a test.

The flake8-compatible file-level spelling is out of scope on purpose, not by
oversight: ruff treats it as a blanket suppression and discards the codes
after it, so it cannot cite a Pylint rule the way this guard polices. A blanket
file-level suppression is a larger problem that belongs to review. None exist
in this repo today.
"""

import pathlib
import re
import subprocess  # noqa: S404 — asks ruff for its own resolved rule set
import sys
from collections.abc import Iterator

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Any Pylint-family code, not just PLC0415: the whole `PL` prefix is disabled,
# so a marker naming PLR/PLW/PLE is equally inert.
#
# Two spellings, both of which ruff honours: the line-level directive, and the
# file-level variant that puts `ruff:` ahead of it. The repo already uses the
# file-level variant for other codes, so omitting it would leave a way back in.
MARKER = re.compile(r"#\s*(?:ruff:\s*)?noqa:[^\n]*\bPL[CREW]\d+", re.IGNORECASE)

# The cases below interpolate these rather than spelling a marker out, so that
# this file does not trip its own scan and need an exemption from it.
_C, _C2, _W, _R = "PLC0415", "PLC2701", "PLW0603", "PLR0913"


def _python_files() -> Iterator[pathlib.Path]:
    """Every tracked Python file, which is the surface CI lints with `ruff check .`.

    Asking git rather than walking a fixed list of directories: a hardcoded
    `("src", "tests", "scripts")` silently omits root-level files such as
    `conftest.py`, so a marker could re-enter there with this guard still green.
    """
    result = subprocess.run(  # noqa: S603  # fixed argv, no shell, no user input
        ["git", "ls-files", "*.py"],  # noqa: S607  # git resolves from PATH
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert result.returncode == 0, f"git ls-files failed: {result.stderr}"

    paths = [REPO_ROOT / line for line in result.stdout.split()]
    # An empty listing must never read as "nothing to check" — that would make
    # this guard pass vacuously anywhere git is unavailable.
    assert len(paths) > 100, f"expected the whole tracked tree, got {len(paths)} files"
    return iter(paths)


def test_pylint_family_is_not_enabled() -> None:
    """The premise. If `PL` is ever enabled, delete this guard rather than fight it.

    Without this, enabling `PL` would leave the guard below still passing while
    its stated reason had quietly become false — the exact failure mode MB-168
    exists to correct.

    Ruff is asked for its *resolved* rule set rather than having `select`,
    `extend-select` and `ignore` re-implemented here. Those interact by prefix
    specificity — `select = ["ALL"]` with `ignore = ["PL"]` leaves the family
    off, while `select = ["PLC0415"]` with `ignore = ["PL"]` leaves that one on
    — and a hand-rolled model of that gets the answer wrong in both directions.
    """
    ruff = pathlib.Path(sys.executable).parent / "ruff"
    result = subprocess.run(  # noqa: S603  # fixed argv, no shell, no user input
        [str(ruff), "check", "--show-settings", "src/moneybin/config.py"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert result.returncode == 0, f"ruff --show-settings failed: {result.stderr}"

    block = result.stdout.partition("linter.rules.enabled = [")[2].partition("]")[0]
    codes = re.findall(r"\(([A-Z]+\d+)\)", block)

    # Fail loudly if the output shape changed. Parsing nothing must never read as
    # "no Pylint rule is enabled" — that is this guard's own failure mode.
    assert codes, (
        "could not read `linter.rules.enabled` out of `ruff check "
        "--show-settings`; its output shape changed, so this premise check is "
        "no longer measuring anything. Fix the parse before trusting it."
    )

    offenders = sorted({c for c in codes if c.startswith("PL")})

    assert not offenders, (
        f"ruff now enables a Pylint-family rule ({', '.join(offenders)}), so a "
        "`noqa` citing one can suppress a real diagnostic. This guard's premise "
        "no longer holds — delete it instead of adding exemptions."
    )


@pytest.mark.parametrize(
    "line",
    [
        f"import json  # noqa: {_C}",
        f"import json  # noqa: {_C} — defer import",
        f"from x import (  # noqa: {_C}  # polars is not cold-start cheap",
        f"import json  # noqa: {_C},{_C2}",
        f"    global _cache  # noqa: {_W}",
        f"def f(  # noqa: {_R}",
        f"# ruff: noqa: {_C}",
        f"#ruff:noqa:{_C}",
        f"import json  # NOQA: {_C.lower()}",
    ],
)
def test_marker_matches_every_spelling_it_must_catch(line: str) -> None:
    """The scan below only ever asserts an empty list, so it cannot prove the regex works.

    Without these, swapping `MARKER` for one that never matches leaves the whole
    guard green — a check whose predicate never fires is indistinguishable from
    one that passed (`.claude/rules/testing.md`, "A Fixture That Never Reaches
    the Predicate Proves Nothing").
    """
    assert MARKER.search(line), f"marker not detected in: {line}"


@pytest.mark.parametrize(
    "line",
    [
        "import subprocess  # noqa: S404 — a rule that is actually enabled",
        "except Exception:  # noqa: BLE001",
        "# ruff: noqa: S101",
        "sql = 'SELECT 1'  # noqa: S608  # TableRef constant",
        f"# {_C} is not enabled, which is why this comment is prose",
        "import json  # deferred: module-scope import would cycle",
    ],
)
def test_marker_leaves_everything_else_alone(line: str) -> None:
    """A live suppression, or prose merely naming a code, must not be flagged."""
    assert not MARKER.search(line), f"false positive on: {line}"


def test_no_inert_pylint_suppression_markers() -> None:
    """No file cites a Pylint code in a noqa, since none of them are enabled."""
    offenders: list[str] = []
    for path in _python_files():
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if MARKER.search(line):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")

    assert not offenders, (
        "These `# noqa: PL…` markers suppress nothing — the `PL` rules are not "
        "enabled. Drop the marker; if the deferred import needs explaining, "
        "write a plain comment saying why:\n  " + "\n  ".join(offenders)
    )
