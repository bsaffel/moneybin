"""Structural guardrail: no `# noqa: PLC…` marker, because none of them suppress.

Ruff's `select` in `pyproject.toml` does not list `PL`, so every Pylint-family
code is disabled and a `noqa` citing one never silenced a diagnostic. MB-168
removed 363 of them. They were not harmless: because no linter ever evaluated
the justifications riding on them, 17 comments claiming a deferred import
avoided a circular import were wrong, and an audit took one of those claims at
face value and had to be retracted. This guard stops the next one appearing.

**Scope.** A plain text scan for the marker across `src/`, `tests/`, and
`scripts/`. It deliberately does not reason about whether a deferred import is
justified — only that it is not justified by citing a rule that is switched
off. Correcting a wrong justification is a review question, not a test.
"""

import pathlib
import re
import subprocess  # noqa: S404 — asks ruff for its own resolved rule set
import sys
from collections.abc import Iterator

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Any Pylint-family code, not just PLC0415: the whole `PL` prefix is disabled,
# so a marker naming PLR/PLW/PLE is equally inert.
MARKER = re.compile(r"#\s*noqa:[^\n]*\bPL[CREW]\d+", re.IGNORECASE)

SCANNED_TREES = ("src", "tests", "scripts")


def _python_files() -> Iterator[pathlib.Path]:
    for tree in SCANNED_TREES:
        yield from sorted((REPO_ROOT / tree).rglob("*.py"))


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
