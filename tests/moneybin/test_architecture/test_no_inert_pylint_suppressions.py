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
import tomllib
from collections.abc import Iterator
from typing import cast

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
    """
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    lint: dict[str, object] = config["tool"]["ruff"]["lint"]

    # Both keys, because `extend-select` enables rules just as `select` does, and
    # reading only one lets the premise go stale without this test noticing —
    # which is the failure it exists to prevent. `ALL` sweeps in the family too.
    enabled: list[str] = []
    for key in ("select", "extend-select"):
        value = lint.get(key)
        if isinstance(value, list):
            enabled.extend(str(code) for code in cast(list[object], value))

    offenders = [c for c in enabled if c.startswith("PL") or c == "ALL"]

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
