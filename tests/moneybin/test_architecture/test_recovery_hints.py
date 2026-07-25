"""Structural guardrail: recovery hints must name a command that still works.

`moneybin transactions review` is a deprecated alias (see the shim in
`src/moneybin/cli/commands/transactions/review.py`). Five production messages
told users to run it after a successful matching run, so the product's own
next-step hint printed a deprecation warning and then a not-implemented stub —
the two worst answers a CLI can give, in that order.

A grep-shaped guard is the right shape here because the defect is a *string*:
the hint sites are `logger.info` literals and MCP `actions[]` entries, so no
type, import, or call-graph check can see them. Repointing them is a one-line
edit, which is exactly why the wrong name creeps back.

Scope is `src/moneybin/` — strings the program can print. A line may still name
the alias when it is labelling it *as* deprecated, which is what the shim and
its cross-references do.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = REPO_ROOT / "src" / "moneybin"

DEPRECATED_INVOCATION = "moneybin transactions review"

# The shim and its cross-references have to name what they deprecate. Keying the
# exemption on the word rather than on a file keeps a future hint added to an
# already-exempt module from slipping through.
EXEMPTING_WORD = "deprecat"


def test_no_message_routes_users_to_the_deprecated_review_alias() -> None:
    offenders = [
        f"{path.relative_to(REPO_ROOT)}:{lineno}"
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        )
        if DEPRECATED_INVOCATION in line and EXEMPTING_WORD not in line.lower()
    ]
    assert not offenders, (
        f"These sites name the deprecated `{DEPRECATED_INVOCATION}` alias. "
        f"Point them at `moneybin review` or `moneybin transactions matches "
        f"pending` instead:\n  " + "\n  ".join(offenders)
    )
