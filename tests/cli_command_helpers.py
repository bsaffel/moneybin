"""Resolve the ``moneybin ...`` commands a user-facing message publishes.

MoneyBin prints recovery commands inside warnings, hints, and doctor findings.
An unregistered name exits 2 with a usage error, and these messages are printed
exactly when something has already gone wrong — so a dead end there lands at
the worst possible moment. Four such strings shipped in one change (``moneybin
audit``, ``moneybin audit undo``, and ``moneybin reviews`` twice; the registered
spellings are ``moneybin system audit …`` and ``moneybin review``).

The helpers resolve what the producing code actually emitted rather than
asserting on literals: reword a message freely, but never publish a command the
CLI does not have.
"""

from __future__ import annotations

import re
import shlex

from typer.testing import CliRunner

# Quoted or backticked — the producing sites use both.
_INVOCATION = re.compile(r"['\"`]moneybin ([a-z][^'\"`]*)['\"`]")

# A value the prose expects the user to substitute, not a command token.
_PLACEHOLDER = re.compile(r"^[<{].*[>}]$")


def moneybin_invocations(text: str) -> list[list[str]]:
    """Argument lists for every ``moneybin ...`` command quoted in ``text``."""
    invocations: list[list[str]] = []
    for quoted in _INVOCATION.findall(text):
        args = [tok for tok in shlex.split(quoted) if not _PLACEHOLDER.match(tok)]
        if args:
            invocations.append(args)
    return invocations


def assert_published_commands_resolve(text: str) -> None:
    """Fail if ``text`` publishes a command the CLI does not register.

    Appends ``--help`` so registration is what is under test rather than the
    command's runtime behaviour: an unregistered name exits 2 either way, and a
    registered one prints its help without touching a database. Empty input is a
    failure, not a pass — a guard whose predicate never ran is indistinguishable
    from one that held.
    """
    from moneybin.cli.main import app  # noqa: PLC0415 — keep collection-time light

    invocations = moneybin_invocations(text)
    assert invocations, f"no `moneybin ...` command published in {text!r}"
    runner = CliRunner()
    for args in invocations:
        result = runner.invoke(app, [*args, "--help"])
        assert result.exit_code == 0, (
            f"published command `moneybin {' '.join(args)}` does not resolve "
            f"(exit {result.exit_code}): {result.output}"
        )
