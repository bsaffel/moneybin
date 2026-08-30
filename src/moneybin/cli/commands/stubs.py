"""Stub commands for features not yet implemented.

These reserve the CLI namespace so a future release can fill it in without
breaking a script that already calls the path. Stubs are hidden from ``--help``
(cli-output-coherence req 31) so the CLI never advertises what it cannot do.
"""

import typer

__all__ = ["_not_implemented"]


def _not_implemented(feature: str) -> None:
    """Print a not-implemented message and return cleanly.

    Args:
        feature: The capability in user vocabulary, as a lowercase noun phrase
            ("budget targets"). Never a spec filename or repo path — an
            installed user has no checkout to open (req 32).

    Exit code policy: stubs return 0, not 1. Per `.claude/rules/cli.md`,
    exit code 1 means "runtime error" (operation ran and failed) — using
    it for "intentional no-op pending implementation" would collide with
    that meaning and force scripts to distinguish stubs from real
    failures via stderr text. The "ran but unimplemented" signal is
    delivered by the message below rather than the exit code.

    Written with `typer.echo(err=True)`, not `logger.warning`, so the message is
    level-independent. `ERROR` and `CRITICAL` are supported `LoggingConfig.level`
    values that drop a WARNING record entirely, which would leave the `db key`
    trio — the stubs that exit 1 — reporting a bare failure code with no reason,
    the exact gap req 32 exists to close. Those three reached the user through an
    unconditional `typer.echo` before this spec, so routing them onto a logger
    would have been a regression for a level range they were immune to.

    The cost is that stub invocations no longer reach the log file. That is the
    tradeoff `.claude/rules/cli.md` already accepts for a line a `typer.echo`
    carries, and it applies to every stub rather than special-casing `db key`.
    """
    typer.echo(
        f"⚠️  This command is not yet implemented. Support for {feature} is "
        "planned — run `moneybin --help` for what works today.",
        err=True,
    )
