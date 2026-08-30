"""Stub commands for features not yet implemented.

These reserve the CLI namespace so a future release can fill it in without
breaking a script that already calls the path. Stubs are hidden from ``--help``
(cli-output-coherence req 31) so the CLI never advertises what it cannot do.
"""

import logging

logger = logging.getLogger(__name__)

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
    delivered via the logged message (which `setup_logging(cli_mode=True)`
    routes to stderr) rather than the exit code.

    Emitted at WARNING, and as one record rather than two: `WARNING` is a
    supported `LoggingConfig.level`, and at INFO this message would vanish
    there — leaving the `db key` stubs, which exit 1, reporting a bare
    failure code with no reason. Splitting the next action into a second
    INFO record would lose exactly the half req 32 requires.
    """
    logger.warning(
        f"⚠️  This command is not yet implemented. Support for {feature} is "
        "planned — run `moneybin --help` for what works today."
    )
