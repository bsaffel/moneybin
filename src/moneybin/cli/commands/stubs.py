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
    """
    logger.info("This command is not yet implemented.")
    logger.info(
        f"💡 Support for {feature} is planned — "
        "run `moneybin --help` for what works today."
    )
