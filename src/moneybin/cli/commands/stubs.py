"""Stub commands for features not yet implemented.

These reserve the CLI namespace and provide clear messages directing
users to the relevant spec or future release. Each stub will be replaced
by a real implementation when its owning spec is executed.
"""

import logging

import typer

logger = logging.getLogger(__name__)

__all__ = ["_not_implemented"]


def _not_implemented(owning_spec: str) -> None:
    """Print a not-implemented message and exit cleanly.

    Args:
        owning_spec: The spec filename under docs/specs/ that owns this feature.

    Exit code policy: stubs return 0, not 1. Per `.claude/rules/cli.md`,
    exit code 1 means "runtime error" (operation ran and failed) — using
    it for "intentional no-op pending implementation" would collide with
    that meaning and force scripts to distinguish stubs from real
    failures via stderr text. The "ran but unimplemented" signal is
    delivered via the logged message (which `setup_logging(cli_mode=True)`
    routes to stderr) rather than the exit code. Revisit if a project-
    wide stub-exit-code policy lands; not changed here to keep the v2
    restructure scoped.
    """
    logger.info("This command is not yet implemented.")
    logger.info(f"💡 See docs/specs/{owning_spec} for the design")


# --- export ---
export_app = typer.Typer(
    help="Export data to CSV, Excel, and other formats", no_args_is_help=True
)


@export_app.command("run")
def export_run() -> None:
    """Export financial data to a file."""
    _not_implemented("export.md")
