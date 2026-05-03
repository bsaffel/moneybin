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
