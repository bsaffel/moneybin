"""Privacy utilities: redaction, consent, and audit commands."""

import logging

import typer

from .redact import privacy_redact

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Privacy utilities: redaction testing, consent, and audit",
    no_args_is_help=True,
)

app.command("redact")(privacy_redact)
