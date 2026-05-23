"""Privacy utilities: redaction, consent, and audit commands."""

import logging

import typer

from .grant import privacy_grant
from .log import privacy_log
from .redact import privacy_redact
from .revoke import privacy_revoke
from .revoke_all import privacy_revoke_all
from .status import privacy_status

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Privacy utilities: redaction testing, consent grants, and audit log",
    no_args_is_help=True,
)

app.command("redact")(privacy_redact)
app.command("grant")(privacy_grant)
app.command("revoke")(privacy_revoke)
app.command("revoke-all")(privacy_revoke_all)
app.command("status")(privacy_status)
app.command("log")(privacy_log)
