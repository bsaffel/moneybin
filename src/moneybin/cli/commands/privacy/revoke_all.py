"""`moneybin privacy revoke-all` — revoke every active consent grant."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.consent import ConsentRevokeAllPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

logger = logging.getLogger(__name__)


def privacy_revoke_all(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Revoke ALL active AI consent grants."""
    if not yes:
        typer.confirm("Revoke ALL active consent grants?", abort=True)
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            count = ConsentService(db).revoke_all(actor="cli.privacy_revoke_all")
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=ConsentRevokeAllPayload(revoked_count=count)),
            output,
            cli_actor="privacy_revoke_all",
        )
        return
    if count:
        logger.info(f"✅ Revoked {count} consent grant(s).")
    else:
        logger.info("No active consent grants to revoke.")
