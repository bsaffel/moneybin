"""`moneybin privacy revoke` — revoke consent for a feature category."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.consent import ConsentMutationPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

logger = logging.getLogger(__name__)


def privacy_revoke(
    category: str = typer.Argument(..., help="Feature category to revoke"),
    backend: str | None = typer.Option(None, "--backend", help="AI backend"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Revoke consent for <category>; takes effect immediately."""
    if not yes:
        typer.confirm(f"Revoke consent for '{category}'?", abort=True)
    with handle_cli_errors():
        with get_database() as db:
            result = ConsentService(db).revoke_consent(
                feature_category=category, backend=backend, actor="cli.privacy_revoke"
            )
    payload = ConsentMutationPayload(
        feature_category=category,
        backend=result.backend,
        consent_mode=None,
        action="revoked" if result.count else "noop",
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="privacy_revoke")
        return
    if result.count:
        logger.info(
            f"✅ Revoked consent for '{category}' (backend '{result.backend}')."
        )
    else:
        logger.info(f"No active consent found for '{category}'.")
