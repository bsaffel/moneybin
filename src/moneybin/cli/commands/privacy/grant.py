"""`moneybin privacy grant` — record consent to share an AI feature category."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.consent import ConsentMode
from moneybin.privacy.payloads.consent import ConsentMutationPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

logger = logging.getLogger(__name__)


def privacy_grant(
    category: str = typer.Argument(..., help="Feature category, e.g. mcp-data-sharing"),
    backend: str | None = typer.Option(
        None, "--backend", help="AI backend (defaults to MONEYBIN_AI__DEFAULT_BACKEND)"
    ),
    mode: ConsentMode = typer.Option(
        ConsentMode.PERSISTENT, "--mode", help="persistent or one-time"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt"),
    output: OutputFormat = output_option,
) -> None:
    """Grant consent to share <category> data with an AI backend.

    Account numbers and other CRITICAL fields always remain masked.
    """
    if not yes:
        typer.confirm(
            f"Grant consent to share '{category}' with backend "
            f"'{backend or 'default'}'?",
            abort=True,
        )
    with handle_cli_errors():
        with get_database() as db:
            grant = ConsentService(db).grant_consent(
                feature_category=category,
                backend=backend,
                consent_mode=mode,
                actor="cli.privacy_grant",
            )
    payload = ConsentMutationPayload(
        feature_category=grant.feature_category,
        backend=grant.backend,
        consent_mode=grant.consent_mode.value,
        action="granted",
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="privacy_grant")
        return
    logger.info(
        f"✅ Granted '{grant.feature_category}' for backend '{grant.backend}' "
        f"({grant.consent_mode.value})."
    )
