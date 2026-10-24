"""`moneybin privacy revoke` — revoke consent for a feature category."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.consent import ConsentMutationPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

from ._presentation import emit_receipt, require_confirmation

logger = logging.getLogger(__name__)


def privacy_revoke(
    category: str = typer.Argument(..., help="Feature category to revoke"),
    backend: str | None = typer.Option(None, "--backend", help="AI backend"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Revoke consent for <category>; takes effect immediately."""
    # Resolve before prompting so the confirmation names the backend actually
    # being revoked (default-backend resolution happens in the service).
    with handle_cli_errors(
        cli_actor="privacy_revoke", payload_type=ConsentMutationPayload
    ):
        ConsentService.validate_category(category)
        resolved_backend = ConsentService.resolve_backend(backend)
        confirmed = require_confirmation(
            yes=yes,
            output=output,
            prompt=f"Revoke consent for '{category}' (backend '{resolved_backend}')?",
        )
        if not confirmed:
            emit_receipt(
                "Consent revoke cancelled", [("Outcome", "No consent was revoked")]
            )
            raise typer.Exit(0)
        with get_database(read_only=False) as db:
            result = ConsentService(db).revoke_consent(
                feature_category=category,
                backend=resolved_backend,
                actor="cli.privacy_revoke",
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
    emit_receipt(
        "Consent revoked" if result.count else "Consent unchanged",
        [
            ("Category", category),
            ("Backend", result.backend),
            (
                "Outcome",
                "Revoked"
                if result.count
                else "No active consent found; no change made",
            ),
        ],
    )
