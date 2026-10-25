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
from moneybin.privacy.disclosure import host_for_recipient_name, known_host_names
from moneybin.privacy.payloads.consent import ConsentMutationPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

from ._presentation import emit_receipt, require_confirmation

logger = logging.getLogger(__name__)


def privacy_revoke(
    category: str = typer.Argument(..., help="Feature category to revoke"),
    backend: str | None = typer.Option(None, "--backend", help="AI backend"),
    recipient: str | None = typer.Option(
        None, "--recipient", help="AI app whose remembered consent to revoke"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Revoke consent for <category>; takes effect immediately."""
    if recipient is not None:
        if backend is not None:
            raise typer.BadParameter(
                "--recipient and --backend are mutually exclusive."
            )
        host = host_for_recipient_name(recipient)
        if host is None:
            raise typer.BadParameter(
                f"Unknown recipient {recipient!r}. "
                f"Known recipients: {', '.join(known_host_names())}."
            )
        with handle_cli_errors(
            cli_actor="privacy_revoke", payload_type=ConsentMutationPayload
        ):
            ConsentService.validate_category(category)
            key = host.recipient_key or ""
            if not require_confirmation(
                yes=yes,
                output=output,
                prompt=f"Revoke consent for '{category}' (recipient '{key}')?",
            ):
                emit_receipt(
                    "Consent revoke cancelled", [("Outcome", "No consent was revoked")]
                )
                raise typer.Exit(0)
            with get_database(read_only=False) as db:
                host_result = ConsentService(db).revoke_host_consent(
                    recipient_key=key, actor="cli.privacy_revoke"
                )
        host_payload = ConsentMutationPayload(
            feature_category=category,
            backend=host_result.backend,
            consent_mode=None,
            action="revoked" if host_result.count else "noop",
        )
        if output == OutputFormat.JSON:
            render_or_json(
                build_envelope(data=host_payload), output, cli_actor="privacy_revoke"
            )
            return
        emit_receipt(
            "Consent revoked" if host_result.count else "Consent unchanged",
            [
                ("Category", category),
                ("Recipient", host_result.backend),
                (
                    "Outcome",
                    "Revoked"
                    if host_result.count
                    else "No active consent found; no change made",
                ),
            ],
        )
        return
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
