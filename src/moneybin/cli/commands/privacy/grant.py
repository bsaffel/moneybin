"""`moneybin privacy grant` — record consent to share an AI feature category."""

from __future__ import annotations

import logging
import sys

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.consent import ConsentMode
from moneybin.privacy.disclosure import (
    host_for_recipient_name,
    known_host_names,
    risk_statement,
)
from moneybin.privacy.payloads.consent import ConsentMutationPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

from ._presentation import emit_receipt, require_confirmation

logger = logging.getLogger(__name__)

_HOST_CATEGORY = "smart-import-parsing"


def _stdin_is_tty() -> bool:
    return sys.stdin.isatty()


def privacy_grant(
    category: str = typer.Argument(..., help="Feature category, e.g. mcp-data-sharing"),
    backend: str | None = typer.Option(
        None, "--backend", help="AI backend (defaults to MONEYBIN_AI__DEFAULT_BACKEND)"
    ),
    recipient: str | None = typer.Option(
        None,
        "--recipient",
        help="AI app that may receive PDF statement content "
        "(claude-ai, claude-code, cursor); interactive only",
    ),
    mode: ConsentMode = typer.Option(
        ConsentMode.PERSISTENT, "--mode", help="persistent or one-time"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Grant consent to share <category> data with an AI backend.

    Account numbers and other CRITICAL fields always remain masked.
    """
    if recipient is not None:
        _grant_recipient(
            category=category,
            recipient=recipient,
            backend=backend,
            mode=mode,
            yes=yes,
            output=output,
        )
        return
    # Resolve + validate before prompting so the user confirms consent for the
    # backend that will actually be recorded — never a placeholder that then
    # errors out after they've already agreed.
    with handle_cli_errors(
        cli_actor="privacy_grant", payload_type=ConsentMutationPayload
    ):
        ConsentService.validate_category(category)
        resolved_backend = ConsentService.resolve_backend(backend)
        confirmed = require_confirmation(
            yes=yes,
            output=output,
            prompt=(
                f"Grant {mode.value} consent to share '{category}' with backend "
                f"'{resolved_backend}'?"
            ),
        )
        if not confirmed:
            emit_receipt(
                "Consent grant cancelled", [("Outcome", "No consent was granted")]
            )
            raise typer.Exit(0)
        with get_database(read_only=False) as db:
            result = ConsentService(db).grant_consent(
                feature_category=category,
                backend=resolved_backend,
                consent_mode=mode,
                actor="cli.privacy_grant",
            )
    grant = result.grant
    payload = ConsentMutationPayload(
        feature_category=grant.feature_category,
        backend=grant.backend,
        consent_mode=grant.consent_mode.value,
        action="granted" if result.created else "noop",
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="privacy_grant")
        return
    pairs = [
        ("Category", grant.feature_category),
        ("Backend", grant.backend),
        ("Mode", grant.consent_mode.value),
        ("Outcome", "Granted" if result.created else "Already active; no change made"),
    ]
    if grant.consent_mode == ConsentMode.ONE_TIME:
        pairs.append((
            "Limitation",
            "One-time enforcement is pending; this grant persists until you revoke it",
        ))
    emit_receipt("Consent granted" if result.created else "Consent unchanged", pairs)


def _grant_recipient(
    *,
    category: str,
    recipient: str,
    backend: str | None,
    mode: ConsentMode,
    yes: bool,
    output: OutputFormat,
) -> None:
    """Remember consent for an AI host: interactive terminal only, never --yes."""
    if backend is not None:
        raise typer.BadParameter("--recipient and --backend are mutually exclusive.")
    if category != _HOST_CATEGORY or mode != ConsentMode.PERSISTENT:
        raise typer.BadParameter(
            f"--recipient applies only to '{_HOST_CATEGORY}' with --mode persistent."
        )
    host = host_for_recipient_name(recipient)
    if host is None:
        raise typer.BadParameter(
            f"Unknown recipient {recipient!r}. "
            f"Known recipients: {', '.join(known_host_names())}."
        )
    if yes or output == OutputFormat.JSON or not _stdin_is_tty():
        raise typer.BadParameter(
            "Granting consent to an AI host requires an interactive terminal "
            "and cannot be skipped with --yes or --output json."
        )
    typer.echo(risk_statement(host, file_names=[]), err=True)
    if not typer.confirm("Remember this consent?", default=False, err=True):
        emit_receipt("Consent grant cancelled", [("Outcome", "No consent was granted")])
        raise typer.Exit(0)
    with handle_cli_errors(
        cli_actor="privacy_grant", payload_type=ConsentMutationPayload
    ):
        with get_database(read_only=False) as db:
            grant = ConsentService(db).grant_host_consent(
                host=host, approved_via="cli_prompt", actor="cli.privacy_grant"
            )
    emit_receipt(
        "Consent granted",
        [
            ("Category", grant.feature_category),
            ("Recipient", grant.backend),
            ("Terms", grant.terms_version or ""),
            ("Outcome", "Granted"),
        ],
    )
