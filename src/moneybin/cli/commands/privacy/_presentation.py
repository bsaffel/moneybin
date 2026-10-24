"""Shared human receipts and confirmation checks for privacy commands."""

from __future__ import annotations

import typer

from moneybin import error_codes
from moneybin.cli.output import OutputFormat, emit_human_result
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy
from moneybin.errors import UserError


def require_confirmation(*, yes: bool, output: OutputFormat, prompt: str) -> bool:
    """Return a deliberate terminal choice, refusing unaskable mutations."""
    if yes:
        return True
    if output == OutputFormat.JSON or not get_terminal_policy().interactive:
        raise UserError(
            "Explicit confirmation is required.",
            code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
            hint="Re-run with --yes after reviewing the requested change.",
        )
    return typer.confirm(prompt, default=False, err=True)


def emit_receipt(title: str, pairs: list[tuple[str, str]]) -> None:
    """Print one unpaged operation result, including under quiet mode."""
    emit_human_result(
        compose_human_result([build_summary(pairs, title=title)]),
        policy=get_terminal_policy(no_pager=True),
        finite_read=False,
        no_pager=True,
        receipt=True,
    )
