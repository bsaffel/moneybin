"""Focused, inline prompts for required CLI choices."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

import typer

from moneybin.cli.terminal import TerminalPolicy


@dataclass(frozen=True, slots=True)
class Choice:
    """One readable label mapped to the stable value a service accepts."""

    value: str
    label: str


def _record_outcome(outcome: Literal["selected", "cancelled", "refused"]) -> None:
    """Record a fixed interactive result only when a command invoked the prompt."""
    from moneybin.cli.output import derive_cli_actor
    from moneybin.metrics.registry import CLI_PROMPT_OUTCOMES_TOTAL

    command = derive_cli_actor()
    if command is not None:
        CLI_PROMPT_OUTCOMES_TOTAL.labels(command=command, outcome=outcome).inc()


def choose_required(
    provided: str | None,
    *,
    choices: Sequence[Choice],
    flag: str,
    policy: TerminalPolicy,
    validate_choice: Callable[[str], str] | None = None,
    prompt_input: Callable[..., str] | None = None,
) -> str:
    """Return an explicit choice or ask for one in an interactive terminal.

    The optional validator keeps prompt answers on the same validation path as
    explicit flags.  Prompt Toolkit stays lazy so help and scripted calls do
    not import terminal input machinery or consume stdin.
    """
    validate = validate_choice or _identity
    if provided is not None:
        return validate(provided)
    if not choices:
        _record_outcome("refused")
        raise typer.BadParameter(
            f"No choices are available; pass {flag} when a target exists.",
            param_hint=flag,
        )
    if not policy.interactive:
        _record_outcome("refused")
        raise typer.BadParameter(
            f"{flag} is required when input is not an interactive terminal.",
            param_hint=flag,
        )

    labels = [choice.label for choice in choices]
    if prompt_input is None:
        prompt_input = _prompt_input(labels)
    try:
        selected_label = prompt_input(f"Choose {flag}: ")
    except (EOFError, KeyboardInterrupt):
        _record_outcome("cancelled")
        raise typer.Abort() from None
    matches = [choice.value for choice in choices if choice.label == selected_label]
    if len(matches) != 1:
        _record_outcome("refused")
        raise typer.BadParameter(
            f"Choose one unique displayed value for {flag}.", param_hint=flag
        )
    try:
        value = validate(matches[0])
    except typer.BadParameter:
        _record_outcome("refused")
        raise
    _record_outcome("selected")
    return value


def _identity(value: str) -> str:
    return value


def _prompt_input(labels: Sequence[str]) -> Callable[..., str]:
    """Build a no-history Prompt Toolkit input function only when needed."""
    from prompt_toolkit import prompt
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.key_binding import KeyBindings

    key_bindings = KeyBindings()

    @key_bindings.add("escape")
    def _cancel(event: object) -> None:
        event.app.exit(exception=EOFError())  # type: ignore[attr-defined]

    _ = _cancel

    completer = WordCompleter(labels, match_middle=True)

    def read(message: str) -> str:
        return prompt(
            message,
            completer=completer,
            complete_while_typing=True,
            key_bindings=key_bindings,
        )

    return read
