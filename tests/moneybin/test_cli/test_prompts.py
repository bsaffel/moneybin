"""Tests for inline CLI choice prompts."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest
import typer
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.key_binding.key_processor import KeyPressEvent
from prompt_toolkit.keys import Keys

from moneybin.cli.prompts import Choice, choose_required
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols


def _policy(*, interactive: bool) -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=interactive,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=False,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )


def test_explicit_choice_never_prompts() -> None:
    prompt_spy = MagicMock()

    assert (
        choose_required(
            "account-1",
            choices=(Choice("account-1", "Everyday · …1234"),),
            flag="--account",
            policy=_policy(interactive=False),
            prompt_input=prompt_spy,
        )
        == "account-1"
    )
    prompt_spy.assert_not_called()


def test_noninteractive_missing_choice_refuses_without_prompting() -> None:
    prompt_spy = MagicMock()

    with pytest.raises(typer.BadParameter, match="--account"):
        choose_required(
            None,
            choices=(Choice("account-1", "Everyday · …1234"),),
            flag="--account",
            policy=_policy(interactive=False),
            prompt_input=prompt_spy,
        )

    prompt_spy.assert_not_called()


def test_ambiguous_label_requires_a_unique_selection() -> None:
    with pytest.raises(typer.BadParameter, match="unique"):
        choose_required(
            None,
            choices=(Choice("one", "Checking"), Choice("two", "Checking")),
            flag="--account",
            policy=_policy(interactive=True),
            prompt_input=MagicMock(return_value="Checking"),
        )


def test_interrupt_cancels_the_choice() -> None:
    with pytest.raises(typer.Abort):
        choose_required(
            None,
            choices=(Choice("account-1", "Everyday · …1234"),),
            flag="--account",
            policy=_policy(interactive=True),
            prompt_input=MagicMock(side_effect=KeyboardInterrupt),
        )


def test_eof_cancels_the_choice() -> None:
    with pytest.raises(typer.Abort):
        choose_required(
            None,
            choices=(Choice("account-1", "Everyday · …1234"),),
            flag="--account",
            policy=_policy(interactive=True),
            prompt_input=MagicMock(side_effect=EOFError),
        )


def test_escape_binding_cancels_the_choice(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the actual Prompt Toolkit Escape binding, not a mock shortcut."""
    import prompt_toolkit

    class _App:
        exception: BaseException | None = None

        def exit(self, *, exception: BaseException) -> None:
            self.exception = exception

        def invalidate(self) -> None:
            pass

    class _Event:
        app = _App()

    def prompt_with_escape(_: str, **kwargs: object) -> str:
        bindings = cast(KeyBindings, kwargs["key_bindings"])
        binding = bindings.get_bindings_for_keys((Keys.Escape,))[0]
        event = _Event()
        binding.call(cast(KeyPressEvent, event))
        assert event.app.exception is not None
        raise event.app.exception

    monkeypatch.setattr(prompt_toolkit, "prompt", prompt_with_escape)

    with pytest.raises(typer.Abort):
        choose_required(
            None,
            choices=(Choice("account-1", "Everyday · …1234"),),
            flag="--account",
            policy=_policy(interactive=True),
        )
