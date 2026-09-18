"""Terminal capabilities resolved from the streams a CLI invocation actually has.

This module intentionally accepts settings rather than loading them.  Help,
completion, and early error paths can therefore choose presentation without
resolving a profile or opening the database.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Literal, Protocol

__all__ = ["TerminalPolicy", "TerminalSymbols", "resolve_terminal_policy"]


class _Stream(Protocol):
    @property
    def encoding(self) -> str | None: ...

    def isatty(self) -> bool: ...


class _CLISettings(Protocol):
    auto_pager: bool
    reduced_motion: bool
    ascii: bool


@dataclass(frozen=True, slots=True)
class TerminalSymbols:
    """Functional symbols that retain their state meaning in ASCII terminals."""

    success: str
    attention: str
    failure: str
    action: str


@dataclass(frozen=True, slots=True)
class TerminalPolicy:
    """The terminal presentation choices for one invocation.

    ``output`` is deliberately explicit: JSON suppresses human presentation,
    while reduced motion only replaces animated progress with static stages.
    Downstream progress helpers can make that distinction without re-reading
    flags or streams.
    """

    output: Literal["text", "json"]
    interactive: bool
    page: bool
    color: bool
    style: bool
    animate_progress: bool
    stage_chatter: bool
    ascii: bool
    width: int
    height: int
    symbols: TerminalSymbols
    minus: str


def _is_tty(stream: _Stream) -> bool:
    return bool(stream.isatty())


def _uses_unicode(stream: _Stream) -> bool:
    encoding = stream.encoding
    return encoding is None or encoding.lower().replace("-", "") in {
        "utf8",
        "utf16",
        "utf32",
    }


def _terminal_dimensions(stream: _Stream) -> tuple[int, int]:
    """Return dimensions for this stream, with portable terminal defaults."""
    fileno = getattr(stream, "fileno", None)
    if callable(fileno):
        try:
            descriptor = fileno()
            if not isinstance(descriptor, int):
                raise OSError("stream has no integer file descriptor")
            size = os.get_terminal_size(descriptor)
        except OSError:
            pass
        else:
            return size.columns, size.lines
    size = shutil.get_terminal_size(fallback=(80, 24))
    return size.columns, size.lines


def resolve_terminal_policy(
    *,
    stdin: _Stream,
    stdout: _Stream,
    stderr: _Stream,
    output: Literal["text", "json"],
    quiet: bool,
    no_pager: bool,
    settings: _CLISettings,
) -> TerminalPolicy:
    """Resolve terminal behavior without reading input or application state.

    Colour belongs to stdout because result renderers write there.  Animated
    progress additionally needs an interactive text session and a terminal
    stderr; static stages remain available for redirected text runs.
    """
    text_output = output == "text"
    interactive = text_output and _is_tty(stdin) and _is_tty(stdout)
    color = text_output and _is_tty(stdout) and "NO_COLOR" not in os.environ
    ascii_output = settings.ascii or not _uses_unicode(stdout)
    symbols = (
        TerminalSymbols(success="OK", attention="!", failure="X", action=">")
        if ascii_output
        else TerminalSymbols(success="✓", attention="!", failure="×", action="›")
    )
    stage_chatter = text_output and not quiet
    width, height = _terminal_dimensions(stdout)
    animate_progress = (
        interactive and _is_tty(stderr) and not quiet and not settings.reduced_motion
    )
    return TerminalPolicy(
        output=output,
        interactive=interactive,
        page=interactive and settings.auto_pager and not no_pager,
        color=color,
        style=color,
        animate_progress=animate_progress,
        stage_chatter=stage_chatter,
        ascii=ascii_output,
        width=width,
        height=height,
        symbols=symbols,
        minus="-" if ascii_output else "−",
    )
