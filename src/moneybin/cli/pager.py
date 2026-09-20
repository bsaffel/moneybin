"""A deliberately small, safe adapter for paging already-rendered CLI output."""

from __future__ import annotations

import os
import re
import subprocess  # noqa: S404  # fixed executable and argv below
from typing import Literal

_ANSI_SGR = re.compile(r"\x1b\[[0-9;]*m")
_OSC = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")
_CONTROL = re.compile(r"[\x00-\x09\x0b-\x1f\x7f-\x9f]")


def _record_fallback(reason: Literal["unavailable", "start_failed"]) -> None:
    """Record only the bounded pager-start outcome for an active CLI command."""
    from moneybin.cli.output import derive_cli_actor
    from moneybin.metrics.registry import CLI_PAGER_FALLBACK_TOTAL

    command = derive_cli_actor()
    if command is not None:
        CLI_PAGER_FALLBACK_TOTAL.labels(command=command, reason=reason).inc()


def _safe_text(text: str, *, color: bool) -> str:
    """Drop terminal controls while retaining Rich's SGR styling when requested."""
    source = _OSC.sub("", text)
    if not color:
        return _CONTROL.sub("", source)
    pieces: list[str] = []
    at = 0
    for style in _ANSI_SGR.finditer(source):
        pieces.append(_CONTROL.sub("", source[at : style.start()]))
        pieces.append(style.group())
        at = style.end()
    pieces.append(_CONTROL.sub("", source[at:]))
    return "".join(pieces)


def page_text(text: str, *, color: bool, wide: bool) -> bool:
    """Page a rendered answer, returning false only if no child could start."""
    args = ["less", "-F", "-X", "-K"]
    if color:
        args.append("-R")
    if wide:
        args.append("-S")
    args.extend(["-P", "q return to shell"])
    environment = os.environ.copy()
    environment.update({
        "LESSHISTFILE": "-",
        "LESSSECURE": "1",
        "LESS": "",
        "LESSOPEN": "",
        "LESSCLOSE": "",
    })
    try:
        process = subprocess.Popen(  # noqa: S603  # fixed executable and argv
            args,
            stdin=subprocess.PIPE,
            text=True,
            env=environment,
        )
    except FileNotFoundError:
        _record_fallback("unavailable")
        return False
    except OSError:
        _record_fallback("start_failed")
        return False
    try:
        process.communicate(_safe_text(text, color=color))
    except BrokenPipeError:
        pass
    except KeyboardInterrupt:
        try:
            process.terminate()
            process.wait()
        except ProcessLookupError:
            pass
        raise
    return True
