"""Tests for the bounded, non-persistent human-result pager."""

from __future__ import annotations

from typing import NoReturn, cast

import pytest

from moneybin.cli import pager


def test_page_text_starts_less_with_safe_fixed_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long answer is sent to a safe child process, never a shell."""
    received: dict[str, object] = {}

    class _Pager:
        returncode = 0

        def communicate(self, text: str) -> None:
            received["text"] = text

    def _popen(args: list[str], **kwargs: object) -> _Pager:
        received["args"] = args
        received["kwargs"] = kwargs
        return _Pager()

    monkeypatch.setattr(pager.subprocess, "Popen", _popen)

    assert pager.page_text("row\x1b]0;title\x07\n", color=False, wide=True)
    assert received["args"] == [
        "less",
        "-F",
        "-X",
        "-K",
        "-S",
        "-P",
        "q return to shell",
    ]
    assert received["text"] == "row\n"
    environment = cast(
        "dict[str, str]", cast("dict[str, object]", received["kwargs"])["env"]
    )
    assert environment["LESSHISTFILE"] == "-"
    assert environment["LESSSECURE"] == "1"
    assert environment["LESSOPEN"] == ""


def test_page_text_uses_ansi_option_only_for_colored_rendering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plain answers do not ask less to interpret escape sequences."""
    arguments: list[list[str]] = []

    class _Pager:
        returncode = 0

        def communicate(self, text: str) -> None:
            pass

    def _popen(args: list[str], **kwargs: object) -> _Pager:
        arguments.append(args)
        return _Pager()

    monkeypatch.setattr(pager.subprocess, "Popen", _popen)

    pager.page_text("answer", color=True, wide=False)
    assert "-R" in arguments[0]


def test_page_text_preserves_only_library_sgr_in_colored_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sanitizing data controls cannot corrupt Rich's color reset sequence."""
    received: list[str] = []

    class _Pager:
        returncode = 0

        def communicate(self, text: str) -> None:
            received.append(text)

    def _popen(*args: object, **kwargs: object) -> _Pager:
        return _Pager()

    monkeypatch.setattr(pager.subprocess, "Popen", _popen)

    pager.page_text("\x1b[31mred\x1b[0m\x1b]0;title\x07\n", color=True, wide=False)

    assert received == ["\x1b[31mred\x1b[0m\n"]


def test_page_text_returns_false_when_less_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The output boundary can print once when no pager process starts."""

    def _missing(*args: object, **kwargs: object) -> NoReturn:
        raise FileNotFoundError

    monkeypatch.setattr(pager.subprocess, "Popen", _missing)

    assert not pager.page_text("answer", color=False, wide=False)


def test_page_text_accepts_early_pager_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing less early is successful reading, not a failed command."""
    calls: list[str] = []

    class _Pager:
        returncode = 0

        def communicate(self, text: str) -> None:
            calls.append("communicate")
            raise BrokenPipeError

        def wait(self) -> int:
            calls.append("wait")
            return self.returncode

    def _popen(*args: object, **kwargs: object) -> _Pager:
        return _Pager()

    monkeypatch.setattr(pager.subprocess, "Popen", _popen)

    assert pager.page_text("answer", color=False, wide=False)
    assert calls == ["communicate", "wait"]


def test_page_text_returns_false_when_pager_exits_unsuccessfully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A completed pager failure leaves the caller to print its ordinary output."""

    class _Pager:
        returncode = 1

        def communicate(self, text: str) -> tuple[str, str]:
            return "", ""

    def _popen(*args: object, **kwargs: object) -> _Pager:
        return _Pager()

    monkeypatch.setattr(pager.subprocess, "Popen", _popen)

    assert not pager.page_text("answer", color=False, wide=False)


def test_page_text_cleans_up_the_child_when_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ctrl+C leaves no less process behind and retains command interruption."""
    calls: list[str] = []

    class _Pager:
        def communicate(self, text: str) -> None:
            raise KeyboardInterrupt

        def terminate(self) -> None:
            calls.append("terminate")

        def wait(self) -> None:
            calls.append("wait")

    def _popen(*args: object, **kwargs: object) -> _Pager:
        return _Pager()

    monkeypatch.setattr(pager.subprocess, "Popen", _popen)

    try:
        pager.page_text("answer", color=False, wide=False)
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("pager swallowed Ctrl+C")

    assert calls == ["terminate", "wait"]
