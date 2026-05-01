"""Verify CLI error output routes to stderr (fd 2), not stdout.

These tests lock the contract for the ``logs`` leaf command: the missing-stream
error path (``typer.echo(..., err=True)``) and the unknown-stream error path
(``logger.error(...)``) must both land on stderr — never stdout.

Click 8.3 removed ``CliRunner(mix_stderr=False)`` — plain ``CliRunner()`` now
keeps streams separate, and ``result.stderr`` returns only the stderr stream.

The unknown-stream path uses ``logger.error()``, which is routed by a
``StreamHandler`` configured in ``setup_logging``. ``CliRunner.invoke`` patches
``sys.stderr`` at invoke time, so the handler must be (re)constructed inside
the invoke to bind to the runner's substituted stream. We do that with a
local Typer wrapper that calls ``setup_logging`` in its callback.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands import logs as logs_module
from moneybin.logging.config import setup_logging


@pytest.fixture()
def runner() -> CliRunner:
    """Return a fresh Typer/Click CliRunner with split streams."""
    return CliRunner()


def _patch_settings(monkeypatch: pytest.MonkeyPatch, log_dir: Path) -> None:
    monkeypatch.setattr(
        "moneybin.cli.commands.logs.get_settings",
        lambda: type(
            "S",
            (),
            {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
        )(),
    )


def _make_wrapper_app() -> typer.Typer:
    """Wrap ``logs_command`` in a Typer that re-binds logging on each invoke.

    The callback calls ``setup_logging`` so the root ``StreamHandler`` is
    constructed against the runner-substituted ``sys.stderr``. Without this,
    ``logger.error`` writes to the real ``sys.stderr`` captured by pytest, not
    to ``result.stderr``.
    """
    wrapper = typer.Typer(no_args_is_help=False)

    @wrapper.callback(invoke_without_command=False)
    def _setup() -> None:
        setup_logging(stream="cli")

    _ = _setup  # silence reportUnusedFunction; Typer keeps the reference
    wrapper.command()(logs_module.logs_command)
    return wrapper


@pytest.fixture(autouse=True)
def _reset_root_logging() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Strip root logger handlers after each test to avoid cross-test bleed."""
    yield
    for handler in list(logging.root.handlers):
        logging.root.removeHandler(handler)


@pytest.mark.unit
def test_logs_missing_stream_error_on_stderr(
    runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bare ``logs`` invocation: 'Missing argument' must appear on stderr."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    _patch_settings(monkeypatch, log_dir)

    result = runner.invoke(logs_module.logs_command_app, [])

    assert result.exit_code == 2, result.output
    assert "missing argument" in (result.stderr or "").lower()
    assert "missing argument" not in result.stdout.lower()


@pytest.mark.unit
def test_logs_unknown_stream_error_on_stderr(
    runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``logs bogus``: ``logger.error`` output must appear on stderr."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    _patch_settings(monkeypatch, log_dir)

    wrapper = _make_wrapper_app()
    result = runner.invoke(wrapper, ["logs-command", "bogus"])

    assert result.exit_code != 0
    stderr = (result.stderr or "").lower()
    assert "unknown stream" in stderr
    assert "unknown stream" not in result.stdout.lower()
