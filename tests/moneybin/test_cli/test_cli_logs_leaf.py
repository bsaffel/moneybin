"""Tests for the new logs-as-leaf command shape."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import logs as logs_module


@pytest.fixture()
def runner() -> CliRunner:
    """Return a fresh Typer CliRunner."""
    return CliRunner()


def _seed_logs(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "cli_2026-04-30.log").write_text(
        "2026-04-30 10:00:00,000 - moneybin.test - INFO - hello cli\n"
    )
    (log_dir / "mcp_2026-04-30.log").write_text(
        "2026-04-30 10:00:00,000 - moneybin.test - INFO - hello mcp\n"
    )


def _patch_settings(
    monkeypatch: pytest.MonkeyPatch,
    log_dir: Path,
) -> None:
    monkeypatch.setattr(
        "moneybin.cli.commands.logs.get_settings",
        lambda: type(
            "S",
            (),
            {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
        )(),
    )


class TestLogsLeafShape:
    """Shape tests for the logs leaf command and `logs_command_app` wrapper."""

    @pytest.mark.unit
    def test_bare_invocation_errors_with_usage(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`moneybin logs` (no stream) must exit non-zero with a usage error."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(logs_module.logs_command_app, [])
        assert result.exit_code == 2, result.output
        combined = (result.output + (result.stderr or "")).lower()
        assert "missing argument" in combined or "usage" in combined

    @pytest.mark.unit
    def test_unknown_stream_errors(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(logs_module.logs_command_app, ["bogus"])
        assert result.exit_code != 0

    @pytest.mark.unit
    def test_known_stream_reads_lines(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        _seed_logs(log_dir)
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(logs_module.logs_command_app, ["cli"])
        assert result.exit_code == 0
        assert "hello cli" in result.output
        assert "hello mcp" not in result.output

    @pytest.mark.unit
    def test_print_path_flag_skips_stream_requirement(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`logs --print-path` must work without supplying a stream."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(logs_module.logs_command_app, ["--print-path"])
        assert result.exit_code == 0, result.output
        assert str(log_dir) in result.output

    @pytest.mark.unit
    def test_prune_flag_skips_stream_requirement(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(
            logs_module.logs_command_app,
            ["--prune", "--older-than", "30d", "--dry-run"],
        )
        assert result.exit_code == 0, result.output

    @pytest.mark.unit
    def test_all_stream_no_longer_accepted(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(logs_module.logs_command_app, ["all"])
        assert result.exit_code != 0

    @pytest.mark.unit
    def test_until_filter_accepted(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        _seed_logs(log_dir)
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(
            logs_module.logs_command_app, ["cli", "--until", "2099-01-01T00:00:00"]
        )
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_absolute_since_accepted(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        _seed_logs(log_dir)
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(
            logs_module.logs_command_app, ["cli", "--since", "2000-01-01T00:00:00"]
        )
        assert result.exit_code == 0
        assert "hello cli" in result.output
