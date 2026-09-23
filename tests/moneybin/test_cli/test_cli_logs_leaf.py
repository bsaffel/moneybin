"""Tests for the logs-as-leaf command shape.

Leaf-shape contract tests (argument requirements, mode-flag exemptions, flag
precedence) live here; behavior tests (view filtering, prune logic, tail) live
in ``test_cli_logs.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import logs as logs_module
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols


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
        assert "missing argument" in combined

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
    def test_print_path_takes_precedence_over_prune(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`--print-path` must win over `--prune` (no prune side effects)."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        result = runner.invoke(
            logs_module.logs_command_app,
            ["--print-path", "--prune", "--older-than", "30d"],
        )
        assert result.exit_code == 0, result.output
        assert str(log_dir) in result.stdout
        assert log_dir.exists()
        assert list(log_dir.iterdir()) == []

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

    @pytest.mark.unit
    def test_long_raw_view_pages_one_complete_answer_and_no_pager_prints_it(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Paging consumes the same bounded raw answer that --no-pager prints."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "cli_2026-04-30.log").write_text(
            "".join(f"raw {index}\n" for index in range(30))
        )
        _patch_settings(monkeypatch, log_dir)
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols("OK", "!", "X", ">"),
            minus="-",
        )
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_terminal_policy",
            lambda *, no_pager=False: policy,
        )
        pages: list[str] = []

        def capture_page(text: str, *, color: bool, wide: bool) -> bool:
            del color, wide
            pages.append(text)
            return True

        monkeypatch.setattr(
            "moneybin.cli.pager.page_text",
            capture_page,
        )

        paged = runner.invoke(logs_module.logs_command_app, ["cli", "-n", "30"])
        direct = runner.invoke(
            logs_module.logs_command_app, ["cli", "-n", "30", "--no-pager"]
        )

        assert paged.exit_code == direct.exit_code == 0
        assert "raw 0" in pages[0] and "raw 29" in pages[0]
        assert (
            pages[0].replace("\n\nq return to shell\n", "").rstrip()
            == direct.stdout.rstrip()
        )

    @pytest.mark.unit
    def test_json_and_follow_do_not_page(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Structured output and live reading bypass the finite-result pager."""
        log_dir = tmp_path / "logs"
        _seed_logs(log_dir)
        _patch_settings(monkeypatch, log_dir)
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_terminal_policy",
            lambda *, no_pager=False: (_ for _ in ()).throw(AssertionError("no pager")),
        )

        result = runner.invoke(
            logs_module.logs_command_app, ["cli", "--output", "json"]
        )

        assert result.exit_code == 0, result.output
        assert isinstance(__import__("json").loads(result.output), list)

    @pytest.mark.unit
    def test_follow_initial_tail_never_pages(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Live reading keeps its initial tail on stdout, never in a pager."""
        log_dir = tmp_path / "logs"
        _seed_logs(log_dir)
        _patch_settings(monkeypatch, log_dir)
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols("OK", "!", "X", ">"),
            minus="-",
        )
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_terminal_policy",
            lambda *, no_pager=False: policy,
        )

        def unexpected_page(text: str, *, color: bool, wide: bool) -> bool:
            del text, color, wide
            pytest.fail("follow must not page")

        def interrupt_follow(_seconds: float) -> None:
            raise KeyboardInterrupt

        monkeypatch.setattr("moneybin.cli.pager.page_text", unexpected_page)
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.time.sleep",
            interrupt_follow,
        )

        result = runner.invoke(logs_module.logs_command_app, ["cli", "--follow"])

        assert result.exit_code == 0, result.output
        assert "hello cli" in result.output

    @pytest.mark.unit
    def test_raw_log_markup_and_controls_are_not_interpreted(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Log content is literal text and terminal controls do not reach stdout."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "cli_2026-04-30.log").write_text(
            "[bold]literal[/bold] \x1b[31mred\x1b[0m\n"
        )
        _patch_settings(monkeypatch, log_dir)

        result = runner.invoke(logs_module.logs_command_app, ["cli"])

        assert result.exit_code == 0, result.output
        assert "[bold]literal[/bold]" in result.output
        assert "\x1b" not in result.output

    @pytest.mark.unit
    @pytest.mark.parametrize("has_directory", [False, True])
    def test_empty_json_view_is_a_bare_array(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        has_directory: bool,
    ) -> None:
        """Both valid empty scopes retain the command's parseable JSON contract."""
        log_dir = tmp_path / "logs"
        if has_directory:
            log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)

        result = runner.invoke(
            logs_module.logs_command_app, ["cli", "--output", "json"]
        )

        assert result.exit_code == 0, result.output
        assert __import__("json").loads(result.output) == []

    @pytest.mark.unit
    @pytest.mark.parametrize("has_directory", [False, True])
    def test_empty_follow_never_pages(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        has_directory: bool,
    ) -> None:
        """Live follow remains unpaged even before a stream has any content."""
        log_dir = tmp_path / "logs"
        if has_directory:
            log_dir.mkdir()
        _patch_settings(monkeypatch, log_dir)
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols("OK", "!", "X", ">"),
            minus="-",
        )
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_terminal_policy",
            lambda *, no_pager=False: policy,
        )

        def unexpected_page(text: str, *, color: bool, wide: bool) -> bool:
            del text, color, wide
            pytest.fail("empty follow must not page")

        monkeypatch.setattr("moneybin.cli.pager.page_text", unexpected_page)

        result = runner.invoke(logs_module.logs_command_app, ["cli", "--follow"])

        assert result.exit_code == 0, result.output

    @pytest.mark.unit
    def test_long_filtered_view_pages_the_same_complete_answer(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A filtered view pages all bounded candidates, without fetching more."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "cli_2026-04-30.log").write_text(
            "".join(
                f"2026-04-30 10:00:{index:02d},000 - test - INFO - match {index}\n"
                for index in range(30)
            )
        )
        _patch_settings(monkeypatch, log_dir)
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols("OK", "!", "X", ">"),
            minus="-",
        )
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_terminal_policy",
            lambda *, no_pager=False: policy,
        )
        pages: list[str] = []

        def capture_page(text: str, *, color: bool, wide: bool) -> bool:
            del color, wide
            pages.append(text)
            return True

        monkeypatch.setattr(
            "moneybin.cli.pager.page_text",
            capture_page,
        )

        paged = runner.invoke(
            logs_module.logs_command_app, ["cli", "--grep", "match", "-n", "30"]
        )
        direct = runner.invoke(
            logs_module.logs_command_app,
            ["cli", "--grep", "match", "-n", "30", "--no-pager"],
        )

        assert paged.exit_code == direct.exit_code == 0
        assert "match 0" in pages[0] and "match 29" in pages[0]
        assert (
            pages[0].replace("\n\nq return to shell\n", "").rstrip()
            == direct.stdout.rstrip()
        )
