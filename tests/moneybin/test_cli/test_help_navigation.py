"""Root discovery preserves one menu between the short and complete views."""

import re
from pathlib import Path

import pytest
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.navigation import HELP_SECTIONS, SHORT_COMMANDS

GROUPS = {
    "Your finances": ["accounts", "assets", "investments", "reports", "transactions"],
    "Review and organize": ["categories", "fx", "merchants", "review"],
    "Import, sync, and export": ["export", "gsheet", "import", "refresh", "sync"],
    "Setup and connections": ["demo", "mcp", "privacy", "profile"],
    "Advanced tools": [
        "db",
        "logs",
        "sql",
        "stats",
        "synthetic",
        "system",
        "transform",
    ],
}
SHORT = {
    "accounts",
    "investments",
    "reports",
    "transactions",
    "review",
    "export",
    "import",
    "sync",
    "demo",
}

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _plain(text: str) -> str:
    """Normalize Rich/Click styling before asserting textual navigation."""
    return _ANSI_RE.sub("", text)


def test_layout_contract_matches_navigation_constants() -> None:
    """Independent acceptance data must name the same root layout as production."""
    assert GROUPS == {
        section: list(commands) for section, commands in HELP_SECTIONS.items()
    }
    assert SHORT == SHORT_COMMANDS


def _commands(text: str) -> list[str]:
    names = "|".join(name for commands in GROUPS.values() for name in commands)
    return re.findall(rf"^\s*[│ ]*({names})\s{{2,}}", text, re.MULTILINE)


@pytest.mark.parametrize("rich_help", [False, True])
@pytest.mark.parametrize("width", [60, 80, 120])
def test_short_menu_expands_without_moving_commands(
    width: int, rich_help: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("COLUMNS", str(width))
    monkeypatch.setattr(app, "rich_markup_mode", "rich" if rich_help else None)
    runner = CliRunner()
    short = runner.invoke(app, [])
    full = runner.invoke(app, ["--help"])
    assert short.exit_code == full.exit_code == 0
    expected = [name for names in GROUPS.values() for name in names]
    full_text = _plain(full.stdout)
    assert _commands(full_text) == expected
    assert _commands(short.stdout) == [name for name in expected if name in SHORT]
    headings = list(GROUPS)
    assert [full_text.index(heading) for heading in headings] == sorted(
        full_text.index(heading) for heading in headings
    )
    assert "Advanced tools" not in short.stdout
    assert "moneybin --help" in short.stdout
    assert "moneybin <command> --help" in short.stdout
    assert not short.stderr
    assert "budget" not in full.stdout
    command = get_command(app)
    assert isinstance(command, TyperGroup)
    visible = {name for name, child in command.commands.items() if not child.hidden}
    assert set(_commands(full_text)) == visible


def test_root_help_uses_short_summaries_without_replacing_group_help() -> None:
    """Root navigation must not flatten the detailed help of child groups."""
    runner = CliRunner()

    root = runner.invoke(app, ["--help"])
    sql = runner.invoke(app, ["sql", "--help"])
    accounts = runner.invoke(app, ["accounts", "--help"])

    assert root.exit_code == sql.exit_code == accounts.exit_code == 0
    assert "Run privacy-safe SQL queries" in root.stdout
    assert "View and manage accounts" in root.stdout
    assert "Privacy-safe ad-hoc SQL (lineage classification + CRITICAL masking)" in (
        sql.stdout
    )
    assert "Account listing, settings, and lifecycle ops" in accounts.stdout


def test_root_usage_marks_the_command_operand_optional() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Usage: moneybin [OPTIONS] [COMMAND] [ARGS]..." in result.stdout


def test_invalid_profile_for_bare_menu_exits_without_runtime_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed explicit profile is rejected before the bare-menu return."""

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail(
            "invalid bare-menu profiles must not initialize runtime or mutate state"
        )

    monkeypatch.setattr("moneybin.cli.main.stash_cli_flags", forbidden)
    monkeypatch.setattr("moneybin.cli.main.setup_observability", forbidden)
    monkeypatch.setattr("moneybin.cli.main.set_current_profile", forbidden)
    monkeypatch.setattr("moneybin.cli.main.register_profile_resolver", forbidden)
    monkeypatch.setattr("moneybin.cli.main.mark_profile_resolution_pending", forbidden)
    monkeypatch.setattr("moneybin.cli.main.show_start_menu", forbidden)

    result = CliRunner().invoke(app, ["--profile", "///"])

    assert result.exit_code == 2
    assert "--profile" in result.stderr


def test_normalizable_profile_for_bare_menu_still_shows_the_menu() -> None:
    """Profile normalization remains available outside commands."""
    result = CliRunner().invoke(app, ["--profile", "bad/name"])

    assert result.exit_code == 0
    assert "MoneyBin - understand your finances" in result.stdout


@pytest.mark.parametrize("args", [[], ["--help"]])
def test_discovery_does_not_resolve_profile_or_initialize_runtime(
    args: list[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Discovery must not initialize runtime or resolve a profile")

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path / "uncreated"))
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr("moneybin.cli.main.setup_observability", forbidden)
    monkeypatch.setattr("moneybin.cli.utils.resolve_profile", forbidden)
    monkeypatch.setattr("moneybin.cli.utils.ensure_default_profile", forbidden)
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert "\x1b[" not in result.stdout
    assert not (tmp_path / "uncreated").exists()
