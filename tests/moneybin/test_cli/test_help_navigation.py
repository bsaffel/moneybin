"""Root discovery preserves one menu between the short and complete views."""

import re
from pathlib import Path

import pytest
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

from moneybin.cli.main import app

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
    assert _commands(full.stdout) == expected
    assert _commands(short.stdout) == [name for name in expected if name in SHORT]
    headings = list(GROUPS)
    assert [full.stdout.index(heading) for heading in headings] == sorted(
        full.stdout.index(heading) for heading in headings
    )
    assert "Advanced tools" not in short.stdout
    assert "moneybin --help" in short.stdout
    assert "moneybin <command> --help" in short.stdout
    assert not short.stderr
    assert "budget" not in full.stdout
    command = get_command(app)
    assert isinstance(command, TyperGroup)
    visible = {name for name, child in command.commands.items() if not child.hidden}
    assert set(_commands(full.stdout)) == visible


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
