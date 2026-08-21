"""Tests for the top-level `moneybin` CLI callback: --version and --home flags."""

import importlib.metadata
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app, get_version

runner = CliRunner()


def _config_path_argv(*prefix: str) -> list[str]:
    """Argv for a command that echoes a path derived from the data home.

    ``mcp config path`` resolves ``<base>/profiles/<profile>/...`` through
    ``get_base_dir()`` and prints it, so it reports the resolved data home
    without needing that home to exist on disk.
    """
    return [
        *prefix,
        "mcp",
        "config",
        "path",
        "--client",
        "claude-code",
        "--profile",
        "p",
    ]


def test_version_flag_prints_installed_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert get_version() in result.stdout


def test_get_version_matches_distribution_metadata() -> None:
    assert get_version() == importlib.metadata.version("moneybin")


def test_version_preempts_subcommand_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--version stats` must print and exit before `stats` runs.

    Guards the ``raise typer.Exit()`` in ``_version_callback``: without it,
    Typer's eager-option handling would print the version and then fall
    through to execute the subcommand in full (cli.md Help Surface
    Contract). A bare ``--version`` invocation can't detect this — with no
    subcommand, ``main_callback``'s body never runs regardless of whether
    the eager exit fires, so the guard has to be exercised through a real
    subcommand that would otherwise touch the database.
    """

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("stats subcommand must not run when --version is set")

    monkeypatch.setattr("moneybin.cli.commands.stats.get_database", _boom)
    result = runner.invoke(app, ["--version", "stats"])
    assert result.exit_code == 0
    assert get_version() in result.stdout


def test_home_flag_overrides_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--home` outranks MONEYBIN_HOME — an explicit flag beats ambient config.

    Seeding MONEYBIN_HOME first is also what keeps this test honest about
    cleanup: the flag writes through to ``os.environ``, and monkeypatch only
    restores keys it recorded.
    """
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path / "from-env"))
    chosen = tmp_path / "from-flag"

    result = runner.invoke(app, _config_path_argv("--home", str(chosen)))

    assert result.exit_code == 0
    expected = chosen.resolve() / "profiles" / "p" / "claude-code-mcp.json"
    assert result.output.strip() == str(expected)


def test_omitting_home_flag_leaves_the_environment_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No `--home` must not clobber a MONEYBIN_HOME the user already set."""
    from_env = tmp_path / "from-env"
    monkeypatch.setenv("MONEYBIN_HOME", str(from_env))

    result = runner.invoke(app, _config_path_argv())

    assert result.exit_code == 0
    expected = from_env.resolve() / "profiles" / "p" / "claude-code-mcp.json"
    assert result.output.strip() == str(expected)


def test_home_flag_is_documented_in_help() -> None:
    """Discoverability is the whole point of the flag, and `--help` is where.

    The environment variable already reached every launch context before this
    flag existed; what it did not do was show up anywhere a user looks.
    """
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "--home" in result.output
    assert "MONEYBIN_HOME" in result.output
    # Both defaults, not just the installed-user one: a flag whose reason for
    # existing is discoverability must not misstate what it defaults to.
    assert ".moneybin" in result.output
    assert "checkout" in result.output
