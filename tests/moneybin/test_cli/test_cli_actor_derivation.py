"""The audit actor a CLI command reports, derived from its click command path.

`handle_cli_errors` and `render_or_json` both stamp `actor="cli.<name>"` onto a
`privacy.log.jsonl` row. The name used to arrive only by hand: a command that
passed `cli_actor=` named itself, and one that did not audited as
`cli.unknown`. Because most commands passed it on the success path
(`render_or_json`) and omitted it on the failure path (`handle_cli_errors`),
one command wrote two different provenances into the audit trail depending
only on whether it succeeded — so the trail could not answer "which command
failed?".

The rule under test: the actor is the click command path with the program name
dropped and the remaining names joined by underscores, hyphens normalized
(`moneybin mcp list-tools` -> `mcp_list_tools`). An explicit `cli_actor=`
always wins, because a shipped actor string is an audit-trail identity and
renaming one would falsify history (see
`test_standard_reference_closure._audit_actor_strings`).
"""

from __future__ import annotations

import ast
import inspect
import json
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import click
import pytest
import typer
import typer.main
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import DatabaseLockError
from moneybin.protocol.envelope import build_envelope

if TYPE_CHECKING:
    from collections.abc import Iterator

runner = CliRunner()


# --------------------------------------------------------------- the rule ---


def _leaf_commands() -> Iterator[tuple[str, click.Command]]:
    """Every leaf command in the real CLI, keyed by its full command path."""
    root = typer.main.get_command(app)

    def walk(cmd: click.Command, path: list[str]) -> Iterator[tuple[str, Any]]:
        if isinstance(cmd, click.Group):
            for name in sorted(cmd.commands):
                yield from walk(cmd.commands[name], [*path, name])
        else:
            yield " ".join(path), cmd

    yield from walk(root, ["moneybin"])


def _expected_actor(command_path: str) -> str:
    """The actor the rule should produce for ``command_path``.

    Spelled out here rather than reusing the production helper: a test that
    calls the implementation to compute its own expectation asserts nothing.
    """
    return "_".join(command_path.split(" ")[1:]).replace("-", "_")


def _declared_actors(command: click.Command) -> set[str]:
    """Literal ``cli_actor=`` strings passed inside a command's own body.

    Reads the callback's source, so an actor threaded through a shared helper
    (``transactions/review.py``) is deliberately invisible — only what the
    command itself declares counts as its declared identity.
    """
    callback = command.callback
    if callback is None:
        return set()
    try:
        source = textwrap.dedent(inspect.getsource(callback))
    except (OSError, TypeError):  # pragma: no cover - source always available
        return set()
    tree = ast.parse(source)
    return {
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "cli_actor"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, str)
    }


#: Commands whose hand-written actor predates the derivation and does NOT match
#: it. Each names its callback function rather than its command path, so the
#: string is either missing the group prefix (``rules_create`` for
#: ``transactions categorize rules create``) or carries a ``_command`` suffix.
#: They keep their strings — a shipped actor is an audit identity, and the
#: explicit kwarg wins by design — and are pinned here so that adopting the
#: derivation for any of them, or introducing a new divergence, fails loudly.
_LEGACY_ACTOR_DIVERGENCES: frozenset[tuple[str, str]] = frozenset({
    ("moneybin import confirm", "import_confirm_command"),
    ("moneybin import files", "import_files_command"),
    ("moneybin import inbox list", "inbox_list"),
    ("moneybin import inbox path", "inbox_path"),
    ("moneybin refresh", "refresh_command"),
    ("moneybin system doctor", "doctor_command"),
    ("moneybin transactions categorize assist", "categorize_assist"),
    ("moneybin transactions categorize auto review", "categorize_auto_review"),
    ("moneybin transactions categorize auto rules", "categorize_auto_rules"),
    ("moneybin transactions categorize auto stats", "categorize_auto_stats"),
    ("moneybin transactions categorize commit", "categorize_commit"),
    (
        "moneybin transactions categorize commit-from-file",
        "categorize_commit_from_file",
    ),
    ("moneybin transactions categorize improve-ai", "categorize_improve_ai"),
    ("moneybin transactions categorize pending", "categorize_pending"),
    ("moneybin transactions categorize rules create", "rules_create"),
    ("moneybin transactions categorize rules delete", "rules_delete"),
    ("moneybin transactions categorize rules list", "rules_list"),
    ("moneybin transactions categorize run", "categorize_run"),
    ("moneybin transactions categorize stats", "categorize_stats"),
    ("moneybin transactions matches history", "matches_history"),
    ("moneybin transactions matches pending", "matches_pending"),
})

#: Floor on how many commands the equivalence check actually compares. Without
#: it a scan that silently matched nothing would pass while asserting nothing.
_MIN_EQUIVALENCE_SAMPLE = 100


def test_derived_actor_matches_every_hand_written_actor() -> None:
    """The rule reproduces the actor each command already declares for itself.

    The strongest evidence available that the normalization is right: every
    command that named itself by hand is a real data point, and the rule has to
    reproduce all of them at once. Dropping ``.replace("-", "_")`` fails this on
    the five hyphenated commands; dropping the group prefix fails it on ~100.
    """
    agreeing: set[str] = set()
    diverging: set[tuple[str, str]] = set()
    for command_path, command in _leaf_commands():
        expected = _expected_actor(command_path)
        for declared in _declared_actors(command):
            if declared == expected:
                agreeing.add(command_path)
            else:
                diverging.add((command_path, declared))

    assert len(agreeing) >= _MIN_EQUIVALENCE_SAMPLE, (
        f"only {len(agreeing)} commands compared — the source scan found far "
        "fewer declared actors than the CLI has, so this test is not checking "
        "what it claims to check"
    )
    assert diverging == _LEGACY_ACTOR_DIVERGENCES


def test_every_command_derives_a_usable_actor() -> None:
    """No command falls back to ``unknown``, and no two share a derived name.

    A duplicate would make the audit trail ambiguous in exactly the way
    ``cli.unknown`` already did, only harder to spot.
    """
    derived = {path: _expected_actor(path) for path, _ in _leaf_commands()}
    assert derived
    assert all(a and a != "unknown" for a in derived.values())
    assert all(a.replace("_", "").isalnum() for a in derived.values())
    assert len(set(derived.values())) == len(derived)


def test_generated_report_commands_agree_with_the_derivation() -> None:
    """The report framework's own actor rule and this one produce one string.

    ``cli_register`` builds ``f"reports_{spec.name}"`` from the underscored
    spec name while the command registers under the hyphenated one, so these
    commands are independent evidence for the hyphen normalization.
    """
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS

    specs = [spec_of(runner) for runner in ALL_REPORTS]
    assert specs
    for spec in specs:
        assert _expected_actor(f"moneybin reports {spec.cli_name}") == (
            f"reports_{spec.name}"
        )


# --------------------------------------------------------- behaviour: CLI ---

probe_app = typer.Typer()
probe_group = typer.Typer()
probe_app.add_typer(probe_group, name="probe-group")


@probe_group.command("bare-command")
def probe_bare(output: OutputFormat = output_option) -> None:
    """No ``cli_actor``: the handler must name the command itself."""
    with handle_cli_errors():
        raise DatabaseLockError("busy")


@probe_group.command("explicit")
def probe_explicit(output: OutputFormat = output_option) -> None:
    """An explicit actor must survive the derivation."""
    with handle_cli_errors(cli_actor="hand_written_actor"):
        raise DatabaseLockError("busy")


@probe_group.command("rendered")
def probe_rendered(output: OutputFormat = output_option) -> None:
    """``render_or_json`` with no ``cli_actor`` must still name the command."""
    render_or_json(build_envelope(data=[]), output)


def _read_events(log_dir: Path) -> list[dict[str, Any]]:
    lines = (log_dir / "privacy.log.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.fixture
def privacy_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    log_dir = tmp_path / "profile"
    log_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir", lambda: log_dir
    )
    return log_dir


def test_bare_error_path_audits_the_command_path(privacy_log_dir: Path) -> None:
    result = runner.invoke(
        probe_app,
        ["probe-group", "bare-command", "--output", "json"],
        prog_name="moneybin",
    )
    assert result.exit_code == 1
    assert _read_events(privacy_log_dir)[0]["actor"] == "cli.probe_group_bare_command"


def test_the_program_name_never_reaches_the_actor(privacy_log_dir: Path) -> None:
    """argv[0] is the one part of the command path a caller chooses.

    Every other segment is a name the CLI registered — click resolves a
    subcommand by exact lookup in ``Group.commands`` and no
    ``token_normalize_func`` is configured — but the root context's
    ``info_name`` is whatever the binary was invoked as. The derivation drops
    it by position, so an alias or an odd path cannot reach privacy.log.jsonl.
    """
    result = runner.invoke(
        probe_app,
        ["probe-group", "bare-command", "--output", "json"],
        prog_name="/opt/odd path/mb-alias",
    )
    assert result.exit_code == 1
    assert _read_events(privacy_log_dir)[0]["actor"] == "cli.probe_group_bare_command"


def test_explicit_actor_overrides_the_derived_one(privacy_log_dir: Path) -> None:
    result = runner.invoke(
        probe_app,
        ["probe-group", "explicit", "--output", "json"],
        prog_name="moneybin",
    )
    assert result.exit_code == 1
    assert _read_events(privacy_log_dir)[0]["actor"] == "cli.hand_written_actor"


def test_render_or_json_audits_the_command_path_without_an_actor(
    privacy_log_dir: Path,
) -> None:
    result = runner.invoke(
        probe_app,
        ["probe-group", "rendered", "--output", "json"],
        prog_name="moneybin",
    )
    assert result.exit_code == 0, result.output
    assert _read_events(privacy_log_dir)[0]["actor"] == "cli.probe_group_rendered"


def test_handle_cli_errors_outside_a_click_context_still_audits(
    privacy_log_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No context is not an error path of its own — it falls back to unknown."""
    monkeypatch.setattr("moneybin.cli.utils._flags.output", OutputFormat.JSON)
    with pytest.raises(typer.Exit), handle_cli_errors():
        raise DatabaseLockError("busy")
    assert _read_events(privacy_log_dir)[0]["actor"] == "cli.unknown"


# ------------------------------------------------- behaviour: real command ---


@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_list_failure_audits_as_gsheet_list(
    mock_build: MagicMock, privacy_log_dir: Path
) -> None:
    """A real bare site: its failure row now names the same command its success row does.

    `gsheet list` declares ``cli_actor="gsheet_list"`` on the success path and
    nothing on the failure path — the 84-command shape this change closes.
    """
    service = MagicMock()
    service.list_connections.side_effect = DatabaseLockError("busy")
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["gsheet", "list", "--output", "json"])

    assert result.exit_code == 1
    assert _read_events(privacy_log_dir)[0]["actor"] == "cli.gsheet_list"
