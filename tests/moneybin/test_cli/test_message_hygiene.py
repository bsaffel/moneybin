"""User-facing message hygiene (cli-output-coherence reqs 16, 17, 19, 31-33).

Requirement 31 hides whole-command stubs from ``--help`` while keeping them
invocable. The enumeration below is deliberate, not a grep for
``_not_implemented``: a command that reaches it on *some* paths is not a stub,
and hiding it would remove working behaviour from ``--help``.
"""

import ast
from pathlib import Path

import click
import pytest
from typer.main import get_command
from typer.testing import CliRunner

from moneybin.cli import main as cli_main
from moneybin.cli.main import app

runner = CliRunner()

# Whole-command stubs: every path through them is unimplemented.
STUB_COMMANDS = (
    "budget delete",
    "budget set",
    "sync key rotate",
    "sync schedule remove",
    "sync schedule set",
    "sync schedule show",
    "transactions categorize ml apply",
    "transactions categorize ml status",
    "transactions categorize ml train",
)

# Commands that reach a not-implemented branch on *some* paths only. These
# must stay visible: hiding them would remove working behaviour from --help.
PARTIALLY_IMPLEMENTED_COMMANDS = ("transactions review",)


def _walk_commands() -> dict[str, click.Command]:
    """Map every executable command path to its click command."""
    found: dict[str, click.Command] = {}

    def walk(command: click.Command, prefix: tuple[str, ...]) -> None:
        if isinstance(command, click.Group):
            if command.invoke_without_command and prefix:
                found[" ".join(prefix)] = command
            for name, child in command.commands.items():
                walk(child, (*prefix, name))
            return
        found[" ".join(prefix)] = command

    walk(get_command(app), ())
    return found


@pytest.mark.parametrize("path", STUB_COMMANDS)
def test_stub_command_is_hidden_from_help(path: str) -> None:
    """A whole-command stub is registered but never advertised."""
    command = _walk_commands()[path]

    assert command.hidden, f"{path} is a stub and must not appear in --help"


@pytest.mark.parametrize("path", PARTIALLY_IMPLEMENTED_COMMANDS)
def test_partially_implemented_command_stays_visible(path: str) -> None:
    """Hiding a command with any working path would remove real behaviour."""
    command = _walk_commands()[path]

    assert not command.hidden, f"{path} has working paths and must stay in --help"


@pytest.mark.parametrize("path", STUB_COMMANDS)
def test_stub_name_absent_from_parent_help(path: str) -> None:
    """The stub's name does not appear in its parent group's --help listing."""
    parent, _, leaf = path.rpartition(" ")
    result = runner.invoke(app, [*parent.split(), "--help"])

    assert result.exit_code == 0, result.output
    assert leaf not in result.stdout, f"{path} is advertised by `{parent} --help`"


# Arguments needed to reach each stub's body.
STUB_INVOCATIONS = {
    "budget delete": ("Food",),
    "budget set": ("Food", "100"),
    "sync key rotate": (),
    "sync schedule remove": (),
    "sync schedule set": (),
    "sync schedule show": (),
    "transactions categorize ml apply": (),
    "transactions categorize ml status": (),
    "transactions categorize ml train": (),
}


@pytest.mark.parametrize("path", STUB_COMMANDS)
def test_stub_message_names_no_repo_path(path: str) -> None:
    """An installed user has no checkout, so a repo path is not a next action."""
    result = runner.invoke(app, [*path.split(), *STUB_INVOCATIONS[path]])
    combined = result.stdout + result.stderr

    assert "docs/specs/" not in combined, f"{path} points the user at a repo path"
    assert ".md" not in combined, f"{path} names a source file"


@pytest.mark.parametrize("path", STUB_COMMANDS)
def test_stub_message_names_a_next_action(path: str) -> None:
    """The message tells the user what they can do instead."""
    result = runner.invoke(app, [*path.split(), *STUB_INVOCATIONS[path]])
    combined = result.stdout + result.stderr

    assert "moneybin --help" in combined, f"{path} offers the user no next action"


def test_partial_command_not_implemented_branch_names_no_repo_path() -> None:
    """Req 32 covers every reachable message, not only the whole-command stubs."""
    result = runner.invoke(app, ["transactions", "review", "--interactive"])
    combined = result.stdout + result.stderr

    assert "docs/specs/" not in combined
    assert ".md" not in combined


@pytest.mark.parametrize("path", STUB_COMMANDS)
def test_stub_exits_zero(path: str) -> None:
    """A stub is an intentional no-op, not a runtime error (req 33)."""
    result = runner.invoke(app, [*path.split(), *STUB_INVOCATIONS[path]])

    assert result.exit_code == 0, result.output


# --- Requirement 17: no user-facing message names an internal dependency ----

# SQLMesh is the transform engine. Users have "transforms", not a vendor.
INTERNAL_DEPENDENCIES = ("SQLMesh",)

CLI_PACKAGE = Path(cli_main.__file__).parent


@pytest.mark.parametrize("path", sorted(_walk_commands()))
def test_help_text_names_no_internal_dependency(path: str) -> None:
    """Help is the CLI's most-read surface; it speaks the user's vocabulary."""
    result = runner.invoke(app, [*path.split(), "--help"])

    assert result.exit_code == 0, result.output
    for dependency in INTERNAL_DEPENDENCIES:
        assert dependency not in result.stdout, f"`{path} --help` names {dependency}"


def _user_facing_strings(module: Path) -> list[str]:
    """Return every string literal this module passes to a user-facing call.

    Comments and internal docstrings are out of scope — requirement 17 is
    about what the user reads, not what a contributor reads.
    """
    tree = ast.parse(module.read_text())
    emitted: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if isinstance(target, ast.Attribute):
            called = target.attr
            owner = getattr(target.value, "id", "")
        else:
            continue
        is_log = owner == "logger" and called in {"info", "warning", "error"}
        is_echo = owner == "typer" and called == "echo"
        if not (is_log or is_echo):
            continue
        for argument in ast.walk(node):
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                emitted.append(argument.value)
    return emitted


def test_runtime_messages_name_no_internal_dependency() -> None:
    """The behavioural partner to the --help test above.

    ``--help`` cannot see a message that is only emitted mid-run, and those
    messages reach the same reader. Scanning the call sites is what covers
    them without executing every long-running command.
    """
    offenders: list[str] = []
    for module in sorted(CLI_PACKAGE.rglob("*.py")):
        for text in _user_facing_strings(module):
            if any(name in text for name in INTERNAL_DEPENDENCIES):
                offenders.append(f"{module.relative_to(CLI_PACKAGE)}: {text!r}")

    assert not offenders, (
        "user-facing messages name an internal dependency:\n" + "\n".join(offenders)
    )
