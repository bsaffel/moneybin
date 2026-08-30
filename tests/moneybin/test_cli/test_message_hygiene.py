"""User-facing message hygiene (cli-output-coherence reqs 16, 17, 19, 31-33).

Requirement 31 hides whole-command stubs from ``--help`` while keeping them
invocable. The enumerations below are deliberate rather than a grep for
``_not_implemented``, which is wrong in both directions: it over-matches
``transactions review``, which reaches that helper only on ``--interactive``
and would lose working behaviour if hidden, and it under-matches the ``db key``
trio, which predates the helper and inlines its own message.
"""

import ast
from pathlib import Path

import click
import pytest
from typer.main import get_command
from typer.testing import CliRunner

import moneybin
from moneybin.cli.commands.stubs import (
    _not_implemented,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.cli.main import app
from moneybin.logging.config import (
    _CONSOLE_SUPPRESSED_PREFIXES,  # pyright: ignore[reportPrivateUsage]
)
from tests.moneybin.test_mcp.test_capability_parity import (
    UNIMPLEMENTED_CLI_INVOCATIONS,
    UNIMPLEMENTED_EXIT_ONE_CLI_INVOCATIONS,
)

runner = CliRunner()

# Whole-command stubs: every path through them is unimplemented. Imported
# rather than restated — `test_capability_parity` owns the one enumeration,
# and it is the file a contributor adding a stub must already edit, so a stub
# cannot reach that list while escaping the assertions below.
STUB_COMMANDS = tuple(sorted(UNIMPLEMENTED_CLI_INVOCATIONS))

# Whole-command stubs that predate the shared helper and exit 1 rather than 0.
# MB-37 preserves exit codes, so the divergence is recorded, not unified.
EXIT_ONE_STUB_COMMANDS = tuple(sorted(UNIMPLEMENTED_EXIT_ONE_CLI_INVOCATIONS))

ALL_STUB_COMMANDS = STUB_COMMANDS + EXIT_ONE_STUB_COMMANDS

# Groups whose every command is a stub. An empty group still promises a
# capability the CLI does not have, which is the trust problem req 31 exists
# to fix, so the group is hidden alongside its leaves.
STUB_GROUPS = (
    "budget",
    "sync key",
    "sync schedule",
    "transactions categorize ml",
)

# A group holding both stubs and working commands stays visible — hiding it
# would take `db key show` and `db key rotate` out of --help with it.
MIXED_GROUPS = ("db key",)

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


def _walk_groups() -> dict[str, click.Group]:
    """Map every command group path to its click group.

    Separate from ``_walk_commands`` because a group built with
    ``no_args_is_help=True`` does not set ``invoke_without_command``, so it
    never lands in the executable-command map.
    """
    found: dict[str, click.Group] = {}

    def walk(command: click.Command, prefix: tuple[str, ...]) -> None:
        if not isinstance(command, click.Group):
            return
        if prefix:
            found[" ".join(prefix)] = command
        for name, child in command.commands.items():
            walk(child, (*prefix, name))

    walk(get_command(app), ())
    return found


@pytest.mark.parametrize("path", ALL_STUB_COMMANDS)
def test_stub_command_is_hidden_from_help(path: str) -> None:
    """A whole-command stub is registered but never advertised."""
    command = _walk_commands()[path]

    assert command.hidden, f"{path} is a stub and must not appear in --help"


@pytest.mark.parametrize("path", STUB_GROUPS)
def test_group_of_only_stubs_is_hidden(path: str) -> None:
    """Hiding the leaves alone leaves a group that lists nothing."""
    group = _walk_groups()[path]

    assert group.hidden, f"`{path}` advertises a group with no working command"


@pytest.mark.parametrize("path", STUB_GROUPS)
def test_hidden_group_is_still_invocable(path: str) -> None:
    """Hiding reserves the namespace; it does not remove it."""
    result = runner.invoke(app, [*path.split(), "--help"])

    assert result.exit_code == 0, result.output


@pytest.mark.parametrize("path", MIXED_GROUPS)
def test_group_with_a_working_command_stays_visible(path: str) -> None:
    """The other direction: a group is hidden for having no working command.

    Without this, an implementation that hides every group containing any
    stub passes the test above while deleting working commands from --help.
    """
    group = _walk_groups()[path]

    assert not group.hidden, f"`{path}` has working commands and must stay in --help"
    assert any(not command.hidden for command in group.commands.values()), (
        f"`{path}` was expected to hold at least one visible command"
    )


@pytest.mark.parametrize("path", PARTIALLY_IMPLEMENTED_COMMANDS)
def test_partially_implemented_command_stays_visible(path: str) -> None:
    """Hiding a command with any working path would remove real behaviour."""
    command = _walk_commands()[path]

    assert not command.hidden, f"{path} has working paths and must stay in --help"


@pytest.mark.parametrize("path", ALL_STUB_COMMANDS)
def test_stub_name_absent_from_parent_help(path: str) -> None:
    """The stub's name does not appear in its parent group's --help listing."""
    parent, _, leaf = path.rpartition(" ")
    result = runner.invoke(app, [*parent.split(), "--help"])

    assert result.exit_code == 0, result.output
    assert leaf not in result.stdout, f"{path} is advertised by `{parent} --help`"


# Arguments needed to reach each stub's body, from the same two maps.
STUB_INVOCATIONS = {
    **UNIMPLEMENTED_CLI_INVOCATIONS,
    **UNIMPLEMENTED_EXIT_ONE_CLI_INVOCATIONS,
}


@pytest.mark.parametrize("path", ALL_STUB_COMMANDS)
def test_stub_message_names_no_repo_path(path: str) -> None:
    """An installed user has no checkout, so a repo path is not a next action."""
    result = runner.invoke(app, [*path.split(), *STUB_INVOCATIONS[path]])
    combined = result.stdout + result.stderr

    assert "docs/specs/" not in combined, f"{path} points the user at a repo path"
    assert ".md" not in combined, f"{path} names a source file"


@pytest.mark.parametrize("path", ALL_STUB_COMMANDS)
def test_stub_message_names_a_next_action(path: str) -> None:
    """The message tells the user what they can do instead.

    Covers both stub families: two message shapes for one situation is the
    coherence failure the spec's "one way to do each thing" rule forbids.
    """
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


@pytest.mark.parametrize("path", EXIT_ONE_STUB_COMMANDS)
def test_exit_one_stub_keeps_its_exit_code(path: str) -> None:
    """The `db key` stubs shipped exiting 1; MB-37 preserves exit codes.

    Unifying them on the 0 policy would be a public-contract change to a
    published command, which this pull request is not making. The split is
    pinned here so it stays a recorded decision rather than an accident.
    """
    result = runner.invoke(app, [*path.split(), *STUB_INVOCATIONS[path]])

    assert result.exit_code == 1, result.output


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_stub_message_survives_every_supported_log_level(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    level: str,
) -> None:
    """No supported log level may turn a stub into a silent no-op.

    All five of ``LoggingConfig.level``'s values must show it, not just the
    default. ``ERROR`` and ``CRITICAL`` drop a WARNING record outright, and the
    three ``db key`` stubs then exit 1 having printed nothing — a bare failure
    code with no reason, which is the whole of what req 32 forbids. Those three
    explained themselves through an unconditional ``typer.echo`` before req 31
    moved them onto the shared helper, so a logger-backed helper regresses
    exactly the range they were previously immune to.

    Setting the level is what makes this a guard rather than a tautology: the
    helper writes with ``typer.echo(err=True)`` and ignores it, so the
    parameters all pass — but they fail at ``ERROR``/``CRITICAL`` the moment
    anyone routes the message back through a logger.

    Calls the helper rather than a command: ``setup_logging(cli_mode=True)``
    rebuilds the root handlers during ``runner.invoke``. That every stub reaches
    this helper is what the parametrized end-to-end tests above establish; this
    one owns the channel the helper emits on.
    """
    with caplog.at_level(level, logger="moneybin.cli.commands.stubs"):
        _not_implemented("scheduled sync")

    err = capsys.readouterr().err
    assert "not yet implemented" in err.lower(), (
        f"the stub message is dropped when the log level is {level}"
    )
    assert "moneybin --help" in err, (
        f"the stub's next action is dropped when the log level is {level}"
    )
    assert "scheduled sync" in err, (
        "the stub message no longer names the feature it stands in for"
    )


# --- Requirement 17: no user-facing message names an internal dependency ----

# SQLMesh is the transform engine and SQLGlot its SQL parser. Users have
# "transforms", not a vendor.
#
# Matched case-insensitively. The first version of this guard compared the
# names verbatim, and `database.py` reached the user with a lowercase
# "sqlmesh migrate failed" — scanned by this test, and walked straight past.
# Casing is not the rule; naming the dependency is.
INTERNAL_DEPENDENCIES = ("SQLMesh", "SQLGlot")


def names_an_internal_dependency(text: str) -> bool:
    """Whether ``text`` names a dependency the user did not choose."""
    folded = text.casefold()
    return any(name.casefold() in folded for name in INTERNAL_DEPENDENCIES)


# `moneybin logs sqlmesh` is the one place the name is legitimately user-facing:
# it is a value the user types and the actual prefix of the log file on disk
# (`sqlmesh-<session>.log`), not vendor vocabulary in prose. Renaming it is a
# CLI public-contract change that also orphans existing log files, so it is out
# of scope for message hygiene.
#
# Declared, not inferred — and the test below asserts the exemption is still
# *needed*, so a stale entry fails rather than quietly widening the guard.
IDENTIFIER_EXEMPT_HELP = frozenset({"logs"})

SRC_ROOT = Path(moneybin.__file__).parent

# Constructors whose fields a renderer prints verbatim, making their string
# arguments user-facing even though the construction site emits nothing itself.
# `system doctor` prints `InvariantResult.name` and `.detail` straight to the
# terminal (`cli/commands/system/doctor.py:118-120`).
RENDERED_VERBATIM_CARRIERS = frozenset({"InvariantResult"})


@pytest.mark.parametrize("path", sorted(_walk_commands()))
def test_help_text_names_no_internal_dependency(path: str) -> None:
    """Help is the CLI's most-read surface; it speaks the user's vocabulary."""
    result = runner.invoke(app, [*path.split(), "--help"])

    assert result.exit_code == 0, result.output
    if path in IDENTIFIER_EXEMPT_HELP:
        assert names_an_internal_dependency(result.stdout), (
            f"`{path} --help` no longer names an internal dependency — drop it "
            f"from IDENTIFIER_EXEMPT_HELP rather than leaving the guard widened"
        )
        return
    assert not names_an_internal_dependency(result.stdout), (
        f"`{path} --help` names an internal dependency"
    )


def _user_facing_strings(module: Path) -> list[tuple[str, bool]]:
    """Return this module's user-facing string literals, each tagged by channel.

    The flag is ``True`` when the string reaches the user only if the module's
    logger does — which is what the console denylist can silence. A
    ``typer.echo`` or ``InvariantResult`` string is written whatever the logging
    config says, so it is tagged ``False`` and stays in scope for every module.

    Comments and internal docstrings are out of scope — requirement 17 is
    about what the user reads, not what a contributor reads.

    Two known blind spots, both narrower than they look:

    - A message assembled in a local variable and then logged reaches the user
      but not this scan. Widening to arbitrary dataflow needs more than the AST.
    - Text raised on an exception (``raise RuntimeError(f"...")``) reaches the
      user through the CLI error path, and this scan never visits those call
      sites at all.

    ``InvariantResult`` is scanned because it is neither: ``system doctor``
    prints its ``name`` and ``detail`` verbatim
    (``cli/commands/system/doctor.py:118-120``), so it is a user-facing call by
    the same standard as ``logger.warning`` — it just renders one layer later.
    It is listed by name rather than matched structurally because a rule like
    "every dataclass whose fields get echoed" is not decidable from one module's
    AST.
    """
    tree = ast.parse(module.read_text())
    emitted: list[tuple[str, bool]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if isinstance(target, ast.Name):
            # A rendered-verbatim carrier, not a call that emits on its own.
            if target.id not in RENDERED_VERBATIM_CARRIERS:
                continue
            via_logger = False
        elif isinstance(target, ast.Attribute):
            called = target.attr
            owner = getattr(target.value, "id", "")
            is_log = owner == "logger" and called in {"info", "warning", "error"}
            is_echo = owner == "typer" and called in {"echo", "secho"}
            if not (is_log or is_echo):
                continue
            via_logger = is_log
        else:
            continue
        for argument in ast.walk(node):
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                emitted.append((argument.value, via_logger))
    return emitted


def _logger_name(module: Path) -> str:
    """The logger name ``logging.getLogger(__name__)`` produces in this module."""
    relative = module.relative_to(SRC_ROOT.parent).with_suffix("")
    parts = relative.parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _reaches_the_console(module: Path) -> bool:
    """Whether this module's log records survive to the user's stderr.

    ``_CONSOLE_SUPPRESSED_PREFIXES`` is a denylist, so a module is visible
    unless it is named there. Deriving the exemption from that tuple rather
    than from a second hand-written list is what keeps the two in step: adding
    a prefix there silences this check for the same module, and nothing else.
    """
    name = _logger_name(module)
    return not any(
        name == prefix or name.startswith(f"{prefix}.")
        for prefix in _CONSOLE_SUPPRESSED_PREFIXES
    )


def test_runtime_messages_name_no_internal_dependency() -> None:
    """The behavioural partner to the --help test above.

    ``--help`` cannot see a message that is only emitted mid-run, and those
    messages reach the same reader. Scanning the call sites is what covers
    them without executing every long-running command.

    Scoped to every console-visible module, not to ``moneybin.cli``: the user
    reads one stderr stream, and the services behind a command write to it on
    the same terms. A CLI-only scan passes while ``transform apply`` prints
    "Running SQLMesh transforms" directly above "Transforms applied".

    The console denylist exempts a module's *logger* strings only. A
    ``typer.echo`` or ``InvariantResult`` string is written whatever the logging
    config says, so suppressing a prefix must not take those out of scope —
    otherwise adding one prefix silently stops guarding two other channels.
    """
    offenders: list[str] = []
    for module in sorted(SRC_ROOT.rglob("*.py")):
        logger_visible = _reaches_the_console(module)
        for text, via_logger in _user_facing_strings(module):
            if via_logger and not logger_visible:
                continue
            if names_an_internal_dependency(text):
                offenders.append(f"{module.relative_to(SRC_ROOT)}: {text!r}")

    assert not offenders, (
        "user-facing messages name an internal dependency:\n" + "\n".join(offenders)
    )
