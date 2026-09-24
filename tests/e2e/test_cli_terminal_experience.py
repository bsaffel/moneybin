"""Real-terminal acceptance checks for the shared CLI presentation boundary.

The child programs intentionally use only synthetic strings and the production
terminal helpers.  They never resolve a profile, open a database, or contact a
service; a PTY still exercises the actual ``less`` child process and cursor
cleanup that a Click test runner cannot observe.
"""

from __future__ import annotations

import os
import shlex
import subprocess  # noqa: S404  # test starts controlled Python child fixtures
import sys
import termios
from collections.abc import Iterator
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import Any

import pexpect
import pytest
from typer._click import Command
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

from moneybin.cli.main import app

pytestmark = pytest.mark.e2e

PathKey = tuple[str, ...]


@pytest.fixture(autouse=True)
def close_terminal_children(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """Reap each PTY even when an expect call times out before normal cleanup."""
    spawn = pexpect.spawn

    def tracked_spawn(*args: Any, **kwargs: Any) -> Any:
        child = spawn(*args, **kwargs)
        request.addfinalizer(lambda: child.close(force=True))
        return child

    monkeypatch.setattr(pexpect, "spawn", tracked_spawn)


def _paths(text: str) -> frozenset[PathKey]:
    """Keep the full registration classification readable as command paths."""
    return frozenset(
        tuple(line.split()) for line in text.strip().splitlines() if line.strip()
    )


def _leaves(
    command: Command, prefix: tuple[str, ...] = ()
) -> Iterator[tuple[str, ...]]:
    """Yield registered leaves without invoking a command callback."""
    if isinstance(command, TyperGroup):
        for name, child in command.commands.items():
            yield from _leaves(child, (*prefix, name))
        return
    yield prefix


def _options(root: Command, path: tuple[str, ...]) -> set[str]:
    """Return one registered leaf's declared spellings without executing it."""
    command = root
    for segment in path:
        assert isinstance(command, TyperGroup), path
        command = command.commands[segment]
    return {option for param in command.params for option in param.opts}


def _spawn_terminal_child(program: str, *, rows: int, columns: int) -> Any:
    """Start one production-renderer fixture beneath a real controlling PTY."""
    child: Any = pexpect.spawn(
        sys.executable,
        ["-c", dedent(program)],
        encoding="utf-8",
        timeout=15,
        env={**os.environ, "TERM": "xterm-256color"},
    )
    child.setwinsize(rows, columns)
    return child


_SHELL_PROMPT = "__TASK8_SHELL__> "


def _spawn_interactive_shell(*, rows: int, columns: int) -> Any:
    """Start a real shell so recovery is observed after the child command exits."""
    child: Any = pexpect.spawn(
        "/bin/sh",
        ["-i"],
        encoding="utf-8",
        timeout=15,
        env={**os.environ, "TERM": "xterm-256color"},
    )
    child.setwinsize(rows, columns)
    child.expect(r"[$#] ")
    child.sendline(f"PS1={shlex.quote(_SHELL_PROMPT)}; export PS1")
    child.expect(_SHELL_PROMPT)
    return child


def _shell_termios(child: Any) -> str:
    """Read the controlling terminal state from the interactive shell itself."""
    child.sendline(
        "task8_termios=$(stty -g); task8_prefix=__TASK8; "
        'printf "%s_TERMIOS=%s__\\n" "$task8_prefix" "$task8_termios"'
    )
    child.expect(r"__TASK8_TERMIOS=([^_]+)__")
    state = str(child.match.group(1))
    child.expect(_SHELL_PROMPT)
    return state


def _run_in_shell(child: Any, program: str) -> None:
    """Run one controlled child and make the shell report its exit status and TTY."""
    command = (
        f"{shlex.quote(sys.executable)} -c {shlex.quote(dedent(program))}; "
        "task8_status=$?; task8_termios=$(stty -g); "
        "task8_prefix=__TASK8; "
        'printf "%s_STATUS=%s__\\n" "$task8_prefix" "$task8_status"; '
        'printf "%s_TERMIOS=%s__\\n" "$task8_prefix" "$task8_termios"'
    )
    child.sendline(command)


def _stable_termios(state: str) -> str:
    """Compare terminal attributes except macOS's kernel-managed pending input bit.

    ``PENDIN`` asks the line discipline to reprint input and clears after the
    Ctrl-C read itself; it is not a program-selected terminal mode.  The test
    does not issue ``stty`` or reset anything: ICANON, ECHO, ISIG, and every
    other observed field remain part of the exact pre/post comparison.
    """
    values = state.split(":")
    values[3] = f"{int(values[3], 16) & ~termios.PENDIN:x}"
    return ":".join(values)


def _assert_shell_recovered(child: Any, *, before: str, expected_status: int) -> None:
    """Require the interactive shell, not the child fixture, to prove recovery."""
    child.expect(r"__TASK8_STATUS=(\d+)__")
    assert int(child.match.group(1)) == expected_status
    child.expect(r"__TASK8_TERMIOS=([^_]+)__")
    assert _stable_termios(str(child.match.group(1))) == _stable_termios(before)
    child.expect(_SHELL_PROMPT)


def _assert_clean_exit(child: Any) -> None:
    """Collect the actual child status after its PTY closes."""
    child.close()
    assert child.exitstatus == 0


def test_registration_inventory_keeps_generated_reports_aliases_and_no_assets_leaf() -> (
    None
):
    """An accidental registration change must not evade the hand-written help list."""
    leaves = set(_leaves(get_command(app)))

    assert len(leaves) == 209
    assert ("assets",) not in leaves
    assert {
        ("sync", "connect"),
        ("sync", "connect-status"),
        ("reports", "spending-trend"),
        ("reports", "cash-flow"),
        ("reports", "recurring-subscriptions"),
        ("reports", "merchant-activity"),
        ("reports", "large-transactions"),
        ("reports", "balance-drift"),
        ("reports", "realized-fx"),
        ("mcp", "config", "path"),
        ("logs",),
    } <= leaves


def test_every_declared_finite_read_keeps_the_shared_human_and_agent_controls() -> None:
    """A new finite reader needs a real escape hatch, not a flag-count heuristic."""
    root = get_command(app)
    finite_reads = {
        ("profile", "list"),
        ("profile", "show"),
        ("accounts", "list"),
        ("accounts", "summary"),
        ("accounts", "get"),
        ("accounts", "resolve"),
        ("accounts", "balance", "show"),
        ("accounts", "balance", "history"),
        ("accounts", "balance", "list"),
        ("accounts", "balance", "reconcile"),
        ("accounts", "links", "pending"),
        ("accounts", "links", "history"),
        ("reports", "list"),
        ("reports", "run"),
        ("reports", "explain"),
        ("reports", "networth"),
        ("reports", "networth-history"),
        ("reports", "spending-trend"),
        ("reports", "cash-flow"),
        ("reports", "recurring-subscriptions"),
        ("reports", "merchant-activity"),
        ("reports", "large-transactions"),
        ("reports", "balance-drift"),
        ("reports", "realized-fx"),
        ("transactions", "list"),
        ("transactions", "audit"),
        ("transactions", "matches", "pending"),
        ("transactions", "matches", "history"),
        ("transactions", "notes", "list"),
        ("transactions", "tags", "list"),
        ("transactions", "splits", "list"),
        ("transactions", "categorize", "pending"),
        ("transactions", "categorize", "stats"),
        ("transactions", "categorize", "auto", "review"),
        ("transactions", "categorize", "auto", "stats"),
        ("transactions", "categorize", "auto", "rules"),
        ("transactions", "categorize", "rules", "list"),
        ("transactions", "categorize", "rules", "list-conflicts"),
        ("investments", "list"),
        ("investments", "holdings"),
        ("investments", "gains"),
        ("investments", "lots", "list"),
        ("investments", "prices", "list"),
        ("investments", "securities", "list"),
        ("investments", "securities", "links", "pending"),
        ("investments", "securities", "links", "history"),
        ("investments", "matches", "pending"),
        ("investments", "matches", "history"),
        ("categories", "list"),
        ("categories", "mappings", "pending"),
        ("merchants", "list"),
        ("merchants", "links", "pending"),
        ("merchants", "links", "history"),
        ("privacy", "status"),
        ("privacy", "log"),
        ("fx", "list"),
        ("system", "status"),
        ("system", "audit", "list"),
        ("system", "audit", "history"),
        ("transform", "status"),
        ("transform", "validate"),
        ("transform", "audit"),
        ("import", "history"),
        ("import", "status"),
        ("import", "formats", "list"),
        ("import", "formats", "show"),
        ("import", "inbox", "list"),
        ("import", "labels", "list"),
        ("sync", "status"),
        ("sync", "link-status"),
        ("sync", "connect-status"),
        ("gsheet", "list"),
        ("gsheet", "status"),
        ("logs",),
        ("stats",),
        ("review",),
        ("sql", "query"),
        ("export", "destination", "list"),
        ("db", "info"),
        ("db", "ps"),
        ("db", "migrate", "status"),
        ("mcp", "list-tools"),
        ("mcp", "list-prompts"),
    }

    classes: dict[str, frozenset[PathKey]] = {
        "finite_read": frozenset(finite_reads - {("logs",)}),
        "diagnostic": _paths(
            """
            fx rate
            db key show
            system doctor
            mcp config path
            import inbox path
            """
        ),
        "native": _paths(
            """
            db query
            import preview
            privacy redact
            """
        ),
        "artifact": _paths(
            """
            transactions categorize export-uncategorized
            export bundle
            export report
            db backup
            mcp install
            """
        ),
        "live_or_modeful": _paths(
            """
            logs
            mcp serve
            db shell
            db ui
            """
        ),
        "stub": _paths(
            """
            budget delete
            budget set
            sync key rotate
            sync schedule remove
            sync schedule set
            sync schedule show
            transactions categorize ml apply
            transactions categorize ml status
            transactions categorize ml train
            db key export
            db key import
            db key verify
            """
        ),
        "mutation_or_receipt": _paths(
            """
            accounts balance assert
            accounts balance assertion-delete
            accounts links run
            accounts links set
            accounts set
            categories create
            categories delete
            categories mappings set
            categories set
            db init
            db key rotate
            db kill
            db lock
            db migrate apply
            db restore
            db unlock
            demo
            export destination add local
            export destination add sheets
            export destination remove
            fx delete
            fx set
            gsheet auth
            gsheet connect
            gsheet disconnect
            gsheet pull
            gsheet reconnect
            import confirm
            import files
            import formats delete
            import labels add
            import labels remove
            import revert
            investments add
            investments lots select
            investments matches run
            investments prices delete
            investments prices pull
            investments prices set
            investments prices token
            investments securities add
            investments securities links set
            investments securities set
            merchants create
            merchants links run
            merchants links set
            privacy grant
            privacy revoke
            privacy revoke-all
            profile create
            profile delete
            profile set
            profile switch
            refresh
            reports create
            reports delete
            reports reclassify
            reports set
            sync connect
            sync disconnect
            sync link
            sync login
            sync logout
            sync pull
            synthetic generate
            synthetic reset
            system audit get
            system audit show
            system audit undo
            transactions categorize assist
            transactions categorize auto accept
            transactions categorize commit
            transactions categorize commit-from-file
            transactions categorize improve-ai
            transactions categorize rules apply
            transactions categorize rules create
            transactions categorize rules delete
            transactions categorize rules resolve
            transactions categorize run
            transactions create
            transactions matches backfill
            transactions matches run
            transactions matches set
            transactions matches undo
            transactions notes add
            transactions notes delete
            transactions notes edit
            transactions review
            transactions splits add
            transactions splits clear
            transactions splits remove
            transactions tags add
            transactions tags remove
            transactions tags rename
            transform apply
            transform plan
            transform restate
            transform seed
            """
        ),
    }
    leaves = set(_leaves(root))
    assert set().union(*classes.values()) == leaves
    for name, paths in classes.items():
        for other_name, other_paths in classes.items():
            if name < other_name:
                assert paths.isdisjoint(other_paths), (name, other_name)

    missing = {
        " ".join(path): {"--output", "--quiet", "--no-pager"} - _options(root, path)
        for path in classes["finite_read"]
        if {"--output", "--quiet", "--no-pager"} - _options(root, path)
    }

    assert missing == {}


def test_native_artifact_and_callback_modes_keep_their_explicit_exceptions() -> None:
    """Mode contracts, rather than a blanket all-flags rule, own these leaves."""
    root = get_command(app)
    assert isinstance(root, TyperGroup)
    assert "--no-pager" not in _options(root, ("db", "query"))
    assert "--output" not in _options(root, ("import", "preview"))
    assert "--output" not in _options(root, ("privacy", "redact"))
    assert "--output" not in _options(root, ("mcp", "install"))
    assert "--no-pager" not in _options(root, ("mcp", "serve"))
    assert "--follow" in _options(root, ("logs",))

    mcp = root.commands["mcp"]
    assert isinstance(mcp, TyperGroup)
    config = mcp.commands["config"]
    assert isinstance(config, TyperGroup)
    assert config.invoke_without_command
    assert config.callback is not None


def test_privacy_redact_is_a_plain_native_filter_without_pager_or_json() -> None:
    """The raw/stdin transform keeps its literal stream contract outside receipts."""
    from moneybin.cli.commands.privacy import app as privacy_app

    result = CliRunner().invoke(privacy_app, ["redact", "example merchant"])

    assert result.exit_code == 0, result.output
    assert result.stdout.endswith("\n")
    assert "\x1b" not in result.stdout


def test_bash_completion_lists_commands_and_options_without_running_profile_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Completion must stay a static registration read, never a profile operation."""
    import moneybin.cli.main as main

    def _unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "completion must not resolve a profile or set up observability"
        )

    monkeypatch.setattr(main, "resolve_profile", _unexpected)
    monkeypatch.setattr(main, "setup_observability", _unexpected)
    runner = CliRunner()

    commands = runner.invoke(
        app,
        [],
        env={
            "_MONEYBIN_COMPLETE": "complete_bash",
            "COMP_WORDS": "moneybin gsheet ",
            "COMP_CWORD": "2",
        },
    )
    options = runner.invoke(
        app,
        [],
        env={
            "_MONEYBIN_COMPLETE": "complete_bash",
            "COMP_WORDS": "moneybin gsheet list --",
            "COMP_CWORD": "3",
        },
    )

    assert commands.exit_code == 0, commands.output
    assert {"list", "status", "connect"} <= set(commands.output.splitlines())
    assert options.exit_code == 0, options.output
    assert {"--output", "--quiet", "--no-pager"} <= set(options.output.splitlines())


def test_short_answer_at_40_columns_returns_without_starting_a_pager() -> None:
    """A pager flash on a short answer makes ordinary commands feel broken."""
    child = _spawn_terminal_child(
        """
        import sys
        from moneybin.cli.output import emit_human_result
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from rich.text import Text

        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=False, settings=CLISettings(),
        )
        emit_human_result(Text("short answer"), policy=policy, finite_read=True)
        print("SHELL_RESTORED")
        """,
        rows=10,
        columns=40,
    )

    child.expect("short answer")
    child.expect("SHELL_RESTORED")
    child.expect(pexpect.EOF)
    assert "q return to shell" not in str(child.before or "")
    _assert_clean_exit(child)


def test_long_wide_answer_uses_actual_less_search_scroll_and_quit_at_80_columns() -> (
    None
):
    """The real pager must search, scroll, and return the shell after ``q``."""
    child = _spawn_terminal_child(
        """
        import sys
        from moneybin.cli.output import emit_human_result
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from rich.text import Text

        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=False, settings=CLISettings(),
        )
        text = "\\n".join(
            f"row {number:03d}  long-column-{'x' * 120} {'needle' if number == 31 else ''}"
            for number in range(80)
        )
        emit_human_result(Text(text), policy=policy, finite_read=True, wide=True)
        print("SHELL_RESTORED")
        """,
        rows=10,
        columns=80,
    )

    child.expect("row 000")
    child.send("/needle\n")
    child.expect("needle")
    child.send(" ")
    child.send("\x1b[C")
    child.send("q")
    child.expect("SHELL_RESTORED")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)


def test_actual_shell_recovers_termios_after_pager_quit() -> None:
    """A real shell must regain its original terminal state after `less` exits."""
    child = _spawn_interactive_shell(rows=10, columns=80)
    before = _shell_termios(child)
    _run_in_shell(
        child,
        """
        import sys
        from moneybin.cli.output import emit_human_result
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from rich.text import Text

        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=False, settings=CLISettings(),
        )
        emit_human_result(
            Text("\\n".join(f"row {number:03d}" for number in range(80))),
            policy=policy,
            finite_read=True,
        )
        """,
    )

    child.expect("row 000")
    child.send("/row 031\n")
    child.expect("row 031")
    child.send(" ")
    child.send("q")
    _assert_shell_recovered(child, before=before, expected_status=0)
    child.sendline("exit")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)


def test_missing_pager_prints_complete_answer_and_restores_the_shell() -> None:
    """A missing external pager is a presentation fallback, never lost data."""
    child = _spawn_terminal_child(
        """
        import os
        import sys
        from moneybin.cli.output import emit_human_result
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from rich.text import Text

        os.environ["PATH"] = "/does-not-exist"
        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=False, settings=CLISettings(),
        )
        emit_human_result(Text("\\n".join(f"row {number:03d}" for number in range(80))), policy=policy, finite_read=True)
        print("SHELL_RESTORED")
        """,
        rows=10,
        columns=120,
    )

    child.expect("row 000")
    child.expect("row 079")
    child.expect("SHELL_RESTORED")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)


def test_no_color_real_terminal_preserves_the_summary_without_escape_sequences() -> (
    None
):
    """NO_COLOR removes decoration only; a terminal answer keeps its values."""
    child = _spawn_terminal_child(
        """
        import os
        import sys
        from moneybin.cli.output import emit_human_result
        from moneybin.cli.render import build_summary
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings

        os.environ["NO_COLOR"] = "1"
        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=False, settings=CLISettings(),
        )
        emit_human_result(
            build_summary([("Loaded", "28 transactions")], title="Sync complete"),
            policy=policy,
            finite_read=False,
            receipt=True,
        )
        """,
        rows=24,
        columns=80,
    )

    transcript = StringIO()
    child.logfile_read = transcript
    child.expect("Sync complete")
    child.expect("Loaded: 28 transactions")
    child.expect(pexpect.EOF)
    assert "\x1b" not in transcript.getvalue()
    _assert_clean_exit(child)


def test_redirected_stdout_and_both_streams_keep_results_and_static_stages(
    tmp_path: Path,
) -> None:
    """Redirection disables pager/style but preserves data and stage disclosure."""
    output_path = tmp_path / "answer.txt"
    stderr_path = tmp_path / "diagnostics.txt"
    program = dedent(
        """
        import sys
        from moneybin.cli.output import emit_human_result
        from moneybin.cli.progress import operation_progress
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from moneybin.progress import ProgressEvent
        from rich.text import Text

        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=False, settings=CLISettings(),
        )
        emit_human_result(Text("canonical -1,234.50 USD"), policy=policy, finite_read=True)
        with operation_progress(policy) as report:
            report(ProgressEvent("Static synthetic stage"))
        """
    )
    shell_command = (
        f"{shlex.quote(sys.executable)} -c {shlex.quote(program)} "
        f"> {shlex.quote(str(output_path))}"
    )
    child = pexpect.spawn(
        "/bin/sh",
        ["-c", shell_command],
        encoding="utf-8",
        timeout=15,
        env={**os.environ, "TERM": "xterm-256color"},
    )
    child.expect("Static synthetic stage")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)
    assert output_path.read_text() == "canonical -1,234.50 USD\n"
    assert "\x1b" not in output_path.read_text()

    with output_path.open("w") as stdout, stderr_path.open("w") as stderr:
        completed = subprocess.run(  # noqa: S603  # test executes this module's fixed synthetic child
            [sys.executable, "-c", program],
            check=False,
            stdout=stdout,
            stderr=stderr,
            text=True,
        )
    assert completed.returncode == 0
    assert output_path.read_text() == "canonical -1,234.50 USD\n"
    assert stderr_path.read_text() == "Static synthetic stage\n"
    assert "\x1b" not in output_path.read_text() + stderr_path.read_text()


def test_escape_cancels_the_real_inline_selector_and_restores_the_shell() -> None:
    """The selector's documented Escape path must return to the shell cleanly."""
    child = _spawn_terminal_child(
        """
        import sys
        import typer
        from moneybin.cli.prompts import Choice, choose_required
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings

        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=True, settings=CLISettings(),
        )
        try:
            choose_required(
                None,
                choices=(Choice("account_1", "Everyday Checking"),),
                flag="--account",
                policy=policy,
            )
        except typer.Abort:
            print("CANCELLED")
        print("SHELL_RESTORED")
        """,
        rows=24,
        columns=120,
    )

    child.expect("Choose --account:")
    child.send("\x1b")
    child.expect("CANCELLED")
    child.expect("SHELL_RESTORED")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)


def test_actual_shell_recovers_termios_after_prompt_escape() -> None:
    """Escape returns Click control to an interactive shell without raw-mode drift."""
    child = _spawn_interactive_shell(rows=24, columns=120)
    before = _shell_termios(child)
    _run_in_shell(
        child,
        """
        import typer
        from moneybin.cli.prompts import Choice, choose_required
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from typer.main import get_command

        prompt_app = typer.Typer()

        @prompt_app.command()
        def select() -> None:
            import sys

            choose_required(
                None,
                choices=(Choice("account_1", "Everyday Checking"),),
                flag="--account",
                policy=resolve_terminal_policy(
                    stdin=sys.stdin,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    output="text",
                    quiet=False,
                    no_pager=True,
                    settings=CLISettings(),
                ),
            )

        get_command(prompt_app).main(args=[], standalone_mode=True)
        """,
    )

    child.expect("Choose --account:")
    child.send("\x1b")
    _assert_shell_recovered(child, before=before, expected_status=1)
    child.sendline("exit")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)


def test_ctrl_c_stops_progress_and_returns_a_clean_terminal() -> None:
    """Interrupted progress must release its live display before the shell resumes."""
    child = _spawn_terminal_child(
        """
        import sys
        import time
        from moneybin.cli.progress import operation_progress
        from moneybin.cli.terminal import resolve_terminal_policy
        from moneybin.config import CLISettings
        from moneybin.progress import ProgressEvent

        policy = resolve_terminal_policy(
            stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, output="text",
            quiet=False, no_pager=True, settings=CLISettings(),
        )
        try:
            with operation_progress(policy) as report:
                report(ProgressEvent("Scanning synthetic rows"))
                time.sleep(30)
        except KeyboardInterrupt:
            print("INTERRUPTED")
        print("SHELL_RESTORED")
        """,
        rows=24,
        columns=120,
    )

    child.expect("Scanning synthetic rows")
    child.sendcontrol("c")
    child.expect("INTERRUPTED")
    child.expect("SHELL_RESTORED")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)


def test_actual_sync_wrapper_interrupts_with_balanced_cursor_and_shell_recovery() -> (
    None
):
    """The real sync interruption path must clean progress and exit 130 in a PTY."""
    child = _spawn_interactive_shell(rows=24, columns=120)
    transcript = StringIO()
    child.logfile_read = transcript
    before = _shell_termios(child)
    _run_in_shell(
        child,
        """
        import time
        from contextlib import contextmanager
        from typer.main import get_command
        from moneybin.cli.commands import sync
        from moneybin.progress import ProgressEvent

        class SyntheticService:
            def pull(self, *, progress, **_: object) -> object:
                progress(ProgressEvent("Scanning " + "synthetic rows"))
                time.sleep(30)
                raise AssertionError("the interruption should arrive before this")

        @contextmanager
        def synthetic_service():
            yield SyntheticService()

        sync._build_sync_service = synthetic_service
        get_command(sync.app).main(args=["pull"], standalone_mode=True)
        """,
    )

    child.expect("Scanning synthetic rows")
    child.sendcontrol("c")
    child.expect("Sync cancelled")
    _assert_shell_recovered(child, before=before, expected_status=130)
    raw = transcript.getvalue()
    assert raw.count("\x1b[?25l") >= 1
    assert raw.count("\x1b[?25l") == raw.count("\x1b[?25h")
    child.sendline("exit")
    child.expect(pexpect.EOF)
    _assert_clean_exit(child)
