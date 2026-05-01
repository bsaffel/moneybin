# CLI Symmetry Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tighten the MoneyBin CLI interface to match conventions from `docker`, `kubectl`, `journalctl`, and `git` — required positional arguments where appropriate, no redundant subcommand layers, consistent flags across read-only commands, and a `--help` surface that's free of side effects.

**Architecture:** Single PR, breaking changes accepted (pre-alpha). The refactor has three layers: (1) collapse single-action groups (`stats`, `logs`) into leaf commands, (2) promote multi-action verb-prefixed commands into proper sub-groups (`categorize auto-*`, `db rotate-key`, `import list-formats/show-format/delete-format`, `sync rotate-key`), (3) standardize the read-only-command surface with `-o/--output` and `-q/--quiet`. The `.claude/rules/cli.md` rule is updated in the same PR so the new conventions are documented.

**Tech Stack:** Typer 0.x (CLI framework), Pytest with `CliRunner`, Python 3.12, DuckDB. No new dependencies.

---

## Scope Summary

### Breaking changes (user-facing)

| Old | New |
|---|---|
| `moneybin stats show` | `moneybin stats` |
| `moneybin logs tail --stream cli -n 50` | `moneybin logs cli -n 50` |
| `moneybin logs tail --stream all` | (removed — pick a stream) |
| `moneybin logs path` | `moneybin logs --print-path` |
| `moneybin logs clean --older-than 30d` | `moneybin logs --prune --older-than 30d` |
| `moneybin categorize auto-review` | `moneybin categorize auto review` |
| `moneybin categorize auto-confirm` | `moneybin categorize auto confirm` |
| `moneybin categorize auto-stats` | `moneybin categorize auto stats` |
| `moneybin categorize auto-rules` | `moneybin categorize auto rules` |
| `moneybin categorize stats` | `moneybin categorize summary` |
| `moneybin import list-formats` | `moneybin import formats list` |
| `moneybin import show-format <name>` | `moneybin import formats show <name>` |
| `moneybin import delete-format <name>` | `moneybin import formats delete <name>` |
| `moneybin db key` | `moneybin db key show` |
| `moneybin db rotate-key` | `moneybin db key rotate` |
| `moneybin sync rotate-key` | `moneybin sync key rotate` |

### Additions

- `moneybin db key {export,import,verify}` — stubs that exit 1 with "not yet implemented" message; tracked in `docs/followups.md`
- `--until` flag on `moneybin logs`
- Absolute timestamp parsing for `--since` (e.g. `2026-04-01T00:00:00Z`)
- `-o/--output {text,json}` and `-q/--quiet` on every read-only command
- `--print-path` flag on `moneybin logs`
- `--prune --older-than DURATION [--dry-run]` flag-mode on `moneybin logs`

### Bugfixes

- `moneybin <cmd> --help` no longer triggers the first-run profile wizard.
- Error messages from `logger.error()` confirmed to land on stderr (fd 2), not stdout.

---

## File Inventory

### Modified

- `src/moneybin/cli/main.py` — fix `--help` wizard bug; update sub-typer registrations for `stats` (group → leaf) and any rename touches.
- `src/moneybin/cli/commands/stats.py` — collapse to a single function exposed via `app.command()` on the root app rather than a sub-group.
- `src/moneybin/cli/commands/logs.py` — rewrite: single leaf command with required positional `stream`, flag-mode `--prune` and `--print-path`, kill `--stream all` merged path, add `--until`, accept absolute `--since`.
- `src/moneybin/cli/commands/categorize.py` — promote `auto-*` to sub-group `auto`; rename `stats` to `summary`; add `-o/-q` to read commands.
- `src/moneybin/cli/commands/import_cmd.py` — promote `*-format(s)` commands to sub-group `formats`; add `-o/-q` to read commands.
- `src/moneybin/cli/commands/db.py` — promote `key` and `rotate-key` to sub-group `key {show,rotate,export,import,verify}`; add `-o/-q` to read commands.
- `src/moneybin/cli/commands/sync.py` — promote `rotate-key` to `key rotate` sub-group; add `-o/-q` on `status`.
- `src/moneybin/cli/commands/profile.py` — add `-o/-q` to `list`/`show`.
- `src/moneybin/cli/commands/matches.py` — add `-o/-q` to `history`.
- `src/moneybin/cli/commands/mcp.py` — add `-o/-q` to `list-tools`/`list-prompts`.
- `src/moneybin/cli/commands/migrate.py` — add `-o/-q` to `status`.
- `src/moneybin/cli/utils.py` — add a `OutputFormat` Literal and a small helper for honoring `-q/--quiet` (suppresses logger.info but keeps logger.error/warning).

### Created

- `src/moneybin/cli/output.py` — *(if not already containing common output helpers)* OR extend the existing `output.py` with `OutputFormat`, `quiet_mode()` context helper. Inspect first.
- `tests/moneybin/test_cli/test_help_no_wizard.py` — verify `--help` is side-effect-free across all command groups.
- `tests/moneybin/test_cli/test_cli_logs_leaf.py` — verify new `logs` leaf shape.
- `tests/moneybin/test_cli/test_cli_db_key.py` — verify new `db key` sub-group.
- `tests/moneybin/test_cli/test_cli_categorize_auto.py` — verify new `categorize auto` sub-group (existing `test_categorize_auto_commands.py` will need updating, not replacing).
- `tests/moneybin/test_cli/test_cli_import_formats.py` — verify new `import formats` sub-group.
- `tests/moneybin/test_cli/test_cli_output_quiet.py` — verify `-o/--output` and `-q/--quiet` standardization.

### Updated

- `.claude/rules/cli.md` — add: leaf-command exception, `-o`/`-q` requirement, `--help` side-effect contract, exit-code/stderr contract, sub-group-promotion rule.
- `docs/followups.md` — add deferred-work section for `db key {export,import,verify}` implementation.
- `README.md` — update CLI examples to match new shapes.
- `tests/e2e/test_e2e_help.py` — update `_HELP_COMMANDS` for new groups (`db key`, `import formats`, `categorize auto`, `sync key`).
- `tests/e2e/test_e2e_readonly.py` — adjust any moved commands.
- `tests/moneybin/test_cli/test_categorize_auto_commands.py` — adapt to new sub-group.
- `tests/moneybin/test_stats_command.py` — adapt to leaf shape.
- `tests/moneybin/test_cli/test_cli_logs.py` — heavy revision for new shape.

### Removed

None. (All renames are in-place; no file deletions.)

---

## Conventions Used in This Plan

- **Profile setup for unit tests:** Use `CliRunner` with `MagicMock` for `get_database()`. Pass `MONEYBIN_PROFILE=test` env var via `monkeypatch.setenv()` and create a temp profile directory when the command runs through the main app entrypoint.
- **TDD discipline:** Write the failing test first, run it to confirm the fail message matches expectations (the test will fail with `AttributeError` or `unexpected extra argument` before the rename, then pass after).
- **Commit cadence:** Commit per task (group of related steps), not per step. Commit messages follow the project's convention (imperative subject, under 72 chars).
- **Branch:** `refactor/cli-require-args-for-logs-stats` (already created in the worktree).

---

## Task 1: Fix `--help` short-circuits profile wizard

**Why first:** Every subsequent task involves running CLI commands with `--help` to verify shape. Today, running `--help` against an unknown profile triggers the first-run wizard. Fix it now so dev iteration is friction-free.

**Files:**
- Modify: `src/moneybin/cli/main.py:46-149`
- Test: `tests/moneybin/test_cli/test_help_no_wizard.py` (new)

- [ ] **Step 1.1: Write the failing test**

```python
"""Verify --help is side-effect free across all command groups."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


_GROUPS = [
    [],  # top-level
    ["profile"],
    ["import"],
    ["sync"],
    ["categorize"],
    ["matches"],
    ["transform"],
    ["synthetic"],
    ["track"],
    ["stats"],
    ["export"],
    ["mcp"],
    ["db"],
    ["logs"],
]


@pytest.mark.unit
@pytest.mark.parametrize("argv", _GROUPS, ids=lambda a: " ".join(a) or "root")
def test_help_does_not_trigger_first_run_wizard(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    argv: list[str],
) -> None:
    """`moneybin <group> --help` must not prompt for profile setup or write files."""
    # Point the home dir at an empty tmp dir so any wizard would have to write here.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    monkeypatch.delenv("MONEYBIN_PROFILE", raising=False)

    result = runner.invoke(app, [*argv, "--help"])

    assert result.exit_code == 0, f"--help failed: {result.output}"
    assert "First name" not in result.stdout
    assert "First name" not in (result.stderr or "")
    # No profile dir should have been created
    assert not list((tmp_path / ".config").rglob("profiles")), (
        "wizard wrote profile data during --help"
    )
```

- [ ] **Step 1.2: Run the test to verify it fails**

```bash
uv run pytest tests/moneybin/test_cli/test_help_no_wizard.py -v
```

Expected: FAIL — wizard prompt appears in output, or test hangs (the wizard reads stdin).

- [ ] **Step 1.3: Patch `main_callback` in `src/moneybin/cli/main.py`**

Add a `--help`/`-h` short-circuit at the top of `main_callback`. Insert *immediately* after the docstring, before the existing `is_profile_cmd` line (~line 81):

```python
import sys

# --help is documentation; it must be inert. Skip profile resolution
# entirely when help is requested so we don't trigger the first-run
# wizard, write profile dirs, or fail when MONEYBIN_PROFILE points at
# a non-existent profile.
_HELP_TOKENS = {"--help", "-h"}
if any(tok in sys.argv for tok in _HELP_TOKENS):
    return
```

- [ ] **Step 1.4: Run the test to verify it passes**

```bash
uv run pytest tests/moneybin/test_cli/test_help_no_wizard.py -v
```

Expected: PASS for all 14 parametrized cases.

- [ ] **Step 1.5: Run full unit test suite to confirm no regressions**

```bash
uv run pytest tests/moneybin -v -m "not integration and not e2e" 2>&1 | tail -20
```

Expected: green.

- [ ] **Step 1.6: Commit**

```bash
git add src/moneybin/cli/main.py tests/moneybin/test_cli/test_help_no_wizard.py
git commit -m "Skip profile resolution on --help to keep help inert"
```

---

## Task 2: Convert `stats` to a leaf command

**Why now:** Smallest structural change; sets the precedent for "single-action group becomes leaf" used in Task 3.

**Files:**
- Modify: `src/moneybin/cli/commands/stats.py` (whole file)
- Modify: `src/moneybin/cli/main.py:184` (registration)
- Test: `tests/moneybin/test_stats_command.py` (revise)

- [ ] **Step 2.1: Update test expectations to leaf shape**

Replace the `TestTyperSingleCommandCollapse` class (lines 19-54) and the `TestStatsShow` class header in `tests/moneybin/test_stats_command.py` with:

```python
"""Tests for the moneybin stats CLI command."""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app as root_app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


class TestStatsLeafShape:
    """Stats is a leaf command — no `show` subcommand, bare invocation runs it."""

    @pytest.mark.unit
    def test_bare_invocation_runs_command(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_PROFILE", "test")
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []
        with (
            patch("moneybin.cli.utils.get_database", return_value=mock_db),
            patch("moneybin.cli.main.ensure_default_profile", return_value="test"),
            patch("moneybin.cli.main.set_current_profile"),
            patch("moneybin.cli.main.get_base_dir") as gbd,
        ):
            gbd.return_value.__truediv__.return_value.__truediv__.return_value.exists.return_value = True
            result = runner.invoke(root_app, ["stats"])

        assert result.exit_code == 0, result.output
        assert "No metrics" in result.stdout

    @pytest.mark.unit
    def test_show_subcommand_no_longer_exists(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_PROFILE", "test")
        result = runner.invoke(root_app, ["stats", "show"])
        assert result.exit_code != 0
        assert (
            "no such option" in result.stdout.lower()
            or "extra argument" in result.stdout.lower()
            or "unknown" in result.stdout.lower()
            or "got unexpected" in result.stdout.lower()
        )
```

Leave the existing `TestStatsShow` content (filter logic tests) as a class but rename it to `TestStatsCommand` and update each call site from `runner.invoke(stats_app, [])` to `runner.invoke(stats_app, [])` — since after this task `stats_app` *is* the command, the invocations stay the same. But change the import at the top of the file to reflect that `stats_app` is now exported as a command, not a Typer group:

```python
from moneybin.cli.commands.stats import (
    stats_command as stats_app,
)  # alias for in-place test compatibility
```

- [ ] **Step 2.2: Run the test to verify it fails**

```bash
uv run pytest tests/moneybin/test_stats_command.py -v
```

Expected: FAIL — `stats_command` doesn't exist yet; ImportError.

- [ ] **Step 2.3: Rewrite `src/moneybin/cli/commands/stats.py`**

Replace lines 18-127 (the `app = typer.Typer(...)` and `@app.command("show") def stats_show(...)`) with a free function and a one-line export. Keep the body of `stats_show` intact, just remove the `@app.command("show")` decorator and rename the symbol to `stats_command`. Add a Typer-callable helper at the bottom for backwards-compat in case anything imports `app`.

```python
"""Stats command for MoneyBin CLI.

Displays lifetime metric aggregates from the app.metrics table.
This is a leaf command (no subcommands) registered directly on the
root app — see `moneybin.cli.main`.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Annotated, Literal

import typer

from moneybin.cli.utils import handle_cli_errors
from moneybin.utils.parsing import parse_duration

logger = logging.getLogger(__name__)


def stats_command(
    since: Annotated[
        str | None,
        typer.Option("--since", help="Time window (e.g., 7d, 24h)"),
    ] = None,
    metric: Annotated[
        str | None,
        typer.Option("--metric", help="Filter to a metric family (e.g., import)"),
    ] = None,
    output: Annotated[
        Literal["text", "json"],
        typer.Option("-o", "--output", help="Output format: text or json"),
    ] = "text",
    quiet: Annotated[
        bool,
        typer.Option("-q", "--quiet", help="Suppress informational output"),
    ] = False,
) -> None:
    """Display lifetime metric aggregates."""
    with handle_cli_errors() as db:
        where_clauses: list[str] = []
        params: list[str | datetime] = []

        if since:
            try:
                delta = parse_duration(since)
            except ValueError as e:
                logger.error(f"❌ {e}")
                raise typer.Exit(1) from e
            cutoff = datetime.now(tz=UTC) - delta
            where_clauses.append("recorded_at >= ?")
            params.append(cutoff)

        if metric:
            escaped = metric.replace("!", "!!").replace("%", "!%").replace("_", "!_")
            where_clauses.append("metric_name LIKE ? ESCAPE '!'")
            params.append(f"%{escaped}%")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        try:
            rows = db.execute(
                f"""
                SELECT metric_name, metric_type, labels,
                       value AS current_value,
                       snapshot_count,
                       last_recorded
                FROM (
                    SELECT metric_name, metric_type, labels, value,
                           COUNT(*) OVER (
                               PARTITION BY metric_name, metric_type, labels
                           ) AS snapshot_count,
                           MAX(recorded_at) OVER (
                               PARTITION BY metric_name, metric_type, labels
                           ) AS last_recorded,
                           ROW_NUMBER() OVER (
                               PARTITION BY metric_name, metric_type, labels
                               ORDER BY recorded_at DESC
                           ) AS rn
                    FROM app.metrics
                    {where_sql}
                )
                WHERE rn = 1
                ORDER BY metric_name
                """,  # noqa: S608 — where_sql is built from validated fragments
                params if params else None,
            ).fetchall()
        except Exception:  # noqa: BLE001 — app.metrics table may not exist yet
            logger.debug("Failed to query app.metrics", exc_info=True)
            rows = []

        if output == "json":
            result = {
                "metrics": [
                    {
                        "name": row[0],
                        "type": row[1],
                        "labels": row[2],
                        "value": row[3],
                        "snapshots": row[4],
                        "last_recorded": row[5].isoformat() if row[5] else None,
                    }
                    for row in rows
                ]
            }
            typer.echo(json.dumps(result, indent=2))
            return

        if not rows:
            if not quiet:
                typer.echo("No metrics recorded yet. Run some operations first.")
            return

        for row in rows:
            name, metric_type, _labels, value, count, _last = row
            display_name = name.replace("moneybin_", "").replace("_", " ").title()
            if metric_type == "counter":
                typer.echo(f"{display_name}: {value:,.0f} total")
            elif metric_type == "gauge":
                typer.echo(f"{display_name}: {value:.2f}")
            elif metric_type == "histogram":
                typer.echo(f"{display_name}: {count} observations (sum={value:.2f}s)")
```

- [ ] **Step 2.4: Update registration in `src/moneybin/cli/main.py`**

Replace this block:

```python
app.add_typer(stats.app, name="stats", help="Show lifetime metric aggregates")
```

with:

```python
app.command(name="stats", help="Show lifetime metric aggregates")(stats.stats_command)
```

Also update the import at the top of `main.py` if Pyright complains — `stats` is still imported as a module.

- [ ] **Step 2.5: Run the test to verify it passes**

```bash
uv run pytest tests/moneybin/test_stats_command.py -v
```

Expected: PASS.

- [ ] **Step 2.6: Smoke-test the CLI**

```bash
uv run moneybin stats --help
uv run moneybin stats show 2>&1 | head -5  # should fail with extra-argument error
```

Expected: help text is shown for `stats`; `stats show` errors with exit code != 0.

- [ ] **Step 2.7: Commit**

```bash
git add src/moneybin/cli/commands/stats.py src/moneybin/cli/main.py tests/moneybin/test_stats_command.py
git commit -m "Convert stats to a leaf command (drop redundant 'show')"
```

---

## Task 3: Convert `logs` to a leaf with required positional stream

**Why now:** Largest structural change. Defines the pattern for "noun command with required input." Pruning and path-printing become flag-modes on the same command.

**Files:**
- Modify: `src/moneybin/cli/commands/logs.py` (substantial rewrite of public surface)
- Modify: `src/moneybin/cli/main.py:196-200` (registration → leaf)
- Test: `tests/moneybin/test_cli/test_cli_logs.py` (heavy revision)
- Test: `tests/moneybin/test_cli/test_cli_logs_leaf.py` (new — focused tests for the new shape)

- [ ] **Step 3.1: Write failing tests for the new leaf shape**

Create `tests/moneybin/test_cli/test_cli_logs_leaf.py`:

```python
"""Tests for the new logs-as-leaf command shape."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import logs as logs_module


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


def _seed_logs(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "cli_2026-04-30.log").write_text(
        "2026-04-30 10:00:00,000 - moneybin.test - INFO - hello cli\n"
    )
    (log_dir / "mcp_2026-04-30.log").write_text(
        "2026-04-30 10:00:00,000 - moneybin.test - INFO - hello mcp\n"
    )


class TestLogsLeafShape:
    @pytest.mark.unit
    def test_bare_invocation_errors_with_usage(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`moneybin logs` (no stream) must exit non-zero with a usage error."""
        result = runner.invoke(logs_module.logs_command_app, [])
        # Typer usage errors exit with code 2
        assert result.exit_code == 2, result.output
        assert (
            "missing argument" in result.stdout.lower() + result.stderr.lower()
            or "usage" in (result.stderr or "").lower()
        )

    @pytest.mark.unit
    def test_unknown_stream_errors(self, runner: CliRunner) -> None:
        result = runner.invoke(logs_module.logs_command_app, ["bogus"])
        assert result.exit_code != 0
        combined = (result.stdout + (result.stderr or "")).lower()
        assert "unknown stream" in combined or "invalid value" in combined

    @pytest.mark.unit
    def test_known_stream_reads_lines(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        _seed_logs(log_dir)
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_settings",
            lambda: type(
                "S",
                (),
                {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
            )(),
        )
        result = runner.invoke(logs_module.logs_command_app, ["cli"])
        assert result.exit_code == 0
        assert "hello cli" in result.stdout
        assert "hello mcp" not in result.stdout

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
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_settings",
            lambda: type(
                "S",
                (),
                {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
            )(),
        )
        result = runner.invoke(logs_module.logs_command_app, ["--print-path"])
        assert result.exit_code == 0, result.output
        assert str(log_dir) in result.stdout

    @pytest.mark.unit
    def test_prune_flag_skips_stream_requirement(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_settings",
            lambda: type(
                "S",
                (),
                {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
            )(),
        )
        result = runner.invoke(
            logs_module.logs_command_app,
            ["--prune", "--older-than", "30d", "--dry-run"],
        )
        assert result.exit_code == 0, result.output

    @pytest.mark.unit
    def test_all_stream_no_longer_accepted(self, runner: CliRunner) -> None:
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
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_settings",
            lambda: type(
                "S",
                (),
                {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
            )(),
        )
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
        monkeypatch.setattr(
            "moneybin.cli.commands.logs.get_settings",
            lambda: type(
                "S",
                (),
                {"logging": type("L", (), {"log_file_path": log_dir / "cli.log"})()},
            )(),
        )
        result = runner.invoke(
            logs_module.logs_command_app, ["cli", "--since", "2000-01-01T00:00:00"]
        )
        assert result.exit_code == 0
        assert "hello cli" in result.stdout
```

- [ ] **Step 3.2: Run the new test file to verify failures**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_logs_leaf.py -v
```

Expected: FAIL — `logs_command_app` doesn't exist; tests can't even import.

- [ ] **Step 3.3: Rewrite `src/moneybin/cli/commands/logs.py`**

Replace the whole file with the new leaf shape. Keep the helper functions (`_LOG_LINE_RE`, `_LogEntry`, `_parse_log_lines`, `_filter_entries`, `_tail_file`, `_find_log_files`, `_VALID_STREAMS`, `_LEVEL_PRIORITY`). Replace `logs_path`, `logs_clean`, `logs_tail` with a single function `logs_command`, and expose it as a Typer-callable through a tiny single-command app called `logs_command_app` (so registration in `main.py` mirrors the `stats` change). Drop the `--stream all` merge code path entirely.

The new function signature:

```python
def logs_command(
    stream: Annotated[
        str | None,
        typer.Argument(
            help="Log stream to view: cli, mcp, sqlmesh. Required unless "
            "--print-path or --prune is used.",
        ),
    ] = None,
    follow: Annotated[
        bool, typer.Option("-f", "--follow", help="Follow log output")
    ] = False,
    lines: Annotated[
        int, typer.Option("-n", "--lines", help="Number of lines to show")
    ] = 20,
    level: Annotated[
        str | None,
        typer.Option(
            "--level",
            help="Minimum log level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
        ),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Time window or absolute timestamp (e.g., 5m, 1h, 7d, 2026-04-01T00:00:00)",
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help="Upper time bound: duration ago or absolute timestamp",
        ),
    ] = None,
    grep: Annotated[
        str | None,
        typer.Option("--grep", help="Regex pattern to filter log messages"),
    ] = None,
    output: Annotated[
        Literal["text", "json"],
        typer.Option("-o", "--output", help="Output format: text or json"),
    ] = "text",
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Suppress informational output")
    ] = False,
    print_path: Annotated[
        bool,
        typer.Option(
            "--print-path",
            help="Print the log directory and exit (no stream required)",
        ),
    ] = False,
    prune: Annotated[
        bool,
        typer.Option(
            "--prune",
            help="Delete old log files instead of viewing (no stream required)",
        ),
    ] = False,
    older_than: Annotated[
        str | None,
        typer.Option(
            "--older-than",
            help="With --prune: delete logs older than this duration (e.g., 30d)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="With --prune: show what would be deleted"),
    ] = False,
) -> None:
    """View, prune, or locate MoneyBin log files for the active profile."""
    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent

    if print_path:
        typer.echo(str(log_dir))
        return

    if prune:
        if not older_than:
            logger.error("❌ --prune requires --older-than DURATION")
            raise typer.Exit(2)
        _do_prune(log_dir, older_than, dry_run=dry_run, quiet=quiet)
        return

    if stream is None:
        # Required positional missing — exit with usage error code.
        typer.echo(
            "Error: Missing argument 'STREAM'. Pick one of: "
            f"{', '.join(sorted(_VALID_STREAMS))}",
            err=True,
        )
        raise typer.Exit(2)

    if stream.lower() not in _VALID_STREAMS:
        logger.error(
            f"❌ Unknown stream '{stream}'. "
            f"Choose from: {', '.join(sorted(_VALID_STREAMS))}"
        )
        raise typer.Exit(2)

    _do_view(
        log_dir=log_dir,
        stream=stream.lower(),
        follow=follow,
        lines=lines,
        level=level,
        since=since,
        until=until,
        grep=grep,
        output=output,
        quiet=quiet,
    )
```

Update `_VALID_STREAMS` to drop `"all"`:

```python
_VALID_STREAMS = {"cli", "mcp", "sqlmesh"}
```

Replace `_find_log_files()` to drop the `"all"` branch:

```python
def _find_log_files(log_dir: Path, stream: str) -> list[Path]:
    """Find log files for the given stream, sorted newest first."""
    return sorted(
        log_dir.glob(f"{stream}_*.log"),
        key=lambda p: p.name,
        reverse=True,
    )
```

Add `_do_view()` and `_do_prune()` helpers extracted from the old `logs_tail` and `logs_clean` bodies, removing the merged-stream branch from `_do_view`. Pseudocode:

```python
def _do_prune(log_dir: Path, older_than: str, *, dry_run: bool, quiet: bool) -> None:
    try:
        delta = parse_duration(older_than)
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(2) from e

    cutoff = datetime.now() - delta
    if not log_dir.exists():
        if not quiet:
            logger.info(f"Log directory does not exist: {log_dir}")
        return

    deleted = 0
    freed_bytes = 0
    for log_file in log_dir.iterdir():
        if not log_file.is_file():
            continue
        mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
        if mtime < cutoff:
            size = log_file.stat().st_size
            if dry_run:
                if not quiet:
                    logger.info(
                        f"  Would delete: {log_file.name} ({size / 1024:.1f} KB)"
                    )
            else:
                log_file.unlink()
                if not quiet:
                    logger.info(f"  Deleted: {log_file.name}")
            deleted += 1
            freed_bytes += size

    if quiet:
        return
    if deleted == 0:
        logger.info(f"No log files older than {older_than}")
    elif dry_run:
        logger.info(
            f"Would delete {deleted} file(s), freeing {freed_bytes / 1024:.1f} KB"
        )
    else:
        logger.info(f"✅ Deleted {deleted} file(s), freed {freed_bytes / 1024:.1f} KB")


def _parse_time_bound(value: str) -> datetime:
    """Parse --since/--until: accepts a duration ('5m') or ISO-8601 timestamp."""
    # Try duration first (existing behavior).
    try:
        delta = parse_duration(value)
        return datetime.now() - delta
    except ValueError:
        pass
    # Fall back to ISO-8601.
    try:
        return datetime.fromisoformat(value.rstrip("Z"))
    except ValueError as e:
        raise ValueError(
            f"--since/--until must be a duration (5m, 1h, 7d) "
            f"or ISO-8601 timestamp; got '{value}'"
        ) from e


def _do_view(
    *,
    log_dir: Path,
    stream: str,
    follow: bool,
    lines: int,
    level: str | None,
    since: str | None,
    until: str | None,
    grep: str | None,
    output: Literal["text", "json"],
    quiet: bool,
) -> None:
    if level and level.upper() not in _LEVEL_PRIORITY:
        logger.error(
            f"❌ Unknown level '{level}'. Choose from: {', '.join(_LEVEL_PRIORITY)}"
        )
        raise typer.Exit(2)

    since_dt: datetime | None = None
    if since:
        try:
            since_dt = _parse_time_bound(since)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(2) from e

    until_dt: datetime | None = None
    if until:
        try:
            until_dt = _parse_time_bound(until)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(2) from e

    grep_pattern: re.Pattern[str] | None = None
    if grep:
        try:
            grep_pattern = re.compile(grep)
        except re.error as e:
            logger.error(f"❌ Invalid regex pattern: {e}")
            raise typer.Exit(2) from e

    if not log_dir.exists():
        if not quiet:
            logger.info(f"No log directory found: {log_dir}")
        return

    log_files = _find_log_files(log_dir, stream)
    if not log_files:
        if not quiet:
            logger.info(f"No log files found for stream '{stream}' in {log_dir}")
        return

    has_filters = bool(
        level or since_dt or until_dt or grep_pattern or output == "json"
    )
    if has_filters:
        read_lines = lines * 10 if (level or grep_pattern) else lines
        raw_lines = _tail_file(log_files[0], read_lines)
        entries = _parse_log_lines(raw_lines)
        filtered = _filter_entries(
            entries,
            level=level,
            since=since_dt,
            until=until_dt,
            pattern=grep_pattern,
        )
        filtered = filtered[-lines:]
        if output == "json":
            typer.echo(json.dumps([e.to_dict() for e in filtered], indent=2))
        else:
            for entry in filtered:
                typer.echo(entry.to_text())
    else:
        for raw_line in _tail_file(log_files[0], lines):
            typer.echo(raw_line.rstrip())

    if follow:
        if not quiet:
            typer.echo("--- Following (Ctrl+C to stop) ---")
        try:
            with open(log_files[0], encoding="utf-8") as f:
                f.seek(0, 2)
                while True:
                    raw_line = f.readline()
                    if raw_line:
                        typer.echo(raw_line.rstrip())
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            pass
```

Update `_filter_entries` to accept `until: datetime | None`:

```python
def _filter_entries(
    entries: list[_LogEntry],
    *,
    level: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    pattern: re.Pattern[str] | None = None,
) -> list[_LogEntry]:
    min_priority = _LEVEL_PRIORITY.get(level.upper(), 0) if level else 0
    result: list[_LogEntry] = []
    for entry in entries:
        entry_priority = _LEVEL_PRIORITY.get(entry.level, 0)
        if entry_priority < min_priority:
            continue
        if since and entry.timestamp() < since:
            continue
        if until and entry.timestamp() > until:
            continue
        if pattern and not pattern.search(entry.message):
            if not any(pattern.search(line) for line in entry.extra_lines):
                continue
        result.append(entry)
    return result
```

At the bottom of the file, expose a single-command Typer app for the test runner and main.py:

```python
logs_command_app = typer.Typer(
    name="logs",
    help="View, prune, or locate MoneyBin log files for the active profile.",
    invoke_without_command=False,
    add_completion=False,
)
logs_command_app.command(name=None)(logs_command)
```

(Using `name=None` registers `logs_command` as the only command, which Typer collapses so `runner.invoke(logs_command_app, ["cli"])` runs `logs_command(stream="cli")` directly.)

- [ ] **Step 3.4: Update registration in `src/moneybin/cli/main.py`**

Replace:

```python
app.add_typer(
    logs.app,
    name="logs",
    help="Manage log files",
)
```

with:

```python
app.command(
    name="logs",
    help="View, prune, or locate MoneyBin log files for the active profile.",
)(logs.logs_command)
```

- [ ] **Step 3.5: Run new tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_logs_leaf.py -v
```

Expected: PASS for all cases.

- [ ] **Step 3.6: Update the existing `tests/moneybin/test_cli/test_cli_logs.py`**

The existing file targets the old `tail`/`clean`/`path` shape. Walk through every test in it and rewrite invocations:

| Old call | New call |
|---|---|
| `runner.invoke(logs.app, ["tail", "--stream", "cli", ...])` | `runner.invoke(logs.logs_command_app, ["cli", ...])` |
| `runner.invoke(logs.app, ["tail", "--stream", "all"])` | DELETE the test (merge mode is gone) |
| `runner.invoke(logs.app, ["path"])` | `runner.invoke(logs.logs_command_app, ["--print-path"])` |
| `runner.invoke(logs.app, ["clean", "--older-than", "30d"])` | `runner.invoke(logs.logs_command_app, ["--prune", "--older-than", "30d"])` |
| `runner.invoke(logs.app, ["clean", "--older-than", "30d", "--dry-run"])` | `runner.invoke(logs.logs_command_app, ["--prune", "--older-than", "30d", "--dry-run"])` |
| Tests asserting `--follow + all` errors | DELETE — `all` is gone |

For each test that touches the deleted "all"-merge path, delete it. Add a brief docstring note at the top of the file: "These tests cover the leaf logs command. The previous group structure (tail/clean/path) is gone — see test_cli_logs_leaf.py for shape tests."

- [ ] **Step 3.7: Run the full logs test suite**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_logs.py tests/moneybin/test_cli/test_cli_logs_leaf.py -v
```

Expected: green.

- [ ] **Step 3.8: Smoke-test**

```bash
uv run moneybin logs --help
uv run moneybin logs 2>&1 | head -5             # exit 2, usage error
uv run moneybin logs --print-path
uv run moneybin logs --prune --older-than 30d --dry-run
```

- [ ] **Step 3.9: Commit**

```bash
git add src/moneybin/cli/commands/logs.py src/moneybin/cli/main.py \
        tests/moneybin/test_cli/test_cli_logs.py \
        tests/moneybin/test_cli/test_cli_logs_leaf.py
git commit -m "Convert logs to leaf with required stream and flag-mode prune/path"
```

---

## Task 4: Promote `categorize auto-*` to a sub-group

**Files:**
- Modify: `src/moneybin/cli/commands/categorize.py`
- Test: `tests/moneybin/test_cli/test_categorize_auto_commands.py` (revise)

- [ ] **Step 4.1: Update test file to new sub-group shape**

In `tests/moneybin/test_cli/test_categorize_auto_commands.py`, find every invocation that uses `auto-review`, `auto-confirm`, `auto-stats`, `auto-rules` and rewrite as `["auto", "review"]`, `["auto", "confirm"]`, etc. Add one new test:

```python
@pytest.mark.unit
def test_auto_subgroup_help_lists_all_actions(runner: CliRunner) -> None:
    from moneybin.cli.commands.categorize import app as categorize_app

    result = runner.invoke(categorize_app, ["auto", "--help"])
    assert result.exit_code == 0
    for action in ("review", "confirm", "stats", "rules"):
        assert action in result.stdout
```

- [ ] **Step 4.2: Run the test to verify failures**

```bash
uv run pytest tests/moneybin/test_cli/test_categorize_auto_commands.py -v
```

Expected: FAIL — `auto` sub-group doesn't exist.

- [ ] **Step 4.3: Restructure `categorize.py`**

In `src/moneybin/cli/commands/categorize.py`:

1. Create a new sub-typer near the top (after the existing `app = typer.Typer(...)`):

```python
auto_app = typer.Typer(
    help="Auto-categorization workflows: review, confirm, stats, rules",
    no_args_is_help=True,
)
app.add_typer(auto_app, name="auto")
```

2. For each existing `@app.command("auto-review")`, `@app.command("auto-confirm")`, `@app.command("auto-stats")`, `@app.command("auto-rules")` — change the decorator to `@auto_app.command("review")`, `@auto_app.command("confirm")`, `@auto_app.command("stats")`, `@auto_app.command("rules")` respectively.
3. Leave the function names alone (no rename needed; only the registered command name changes).

- [ ] **Step 4.4: Run the test to verify it passes**

```bash
uv run pytest tests/moneybin/test_cli/test_categorize_auto_commands.py -v
```

Expected: PASS.

- [ ] **Step 4.5: Commit**

```bash
git add src/moneybin/cli/commands/categorize.py tests/moneybin/test_cli/test_categorize_auto_commands.py
git commit -m "Promote categorize auto-* commands to 'auto' sub-group"
```

---

## Task 5: Rename `categorize stats` to `categorize summary`

**Why now:** Resolves the collision with top-level `stats` introduced in Task 2. Small change, lands cleanly after Task 4 reorganized the module.

**Files:**
- Modify: `src/moneybin/cli/commands/categorize.py` — rename one decorator
- Test: search-and-update any reference

- [ ] **Step 5.1: Find references**

```bash
grep -rn "categorize.*\"stats\"\|categorize stats" tests/ src/ docs/ README.md 2>/dev/null
```

Note locations.

- [ ] **Step 5.2: Update the decorator**

In `src/moneybin/cli/commands/categorize.py`, find `@app.command("stats")` (the top-level one *not* under `auto`) and rename to `@app.command("summary")`. Update the function's docstring to reflect "summary" instead of "stats."

- [ ] **Step 5.3: Update tests and docs**

For every reference found in step 5.1 outside of `categorize.py`, rewrite `["stats"]` → `["summary"]` and "categorize stats" → "categorize summary."

- [ ] **Step 5.4: Run categorize tests**

```bash
uv run pytest tests/moneybin/test_cli/ -v -k categorize
```

Expected: green.

- [ ] **Step 5.5: Smoke-test**

```bash
uv run moneybin categorize summary --help
uv run moneybin categorize stats 2>&1 | head -3   # should error or show generic help
```

- [ ] **Step 5.6: Commit**

```bash
git add src/moneybin/cli/commands/categorize.py tests/ docs/ README.md
git commit -m "Rename categorize stats to summary to avoid top-level collision"
```

---

## Task 6: Promote `import` format commands to a sub-group

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py`
- Test: `tests/moneybin/test_cli/test_cli_import_formats.py` (new)
- Update: any existing tests that use `list-formats`, `show-format`, `delete-format`

- [ ] **Step 6.1: Write new sub-group test**

Create `tests/moneybin/test_cli/test_cli_import_formats.py`:

```python
"""Tests for the import formats sub-group."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app as import_app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


class TestImportFormatsSubgroup:
    @pytest.mark.unit
    def test_formats_help_lists_actions(self, runner: CliRunner) -> None:
        result = runner.invoke(import_app, ["formats", "--help"])
        assert result.exit_code == 0
        for action in ("list", "show", "delete"):
            assert action in result.stdout

    @pytest.mark.unit
    def test_old_compound_names_no_longer_exist(self, runner: CliRunner) -> None:
        for old in ("list-formats", "show-format", "delete-format"):
            result = runner.invoke(import_app, [old, "--help"])
            assert result.exit_code != 0, f"{old} should be gone"
```

- [ ] **Step 6.2: Run the test to verify failures**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_import_formats.py -v
```

Expected: FAIL — sub-group doesn't exist.

- [ ] **Step 6.3: Restructure `import_cmd.py`**

In `src/moneybin/cli/commands/import_cmd.py`:

1. Add a sub-typer:

```python
formats_app = typer.Typer(
    help="Manage tabular import format definitions",
    no_args_is_help=True,
)
app.add_typer(formats_app, name="formats")
```

2. Rewrite the three decorators:

| Old | New |
|---|---|
| `@app.command("list-formats")` | `@formats_app.command("list")` |
| `@app.command("show-format")` | `@formats_app.command("show")` |
| `@app.command("delete-format")` | `@formats_app.command("delete")` |

Function bodies and arguments are unchanged.

- [ ] **Step 6.4: Update existing tests**

Find any test using the old compound names:

```bash
grep -rn 'list-formats\|show-format\|delete-format' tests/ docs/ README.md 2>/dev/null
```

Rewrite each invocation: `["list-formats"]` → `["formats", "list"]`, etc.

- [ ] **Step 6.5: Run the import test suite**

```bash
uv run pytest tests/moneybin/test_cli/ -v -k import
```

Expected: green.

- [ ] **Step 6.6: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/ docs/ README.md
git commit -m "Promote import format commands to 'formats' sub-group"
```

---

## Task 7: Add `db key` sub-group with stubs for export/import/verify

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`
- Test: `tests/moneybin/test_cli/test_cli_db_key.py` (new)
- Update: any tests using `db key` or `db rotate-key`

- [ ] **Step 7.1: Write tests for the new sub-group**

Create `tests/moneybin/test_cli/test_cli_db_key.py`:

```python
"""Tests for the db key sub-group."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.db import app as db_app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


class TestDbKeySubgroup:
    @pytest.mark.unit
    def test_key_help_lists_all_actions(self, runner: CliRunner) -> None:
        result = runner.invoke(db_app, ["key", "--help"])
        assert result.exit_code == 0
        for action in ("show", "rotate", "export", "import", "verify"):
            assert action in result.stdout

    @pytest.mark.unit
    @pytest.mark.parametrize("action", ["export", "import", "verify"])
    def test_stub_actions_exit_with_not_implemented(
        self, runner: CliRunner, action: str
    ) -> None:
        result = runner.invoke(db_app, ["key", action])
        assert result.exit_code == 1
        combined = (result.stdout + (result.stderr or "")).lower()
        assert "not yet implemented" in combined

    @pytest.mark.unit
    def test_old_rotate_key_no_longer_exists(self, runner: CliRunner) -> None:
        result = runner.invoke(db_app, ["rotate-key", "--help"])
        assert result.exit_code != 0
```

- [ ] **Step 7.2: Run to verify failures**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_db_key.py -v
```

Expected: FAIL.

- [ ] **Step 7.3: Restructure `db.py`**

In `src/moneybin/cli/commands/db.py`:

1. Add a sub-typer:

```python
key_app = typer.Typer(
    help="Manage the encryption key for the active profile's database",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key")
```

2. Move the existing `@app.command("key")` body to `@key_app.command("show")`. Update the function name from `db_key` (or whatever it is) to `db_key_show`.
3. Move the existing `@app.command("rotate-key")` body to `@key_app.command("rotate")`. Update the function name to `db_key_rotate`.
4. Add three stub commands:

```python
@key_app.command("export")
def db_key_export(
    output_path: Annotated[
        Path | None,
        typer.Option(
            "--out", help="File to write the exported (passphrase-wrapped) key to"
        ),
    ] = None,
) -> None:
    """Export the encryption key (passphrase-wrapped) for backup. NOT YET IMPLEMENTED."""
    typer.echo(
        "db key export is not yet implemented. Tracked in docs/followups.md.",
        err=True,
    )
    raise typer.Exit(1)


@key_app.command("import")
def db_key_import(
    input_path: Annotated[
        Path,
        typer.Argument(help="Path to the previously exported key file"),
    ],
) -> None:
    """Restore an encryption key from a backup file. NOT YET IMPLEMENTED."""
    typer.echo(
        "db key import is not yet implemented. Tracked in docs/followups.md.",
        err=True,
    )
    raise typer.Exit(1)


@key_app.command("verify")
def db_key_verify() -> None:
    """Verify the stored key actually decrypts the active database. NOT YET IMPLEMENTED."""
    typer.echo(
        "db key verify is not yet implemented. Tracked in docs/followups.md.",
        err=True,
    )
    raise typer.Exit(1)
```

- [ ] **Step 7.4: Update existing tests**

```bash
grep -rn '"key"\|"rotate-key"' tests/ 2>/dev/null
```

For each `runner.invoke(db_app, ["key"])` → `runner.invoke(db_app, ["key", "show"])`. For each `["rotate-key"]` → `["key", "rotate"]`.

- [ ] **Step 7.5: Run the db test suite**

```bash
uv run pytest tests/moneybin/test_cli/test_db_commands.py tests/moneybin/test_cli/test_cli_db_key.py -v
```

Expected: green.

- [ ] **Step 7.6: Commit**

```bash
git add src/moneybin/cli/commands/db.py tests/moneybin/test_cli/
git commit -m "Add db key sub-group; stub export/import/verify"
```

---

## Task 8: Promote `sync rotate-key` to `sync key rotate`

**Files:**
- Modify: `src/moneybin/cli/commands/sync.py`
- Update: tests referencing `sync rotate-key`

- [ ] **Step 8.1: Write a small test for the new shape**

Append to an existing sync test file (or create `tests/moneybin/test_cli/test_cli_sync_key.py`):

```python
import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.sync import app as sync_app


@pytest.mark.unit
def test_sync_key_help_lists_rotate() -> None:
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(sync_app, ["key", "--help"])
    assert result.exit_code == 0
    assert "rotate" in result.stdout


@pytest.mark.unit
def test_sync_rotate_key_no_longer_exists() -> None:
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(sync_app, ["rotate-key", "--help"])
    assert result.exit_code != 0
```

- [ ] **Step 8.2: Run to verify failure**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_sync_key.py -v
```

Expected: FAIL.

- [ ] **Step 8.3: Restructure `sync.py`**

In `src/moneybin/cli/commands/sync.py`:

```python
key_app = typer.Typer(
    help="Manage the sync server's encryption key",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key")
```

Move `@app.command("rotate-key")` body to `@key_app.command("rotate")`.

- [ ] **Step 8.4: Update existing tests**

```bash
grep -rn 'sync.*rotate-key\|"rotate-key"' tests/ docs/ README.md 2>/dev/null
```

Rewrite invocations.

- [ ] **Step 8.5: Run sync tests**

```bash
uv run pytest tests/moneybin/ -v -k sync
```

- [ ] **Step 8.6: Commit**

```bash
git add src/moneybin/cli/commands/sync.py tests/ docs/ README.md
git commit -m "Promote sync rotate-key to 'key rotate' sub-group"
```

---

## Task 9: Standardize `-o/--output` and `-q/--quiet` across read-only commands

**Why now:** All structural renames are done; safe to apply consistent flags without re-reorganizing.

**Files affected (each gets `-o/--output {text,json}` and `-q/--quiet`):**

- `src/moneybin/cli/commands/db.py` — `info`, `query`, `ps`, `key show`
- `src/moneybin/cli/commands/import_cmd.py` — `status`, `history`, `formats list`, `formats show`
- `src/moneybin/cli/commands/categorize.py` — `summary`, `auto stats`, `list-rules`
- `src/moneybin/cli/commands/sync.py` — `status`
- `src/moneybin/cli/commands/profile.py` — `list`, `show`
- `src/moneybin/cli/commands/matches.py` — `history`
- `src/moneybin/cli/commands/mcp.py` — `list-tools`, `list-prompts`
- `src/moneybin/cli/commands/migrate.py` — `status`

Read-only = no DB writes, no network calls, no filesystem mutations.

- [ ] **Step 9.1: Write a parametrized test that asserts all read-only commands accept `-o json` and `-q`**

Create `tests/moneybin/test_cli/test_cli_output_quiet.py`:

```python
"""Verify -o/--output and -q/--quiet are accepted by every read-only command."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app as root_app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


_READ_ONLY_HELP_PATHS: list[list[str]] = [
    ["db", "info", "--help"],
    ["db", "query", "--help"],
    ["db", "ps", "--help"],
    ["db", "key", "show", "--help"],
    ["import", "status", "--help"],
    ["import", "history", "--help"],
    ["import", "formats", "list", "--help"],
    ["import", "formats", "show", "--help"],
    ["categorize", "summary", "--help"],
    ["categorize", "auto", "stats", "--help"],
    ["categorize", "list-rules", "--help"],
    ["sync", "status", "--help"],
    ["profile", "list", "--help"],
    ["profile", "show", "--help"],
    ["matches", "history", "--help"],
    ["mcp", "list-tools", "--help"],
    ["mcp", "list-prompts", "--help"],
    ["db", "migrate", "status", "--help"],
    ["stats", "--help"],
    ["logs", "--help"],
]


@pytest.mark.unit
@pytest.mark.parametrize("argv", _READ_ONLY_HELP_PATHS, ids=lambda a: " ".join(a))
def test_read_only_command_advertises_output_and_quiet(
    runner: CliRunner, argv: list[str]
) -> None:
    result = runner.invoke(root_app, argv)
    assert result.exit_code == 0, result.output
    out = result.stdout
    # Both flags must appear in the help text
    assert "--output" in out, f"missing --output in {' '.join(argv)}"
    assert "--quiet" in out, f"missing --quiet in {' '.join(argv)}"
    # Short forms must also be present
    assert "-o" in out, f"missing -o in {' '.join(argv)}"
    assert "-q" in out, f"missing -q in {' '.join(argv)}"
```

- [ ] **Step 9.2: Run to see how many fail**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_output_quiet.py -v 2>&1 | tail -40
```

Expected: many failures. Note which ones to fix.

- [ ] **Step 9.3: Add the flags command-by-command**

For each command in the list, follow this template (using `db.info` as the example):

```python
def db_info(
    # ... existing params ...
    output: Annotated[
        Literal["text", "json"],
        typer.Option("-o", "--output", help="Output format: text or json"),
    ] = "text",
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Suppress informational output")
    ] = False,
) -> None:
    """..."""
    # existing body
    if output == "json":
        # serialize the same data as JSON instead of printing tables
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    if not quiet:
        # existing typer.echo or logger.info calls go here
        ...
```

For each command:
1. Add the two `Annotated[...]` params.
2. If the command currently prints with `typer.echo()` or `logger.info()`, gate informational lines behind `if not quiet:` and add a JSON branch when `output == "json"`.
3. Result rows are **never** suppressed by `-q` — they're the data, not the chatter. `-q` only kills "Loaded N records" / "✅" / progress lines.

For commands that already had `--output` but in different form (e.g., `stats` from Task 2 already has both — leave it), confirm conformance.

- [ ] **Step 9.4: Run the parametrized test until it passes**

```bash
uv run pytest tests/moneybin/test_cli/test_cli_output_quiet.py -v
```

Expected: all 20 cases PASS.

- [ ] **Step 9.5: Run the full unit suite**

```bash
uv run pytest tests/moneybin -v -m "not integration and not e2e" 2>&1 | tail -10
```

Expected: green.

- [ ] **Step 9.6: Commit**

```bash
git add src/moneybin/cli/commands/ tests/moneybin/test_cli/test_cli_output_quiet.py
git commit -m "Standardize -o/--output and -q/--quiet on read-only commands"
```

---

## Task 10: Audit error logs land on stderr (fd 2)

**Why:** The new `cli.md` rule (Task 11) will codify the stderr/stdout contract. We need to confirm `logger.error()` actually writes to fd 2 today.

**Files:**
- Inspect: `src/moneybin/observability.py` (or wherever logging is set up)
- Test: `tests/moneybin/test_cli/test_error_routing.py` (new)

- [ ] **Step 10.1: Write a test that asserts errors land on stderr**

```python
"""Verify CLI error output routes to stderr (fd 2), not stdout."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app as root_app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)  # critical: keep streams separate


@pytest.mark.unit
def test_logs_unknown_stream_error_on_stderr(runner: CliRunner) -> None:
    result = runner.invoke(root_app, ["logs", "bogus"])
    assert result.exit_code != 0
    # The error message must appear on stderr, not stdout.
    assert (
        "unknown stream" in (result.stderr or "").lower()
        or "missing argument" in (result.stderr or "").lower()
    )
    assert "unknown stream" not in result.stdout.lower()


@pytest.mark.unit
def test_logs_missing_stream_error_on_stderr(runner: CliRunner) -> None:
    result = runner.invoke(root_app, ["logs"])
    assert result.exit_code == 2
    assert "missing argument" in (result.stderr or "").lower()
```

- [ ] **Step 10.2: Run the test**

```bash
uv run pytest tests/moneybin/test_cli/test_error_routing.py -v
```

If the test fails, inspect `setup_observability()` in `src/moneybin/observability.py`. The CLI logger should have a `StreamHandler(sys.stderr)`, *not* `StreamHandler()` (which defaults to stderr in newer Python but stdout in some configs). Confirm the handler explicitly targets stderr. Also note: `typer.echo(..., err=True)` is required for direct echoes that are errors — make sure the error paths added in Task 3 (`logs`) use `err=True` where they call `typer.echo` for diagnostics.

- [ ] **Step 10.3: Fix any routing problems found**

If the handler in `setup_observability` does not target stderr, change it to `logging.StreamHandler(sys.stderr)`. Re-run the test.

- [ ] **Step 10.4: Commit**

```bash
git add src/moneybin/observability.py tests/moneybin/test_cli/test_error_routing.py
git commit -m "Verify and enforce CLI error output routes to stderr"
```

(If no source changes were needed, just commit the test.)

---

## Task 11: Update `.claude/rules/cli.md` with the new conventions

**Files:**
- Modify: `.claude/rules/cli.md`

- [ ] **Step 11.1: Append a new "Leaf Commands and Sub-Groups" section after `## Command Group Registration`**

Insert this content (between lines 47 and 49 of the current file):

```markdown
## Leaf Commands vs Sub-Groups

A **leaf command** is a top-level command with no subcommands (e.g., `moneybin stats`, `moneybin logs <stream>`). A **sub-group** is a `typer.Typer()` parent with multiple registered actions (e.g., `moneybin db ...`, `moneybin import formats ...`).

**Choose leaf when:**
- The command represents a single action with no plausible siblings (`stats`, `logs`).
- Auxiliary modes can be expressed as flags (`--print-path`, `--prune`) without crowding help text.

**Choose sub-group when:**
- 2+ distinct actions exist on the same noun (`db key {show,rotate,export,import,verify}`, `import formats {list,show,delete}`).
- Future actions are likely (reserve the namespace).

**Required arguments for leaf commands:** Leaf commands MAY require arguments and exit non-zero (code `2`) with a usage error when invoked bare. This is the convention of `docker logs CONTAINER`, `kubectl logs POD`, `tail FILE`. The `no_args_is_help=True` rule applies to **groups**, not leaves; a leaf with required positionals must surface a usage error, not help, so scripts can detect mis-invocation.

## Help Surface Contract

`--help` and `-h` MUST be **side-effect free**. They MUST NOT:
- Trigger first-run wizards
- Read or write profile data
- Open database connections
- Hit external services

`main_callback` (in `src/moneybin/cli/main.py`) short-circuits when `--help`/`-h` appears in `sys.argv`. Do not undo that guard.

## Exit Codes & stderr

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Runtime error (operation ran and failed: file not found, DB locked, API 500) |
| `2` | Usage error (missing arg, invalid flag, unknown subcommand) |

Diagnostic output (errors, warnings, progress, status) goes to **stderr** (fd 2). Data output (rows, JSON, the thing the user asked for) goes to **stdout** (fd 1). Help text from `--help` goes to stdout — it's documentation the user requested, and pipes (`| less`) must work.

Use `typer.echo(msg, err=True)` for direct error echoes, and confirm the project logger's `StreamHandler` targets `sys.stderr` (see `src/moneybin/observability.py`). `logger.error()` and `logger.warning()` must reach fd 2; `logger.info()` may reach either as long as it doesn't pollute scripts capturing stdout.

## Standard Flags on Read-Only Commands

Every command that **reads but does not mutate** state MUST accept:

- `-o, --output {text,json}` — output format. `text` is human-readable, `json` is machine-readable. The `json` branch must serialize the same data the text branch displays.
- `-q, --quiet` — suppress informational output (status lines, progress, `✅`). Result rows are NEVER suppressed by `-q` — they are the data.

This makes every read command pipeable into `jq`, scripts, and AI agents. Audit-tested by `tests/moneybin/test_cli/test_cli_output_quiet.py`.
```

- [ ] **Step 11.2: Update the existing `## Command Group Registration` section**

Find this paragraph in the existing file (around line 46-47):

```markdown
- **`no_args_is_help=True`**: Every `typer.Typer()` group must set this flag so bare invocation shows help text consistently.
```

Replace with:

```markdown
- **`no_args_is_help=True`**: Every `typer.Typer()` *group* must set this flag so bare invocation shows help text consistently. Leaf commands (registered via `app.command()` directly on the root app, like `stats` and `logs`) follow a different convention — see "Leaf Commands vs Sub-Groups" below.
```

- [ ] **Step 11.3: Commit**

```bash
git add .claude/rules/cli.md
git commit -m "Document leaf-command, --help, exit-code, and -o/-q rules"
```

---

## Task 12: Add deferred-work entry to `docs/followups.md`

**Files:**
- Modify: `docs/followups.md`

- [ ] **Step 12.1: Append a new section to `docs/followups.md`**

Add at the end of the file:

```markdown
## `db key {export,import,verify}` (post-PR for CLI symmetry refactor)

The CLI symmetry refactor introduced the `db key` sub-group with stubs for three operations that are not yet implemented. They exist to reserve the command namespace and exit with code 1 + a "not yet implemented" message.

### `db key export`

Export the active profile's encryption key, wrapped in a user-supplied passphrase, to a backup file. Use case: disaster recovery when the keychain is lost.

Design considerations:
- Wrap with Argon2id-derived KEK + AES-256-GCM (same primitives as data-protection).
- Output format should be portable: a small JSON envelope with `version`, `kdf_params`, `nonce`, `ciphertext`, `tag`.
- File should be marked `0600`.
- Must NOT print the unwrapped key to stdout.

### `db key import`

The inverse: read a backup file, prompt for the passphrase, restore into the keychain for the active profile.

Design considerations:
- Detect collision with an existing keychain entry; require `--force` to overwrite.
- After import, run `db key verify` automatically.

### `db key verify`

Confirm the stored key actually decrypts the active profile's database (open + read a single row from a known table). Useful after restore, or as a periodic check.

Design considerations:
- Must not modify the database.
- Should differentiate "key wrong" from "DB missing" in the error message.

### Why deferred

Each requires a small spec covering the file format, passphrase prompt UX, and rotation interaction. Better to land the CLI shape now and iterate the implementations against a reference spec than to inline-design under PR pressure.
```

- [ ] **Step 12.2: Commit**

```bash
git add docs/followups.md
git commit -m "Track db key export/import/verify implementation as follow-up"
```

---

## Task 13: Update README and run end-to-end verification

**Files:**
- Modify: `README.md` — any CLI examples affected by renames
- Modify: `tests/e2e/test_e2e_help.py` — `_HELP_COMMANDS` for new groups
- Possibly modify: `tests/e2e/test_e2e_readonly.py` — adjust paths

- [ ] **Step 13.1: Find affected README examples**

```bash
grep -nE 'moneybin (logs tail|logs path|logs clean|stats show|categorize auto-|categorize stats|import (list-formats|show-format|delete-format)|db rotate-key|sync rotate-key)' README.md
```

For each match, rewrite to the new shape.

- [ ] **Step 13.2: Update `tests/e2e/test_e2e_help.py`**

Find `_HELP_COMMANDS` and:
- Add: `["db", "key"]`, `["import", "formats"]`, `["categorize", "auto"]`, `["sync", "key"]`
- Remove or adjust: any entry that referenced `logs path`, `logs tail`, `logs clean`, `stats show`, `categorize auto-*`, `import list-formats`/`show-format`/`delete-format`, `db rotate-key`, `sync rotate-key`.

- [ ] **Step 13.3: Update `tests/e2e/test_e2e_readonly.py`**

Search for old command paths:

```bash
grep -nE '"logs", *"tail"|"logs", *"path"|"logs", *"clean"|"stats", *"show"|"auto-(review|confirm|stats|rules)"|"list-formats"|"show-format"|"delete-format"|"rotate-key"' tests/e2e/
```

Rewrite each match to the new shape.

- [ ] **Step 13.4: Run the full test suite**

```bash
make check test 2>&1 | tail -30
```

Expected: format/lint/type-check/tests all green. If pyright complains about anything, fix in place — don't suppress.

- [ ] **Step 13.5: Run the scenario verifier**

```bash
uv run moneybin synthetic verify 2>&1 | tail -10
```

Expected: success. (This catches whole-pipeline regressions.)

- [ ] **Step 13.6: Run `/simplify` per the shipping rule**

```bash
# Invoke the simplify skill via Claude Code:
# /simplify
```

Apply any cleanup the skill identifies.

- [ ] **Step 13.7: Final commit**

```bash
git add README.md tests/e2e/
git commit -m "Update docs and e2e tests for CLI symmetry refactor"
```

- [ ] **Step 13.8: Push and open PR**

```bash
git push -u origin refactor/cli-require-args-for-logs-stats
gh pr create --title "Refactor CLI for symmetry with docker/kubectl conventions" --body "$(cat <<'EOF'
## Summary

- Collapse single-action groups (`stats`, `logs`) into leaf commands; `logs` requires a positional stream like `docker logs CONTAINER`.
- Promote multi-action verb-prefixed commands into proper sub-groups (`categorize auto`, `db key`, `import formats`, `sync key`).
- Standardize `-o/--output {text,json}` and `-q/--quiet` on every read-only command.
- Fix `--help` first-run-wizard side effect.
- Document new conventions in `.claude/rules/cli.md`.

Breaking change. Pre-alpha — see migration table in plan doc.

## Test plan

- [x] Unit suite green (`uv run pytest tests/moneybin -m 'not integration and not e2e'`)
- [x] E2E suite green (`uv run pytest tests/e2e`)
- [x] Scenario verifier green (`uv run moneybin synthetic verify`)
- [x] `make check test` clean
- [x] Smoke-tested key new shapes manually:
  - `moneybin logs cli -n 5`
  - `moneybin logs --print-path`
  - `moneybin logs --prune --older-than 30d --dry-run`
  - `moneybin stats`
  - `moneybin categorize auto review --help`
  - `moneybin db key show`
  - `moneybin db key export` (stub exit)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Checklist (run after writing the plan, before execution)

- [ ] Every breaking change in the "Breaking changes" table has a task.
- [ ] Every "Additions" item has a task.
- [ ] Every file in "File Inventory → Modified" appears in at least one task.
- [ ] No step says "TBD," "fill in," "similar to," or "add appropriate handling" without inline code.
- [ ] Type and signature names match across tasks (e.g., `logs_command_app` consistent in Task 3 tests and main.py registration).
- [ ] Every task ends with a commit step.
- [ ] The cli.md rule update (Task 11) covers every new convention introduced in Tasks 1–10.

If any check fails, fix it inline and re-run.
