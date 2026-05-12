---
description: "CLI development: Typer patterns, error handling, command registration, non-interactive parity"
paths: ["src/moneybin/cli/**", "src/moneybin/main.py"]
---

# CLI Development

## Core Principle

CLI commands are **thin wrappers** around tested business logic. Delegate complex work to business logic classes.

## Consumer Model

CLI serves three peer consumers, not just one:

1. **Humans at terminals** — the obvious case. Get nice output, helpful errors, interactive prompts where useful.
2. **Shell scripts and pipelines** — `--output json` + `jq`, exit codes, stdout/stderr separation.
3. **AI agents** — Claude Code, Codex CLI, Gemini CLI, and similar agents drive CLI commands directly as a peer pathway to MCP. They pipe and chain commands the way humans use shells, and parse JSON output the way scripts do.

**The CLI is a first-class agent surface, not a fallback for users without MCP.** When MoneyBin offers a capability via MCP, it ships with a CLI equivalent (per `mcp-server.md` principle 5) — and that CLI is designed for both humans and agents from the start.

What this means in practice:

- Data primitives (export commands, file-based inputs, stdin/stdout JSON) are designed once and serve all three consumers.
- Redaction contracts apply identically across CLI and MCP — never assume CLI users are "trusted enough to skip redaction."
- Every interactive prompt must have a flag equivalent (see Non-Interactive Parity below) — agents cannot navigate prompts.
- `--output json` returns the same envelope shape MCP returns (see `mcp-server.md` Response Envelope).

When designing a new command, ask: "Could an agent drive this end-to-end without a human?" If not, redesign — that's a flag-equivalence gap or a JSON-output gap, not an acceptable limitation.

## Standard Pattern

```python
@app.command("command-name")
def command_function(source_path: Path = typer.Option(..., help="Description")) -> None:
    """Clear command description."""
    setup_logging(cli_mode=True)
    try:
        config = ConfigClass(source_path=source_path)
        processor = BusinessClass(config)
        results = processor.main_operation()
        logger.info(f"Processed {len(results)} records")
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e
```

## Error Handling

- Catch specific exceptions (FileNotFoundError, PermissionError, etc.)
- Any command that calls `get_database()` must also catch `DatabaseKeyError` with a "run `moneybin db unlock`" message.
- Use `raise typer.Exit(code) from e` for error chaining
- Exit codes: 0 = success, 1 = general error, 2+ = command-specific

## Secrets in Error Output

Recovery messages containing keys, tokens, or credentials must go to stderr via `typer.echo(..., err=True)` — **never through `logger.*()`**. The log pipeline persists to files and hex keys won't match PII regex patterns.

## Multi-State Operations

When a command modifies multiple persistent stores in sequence (e.g., file move + keychain update), wrap later steps in try/except with recovery guidance: tell the user what state they're in, where the backup is, and don't delete backups until all steps succeed.

## Command Group Registration

- **Workflow ordering**: Top-level commands in `main.py` are registered in workflow order: setup → ingest → enrich → pipeline → analyze → output → integrations → ops. New commands should be inserted at the appropriate workflow stage.
- **`no_args_is_help=True`**: Every `typer.Typer()` *group* must set this flag so bare invocation shows help text consistently. Leaf commands (registered via `app.command()` directly on the root app, like `stats` and `logs`) follow a different convention — see "Leaf Commands vs Sub-Groups" below. Do not use `invoke_without_command=True` callbacks as a substitute — that flag runs the callback even when a subcommand is provided, causing confusing side effects like duplicate setup or output.
## Cold-Start Hygiene

Every E2E test, every shell autocomplete, and every CLI invocation pays the full module-import cost for `moneybin.cli.main`. Keep that path light.

- **Defer heavy transitive imports inside command bodies.** `fastmcp`, `sqlmesh`, `polars`, and similar (anything that pulls in a parser, ORM, or large package graph) must not be imported at module top in `src/moneybin/cli/commands/*` or any module those imports load. Put the import inside the function that uses it:

  ```python
  @app.command("serve")
  def serve(...) -> None:
      from moneybin.mcp.server import build_server  # noqa: PLC0415 — defer import
      build_server(...).run()
  ```

- **Verify with `importtime`.** When adding a new command module, confirm the cold-start path stays clean:

  ```bash
  uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -iE "<heavy-dep>"
  ```

  Should produce no output for `fastmcp`, `sqlmesh`, or `polars`.

## Leaf Commands vs Sub-Groups

A **leaf command** is a top-level command with no subcommands (e.g., `moneybin stats`, `moneybin logs <stream>`). A **sub-group** is a `typer.Typer()` parent with multiple registered actions (e.g., `moneybin db ...`, `moneybin import formats ...`).

**Choose leaf when:**
- The command represents a single action with no plausible siblings (`stats`, `logs`).
- Auxiliary modes can be expressed as flags (`--print-path`, `--prune`) without crowding help text.

**Choose sub-group when:**
- 2+ distinct actions exist on the same noun (`db key {show,rotate,export,import,verify}`, `import formats {list,show,delete}`).
- Future actions are likely (reserve the namespace).

**Naming convention for leaf functions:** Free-function leaf commands use `<name>_command` (e.g., `stats_command`, `logs_command`) to avoid shadowing the surrounding module name. Sub-group commands continue to use `<group>_<verb>` (e.g., `db_key_show`, `db_key_rotate`).

**Required arguments for leaf commands:** Leaf commands MAY require arguments and exit non-zero (code `2`) with a usage error when invoked bare. This is the convention of `docker logs CONTAINER`, `kubectl logs POD`, `tail FILE`. The `no_args_is_help=True` rule applies to **groups**, not leaves; a leaf with required positionals must surface a usage error, not help, so scripts can detect mis-invocation.

## Help Surface Contract

`--help` and `-h` MUST be **side-effect free**. They MUST NOT:

- Trigger first-run wizards
- Read or write profile data
- Open database connections
- Hit external services

`main_callback` (in `src/moneybin/cli/main.py`) MUST stay inert — only register the lazy profile resolver, never call `resolve_profile()` directly. Help paths exit before any command body runs, so the lazy path is what keeps them side-effect free.

## Exit Codes & stderr

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Runtime error (operation ran and failed: file not found, DB locked, API 500) |
| `2` | Usage error (missing arg, invalid flag, unknown subcommand) |

Diagnostic output (errors, warnings, progress, status) goes to **stderr** (fd 2). Data output (rows, JSON, the thing the user asked for) goes to **stdout** (fd 1). Help text from `--help` goes to stdout — it's documentation the user requested, and pipes (`| less`) must work.

Use `typer.echo(msg, err=True)` for direct error echoes. The project logger's `StreamHandler` already targets `sys.stderr` (see `src/moneybin/logging/config.py`). `logger.error()` and `logger.warning()` reach fd 2; `logger.info()` may reach either as long as it doesn't pollute scripts capturing stdout. Locked by `tests/moneybin/test_cli/test_error_routing.py`.

## Standard Flags on Read-Only Commands

Every command that **reads but does not mutate** state MUST accept:

- `-o, --output {text,json}` — output format. `text` is human-readable, `json` is machine-readable. The `json` branch must serialize the same data the text branch displays.
- `-q, --quiet` — suppress informational output (status lines, progress, `✅`). Result rows are NEVER suppressed by `-q` — they are the data.
- `--json-fields` — comma-separated field projection for `--output json` (e.g. `--json-fields id,date,amount`). Only applies when `--output json` is active; silently ignored otherwise. Added progressively as each read-only command is extended — declare as `json_fields: str | None = json_fields_option` and pass to `render_or_json(json_fields=json_fields)`. Commands that implement it MUST enumerate available field names in their `--help` text (e.g. `"Available fields: id, date, amount, description, category, account_id"`).

`db query` extends `--output` to `text|json|csv|markdown|box` since DuckDB's CLI supports all five natively.

This makes every read command pipeable into `jq`, scripts, and AI agents. Audit-tested by `tests/moneybin/test_cli/test_cli_output_quiet.py`.

## Conventions

- Kebab-case for command names
- Clear help text for all commands and options
- Progress updates for long operations

## Non-Interactive Parity

Every interactive prompt (confirmation, selection, wizard step) must have a flag equivalent that expresses the same intent in a single invocation. AI agents and scripts cannot navigate interactive prompts.

- **Confirmations** → `--yes` / `-y` to auto-accept
- **Field selection** → named flags (e.g., `--date-col=X`, `--amount-col=Y`)
- **Declining/skipping** → `--skip` or equivalent
- **Multi-step wizards** → each step's choice expressible as a flag; all flags combinable in one invocation

Combined with `--output json` (see `mcp-architecture.md` §7), this makes every CLI command fully automatable by AI agents (Claude Code, Codex) and shell scripts.

## Icon Usage

Use icons **sparingly** — only where they add scanability, not decoration.

| Signal | Icon | When to use |
|--------|------|-------------|
| Success | `✅` | Final line of a successful action command |
| Error | `❌` | `logger.error(...)` messages |
| Warning | `⚠️` | `logger.warning(...)` messages |
| Working | `⚙️` | Start of a long-running operation (sync, load, transform) |
| Hint | `💡` | Optional follow-up tips after an error |
| Bug report | `🐛` | Link to issue tracker after an unexpected error |
| Review | `👀` | Items that need user attention or review |

Do **not** add icons to ordinary informational log lines (paths, counts, results rows). Query/display commands (`status`, `stats`, `list-*`) don't need a trailing ✅ — they just display data. No decorative icons (📈📊📁) — only the semantic icons in the table above.
