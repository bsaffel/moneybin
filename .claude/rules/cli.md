---
description: "CLI development: Typer patterns, error handling, command registration, non-interactive parity"
paths: ["src/moneybin/cli/**", "src/moneybin/main.py"]
---

# CLI Development

## Core Principle

CLI commands are **thin wrappers** around tested business logic. Delegate complex work to business logic classes.

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
- **`no_args_is_help=True`**: Every `typer.Typer()` group must set this flag so bare invocation shows help text consistently. Do not use `invoke_without_command=True` callbacks as a substitute — that flag runs the callback even when a subcommand is provided, causing confusing side effects like duplicate setup or output.

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
