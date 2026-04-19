---
globs: ["src/moneybin/cli/**"]
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
- Use `raise typer.Exit(code) from e` for error chaining
- Exit codes: 0 = success, 1 = general error, 2+ = command-specific

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

Do **not** add icons to ordinary informational log lines (paths, counts, results rows). Query/display commands (`status`, `stats`, `list-*`) don't need a trailing ✅ — they just display data.

```python
# Good
logger.info("⚙️  Starting sync from all institutions...")
logger.info("✅ Imported %d transactions", count)
logger.error("❌ File not found: %s", path)
logger.warning("⚠️  No new data to sync")
logger.info("💡 Run 'moneybin db init' to create the database first")
logger.error("🐛 Report issues at https://github.com/bsaffel/moneybin/issues")
logger.info("👀 3 auto-generated rules need review")

# Bad — wrong icon semantics or decorative noise
logger.info("📈 Beginning incremental sync...")  # chart ≠ working
logger.info("📊 Loading results:")  # chart ≠ working
logger.info("📁 Data saved to: %s", path)  # no icon needed for paths
```
