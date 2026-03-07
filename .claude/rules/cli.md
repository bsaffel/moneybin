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
- Emoji in user-facing output: `[checkmark]` `[x]` `[warning]` `[rocket]`
- Clear help text for all commands and options
- Progress updates for long operations
