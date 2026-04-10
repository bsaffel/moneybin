# Distribution Roadmap

## Overview

MoneyBin is currently designed for local development use (cloned repo, `uv run`). This document tracks what would need to change before it can be distributed as a pip-installable package for end users who have no knowledge of the codebase.

## Current Assumptions

- User clones the repo and runs `uv run moneybin` from the project root.
- `MONEYBIN_ENVIRONMENT` defaults to `"development"`, so `get_base_dir()` returns `Path.cwd()`.
- Data, logs, and the DuckDB file land relative to wherever the user runs the command from.
- `sqlmesh/config.py` sets `MONEYBIN_HOME` to the project root to prevent sqlmesh from scattering files when invoked from a non-root directory.

## Before Distribution

### 1. Fix `get_base_dir()` default for installed users

**File:** `src/moneybin/config.py`

The `"development"` branch of `get_base_dir()` returns `Path.cwd()`, which is correct for contributors running from the repo root but wrong for pip users — they would get a new database wherever they happen to be standing in the terminal.

The fix: invert the default. Use `~/.moneybin/` unless `MONEYBIN_ENVIRONMENT=development` is explicitly set.

```python
def get_base_dir() -> Path:
    moneybin_home = os.getenv("MONEYBIN_HOME")
    if moneybin_home:
        return Path(moneybin_home).expanduser().resolve()

    environment = os.getenv("MONEYBIN_ENVIRONMENT", "production")  # changed default
    if environment == "development":
        return Path.cwd().resolve()

    return (Path.home() / ".moneybin").resolve()
```

Contributors would set `MONEYBIN_ENVIRONMENT=development` in their `.env` or shell profile. Installed users get a stable `~/.moneybin/` out of the box.

### 2. SQLMesh availability for pip users

`sqlmesh/config.py` is part of the repo but not the installed package. A pip user has no `sqlmesh/` directory, so `uv run sqlmesh -p sqlmesh` would not work. Options:

- Bundle a minimal `config.py` as a package data file and expose a `moneybin sqlmesh-project` command that writes it out.
- Ship a pre-built `config.yaml` instead of `config.py` (loses dynamic path resolution).
- Document that SQLMesh commands are developer-only and not part of the end-user workflow.

### 3. First-run experience

A pip user needs `moneybin db init` (or equivalent) to create `~/.moneybin/` and initialize the schema before any other command works. Consider a startup check that detects a missing database and prints a clear error with the init command rather than a raw DuckDB exception.

### 4. Package metadata

`pyproject.toml` should have accurate `[project]` metadata (author, license, homepage, classifiers) before publishing to PyPI.

## Out of Scope Until Distribution

- PyPI publish workflow (GitHub Actions release job)
- Homebrew formula
- Windows path compatibility (`~/.moneybin` vs `%APPDATA%`)
- Auto-update / version check mechanism
