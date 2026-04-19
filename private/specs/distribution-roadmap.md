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

## MCP Platform Compatibility

MoneyBin's MCP server (FastMCP, stdio transport) is the primary distribution surface for AI assistant integration. MCP has been adopted by all major platforms, making it the "write once, run everywhere" layer.

### Platform Matrix

| Platform | MCP Support | Transport | MoneyBin Status | Notes |
|----------|------------|-----------|-----------------|-------|
| Claude Desktop | Full (tools, resources, prompts) | stdio | **Works today** | Primary development target |
| Claude Code | Full + plugins/skills layer | stdio | **Works today** | Skills are optional enhancement |
| ChatGPT | Tools only (announced March 2025) | Streamable HTTP | **Not supported** | Requires remote transport |
| Cursor / Windsurf / VS Code | Full | stdio | **Works today** | No additional work needed |
| Google Gemini | MCP support announced | stdio / HTTP | **Untested** | Likely works via stdio |
| Apple Intelligence | MCP support announced | TBD | **Too early** | Spec not finalized |

### Transport Strategy

**stdio (current):** Works for all local clients — Claude Desktop, Claude Code, IDEs. No changes needed. This is the right default for a local-first financial data tool.

**Streamable HTTP (future):** Required for ChatGPT and any remote client. FastMCP supports this with minimal code changes, but there is a fundamental design tension:

- MoneyBin is local-first: DuckDB on disk, no cloud storage, privacy by design.
- Remote transport implies the server is reachable from the internet, which means either:
  - A local server exposed via tunnel (ngrok-style) — poor UX for end users.
  - A hosted server — conflicts with local-first privacy model.
  - ChatGPT adding local MCP server support — unclear timeline.

**Decision:** Defer HTTP transport until one of: (a) ChatGPT supports local stdio MCP servers, (b) a hosted mode is designed that aligns with the privacy model (e.g., moneybin-server as the remote MCP endpoint), or (c) user demand justifies the complexity.

### Cross-Platform Workflow Guidance

Three mechanisms exist for guiding AI assistants through MoneyBin workflows:

| Mechanism | Scope | Platform Support | Investment |
|-----------|-------|-----------------|------------|
| **MCP Prompts** | Cross-platform | All MCP clients | **Primary — invest here** |
| **Claude Code Skills** | Claude Code only | Claude Code | Optional polish |
| **MCP Apps** | Cross-platform (UI) | Very limited | Defer |

**MCP Prompts** (9 today: `monthly_review`, `categorize_transactions`, `tax_preparation`, etc.) are the cross-platform equivalent of skills. They work everywhere MCP is supported and should be the primary investment for guided workflows.

**Claude Code Skills/Plugins** add Claude Code-specific workflow polish (e.g., opinionated TDD flows, automatic plan generation). Since MCP prompts already cover guided workflows, skills are a nice-to-have for Claude Code power users, not a distribution requirement.

**MCP Apps** add interactive UI (HTML/JS views embedded in the client). Relevant use cases for MoneyBin:
- Plaid Link OAuth flow (already scoped as post-MVP in `sync-client-integration.md`)
- Spending dashboards and charts
- Bulk categorization forms

Platform support is too thin to invest today. Revisit when Claude Desktop and at least one other major platform ship MCP Apps support.

## Out of Scope Until Distribution

- PyPI publish workflow (GitHub Actions release job)
- Homebrew formula
- Windows path compatibility (`~/.moneybin` vs `%APPDATA%`)
- Auto-update / version check mechanism
