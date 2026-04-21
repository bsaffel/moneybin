# Observability

> Status: implemented
> Dependencies: privacy-data-protection.md (provides `SanitizedLogFormatter`)
> Scope: Logging consolidation, structured metrics, instrumentation API, CLI commands

## Overview

MoneyBin's observability system provides unified logging, metrics collection, and instrumentation through a single entry point. It consolidates the current dual-config logging mess into one Pydantic-based configuration, adds `prometheus_client`-backed metrics with DuckDB persistence, and provides a decorator/context manager API for instrumenting operations with minimal boilerplate.

### Design Principles

- **Developer/operator quality of life is first-class.** It should be trivially easy to add observability to any new operation.
- **Metrics and logs coexist.** They serve different purposes — metrics for aggregates/trends, logs for event-level detail.
- **PII never reaches logs.** The `SanitizedLogFormatter` wraps all formatters unconditionally.
- **Profile-bound.** All log files and metrics are scoped to the active profile.

## Architecture

```
src/moneybin/
├── observability.py              # Public API: setup_observability(), @tracked, track_duration()
├── logging/
│   ├── __init__.py               # Re-exports setup_logging (internal)
│   ├── config.py                 # setup_logging() implementation
│   └── formatters.py             # HumanFormatter, JSONFormatter
├── metrics/
│   ├── __init__.py               # init_metrics(), public metric constants
│   ├── registry.py               # Metric definitions (Counter, Histogram, Gauge)
│   ├── instruments.py            # @tracked, track_duration implementations
│   └── persistence.py            # flush_to_duckdb(), load_from_duckdb()
├── cli/commands/
│   ├── logs.py                   # logs clean, logs path, logs tail
│   └── stats.py                  # stats command
└── config.py                     # LoggingConfig (Pydantic, on MoneyBinSettings)
```

### Public API

```python
from moneybin.observability import setup_observability, tracked, track_duration
```

Consumers never import from `moneybin.logging` or `moneybin.metrics` directly (except for manual gauge/counter access). The `observability` module is the sole public surface for instrumentation.

Standard Python logging remains unchanged:

```python
import logging

logger = logging.getLogger(__name__)
```

## 1. Logging Consolidation

### Single Source of Truth

The Pydantic `LoggingConfig` on `MoneyBinSettings` is the only logging configuration. The existing `@dataclass LoggingConfig` in `src/moneybin/logging/config.py` and its `from_environment()` classmethod are deleted.

```python
class LoggingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_to_file: bool = True
    log_file_path: Path = Field(
        default=Path("logs/default/moneybin.log"),
        description="Base path for log files (profile directory derived from this)",
    )
    max_file_size_mb: int = Field(default=50, ge=1, le=1000)
    backup_count: int = Field(default=5, ge=1, le=50)
    format: Literal["human", "json"] = "human"
    sanitize: bool = Field(
        default=True,
        description="PII sanitization on all log output. Always on — exists for visibility only.",
    )
```

Env var override: `MONEYBIN_LOGGING__FORMAT=json`, etc.

### Deleted Code

- `LoggingConfig` dataclass in `logging/config.py`
- `from_environment()` classmethod
- `setup_dagster_logging()` — callers use `setup_observability(stream="sqlmesh")`
- `get_log_config_summary()` — replaced by `moneybin stats`
- Per-command `setup_logging()` calls in CLI command files — initialization moves to the single `main.py` callback

### Simplified Setup

```python
def setup_logging(stream: str = "cli", verbose: bool = False, profile: str | None = None) -> None:
```

Reads from `get_settings().logging` internally. No config objects passed around.

## 2. Log File Layout

All log files live under `~/.moneybin/logs/{profile}/` with name-first, date-second naming:

```
~/.moneybin/logs/{profile}/
├── cli_2026-04-20.log
├── cli_2026-04-19.log
├── mcp_2026-04-20.log
├── sqlmesh_2026-04-20.log
```

### Stream Routing

| Stream | File pattern | stderr | Notes |
|--------|-------------|--------|-------|
| `cli` | `cli_YYYY-MM-DD.log`, append | Always | All CLI commands for the day |
| `mcp` | `mcp_YYYY-MM-DD.log`, append | Always | File handler off by default for stdio transport; override with `MONEYBIN_LOGGING__MCP_FILE=true` |
| `sqlmesh` | `sqlmesh_YYYY-MM-DD.log`, append | Suppressed | SQLMesh output is noisy; file only |

### MCP Transport Detection

- **stdio** — stdin is not a TTY. File handler off by default (host captures stderr).
- **Local/HTTP** — stdin is a TTY or started via `moneybin mcp serve`. File handler on.

Override: `MONEYBIN_LOGGING__MCP_FILE=true` forces file logging in hosted mode.

### Profile Resolution

Derived from `MONEYBIN_PROFILE` env var or the database path. MCP server resolves profile the same way as CLI.

### Retention

Manual only. No automatic deletion or rotation.

```bash
moneybin logs clean --older-than 30d            # Delete old logs
moneybin logs clean --older-than 30d --dry-run  # Preview
```

## 3. Formatters & Sanitization

### Formatter Stack

```
SanitizedLogFormatter (outermost — always applied)
  └── wraps either:
      ├── HumanFormatter
      └── JSONFormatter
```

The `SanitizedLogFormatter` decorates the inner formatter's output, applying regex-based masking before records reach any handler. It is format-agnostic — works identically on human or JSON output.

### Sanitization Patterns

From the privacy-data-protection spec:

- SSN: `123-45-6789` → `***-**-****`
- Account numbers (8+ digits): `12345678901234` → `****...1234`
- Dollar amounts: `$1,234.56` → `$***`

Always on. Not configurable to disable.

### HumanFormatter

Default format. Two variants:

- **CLI stderr:** `%(message)s` — minimal, no timestamp clutter
- **File / MCP stderr:** `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### JSONFormatter

One JSON object per line. Includes `timestamp`, `logger`, `level`, `message`, plus any `extra` dict fields. Activated by `MONEYBIN_LOGGING__FORMAT=json` config setting.

### Handler Assignment

| Handler | Formatter |
|---------|-----------|
| stderr (CLI) | Sanitized → Human (message-only) |
| stderr (MCP) | Sanitized → Human (full format) |
| File (all streams) | Sanitized → Human or JSON (per config) |

## 4. Metrics — Registry & Instrumentation

### Library

`prometheus_client` for in-process metric types (Counter, Histogram, Gauge). No HTTP exposition — DuckDB is the only sink.

### Initial Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `moneybin_import_records_total` | Counter | `source_type` |
| `moneybin_import_duration_seconds` | Histogram | `source_type` |
| `moneybin_import_errors_total` | Counter | `source_type`, `error_type` |
| `moneybin_sqlmesh_run_duration_seconds` | Histogram | `model` |
| `moneybin_dedup_matches_total` | Counter | — |
| `moneybin_categorization_auto_rate` | Gauge | — |
| `moneybin_categorization_rules_fired_total` | Counter | `rule_id` |
| `moneybin_mcp_tool_calls_total` | Counter | `tool_name` |
| `moneybin_mcp_tool_duration_seconds` | Histogram | `tool_name` |
| `moneybin_db_query_duration_seconds` | Histogram | `operation` |

### Instrumentation API

```python
from moneybin.observability import tracked, track_duration


# Decorator — auto records duration, call count, errors
@tracked("import", labels={"source_type": "csv"})
def import_file(path: Path) -> ImportResult: ...


# Context manager — timing a block within a function
with track_duration("dedup"):
    deduplicated = run_dedup(txns)

# Manual — for gauges or custom logic
from moneybin.metrics import CATEGORIZATION_AUTO_RATE

CATEGORIZATION_AUTO_RATE.set(0.78)
```

### Behavior

- `@tracked` automatically: increments call counter, observes duration in histogram, increments error counter on exception.
- Both `@tracked` and `track_duration` emit a DEBUG-level log line on completion with operation name, duration, and labels.
- Adding a new metric: define in `registry.py` (one line), use `@tracked` or record manually at the call site.

## 5. Metrics Persistence

### Storage Table

```sql
CREATE TABLE app.metrics (
    metric_name VARCHAR NOT NULL,     -- e.g. 'moneybin_import_records_total'
    metric_type VARCHAR NOT NULL,     -- 'counter', 'histogram', 'gauge'
    labels JSON,                      -- {"source_type": "csv"}
    value DOUBLE NOT NULL,            -- counter/gauge value, or histogram sum
    bucket_bounds DOUBLE[],           -- histogram upper bounds (NULL for counter/gauge)
    bucket_counts BIGINT[],           -- histogram cumulative counts per bucket
    recorded_at TIMESTAMP NOT NULL    -- snapshot timestamp
);
```

### Flush Strategy

- **On shutdown** — `atexit` handler serializes the full prometheus registry to `app.metrics`. Primary persistence path.
- **Periodic** — every 5 minutes (configurable) for long-running processes (MCP server). Protects against unclean shutdown.
- **Additive** — each flush appends a new snapshot row per metric.

### Load on Startup

- **Counters** — cumulative. Restored from last snapshot so lifetime totals persist across restarts.
- **Gauges** — point-in-time. Not restored (reflect current state).
- **Histograms** — bucket counts restored for meaningful percentile calculations across sessions.

## 6. MCP Server Strategy

The MCP server calls `setup_observability(stream="mcp")` at startup.

### Specifics

- Metrics flush uses the periodic strategy (every 5 min) since MCP sessions can be long.
- Tool call instrumentation is automatic — a decorator on all tool handlers records `mcp_tool_calls_total` and `mcp_tool_duration_seconds` without per-tool opt-in.
- Privacy middleware decisions logged at INFO: `"Consent not granted, returning degraded response"`.

### What the AI Host Sees on stderr

```
2026-04-20 14:23:01 - moneybin.mcp - INFO - Server started, database: ~/.moneybin/data/default.db
2026-04-20 14:23:05 - moneybin.mcp - INFO - Tool spending.summary called
2026-04-20 14:23:05 - moneybin.mcp - INFO - Consent not granted, returning degraded response
```

## 7. CLI Commands

### `moneybin logs`

```bash
moneybin logs clean --older-than 30d            # Delete logs older than duration
moneybin logs clean --older-than 30d --dry-run  # Preview what would be deleted
moneybin logs path                              # Print log directory for current profile
moneybin logs tail                              # Tail latest CLI log
moneybin logs tail --stream mcp                 # Tail latest MCP log
moneybin logs tail --stream sqlmesh             # Tail latest SQLMesh log
moneybin logs tail -f                           # Follow mode
```

### `moneybin stats`

```bash
moneybin stats                    # Lifetime aggregates, all metrics
moneybin stats --since 7d        # Time-windowed view
moneybin stats --metric import   # Filter to a specific metric family
moneybin stats --output json     # Machine-readable for AI agents
```

### Example Output

```
$ moneybin stats
Import Records:     12,847 total (247 today)
Import Duration:    p50=0.8s  p95=2.1s
Auto-categorized:   78% of transactions
MCP Tool Calls:     1,203 total (top: spending.summary, transactions.search)
Dedup Matches:      1,891 records merged
```

Both commands respect active profile (`--profile` flag or `MONEYBIN_PROFILE`). Both support `--output json` for AI agent consumption. No interactive prompts.

## 8. Initialization

### Entry Point

```python
from moneybin.observability import setup_observability

# CLI (main.py callback — once, not per-command)
setup_observability(stream="cli", verbose=verbose, profile=profile)

# MCP server
setup_observability(stream="mcp", profile=profile)

# SQLMesh transform commands
setup_observability(stream="sqlmesh", profile=profile)
```

### What `setup_observability()` Does

1. Reads `get_settings().logging` for configuration
2. Calls `setup_logging()` — configures handlers, formatters, sanitizer for the given stream
3. Calls `init_metrics()` — initializes prometheus registry, loads prior state from DuckDB
4. Registers `atexit` handler for metrics flush on shutdown
5. For MCP stream: starts periodic flush timer (every 5 min)

### Dependency on privacy-data-protection

The `SanitizedLogFormatter` is defined in `src/moneybin/log_sanitizer.py` by the privacy spec. This spec wires it into all handlers. If privacy ships first (expected), the formatter exists. If observability ships first, the sanitizer is a no-op passthrough until privacy lands.

## 9. Migration Path

### Existing Code Changes

| Current | After |
|---------|-------|
| `from moneybin.logging import setup_logging` | `from moneybin.observability import setup_observability` |
| `setup_logging(cli_mode=True)` per command | Single `setup_observability(stream="cli")` in `main.py` callback |
| `setup_dagster_logging()` | `setup_observability(stream="sqlmesh")` |
| `LoggingConfig.from_environment()` | Deleted — `get_settings().logging` |
| `get_log_config_summary()` | Deleted — `moneybin stats` |

### New Dependencies

- `prometheus_client` — metric types and in-process registry
- `python-json-logger` — JSON log formatter (lightweight, stdlib-compatible)
