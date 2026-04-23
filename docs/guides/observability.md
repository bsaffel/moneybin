# Observability

MoneyBin includes structured logging, application metrics, and instrumentation for visibility into operations.

## Logging

Stream-based structured logging with daily rotating log files and automatic PII sanitization.

### CLI Commands

```bash
# Follow log output in real time
moneybin logs tail -f

# View logs from a specific stream
moneybin logs tail --stream mcp

# Print the log directory path
moneybin logs path

# Delete logs older than 30 days
moneybin logs clean --older-than 30d
```

### Log Streams

| Stream | File pattern | Content |
|--------|-------------|---------|
| `cli` | `cli_YYYY-MM-DD.log` | CLI command execution |
| `mcp` | `mcp_YYYY-MM-DD.log` | MCP server operations |
| `sqlmesh` | `sqlmesh_YYYY-MM-DD.log` | SQLMesh pipeline runs |

Log files are stored per-profile under `~/.moneybin/profiles/<name>/logs/`.

### PII Sanitization

A `SanitizedLogFormatter` automatically detects and masks PII patterns in all log output:

| Pattern | Example | Masked as |
|---------|---------|-----------|
| SSN | `123-45-6789` | `***-**-****` |
| Account numbers | `1234567890` | `****7890` |
| Dollar amounts | `$1,234.56` | `$*,***.**` |

The sanitizer masks and warns — it never suppresses log entries. Financial data never appears in log files.

## Metrics

`prometheus_client`-backed counters, gauges, and histograms track operations across sessions with automatic DuckDB persistence.

```bash
# View lifetime metric aggregates
moneybin stats show
```

Metrics are persisted to the `app.metrics` table in DuckDB and flushed on shutdown plus periodically during long operations. This means metrics survive process restarts — you see lifetime aggregates, not just the current session.

### What's Tracked

- Import counts and durations
- Transform pipeline runs
- Categorization operations
- MCP tool invocations
- Database operations

## Instrumentation

Two instrumentation primitives for zero-boilerplate operation timing:

### `@tracked` Decorator

Wraps a function with automatic duration tracking:

```python
from moneybin.observability import tracked


@tracked
def import_ofx(
    file_path: Path,
) -> ImportResult: ...  # duration and success/failure automatically recorded
```

### `track_duration` Context Manager

Measures arbitrary code blocks:

```python
from moneybin.observability import track_duration

with track_duration("transform_pipeline"):
    sqlmesh_apply()
```

Both feed into the prometheus_client metrics system for ongoing visibility into performance characteristics.
