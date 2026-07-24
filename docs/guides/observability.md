<!-- Last reviewed: 2026-07-18 -->
# Observability

What MoneyBin records about itself, how to read it, and what's intentionally NOT recorded. Three surfaces: structured logs (per-profile log files + stderr), persisted metrics (in the `app.metrics` table), and the `system doctor` integrity sweep. The privacy threat model lives in [`threat-model.md`](threat-model.md); this guide is operational.

---

## Logs

- **Default location.** `<base>/profiles/<profile>/logs/{stream}_YYYY-MM-DD.log`, where `<base>` is the MoneyBin data directory (`~/.moneybin` by default). One file per stream per day.
- **Streams.** `cli` (CLI command execution), `mcp` (MCP server activity), `sqlmesh` (transform pipeline). SQLMesh output is routed to its own file and suppressed from the console; WARNING and above still reach stderr.
- **Default verbosity.** `INFO`. Pass `--verbose` on any command to raise the root level to `DEBUG`.
- **Format.** `human` by default. Set `MONEYBIN_LOGGING__FORMAT=json` to emit structured JSON to files. The **console always stays human-readable** regardless of `LOGGING__FORMAT` — JSON applies only to file handlers. Capture stderr if you want human-readable lines in journald/Docker; tail the file (or `moneybin logs <stream> --output json`) for JSON.
- **PII sanitization.** Every handler wraps its formatter in `SanitizedLogFormatter` (`src/moneybin/log_sanitizer.py`). Three patterns are masked unconditionally before bytes hit disk or terminal: SSNs (`NNN-NN-NNNN` → `***-**-****`), digit runs of 8+ (→ `****...NNNN` with last 4 retained), and dollar amounts (`$N`, `$N,NNN`, `$N.NN` → `$***`). The formatter masks and emits — it never drops a record. It is a safety net, not a substitute for clean log statements. The privacy ground rules around what may be logged in the first place live in [`threat-model.md`](threat-model.md#pii-redaction).
- **File permissions.** Log files are created with `0600` on POSIX so only the owning user can read them.

### Log-line shapes

**Human format** (default; single line per record):

```
2026-04-21 14:30:00,123 - moneybin.services.refresh - INFO - Refresh complete in 4.21s
```

CLI-stream console output uses a message-only variant — no timestamp prefix — so user-facing output stays clean. File output always carries the timestamp + logger + level prefix.

**JSON format** (`MONEYBIN_LOGGING__FORMAT=json`, file output only):

```json
{"timestamp": "2026-04-21T14:30:00.123456+00:00", "logger": "moneybin.services.refresh", "level": "INFO", "message": "Refresh complete in 4.21s"}
```

Required keys on every JSON record: `timestamp` (ISO 8601 UTC), `logger`, `level`, `message`. When the record carries exception info, an `exception` key holds the formatted traceback. Any non-standard `LogRecord` attribute set via `extra={...}` is copied verbatim alongside the required keys — but **MoneyBin does not currently emit structured event keys** (`event=refresh.completed`, etc.) as a code convention. Today, "did X succeed?" is answered by matching `message` substrings. Stable event names are tracked as a follow-up; until then, treat the `message` field as best-effort prose, not a contract.

### Reading and managing logs

```bash
# Tail the live CLI log
moneybin logs cli -f

# Last 100 ERROR-or-worse from the MCP stream in the last hour
moneybin logs mcp -n 100 --level ERROR --since 1h

# Search the SQLMesh log for a model name
moneybin logs sqlmesh --grep "fct_transactions"

# Emit structured rows for piping into jq
moneybin logs cli --output json --since 24h | jq '.[] | select(.level=="ERROR")'

# Print the log directory and exit
moneybin logs --print-path

# Prune logs older than 30 days (dry-run first)
moneybin logs --prune --older-than 30d --dry-run
moneybin logs --prune --older-than 30d
```

`moneybin logs` is a leaf command — a bare invocation exits `2` with a usage error. `--since` and `--until` accept a duration (`5m`, `1h`, `7d`) or an ISO-8601 timestamp.

### Log rotation

MoneyBin does **not** rotate log files itself — the daily filename rolls over at midnight, but old files accumulate until you prune them. Two patterns:

```bash
# Idempotent prune from cron / systemd timer
0 3 * * * moneybin logs --prune --older-than 30d
```

Or hand the directory to `logrotate(8)` (Linux):

```
/home/youruser/.moneybin/profiles/*/logs/*.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
    create 0600 youruser youruser
}
```

macOS users can drop an equivalent stanza into `/etc/newsyslog.d/`. Expected growth is rough — a quiet day with no imports adds well under a megabyte per stream; a heavy import or transform run can push the `sqlmesh` log into the tens of megabytes. If retention matters more than recent detail, prune by age; if disk matters more, rotate by size.

### Disabling file logging

Set `MONEYBIN_LOGGING__LOG_TO_FILE=false` (or `log_to_file: false` in your profile config) to skip file handlers entirely. Stderr output is unaffected — point your container, journald, or service manager at stderr to capture it elsewhere.

---

## Metrics

> **There is no HTTP `/metrics` endpoint today.** DuckDB (`app.metrics`) is the only sink. A Prometheus / OTel scrape endpoint is a candidate for a future release; until it ships, treat `app.metrics` and `moneybin stats` as the read surface.

- **Storage.** Persisted to the `app.metrics` table in the per-profile encrypted DuckDB. Each flush appends a snapshot row per (metric, labels).
- **Backend.** `prometheus_client` in-process registry — used for its instrument API, not its HTTP exposition.
- **Flush cadence.** On process exit (`atexit`), and every `MONEYBIN_METRICS__FLUSH_INTERVAL_SECONDS` (default `300`) for long-running processes like `moneybin mcp serve`. Flushes are skipped when no write connection was opened that session — read-only invocations don't take a write lock just to persist counters.
- **Counter restore.** On startup, counters are restored from the most recent snapshot so lifetime totals survive restarts. Gauges (point-in-time values) are not restored — after a restart, gauges read `0` until the next observation.

### Naming and labels

All metric names are prefixed `moneybin_` and registered in `src/moneybin/metrics/registry.py`. Label conventions are family-local — there is no single global set, and there is no `profile` label (each profile has its own `app.metrics` table). Common labels you will see:

- `source_type` (import family) — `ofx`, `csv`, `pdf`, `tabular`, etc. Bounded by the import-format catalog.
- `tool_name` (MCP family) — one value per registered MCP tool.
- `model` (SQLMesh family) — qualified model name, e.g. `core.fct_transactions`.
- `provider` (sync family) — `plaid` today.
- `error_code` (sync errors) — Plaid error codes like `ITEM_LOGIN_REQUIRED`, `INSTITUTION_DOWN`, `RATE_LIMIT_EXCEEDED`.
- `outcome` / `status` / `result` (import, sync, account match) — small enumerated sets per metric.
- `merchant_id` (categorization exemplar gauge) — unbounded in principle; an exemplar gauge alarm fires above 200 per merchant.

Cardinality is bounded by the underlying domain in every case except `merchant_id`. If you ship a `moneybin_*` metric, prefer labels with a known enumeration over open strings.

### `app.metrics` schema

| Column | Type | Purpose |
|---|---|---|
| `metric_name` | `VARCHAR NOT NULL` | Prometheus metric name (e.g. `moneybin_import_records_total`) |
| `metric_type` | `VARCHAR NOT NULL` | `counter`, `gauge`, or `histogram` (`CHECK`-constrained) |
| `labels` | `JSON` | Label key-value pairs as a JSON object |
| `value` | `DOUBLE NOT NULL` | Counter/gauge value, or histogram sum |
| `bucket_bounds` | `DOUBLE[]` | Histogram upper bounds; `NULL` for counter/gauge |
| `bucket_counts` | `BIGINT[]` | Histogram cumulative bucket counts; `NULL` for counter/gauge |
| `recorded_at` | `TIMESTAMP NOT NULL` | Snapshot timestamp |

### What's tracked

Families (see `src/moneybin/metrics/registry.py` for the complete list):

- **Import** — record counts, durations, errors, inbox outcomes, batch sizes; format-detection and batch lifecycle for tabular/OFX.
- **SQLMesh transforms** — per-model run duration.
- **Dedup & transfer detection** — pairs scored, matches, confidence distribution, pending-review gauge.
- **Categorization** — auto-rate, rule firings, matcher outcomes, write-skip-by-precedence, post-commit snowball latency, per-merchant exemplar counts.
- **Account matching** — outcome counters during tabular import.
- **MCP server** — per-tool call counts and duration.
- **Sync (Plaid via `moneybin-sync`)** — pull duration and outcomes, transactions loaded, per-institution errors by code, refresh-token rotation, connect-flow outcomes.
- **Audit log**, **Database** (query duration), **Synthetic data** — counters and durations.

`@tracked` / `track_duration()` additionally write `moneybin_tracked_calls_total`, `moneybin_tracked_duration_seconds`, and `moneybin_tracked_errors_total` — generic series for cross-cutting concerns where a named family doesn't fit.

### Reading metrics

```bash
# Lifetime aggregates (latest snapshot per metric+labels)
moneybin stats

# Filter to a metric family
moneybin stats --metric import

# Last 24h of activity
moneybin stats --since 24h

# Machine-readable
moneybin stats --output json | jq '.data[] | select(.type=="counter")'
```

`stats` returns the most recent snapshot per `(metric_name, labels)` and reports counts as `N total`, gauges as `value`, and histograms as `N observations (sum=Ns)`. Cumulative counters are not summed across snapshots — that would double-count.

---

## `moneybin system doctor`

A read-only sweep that asks: is the pipeline internally consistent right now?

```bash
moneybin system doctor                # human-readable
moneybin system doctor --verbose      # also print affected transaction IDs
moneybin system doctor --output json  # ResponseEnvelope for agents
```

What it audits (via `DoctorService`, which calls SQLMesh named audits plus two service-layer checks):

- **SQLMesh audits** on `core.fct_transactions` — FK integrity to `dim_accounts`, sign convention, transfer-pair balance, and any other named audits attached to core models.
- **Staging coverage** — every staged row reaches `core.fct_transactions`.
- **Categorization coverage** — share of transactions with a category assigned. Warns (not fails) when under 50% of non-transfer rows are categorized.

Exit codes: `0` if every invariant passes or warns; `1` if any fails. `--verbose` lists the offending IDs per failing invariant. The equivalent agent call is `system_status(sections=['doctor'], detail='full')` — the same checks in the standard response envelope.

### JSON envelope shape

`--output json` returns the standard `ResponseEnvelope`. Successful run:

```json
{
  "status": "ok",
  "summary": {"total_count": 5, "returned_count": 5, "has_more": false, "sensitivity": "low", "display_currency": "USD"},
  "data": {
    "passing": 5,
    "failing": 0,
    "warning": 0,
    "skipped": 0,
    "transaction_count": 12453,
    "invariants": [
      {"name": "fct_transactions_fk_dim_accounts", "status": "pass", "detail": null, "affected_ids": []},
      {"name": "fct_transactions_sign_convention", "status": "pass", "detail": null, "affected_ids": []}
    ]
  },
  "actions": []
}
```

On failure (exit `1`), the top-level `status` flips to `"error"`, an `error` object appears (`{"code": "invariant_failure", "message": "N invariant(s) failing"}`), and the offending entries in `data.invariants[]` carry `"status": "fail"` with `detail` set to `"N violation(s)"`. Match on `data.invariants[].status == "fail"` (or top-level `status == "error"`) to drive alerts. `affected_ids` is populated only when `--verbose` is passed.

---

## Programmatic monitoring

For agents and watchdog scripts driving MoneyBin directly:

- **Tail logs**: `moneybin logs <stream> -f --output json | your-event-handler`. JSON keys are fixed (`timestamp`, `logger`, `level`, `message`); `message` substring matching is the current contract for event detection.
- **Poll metrics**: `moneybin stats --output json` on a timer. Reads the latest snapshot per `(metric_name, labels)` from `app.metrics`.
- **Poll health**: `moneybin system doctor --output json` (CLI) or `system_status(sections=['doctor'], detail='full')` (agent surface). Match on `data.invariants[].status` or top-level `status`.
- **Cost signals**: MoneyBin does not call hosted LLMs and does not track token cost. If your agent (Claude Code, Codex, etc.) drives MoneyBin via MCP, cost tracking is your client's responsibility.

There is **no event subscription, webhook, or push notification** today. All access patterns are pull. A subscription contract is a candidate for a post-v1 release.

For consolidated CLI exit-code and MCP error-envelope taxonomy, see [`cli-reference.md`](cli-reference.md) and [`mcp-server.md`](mcp-server.md) respectively.

---

## Staleness signals

Lifetime counters survive restarts; freshness gauges don't. To answer "when did X last succeed," combine three sources:

- **Per-institution sync state**: `moneybin sync status --output json | jq '.data.institutions[] | {name, last_successful_pull_at, error}'`. Look for rows where `error` is non-null or `last_successful_pull_at` is older than your alert threshold.
- **Refresh log**: `moneybin logs cli --grep "Refresh complete" --since 24h --output json` — empty result means no successful refresh in the last day.
- **Metric snapshot recency**: query `app.metrics` for the latest `recorded_at` per metric — a successful pipeline writes there at every flush.

```sql
-- "When did each sync provider last succeed?"
SELECT json_extract_string(labels, '$.provider') AS provider,
       MAX(recorded_at) AS last_seen
FROM app.metrics
WHERE metric_name = 'moneybin_sync_pull_outcomes_total'
  AND json_extract_string(labels, '$.status') = 'success'
GROUP BY 1;
```

---

## Alerting recipes

Three patterns that compose with the cron / systemd timer of your choice:

**1. Doctor failure** — the canonical pipeline-health check.

```bash
moneybin system doctor --output json \
  | jq -e '.status == "ok"' >/dev/null \
  || /usr/local/bin/alert "moneybin doctor failed on $(hostname)"
```

**2. Stale sync** — no successful pull in the last 24h.

```bash
stale=$(moneybin sync status --output json \
  | jq -r '.data.institutions[] | select(.error != null or .last_successful_pull_at < (now - 86400 | strftime("%Y-%m-%dT%H:%M:%SZ"))) | .name')
[ -n "$stale" ] && /usr/local/bin/alert "moneybin: stale sync — $stale"
```

**3. Lock-contention spike** — count `DatabaseLockError` events per hour.

```bash
count=$(moneybin logs cli --since 1h --output json \
  | jq '[.[] | select(.message | contains("DatabaseLockError"))] | length')
[ "$count" -gt 5 ] && /usr/local/bin/alert "moneybin: $count lock errors in last hour"
```

Recipes use `message` substring matching because stable event names are not yet a code convention (see "Log-line shapes" above).

---

## Headless and container deployment

The CLI assumes operator presence for unlock and recovery flows; everything else is scriptable. Headless patterns:

**systemd unit** (long-running MCP server, key injected from a secret manager):

```ini
[Unit]
Description=MoneyBin MCP server
After=network-online.target

[Service]
Type=simple
User=moneybin
Environment=MONEYBIN_DATABASE__ENCRYPTION_KEY=...
Environment=MONEYBIN_LOGGING__LOG_TO_FILE=false
Environment=MONEYBIN_LOGGING__FORMAT=human
ExecStart=/usr/local/bin/moneybin mcp serve
Restart=on-failure
StandardError=journal

[Install]
WantedBy=multi-user.target
```

With `LOG_TO_FILE=false`, stderr is captured by journald (`journalctl -u moneybin`). If you switch to `LOG_TO_FILE=true`, the per-profile log files coexist with journald — both receive output.

**Docker healthcheck** — fail the container if pipeline integrity breaks:

```dockerfile
HEALTHCHECK --interval=5m --timeout=30s --start-period=1m \
  CMD moneybin system doctor || exit 1
```

For the encryption-key injection contract and recovery flow when the headless host loses its key, see [`database-security.md`](database-security.md).

---

## What is NOT recorded

- **Account numbers, descriptions, merchant names.** Logs never emit them; the `SanitizedLogFormatter` masks any account-shaped digit runs that slip through. They live in DuckDB tables because that is where they have to live, and the database is encrypted at rest.
- **Dollar amounts in log lines.** Masked to `$***` by the sanitizer.
- **PII in error messages.** CLI and MCP error envelopes return generic messages; stack traces with financial data in locals are caught at the boundary (see `.claude/rules/security.md`).
- **User-supplied passphrases.** Read via keyring or env var, never written to logs.
- **Telemetry, analytics, update checks.** None — the MoneyBin client is silent on the network unless you explicitly invoke `moneybin sync`. See the network-boundary section in [`threat-model.md`](threat-model.md#network-boundary).

The threat model is the source of truth for what crosses each boundary; this guide tells you where to look for evidence.
