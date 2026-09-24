<!-- Last reviewed: 2026-09-14 -->
# Observability

What MoneyBin records about itself, how to read it, and what's intentionally NOT recorded. Three surfaces: structured logs (per-profile log files + stderr), persisted metrics (in the `app.metrics` table), and the `system doctor` integrity sweep. The privacy threat model lives in [`threat-model.md`](threat-model.md); this guide is operational.

## Logs

- **Default location.** `<base>/profiles/<profile>/logs/{stream}_YYYY-MM-DD.log`, where `<base>` is the MoneyBin data directory (`~/.moneybin` by default). One file per stream per day.
- **Streams.** `cli` (CLI command execution), `mcp` (MCP server activity), `sqlmesh` (transform pipeline). SQLMesh output is routed to its own file and suppressed from the console; WARNING and above still reach stderr.
- **CLI console and verbosity.** The CLI prints its requested result on stdout. Without the root flag, its console logger shows WARNING and ERROR on stderr; INFO and DEBUG diagnostics stay out of the console. Put `--verbose` before the command, as in `moneybin --verbose import files statement.csv`, to raise that CLI console logger to DEBUG and send its INFO and DEBUG diagnostics to stderr. Warnings, errors, and requested results remain visible without `--verbose`.
- **Format and file logging.** `human` is the console format regardless of `MONEYBIN_LOGGING__FORMAT`; that setting changes file handlers only. `MONEYBIN_LOGGING__LOG_TO_FILE=false` disables the CLI's daily file, not its stderr handler: warnings and errors still reach stderr, and `--verbose` still exposes CLI INFO and DEBUG diagnostics there. MCP and SQLMesh use their own stream handlers; this paragraph describes CLI console behavior. Capture stderr for human-readable lines in journald/Docker; tail the file (or `moneybin logs <stream> --output json`) for JSON when file logging is enabled.
- **PII sanitization.** Every handler wraps its formatter in `SanitizedLogFormatter` (`src/moneybin/log_sanitizer.py`). Three patterns are masked unconditionally before bytes hit disk or terminal: SSNs (`NNN-NN-NNNN` → `***-**-****`), digit runs of 8+ (→ `****...NNNN` with last 4 retained), and dollar amounts (`$N`, `$N,NNN`, `$N.NN` → `$***`). The formatter masks and emits — it never drops a record. It is a safety net, not a substitute for clean log statements. The privacy ground rules around what may be logged in the first place live in [`threat-model.md`](threat-model.md#pii-redaction).
- **File permissions.** Log files are created with `0600` on POSIX so only the owning user can read them.

### Log-line shapes

**Human format** (default; single line per record):

```
2026-04-21 14:30:00,123 - moneybin.orchestration.refresh - INFO - Refresh complete in 4.21s
```

The prefix follows the invoking process, not the file. A `moneybin mcp serve` process writes the full prefix above. A CLI invocation uses a message-only variant — no timestamp, logger, or level — on the console *and* in the files it writes, including the `sqlmesh` file it fills during a transform, so `cli_YYYY-MM-DD.log` after a CLI run reads as a transcript of what the terminal showed. Which prefix a file carries decides what `moneybin logs` can filter on it — see "Reading and managing logs".

**JSON format** (`MONEYBIN_LOGGING__FORMAT=json`, file output only):

```json
{"timestamp": "2026-04-21T14:30:00.123456+00:00", "logger": "moneybin.orchestration.refresh", "level": "INFO", "message": "Refresh complete in 4.21s"}
```

Required keys on every JSON record: `timestamp` (ISO 8601 UTC), `logger`, `level`, `message`. When the record carries exception info, an `exception` key holds the formatted traceback. Any non-standard `LogRecord` attribute set via `extra={...}` is copied verbatim alongside the required keys — but **MoneyBin does not currently emit structured event keys** (`event=refresh.completed`, etc.) as a code convention. Today, "did X succeed?" is answered by matching `message` substrings. Stable event names are tracked as a follow-up; until then, treat the `message` field as best-effort prose, not a contract.

### Reading and managing logs

The stream argument, the follow and prune flags, and `--print-path` are in the generated [`logs` CLI reference](../reference/cli/logs.md). A plain read tails the file:

```console
$ uv run moneybin logs cli -n 5
Using profile: demo
Profile resolved from config.yaml
Using profile: demo
Profile resolved from config.yaml
Using profile: demo
Profile resolved from config.yaml
```

Five lines, message-only, because a CLI run wrote them (see "Log-line shapes" above).

Three behaviours the flag list does not carry:

- **`moneybin logs` is a leaf command.** A bare invocation exits `2` with a usage error.
- **`--since` and `--until` accept a duration** (`5m`, `1h`, `7d`) or an ISO-8601 timestamp.
- **`--level`, `--grep`, `--since`, `--until`, and `--output json` read only lines carrying the `timestamp - logger - level - message` prefix.** A CLI-written line has no prefix, so it is dropped: on a profile whose logs came from CLI runs, `moneybin logs cli --grep "Refresh complete"` prints nothing and `moneybin logs cli --output json` returns `[]`, even with the matching line sitting in the file. Filter those streams with `moneybin logs cli -n 200` piped into `grep`, or read the file directly; the filters work as documented against an `mcp` stream written by `moneybin mcp serve`.

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

macOS users can drop an equivalent stanza into `/etc/newsyslog.d/`. For a size to start from: building the family demo profile (4 accounts, 2,886 transactions) and running one `moneybin refresh` wrote 16 KB to that day's `cli` stream and 324 KB to its `sqlmesh` stream — the transform pipeline produces roughly twenty times the volume the commands do, and it scales with how much each run rebuilds, not with how many commands you ran. A heavy import or transform run can push the `sqlmesh` log into the tens of megabytes. If retention matters more than recent detail, prune by age; if disk matters more, rotate by size.

### Disabling file logging

Set `MONEYBIN_LOGGING__LOG_TO_FILE=false` (or `log_to_file: false` in your profile config) to skip file handlers entirely. Stderr output is unaffected — point your container, journald, or service manager at stderr to capture it elsewhere.

## Metrics

> **There is no HTTP `/metrics` endpoint.** DuckDB (`app.metrics`) is the only sink, and nothing listens on a port. Read metrics with `moneybin stats` or by querying `app.metrics` directly; a Prometheus or OTel scraper needs a shim you write against one of those two.

- **Storage.** Persisted to the `app.metrics` table in the per-profile encrypted DuckDB. Each flush appends a snapshot row per (metric, labels).
- **Backend.** `prometheus_client` in-process registry — used for its instrument API, not its HTTP exposition.
- **Flush cadence.** Once per session, at the end. CLI runs flush from an `atexit` hook; `moneybin mcp serve` flushes when the server closes its database. There is no interval flush and no setting to configure one, so a long-lived MCP session writes nothing to `app.metrics` until it shuts down. Flushes are skipped when no write connection was opened that session — read-only invocations don't take a write lock just to persist counters.
- **Counter restore.** On startup, counters are restored from the most recent snapshot so lifetime totals survive restarts. Gauges (point-in-time values) are not restored — after a restart, gauges read `0` until the next observation.

### Naming and labels

All metric names are prefixed `moneybin_` and registered in `src/moneybin/metrics/registry.py`. Label conventions are family-local — there is no single global set, and there is no `profile` label (each profile has its own `app.metrics` table). Common labels you will see:

- `source_type` (import family) — `ofx`, `csv`, `pdf`, `tabular`, etc. Bounded by the import-format catalog.
- `tool_name` (MCP family) — one value per registered MCP tool.
- `model` (SQLMesh family) — qualified model name, e.g. `core.fct_transactions`.
- `provider` (sync family) — `plaid` today.
- `error_code` (sync errors) — Plaid error codes like `ITEM_LOGIN_REQUIRED`, `INSTITUTION_DOWN`, `RATE_LIMIT_EXCEEDED`.
- `outcome` / `status` / `result` (import, sync, account match) — small enumerated sets per metric.
- `command` (CLI rendering family) — the underscored command path, e.g. `reports_run`, `transactions_list`. The same derivation the audit trail uses, so a counter and an audit row name one command the same way.
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
- **CLI text rendering** — `--wide` requests and column omissions per command, and invocations of commands that are still stubs. These persist only on sessions that also write business data, since a read-only run does not take a write lock just to flush counters — read the wide-versus-omitted ratio as a direction to look, never as a census.
- **Audit log**, **Database** (query duration), **Synthetic data** — counters and durations.

Every metric is recorded manually (`METRIC.labels(...).inc()` / `.observe()`) at the call site that matters — there is no generic instrumentation decorator.

### Reading metrics

`--metric`, `--since`, and `--output` are in the generated [`stats` CLI reference](../reference/cli/stats.md). A bare `moneybin stats` prints every subsystem that has a snapshot:

```console
$ uv run moneybin stats
Using profile: demo
Import pipeline
Import Batch Size:           3 snapshots (sum=0.00 files)
Inbox Sync Duration Seconds: 3 snapshots (sum=0.00 s)
Tabular import
OFX Fitid Collision Repaired: 0 total
PDF Extraction Confidence:    3 snapshots (sum=0.00 score)
PDF Replay Guard Failure:     0 total
Smart import confirmation
Import Detection Score: 3 snapshots (sum=0.00 score)
SQLMesh transforms
SQLMesh Run Duration Seconds (model: transform_apply): 2 snapshots (sum=7.05 s)
```

Another 88 lines follow, under the headers from Deduplication through User-created reports. `--metric` narrows to one family by name substring — it matches the header's metrics, not the header:

```console
$ uv run moneybin stats --metric import
Using profile: demo
Import pipeline
Import Batch Size: 3 snapshots (sum=0.00 files)
Smart import confirmation
Import Detection Score: 3 snapshots (sum=0.00 score)
```

`stats` returns the most recent snapshot per `(metric_name, labels)`, groups the result under a header per subsystem, and reports counters as `N total`, gauges as `value`, and histograms as `N snapshots (sum=X <unit>)`. Cumulative counters are not summed across snapshots — that would double-count.

Two details of that line are worth reading precisely:

- **`N` counts stored snapshots, not observations.** A snapshot is one flush, and a flush writes the metric's running total — so `4 snapshots` means the metric was written four times, which says nothing about how many files were imported or how many queries ran.
- **The unit is declared, not derived from the name.** It comes from `HISTOGRAM_UNITS` in `src/moneybin/metrics/registry.py`. Nine histograms end in a suffix naming a *dimension* rather than a unit — `_batch_size`, `_score`, `_confidence`, `_ratio`, `_rows_affected` — and `moneybin_import_batch_size` counts files, a fact that lives only in the declaration. Adding a histogram without adding its unit fails a test rather than printing an unlabelled figure.
- **The subsystem is declared too**, in `METRIC_DOMAINS` beside it, and for the same reason: a name's leading token is not a subsystem. Categorization declares metrics under five prefixes — `categorization_`, `categorize_`, `auto_rule_`, `rule_` and `merchant_exemplar_count` — so grouping on the first underscore scattered one subsystem across four headers. Blocks print in the registry's declaration order, and a metric name kept from an older version prints under `Other` rather than vanishing. Both tables key on the name `app.metrics` stores, which for a counter is the declaration minus its `_total` — the flush strips that suffix, so `Counter("…_records_total")` is looked up as `…_records`.

`stats --output json` is one of the operations-metadata reads that stay off the standard response envelope (`logs`, `migrate status`, `db info`, and `db ps` are the others; with the `db query` operator bypass, the CLI reference names all six). It emits `{"metrics": [...]}`, each entry carrying `name`, `type`, `labels`, `value`, `snapshots`, and `last_recorded` — so the jq path is `.metrics[]`, not `.data[]`, and there is no top-level `status` to match on.

## `moneybin system doctor`

A read-only sweep that asks: is the pipeline internally consistent right now?

A clean sweep prints one line. On the family demo profile, immediately after `moneybin refresh`:

```console
$ uv run moneybin system doctor
Using profile: demo

65 invariants checked across 2,886 transactions — all passing
```

`--verbose` names each invariant that ran, plus the affected IDs on anything failing:

```console
$ uv run moneybin system doctor --verbose
Using profile: demo
✅ fct_transactions_fk_integrity
✅ fct_investment_transactions_fk_integrity
✅ fct_investment_transactions_sign_convention
✅ fct_investment_transactions_uniqueness
✅ fct_transactions_sign_convention
✅ bridge_transfers_balanced
✅ transform_model_presence
```

The other 58 invariant rows and the closing summary line are cut. `--full` scans every protected `app.*` row instead of a sample, and `--output json` returns the envelope below; both are in the generated [`system` CLI reference](../reference/cli/system.md).

What it audits, via `DoctorService`:

- **SQLMesh named audits** attached to core models — FK integrity and sign convention on `core.fct_transactions`, the same two plus uniqueness on `core.fct_investment_transactions`, and transfer-pair balance on `core.bridge_transfers`.
- **Transform model presence** — the SQLMesh models the pipeline expects are materialized.
- **Dedup reconciliation** and **cross-source duplicates** — duplicate account overlap, plus cross-source duplicate transactions that have no merge proposal.
- **Categorization coverage** — share of the transactions that need a category that have one. Scoped to the population `core.uncategorized_queue` is drawn from, so transfer legs and archived accounts are out. Warns (not fails) when under 50%.
- **Currency integrity** — profile currencies and rows carrying an unknown currency.
- **Protected `app.*` audit coverage** — one check per repository-wrapped table (`user_categories`, `categorization_rules`, `account_settings`, `balance_assertions`, `imports`, and the rest), verifying every mutation left an audit-log row. Sampled over recent rows by default; `--full` scans the whole table.
- **Orphaned app state** — `app.*` rows pointing at accounts, categories, or transactions that no longer exist.
- **13 investment checks** — staging rejects, opening-lot review, unmodeled legs, holdings-snapshot divergence, source overlap, unresolved securities, conflicting security references, unreported and phantom holdings, price disagreement, unpriced holdings, stale prices, and unmapped price sources.

Exit codes: `0` if every invariant passes, warns, or is skipped; `1` if any fails. `--verbose` lists the offending IDs per failing invariant. The equivalent agent call is `system_status(sections=['doctor'], detail='full')` — the same checks in the standard response envelope.

### JSON envelope shape

`--output json` returns the standard `ResponseEnvelope` on one line; `jq` is doing the indenting here:

```console
$ uv run moneybin system doctor --output json | jq .
{
  "status": "ok",
  "summary": {
    "total_count": 1,
    "returned_count": 1,
    "has_more": false,
    "sensitivity": "low",
    "display_currency": null
  },
  "data": {
    "passing": 65,
    "failing": 0,
    "warning": 0,
    "skipped": 0,
    "transaction_count": 2886,
    "invariants": [
      {
        "name": "fct_transactions_fk_integrity",
        "status": "pass",
        "detail": null,
        "affected_ids": [],
        "recovery_actions": []
      },
      {
        "name": "investment_unmapped_price_source",
        "status": "pass",
        "detail": null,
        "affected_ids": [],
        "recovery_actions": []
      }
    ]
  },
  "actions": []
}
```

The `Using profile: demo` line the CLI writes to stderr and the 63 `invariants` entries between the first and the last are cut. `summary.total_count` counts the sweep, not its invariants — the per-invariant tallies are `data.passing` and `data.failing`.

On failure (exit `1`), the top-level `status` flips to `"error"`, an `error` object appears (`{"code": "audit_invariant_failure", "message": "N invariant(s) failing"}`), and the offending entries in `data.invariants[]` carry `"status": "fail"` with `detail` set to `"N violation(s)"`. Match on `data.invariants[].status == "fail"` (or top-level `status == "error"`) to drive alerts. `affected_ids` is populated only when `--verbose` is passed.

## Programmatic monitoring

For agents and watchdog scripts driving MoneyBin directly:

- **Tail logs**: `moneybin logs <stream> --output json | your-event-handler`, polled on a timer. JSON keys are fixed (`timestamp`, `logger`, `level`, `message`); `message` substring matching is the current contract for event detection. Two limits: `--output json` applies only to the backfill — a bare JSON array rather than the standard envelope, and once `-f` starts following, every new line is echoed as raw text, so a follow loop and a JSON parser cannot be combined. And the array holds only prefix-carrying lines, so a stream written by CLI runs returns `[]` (see "Reading and managing logs"); pipe `moneybin logs <stream> -n <N>` into your own matcher for those.
- **Poll metrics**: `moneybin stats --output json` on a timer. Reads the latest snapshot per `(metric_name, labels)` from `app.metrics`.
- **Poll health**: `moneybin system doctor --output json` (CLI) or `system_status(sections=['doctor'], detail='full')` (agent surface). Match on `data.invariants[].status` or top-level `status`.
- **Cost signals**: MoneyBin does not call hosted LLMs and does not track token cost. If your agent (Claude Code, Codex, etc.) drives MoneyBin via MCP, cost tracking is your client's responsibility.

There is **no event subscription, webhook, or push notification**. Every access pattern above is a pull, so a watchdog runs one of them on a timer it sets itself.

For consolidated CLI exit-code and MCP error-envelope taxonomy, see [`cli-reference.md`](cli-reference.md) and [`mcp-server.md`](mcp-server.md) respectively.

## Staleness signals

Lifetime counters survive restarts; freshness gauges don't. To answer "when did X last succeed," combine three sources:

- **Per-institution sync state**: `moneybin sync status --output json | jq '.data.connections[] | {institution_name, last_sync, error_code}'`. Look for rows where `error_code` is non-null or `last_sync` is older than your alert threshold.
- **Refresh log**: `moneybin logs cli -n 500 | grep "Refresh complete"` — no match means no successful refresh in the file being read. The `--grep` and `--output json` forms of this read return nothing on a CLI-written stream whether or not a refresh ran; the reason is under "Reading and managing logs".
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

## Alerting recipes

Three patterns that compose with the cron / systemd timer of your choice:

**1. Doctor failure** — the canonical pipeline-health check.

```bash
moneybin system doctor --output json \
  | jq -e '.status == "ok"' >/dev/null \
  || /usr/local/bin/alert "MoneyBin doctor failed on $(hostname)"
```

**2. Stale sync** — no successful pull in the last 24h.

```bash
stale=$(moneybin sync status --output json \
  | jq -r '.data.connections[] | select(.error_code != null or .last_sync < (now - 86400 | strftime("%Y-%m-%dT%H:%M:%SZ"))) | .institution_name')
[ -n "$stale" ] && /usr/local/bin/alert "moneybin: stale sync — $stale"
```

**3. Lock-contention spike** — count `DatabaseLockError` events per hour.

```bash
count=$(moneybin logs mcp --since 1h --output json \
  | jq '[.[] | select(.message | contains("DatabaseLockError"))] | length')
[ "$count" -gt 5 ] && /usr/local/bin/alert "moneybin: $count lock errors in last hour"
```

That recipe reads the `mcp` stream because `--since` and `--output json` drop the unprefixed lines a CLI run writes. Against the `cli` stream, count with `moneybin logs cli -n 500 | grep -c DatabaseLockError` instead, and bound the window by line count rather than by time.

Recipes use `message` substring matching because stable event names are not yet a code convention (see "Log-line shapes" above).

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
Environment=MONEYBIN_PROFILE=default
Environment=MONEYBIN_PROFILE__DEFAULT__DATABASE__ENCRYPTION_KEY=...
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

## What is NOT recorded

- **Account numbers, descriptions, merchant names.** Logs never emit them; the `SanitizedLogFormatter` masks any account-shaped digit runs that slip through. They live in DuckDB tables because that is where they have to live, and the database is encrypted at rest.
- **Dollar amounts in log lines.** Masked to `$***` by the sanitizer.
- **PII in error messages.** CLI and MCP error envelopes return generic messages; stack traces with financial data in locals are caught at the boundary (see `.claude/rules/security.md`).
- **User-supplied passphrases.** Read via keyring or env var, never written to logs.
- **Telemetry, analytics, update checks.** None — the MoneyBin client is silent on the network unless you explicitly invoke `moneybin sync`. See the network-boundary section in [`threat-model.md`](threat-model.md#network-boundary).

The threat model is the source of truth for what crosses each boundary; this guide tells you where to look for evidence.

## What is not built yet

- **No HTTP `/metrics` endpoint.** `app.metrics` is the only sink; read it with `moneybin stats` or by querying the table. A Prometheus or OTel scraper needs a shim you write against one of those two. ([Metrics](#metrics))
- **No interval flush.** Metrics are written once per session, at exit, and only when the session took a write lock. A long-lived `mcp serve` process writes nothing until it stops. ([Metrics](#metrics))
- **No event subscription, webhook, or push notification.** Every access pattern is a pull on a timer you set. ([Programmatic monitoring](#programmatic-monitoring))
- **No stable event names in log records.** `message` substring matching is the contract for detecting an event. ([Log-line shapes](#log-line-shapes))
- **`moneybin logs` filters skip CLI-written lines.** `--level`, `--grep`, `--since`, `--until`, and `--output json` read only prefixed lines, which a CLI run does not write; pipe `logs <stream> -n <N>` into `grep` for those streams. ([Reading and managing logs](#reading-and-managing-logs))
