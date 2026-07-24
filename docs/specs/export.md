# Export bundles and report delivery

> Milestone **M1O**.
> Status: implemented
> Type: Feature
> Last updated: 2026-07-21 — implementation reconciled across CLI and MCP.
> Companions: [`moneybin-cli.md`](moneybin-cli.md),
> [`moneybin-mcp.md`](moneybin-mcp.md),
> [`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md),
> [`connect-gsheet.md`](connect-gsheet.md),
> [`reports-overview.md`](reports-overview.md),
> [`reports-foundation.md`](reports-foundation.md),
> [`privacy-data-classification.md`](privacy-data-classification.md),
> [`privacy-data-protection.md`](privacy-data-protection.md), and
> [`smart-import-inbox.md`](smart-import-inbox.md).

## Goal

Export a complete canonical data bundle or one resolved report to local CSV,
Parquet, XLSX, or a dedicated Google Sheets destination. Every artifact is
verifiable: it names its data contract, redaction mode, creation time, and
integrity checks.

## Background

MoneyBin imports from files and live Sheets. M1O closes the reciprocal data-exit
path with canonical bundle and registered-report delivery on both the CLI and
MCP surfaces.

The user-facing workspace already has a deliberate split from encrypted app
state: `~/Documents/MoneyBin/<profile>/` contains visible files; `~/.moneybin/`
contains the encrypted database, keys, logs, and profile metadata. Exports join
the visible workspace beside the inbox:

```text
~/Documents/MoneyBin/<profile>/
├── inbox/
├── processed/
├── failed/
└── exports/
```

The existing Google Sheets connector is input-only: it uses the read-only
Sheets OAuth scope and treats each connected workbook as source-of-truth. An
export destination is a different object. MoneyBin must never publish into a
workbook registered as an inbound connection.

The reports framework is the sole report execution contract. A report export
uses its catalog/runner and provenance path; it never accepts SQL or creates a
second report query path. A future dynamic report therefore exports through the
same path without an export-specific integration.

## Requirements

### R1 — Two export subjects

Every run chooses one subject:

1. **Bundle** exports a closed catalog of 13 canonical, interoperable semantic
   tables: `accounts`, `transactions`, `transaction_lines`, `transfers`,
   `balances`, `balances_daily`, `categories`, `merchants`, `securities`,
   `investment_transactions`, `investment_lots`, `realized_gains`, and
   `holdings`. Transaction categories, notes, tags, and splits travel in their
   canonical transaction and line representations. The catalog is explicit in
   code; adding a new core table does not silently change a user's portable
   artifact. It excludes raw source copies, operational metadata, and
   app-internal state that has no portability contract.
2. **Report** exports exactly one named report and one resolved parameter set.
   The report is found through the catalog and executes exactly once through
   its existing runner, retaining the resulting provenance receipt.

`bundle` is intentionally not named `all`: it does not promise a dump of every
internal database table. `transactions` is intentionally not a top-level export
subject: a portable ledger includes accounts, categories, and curation as well
as transaction rows.

### R2 — One prepared snapshot, one renderer

`ExportService` resolves the requested subject into an immutable,
format-neutral prepared snapshot. The snapshot contains:

- the ordered data tables and typed columns;
- the profile, UTC creation timestamp, export kind, and format version;
- the selected redaction mode and output column classes;
- row counts and per-table checksums; and
- a generated data dictionary.

For a report, the snapshot additionally carries its provenance receipt: report
identifier, resolved parameters, SQL, lineage, output classes, freshness, and
graduation eligibility. This is the same information exposed by the report
verification surface, rendered as artifact metadata rather than a new query
path. Durable report artifacts are complete-or-fail: interactive MCP response
row caps do not truncate the prepared snapshot.

The report framework exposes the executed records and class map internally
before its terminal response renderer masks them. `ExportService` uses that
shared execution result and applies the selected export policy through the same
redaction engine. Existing CLI and MCP report responses retain their current
redacted terminal path. Export therefore changes the output policy, not report
selection, parameter binding, SQL generation, or provenance.

Renderers receive only a prepared snapshot. They do not select tables, invoke
reports, resolve privacy classes, or decide whether output is redacted. The v1
renderers are CSV, Parquet, XLSX, and Google Sheets. Adding a later format is a
renderer plus its integrity tests; it does not add another export-selection or
privacy path.

### R3 — Redaction is an explicit per-run decision

The surfaces make the per-run choice differently. Interactive CLI omission
prompts on every run; `--yes` and non-TTY execution select the redacted
default. `--unredacted` selects unredacted output affirmatively. No destination
stores a remembered redaction preference. In v1, `redacted` applies MoneyBin's
existing privacy redaction engine: it masks CRITICAL-class identifiers, while
lower-tier fields such as amounts, dates, merchant names, and descriptions
remain in the artifact. It is protection against critical-identifier exposure,
not a claim that the artifact is fully anonymized.

MCP callers supply `redaction_mode="redacted"` or
`redaction_mode="unredacted"`. An explicit `redaction_mode` does not prompt.
Omission elicits the choice when the client supports elicitation; otherwise the
tool returns a structured `mutation_redaction_choice_required` refusal. It never infers
unredacted output.

The redaction choice is an output policy after canonical selection or report
execution. It does not alter the underlying report query path. The manifest,
workbook metadata, or managed Sheets manifest tab states whether output is
redacted and records the emitted output classes.

### R4 — Local destination and immutable artifacts

`local:exports` is the built-in, profile-scoped destination. It resolves from
the existing configured inbox root:

```text
<inbox_root>/<profile>/exports/
```

The default profile export path is
`~/Documents/MoneyBin/<profile>/exports/`.

With the default inbox root and profile `personal`, the resolved path is:

```text
~/Documents/MoneyBin/personal/exports/
```

Each local run creates a new timestamped artifact. It never overwrites a prior
successful export. The service stages output in a restrictive temporary
directory, validates files and metadata, then publishes the completed artifact.
Directories are `0700`; files are `0600`.

CSV and Parquet use directory bundles:

```text
export-20260721T184233Z/
├── manifest.json
├── checksums.sha256
├── data-dictionary.json
└── tables/
    ├── accounts.csv
    ├── transactions.csv
    └── ...
```

Parquet changes only table file extensions and native column representation.
The manifest format and table set remain identical. `--compress zip` creates a
portable ZIP of a completed CSV or Parquet bundle. ZIP is the only v1
compression format. XLSX is already a ZIP container and rejects the option.

XLSX creates one timestamped workbook in the same destination root. It has one
worksheet per data table plus visible MoneyBin manifest and data-dictionary
worksheets. It is not a second logical bundle contract.

### R5 — Named destinations

The built-in `local:exports` destination is derived, not stored. Users can add
other local roots and Google Sheets destinations by name. Names are unique
across destination kinds and resolve through the standard explicit-id → exact
name → unambiguous-normalized-name contract.

Saved names must contain non-whitespace text and cannot contain `:` because
every target must remain addressable as `kind:name`. Normalization uses NFKC,
case folding, and whitespace folding. The normalized bare local name `exports`
is reserved for the derived `local:exports` target.

```text
moneybin export destination list
moneybin export destination add local <name> <path>
moneybin export destination add sheets <name> <url>
moneybin export destination remove <name>
```

Removing a destination removes MoneyBin's configuration only. It never deletes
a local artifact, a spreadsheet, a managed tab, or a user-owned tab.

### R6 — Google Sheets is an output-only destination

Adding a Sheets destination upgrades the existing installed-app OAuth grant to
the Sheets write scope through an explicit user interaction. The target stores
its stable spreadsheet ID, not its mutable URL. It is rejected if that ID is
already an inbound `gsheet` connection.

MoneyBin owns a named prefix of tabs in the destination workbook. A bundle
exports one canonical table per managed tab plus bundle manifest and
data-dictionary tabs. A report export owns one report-result tab plus separate
report manifest and data-dictionary tabs. MoneyBin
does not append to, modify, or read as source-of-truth any user-owned tab.
The prefix separates `Bundle Manifest`/`Bundle Dictionary`, `Report
Manifest`/`Report Dictionary`, result tabs, and temporary staging tabs, so a
report cannot replace any part of a bundle receipt by name or vice versa.

Sheets destinations are latest-state presentations, not archives. Each
successful run replaces only the matching managed tabs. Local artifacts retain
the immutable history.

Google Sheets has no workbook-wide transaction. The renderer must therefore:

1. write and validate temporary managed tabs;
2. retain the previous visible managed tabs until validation succeeds;
3. promote the new tabs and update the manifest tab; and
4. preserve the last known-good visible state on any failure.

Failed staging tabs remain clearly marked for recovery or cleanup. User-owned
tabs remain untouched in every failure mode.

Snapshot and destination reads use a short-lived read-only DuckDB connection.
The connection closes before local rendering, OAuth, or Sheets network I/O.
For a Sheets run, a hashed per-workbook role lease is acquired before that
connection closes; while holding it MoneyBin rechecks both the saved destination
and the absence of an inbound connection. Inbound connection insertion and
destination mutation use the same lease, preventing a workbook from changing
roles during publication without holding the global database writer lock over
network I/O.

MCP request cancellation is shared with renderer worker threads. Local atomic
rename and Sheets promotion are final publication barriers: after a timeout has
been reported, a surviving `to_thread` worker cannot publish an artifact or
promote managed tabs. If final promotion already entered, timeout cleanup waits
for that atomic boundary to leave before returning.

### R7 — CLI surface

The CLI owns the human-oriented command grammar:

```text
moneybin export bundle [--format csv|parquet|xlsx]
                        [--to local:<name>|sheets:<name>]
                        [--compress zip] [--unredacted]

moneybin export report <report-id> [--param key=value]
                                    [--format csv|parquet|xlsx]
                                    [--to local:<name>|sheets:<name>]
                                    [--compress zip] [--unredacted]
```

`moneybin export bundle` defaults to CSV and `local:exports`. A Sheets target
implies its native format and rejects `--format` and `--compress`. A report
export always represents one report and one parameter binding; it does not
combine multiple reports into a workbook or bundle.

The CLI prints the resolved absolute local path or named Sheets destination
before it writes. Its JSON output uses the standard response envelope and
reports artifact identity, destination, row counts, output classes, redaction
mode, checksums where applicable, and recovery actions on failure.

### R8 — MCP surface and parity

MCP and CLI expose the same observable outcomes over the same services. The
operating 47-tool standard registry uses exactly two export-specific tools and
an existing status read:

- `export_run` runs a bundle or report export after the caller supplies its
  destination, format, report parameters where applicable, and redaction
  choice.
- `exports_set` creates, updates, or removes one named destination through a
  typed target-state mutation.
- `system_status(sections=["exports"])` lists usable destinations and export
  readiness without consuming a third tool slot.

The split is deliberate. Reading destination status, configuring a destination,
and writing exported data have different read/write, confirmation, and recovery
contracts. A broad `export(operation=...)` tool would collapse those boundaries
into a large union and violate the surface-design contract. The registry remains
at 47 tools under the 50-tool hard limit; reports extend the catalog behind the
existing `reports` tool and consume no additional slots.

The CLI applies the R3 safe default rules. MCP requires an explicit mode or the
R3 elicitation/refusal path; no MCP prompt occurs when the mode is supplied.

### R9 — Observability and recoverability

The feature records counts, format, destination kind, redaction mode, duration,
and outcome. It never logs financial values, full local paths, spreadsheet URLs,
or generated table contents. Destination configuration mutations use a
repository and paired `app.audit_log` record under Invariant 10.

The completed local manifest and the subject-specific Sheets manifest tab are
the user-visible receipts. CLI and MCP receipts also identify the selected
format. Export runs do not add mutable application history merely to duplicate
those immutable receipts.

## Data model

### `app.export_destinations`

Named non-default destinations are protected mutable application state and use
an `ExportDestinationsRepo`. The table contains:

| Column | Type | Meaning |
|---|---|---|
| `destination_id` | `VARCHAR PRIMARY KEY` | Opaque truncated UUID. |
| `name` | `VARCHAR UNIQUE` | Stable user-facing reference. |
| `kind` | `VARCHAR` | Closed v1 set: `local` or `sheets`. |
| `local_path` | `VARCHAR NULL` | Absolute root for a local destination only. |
| `spreadsheet_id` | `VARCHAR NULL` | Stable Google workbook identity for Sheets only. |
| `managed_tab_prefix` | `VARCHAR NULL` | MoneyBin-owned Sheets tab namespace. |
| `created_at`, `updated_at` | `TIMESTAMP` | House timestamps. |

The kind-specific fields are mutually exclusive and validated before mutation.
The built-in `local:exports` remains derived from
`MoneyBinSettings.profile_inbox_dir / "exports"`; it has no row to drift from
the active profile.

### Artifact manifest

The on-disk `manifest.json` and its equivalent workbook/Sheets metadata include
a versioned artifact schema, export ID, subject, creation time, destination
kind, redaction mode, table names and schemas, row counts, checksums, and data
dictionary reference. Report manifests add report identifier, resolved
parameters, query/provenance receipt, class map, and freshness information.
Parameters receive the same selected redaction policy as result columns; their
classes travel with the manifest so a verifier can tell what was withheld.

Writers emit the current version. Readers and verification helpers support the
current and immediately preceding artifact versions once the public contract
locks at launch.

## Implementation boundaries

### Components

| Component | Responsibility |
|---|---|
| `ExportService` | Resolve subject, require redaction decision, prepare snapshot, dispatch one renderer. |
| `ExportDestinationsRepo` | Audited CRUD for named non-default destinations. |
| Bundle resolver | Select canonical portable tables and required curation. |
| Report resolver | Invoke the report catalog/runner and collect provenance metadata. |
| Renderers | Write prepared snapshots as CSV, Parquet, XLSX, or Sheets. |
| Sheets output adapter | Write-scope OAuth, temporary managed tabs, promotion, recovery. |

The renderer boundary is deliberately small across the four implemented
renderers. It centralizes the public artifact contract without introducing a
general plugin system.

### Implemented locations

- `src/moneybin/config.py` — derive the profile exports directory from the
  existing user-facing workspace root.
- `src/moneybin/exports/` and `src/moneybin/repositories/` — export service,
  destination repository, artifact model, and renderers.
- `src/moneybin/connectors/gsheet/` — explicit write-scope OAuth upgrade and
  output adapter; extend the fake Sheets client for staging/promotion tests.
- `src/moneybin/cli/` — expose the documented export grammar.
- `src/moneybin/mcp/` — register `export_run` and `exports_set`, extend
  `system_status`, capability map, and bounded-registry fixtures.
- `src/moneybin/metrics/registry.py` — export counters and duration metrics.
- `docs/` — CLI reference, MCP guide, export contract, capability map, roadmap,
  feature snapshot, and release notes.

## Testing strategy

1. **Prepared snapshots:** assert canonical bundle selection excludes raw and
   internal tables, retains required curation, preserves types, and emits a
   complete dictionary and checksum manifest.
2. **Reports:** assert built-in and dynamic-capable report exports use the
   catalog/runner, not ad-hoc SQL, and retain the provenance receipt.
3. **Renderers:** read CSV and Parquet back through DuckDB; inspect XLSX
   worksheets and metadata; verify table row counts and checksums.
4. **Privacy:** prove redacted default, explicit unredacted override, no
   remembered choice, and manifest class/redaction annotations.
5. **Local filesystem:** prove profile-scoped Documents paths, restrictive
   permissions, timestamped non-overwrite behavior, ZIP output, and staging
   recovery.
6. **Sheets:** use the fake client to prove inbound-destination collision
   refusal, write-scope setup, bundle/report metadata isolation, role-lease
   serialization, cancellable staging promotion, and preservation of the last
   successful snapshot on failure.
7. **Surfaces:** exercise CLI and MCP capability parity, actual rendered MCP
   schemas, confirmations, error envelopes, the 47-tool current registry, and
   the 50-tool hard limit.

## Dependencies

- Existing encrypted `Database`, `MoneyBinSettings`, profile workspace, and
  repository/audit pattern.
- DuckDB's supported CSV and Parquet writers.
- An XLSX writer compatible with the project dependency policy.
- Existing Google OAuth, Sheets API wrapper, and fake Sheets client, extended
  only after verifying the official Google Sheets API write contract.
- Existing report catalog and runner. Dynamic reports are not a prerequisite;
  when they land, they use the same catalog path.

## Out of scope

- Raw database dumps or raw import-file copies.
- Arbitrary SQL export.
- Report materialization, promotion, sharing, installation, or scheduling.
- Multiple report results in one export.
- Writing to an inbound Sheets connection or any user-owned tab.
- Persistent redaction preferences.
- Compression formats other than ZIP.
- Scheduled exports, external-storage destinations, and other spreadsheet
  providers.
