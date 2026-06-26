<!-- Last reviewed: 2026-05-17 -->
# Data Import

MoneyBin ingests financial data from files you already have (CSV, TSV, Excel, Parquet, Feather, OFX/QFX/QBO) and from Plaid-connected banks. Every file lands in `raw.*`, flows through the SQLMesh pipeline into `core.fct_transactions` / `core.dim_accounts`, and is queryable by the CLI, MCP server, and any DuckDB client. This guide walks through the entry points by source tool and by file format, plus the housekeeping commands you'll reach for after the first import.

> **How does MoneyBin know two files are the same account?** When you import the
> same account from more than one source (a QFX and a CSV, history files plus
> Plaid), MoneyBin collapses them into one canonical account and asks you to
> confirm when it isn't sure. The full signal-by-signal breakdown — and what each
> file format provides — is in [Account Matching](../reference/account-matching.md).

## Before you import

You only need a profile and an initialized database to import. If you've already done `moneybin profile create` and `moneybin db init`, skip ahead. Otherwise:

```bash
moneybin profile create main          # one-time per identity (work vs personal)
moneybin db init                      # one-time per profile
moneybin db unlock                    # each session
moneybin system doctor                # exits 0 when ready to import
```

See the [Profiles guide](profiles.md) and [Database and security guide](database-security.md) for detail. Plaid sync needs additional setup — see [Live banking sync](#live-banking-sync-plaid).

## Back up first

Before pointing MoneyBin at real history, snapshot the profile:

```bash
moneybin db backup                    # encrypted snapshot under data/<profile>/backups/
moneybin db restore <backup-path>     # roll back if an import goes wrong
```

`import revert` (below) handles batch-level rollback after a single import, but a full `db backup` is the right thing to do before your first real ingest of years of history.

## From your previous tool

If you're migrating from another personal-finance tool, start here. The named formats below are matched by **header signature** — the unique set of column headers each tool exports — on first import; future imports of the same shape skip detection.

### Tiller

Tiller users have the deepest first-class support. The `tiller` format profile matches the standard Tiller Money sheet export (Transactions tab → "Download as CSV").

```bash
moneybin import files ~/Downloads/transactions.csv --format tiller
```

Auto-detection picks it up without `--format` — the header signature is distinctive:

```bash
moneybin import files ~/Downloads/transactions.csv
```

Re-import overlapping months without fear. Cross-source dedup (SHA-256 content hashes) means the same row imported twice is a no-op.

### Mint (and Mint successors)

The `mint` format profile reads the standard Mint CSV export. Even though Mint itself shut down, the export format is preserved by every Mint-successor tool that offered a "bring your history" import path.

```bash
moneybin import files ~/Downloads/transactions.csv --format mint
```

If you've since moved to a different tool and have that tool's export, see the relevant subsection below or fall back to the [generic CSV path](#csv-tsv-excel-parquet-feather).

### YNAB

The `ynab` format profile reads the YNAB "All Transactions" export (Budget → Export budget data → unzip → the `Register.csv` file).

```bash
moneybin import files ~/Downloads/Register.csv --format ynab
```

### Maybe / Sure

The `maybe` format profile reads Maybe Finance's CSV export.

```bash
moneybin import files ~/Downloads/maybe-export.csv --format maybe
```

### Lunch Money

No first-class profile yet. Lunch Money exports clean CSV (Settings → Developers → "Export to CSV") that auto-detection handles:

```bash
moneybin import files ~/Downloads/lunchmoney-export.csv --account-name "Checking"
```

If auto-detection picks the wrong column for date or amount on the first import, override and the choice is saved for next time:

```bash
moneybin import files ~/Downloads/lunchmoney-export.csv \
  --override date="Date" --override amount="Amount"
```

### Monarch / Copilot

Same shape as Lunch Money: no named migration profile yet, but auto-detection reads their exports. Both tools expose a "Download transactions" CSV in account settings.

```bash
moneybin import files ~/Downloads/monarch-transactions.csv --account-name "Joint Checking"
```

There is no automated API pull from Monarch or Copilot today — you export, you import.

### Beancount / hledger

No direct ledger ingest. MoneyBin doesn't parse `.beancount` postings or `journal` files, and there's no plan to round-trip back to ledger syntax.

The working path: export the same source transactions your ledger was built from — OFX/QFX downloads from your bank, or a CSV per account — and import those. If your ledger has data that doesn't exist anywhere else (manual adjustments, opening balances), use [manual transaction entry](#manual-transaction-entry) for the unique rows. If round-tripping to plain-text accounting is a hard requirement, Beancount + Fava remains the better tool.

### Generic CSV from any other tool

If your previous tool isn't listed and exports CSV (Actual Budget, Firefly III, GnuCash, a spreadsheet you maintained by hand), the tabular importer handles it directly. Skip ahead to [CSV / TSV / Excel / Parquet / Feather](#csv-tsv-excel-parquet-feather).

## What survives the trip

The migration question that matters: **what carries over from your old tool, and what doesn't?** MoneyBin preserves the source columns each format profile knows about; everything else is dropped at the staging layer. The table below summarizes by source class. A ✅ means the field lands in `core.fct_transactions` (or an adjacent core table) and is queryable post-import; 🟡 means partial; ❌ means the column is read off the source row but not persisted.

| Source | Categories | Notes / Memos | Tags / Labels | Splits | Transfers | Account names |
|--------|------------|---------------|---------------|--------|-----------|---------------|
| **Tiller** | ✅ | ✅ (Full Description) | ❌ | ❌ source-side; rebuild via `transactions splits` | 🟡 detected post-load via matching | ✅ multi-account in one file |
| **Mint** | ✅ | ✅ (Original Description) | ❌ (Labels column dropped) | ❌ | 🟡 detected post-load | ✅ multi-account in one file |
| **YNAB** | ✅ (Category Group/Category) | ✅ (Memo) | 🟡 (Flag preserved as status) | ❌ source-side; rebuild via `transactions splits` | 🟡 detected post-load | ✅ |
| **Maybe / Sure** | ✅ | ✅ (note) | ❌ (tags column dropped) | ❌ | 🟡 detected post-load | ✅ |
| **Generic CSV** (Lunch Money, Monarch, Copilot) | ✅ if a category column is detected | ✅ if a memo/notes column is detected | ❌ | ❌ | 🟡 detected post-load | ✅ if column present, else use `--account-name` |
| **OFX / QFX / QBO** | ❌ (format carries none) | ✅ (`<MEMO>`) | ❌ | ❌ | 🟡 detected post-load | ✅ |
| **Plaid sync** | 🟡 (Plaid's PFC taxonomy, separate from MoneyBin categories) | ✅ | ❌ | ❌ | 🟡 detected post-load | ✅ |

**A few specifics to set expectations:**

- **Source categories are preserved verbatim** in `core.fct_transactions.category` and `subcategory`. They are *not* mapped onto MoneyBin's category taxonomy — instead they bootstrap your categorization history and you can layer rules and overrides on top. See the [categorization guide](categorization.md).
- **YNAB envelope state** (budgeted-but-unspent, Age of Money, scheduled transactions) does not survive — MoneyBin's budgeting surface is on the roadmap, not shipped.
- **Splits** in the source file are not parsed as separate child rows on import. The parent row's amount lands intact; you rebuild splits via `moneybin transactions splits add` if you want them broken out.
- **Transfers** are detected *after* import by the matching pipeline (`core.bridge_transfers`), not from any source column. Two rows on opposite sides of the same transfer collapse into one logical event after refresh, whether they came from one file or two different sources.
- **Tags / Labels** are not yet a first-class concept on imported rows — `moneybin transactions tags add` lets you tag manually post-import.

## Importing history, then connecting Plaid

A common migration pattern: bring years of history in from files, then connect Plaid for ongoing sync. The recommended order is **history first, Plaid second**, because cross-source dedup (per-row content hashes on date + amount + description + account) collapses overlaps in `core.fct_transactions` regardless of order.

```bash
moneybin import files ~/Downloads/tiller-export.csv     # 5 years of history
moneybin sync link --institution "Chase"                # now link live
moneybin sync pull                                       # last 18 months from Plaid overlaps history; dedup handles it
```

The bridge tables (`core.bridge_*`) record which source contributed each row, so provenance is preserved even after dedup. If you ever need to inspect overlap, query `app.match_decisions` for `match_type = 'dedup'`.

## How long this takes

Order-of-magnitude, not benchmarks:

- A single 10 MB CSV: a few seconds end-to-end (extract + load + refresh).
- 5 years of monthly Tiller exports: tens of seconds total.
- A large multi-year institutional CSV dump (100k+ rows): can stretch to a minute or two with the refresh pipeline included.
- Pass `--no-refresh` to defer the SQLMesh apply when chaining many imports; finish with one `moneybin transform apply`.

Actual timing for any specific batch appears in `moneybin import history` and in the structured log under the profile data directory.

## By file format

If you're working from raw bank or institution exports rather than another personal-finance tool, organize by file type.

### OFX / QFX / QBO

Most US banks and credit cards expose OFX or QFX downloads in their online portals; QBO is the QuickBooks variant.

```bash
moneybin import files ~/Downloads/checking.qfx
moneybin import files ~/Downloads/*.ofx
```

**What gets extracted:** accounts (name, type, institution, account ID), transactions (date, amount, description, type, FITID — OFX's per-transaction unique-ID field), and balances (ledger and available, as-of date).

**Institution resolution** runs in order:

1. The `<FI><ORG>` element inside the OFX header.
2. The `<FI><FID>` element matched against a static lookup of well-known FIDs (the OFX standard's institution identifiers — Wells Fargo, Chase, etc.).
3. A filename heuristic (`wellsfargo_2025.qfx`).
4. The `--institution` flag, consulted only if steps 1–3 yield nothing.

```bash
moneybin import files ~/Downloads/statement.qfx --institution "Wells Fargo"
```

You almost never need step 4 — only if your bank uses a non-standard FID the importer can't auto-resolve.

**Re-import safety.** OFX files carry their own transaction IDs (FITID), so re-importing the same statement is a no-op. The import log also tracks file-content hashes — re-running the exact same file is short-circuited. Pass `--force` to re-import anyway (creates a new batch).

**Description cleanup.** OFX `<PAYEE>` and `<MEMO>` fields are HTML-entity-decoded at import; banks that double-escape (Wells Fargo's `AT&amp;amp;T`) are unwound to `AT&T`.

### CSV / TSV / Excel / Parquet / Feather

One pipeline handles all five. Same command, file-type-driven dispatch.

```bash
moneybin import files ~/Downloads/chase_activity.csv --account-name "Chase Checking"
moneybin import files ~/Downloads/report.xlsx --sheet "Transactions"
moneybin import files ~/Downloads/export.parquet --account-name "Main Account"
```

**What the smart importer saves you from:** writing a column-mapping file by hand. It detects format (encoding, delimiter, file type, preamble rows), finds the header row, matches headers to canonical fields via a 100+ entry alias dictionary, and validates each guess against actual data (a column mapped as `date` is checked for date-parseable values). On success the mapping is saved as a user format and subsequent files with the same header signature skip detection. Full design: [smart-import-tabular spec](../specs/smart-import-tabular.md).

A three-tier **confidence score** drives prompts: high-confidence mappings load without prompting; medium and low surface the inferred mapping and ask before continuing. Pass `-y` / `--yes` to auto-accept the top match for unattended runs.

**Supported formats:**

| Format | Extensions | Notes |
|--------|-----------|-------|
| CSV | `.csv` | Auto-detects delimiter (comma, semicolon, pipe). |
| TSV | `.tsv`, `.tab` | Tab-delimited. |
| Excel | `.xlsx` | Auto-selects the largest sheet; `--sheet` overrides. |
| Parquet | `.parquet` | |
| Feather | `.feather` | |

**Sign conventions.** Different institutions encode expenses and income differently. Auto-detection usually picks the right one; `--sign` overrides:

| Convention | Meaning | Typical sources |
|-----------|---------|----------------|
| `negative_is_expense` | Negative = expense (most common) | Chase, Wells Fargo |
| `negative_is_income` | Negative = income (inverted) | Some credit cards |
| `split_debit_credit` | Separate debit and credit columns | Citi, many European banks |

**Number formats.** Specify with `--number-format` when needed: `us` (`1,234.56`), `european` (`1.234,56`), `swiss_french` (`1'234.56`), `zero_decimal` (`123456` cents).

**Preview before committing.** `moneybin import preview` runs detection and column-mapping without writing to the database:

```bash
moneybin import preview ~/Downloads/transactions.csv
moneybin import preview ~/Downloads/report.xlsx --sheet Sheet2
```

**Most common per-file overrides** (single-file mode only — passing multiple paths disables them):

| Flag | Purpose |
|------|---------|
| `-n, --account-name` | Account name when the file is single-account and the column doesn't carry one. |
| `-f, --format` | Force a named format (`tiller`, `mint`, etc.). |
| `--override` | Field-to-column override, repeatable (`--override date=Posted --override amount=Amt`). |
| `--sign` | Sign convention override. |
| `-y, --yes` | Auto-accept the top fuzzy account match without prompting. |

Full flag list (institution overrides, date format, encoding, sheet, delimiter, safety-limit toggles, format-save toggles): [CLI reference](cli-reference.md).

### PDF (native-text)

Bank-statement PDFs with selectable text. Drop them into `moneybin import files` or the watched inbox.

```bash
moneybin import files ~/Downloads/chase_statement.pdf
moneybin import files ~/Downloads/*.pdf
```

**What the smart importer saves you from:** writing a layout recipe by hand. On first contact MoneyBin reads the PDF locally with `pdfplumber`, derives a recipe (column positions, header names, date format, sign convention, number format, and the start/end anchors that bound the transaction table), validates the extracted rows by reconciling their sum against the statement's reported balance delta (±1¢ tolerance), and persists the recipe to `app.pdf_formats` keyed by a fingerprint of the layout (issuer + ordered column headers + page bucket). The next statement from the same institution skips derivation entirely — the saved recipe replays in milliseconds.

**What happens to your data:**

- **Transaction-shaped PDFs** (statements with a date / description / amount table) land in `raw.tabular_transactions` (`source_type='pdf'`) and flow through the SQLMesh pipeline to `core.fct_transactions` like any other source. Categorization, search, reports — all work the same. *Caveat for inbox-routed PDFs:* `moneybin import inbox` does not yet trigger the SQLMesh refresh for `file_type='pdf'`, so raw rows land but core/reports won't see them until a `moneybin transform apply` runs. Inbox-routed OFX and tabular files refresh automatically; the inbox refresh gate will be extended to PDFs in a follow-up.
- **Non-transaction PDFs**, and transaction PDFs that don't reconcile cleanly, fall back to the seed path: the extracted tables land as queryable JSON in `raw.pdf_seeds` with an auto-generated typed view (`raw.pdf_<alias>`). You can `SELECT` against the view via `moneybin sql query` or `db query`, but the rows do not flow to `core.fct_transactions`.

**When the fallback triggers** (any one of):

- The statement's reported balance delta and the extracted-transaction sum disagree by more than 1¢ (often a missed footer total row, a column-header misclassification, or a statement that splits transactions across multiple tables MoneyBin's derivation didn't merge).
- The first-pass extraction confidence on column types is low (typically scanned-then-OCR'd PDFs with brittle column boundaries).
- The PDF has no balance-summary metadata to reconcile against.
- The transaction table extracts zero rows.
- The PDF's number format is anything other than `us`. The executor today only routes `us`-format statements to `raw.tabular_transactions`; `european`, `swiss_french`, and `zero_decimal` are recognized at the recipe level but always fall back to the seed path until executor support lands.

In every fallback case the recipe is NOT saved — MoneyBin only persists recipes that round-trip cleanly. Re-imports of the same statement either replay the saved recipe (no derivation cost) or fall back again to the seed path.

**Privacy posture.** PDF content stays local — no network egress, no LLM. The deterministic recipe ladder handles the column-shapes statements typically use; an opt-in agent-bridge rung that escalates harder layouts to the LLM agent you're already driving MoneyBin with is the next phase (Phase 2b, in flight).

**Listing saved PDF formats:**

```bash
moneybin import formats list --type=pdf
moneybin import formats show chase_a1b2c3d4e5f6   # works across tabular and PDF formats
```

PDF format names are `{issuer_slug}_{12-char SHA-256 hex of the layout fingerprint}` — the exact name appears in `formats list`. Recipe version is a separate column, not part of the name. The list view shows name, institution, routing (`transactions` / `seed`), front-end (`pdfplumber` / `vision`), recipe version, times-used, and last-used date.

**Re-import safety.** Each transaction's `transaction_id` is a content hash over the statement period, transaction date, raw amount, description, and account — row position is deliberately excluded so a recipe tweak that shifts row order doesn't renumber every following `transaction_id`. Re-running the same PDF from the same path produces zero net new transaction rows: the `(transaction_id, account_id, source_file)` primary key on `raw.tabular_transactions` rejects the duplicates. Each call does still open a fresh `raw.import_log` entry, and re-importing the same content from a *different* path will write a new set of raw rows (because `source_file` is part of the dedup key). `--force` does not currently apply to PDFs — it is an OFX-only flag.

**Reverting.** Every PDF import — routed-transactions path or seed-path fallback — is reversible by `import_id`:

```bash
# Capture import_id, then back it out:
moneybin import files ~/Downloads/chase_statement.pdf --output json | jq -r '.data.files[0].import_id'
moneybin import revert <import_id>
```

## Live banking sync (Plaid)

Plaid-connected sync pulls transactions, balances, and accounts directly from supported US banks. The connection brokers through `moneybin-sync` (the Plaid integration backend you can self-host).

One-time setup:

```bash
moneybin sync login                                # device auth flow with moneybin-sync
moneybin sync link --institution "Chase"           # opens Plaid Hosted Link in your browser
```

Pull on demand:

```bash
moneybin sync pull                                 # cursor-based incremental sync
moneybin sync pull --institution "Chase"           # one institution only
moneybin sync pull --force                         # reset cursor; re-fetch full history
```

Plaid rows land in `raw.plaid_*` and flow through SQLMesh into the same `core.fct_transactions` and `core.dim_accounts` as your file imports. Cross-source dedup runs automatically, so a Plaid transaction and the same transaction from an OFX import collapse to one canonical row.

**Coverage today:** cash and credit-card accounts flow through the canonical pipeline. Investment, loan, mortgage, and HSA accounts get loaded if Plaid exposes them, but the holdings / cost-basis / balance-sheet surfaces those deserve land with the investments milestone — see the [roadmap](../roadmap.md).

`sync pull` runs the post-load refresh pipeline (matching, SQLMesh apply, categorization) automatically; pass `--no-refresh` to defer.

```bash
moneybin sync status                               # connected institutions, last sync, health
```

## Live tabular sync (Google Sheets)

Google Sheets connects via direct OAuth — no aggregator, no moneybin-sync mediation — and re-pulls on every `moneybin refresh`. Use for a Tiller-style ledger sheet (full matching/categorization pipeline) or any other sheet you maintain (lands as queryable JSON + typed views).

```bash
moneybin gsheet auth                                            # one-time OAuth (browser flow)
moneybin gsheet connect "https://docs.google.com/spreadsheets/d/.../edit#gid=0"
moneybin gsheet pull                                            # explicit pull (also runs on refresh)
moneybin gsheet                                                  # list connected sheets
```

See the [Google Sheets guide](connect-gsheet.md) for adapter choice, drift recovery, and the limitations of the read-only OAuth scope.

## Inbox: drain a watched folder

Drop files into the inbox directory and `moneybin import inbox` drains them in one batch.

```bash
moneybin import inbox path                         # print the inbox path
moneybin import inbox list                         # dry-run: show what would be processed
moneybin import inbox                              # drain it
```

The inbox lives at `~/Documents/MoneyBin/<profile>/inbox/`. Successes move to `processed/YYYY-MM/`; failures move to `failed/YYYY-MM/` with a YAML error sidecar describing what went wrong. A per-profile lockfile at `~/Documents/MoneyBin/<profile>/.inbox.lock` (advisory `flock`) prevents concurrent drains; a crashed drain releases the lock on process exit, so the next invocation proceeds normally.

There is no built-in `--watch` mode today — cron or `launchd`/`systemd` against `moneybin import inbox` is the supported pattern.

Useful when you keep a folder of monthly OFX downloads or a shared download directory — drop, drain, done.

## Re-importing and dedup

Two layers prevent duplicates:

1. **The import log.** Each completed import records a SHA-256 of the source file. Re-running the same file is short-circuited — nothing is loaded, no batch is created. Pass `--force` / `-F` to load anyway (creates a new batch).
2. **Per-row content hashes.** Inside the SQLMesh pipeline, cross-source dedup matches rows by content hash (date + amount + description + account) across CSV, OFX, and Plaid. Two imports of the same transaction collapse to one canonical row in `core.fct_transactions`; the bridge tables retain provenance for both sources.

So: re-importing a file is a no-op. Importing the same transaction from two different sources is also a no-op — the second source contributes its provenance without double-counting.

## Reverting an import

If a whole batch landed wrong (wrong account, wrong format, garbled file), revert it.

```bash
moneybin import history                            # list recent batches with their IDs
moneybin import revert abc123-...                  # delete all rows from that batch
moneybin import revert abc123-... --yes            # skip the confirmation prompt
```

Revert deletes all transactions and accounts loaded in the specified batch and marks the batch as reverted in the import log. The original file is untouched on disk — you can re-import after fixing whatever was wrong (different `--format`, `--account-name`, etc.). Reverts cascade through downstream `core.*` and `reports.*` tables on the next refresh.

**Fixing one row without nuking the batch.** There's no general `transactions update` command today (a known gap). The shipped subcommands cover the most common corrections:

- Add or correct notes: `moneybin transactions notes add <id> "..."`
- Add or correct tags: `moneybin transactions tags add <id> ...`
- Split into child rows: `moneybin transactions splits add <id> ...`
- Re-categorize: `moneybin transactions categorize <id> ...`

For anything beyond those (rewriting the amount or date on a single row), the current path is revert the batch, fix the source file, and re-import.

## Manual transaction entry

For cash, gifts, reimbursements, and anything else that doesn't come from a file or sync.

```bash
moneybin transactions create --date 2026-05-17 --amount -42.50 \
  --description "Coffee with Alex" --account-name "Cash"
```

One transaction at a time. For bulk paste, build a small CSV and run it through `moneybin import files`. Once a transaction exists, notes, tags, and splits live on top — see the [categorization guide](categorization.md).

## Inspecting what's already imported

```bash
moneybin import status                             # per-table row counts and date ranges
moneybin import history                            # batch log with IDs, status, confidence
moneybin import history --import-id abc123        # one batch in detail
moneybin import formats list                       # built-in and user-saved formats
moneybin import formats show tiller                # field mapping and signature for one format
moneybin import formats delete my_custom_format    # remove a user-saved format (built-ins are protected)
```

Pair any read command with `--output json` for machine-readable output — the same envelope shape the MCP server uses.

## For scripts and agents

`moneybin import` is designed to drive from cron, CI, or an agent loop. The contract:

**Non-interactive flags.**

- `--yes` / `-y` — auto-accept the top fuzzy account match without prompting. Does **not** auto-accept low-confidence column mappings on first detection of a new format — those still require interactive review, so first-touch of a new format should be done interactively, then automated thereafter.
- `--output json` — emits the [standard response envelope](mcp-server.md#response-envelope) on stdout.
- `--no-refresh` — defer the post-load SQLMesh apply. Useful when chaining many imports.
- `--force` / `-F` — re-import a file already in the log.

**Exit codes for `moneybin import files`.**

- `0` — every file imported and (when refresh is enabled) the post-load refresh succeeded.
- `1` — at least one file failed, or the refresh pipeline failed. Per-file failures do **not** abort the batch (the rest still import); the non-zero exit signals "look at the envelope."
- `2` — usage error (missing arg, bad flag).

The same contract applies to `moneybin import inbox`: the command exits 0 when the drain completes, even if individual files moved to `failed/`. Detect per-file failure via the `--output json` envelope or by checking the `failed/` directory — do not rely on exit code alone for the inbox.

**`--output json` envelope shape** (mutating-command envelope; see [mcp-server.md](mcp-server.md#response-envelope) for the full schema):

```json
{
  "data": {
    "imported_count": 2,
    "failed_count": 1,
    "total_count": 3,
    "transforms_applied": true,
    "transforms_duration_seconds": 4.2,
    "files": [
      {"path": "a.ofx", "status": "imported", "source_type": "ofx", "rows_loaded": 142, "import_id": "..."},
      {"path": "b.csv", "status": "imported", "source_type": "csv", "rows_loaded": 88, "import_id": "..."},
      {"path": "c.xlsx", "status": "failed", "source_type": null, "rows_loaded": 0, "import_id": null, "error": "ValueError"}
    ]
  },
  "metadata": {"sensitivity": "low", ...}
}
```

`transforms_error` is set on the envelope when refresh failed; non-zero exit follows.

**Concurrency.** The inbox lockfile serializes inbox drains within a profile. There is no equivalent lock around bare `moneybin import files` — two parallel invocations against the same profile race on the import log. The supported pattern is: serialize at the caller (one cron job, one agent worker), or drop files in the inbox and let the inbox lock handle ordering.

**SIGTERM mid-import.** Not yet a guaranteed clean rollback. If a file is mid-load when the process dies, the import-log row may stay in `in_progress` state; rerun against a fresh process and the next `import files` against the same file is short-circuited by the file-hash log. A clean partial-batch rollback contract is planned but not shipped — for now, treat SIGTERM as "may need a manual `import revert` on the partial batch."

## What's not supported yet

The honest gap list. See the [roadmap](../roadmap.md) for current sequencing.

- **Direct Beancount / hledger ingest.** No plain-text-accounting parsers; export to OFX or CSV instead.
- **Automated migration from Monarch or Copilot.** No API pull; CSV-only.
- **Broker / investment statements.** Plaid investment accounts load if exposed, but holdings, cost basis, and FIFO lot tracking land with the investments milestone.
- **Multi-currency at import time.** Today MoneyBin treats every amount as USD. Original-currency preservation and FX gain/loss are planned.
- **Scanned / image-only PDF.** PDFs without selectable text (scanned pages, fax-quality images) are not supported — text extraction yields no rows and the import fails with a zero-row error. Use a document scanner with OCR to produce a native-text PDF first. The Phase 2b agent-bridge rung will not change this — vision-capable extraction is a separate milestone.
- **General-purpose row-level updates.** No `transactions update` command; use notes, tags, splits, categorize subcommands or revert and re-import.
- **`--watch` mode for the inbox.** Cron or `launchd`/`systemd` is the supported pattern today.
- **Bulk manual transaction entry.** One row at a time via `moneybin transactions create`; for batches, build a CSV and import it.
