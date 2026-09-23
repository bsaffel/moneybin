<!-- Last reviewed: 2026-09-17 -->
# Data Import

MoneyBin ingests financial data from files you already have (CSV, TSV, Excel, Parquet, Feather, OFX/QFX/QBO, native-text PDF) and from Plaid-connected banks. Every file lands in `raw.*`, flows through the SQLMesh pipeline into `core.fct_transactions` / `core.dim_accounts`, and is queryable by the CLI, MCP server, and any DuckDB client. This guide walks through the entry points by source tool and by file format, plus the housekeeping commands you'll reach for after the first import.

When the same account arrives from more than one source (a QFX and a CSV, history files plus Plaid), MoneyBin collapses them into one canonical account, and stops to ask when the signal is weak. The signal-by-signal breakdown — and what each file format provides — is in [Account Matching](../reference/account-matching.md).

Every transcript below is real output captured against one empty profile, from four synthetic files written for this guide: a CSV with a non-obvious column layout, an "Example Bank" OFX, an unreadable text file, and a CSV passed with a format name that does not exist. Every command ran from the directory holding the four files, so the paths are the bare names as typed. Trims are noted where they occur, and `import history` was captured before the forced re-import shown under [For scripts and agents](#for-scripts-and-agents).

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
moneybin db backup                       # encrypted snapshot under data/<profile>/backups/
moneybin db restore --from <backup-path> # roll back if an import goes wrong
```

`import revert` (below) handles batch-level rollback after a single import, but a full `db backup` is the right thing to do before your first real ingest of years of history.

## From your previous tool

If you're migrating from another personal-finance tool, start here. The named formats below are matched by **header signature** — the unique set of column headers each tool exports — on first import; future imports of the same shape skip detection.

### Tiller

The `tiller` format profile matches the standard Tiller Money sheet export (Transactions tab → "Download as CSV"). It is one of three built-in format profiles — `tiller`, `mint`, `ynab` — that `moneybin import formats list` reports with source `builtin`.

```bash
moneybin import files ~/Downloads/transactions.csv --format tiller
```

Auto-detection picks it up without `--format` — the header signature is distinctive:

```bash
moneybin import files ~/Downloads/transactions.csv
```

Overlapping months are safe to re-import. Cross-source dedup (SHA-256 content hashes) collapses a row imported twice into one canonical row in `core.fct_transactions`.

### Mint (and Mint successors)

The `mint` format profile reads the standard Mint CSV export. Even though Mint itself shut down, the export format is preserved by every Mint-successor tool that offered a "bring your history" import path.

```bash
moneybin import files ~/Downloads/transactions.csv --format mint
```

If you've since moved to a different tool and have that tool's export, see the relevant subsection below or fall back to the [generic CSV path](#csv--tsv--excel--parquet--feather).

### YNAB

The `ynab` format profile reads the YNAB "All Transactions" export (Budget → Export budget data → unzip → the `Register.csv` file).

```bash
moneybin import files ~/Downloads/Register.csv --format ynab
```

### Maybe / Sure

No named format profile. `--format maybe` is refused, naming what does exist:

```console
$ moneybin import files checking.csv --format maybe
Using profile: main
Importing CSV file: checking.csv
Import failed for one file: ValueError
❌ checking.csv [?] — 0 rows
   Unknown format 'maybe'. Available: ['everyday-checking', 'mint', 'tiller', 'ynab']
```

(`everyday-checking` in that list is a user-saved format this profile picked up earlier in the guide, not a built-in.) Maybe Finance's CSV export goes through the generic tabular path below — omit `--format` and confirm the detected mapping once.

### Lunch Money

No first-class profile yet. Lunch Money exports clean CSV (Settings → Developers → "Export to CSV") that auto-detection reads correctly:

```bash
moneybin import files ~/Downloads/lunchmoney-export.csv --account-name "Checking"
```

The first file of a layout MoneyBin hasn't seen before returns `confirmation_required` with the detected mapping — review it, then re-run with `--confirm` to load it (the mapping is then saved, so every later file with the same header signature loads without asking):

```bash
moneybin import files ~/Downloads/lunchmoney-export.csv --account-name "Checking" --confirm
```

If auto-detection picks the wrong column for date or amount, correct it directly — an explicit override resolves immediately, even on first contact, and the choice is saved for next time:

```bash
moneybin import files ~/Downloads/lunchmoney-export.csv \
  --override date="Date" --override amount="Amount"
```

### Monarch / Copilot

Same shape as Lunch Money: no named migration profile yet, but auto-detection reads their exports. Both tools expose a "Download transactions" CSV in account settings. As with any new layout, the first import returns `confirmation_required` for review; re-run with `--confirm` to load it:

```bash
moneybin import files ~/Downloads/monarch-transactions.csv --account-name "Joint Checking"
moneybin import files ~/Downloads/monarch-transactions.csv --account-name "Joint Checking" --confirm
```

There is no automated API pull from Monarch or Copilot today — you export, you import.

### Beancount / hledger

No direct ledger ingest. MoneyBin doesn't parse `.beancount` postings or `journal` files, and there's no plan to round-trip back to ledger syntax.

The working path: export the same source transactions your ledger was built from — OFX/QFX downloads from your bank, or a CSV per account — and import those. If your ledger has data that doesn't exist anywhere else (manual adjustments, opening balances), use [manual transaction entry](#manual-transaction-entry) for the unique rows. If round-tripping to plain-text accounting is a hard requirement, Beancount + Fava remains the better tool.

### Generic CSV from any other tool

If your previous tool isn't listed and exports CSV (Actual Budget, Firefly III, GnuCash, a spreadsheet you maintained by hand), the tabular importer handles it directly. Skip ahead to [CSV / TSV / Excel / Parquet / Feather](#csv--tsv--excel--parquet--feather).

## What survives the trip

The migration question that matters: **what carries over from your old tool, and what doesn't?** MoneyBin preserves the source columns each format profile knows about; everything else is dropped at the staging layer. The table below summarizes by source class. A ✅ means the field lands in `core.fct_transactions` (or an adjacent core table) and is queryable post-import; 🟡 means partial; ❌ means the column is read off the source row but not persisted.

| Source | Categories | Notes / Memos | Tags / Labels | Splits | Transfers | Account names |
|--------|------------|---------------|---------------|--------|-----------|---------------|
| **Tiller** | ✅ | ✅ (Full Description) | ❌ | ❌ source-side; rebuild via `transactions splits` | 🟡 detected post-load via matching | ✅ multi-account in one file |
| **Mint** | ✅ | ✅ (Original Description) | ❌ (Labels column dropped) | ❌ | 🟡 detected post-load | ✅ multi-account in one file |
| **YNAB** | ✅ (Category Group/Category) | ✅ (Memo) | 🟡 (Flag preserved as status) | ❌ source-side; rebuild via `transactions splits` | 🟡 detected post-load | ✅ |
| **Generic CSV** (Maybe / Sure, Lunch Money, Monarch, Copilot) | ✅ if a category column is detected | ✅ if a memo/notes column is detected | ❌ | ❌ | 🟡 detected post-load | ✅ if column present, else use `--account-name` |
| **OFX / QFX / QBO** | ❌ (format carries none) | ✅ (`<MEMO>`) | ❌ | ❌ | 🟡 detected post-load | ✅ |
| **Plaid sync** | 🟡 (Plaid's PFC taxonomy, separate from MoneyBin categories) | 🟡 (`memo` is always NULL; Plaid's raw text lands in the separate `original_description` column) | ❌ | ❌ | 🟡 detected post-load | ✅ |

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

The refresh pipeline, not the file, sets the floor. A 3-transaction OFX imported after one 7-row CSV batch took 10.0s wall, of which the SQLMesh transforms were 7.5s:

```console
$ time moneybin import files savings.ofx
Using profile: main
Importing OFX file: savings.ofx
Created import batch: 5f5a8552...
Extracted 1 account(s), 3 transaction(s)
Import 5f5a8552... finalized: complete (5 imported, 0 rejected)
Running transforms
Transforms completed in 7.53s
Account-link backfill wrote 0 new pending decisions
Merchant linking complete: 0 linked automatically, 0 sent for review.
  Accounts: 1
  Transactions: 3
  Balances: 1
  Date range: 2026-01-05 to 2026-01-31
  Core tables rebuilt (dim_accounts, fct_transactions)
✅ savings.ofx [ofx] — 3 rows
👀 Created account: EXAMPLE BANK savings …5678 (7165edabcd6f)
✅ Core tables rebuilt
```

Six lines are trimmed above: the extractor's output-directory and input-file lines and the `Import complete:` heading, each of which prints the file's absolute path; the two-line `FutureWarning` the SQLMesh dependency emits; and the rename hint under the created account. The shell's `time` line is trimmed as well; it read `10.044 total`.

Read that as a cost per batch, not per row: the refresh runs once at the end of the batch however many rows it carried, so chaining twelve monthly files into one command costs it once rather than twelve times. Pass `--no-refresh` to defer the SQLMesh apply when chaining many imports, and finish with one `moneybin transform apply`.

The text path prints the refresh duration as `Transforms completed in Ns` on every run. In `--output json`, `data.transforms_duration_seconds` is populated only for a multi-file batch; a single-file invocation reports `null` there. Per-batch row counts are in `moneybin import history`.

## By file format

If you're working from raw bank or institution exports rather than another personal-finance tool, organize by file type.

**Any format can stop to confirm an account.** Before a file loads, MoneyBin resolves which account it belongs to. A strong signal — a remembered binding, a full account number, a persistent token — adopts silently. A weak one (`institution` + last-4, or a similar display name) stops the import before a single row lands and asks which account it is:

```bash
moneybin import confirm <file> --accept --account-binding @0=<account_id|new>
```

`@0` is the first account the file declares, `@1` the second; `new` mints a distinct account rather than adopting the candidate. Agents get the same stop, not a pass. A file matching nothing has only one possible answer, so it loads and reports the account it created instead of asking — except a bare Date/Description/Amount CSV, which names no account at all and is asked with a pick-list. Full ladder: [account matching](../reference/account-matching.md).

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

**Re-import safety.** OFX is the one channel where the import log refuses a repeat outright. It hashes the raw file bytes and matches on content first, path second, and a second run of the same file never opens a batch:

```console
$ moneybin import files savings.ofx
Using profile: main
Importing OFX file: savings.ofx
Import failed for one file: ValueError
❌ savings.ofx [?] — 0 rows
   File already imported (import_id 5f5a8552...). Use --force to re-import.
```

That exits 1. Pass `--force` to import anyway, which creates a new batch and leaves the old one in place. OFX rows also carry their own transaction IDs (FITID), so even a forced re-import contributes no new canonical transactions. If a bank reuses one FITID for two distinct same-day transactions (a real institution bug), MoneyBin disambiguates them so both survive instead of one silently dropping.

**Description cleanup.** OFX `<PAYEE>` and `<MEMO>` fields are HTML-entity-decoded at import; banks that double-escape (Wells Fargo's `AT&amp;amp;T`) are unwound to `AT&T`.

### CSV / TSV / Excel / Parquet / Feather

One pipeline handles all five. Same command, file-type-driven dispatch.

```bash
moneybin import files ~/Downloads/chase_activity.csv --account-name "Chase Checking"
moneybin import files ~/Downloads/report.xlsx --sheet "Transactions"
moneybin import files ~/Downloads/export.parquet --account-name "Main Account"
```

**No column-mapping file to write.** The importer detects format (encoding, delimiter, file type, preamble rows), finds the header row, matches headers to canonical fields via an alias table of 100 entries across 21 destination fields (`src/moneybin/extractors/tabular/field_aliases.py`), and validates each guess against actual data (a column mapped as `date` is checked for date-parseable values). Full design: [smart-import-tabular spec](../specs/smart-import-tabular.md).

**Every new layout confirms once.** The first file of a header shape MoneyBin hasn't saved before returns `confirmation_required` with the detected mapping and sample values — a three-tier **confidence score** (high/medium/low) changes what the proposal shows, never whether it asks. Here is a real first contact with a file whose headers are `Posting Dt,Txn Detail,Debit Amt,Credit Amt,Running Bal`. The `jq` filter drops the `samples` object, which echoes cell values from every mapped column and is why `sensitivity` is `critical`; nothing else is edited:

```console
$ moneybin import files checking.csv --account-name "Everyday Checking" --output json | jq 'del(.data.samples)'
Using profile: main
Importing CSV file: checking.csv
{
  "status": "ok",
  "summary": {
    "total_count": 1,
    "returned_count": 1,
    "has_more": false,
    "sensitivity": "critical",
    "display_currency": null
  },
  "data": {
    "status": "confirmation_required",
    "channel": "tabular",
    "tier": "medium",
    "score": 0.85,
    "reason": "unknown_layout",
    "error_message": "",
    "proposed_mapping": {
      "debit_amount": "Debit Amt",
      "credit_amount": "Credit Amt",
      "transaction_date": "Posting Dt",
      "amount": "Running Bal",
      "description": "Txn Detail"
    },
    "flagged": [
      "transaction_date",
      "amount",
      "description"
    ],
    "missing_required": [],
    "unmapped_columns": [],
    "bridge_payload": null,
    "sign_convention": null,
    "sign_prior_convention": null,
    "sign_evidence": [],
    "sign_sample_rows": [],
    "account_proposals": [],
    "header_position_ambiguous_rows": []
  },
  "actions": [
    "Re-run with --confirm to accept the proposed mapping as-is.",
    "Re-run with --mapping <field>=<column> to override specific fields.",
    "Run 'moneybin import confirm checking.csv --accept' as a subcommand.",
    "Run 'moneybin import preview checking.csv' to inspect the proposal."
  ]
}
```

That proposal is wrong in a way worth reading closely: it mapped `amount` to `Running Bal`, the running balance. Accepting it as-is would have loaded balances as transaction amounts. Correcting it is the `--mapping` path, and an explicit override resolves on first contact:

```console
$ moneybin import confirm checking.csv --accept --account-name "Everyday Checking" \
    --mapping debit_amount="Debit Amt" --mapping credit_amount="Credit Amt" \
    --sign split_debit_credit
Using profile: main
Importing CSV file: checking.csv
Created import batch: 0d2e0960...
Transform complete: 7 accepted, 0 rejected
Loaded 7 transactions
Loaded 1 accounts
Import 0d2e0960... finalized: complete (7 imported, 0 rejected)
Auto-saved format 'everyday-checking' for future imports
Import complete: Imported CSV file: checking.csv
  Accounts: 1
  Transactions: 7
  Date range: 2026-01-04 to 2026-01-28
✅ Imported checking.csv: 7 rows (import_id: 0d2e0960-5434-40ec-93f3-3ad445c9b44c)
👀 Created account: Everyday Checking (b1c8ab3f8776)
💡 Run 'moneybin transform apply' to rebuild derived tables.
```

One line is trimmed: the rename hint under the created account. Note the last line — `import confirm` loads the rows but does not run the refresh that `import files` runs, so derived tables need a `moneybin transform apply` (or a `moneybin refresh`) afterward.

`--sign split_debit_credit` is required here rather than optional: the proposal resolves a single `amount` column, and asking for the split convention without also mapping both halves is refused rather than guessed —

```console
❌ Sign convention 'split_debit_credit' does not fit this file's columns: the mapping resolves a single amount column, which this convention does not read. Re-run with --sign negative_is_expense or --sign negative_is_income, or map both debit_amount and credit_amount; nothing was imported.
```

Either resolution path saves the mapping as a user format — named `everyday-checking` above, after the account — so every later file with the same header signature loads without a prompt. `-y` / `--yes` is unrelated: it auto-accepts the top fuzzy *account name* match, not a column mapping.

The loaded amounts carry the sign convention, not the file's raw columns:

```console
$ moneybin sql query "SELECT transaction_date, description, amount FROM raw.tabular_transactions ORDER BY transaction_date"
Using profile: main
transaction_date | description | amount
2026-01-04 | COFFEE STAND | -4.75
2026-01-06 | GROCERY MARKET | -82.40
2026-01-10 | PAYROLL DEPOSIT | 2400.00
2026-01-14 | ELECTRIC UTILITY | -118.60
2026-01-19 | BOOKSHOP | -26.10
2026-01-23 | PHARMACY | -41.05
2026-01-28 | RENT PAYMENT | -1450.00
```

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

On first contact with a tabular format, an inference of `negative_is_income` blocks the import instead of silently flipping every amount — MoneyBin asks a person to confirm this really is a credit-card-shaped ledger. Re-run with `--confirm-sign` to ratify, or `--sign negative_is_expense` to override; either way the saved format remembers the choice, so later imports of the same layout replay without asking again. Separately, if the running balance in the file doesn't reconcile with the detected signs, MoneyBin prints a `⚠️` warning after import rather than blocking — re-run with `--sign` if amounts look wrong.

**Number formats.** Specify with `--number-format` when needed: `us` (`1,234.56`), `european` (`1.234,56`), `swiss_french` (`1'234.56`), `zero_decimal` (`123456` cents).

**Preview before committing.** `moneybin import preview` runs detection and column-mapping without writing to the database — use it to inspect a new layout's proposed mapping before deciding whether to `--confirm` or `--override` it:

```bash
moneybin import preview ~/Downloads/transactions.csv
moneybin import preview ~/Downloads/report.xlsx --sheet Sheet2
```

**Per-file overrides are single-file mode only.** `--account-name`, `--format`, `--override` / `--mapping`, `--sign`, `--date-format`, `--number-format`, `--sheet`, `--delimiter`, `--encoding`, `--institution` and `--account-id` are all read only when exactly one path is supplied; passing several paths with any of these set prints this warning first:

```console
⚠️  Per-file flags only apply in single-file mode and will be ignored. Use one file per command for per-file overrides.
```

`--confirm`, `--confirm-sign`, and `--account-binding` are not covered by that warning: each answers a specific file's confirmation gate, so multiple files make the answer ambiguous, and the batch path rejects the combination outright with a `BadParameter` usage error (exit code 2) instead of forwarding or dropping it. Re-run per file, or import without these flags to surface `confirmation_required` envelopes and ratify them with `moneybin import confirm <file>`.

Every option, with its type and default: [`moneybin import files` reference](../reference/cli/import.md#moneybin-import-files).

### PDF (native-text)

Bank-statement PDFs with selectable text. Drop them into `moneybin import files` or the watched inbox.

```bash
moneybin import files ~/Downloads/chase_statement.pdf
moneybin import files ~/Downloads/*.pdf
```

**No layout recipe to write.** On first contact MoneyBin reads the PDF locally with `pdfplumber`, derives a recipe (column positions, header names, date format, sign convention, number format, and the start/end anchors that bound the transaction table), validates the extracted rows by reconciling their sum against the statement's reported balance delta (±1¢ tolerance), and persists the recipe to `app.pdf_formats` keyed by a fingerprint of the layout (issuer + ordered column headers + page bucket). The next statement from the same institution skips derivation entirely — the saved recipe replays in milliseconds.

A statement whose own disclosures (minimum payment, credit limit, APR) name it a credit card proposes inverting every amount — never applied silently, and confirmed once per saved format rather than once per statement. `moneybin import files <path>.pdf --confirm` ratifies it — note that `import confirm` accepts `--confirm` only alongside `--bridge-response`, so the `import files` form is the one to use here; `--sign negative_is_expense` overrules a false positive. The MCP equivalent, `import_confirm`, elicits the human directly instead of requiring a scripted retry.

**What happens to your data:**

- **Transaction-shaped PDFs** (statements with a date / description / amount table) land in `raw.tabular_transactions` (`source_type='pdf'`) and flow through the SQLMesh pipeline to `core.fct_transactions` like any other source. Categorization, search, reports — all work the same. *Caveat for inbox-routed PDFs:* `moneybin import inbox` does not yet trigger the SQLMesh refresh for `file_type='pdf'`, so raw rows land but core/reports won't see them until a `moneybin transform apply` runs. Inbox-routed OFX and tabular files refresh automatically; the inbox refresh gate will be extended to PDFs in a follow-up.
- **Non-transaction PDFs**, and transaction PDFs that don't reconcile cleanly, fall back to the seed path: the extracted tables land as queryable JSON in `raw.pdf_seeds` with an auto-generated typed view (`raw.pdf_<alias>`). The rows do not flow to `core.fct_transactions`. Read the view with `moneybin db query`, `db shell`, `moneybin sql query`, or the `sql_query` MCP tool — the last two reach `raw` and `prep` as well as `core`/`app`/`reports`, and mask a `raw.pdf_<alias>` view by value shape rather than by column class: an SSN-shaped value comes back `***-**-****`, an unbroken run of 8 or more digits keeps only its last four (`12345678` → `****...5678`), and a 4-to-7 digit account number, or one written `1234-5678`, passes through — so does one carrying a decimal point, which the view types `DECIMAL` and the scan skips. That scan is the only masking these views get. See [`sql_query` rules](sql-access.md#sql_query-rules-mcp-tool-and-moneybin-sql-query-cli).

**When the fallback triggers** (any one of):

- The statement's reported balance delta and the extracted-transaction sum disagree by more than 1¢ (often a missed footer total row, a column-header misclassification, or a statement that splits transactions across multiple tables MoneyBin's derivation didn't merge).
- The first-pass extraction confidence on column types is low (typically scanned-then-OCR'd PDFs with brittle column boundaries).
- The PDF has no balance-summary metadata to reconcile against.
- The transaction table extracts zero rows.
- The PDF's number format is anything other than `us`. The executor today only routes `us`-format statements to `raw.tabular_transactions`; `european`, `swiss_french`, and `zero_decimal` are recognized at the recipe level but always fall back to the seed path until executor support lands.

In every fallback case the recipe is NOT saved — MoneyBin only persists recipes that round-trip cleanly. Re-imports of the same statement either replay the saved recipe (no derivation cost) or fall back again to the seed path.

**Preview before committing.** `moneybin import preview <path>.pdf` runs the same deterministic-recipe rung without importing — no `raw.*` rows, no `app.import_log` entry — and reports whether the statement would extract cleanly, how many rows, and any pending sign-convention confirmation, the same way `import preview` does for tabular files. One exception to "no writes": a bridge-eligible layout escalates during the preview and writes its `smart_import_parse` audit row, so the preview opens the database writable. See [Privacy posture](#pdf-native-text) below.

**Privacy posture.** The deterministic recipe ladder runs entirely on your machine — no network egress, no model call — and handles the column shapes statements typically use. A layout it can't crack escalates to the LLM agent you're already driving MoneyBin with, on MCP clients that support it, rather than silently falling back to the seed path.

**When an agent drives that escalation, the statement leaves your machine.** The bridge payload carries the document's text and its sample table rows verbatim — there is no redacted preview — and it reaches your MCP client, and from there whichever model provider that client uses, in the same tool result that asks you to ratify the hand-off. Ratifying governs whether the extracted rows get imported, not whether the content was sent. MoneyBin writes an `app.audit_log` row (`action: smart_import_parse`) for every hand-off; replay them with `moneybin system audit list`. A CLI-driven import never attempts the escalation at all: the hand-off requires a driving agent, so a layout the deterministic ladder can't crack falls through to the seed path (`raw.pdf_seeds`) on your machine instead, and nothing is sent. Scanned or image-only PDFs are outside what either rung can read — see [What's not supported yet](#whats-not-supported-yet).

**Listing saved PDF formats:**

```bash
moneybin import formats list --type=pdf
moneybin import formats show chase_a1b2c3d4e5f6   # works across tabular and PDF formats
```

PDF format names are `{issuer_slug}_{12-char SHA-256 hex of the layout fingerprint}` — the exact name appears in `formats list`. Recipe version is a separate column, not part of the name. The list view shows name, institution, routing (`transactions` / `seed`) and last-used date; `--wide` adds front-end, recipe version and times-used, which are provenance for a format that misbehaves rather than part of the answer. Every saved format reads `pdfplumber` under front-end — it is the only extractor wired up today.

**Re-import safety.** Each transaction's `transaction_id` is a content hash over the statement period, transaction date, raw amount, description, and account — row position is deliberately excluded so a recipe tweak that shifts row order doesn't renumber every following `transaction_id`. Re-running the same PDF from the same path produces zero net new transaction rows: the `(transaction_id, account_id, source_file)` primary key on `raw.tabular_transactions` rejects the duplicates. Each call does still open a fresh `app.import_log` entry, and re-importing the same content from a *different* path will write a new set of raw rows (because `source_file` is part of the dedup key). `--force` does not currently apply to PDFs — it is an OFX-only flag.

**Self-healing recipes.** If a saved recipe stops reconciling (for example, after an extraction fix ships), MoneyBin re-derives it from the document on the next import and keeps the repair only if the fresh recipe clears the same ±1¢ gate a first-contact recipe must clear — persisted as a new recipe version, audited and reversible. Two cases still ask a person: a recipe you or the agent bridge authored is never auto-rewritten this way, and a repair that would change the sign convention (income ↔ expense) needs the same confirmation as a first-contact card detection. Otherwise the statement seeds again, same as first contact.

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

**Coverage today:** cash, credit-card, and investment accounts flow through the canonical pipeline — Plaid Investments sync feeds securities, investment transactions, and dated holdings snapshots into the same ledger `moneybin investments` reads. Loan, mortgage, and HSA accounts load if Plaid exposes them, but MoneyBin doesn't yet capture their subtype-specific fields (APR, escrow, appraisal data) — see the [roadmap](../roadmap.md).

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
moneybin gsheet list                                             # list connected sheets
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

The drain is one batch with one closing refresh, so a folder of twelve monthly OFX downloads costs the refresh pipeline once rather than twelve times.

## Re-importing and dedup

Two layers prevent duplicates:

1. **The import log — OFX only.** Every import records a SHA-256 of the source file's raw bytes, but only the OFX channel refuses a repeat on it: a second `import files` of the same OFX opens no batch and loads nothing until `--force` / `-F` is passed. A tabular or PDF re-import does open a new batch and does re-read the file; nothing stops it at the log.
2. **Per-row content hashes.** Inside the SQLMesh pipeline, cross-source dedup matches rows by content hash (date + amount + description + account) across CSV, OFX, and Plaid. Two imports of the same transaction collapse to one canonical row in `core.fct_transactions`; the bridge tables retain provenance for both sources.

So layer 2 is what makes a repeat harmless, not layer 1. Re-importing the same CSV twice leaves `core.fct_transactions` unchanged and `raw.tabular_transactions` unchanged — the raw table's `(transaction_id, account_id, source_file)` primary key rejects the duplicates — but it does leave a second row in `moneybin import history`, which is how you tell the two runs apart. Importing the same transaction from two different sources is likewise a no-op on the canonical row; the second source contributes its provenance without double-counting.

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
- Re-categorize: `moneybin transactions categorize commit --input one-row.json`, where the file holds a one-element JSON array of `{transaction_id, category, subcategory}`

For anything beyond those (rewriting the amount or date on a single row), the current path is revert the batch, fix the source file, and re-import.

## Manual transaction entry

For cash, gifts, reimbursements, and anything else that doesn't come from a file or sync.

```bash
moneybin transactions create --account chk_001 --date 2026-05-17 \
  -- -42.50 "Coffee with Alex"
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
moneybin import labels add abc123 tax-2025          # attach free-text labels to a batch
moneybin import labels list --import-id abc123     # labels on one batch
moneybin import labels list                         # every label in use, with counts
moneybin import labels remove abc123 tax-2025       # detach a label
```

`import history` is the batch log, newest first. After the two imports above — the CSV that went through `import confirm`, then the OFX:

```console
$ moneybin import history
Using profile: main
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┓
┃ import                               ┃ status   ┃ imported ┃ rejected ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━┩
│ 5f5a8552-2e85-4a94-8221-46107a376e5e │ complete │ 5        │ 0        │
│ 0d2e0960-5434-40ec-93f3-3ad445c9b44c │ complete │ 7        │ 0        │
└──────────────────────────────────────┴──────────┴──────────┴──────────┘
4 of 5 columns shown — --wide for all
```

The `imported` count is rows written to `raw.*`, not transactions: the OFX batch reports 5 for 3 transactions plus 1 account plus 1 balance. `--wide` adds the source file.

`import formats list` shows what a later import will match against. The three `builtin` rows ship with MoneyBin; `user` rows are mappings your own confirmations saved:

```console
$ moneybin import formats list --type=tabular
Using profile: main

Tabular formats (4)
┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ name              ┃ institution ┃ sign convention    ┃ date format ┃ source  ┃
┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━┩
│ everyday-checking │ unknown     │ split_debit_credit │ %Y-%m-%d    │ user    │
│ mint              │ Mint        │ negative_is_expens │ %m/%d/%Y    │ builtin │
│                   │             │ e                  │             │         │
│ tiller            │ Tiller      │ negative_is_expens │ %m/%d/%Y    │ builtin │
│                   │             │ e                  │             │         │
│ ynab              │ YNAB        │ split_debit_credit │ %m/%d/%Y    │ builtin │
└───────────────────┴─────────────┴────────────────────┴─────────────┴─────────┘
```

`--type=pdf` lists saved PDF recipes instead, and `--type=all` (the default) lists both.

Pair any read command with `--output json` for machine-readable output — the same envelope shape the MCP server uses.
On MCP, list formats with `import_status(sections=["formats"])` and remove a
user-saved format with
`import_revert(operation="delete_saved_format", format_name="...")`. The
destructive tool rejects built-ins and records the deletion in the audit log.
Set a batch's full label state with `import_labels_set(import_id=..., labels=[...])`.

## For scripts and agents

`moneybin import` is designed to drive from cron, CI, or an agent loop. The contract:

**Non-interactive flags.** Every flag and its type: [`moneybin import files` reference](../reference/cli/import.md#moneybin-import-files) and [`moneybin import confirm`](../reference/cli/import.md#moneybin-import-confirm). What `--help` does not tell you:

- The four confirmation flags answer four different questions and none substitutes for another. `--yes` / `-y` auto-accepts the top fuzzy *account name* match — nothing else. `--confirm` accepts a proposed column mapping (tabular) or ratifies a proposed sign inversion (PDF). `--confirm-sign` ratifies an inferred sign inversion for a tabular file. `--account-binding REF=<account_id|new>` answers an account-identity gate, repeatable, where `REF` is the referent the gate printed (`@0` for the file's first account).
- A brand-new layout needs `--confirm` or an explicit `--mapping` exactly once, regardless of detector confidence. A saved layout replays without either.
- On retry, re-supply every `--account-binding` you gave before. No partial answer persists between calls.
- `--force` / `-F` only reaches the OFX channel; see [Re-importing and dedup](#re-importing-and-dedup).
- `--output json` puts the [standard response envelope](cli-reference.md#output-envelopes) on stdout and nothing else; the `Using profile:` / `Importing CSV file:` status lines go to stderr, so `2>/dev/null` leaves stdout parseable as one JSON document.

`moneybin import confirm <path>` is the recovery command for a `confirmation_required` response — pass `--accept`, `--mapping <field>=<column>` (repeatable), `--confirm-sign`, or `--account-binding` depending on what's pending; see its `--help` for the full set. Mapping and sign proposals are tabular; an **account** confirmation reaches it from any of the three channels — tabular, OFX, or PDF. Carry `--institution` back when the original `import files` call needed one: institution resolution runs before the account gate, so an OFX whose issuer is underivable never reaches the gate on a re-run without it. `import confirm` also accepts the same eight file-reading options as `import files` — `--format`, `--date-format`, `--number-format`, `--sheet`, `--delimiter`, `--encoding`, `--no-row-limit`, `--no-size-limit` — and so does `import preview`, so all three read a file the same way. The last two carry the most weight on the files that need them: a file above the row or size threshold reaches a confirmation only because the caller lifted the limit, so a retry that drops the flag is refused before it reaches the confirmation it answers. MoneyBin's own printed recovery commands always carry whichever of these the original call used, which is what makes a copy-pasted retry — or a copy-pasted preview — agree with the import it came from. A **PDF** sign-ratification proposal takes a different path: `import confirm` accepts `--confirm` only alongside `--bridge-response`, so re-run `moneybin import files <path>.pdf --confirm` to ratify one. The MCP equivalent is `import_confirm`, which elicits the human directly instead of requiring a second scripted call.

**Exit codes for `moneybin import files`.**

- `0` — at least one file imported and (when refresh is enabled) the post-load refresh succeeded.
- `1` — **every** file in the batch failed, or the refresh pipeline failed.
- `2` — usage error (missing arg, bad flag).

A partial batch exits `0`. Per-file failures do not abort the batch, and they do not reach the exit code either: one imported file plus one failed file is exit `0` with `failed_count: 1` in the envelope. A single-file invocation is the special case where "every file failed" and "one file failed" are the same condition, which is why a lone bad file does exit `1`. Scripts must read `data.failed_count` or each file's `status`, not the exit code.

A `confirmation_required` result does not, by itself, flip a batch's exit code to `1` — check each file's `status` field, not just the exit code, to catch one waiting on confirmation. Single-file invocations differ: `--output json` (or any non-TTY caller) exits `0` on `confirmation_required` so the envelope parses cleanly; the interactive text path exits `1`.

The same contract applies to `moneybin import inbox`: the command exits 0 when the drain completes, even if individual files moved to `failed/`. Detect per-file failure via the `--output json` envelope or by checking the `failed/` directory — do not rely on exit code alone for the inbox.

**`--output json` envelope shape.** A real partial batch — one OFX imported, one unreadable file. The `jq` filter drops the `data.stages` array; nothing else is edited:

```console
$ moneybin import files savings.ofx broken.txt --force --output json 2>/dev/null | jq 'del(.data.stages)'
{
  "status": "ok",
  "summary": {
    "total_count": 1,
    "returned_count": 1,
    "has_more": false,
    "sensitivity": "medium",
    "display_currency": null
  },
  "data": {
    "imported_count": 1,
    "failed_count": 1,
    "total_count": 2,
    "transforms_applied": true,
    "transforms_duration_seconds": 6.078466834034771,
    "transfers_retired": 0,
    "files": [
      {
        "path": "savings.ofx",
        "status": "imported",
        "source_type": "ofx",
        "rows_loaded": 3,
        "import_id": "4082db7d-066f-4882-8d85-8e129951b55c",
        "sign_correction_suggested": false,
        "sign_override_replayed": false
      },
      {
        "path": "broken.txt",
        "status": "failed",
        "source_type": null,
        "rows_loaded": 0,
        "import_id": null,
        "sign_correction_suggested": false,
        "sign_override_replayed": false,
        "error": "No data rows found in broken.txt",
        "error_code": "infra_invalid_input"
      }
    ],
    "identity_errors": [],
    "rate_pairs_failed": [],
    "rate_pairs_unsupported": [],
    "rate_pairs_discarded": []
  },
  "actions": []
}
```

`data.stages`, dropped by the filter, is six `{step, ran, counts, error}` objects, one per refresh step (`gsheet`, `match`, `transform`, `categorize`, `identity`, `rates`). Note `"status": "ok"` on a batch that lost a file — top-level `status` flips to `error` only when every file fails, the same condition the exit code follows. `summary.total_count` counts envelope payloads, not files; `data.total_count` counts files.

`transforms_error` is set on the envelope when refresh failed; non-zero exit follows. Each file entry carries `sign_correction_suggested` and `sign_override_replayed` (see [Sign conventions](#csv--tsv--excel--parquet--feather)), and a `confirmation_payload` object when `status` is `"confirmation_required"`. Full schema: [cli-reference.md](cli-reference.md#output-envelopes).

**Concurrency.** The inbox lockfile serializes inbox drains within a profile. There is no equivalent lock around bare `moneybin import files` — two parallel invocations against the same profile race on the import log. The supported pattern is: serialize at the caller (one cron job, one agent worker), or drop files in the inbox and let the inbox lock handle ordering.

**SIGTERM mid-import.** Not yet a guaranteed clean rollback. If a file is mid-load when the process dies, the import-log row may stay in `importing` state. What happens on rerun depends on the format: an OFX rerun is refused while that row is stuck, and needs `--force`; a tabular or PDF rerun starts a fresh batch rather than short-circuiting, so it can double-load the file. A clean partial-batch rollback contract is planned — for now, treat SIGTERM as "may need a manual `import revert` on the partial batch."

## What's not supported yet

The honest gap list. See the [roadmap](../roadmap.md) for current sequencing.

- **Direct Beancount / hledger ingest.** No plain-text-accounting parsers; export to OFX or CSV instead.
- **Automated migration from Monarch or Copilot.** No API pull; CSV-only.
- **Broker / investment statement file import.** No CSV/PDF path loads trades directly into the investment ledger — bring positions in via Plaid Investments sync (`sync pull`) or manual entry (`moneybin investments add`). Holdings, cost basis (FIFO/HIFO/specific-ID/average), and realized gains are tracked once positions land; tie-out against a real broker 1099-B for a full tax year is still open.
- **Display conversion of the aggregate reports.** Every transaction and balance captures its own `currency_code` at import (from OFX `<CURDEF>` or Plaid). Five reports price their rows into one display currency — the three net-worth reports (`net-worth`, `net-worth-currencies`, `net-worth-accounts`), `large-transactions`, `balance-drift` — because each row carries one amount and one date to price it on; `--display-currency` selects it, and the profile's `home_currency` is the default. Four aggregate per currency, so a row holds no single amount to convert and they sub-total each currency separately. Rates come from the last `moneybin refresh`: a report read never fetches one, and any row that cannot be priced falls the whole report back to per-currency sub-totals rather than converting part of it. The `realized_fx` report deliberately keeps `disposed_amount` in `currency_code` and `proceeds`, `cost_basis`, `fee_amount`, and `gain_loss` in `home_currency`; display conversion does not re-price those audited amounts. Realized FX still awaits a deliberate EUR/USD statement tie-out.
- **Scanned / image-only PDF.** PDFs without a selectable text layer (scanned pages, fax-quality images) are not supported — MoneyBin returns an explicit error naming the gap rather than importing zero rows silently. Use a document scanner with OCR to produce a native-text PDF first; reading the page image directly (vision-capable extraction) is not yet built.
- **General-purpose row-level updates.** No `transactions update` command; use notes, tags, splits, categorize subcommands or revert and re-import.
- **`--watch` mode for the inbox.** Cron or `launchd`/`systemd` is the supported pattern today.
- **Bulk manual transaction entry.** One row at a time via `moneybin transactions create`; for batches, build a CSV and import it.
- **Named format profiles for Maybe / Sure, Lunch Money, Monarch, and Copilot.** Three built-ins ship — `tiller`, `mint`, `ynab`. Every other tool's export goes through the generic tabular path and confirms its mapping once.
- **A duplicate-file guard outside OFX.** Re-running the same tabular or PDF file opens a new batch and re-reads it. Row-level dedup keeps the data correct, so the cost is a second `import history` row, not a double-count. Revert the extra batch with `moneybin import revert <import_id>` if the log matters to you.
- **A refresh after `moneybin import confirm`.** `import files` runs the post-load refresh; `import confirm` loads the rows and prints `💡 Run 'moneybin transform apply' to rebuild derived tables` instead. Run that, or a `moneybin refresh`, before reading any `core.*` or `reports.*` table.
- **A non-zero exit on a partial batch.** One failed file among several still exits `0`; read `data.failed_count`. See [Exit codes](#for-scripts-and-agents).
