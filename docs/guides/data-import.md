<!-- Last reviewed: 2026-09-02 -->
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
moneybin db backup                       # encrypted snapshot under data/<profile>/backups/
moneybin db restore --from <backup-path> # roll back if an import goes wrong
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

If you've since moved to a different tool and have that tool's export, see the relevant subsection below or fall back to the [generic CSV path](#csv--tsv--excel--parquet--feather).

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
| **Maybe / Sure** | ✅ | ✅ (note) | ❌ (tags column dropped) | ❌ | 🟡 detected post-load | ✅ |
| **Generic CSV** (Lunch Money, Monarch, Copilot) | ✅ if a category column is detected | ✅ if a memo/notes column is detected | ❌ | ❌ | 🟡 detected post-load | ✅ if column present, else use `--account-name` |
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

Order-of-magnitude, not benchmarks:

- A single 10 MB CSV: a few seconds end-to-end (extract + load + refresh).
- 5 years of monthly Tiller exports: tens of seconds total.
- A large multi-year institutional CSV dump (100k+ rows): can stretch to a minute or two with the refresh pipeline included.
- Pass `--no-refresh` to defer the SQLMesh apply when chaining many imports; finish with one `moneybin transform apply`.

Actual timing for any specific batch appears in `moneybin import history` and in the structured log under the profile data directory.

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

**Re-import safety.** OFX files carry their own transaction IDs (FITID), so re-importing the same statement is a no-op. The import log also tracks file-content hashes — re-running the exact same file is short-circuited. Pass `--force` to re-import anyway (creates a new batch). If a bank reuses the same FITID for two distinct same-day transactions (a real institution bug), MoneyBin disambiguates them so both survive instead of one silently dropping.

**Description cleanup.** OFX `<PAYEE>` and `<MEMO>` fields are HTML-entity-decoded at import; banks that double-escape (Wells Fargo's `AT&amp;amp;T`) are unwound to `AT&T`.

### CSV / TSV / Excel / Parquet / Feather

One pipeline handles all five. Same command, file-type-driven dispatch.

```bash
moneybin import files ~/Downloads/chase_activity.csv --account-name "Chase Checking"
moneybin import files ~/Downloads/report.xlsx --sheet "Transactions"
moneybin import files ~/Downloads/export.parquet --account-name "Main Account"
```

**What the smart importer saves you from:** writing a column-mapping file by hand. It detects format (encoding, delimiter, file type, preamble rows), finds the header row, matches headers to canonical fields via a 100+ entry alias dictionary, and validates each guess against actual data (a column mapped as `date` is checked for date-parseable values). Full design: [smart-import-tabular spec](../specs/smart-import-tabular.md).

**Every new layout confirms once.** The first file of a header shape MoneyBin hasn't saved before returns `confirmation_required` with the detected mapping and sample values — a three-tier **confidence score** (high/medium/low) changes what the proposal shows, never whether it asks. Re-run with `--confirm` to accept the mapping as shown, or `--override <field>=<column>` (repeatable) to correct a field — an explicit override resolves immediately, even on first contact. Either path saves the mapping as a user format, so every later file with the same header signature loads without a prompt. `-y` / `--yes` is unrelated: it auto-accepts the top fuzzy *account name* match, not a column mapping.

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

**Most common per-file overrides** (single-file mode only — passing multiple paths disables them):

| Flag | Purpose |
|------|---------|
| `-n, --account-name` | Account name when the file is single-account and the column doesn't carry one. |
| `-f, --format` | Force a named format (`tiller`, `mint`, etc.). |
| `--override` | Field-to-column override, repeatable (`--override date=Posted --override amount=Amt`). |
| `--confirm` | Accept the proposed mapping as-is after a `confirmation_required` response. |
| `--confirm-sign` | Ratify an inferred credit-card-shaped sign inversion (see [Sign conventions](#csv--tsv--excel--parquet--feather) above). |
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

**Preview before committing.** `moneybin import preview <path>.pdf` runs the same deterministic-recipe rung without importing — no `raw.*` rows, no `raw.import_log` entry — and reports whether the statement would extract cleanly, how many rows, and any pending sign-convention confirmation, the same way `import preview` does for tabular files. One exception to "no writes": a bridge-eligible layout escalates during the preview and writes its `smart_import_parse` audit row, so the preview opens the database writable. See [Privacy posture](#pdf-native-text) below.

**Privacy posture.** The deterministic recipe ladder runs entirely on your machine — no network egress, no model call — and handles the column shapes statements typically use. A layout it can't crack escalates to the LLM agent you're already driving MoneyBin with, on MCP clients that support it, rather than silently falling back to the seed path.

**When an agent drives that escalation, the statement leaves your machine.** The bridge payload carries the document's text and its sample table rows verbatim — there is no redacted preview — and it reaches your MCP client, and from there whichever model provider that client uses, in the same tool result that asks you to ratify the hand-off. Ratifying governs whether the extracted rows get imported, not whether the content was sent. MoneyBin writes an `app.audit_log` row (`action: smart_import_parse`) for every hand-off; replay them with `moneybin system audit list`. A CLI-driven import never attempts the escalation at all: the hand-off requires a driving agent, so a layout the deterministic ladder can't crack falls through to the seed path (`raw.pdf_seeds`) on your machine instead, and nothing is sent. Scanned or image-only PDFs are outside what either rung can read — see [What's not supported yet](#whats-not-supported-yet).

**Listing saved PDF formats:**

```bash
moneybin import formats list --type=pdf
moneybin import formats show chase_a1b2c3d4e5f6   # works across tabular and PDF formats
```

PDF format names are `{issuer_slug}_{12-char SHA-256 hex of the layout fingerprint}` — the exact name appears in `formats list`. Recipe version is a separate column, not part of the name. The list view shows name, institution, routing (`transactions` / `seed`) and last-used date; `--wide` adds front-end, recipe version and times-used, which are provenance for a format that misbehaves rather than part of the answer. Every saved format reads `pdfplumber` under front-end — it is the only extractor wired up today.

**Re-import safety.** Each transaction's `transaction_id` is a content hash over the statement period, transaction date, raw amount, description, and account — row position is deliberately excluded so a recipe tweak that shifts row order doesn't renumber every following `transaction_id`. Re-running the same PDF from the same path produces zero net new transaction rows: the `(transaction_id, account_id, source_file)` primary key on `raw.tabular_transactions` rejects the duplicates. Each call does still open a fresh `raw.import_log` entry, and re-importing the same content from a *different* path will write a new set of raw rows (because `source_file` is part of the dedup key). `--force` does not currently apply to PDFs — it is an OFX-only flag.

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

Pair any read command with `--output json` for machine-readable output — the same envelope shape the MCP server uses.
On MCP, list formats with `import_status(sections=["formats"])` and remove a
user-saved format with
`import_revert(operation="delete_saved_format", format_name="...")`. The
destructive tool rejects built-ins and records the deletion in the audit log.
Set a batch's full label state with `import_labels_set(import_id=..., labels=[...])`.

## For scripts and agents

`moneybin import` is designed to drive from cron, CI, or an agent loop. The contract:

**Non-interactive flags.**

- `--yes` / `-y` — auto-accept the top fuzzy account match without prompting. Unrelated to column-mapping or sign confirmation.
- `--confirm` — accept the proposed mapping (tabular) or ratify a proposed sign inversion (PDF) after a `confirmation_required` response. A brand-new layout always needs this once, regardless of detector confidence; a saved layout replays without it.
- `--confirm-sign` — ratify an inferred sign inversion for a tabular file specifically.
- `--account-binding REF=<account_id|new>` — answer an account confirmation without a prompt (repeatable). `REF` is the positional referent the confirmation printed: `@0` for the first account the file declares.
- `--output json` — emits the [standard response envelope](cli-reference.md#output-envelopes) on stdout.
- `--no-refresh` — defer the post-load SQLMesh apply. Useful when chaining many imports.
- `--force` / `-F` — re-import a file already in the log.

`moneybin import confirm <path>` is the recovery command for a `confirmation_required` response — pass `--accept`, `--mapping <field>=<column>` (repeatable), `--confirm-sign`, or `--account-binding` depending on what's pending; see its `--help` for the full set. Mapping and sign proposals are tabular; an **account** confirmation reaches it from any of the three channels — tabular, OFX, or PDF. Carry `--institution` back when the original `import files` call needed one: institution resolution runs before the account gate, so an OFX whose issuer is underivable never reaches the gate on a re-run without it. A **PDF** sign-ratification proposal takes a different path: `import confirm` accepts `--confirm` only alongside `--bridge-response`, so re-run `moneybin import files <path>.pdf --confirm` to ratify one. The MCP equivalent is `import_confirm`, which elicits the human directly instead of requiring a second scripted call.

**Exit codes for `moneybin import files`.**

- `0` — every file imported and (when refresh is enabled) the post-load refresh succeeded.
- `1` — at least one file failed, or the refresh pipeline failed. Per-file failures do **not** abort the batch (the rest still import); the non-zero exit signals "look at the envelope."
- `2` — usage error (missing arg, bad flag).

A `confirmation_required` result does not, by itself, flip a batch's exit code to `1` — check each file's `status` field, not just the exit code, to catch one waiting on confirmation. Single-file invocations differ: `--output json` (or any non-TTY caller) exits `0` on `confirmation_required` so the envelope parses cleanly; the interactive text path exits `1`.

The same contract applies to `moneybin import inbox`: the command exits 0 when the drain completes, even if individual files moved to `failed/`. Detect per-file failure via the `--output json` envelope or by checking the `failed/` directory — do not rely on exit code alone for the inbox.

**`--output json` envelope shape** (mutating-command envelope; see [cli-reference.md](cli-reference.md#output-envelopes) for the full schema):

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

`transforms_error` is set on the envelope when refresh failed; non-zero exit follows. Each file entry also carries `sign_correction_suggested` and `sign_override_replayed` booleans (see [Sign conventions](#csv--tsv--excel--parquet--feather)), and a `confirmation_payload` object when `status` is `"confirmation_required"`.

**Concurrency.** The inbox lockfile serializes inbox drains within a profile. There is no equivalent lock around bare `moneybin import files` — two parallel invocations against the same profile race on the import log. The supported pattern is: serialize at the caller (one cron job, one agent worker), or drop files in the inbox and let the inbox lock handle ordering.

**SIGTERM mid-import.** Not yet a guaranteed clean rollback. If a file is mid-load when the process dies, the import-log row may stay in `importing` state. What happens on rerun depends on the format: an OFX rerun is refused while that row is stuck, and needs `--force`; a tabular or PDF rerun starts a fresh batch rather than short-circuiting, so it can double-load the file. A clean partial-batch rollback contract is planned — for now, treat SIGTERM as "may need a manual `import revert` on the partial batch."

## What's not supported yet

The honest gap list. See the [roadmap](../roadmap.md) for current sequencing.

- **Direct Beancount / hledger ingest.** No plain-text-accounting parsers; export to OFX or CSV instead.
- **Automated migration from Monarch or Copilot.** No API pull; CSV-only.
- **Broker / investment statement file import.** No CSV/PDF path loads trades directly into the investment ledger — bring positions in via Plaid Investments sync (`sync pull`) or manual entry (`moneybin investments add`). Holdings, cost basis (FIFO/HIFO/specific-ID/average), and realized gains are tracked once positions land; tie-out against a real broker 1099-B for a full tax year is still open.
- **Display conversion of the aggregate reports.** Every transaction and balance captures its own `currency_code` at import (from OFX `<CURDEF>` or Plaid). Three reports price their rows into one display currency — `networth`, `large-transactions`, `balance-drift` — because each row carries one amount and one date to price it on; `--display-currency` selects it, and the profile's `home_currency` is the default. The other five aggregate per currency, so a row holds no single amount to convert and they sub-total each currency separately. Rates come from the last `moneybin refresh`: a report read never fetches one, and any row that cannot be priced falls the whole report back to per-currency sub-totals rather than converting part of it. FX gain/loss is not computed.
- **Scanned / image-only PDF.** PDFs without a selectable text layer (scanned pages, fax-quality images) are not supported — MoneyBin returns an explicit error naming the gap rather than importing zero rows silently. Use a document scanner with OCR to produce a native-text PDF first; reading the page image directly (vision-capable extraction) is not yet built.
- **General-purpose row-level updates.** No `transactions update` command; use notes, tags, splits, categorize subcommands or revert and re-import.
- **`--watch` mode for the inbox.** Cron or `launchd`/`systemd` is the supported pattern today.
- **Bulk manual transaction entry.** One row at a time via `moneybin transactions create`; for batches, build a CSV and import it.
