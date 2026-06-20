<!-- Last reviewed: 2026-05-17 -->
# Data Sources

Every supported data source — what file formats and API integrations MoneyBin can ingest, what fields each preserves, where the data lands. For the how-to (running the import), see [`docs/guides/data-import.md`](../guides/data-import.md). For the resulting schema, see [`docs/reference/data-model.md`](data-model.md). For how these sources resolve to one canonical account — which identity signal each format provides — see [`docs/reference/account-matching.md`](account-matching.md).

Reference for engineers wiring automation against the import path and for migrants evaluating what MoneyBin can eat. Every claim below is verified against current source (loaders in `src/moneybin/loaders/`, extractors in `src/moneybin/extractors/`, format profiles in `src/moneybin/data/tabular_formats/`, raw schema in `src/moneybin/sql/schema/`).

## Source-type identifiers

`source_type` is the canonical provenance discriminator on every row in `core.fct_transactions` and on every `raw.*` table. Use it for filtering, dedup scope, and audit.

| Source | `source_type` value | Raw landing tables |
|--------|---------------------|--------------------|
| Tabular files (CSV/TSV/Excel/Parquet/Feather) | `csv`, `tsv`, `excel`, `parquet`, `feather` | `raw.tabular_transactions`, `raw.tabular_accounts` |
| OFX / QFX / QBO | `ofx` | `raw.ofx_transactions`, `raw.ofx_accounts`, `raw.ofx_balances`, `raw.ofx_institutions` |
| Plaid sync | `plaid` | `raw.plaid_transactions`, `raw.plaid_accounts`, `raw.plaid_balances` |
| Manual entry | `manual` | `raw.manual_transactions` |

Tabular `source_type` is one of five file-format values — there is no single "tabular" family tag in core. Filter with `source_type IN ('csv','tsv','excel','parquet','feather')` when you want every tabular row regardless of file type. Every batch — regardless of source — registers an `import_id` in `raw.import_log` and stamps it on every row it produced. That id is the unit of `moneybin import revert`.

`source_origin` is a finer-grained tag scoped beneath `source_type`: institution slug for OFX (`wells_fargo`, `chase`), Plaid item id for Plaid, format name for tabular (`tiller`, `chase_credit`), and the literal `'user'` for manual entries.

## Sign conventions across sources

The canonical rule in `core.fct_transactions` is **negative = expense, positive = income**. Each source arrives with its own convention; the transform that lands it in `core` enforces the canonical sign. Do not flip anywhere else.

| Source | Source-side convention | Where the canonical sign is enforced |
|---|---|---|
| `tiller`, `mint`, `maybe`, `chase_credit` | `negative_is_expense` (already canonical) | No flip; pass-through in `prep.stg_tabular__transactions` |
| `ynab`, `citi_credit` | `split_debit_credit` (separate Outflow/Debit + Inflow/Credit columns) | Merged into one signed `amount` in tabular transforms (`src/moneybin/extractors/tabular/transforms.py`) |
| OFX | `negative_is_expense` (OFX native) | No flip |
| Plaid | `positive_is_expense` (Plaid native) | Flipped in `prep.stg_plaid__transactions` |
| Manual | `negative_is_expense` (enforced at write) | No flip |

## Tabular formats

One pipeline, file-type-driven dispatch: CSV, TSV, Excel (`.xlsx`/`.xls`), Parquet, and Feather all land in `raw.tabular_transactions` via the smart importer. `.txt` and `.dat` are accepted as CSV.

### Format detection: order of operations

The order below is what `ImportService._import_tabular_file` executes (`src/moneybin/services/import_service.py`). Each step short-circuits on success.

1. **Explicit `--format <name>` lookup.** If supplied, the name is resolved against the merged format set (built-ins + user-saved); an unknown name raises `ValueError`. Skips header detection entirely.
2. **Stage 1 — file-format detection.** Extension + magic bytes determine file type (CSV / TSV / Excel / Parquet / Feather). Encoding is detected from BOM, then UTF-8 try, then `charset-normalizer` fallback. Delimiter for text formats is detected by per-line counting of `,`, `\t`, `|`, `;` and picking the highest mean / lowest variance (`src/moneybin/extractors/tabular/format_detector.py`).
3. **Stage 3 — header-signature match.** Once the file is read, headers are matched against every format in `merge_formats(builtins, user_formats)`. User-saved formats override built-ins on name collision (`merge_formats` in `src/moneybin/extractors/tabular/formats.py`). **First-match wins** — iteration order is `dict` insertion order, which is built-ins first (sorted alphabetically by filename) followed by user-saved formats. Matching is case-insensitive subset (`matches_headers`): every header in the profile's signature must appear in the file's headers, extra columns are tolerated.
4. **Column-mapper fallback.** No format matched — `map_columns()` (`src/moneybin/extractors/tabular/column_mapper.py`) attempts header-alias matching against a ~100-entry alias dictionary (`field_aliases.py`), then content-based discovery for any unmapped required field (date, amount, description) using sample-row analysis.
5. **Confidence assignment** (`_assign_confidence`):
   - `high` — `transaction_date`, `amount` (or `debit_amount`+`credit_amount`), and `description` all matched via header alias, with a detected date format.
   - `medium` — all three required fields mapped, but at least one came from content-based discovery, or date format is unresolved.
   - `low` — any required field is missing. Raises `ValueError`; the import does not proceed.
6. **Profile persistence.** When a detected mapping is accepted, it's saved as a user format in `app.tabular_formats` keyed by header signature, so subsequent files of the same shape skip to step 3 (first-match success).

**`--yes` semantics — read before scripting.** `--yes` auto-accepts the top fuzzy match when resolving an `--account-name` against existing `dim_accounts` rows. It does **NOT** auto-accept low-confidence first-time column mappings; a low-confidence mapping raises `ValueError` regardless. For unattended automation against an unknown export shape: run one interactive import first to save the format profile, then re-run with `--yes` against subsequent files.

**Programmatic profile authoring is not exposed today.** New built-in profiles require a YAML file in `src/moneybin/data/tabular_formats/`; user formats are populated only via interactive smart-import acceptance and the `moneybin import formats list / show / delete` surface. There is no public API to register a format from outside the repo.

### Locale and encoding

| Knob | Default | Override flag | Behavior |
|---|---|---|---|
| Encoding | Auto: BOM (`utf-8-sig`, `utf-16-le`, `utf-16-be`) → UTF-8 try → `charset-normalizer` best guess → fall back to `utf-8` | `--encoding <name>` | Accepts any Python codec name (`utf-8`, `latin-1`, `cp1252`, `utf-16`, …). |
| Delimiter | Auto: scan first 20 lines, pick highest mean / lowest variance from `,`, `\t`, `|`, `;` | `--delimiter <char>` | Forces the delimiter; `file_type` is re-mapped (`\t` → `tsv`, etc.). |
| Date format | Per-profile `date_format` (strptime pattern); for smart-detected formats, inferred from sample values | `--date-format <strptime>` | Single explicit format for the whole file. |
| Number format | Per-profile `number_format`; smart-detected from amount samples | `--number-format {us\|european\|swiss_french\|zero_decimal}` | `us` = `.` decimal, `,` thousands. `european` = `,` decimal, `.` thousands. `swiss_french` = `.` decimal, `'` thousands. `zero_decimal` = integer-only (no decimal separator). |
| Sign convention | Per-profile or inferred | `--sign {negative_is_expense\|negative_is_income\|split_debit_credit}` | Overrides detection. |
| Excel sheet | Largest sheet | `--sheet <name>` | Explicit sheet by name. |

**Multi-currency files.** The `currency` column is parsed when present (e.g., the `maybe` profile reads it into `raw.tabular_transactions.currency`), but `core.*` is single-currency today — every row is treated as USD downstream regardless of the per-row currency. Mixed-currency files import without warning. FX conversion lands with the multi-currency milestone.

Each named profile below ships in `src/moneybin/data/tabular_formats/<name>.yaml` and matches first-import autodetection without needing `--format`. Field-mapping legend: each profile lists `field_mapping` (source-column → canonical field). Anything appearing in `header_signature` but **not** in `field_mapping` is read off the row but not persisted — those are the "fields dropped" entries.

### `tiller`

- **Required columns:** `Date`, `Description`, `Category`, `Amount`, `Account`, `Account #`, `Institution`, `Transaction ID`.
- **Fields preserved → `raw.tabular_transactions`:**

  | Source column | Canonical field |
  |---|---|
  | `Date` | `transaction_date` |
  | `Description` | `description` |
  | `Full Description` | `memo` |
  | `Category` | `category` |
  | `Amount` | `amount` |
  | `Account` | account name (matched to `dim_accounts`) |
  | `Account #` | account number (masked) |
  | `Institution` | institution name |
  | `Transaction ID` | `source_transaction_id` |
- **Fields dropped:** none — every column in the signature is mapped.
- **Notes:** Date `%m/%d/%Y`, US number format, sign `negative_is_expense`. Multi-account file (`multi_account: true`) — one file carries rows for every account on the sheet.

### `mint`

- **Required columns:** `Date`, `Description`, `Original Description`, `Amount`, `Transaction Type`, `Category`, `Account Name`, `Labels`, `Notes`.
- **Fields preserved → `raw.tabular_transactions`:**

  | Source column | Canonical field |
  |---|---|
  | `Date` | `transaction_date` |
  | `Description` | `description` |
  | `Original Description` | `memo` |
  | `Amount` | `amount` |
  | `Transaction Type` | `transaction_type` |
  | `Category` | `category` |
  | `Account Name` | account name |
- **Fields dropped:** `Labels`, `Notes` — Mint's user-applied labels and the Notes column are read but not persisted on this profile. To preserve them, post-import use `moneybin transactions tags add` / `transactions notes add`.
- **Notes:** Date `%m/%d/%Y`, US number format, sign `negative_is_expense`. Multi-account. Format is preserved by every Mint-successor tool that offered a "bring your history" path.

### `ynab`

- **Required columns:** `Account`, `Flag`, `Date`, `Payee`, `Category Group/Category`, `Category Group`, `Category`, `Memo`, `Outflow`, `Inflow`, `Cleared`.
- **Fields preserved → `raw.tabular_transactions`:**

  | Source column | Canonical field |
  |---|---|
  | `Date` | `transaction_date` |
  | `Payee` | `description` |
  | `Category Group/Category` | `category` |
  | `Outflow` | debit side (merged via `split_debit_credit`) |
  | `Inflow` | credit side |
  | `Memo` | `memo` |
  | `Cleared` | `status` |
- **Fields dropped:** `Flag`, `Category Group`, `Category` (the un-prefixed columns are dropped in favor of the combined `Category Group/Category` field).
- **Notes:** Date `%m/%d/%Y`, sign `split_debit_credit`. YNAB envelope state — budgeted-but-unspent, Age of Money, scheduled transactions — does NOT survive; only posted-transaction history lands. Export path: Budget → Export budget data → unzip → `Register.csv`.

### `maybe`

- **Required columns:** `date`, `name`, `amount`, `currency`, `account`, `category`, `tags`, `note`.
- **Field mapping:** `date` → `transaction_date`, `name` → `description`, `amount` → `amount`, `currency` → `currency`, `account` → account name, `category` → `category`, `note` → `memo`.
- **Fields dropped:** `tags`. Post-import: `moneybin transactions tags add`.
- **Notes:** Date `%Y-%m-%d`, sign `negative_is_expense`. Multi-account. Also covers Sure (inherited schema). `currency` lands but downstream `core.*` is single-currency — see "Locale and encoding" above.

### `chase_credit`

- **Required columns:** `Transaction Date`, `Post Date`, `Description`, `Category`, `Type`, `Amount`, `Memo`.
- **Field mapping:** all seven columns map 1:1 to canonical fields (`Transaction Date` → `transaction_date`, `Post Date` → `post_date`, `Type` → `transaction_type`; remaining columns retain their lower-cased name).
- **Fields dropped:** none.
- **Notes:** Date `%m/%d/%Y`, sign `negative_is_expense`. Single-account file — `--account-name` required.

### `citi_credit`

- **Required columns:** `Status`, `Date`, `Description`, `Debit`, `Credit`, `Member Name`.
- **Field mapping:** `Date` → `transaction_date`, `Description` → `description`, `Debit`+`Credit` → signed `amount`, `Status` → `status`, `Member Name` → `member_name`.
- **Fields dropped:** none.
- **Notes:** Date `%m/%d/%Y`, sign `split_debit_credit`. Single-account file.

### Per-file overrides

Combinable with named profiles (override only the named flag) or with smart-detected files: `--account-name`, `--account-id`, `--format`, `--override field=column` (repeatable), `--sign`, `--number-format`, `--date-format`, `--sheet`, `--encoding`, `--delimiter`.

## OFX / QFX / QBO

One extractor, three formats — Open Financial Exchange and its Quicken (QFX) and QuickBooks (QBO) variants. The OFX extractor reads both SGML and XML payloads and tolerates single-line headers. Implementation: `src/moneybin/extractors/ofx_extractor.py`.

**Fields parsed from each `<STMTTRN>`:**

| OFX element | Canonical field on `raw.ofx_transactions` |
|---|---|
| `<FITID>` | `source_transaction_id` |
| `<TRNTYPE>` | `transaction_type` (e.g. `DEBIT`, `CREDIT`, `CHECK`, `XFER`) |
| `<DTPOSTED>` | `date_posted` |
| `<TRNAMT>` | `amount` (signed; OFX uses negative-is-expense natively) |
| `<NAME>` / `<PAYEE>` | `payee` |
| `<MEMO>` | `memo` |
| `<CHECKNUM>` | `check_number` |

**Account-level fields parsed:** `account_id`, `routing_number`, `account_type` (e.g. `CHECKING`, `SAVINGS`, `CREDITCARD`). Landed in `raw.ofx_accounts`. **Balance fields:** statement `start_date` / `end_date`, `ledger_balance`, `available_balance`, `balance_date` → `raw.ofx_balances`. **Institution fields:** `<FI><ORG>`, `<FI><FID>` → `raw.ofx_institutions`.

**Institution resolution.** The institution slug is resolved through a chain (`src/moneybin/extractors/institution_resolution.py`): `<FI><ORG>` snake-cased → static `<FI><FID>` lookup table covering major US banks (Chase, Citi, Bank of America, Wells Fargo, US Bank) → filename regex (`wells_fargo`, `chase`, `bank_of_america`, `citi`, `us_bank`, `capital_one`, `discover`, `amex`) → `--institution` override → interactive prompt → `InstitutionResolutionError`.

**Description cleanup.** `<NAME>` and `<MEMO>` are HTML-entity-decoded at extraction; banks that double-escape are unwound via a bounded triple-pass `html.unescape` loop.

## Plaid sync

Live banking sync brokered through `moneybin-server`. Implementation: `src/moneybin/loaders/plaid_loader.py`, `src/moneybin/services/sync_service.py`, `src/moneybin/connectors/sync_client.py`. The client never talks to Plaid directly — it talks to the moneybin-server API, which holds the Plaid integration as an implementation detail.

**What's pulled per sync:**

- **Accounts** (`raw.plaid_accounts`): `account_id`, `account_type`, `account_subtype`, `institution_name`, `official_name`, `mask` (last-4).
- **Transactions** (`raw.plaid_transactions`): `transaction_id`, `account_id`, `transaction_date`, `amount`, `description`, `merchant_name`, `category`, `pending`.
- **Balances** (`raw.plaid_balances`): `account_id`, `balance_date`, `current_balance`, `available_balance`.
- **Removed transactions:** Plaid's incremental sync emits a separate `removed_transactions` list; corresponding rows are deleted from `raw.plaid_transactions` and surfaced as `transactions_removed` in the `PullResult`.

**Sign convention.** `raw.plaid_transactions.amount` preserves Plaid's native convention (positive = expense). The sign flip happens exactly once, in `prep.stg_plaid__transactions`, so downstream `core.*` rows match the canonical MoneyBin convention (negative = expense).

**Account-type coverage today:**

| Plaid account type | Status in core pipeline |
|---|---|
| Cash (`depository`: checking, savings) | First-class — flows into `core.fct_transactions` and `core.dim_accounts` |
| Credit cards (`credit`) | First-class |
| Investments (`investment`, `brokerage`) | Rows land in `raw.plaid_*` if exposed, but holdings / cost-basis / lot-tracking surfaces are not implemented |
| Loans / mortgages (`loan`) | Rows land if exposed; no first-class treatment |
| HSA (`depository.hsa`) | Rows land if exposed; no first-class treatment |

**Incremental sync.** Plaid uses cursor-based incremental sync — each `sync pull` resumes from the last cursor stored server-side. `--force` resets the cursor and re-fetches full history; cross-source dedup collapses the overlap downstream.

## JSON output (`--output json`)

Every read-only and write-shaped CLI command supports `--output json` and emits the cross-transport response envelope (`src/moneybin/protocol/envelope.py`):

```json
{
  "status": "ok",
  "summary": {
    "total_count": 1,
    "returned_count": 1,
    "has_more": false,
    "sensitivity": "low",
    "display_currency": "USD"
  },
  "data": { ... },
  "actions": []
}
```

`status` flips to `"error"` and an `error` block is added on classified failure. `summary.degraded` + `summary.degraded_reason` appear when an MCP tool returns aggregates in place of row-level data without consent.

**`moneybin import files --output json` `data` shape** (`src/moneybin/cli/commands/import_cmd.py`):

```json
{
  "imported_count": 142,
  "failed_count": 0,
  "total_count": 142,
  "transforms_applied": true,
  "transforms_duration_seconds": 3.7,
  "files": [
    {
      "path": "/abs/path/statement.ofx",
      "status": "imported",
      "source_type": "ofx",
      "rows_loaded": 142,
      "import_id": "8f3a...",
      "error": null
    }
  ]
}
```

On extractor failure (single-file path), the same envelope shape is emitted with `failed_count: 1` and `files[0].error` set to the exception class name (e.g. `"ValueError"`, `"PermissionError"`).

**`moneybin sync pull --output json` `data` shape** — the Pydantic `PullResult` model serialized via `model_dump_json` (`src/moneybin/connectors/sync_models.py`):

```json
{
  "job_id": "...",
  "transactions_loaded": 87,
  "accounts_loaded": 4,
  "balances_loaded": 4,
  "transactions_removed": 1,
  "institutions": [
    {
      "provider_item_id": "...",
      "institution_name": "Chase",
      "status": "completed",
      "transaction_count": 87,
      "error": null,
      "error_code": null
    }
  ],
  "transforms_applied": true,
  "transforms_duration_seconds": 2.1,
  "transforms_error": null
}
```

Note: `sync pull` emits the `PullResult` directly (no envelope wrapper) — the import command emits the envelope. This is a known asymmetry; sync commands will adopt the envelope alongside the rest of the surface.

## Idempotency across sources

Re-importing the same content produces no duplicates because every raw table dedupes on a stable per-row key. Re-importing a corrected file (different content) produces new rows under a new `import_id`.

| Source | Dedup key on raw table | Mechanism | Notes |
|---|---|---|---|
| Tabular | `(transaction_id, account_id, source_file)` | When the source carries a transaction ID column (e.g., Tiller's `Transaction ID`), `transaction_id` is `<account_id>:<source_id>`. Otherwise, content hash: `<source_type>_<sha256-of "date\|amount\|description\|account_id">[:16]` (`src/moneybin/extractors/tabular/transforms.py`). Loader uses `on_conflict="upsert"`. | Primary key includes `source_file`, so re-importing the same content under a different filename produces a second raw row. Cross-source dedup in `core` collapses these. |
| OFX | `(source_transaction_id, account_id, source_file)` | Source-provided `<FITID>`. Loader uses `on_conflict="upsert"`. | Same `source_file` caveat as tabular. |
| Plaid | `transaction_id` | Source-provided. Loader upserts in place; Plaid's `removed_transactions` list triggers deletes. | Cursor-driven — incremental by default; `--force` resets and re-fetches. |
| Manual | `source_transaction_id` (`manual_<uuid4>[:12]`) | New ID per `transactions create` call. | A second create call with identical fields creates a new row — there is no content-hash collapse for manual entries. |

There is no file-content SHA-256 short-circuit before extraction — re-running an unchanged file re-parses and upserts; row counts in `raw.import_log` reflect the upsert, not new inserts.

## Failure modes

Per-source error surfaces. CLI exits 1 with the exception class name visible in `--output json` at `data.files[].error`.

| Source | Exception | Trigger |
|---|---|---|
| Tabular | `ValueError` | Smart-import confidence `low` (date / amount / description not all mapped); unknown `--format` name; zero data rows; single-account profile with no `--account-name` / `--account-id`; unsupported extension; size-limit trip (use `--no-size-limit`). |
| OFX | `ValueError` | Malformed OFX payload or read error (wraps the underlying parser exception). |
| OFX | `InstitutionResolutionError` | Institution chain exhausted with no match and no `--institution` override (non-interactive only). |
| Plaid | `httpx`-shaped errors via `sync_client` | Auth / network / rate-limit failures from moneybin-server. |
| All | `DatabaseKeyError`, `DatabaseLockError`, `DatabaseNotInitializedError` | Database lifecycle; surfaced with `db unlock` guidance. |

## Manual entry

`moneybin transactions create` for cash, gifts, reimbursements, anything that doesn't come from a file or sync. Backed by `src/moneybin/services/transaction_service.py` and lands in `raw.manual_transactions` (`src/moneybin/sql/schema/raw_manual_transactions.sql`).

**Accepted fields:**

| Field | Required | Notes |
|---|---|---|
| `transaction_date` | yes | ISO date or any parseable form. |
| `amount` | yes | Signed `Decimal`; negative = expense. Non-zero. |
| `description` | yes | Free text; non-empty. |
| `account_name` (or `account_id`) | yes | Must resolve to an existing `core.dim_accounts` row. |
| `merchant_name` | no | Resolved against `core.dim_merchants` on the next pipeline pass. |
| `memo` | no | Free text. |
| `category`, `subcategory` | no | If supplied, written to `app.transaction_categories` (NOT to the raw row — categories live on the app layer for every source). |
| `payment_channel` | no | `in_store` / `online` / `other`. |
| `transaction_type` | no | Free-text type code. |
| `check_number` | no | Free text. |
| `currency_code` | no | Defaults to `USD`. |

**Resulting raw row** (`raw.manual_transactions`):

```text
source_transaction_id = 'manual_' || <12-hex UUID4>
source_type           = 'manual'
source_origin         = 'user'
import_id             = <new raw.import_log row>
account_id            = <resolved from dim_accounts>
transaction_date, amount, description, merchant_name, memo,
payment_channel, transaction_type, check_number,
currency_code         = <as supplied>
category, subcategory = NULL  -- categories always live in app.transaction_categories
created_by            = 'cli' | 'mcp'
```

`created_by` is hardcoded per surface: `'cli'` for `moneybin transactions create`, `'mcp'` for the `transactions_create` MCP tool. No other values are written today; the column is `VARCHAR NOT NULL` to leave room for multi-user identity later.

In `core.fct_transactions` the row appears with `source_type = 'manual'` and is treated identically to any imported row — same dedup, same matching pipeline, same MCP / CLI access. One CLI call = one batch = one `raw.import_log` row. Bulk manual entry: build a CSV and run it through `moneybin import files` instead.

## Inbox: watched folder

Drop files into the per-profile inbox; `moneybin import inbox` drains them in one batch. Implementation: `src/moneybin/services/inbox_service.py`.

**Layout** (per profile, under `MoneyBinSettings.import_.inbox_root`, default `~/Documents/MoneyBin`):

```text
~/Documents/MoneyBin/<profile>/
├── inbox/             # drop files here
├── processed/YYYY-MM/ # successes move here, dated by drain month
├── failed/YYYY-MM/    # failures move here with a .error.yml sidecar
└── .inbox.lock        # advisory flock — prevents concurrent drains per profile
```

`moneybin import inbox` (no subcommand) drains pending files via the default callback. `moneybin import inbox list` is a dry-run preview of pending files. `moneybin import inbox path` prints the active inbox directory.

**Failure sidecars.** Each failed file lands in `failed/YYYY-MM/` alongside a `<filename>.error.yml` describing what went wrong; exception text is length-capped to keep unbounded library messages from leaking sensitive content. The drain exits 0 even if individual files moved to `failed/` — parse the `--output json` envelope (same shape as `import files`) or check the `failed/` directory directly to detect per-file failures. `.inbox.lock` is an advisory `flock`; a crashed drain releases it on process exit. There is no built-in `--watch` mode; cron, `launchd`, or `systemd` is the supported scheduling pattern.

## What MoneyBin doesn't ingest today

Honest gap list. See [`docs/roadmap.md`](../roadmap.md) for current sequencing.

- **Beancount / hledger ledger files.** No plain-text-accounting parsers. Workaround: export the source transactions your ledger was built from and import those.
- **Broker / investment statements.** No eTrade, Schwab, Fidelity, or Vanguard CSV parsers. Plaid investment accounts load raw rows if exposed, but holdings, cost basis, and FIFO lot tracking are not implemented.
- **HSA / 401(k) transaction history outside Plaid.** If Plaid exposes the account, raw rows land; otherwise unsupported.
- **Multi-currency.** Every amount is treated as USD downstream. Source `currency` columns are read into `raw.tabular_transactions.currency` but original-currency preservation and FX gain/loss are not implemented.
- **PDFs.** No PDF formats are supported. Bank-statement PDFs, W-2 forms, 1099 forms, receipts, and brokerage statements are not extracted.
- **Tax forms.** No W-2, 1040, 1099-INT/DIV/B, K-1, or state-form parsers.
- **Direct Monarch / Copilot API pulls.** CSV-only — export from the tool, import the file.
- **Programmatic format-profile registration.** New profiles require a YAML file in the repo or interactive acceptance during smart-import; no external registration API.
- **Bulk manual entry.** `moneybin transactions create` is one row per call. For batches, build a CSV.
