<!-- Last reviewed: 2026-09-02 -->
# Data Sources

Every supported data source — what file formats and API integrations MoneyBin can ingest, what fields each preserves, where the data lands. For the how-to (running the import), see [`docs/guides/data-import.md`](../guides/data-import.md). For the resulting schema, see [`docs/reference/data-model.md`](data-model.md). For how these sources resolve to one canonical account — which identity signal each format provides — see [`docs/reference/account-matching.md`](account-matching.md).

Reference for engineers wiring automation against the import path and for migrants evaluating what MoneyBin can eat. Every claim below is verified against current source (loaders in `src/moneybin/loaders/`, extractors in `src/moneybin/extractors/`, format profiles in `src/moneybin/data/tabular_formats/`, raw schema in `src/moneybin/sql/schema/`).

## Source-type identifiers

`source_type` is the canonical provenance discriminator on every row in `core.fct_transactions` and on every `raw.*` transaction, account, and balance table. Use it for filtering, dedup scope, and audit.

**The seed-path tables are the exception.** `raw.pdf_seeds` and `raw.gsheet_seeds` (and the internal `raw.import_preview_snapshots`) carry no `source_type` column — a seed row's provenance is its `alias` or `connection_id`, not a family tag, because the seed path accepts shapes that have no source family. Filtering those tables by `source_type = '...'` fails with a binder error rather than returning zero rows, so branch on the table, not on a value.

| Source | `source_type` value | Raw landing tables |
|--------|---------------------|--------------------|
| Tabular files (CSV/TSV/Excel/Parquet/Feather) | `csv`, `tsv`, `excel`, `parquet`, `feather` | `raw.tabular_transactions`, `raw.tabular_accounts` |
| OFX / QFX / QBO | `ofx` | `raw.ofx_transactions`, `raw.ofx_accounts`, `raw.ofx_balances`, `raw.ofx_institutions` |
| PDF statements | `pdf` | `raw.tabular_transactions` (transaction-shaped documents); `raw.pdf_seeds` + generated `raw.pdf_<alias>` views (everything else — no `source_type` column; see the note above) |
| Plaid sync | `plaid` | `raw.plaid_transactions`, `raw.plaid_accounts`, `raw.plaid_balances`; investments: `raw.plaid_securities`, `raw.plaid_investment_transactions`, `raw.plaid_investment_holdings`, `raw.plaid_investment_holding_lots`, `raw.plaid_investment_holdings_snapshots` |
| Manual entry | `manual` | `raw.manual_transactions`; investments: `raw.manual_investment_transactions` |

Tabular `source_type` is one of five file-format values — there is no single "tabular" family tag in core. Filter with `source_type IN ('csv','tsv','excel','parquet','feather')` when you want every tabular row regardless of file type. PDF transaction-shaped rows share `raw.tabular_transactions` with the five file formats above, tagged `source_type='pdf'` — include it explicitly when a query means "every row that went through the tabular pipeline." Every batch — regardless of source — registers an `import_id` in `raw.import_log` and stamps it on every row it produced. That id is the unit of `moneybin import revert`.

`source_origin` is a finer-grained tag scoped beneath `source_type`: institution slug for OFX (`wells_fargo`, `chase`), Plaid item id for Plaid, format name for tabular (`tiller`, `chase_credit`), and the literal `'user'` for manual entries.

## Sign conventions across sources

The canonical rule in `core.fct_transactions` is **negative = expense, positive = income**. Each source arrives with its own convention; the transform that lands it in `core` enforces the canonical sign. Do not flip anywhere else.

| Source | Source-side convention | Where the canonical sign is enforced |
|---|---|---|
| `tiller`, `mint`, `maybe`, `chase_credit` | `negative_is_expense` (already canonical) | No flip; pass-through in `prep.stg_tabular__transactions` |
| `ynab`, `citi_credit` | `split_debit_credit` (separate Outflow/Debit + Inflow/Credit columns) | Merged into one signed `amount` in tabular transforms (`src/moneybin/extractors/tabular/transforms.py`) |
| OFX | `negative_is_expense` (OFX native) | No flip |
| PDF | Derived per document — `negative_is_expense` (bank statements) or `negative_is_income` (credit-card charges post positive, payments negative) | Detected during recipe derivation; a `negative_is_income` recipe requires human ratification before it writes (see "PDF statements" below) — never applied silently |
| Plaid | `positive_is_expense` (Plaid native) | Flipped in `prep.stg_plaid__transactions` |
| Manual | `negative_is_expense` (enforced at write) | No flip |

## Account-type normalization

`core.dim_accounts.account_type` normalizes every source's native spelling to one five-value vocabulary — `depository`, `credit`, `loan`, `investment`, `other` — through `seeds.account_type_map` (`src/moneybin/sqlmesh/models/seeds/account_type_map.csv`, 19 rows). Raw tables keep the source's own spelling (OFX `CHECKING`/`CREDITLINE`, Plaid `depository`/`credit`, the PDF importer's `credit`); the three `stg_*__accounts` staging views normalize through the shared map before `core` sees a value. An unrecognized spelling resolves to `NULL`, never a guessed type, so a later, stronger source can still supply the real value on merge. Finer detail survives in `account_subtype` (`checking`, `savings`, `money market`, `cd`, `cash management`, `line of credit`, `credit card`, `mortgage`, `student`, `brokerage`, …) — Plaid's own subtype wins over the registry's when both are present.

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

**Multi-currency files.** The `currency` column is parsed when present (e.g., the `maybe` profile reads it into `raw.tabular_transactions.currency`) and carried through to `core.fct_transactions.currency_code` — every source (tabular, OFX `<CURDEF>`, Plaid, manual) captures its own currency end-to-end rather than defaulting the unknown case to USD; a row without one inherits `core.dim_accounts.currency_code`. Reports sub-total each currency separately rather than adding the raw numbers, and `moneybin system doctor` reports a profile holding more than one. Conversion is a display step on top of that, not a change to the stored data: set `moneybin profile set home_currency <ISO 4217>`, or pass `--display-currency` per call, and the three reports whose rows each carry one amount and one date to price it on — net worth, balance drift, large transactions — return a single converted figure. Every other report stays segmented per currency. No converted amount is written anywhere; `raw.*` and `core.*` keep the currency the source stated.

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
- **Notes:** Date `%Y-%m-%d`, sign `negative_is_expense`. Multi-account. Also covers Sure (inherited schema). `currency` carries through to `core.fct_transactions.currency_code` with no conversion — see "Multi-currency files" above.

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

One extractor, three formats — Open Financial Exchange and its Quicken (QFX) and QuickBooks (QBO) variants. The OFX extractor reads both SGML and XML payloads and tolerates single-line headers. Implementation: `src/moneybin/extractors/ofx/extractor.py`.

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

**Account-level fields parsed:** `account_id`, `routing_number`, `account_type` (raw OFX spelling — e.g. `CHECKING`, `SAVINGS`, `CREDITCARD`; normalized in `core.dim_accounts` per "Account-type normalization" above), `currency_code` (from `<CURDEF>`). Landed in `raw.ofx_accounts`. **Balance fields:** statement `start_date` / `end_date`, `ledger_balance`, `available_balance`, `balance_date`, `currency_code` → `raw.ofx_balances`. **Institution fields:** `<FI><ORG>`, `<FI><FID>` → `raw.ofx_institutions`.

**Institution resolution.** Two independent resolutions run off the same `<FI>` block (`src/moneybin/extractors/institution_resolution.py`). The **slug** (`source_origin`, an input to the transaction-id content hash) resolves through a chain: `<FI><ORG>` snake-cased → `<FI><FID>` lookup in `seeds.institutions` (5 rows: Chase, Citi, Bank of America, Wells Fargo, U.S. Bank) → filename regex (`wells_fargo`, `chase`, `bank_of_america`, `citi`, `us_bank`, `capital_one`, `discover`, `amex`) → `--institution` override → interactive prompt → `InstitutionResolutionError`. Because `<ORG>` wins whenever a bank publishes one, most banks' slugs never reach the registry step — Chase resolves to `b1`, not `chase`, since Chase's `<ORG>` is `B1`. Separately, `core.dim_accounts.institution_name` (the **display** name) joins `seeds.institutions` on `<FI><FID>` directly, independent of which step produced the slug — this is what turns Chase's opaque `<ORG>` code `B1` into "Chase" for display without touching `source_origin` or transaction-id identity.

**Description cleanup.** `<NAME>` and `<MEMO>` are HTML-entity-decoded at extraction; banks that double-escape are unwound via a bounded triple-pass `html.unescape` loop.

## PDF statements

Generic PDF ingestion, not a per-institution parser — one extractor drives every PDF through a three-rung ladder, reusing the tabular pipeline once a document becomes rows. Implementation: `src/moneybin/extractors/pdf/`.

**Extraction ladder** (`routing.py`, `auto_derive.py`, `bridge.py`):

1. **Deterministic replay.** A layout-fingerprint match against `app.pdf_formats` replays its saved recipe — no LLM involvement. `front_end` records which extractor produced the recipe. Every row written today reads `pdfplumber` — it is the only front end wired up; no camelot or vision path writes this field yet.
2. **Deterministic auto-derive.** No fingerprint match — `derive_recipe` reconstructs the transaction table from pdfplumber's ruled tables, falling back to whitespace-column reconstruction from raw text lines when the statement draws no ruling lines. A confidence threshold gates whether the derived recipe saves and runs, or escalates.
3. **Bridge escalation.** A transaction-shaped document (`transaction_table_underivable`) the deterministic rung can't crack is surfaced to the driving agent, which proposes a recipe; a `smart_import_parse` audit-log row records the hand-off every time. A scanned PDF with no text layer returns `import_pdf_no_text_layer` instead of attempting extraction — no vision/OCR backend runs today.

**Routing — two outcomes**, gated by a ±1¢ reconciliation check (statement balance math, row count) before either one commits:

| Outcome | Where it lands | Notes |
|---|---|---|
| Transaction-shaped, reconciles | `raw.tabular_transactions`, `source_type='pdf'` | Same categorization / reports pipeline as every other tabular source. The account resolver uses an opaque document-content native key under an issuer-independent origin. An identifier proven complete is retained only as a validated-routing-scoped encrypted `full_number` ref and may auto-adopt. Issuer, last four, labelled name/product, and incoming-ledger overlap support candidate review; currency constrains that overlap, while balances remain reconciliation evidence. Partial or unscoped statements never become strong keys. Existing links made under the legacy issuer-plus-last-four derivation are shown alongside current candidates, not silently adopted. |
| Not transaction-shaped (`no_transaction_table`), or transaction-shaped with a non-US number locale (`unsupported_number_format`) | `raw.pdf_seeds` (JSON) + auto-generated `raw.pdf_<alias>` typed view | Catch-all. The alias is derived from the file stem (`ImportService._pdf_alias`) — there is no `--alias` flag on `import files`; rename the file to change the view name. Does not participate in matching, categorization, or `reports.*`. Read the view with `moneybin db query` / `db shell`, `moneybin sql query`, or the `sql_query` MCP tool — the last two reach `raw` and `prep` as well as `core`/`app`/`reports`, and mask a `raw.pdf_<alias>` view by value shape rather than by column class. That scan is the only masking these views get; see [`sql_query` rules](../guides/sql-access.md#sql_query-rules-mcp-tool-and-moneybin-sql-query-cli). |

**Sign convention.** A bank statement is `negative_is_expense`, like OFX. A credit-card statement is natively `negative_is_income` (charges post positive, payments negative) — the inverse of every other source. MoneyBin requires explicit human ratification before a `negative_is_income` recipe writes anything: MCP elicitation when the client supports it, otherwise `moneybin import files <file> --confirm` for a deterministically derived recipe. (`moneybin import confirm <file> --bridge-response <response>.json --confirm` is the separate agent-bridge path — it ratifies a recipe the driving agent proposed and needs that response file, which a deterministic derivation never produces.) The confirmation carries the evidence (a printed-vs-recorded sample) and is never applied silently — a wrong inversion corrupts every row, and a saved recipe would replay the error forever. `--sign` is a durable override but only ratifies; an agent actor can never set it (rejected at the bridge boundary).

**Reversibility.** Every PDF import gets an `import_id` in `raw.import_log`, identical to tabular and OFX — `moneybin import revert` undoes it. The two PDF outcomes identify rows differently. A transaction-shaped row hashes its **content** — statement period, date, raw amount or debit/credit, description, and account — and deliberately excludes page and row position, so re-running a recipe that reorders rows does not change transaction IDs; two genuinely identical rows survive because a collision appends an occurrence index. A seed row is position-aware instead, hashing `alias|<doc_key>|p<page>r<row_index>|content`, which preserves duplicate cells within a page. Either way, re-importing the same statement is a no-op.

**Out of scope today.** Brokerage positions/holdings PDFs route to seed — no core investments table reads `raw.pdf_seeds`. W-2, 1099, and other tax-form PDFs are not extracted; no tax-form parser exists. Scanned/image-only PDFs (no text layer) are declined outright.

## Plaid sync

Live banking sync brokered through `moneybin-sync`. Implementation: `src/moneybin/extractors/plaid/extractor.py`, `src/moneybin/services/sync_service.py`, `src/moneybin/connectors/sync_client.py`. The client never talks to Plaid directly — it talks to the moneybin-sync API, which holds the Plaid integration as an implementation detail.

**What's pulled per sync:**

- **Accounts** (`raw.plaid_accounts`): `account_id`, `persistent_account_id`, `account_type`, `account_subtype`, `institution_name`, `name`, `official_name`, `mask` (last-4). `persistent_account_id` is the id that survives a relink where `account_id` does not; Plaid populates it only for depository accounts at institutions using tokenized account numbers (Chase, PNC, US Bank), so it is NULL elsewhere — and on every row synced before MoneyBin captured it.
- **Transactions** (`raw.plaid_transactions`): `transaction_id`, `account_id`, `transaction_date`, `amount`, `description`, `merchant_name`, `category`, `pending`, `iso_currency_code`. Transactions carry the ISO field only — `unofficial_currency_code` is captured for balances, securities, investment transactions, and holdings, but not here.
- **Balances** (`raw.plaid_balances`): `account_id`, `balance_date`, `current_balance`, `available_balance`, `iso_currency_code` / `unofficial_currency_code`.
- **Removed transactions:** Plaid's incremental sync emits a separate `removed_transactions` list; corresponding rows are deleted from `raw.plaid_transactions` and surfaced as `transactions_removed` in the `PullResult`.

**Sign convention.** `raw.plaid_transactions.amount` preserves Plaid's native convention (positive = expense). The sign flip happens exactly once, in `prep.stg_plaid__transactions`, so downstream `core.*` rows match the canonical MoneyBin convention (negative = expense).

**Account-type coverage today:**

| Plaid account type | Status in core pipeline |
|---|---|
| Cash (`depository`: checking, savings) | First-class — flows into `core.fct_transactions` and `core.dim_accounts` |
| Credit cards (`credit`) | First-class |
| Investments (`investment`, `brokerage`) | First-class — see "Investments" below |
| Loans / mortgages (`loan`) | Rows land if exposed; no first-class treatment |
| HSA (`depository.hsa`) | Rows land if exposed; no first-class treatment |

**Incremental sync.** Plaid uses cursor-based incremental sync — each `sync pull` resumes from the last cursor stored server-side. `--force` resets the cursor and re-fetches full history; cross-source dedup collapses the overlap downstream.

### Investments

A Plaid investment/brokerage account pulls five entities beyond the depository/credit set above, all captured through the same `sync pull`:

- **Securities** (`raw.plaid_securities`): `security_id`, `ticker_symbol`, `market_identifier_code`, `security_name`, `security_type`, `close_price`, `close_price_as_of`, `iso_currency_code` / `unofficial_currency_code`. Upserted in place, keyed `(security_id, source_origin)` — one row per security **per connected institution**, each overwritten on that connection's next pull. A security you hold at two connected brokerages is two raw rows; `SecurityResolver` merges them when resolving `core.dim_securities`, so count securities there, not here.
- **Security prices** (`raw.security_prices`, append-only): the security-level close captured alongside every securities upsert, keyed `(source_type, source_origin, provider_security_key, price_date, quote_currency)` — the currency is part of the key, so the same security and date can carry one close per quote currency. `core.fct_security_prices` resolves one close per security/date/currency; `core.dim_holdings` uses it to price a position (`market_value`, `unrealized_gain`, and `valuation_status` — `valued` / `carried_forward` / `unpriced` / `withheld` / `source_overlap`, null rather than zero when a price can't be trusted).
- **Investment transactions** (`raw.plaid_investment_transactions`): `investment_transaction_id`, `account_id`, `security_id`, `transaction_date`, `quantity`, `amount`, `price`, `fees`, `iso_currency_code` / `unofficial_currency_code`, plus Plaid's own `investment_transaction_type` / `investment_transaction_subtype`. `prep.stg_plaid__investment_transactions` maps Plaid's ~6 types × ~48 subtypes onto MoneyBin's closed 14-value ledger vocabulary (`buy`, `sell`, `dividend`, `fee`, `split`, `transfer_in`, …) and flips the amount sign (Plaid: positive = cash out) — the only place that flip happens. A row the taxonomy can't map lands with `ledger_include=FALSE` and a `review_reason` instead of guessing.
- **Holdings** (`raw.plaid_investment_holdings`): `account_id`, `security_id`, `holdings_date`, `institution_price`, `institution_price_as_of`, `institution_value`, `cost_basis`, `quantity`, `iso_currency_code` / `unofficial_currency_code`, `vested_quantity`, `vested_value`. Keyed `(account_id, security_id, source_origin, source_file)` — one row per position per connection **per snapshot**, where `source_file` (`sync_{job_id}`) is the snapshot identity. Every pull retains its own full snapshot rather than overwriting the previous one, so aggregating this table across pulls double-counts positions. Read `core.dim_holdings` for current holdings, or restrict to the newest `source_file` per `source_origin`.
- **Holding lots** (`raw.plaid_investment_holding_lots`): per-lot detail inside a holding (`institution_lot_id`, `original_purchase_datetime`, `quantity`, `purchase_price`, `cost_basis`, `current_value`, `position_type`) when the institution reports lot-level tax data. Keyed `(account_id, security_id, source_origin, lot_index, source_file)` — snapshot-retained on the same terms as holdings above, so the same newest-snapshot restriction applies.
- **Holdings snapshots** (`raw.plaid_investment_holdings_snapshots`): one row per (item, holdings pull) — the receipt that the pull happened and what it returned (`holdings_date`, `holdings_count`), independent of whether any holdings rows were written. `core.dim_holdings` reads the newest snapshot to decide "as of when," not the presence of holdings rows — a fully liquidated brokerage account writes zero holdings but still writes its snapshot, so the newest-snapshot join (not a row scan) is what correctly shows an emptied account holding nothing.

Landing tables: `core.dim_securities`, `core.fct_investment_transactions`, `core.fct_investment_lots`, `core.fct_realized_gains`, `core.dim_holdings`. Securities resolve to canonical identity through `app.security_links` (same accept/decide pattern as account linking); an unresolved security's rows carry `NULL` `security_id` rather than the raw provider id, so an unbound security can't silently masquerade as canonical.

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

`status` flips to `"error"` and an `error` block is added on classified failure. `summary.degraded` + `summary.degraded_reason` appear when the response is less than what was asked for — three cases today: an MCP tool returns aggregates in place of row-level data without consent, a `system status` section could not be read, or a report's stored column classification is stale. `degraded_reason` names which one. `summary.display_currency` is always emitted and is `null` when the rows span more than one currency or none is known — read each row's `currency_code` in that case.

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

**`moneybin sync pull --output json` `data` shape** — the `SyncPullPayload` the `sync_pull` MCP tool also returns (`src/moneybin/privacy/payloads/sync.py`), projected from the internal `PullResult` model:

```json
{
  "job_id": "...",
  "transactions_loaded": 87,
  "accounts_loaded": 4,
  "balances_loaded": 4,
  "transactions_removed": 1,
  "securities_loaded": 12,
  "investment_transactions_loaded": 9,
  "holdings_loaded": 12,
  "holding_lots_loaded": 0,
  "security_prices_loaded": 12,
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

The investments fields default to `0` for a non-investment account. `PullResult` also carries `opening_bootstrap_rows`, `investment_source_overlap_accounts` (accounts with both manual and Plaid investment rows — lots and gains double-count until one source is chosen per account), and `security_resolution` / `security_resolution_error` (the `app.security_links` resolution sweep run after load), omitted above for brevity.

Every `sync` command that emits JSON goes through the same `render_or_json` path as `import files`, so the shape above is the `data` block of the envelope, not a bare payload.

## Idempotency across sources

Re-importing the same content produces no duplicates because every raw table dedupes on a stable per-row key. Re-importing a corrected file (different content) produces new rows under a new `import_id`.

| Source | Dedup key on raw table | Mechanism | Notes |
|---|---|---|---|
| Tabular | `(transaction_id, account_id, source_file)` | When the source carries a transaction ID column (e.g., Tiller's `Transaction ID`), `transaction_id` is `<account_id>:<source_id>`. Otherwise, content hash: `<source_type>_<sha256-of "date\|amount\|description\|account_id">[:16]` (`src/moneybin/extractors/tabular/transforms.py`). Loader uses `on_conflict="upsert"`. | Primary key includes `source_file`, so re-importing the same content under a different filename produces a second raw row. Cross-source dedup in `core` collapses these. |
| OFX | `(source_transaction_id, account_id, source_file)` | Source-provided `<FITID>`. Loader uses `on_conflict="upsert"`. | Same `source_file` caveat as tabular. |
| PDF (transactions) | Same as Tabular — `(transaction_id, account_id, source_file)` | Content hash, `pdf_` prefix (`services/import_service.py::_import_pdf_transactions`), same occurrence-suffix rule as tabular. | Lands in `raw.tabular_transactions` alongside every other tabular source. |
| PDF (seeds) | `(alias, row_hash)` | Position-aware content hash: `pdf_<sha256-of "alias\|<doc_key>\|p<page>r<row_idx>\|json(row)">[:16]` (`extractors/pdf/seed_store.py`). Loader uses `INSERT OR IGNORE`. | Re-imports keep the first import's `import_id`. The page+row-index component is load-bearing — two genuinely identical rows (same-day, same-amount charges) at different physical positions both survive; a pure content hash would collapse them to one. `<doc_key>` is `_document_key(doc)`, a hash of the document's extracted content: an alias is only a filename stem, so it keeps a row identical in both content and position across two statements sharing one alias from colliding. |
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
| PDF | `UserError` (code `import_pdf_no_text_layer`) | Scanned / image-only PDF with no selectable text layer; no vision backend to fall back to. |
| PDF | `typer.BadParameter` (exit 2, `moneybin import confirm`) | `--bridge-response` combined with `--accept`/`--mapping`; or supplied without `--confirm`, since its recipe may invert every amount in the statement. |
| Plaid | `httpx`-shaped errors via `sync_client` | Auth / network / rate-limit failures from moneybin-sync. |
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
| `currency_code` | no | Written only when `--currency` is passed. Omitted, the row inherits `core.dim_accounts.currency_code` in `core.fct_transactions`; no literal default is applied. |

**Resulting raw row** (`raw.manual_transactions`):

```text
source_transaction_id = 'manual_' || <12-hex UUID4>
source_type           = 'manual'
source_origin         = 'user'
import_id             = <new raw.import_log row>
account_id            = <resolved from dim_accounts>
transaction_date, amount, description, merchant_name, memo,
payment_channel, transaction_type, check_number,
currency_code         = <as supplied, else NULL>
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
- **Broker / investment statements outside Plaid.** No eTrade, Schwab, Fidelity, or Vanguard CSV parsers, and no investments-aware PDF routing — a brokerage positions/holdings PDF lands in `raw.pdf_seeds`, not a core investments table. A Plaid-linked brokerage account is first-class: securities, investment transactions, holdings, and cost basis (FIFO, HIFO, specific-identification, average-cost) are implemented — see "Investments" under Plaid sync above.
- **HSA / 401(k) transaction history outside Plaid.** If Plaid exposes the account, raw rows land; otherwise unsupported.
- **FX conversion inside `core.*`.** Conversion is presentation-only: three reports price into a display currency, and every stored amount keeps the currency its source stated — see "Multi-currency files" above. So `core.fct_balances_daily` still drops a transaction denominated in anything but the balance's own currency rather than converting it, and that movement resurfaces as `reconciliation_delta` drift instead. `moneybin refresh` caches rates into the home currency only; another target falls back to per-currency segmentation until its own rates are stored.
- **Scanned / image-only PDFs.** A PDF with no text layer is declined outright (`import_pdf_no_text_layer`); no vision/OCR backend runs. Text-layer bank and credit-card statements extract deterministically or through agent-assisted recipe derivation — see "PDF statements" above.
- **Tax forms.** No W-2, 1040, 1099-INT/DIV/B, K-1, or state-form parsers — including from a text-layer PDF, which lands in `raw.pdf_seeds` rather than a tax-shaped table.
- **Direct Monarch / Copilot API pulls.** CSV-only — export from the tool, import the file.
- **Programmatic format-profile registration.** New profiles require a YAML file in the repo or interactive acceptance during smart-import; no external registration API.
- **Bulk manual entry.** `moneybin transactions create` is one row per call. For batches, build a CSV.
