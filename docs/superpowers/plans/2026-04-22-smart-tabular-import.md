# Smart Tabular Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing CSV-only import system with a universal tabular importer that handles CSV, TSV, Excel, Parquet, and Feather files through a five-stage pipeline (detect → read → map → transform → load), with heuristic column mapping, confidence tiers, multi-account support, import batch tracking, and 6 built-in institution formats.

**Architecture:** Five-stage pipeline where Stages 1–2 are format-specific (different readers per file type) and Stages 3–5 are format-agnostic (identical logic operating on Polars DataFrames). The format-agnostic boundary is a Polars DataFrame with string column names and raw values. A `TabularFormat` Pydantic model replaces the old `CSVProfile`. Formats are stored in `app.tabular_formats` (DB) with YAML seed files as fallback. Every import gets a UUID `import_id` stamped on all rows, enabling history and reverting. The service layer is shared between CLI and MCP.

**Tech Stack:** Python 3.12, Polars (all readers + transforms), DuckDB (storage via `Database.ingest_dataframe()`), openpyxl (Excel via Polars), charset-normalizer (encoding detection), PyYAML (format seed files), Pydantic v2 (TabularFormat model), Typer (CLI), pytest

**Spec:** `docs/specs/smart-import-tabular.md` (child of `docs/specs/smart-import-overview.md`)

---

## Design Notes

### Relationship to Existing CSV System

This plan replaces the existing `CSVExtractor` + `CSVProfile` + `CSVLoader` system. The old system is format-based (YAML profiles with explicit column mappings) and CSV-only. The new system adds heuristic detection, multi-format support, confidence tiers, and import batch tracking. The migration is a clean replacement — old files are removed, not wrapped.

### Database.ingest_dataframe() Already Exists

The `Database.ingest_dataframe()` method (`src/moneybin/database.py:277`) already supports `insert`, `replace`, and `upsert` modes with Arrow zero-copy. The spec calls for this method, but it's already implemented. We use it as-is.

### Column Rename: source_system → source_type

The existing core models use `source_system` (hardcoded `'ofx'` or `'csv'`). The new system introduces `source_type` as a column in raw tables. The core models are rewritten to use `source_type` instead of `source_system`. This is a rename in the SELECT alias — no data migration needed for raw tables. A database migration handles renaming in core tables for existing databases.

### What This Plan Does NOT Cover

- Database migration from `raw.csv_*` → `raw.tabular_*` (separate migration spec)
- JSON/JSONL import (separate spec per overview)
- Investment transaction routing (Level 2)
- Legacy .xls support (deferred)
- Batch folder import (post-v1)

---

## File Structure

### Files to Create

| File | Responsibility |
|---|---|
| `src/moneybin/extractors/tabular/format_detector.py` | Stage 1: extension, magic bytes, delimiter sniffing, encoding detection |
| `src/moneybin/extractors/tabular/readers.py` | Stage 2: file-type-specific readers producing Polars DataFrames |
| `src/moneybin/extractors/tabular/field_aliases.py` | FIELD_ALIASES constant, header normalization, alias matching |
| `src/moneybin/extractors/tabular/column_mapper.py` | Stage 3: header matching, content validation, format lookup, confidence tiers |
| `src/moneybin/extractors/tabular/sign_convention.py` | Sign convention inference and amount normalization helpers |
| `src/moneybin/extractors/tabular/date_detection.py` | Date format detection, DD/MM disambiguation, number format detection |
| `src/moneybin/extractors/tabular/transforms.py` | Stage 4: date parsing, amount normalization, ID generation, validation |
| `src/moneybin/extractors/tabular/formats.py` | TabularFormat Pydantic model, DB operations, YAML loading |
| `src/moneybin/extractors/tabular/__init__.py` | Package init, re-exports |
| `src/moneybin/loaders/tabular_loader.py` | Stage 5: import batch tracking, raw table writes, format save |
| `src/moneybin/sql/schema/raw_tabular_transactions.sql` | DDL for `raw.tabular_transactions` |
| `src/moneybin/sql/schema/raw_tabular_accounts.sql` | DDL for `raw.tabular_accounts` |
| `src/moneybin/sql/schema/raw_import_log.sql` | DDL for `raw.import_log` |
| `src/moneybin/sql/schema/app_tabular_formats.sql` | DDL for `app.tabular_formats` |
| `src/moneybin/data/tabular_formats/chase_credit.yaml` | Built-in Chase credit format |
| `src/moneybin/data/tabular_formats/citi_credit.yaml` | Built-in Citi credit format |
| `src/moneybin/data/tabular_formats/tiller.yaml` | Built-in Tiller format |
| `src/moneybin/data/tabular_formats/mint.yaml` | Built-in Mint format |
| `src/moneybin/data/tabular_formats/ynab.yaml` | Built-in YNAB format |
| `src/moneybin/data/tabular_formats/maybe.yaml` | Built-in Maybe/Sure format |
| `sqlmesh/models/prep/stg_tabular__transactions.sql` | Staging view for tabular transactions |
| `sqlmesh/models/prep/stg_tabular__accounts.sql` | Staging view for tabular accounts |
| `tests/moneybin/test_extractors/test_tabular/test_format_detector.py` | Tests for Stage 1 |
| `tests/moneybin/test_extractors/test_tabular/test_readers.py` | Tests for Stage 2 |
| `tests/moneybin/test_extractors/test_tabular/test_field_aliases.py` | Tests for alias matching |
| `tests/moneybin/test_extractors/test_tabular/test_column_mapper.py` | Tests for Stage 3 |
| `tests/moneybin/test_extractors/test_tabular/test_date_detection.py` | Tests for date/number detection |
| `tests/moneybin/test_extractors/test_tabular/test_sign_convention.py` | Tests for sign convention |
| `tests/moneybin/test_extractors/test_tabular/test_transforms.py` | Tests for Stage 4 |
| `tests/moneybin/test_extractors/test_tabular/test_formats.py` | Tests for format system |
| `tests/moneybin/test_extractors/test_tabular/__init__.py` | Test package init |
| `tests/moneybin/test_loaders/test_tabular_loader.py` | Tests for Stage 5 |
| `tests/moneybin/test_services/test_tabular_import_service.py` | Tests for service layer |
| `tests/moneybin/test_cli/test_import_cmd_tabular.py` | Tests for CLI commands |
| `tests/fixtures/tabular/` | Test fixture directory (CSV, TSV, Excel, Parquet, Feather files) |

### Files to Modify

| File | Change |
|---|---|
| `src/moneybin/schema.py` | Add new schema files to `_SCHEMA_FILES` list |
| `src/moneybin/tables.py` | Add `TABULAR_TRANSACTIONS`, `TABULAR_ACCOUNTS`, `IMPORT_LOG`, `TABULAR_FORMATS` constants |
| `src/moneybin/services/import_service.py` | Replace `_import_csv()` with tabular pipeline, add tabular file type detection, add import history/revert functions |
| `src/moneybin/cli/commands/import_cmd.py` | New options (`--account-name`, `--yes`, `--override`, `--save-format`, etc.), new subcommands (`history`, `revert`, `preview`, `list-formats`, etc.) |
| `src/moneybin/mcp/write_tools.py` | Replace `import_file` tool, add `import_preview`, `import_history`, `import_revert`, `list_formats` |
| `sqlmesh/models/core/dim_accounts.sql` | Replace `csv_accounts` CTE with `tabular_accounts`, rename `source_system` → `source_type` |
| `sqlmesh/models/core/fct_transactions.sql` | Replace `csv_transactions` CTE with `tabular_transactions`, rename `source_system` → `source_type` |
| `src/moneybin/metrics/registry.py` | Add tabular-specific metrics (detection confidence, format matches, batch tracking) |
| `pyproject.toml` | Add `openpyxl` dependency |

### Files to Remove (after new system is working)

| File | Replaced by |
|---|---|
| `src/moneybin/extractors/csv_extractor.py` | `src/moneybin/extractors/tabular/` package |
| `src/moneybin/extractors/csv_profiles.py` | `src/moneybin/extractors/tabular/formats.py` |
| `src/moneybin/loaders/csv_loader.py` | `src/moneybin/loaders/tabular_loader.py` |
| `src/moneybin/data/csv_profiles/` | `src/moneybin/data/tabular_formats/` |
| `src/moneybin/sql/schema/raw_csv_transactions.sql` | `raw_tabular_transactions.sql` |
| `src/moneybin/sql/schema/raw_csv_accounts.sql` | `raw_tabular_accounts.sql` |
| `sqlmesh/models/prep/stg_csv__transactions.sql` | `stg_tabular__transactions.sql` |
| `sqlmesh/models/prep/stg_csv__accounts.sql` | `stg_tabular__accounts.sql` |
| `tests/moneybin/test_extractors/test_csv_extractor.py` | `test_tabular/` package |
| `tests/moneybin/test_extractors/test_csv_profiles.py` | `test_tabular/test_formats.py` |

---

## Phase 1: Data Model & Infrastructure

Foundation tables, constants, and dependencies. Everything else builds on this.

### Task 1: Add openpyxl dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add openpyxl**

```bash
uv add openpyxl
```

- [ ] **Step 2: Verify installation**

```bash
uv run python -c "import openpyxl; print(openpyxl.__version__)"
```

Expected: version number printed, no errors.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "deps: add openpyxl for Excel (.xlsx) reading via Polars"
```

### Task 2: Create raw.tabular_transactions schema

**Files:**
- Create: `src/moneybin/sql/schema/raw_tabular_transactions.sql`
- Test: Verified via Task 6 integration

- [ ] **Step 1: Write the DDL**

```sql
/* Imported financial transactions from tabular file sources (CSV, TSV, Excel,
   Parquet, Feather). Each row represents a single transaction as extracted from the
   source file with minimal transformation — amounts are sign-normalized but all
   original values are preserved for audit. */
CREATE TABLE IF NOT EXISTS raw.tabular_transactions (
    transaction_id VARCHAR NOT NULL,            -- Deterministic identifier: source_transaction_id when available, else SHA-256 hash of date|amount|description|account_id|row_number
    account_id VARCHAR NOT NULL,                -- Source-system account identifier; for multi-account files extracted from per-row account column, for single-account files provided or generated
    transaction_date DATE NOT NULL,             -- Primary transaction date parsed from source using detected or specified date format
    post_date DATE,                             -- Settlement or posting date when distinct from transaction date; NULL if source provides only one date
    amount DECIMAL(18, 2) NOT NULL,             -- Normalized amount: negative = expense, positive = income regardless of source sign convention
    original_amount VARCHAR,                    -- Raw amount string exactly as it appeared in the source file before sign normalization and parsing
    original_date_str VARCHAR,                  -- Raw date string exactly as it appeared in the source file before format parsing
    description VARCHAR,                        -- Primary transaction description, payee, or merchant name from source
    memo VARCHAR,                               -- Supplementary transaction details, extended description, or notes from source
    category VARCHAR,                           -- Source-provided transaction category if present; preserved as-is for migration bootstrap, not MoneyBin categorization
    subcategory VARCHAR,                        -- Source-provided transaction subcategory if present; preserved as-is
    transaction_type VARCHAR,                   -- Source-provided transaction type (e.g. Sale, Return, Payment, Dividend, Fee, Transfer)
    status VARCHAR,                             -- Source-provided transaction status (e.g. Cleared, Pending, Posted, Reconciled)
    check_number VARCHAR,                       -- Check or cheque number for check-based transactions
    source_transaction_id VARCHAR,              -- Institution-assigned unique transaction identifier if present; strongest dedup signal for same-source re-imports
    reference_number VARCHAR,                   -- Institution-assigned reference, confirmation, or receipt number; not guaranteed unique across transactions
    balance DECIMAL(18, 2),                     -- Running account balance after this transaction if provided by source
    currency VARCHAR,                           -- ISO 4217 currency code if present in source (e.g. USD, EUR); captured now, multi-currency processing deferred
    member_name VARCHAR,                        -- Account holder, cardholder, or member name if present in source
    source_file VARCHAR NOT NULL,               -- Absolute path to the imported file at time of extraction
    source_type VARCHAR NOT NULL,               -- Import pathway that produced this record: csv, tsv, excel, parquet, feather, pipe
    source_origin VARCHAR NOT NULL,             -- Institution/connection/format that produced this data (e.g. "chase_credit", "tiller", Plaid item_id); scopes Tier 2b dedup
    import_id VARCHAR NOT NULL,                 -- UUID linking this row to its import batch in raw.import_log; enables import reverting and history
    row_number INTEGER,                         -- 1-based row/line number in the source file; invaluable for debugging import issues and deterministic hash generation
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the extraction pipeline processed this record
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Timestamp when this record was written to the raw table
    PRIMARY KEY (transaction_id, account_id, source_file)
);
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/sql/schema/raw_tabular_transactions.sql
git commit -m "feat: add raw.tabular_transactions schema"
```

### Task 3: Create raw.tabular_accounts schema

**Files:**
- Create: `src/moneybin/sql/schema/raw_tabular_accounts.sql`

- [ ] **Step 1: Write the DDL**

```sql
/* Accounts discovered during tabular file imports. For single-account files, one
   record is created from the --account-name flag. For multi-account files (Tiller,
   Mint), one record per unique account found in the data. Account numbers are stored
   here (not per-transaction) and masked at the application layer for display. */
CREATE TABLE IF NOT EXISTS raw.tabular_accounts (
    account_id VARCHAR NOT NULL,                -- Source-system account identifier
    account_name VARCHAR NOT NULL,              -- Human-readable account label provided by user or extracted from multi-account file
    account_number VARCHAR,                     -- Full account number if available in source; stored encrypted at rest, masked at application layer for all output
    account_number_masked VARCHAR,              -- Last 4 digits for display (e.g. "...4521"); derived from account_number or extracted directly if source only provides masked
    account_type VARCHAR,                       -- Account type if known (e.g. checking, savings, credit, brokerage, investment)
    institution_name VARCHAR,                   -- Financial institution name from format metadata, source file content, or user input
    currency VARCHAR,                           -- Default currency for this account if known (ISO 4217 code)
    source_file VARCHAR NOT NULL,               -- Absolute path to the imported file that created or updated this account record
    source_type VARCHAR NOT NULL,               -- Import pathway that produced this record: csv, tsv, excel, parquet, feather, pipe
    source_origin VARCHAR NOT NULL,             -- Institution/connection/format that produced this data; matches the format name for tabular imports
    import_id VARCHAR NOT NULL,                 -- UUID linking this row to its import batch in raw.import_log; enables import reverting and history
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the extraction pipeline processed this record
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Timestamp when this record was written to the raw table
    PRIMARY KEY (account_id, source_file)
);
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/sql/schema/raw_tabular_accounts.sql
git commit -m "feat: add raw.tabular_accounts schema"
```

### Task 4: Create raw.import_log schema

**Files:**
- Create: `src/moneybin/sql/schema/raw_import_log.sql`

- [ ] **Step 1: Write the DDL**

```sql
/* Audit log of every tabular file import. Each import batch gets a UUID that is
   stamped on every raw row it produces, enabling import history, reverting, and
   diagnostics. */
CREATE TABLE IF NOT EXISTS raw.import_log (
    import_id VARCHAR PRIMARY KEY,              -- UUID generated at the start of each import batch
    source_file VARCHAR NOT NULL,               -- Absolute path to the imported file
    source_type VARCHAR NOT NULL,               -- File format: csv, tsv, excel, parquet, feather, pipe
    source_origin VARCHAR NOT NULL,             -- Format/institution that produced this data
    format_name VARCHAR,                        -- Name of the matched or saved format (NULL if no format matched)
    format_source VARCHAR,                      -- How the format was resolved: "built-in", "saved", "detected", "override"
    account_names JSON NOT NULL,                -- List of account names affected by this import
    status VARCHAR NOT NULL DEFAULT 'importing' CHECK (status IN ('importing', 'complete', 'partial', 'failed', 'reverted')), -- Lifecycle: importing → complete | partial | failed | reverted
    rows_total INTEGER,                         -- Total rows in source file (before filtering)
    rows_imported INTEGER,                      -- Rows successfully written to raw tables
    rows_rejected INTEGER DEFAULT 0,            -- Rows that failed validation (with reasons in rejection_details)
    rows_skipped_trailing INTEGER DEFAULT 0,    -- Trailing junk rows removed by skip patterns
    rejection_details JSON,                     -- Per-rejected-row: [{row_number, reason}]
    detection_confidence VARCHAR,               -- Confidence tier of the column mapping: high, medium, low (NULL if format matched)
    number_format VARCHAR,                      -- Detected number convention: us, european, swiss_french, zero_decimal
    date_format VARCHAR,                        -- Date format string used for parsing
    sign_convention VARCHAR,                    -- Sign convention applied: negative_is_expense, negative_is_income, split_debit_credit
    balance_validated BOOLEAN,                  -- Whether running balance validation passed (NULL if no balance column)
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the import batch began
    completed_at TIMESTAMP,                     -- When the import batch finished (NULL if still running or failed)
    reverted_at TIMESTAMP                       -- When the import was reverted (NULL if not reverted)
);
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/sql/schema/raw_import_log.sql
git commit -m "feat: add raw.import_log schema for import batch tracking"
```

### Task 5: Create app.tabular_formats schema

**Files:**
- Create: `src/moneybin/sql/schema/app_tabular_formats.sql`

- [ ] **Step 1: Write the DDL**

```sql
/* Saved column mappings for known tabular file formats. Built-in formats (Chase,
   Citi, Tiller, Mint, YNAB) are seeded from YAML files on db init. User formats
   are auto-saved after successful heuristic detection or created via --override +
   --save-format. User formats override built-ins of the same name. */
CREATE TABLE IF NOT EXISTS app.tabular_formats (
    name VARCHAR PRIMARY KEY,                   -- Machine identifier for this format (e.g. "chase_credit", "tiller", "mint")
    institution_name VARCHAR NOT NULL,          -- Human-readable institution or tool name (e.g. "Chase", "Tiller", "Mint")
    file_type VARCHAR NOT NULL DEFAULT 'auto',  -- Expected file type: csv, tsv, xlsx, parquet, feather, pipe, or "auto" for any type
    delimiter VARCHAR,                          -- Explicit delimiter character for text formats; NULL means auto-detected at import time
    encoding VARCHAR NOT NULL DEFAULT 'utf-8',  -- Character encoding for text formats (e.g. utf-8, latin-1, windows-1252)
    skip_rows INTEGER NOT NULL DEFAULT 0,       -- Number of non-data rows to skip before the header row in the source file
    sheet VARCHAR,                              -- Excel sheet name to read; NULL means auto-select the sheet with the most data rows
    header_signature JSON NOT NULL,             -- Ordered list of column names that uniquely fingerprint this format for auto-detection (case-insensitive subset matching)
    field_mapping JSON NOT NULL,                -- Mapping of destination field names to source column names
    sign_convention VARCHAR NOT NULL,           -- How amounts are represented in the source: negative_is_expense, negative_is_income, split_debit_credit
    date_format VARCHAR NOT NULL,               -- strftime format string for parsing date values (e.g. "%m/%d/%Y", "%Y-%m-%d")
    number_format VARCHAR NOT NULL DEFAULT 'us', -- Number convention: us (1,234.56), european (1.234,56), swiss_french (1 234,56), zero_decimal (1,234)
    skip_trailing_patterns JSON,                -- Regex patterns for trailing non-data rows: NULL = use default patterns, [] = no patterns, ["^Total"] = custom
    multi_account BOOLEAN NOT NULL DEFAULT FALSE, -- Whether this format expects per-row account identification (Tiller, Mint, Monarch)
    source VARCHAR NOT NULL DEFAULT 'detected', -- How this format was created: "detected", "manual", "built-in-override"
    times_used INTEGER NOT NULL DEFAULT 0,      -- Number of successful imports completed using this format
    last_used_at TIMESTAMP,                     -- Timestamp of the most recent successful import using this format
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this format was first created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp when this format was last modified
);
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/sql/schema/app_tabular_formats.sql
git commit -m "feat: add app.tabular_formats schema for format persistence"
```

### Task 6: Register schemas and table constants

**Files:**
- Modify: `src/moneybin/schema.py:35-56` (add new files to `_SCHEMA_FILES`)
- Modify: `src/moneybin/tables.py` (add new `TableRef` constants)
- Test: `tests/moneybin/test_schema_registration.py` (if exists) or manual verification

- [ ] **Step 1: Write failing test for table constants**

Create `tests/moneybin/test_extractors/test_tabular/__init__.py` (empty) and verify the new constants exist:

```python
# tests/moneybin/test_tabular_tables.py
"""Verify tabular table registry constants exist and are well-formed."""

from moneybin.tables import (
    IMPORT_LOG,
    TABULAR_ACCOUNTS,
    TABULAR_FORMATS,
    TABULAR_TRANSACTIONS,
)


def test_tabular_transactions_ref() -> None:
    assert TABULAR_TRANSACTIONS.schema == "raw"
    assert TABULAR_TRANSACTIONS.name == "tabular_transactions"
    assert TABULAR_TRANSACTIONS.full_name == "raw.tabular_transactions"


def test_tabular_accounts_ref() -> None:
    assert TABULAR_ACCOUNTS.schema == "raw"
    assert TABULAR_ACCOUNTS.name == "tabular_accounts"
    assert TABULAR_ACCOUNTS.full_name == "raw.tabular_accounts"


def test_import_log_ref() -> None:
    assert IMPORT_LOG.schema == "raw"
    assert IMPORT_LOG.name == "import_log"
    assert IMPORT_LOG.full_name == "raw.import_log"


def test_tabular_formats_ref() -> None:
    assert TABULAR_FORMATS.schema == "app"
    assert TABULAR_FORMATS.name == "tabular_formats"
    assert TABULAR_FORMATS.full_name == "app.tabular_formats"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/moneybin/test_tabular_tables.py -v
```

Expected: ImportError — `TABULAR_TRANSACTIONS` etc. not found in `moneybin.tables`.

- [ ] **Step 3: Add table constants to tables.py**

Add after the existing CSV constants in `src/moneybin/tables.py`:

```python
# -- Raw tabular tables (replaces csv_* tables) --
TABULAR_TRANSACTIONS = TableRef("raw", "tabular_transactions")
TABULAR_ACCOUNTS = TableRef("raw", "tabular_accounts")
IMPORT_LOG = TableRef("raw", "import_log")

# -- App tabular tables --
TABULAR_FORMATS = TableRef("app", "tabular_formats")
```

- [ ] **Step 4: Register schema files in schema.py**

Add to `_SCHEMA_FILES` list in `src/moneybin/schema.py` (after the existing `raw_csv_*` entries):

```python
    "raw_tabular_transactions.sql",
    "raw_tabular_accounts.sql",
    "raw_import_log.sql",
    "app_tabular_formats.sql",
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_tabular_tables.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/tables.py src/moneybin/schema.py tests/moneybin/test_tabular_tables.py
git commit -m "feat: register tabular schemas and table constants"
```

---

## Phase 2: Format System (TabularFormat Model + YAML)

The format system is the foundation for both built-in format matching (Stage 3, Step 1) and format persistence (Stage 5). Build it before the detection engine so the detection engine can use it.

### Task 7: Create field aliases module

**Files:**
- Create: `src/moneybin/extractors/tabular/__init__.py`
- Create: `src/moneybin/extractors/tabular/field_aliases.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_field_aliases.py`

- [ ] **Step 1: Create package init**

```python
# src/moneybin/extractors/tabular/__init__.py
"""Smart tabular import — universal tabular file importer."""
```

- [ ] **Step 2: Write failing tests for alias matching**

```python
# tests/moneybin/test_extractors/test_tabular/__init__.py
```

```python
# tests/moneybin/test_extractors/test_tabular/test_field_aliases.py
"""Tests for header normalization and alias matching."""

import pytest

from moneybin.extractors.tabular.field_aliases import (
    FIELD_ALIASES,
    match_header_to_field,
    normalize_header,
)


class TestNormalizeHeader:
    def test_lowercase(self) -> None:
        assert normalize_header("Transaction Date") == "transaction date"

    def test_strip_whitespace(self) -> None:
        assert normalize_header("  Amount  ") == "amount"

    def test_collapse_multiple_spaces(self) -> None:
        assert normalize_header("Transaction   Date") == "transaction date"

    def test_replace_underscores(self) -> None:
        assert normalize_header("transaction_date") == "transaction date"

    def test_replace_hyphens(self) -> None:
        assert normalize_header("transaction-date") == "transaction date"

    def test_strip_quotes(self) -> None:
        assert normalize_header('"Amount"') == "amount"
        assert normalize_header("'Amount'") == "amount"


class TestMatchHeaderToField:
    def test_exact_alias_match(self) -> None:
        assert match_header_to_field("Transaction Date") == "transaction_date"

    def test_normalized_alias_match(self) -> None:
        assert match_header_to_field("TRANSACTION_DATE") == "transaction_date"

    def test_amount_match(self) -> None:
        assert match_header_to_field("Amount") == "amount"

    def test_description_match(self) -> None:
        assert match_header_to_field("Payee") == "description"

    def test_debit_match(self) -> None:
        assert match_header_to_field("Debit Amount") == "debit_amount"

    def test_credit_match(self) -> None:
        assert match_header_to_field("Credit") == "credit_amount"

    def test_post_date_match(self) -> None:
        assert match_header_to_field("Posting Date") == "post_date"

    def test_check_number_match(self) -> None:
        assert match_header_to_field("Check #") == "check_number"

    def test_account_name_match(self) -> None:
        assert match_header_to_field("Account") == "account_name"

    def test_no_match_returns_none(self) -> None:
        assert match_header_to_field("Gobbledygook Column") is None

    def test_all_aliases_are_normalized(self) -> None:
        """Every alias in the table must equal its normalized form."""
        for field, aliases in FIELD_ALIASES.items():
            for alias in aliases:
                assert alias == normalize_header(alias), (
                    f"Alias '{alias}' for field '{field}' is not pre-normalized"
                )
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_field_aliases.py -v
```

Expected: ImportError — module not found.

- [ ] **Step 4: Implement field_aliases.py**

```python
# src/moneybin/extractors/tabular/field_aliases.py
"""Header normalization and alias matching for column mapping.

The FIELD_ALIASES table maps each destination field to a ranked list of
normalized header strings. Headers from source files are normalized before
matching (lowercase, collapse whitespace, strip quotes, replace separators).
"""

import re

# All alias values MUST be pre-normalized (lowercase, single spaces, no
# quotes, no underscores/hyphens). The test suite enforces this invariant.
FIELD_ALIASES: dict[str, list[str]] = {
    # Required fields (must find all three or detection fails)
    "transaction_date": [
        "transaction date",
        "trans date",
        "date",
        "effective date",
        "trade date",
        "txn date",
    ],
    "amount": [
        "amount",
        "transaction amount",
        "trans amount",
        "net amount",
    ],
    "description": [
        "description",
        "payee",
        "merchant",
        "narrative",
        "transaction description",
        "details",
        "name",
    ],
    # Amount variants (detected as amount if no single amount column)
    "debit_amount": [
        "debit",
        "debit amount",
        "withdrawals",
        "withdrawal",
        "money out",
        "debit amt",
        "outflow",
    ],
    "credit_amount": [
        "credit",
        "credit amount",
        "deposits",
        "deposit",
        "money in",
        "credit amt",
        "inflow",
    ],
    # Optional transaction fields
    "post_date": [
        "post date",
        "posting date",
        "settlement date",
        "posted date",
        "settle date",
    ],
    "memo": [
        "memo",
        "notes",
        "additional info",
        "extended description",
        "full description",
    ],
    "category": ["category", "transaction category"],
    "subcategory": ["subcategory", "sub category"],
    "transaction_type": [
        "type",
        "transaction type",
        "trans type",
        "tran type",
    ],
    "status": ["status", "state", "transaction status", "cleared"],
    "check_number": [
        "check number",
        "check no",
        "check #",
        "cheque number",
        "check",
    ],
    "source_transaction_id": [
        "transaction id",
        "trans id",
        "txn id",
        "transaction #",
        "fitid",
        "id",
        "unique id",
    ],
    "reference_number": [
        "reference",
        "ref",
        "confirmation",
        "conf number",
        "reference number",
        "ref number",
        "receipt",
    ],
    "balance": [
        "balance",
        "running balance",
        "available balance",
        "ledger balance",
    ],
    "currency": ["currency", "currency code", "ccy", "cur"],
    "member_name": [
        "member name",
        "account holder",
        "cardholder",
        "card member",
    ],
    # Account-identifying fields (trigger multi-account mode)
    "account_name": [
        "account",
        "account name",
        "acct name",
        "acct",
    ],
    "account_number": [
        "account #",
        "account number",
        "acct #",
        "acct number",
        "account no",
    ],
    "institution_name": [
        "institution",
        "bank",
        "bank name",
        "financial institution",
    ],
    "account_type": [
        "account type",
        "acct type",
        "class",
    ],
}

# Pre-built reverse lookup: normalized alias → destination field name.
# Built once at import time. First alias wins (earlier = higher priority).
_ALIAS_TO_FIELD: dict[str, str] = {}
for _field, _aliases in FIELD_ALIASES.items():
    for _alias in _aliases:
        if _alias not in _ALIAS_TO_FIELD:
            _ALIAS_TO_FIELD[_alias] = _field

# Fields that trigger multi-account mode when detected
ACCOUNT_IDENTIFYING_FIELDS: frozenset[str] = frozenset(
    {"account_name", "account_number", "institution_name", "account_type"}
)

# Required fields — detection fails if any of these can't be mapped
REQUIRED_FIELDS: frozenset[str] = frozenset(
    {"transaction_date", "amount", "description"}
)

_NORMALIZE_RE = re.compile(r"[\s_\-]+")
_QUOTE_RE = re.compile(r"""^["']|["']$""")


def normalize_header(header: str) -> str:
    """Normalize a column header for alias matching.

    Applies: lowercase, strip outer whitespace, strip quotes, replace
    underscores and hyphens with spaces, collapse multiple spaces.

    Args:
        header: Raw column header string from a source file.

    Returns:
        Normalized header string.
    """
    h = header.strip().lower()
    h = _QUOTE_RE.sub("", h)
    h = _NORMALIZE_RE.sub(" ", h)
    return h.strip()


def match_header_to_field(header: str) -> str | None:
    """Match a source column header to a destination field.

    Args:
        header: Raw column header string from a source file.

    Returns:
        Destination field name if matched, None otherwise.
    """
    normalized = normalize_header(header)
    return _ALIAS_TO_FIELD.get(normalized)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_field_aliases.py -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/extractors/tabular/__init__.py src/moneybin/extractors/tabular/field_aliases.py tests/moneybin/test_extractors/test_tabular/__init__.py tests/moneybin/test_extractors/test_tabular/test_field_aliases.py
git commit -m "feat: add field alias table and header normalization"
```

### Task 8: Create TabularFormat Pydantic model and YAML loading

**Files:**
- Create: `src/moneybin/extractors/tabular/formats.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_formats.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_formats.py
"""Tests for TabularFormat model and YAML loading."""

from pathlib import Path

import pytest

from moneybin.extractors.tabular.formats import (
    TabularFormat,
    load_builtin_formats,
)


class TestTabularFormatModel:
    def test_minimal_valid_format(self) -> None:
        fmt = TabularFormat(
            name="test_bank",
            institution_name="Test Bank",
            header_signature=["Date", "Amount", "Description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%m/%d/%Y",
        )
        assert fmt.name == "test_bank"
        assert fmt.multi_account is False
        assert fmt.number_format == "us"

    def test_split_debit_credit_requires_both_columns(self) -> None:
        fmt = TabularFormat(
            name="test_split",
            institution_name="Test",
            header_signature=["Date", "Debit", "Credit", "Desc"],
            field_mapping={
                "transaction_date": "Date",
                "debit_amount": "Debit",
                "credit_amount": "Credit",
                "description": "Desc",
            },
            sign_convention="split_debit_credit",
            date_format="%m/%d/%Y",
        )
        assert fmt.sign_convention == "split_debit_credit"

    def test_invalid_sign_convention_rejected(self) -> None:
        with pytest.raises(ValueError, match="sign_convention"):
            TabularFormat(
                name="bad",
                institution_name="Bad",
                header_signature=["Date"],
                field_mapping={"transaction_date": "Date"},
                sign_convention="invalid",
                date_format="%Y",
            )

    def test_invalid_number_format_rejected(self) -> None:
        with pytest.raises(ValueError, match="number_format"):
            TabularFormat(
                name="bad",
                institution_name="Bad",
                header_signature=["Date"],
                field_mapping={"transaction_date": "Date"},
                sign_convention="negative_is_expense",
                date_format="%Y",
                number_format="invalid",
            )

    def test_header_signature_match_subset(self) -> None:
        fmt = TabularFormat(
            name="test",
            institution_name="Test",
            header_signature=["Date", "Amount"],
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        # File has more columns than the signature — should still match
        file_headers = ["Date", "Amount", "Description", "Category"]
        assert fmt.matches_headers(file_headers)

    def test_header_signature_no_match(self) -> None:
        fmt = TabularFormat(
            name="test",
            institution_name="Test",
            header_signature=["Date", "Amount", "Payee"],
            field_mapping={},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        file_headers = ["Date", "Amount", "Description"]
        assert not fmt.matches_headers(file_headers)

    def test_header_match_case_insensitive(self) -> None:
        fmt = TabularFormat(
            name="test",
            institution_name="Test",
            header_signature=["Transaction Date", "Amount"],
            field_mapping={},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        file_headers = ["TRANSACTION DATE", "AMOUNT", "DESC"]
        assert fmt.matches_headers(file_headers)


class TestLoadBuiltinFormats:
    def test_builtin_formats_load(self) -> None:
        formats = load_builtin_formats()
        assert len(formats) >= 6
        assert "chase_credit" in formats
        assert "citi_credit" in formats
        assert "tiller" in formats
        assert "mint" in formats
        assert "ynab" in formats
        assert "maybe" in formats

    def test_chase_credit_format(self) -> None:
        formats = load_builtin_formats()
        chase = formats["chase_credit"]
        assert chase.institution_name == "Chase"
        assert chase.sign_convention == "negative_is_expense"
        assert chase.date_format == "%m/%d/%Y"
        assert "transaction_date" in chase.field_mapping
        assert "amount" in chase.field_mapping
        assert "description" in chase.field_mapping

    def test_tiller_is_multi_account(self) -> None:
        formats = load_builtin_formats()
        assert formats["tiller"].multi_account is True

    def test_maybe_is_multi_account(self) -> None:
        formats = load_builtin_formats()
        assert formats["maybe"].multi_account is True

    def test_ynab_is_single_account(self) -> None:
        formats = load_builtin_formats()
        assert formats["ynab"].multi_account is False

    def test_citi_is_split_debit_credit(self) -> None:
        formats = load_builtin_formats()
        assert formats["citi_credit"].sign_convention == "split_debit_credit"

    def test_to_yaml_roundtrip(self, tmp_path: Path) -> None:
        fmt = TabularFormat(
            name="roundtrip_test",
            institution_name="Test",
            header_signature=["Date", "Amount"],
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
            number_format="european",
            skip_trailing_patterns=["^Total"],
        )
        yaml_path = tmp_path / "roundtrip_test.yaml"
        fmt.to_yaml(yaml_path)

        loaded = TabularFormat.from_yaml(yaml_path)
        assert loaded.name == fmt.name
        assert loaded.number_format == "european"
        assert loaded.skip_trailing_patterns == ["^Total"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_formats.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement formats.py**

```python
# src/moneybin/extractors/tabular/formats.py
"""TabularFormat model and format loading/persistence.

Formats describe how to read a specific institution's tabular export:
column mapping, date format, sign convention, delimiter, etc. Built-in
formats ship as YAML files; user formats are stored in the database.
"""

import logging
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, field_validator, model_validator

logger = logging.getLogger(__name__)

_BUILTIN_FORMATS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "tabular_formats"
)

SignConventionType = Literal[
    "negative_is_expense", "negative_is_income", "split_debit_credit"
]
NumberFormatType = Literal["us", "european", "swiss_french", "zero_decimal"]


class TabularFormat(BaseModel, frozen=True):
    """Column mapping for a specific institution's tabular export format.

    Immutable (frozen) for safety — create a new instance to modify.
    """

    name: str
    """Machine identifier, e.g. ``chase_credit``."""

    institution_name: str
    """Human-readable institution name, e.g. ``Chase``."""

    file_type: str = "auto"
    """Expected file type: csv, tsv, xlsx, parquet, feather, pipe, or auto."""

    delimiter: str | None = None
    """Explicit delimiter for text formats; None = auto-detect."""

    encoding: str = "utf-8"
    """Character encoding for text formats."""

    skip_rows: int = 0
    """Non-data rows to skip before the header row."""

    sheet: str | None = None
    """Excel sheet name; None = auto-select largest."""

    header_signature: list[str]
    """Column names that fingerprint this format (case-insensitive subset)."""

    field_mapping: dict[str, str]
    """Destination field → source column name mapping."""

    sign_convention: SignConventionType
    """How amounts are represented in the source."""

    date_format: str
    """strftime format string for date parsing."""

    number_format: NumberFormatType = "us"
    """Number convention: us, european, swiss_french, zero_decimal."""

    skip_trailing_patterns: list[str] | None = None
    """Regex patterns for trailing junk rows. None = use defaults."""

    multi_account: bool = False
    """Whether this format has per-row account identification."""

    source: str = "detected"
    """How created: detected, manual, built-in-override."""

    times_used: int = 0
    """Successful import count."""

    last_used_at: str | None = None
    """Timestamp of last successful import."""

    @field_validator("sign_convention", mode="before")
    @classmethod
    def _validate_sign_convention(cls, v: str) -> str:
        valid = {"negative_is_expense", "negative_is_income", "split_debit_credit"}
        if v not in valid:
            raise ValueError(
                f"sign_convention must be one of {valid}, got {v!r}"
            )
        return v

    @field_validator("number_format", mode="before")
    @classmethod
    def _validate_number_format(cls, v: str) -> str:
        valid = {"us", "european", "swiss_french", "zero_decimal"}
        if v not in valid:
            raise ValueError(
                f"number_format must be one of {valid}, got {v!r}"
            )
        return v

    def matches_headers(self, file_headers: list[str]) -> bool:
        """Check if a file's headers match this format's signature.

        Case-insensitive subset match: every header in the signature must
        appear in the file's headers.

        Args:
            file_headers: Column headers from the source file.

        Returns:
            True if signature is a subset of file_headers.
        """
        normalized_file = {h.strip().lower() for h in file_headers}
        return all(
            sig.strip().lower() in normalized_file
            for sig in self.header_signature
        )

    def to_yaml(self, path: Path) -> None:
        """Serialize this format to a YAML file.

        Args:
            path: File path to write.
        """
        data = self.model_dump(
            exclude={"times_used", "last_used_at", "source"},
            exclude_none=True,
        )
        # Convert file_type 'auto' default to omit from YAML
        if data.get("file_type") == "auto":
            data.pop("file_type", None)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    @classmethod
    def from_yaml(cls, path: Path) -> "TabularFormat":
        """Load a format from a YAML file.

        Args:
            path: Path to YAML file.

        Returns:
            TabularFormat instance.
        """
        with open(path) as f:
            data = yaml.safe_load(f)
        # YAML files use 'format' key for file_type (legacy compat)
        if "format" in data and "file_type" not in data:
            data["file_type"] = data.pop("format")
        return cls(**data)


def load_builtin_formats() -> dict[str, TabularFormat]:
    """Load all built-in format YAML files.

    Returns:
        Dict mapping format name to TabularFormat instance.
    """
    formats: dict[str, TabularFormat] = {}
    if not _BUILTIN_FORMATS_DIR.exists():
        logger.warning(f"Built-in formats directory not found: {_BUILTIN_FORMATS_DIR}")
        return formats

    for yaml_path in sorted(_BUILTIN_FORMATS_DIR.glob("*.yaml")):
        try:
            fmt = TabularFormat.from_yaml(yaml_path)
            formats[fmt.name] = fmt
            logger.debug(f"Loaded built-in format: {fmt.name}")
        except Exception:
            logger.warning(f"Failed to load format: {yaml_path}", exc_info=True)

    return formats
```

- [ ] **Step 4: Run tests to verify they pass (will fail — YAML files don't exist yet)**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_formats.py::TestTabularFormatModel -v
```

Expected: Model tests PASS. Built-in format tests FAIL (no YAML files yet).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/formats.py tests/moneybin/test_extractors/test_tabular/test_formats.py
git commit -m "feat: add TabularFormat model with YAML serialization"
```

### Task 9: Create built-in format YAML files

**Files:**
- Create: `src/moneybin/data/tabular_formats/chase_credit.yaml`
- Create: `src/moneybin/data/tabular_formats/citi_credit.yaml`
- Create: `src/moneybin/data/tabular_formats/tiller.yaml`
- Create: `src/moneybin/data/tabular_formats/mint.yaml`
- Create: `src/moneybin/data/tabular_formats/ynab.yaml`
- Create: `src/moneybin/data/tabular_formats/maybe.yaml`

- [ ] **Step 1: Create chase_credit.yaml**

```yaml
name: chase_credit
institution_name: Chase
format: csv
header_signature:
  - Transaction Date
  - Post Date
  - Description
  - Category
  - Type
  - Amount
  - Memo
field_mapping:
  transaction_date: Transaction Date
  post_date: Post Date
  description: Description
  category: Category
  transaction_type: Type
  amount: Amount
  memo: Memo
sign_convention: negative_is_expense
date_format: "%m/%d/%Y"
number_format: us
skip_trailing_patterns: null
```

- [ ] **Step 2: Create citi_credit.yaml**

```yaml
name: citi_credit
institution_name: Citi
format: csv
header_signature:
  - Status
  - Date
  - Description
  - Debit
  - Credit
  - Member Name
field_mapping:
  transaction_date: Date
  description: Description
  debit_amount: Debit
  credit_amount: Credit
  status: Status
  member_name: Member Name
sign_convention: split_debit_credit
date_format: "%m/%d/%Y"
number_format: us
skip_trailing_patterns: null
```

- [ ] **Step 3: Create tiller.yaml**

```yaml
name: tiller
institution_name: Tiller
format: csv
multi_account: true
header_signature:
  - Date
  - Description
  - Category
  - Amount
  - Account
  - Account #
  - Institution
  - Transaction ID
field_mapping:
  transaction_date: Date
  description: Description
  category: Category
  amount: Amount
  account_name: Account
  account_number: "Account #"
  institution_name: Institution
  source_transaction_id: Transaction ID
  memo: Full Description
sign_convention: negative_is_expense
date_format: "%m/%d/%Y"
number_format: us
skip_trailing_patterns: null
```

- [ ] **Step 4: Create mint.yaml**

```yaml
name: mint
institution_name: Mint
format: csv
multi_account: true
header_signature:
  - Date
  - Description
  - Original Description
  - Amount
  - Transaction Type
  - Category
  - Account Name
  - Labels
  - Notes
field_mapping:
  transaction_date: Date
  description: Description
  memo: Original Description
  amount: Amount
  transaction_type: Transaction Type
  category: Category
  account_name: Account Name
sign_convention: negative_is_expense
date_format: "%m/%d/%Y"
number_format: us
skip_trailing_patterns: null
```

- [ ] **Step 5: Create ynab.yaml**

```yaml
name: ynab
institution_name: YNAB
format: csv
header_signature:
  - Account
  - Flag
  - Date
  - Payee
  - Category Group/Category
  - Category Group
  - Category
  - Memo
  - Outflow
  - Inflow
  - Cleared
field_mapping:
  transaction_date: Date
  description: Payee
  category: Category Group/Category
  debit_amount: Outflow
  credit_amount: Inflow
  memo: Memo
  status: Cleared
sign_convention: split_debit_credit
date_format: "%m/%d/%Y"
number_format: us
skip_trailing_patterns: null
```

- [ ] **Step 6: Create maybe.yaml**

```yaml
name: maybe
institution_name: Maybe / Sure
format: csv
multi_account: true
header_signature:
  - date
  - name
  - amount
  - currency
  - account
  - category
  - tags
  - note
field_mapping:
  transaction_date: date
  description: name
  amount: amount
  currency: currency
  account_name: account
  category: category
  memo: note
sign_convention: negative_is_expense
date_format: "%Y-%m-%d"
number_format: us
skip_trailing_patterns: null
```

- [ ] **Step 7: Run built-in format tests**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_formats.py -v
```

Expected: All tests PASS including `TestLoadBuiltinFormats`.

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/data/tabular_formats/
git commit -m "feat: add 6 built-in tabular format YAML files (Chase, Citi, Tiller, Mint, YNAB, Maybe)"
```

---

## Phase 3: Format Detection (Stage 1)

Determine file type, delimiter, and encoding before reading.

### Task 10: Implement format detector

**Files:**
- Create: `src/moneybin/extractors/tabular/format_detector.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_format_detector.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_format_detector.py
"""Tests for file format detection (Stage 1)."""

from pathlib import Path

import pytest

from moneybin.extractors.tabular.format_detector import (
    FormatInfo,
    detect_delimiter,
    detect_encoding,
    detect_format,
)


class TestDetectFormat:
    def test_csv_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "data.csv"
        f.write_text("a,b,c\n1,2,3\n")
        info = detect_format(f)
        assert info.file_type == "csv"
        assert info.delimiter == ","

    def test_tsv_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "data.tsv"
        f.write_text("a\tb\tc\n1\t2\t3\n")
        info = detect_format(f)
        assert info.file_type == "tsv"
        assert info.delimiter == "\t"

    def test_tab_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "data.tab"
        f.write_text("a\tb\tc\n1\t2\t3\n")
        info = detect_format(f)
        assert info.file_type == "tsv"

    def test_txt_sniffs_delimiter(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("a|b|c\n1|2|3\n4|5|6\n")
        info = detect_format(f)
        assert info.file_type == "pipe"
        assert info.delimiter == "|"

    def test_xlsx_extension(self, tmp_path: Path) -> None:
        """Excel files detected by extension (magic bytes tested separately)."""
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Date", "Amount", "Desc"])
        ws.append(["2026-01-01", 42.50, "Test"])
        wb.save(tmp_path / "data.xlsx")

        info = detect_format(tmp_path / "data.xlsx")
        assert info.file_type == "excel"

    def test_parquet_extension(self, tmp_path: Path) -> None:
        import polars as pl

        df = pl.DataFrame({"a": [1], "b": [2]})
        path = tmp_path / "data.parquet"
        df.write_parquet(path)
        info = detect_format(path)
        assert info.file_type == "parquet"

    def test_feather_extension(self, tmp_path: Path) -> None:
        import polars as pl

        df = pl.DataFrame({"a": [1], "b": [2]})
        path = tmp_path / "data.feather"
        df.write_ipc(path)
        info = detect_format(path)
        assert info.file_type == "feather"

    def test_unsupported_extension_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "data.json"
        f.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported"):
            detect_format(f)


class TestDetectDelimiter:
    def test_comma(self) -> None:
        lines = ["a,b,c", "1,2,3", "4,5,6"]
        assert detect_delimiter(lines) == ","

    def test_tab(self) -> None:
        lines = ["a\tb\tc", "1\t2\t3", "4\t5\t6"]
        assert detect_delimiter(lines) == "\t"

    def test_pipe(self) -> None:
        lines = ["a|b|c", "1|2|3", "4|5|6"]
        assert detect_delimiter(lines) == "|"

    def test_semicolon(self) -> None:
        lines = ["a;b;c", "1;2;3", "4;5;6"]
        assert detect_delimiter(lines) == ";"

    def test_fallback_to_comma(self) -> None:
        """If no clear winner, default to comma."""
        lines = ["hello world"]
        assert detect_delimiter(lines) == ","


class TestDetectEncoding:
    def test_utf8(self, tmp_path: Path) -> None:
        f = tmp_path / "utf8.csv"
        f.write_text("café,naïve\n", encoding="utf-8")
        assert detect_encoding(f) == "utf-8"

    def test_latin1(self, tmp_path: Path) -> None:
        f = tmp_path / "latin1.csv"
        f.write_bytes("caf\xe9,na\xefve\n".encode("latin-1"))
        enc = detect_encoding(f)
        # charset-normalizer may return "iso-8859-1" or "latin-1" or "cp1252"
        assert enc in ("iso-8859-1", "latin-1", "cp1252", "windows-1252")


class TestSizeGuardrails:
    def test_text_file_over_25mb_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "big.csv"
        # Create a file just over 25 MB
        f.write_bytes(b"a,b,c\n" + b"1,2,3\n" * (25 * 1024 * 1024 // 6 + 1))
        with pytest.raises(ValueError, match="25 MB"):
            detect_format(f)

    def test_text_file_over_25mb_with_override(self, tmp_path: Path) -> None:
        f = tmp_path / "big.csv"
        f.write_bytes(b"a,b,c\n" + b"1,2,3\n" * (25 * 1024 * 1024 // 6 + 1))
        info = detect_format(f, no_size_limit=True)
        assert info.file_type == "csv"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_format_detector.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement format_detector.py**

```python
# src/moneybin/extractors/tabular/format_detector.py
"""Stage 1: File format detection.

Determines file type, delimiter, encoding, and enforces size guardrails
before any data is read.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Extension → file_type mapping
_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".tab": "tsv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".feather": "feather",
    ".arrow": "feather",
    ".ipc": "feather",
    # .txt and .dat require sniffing
}

# Magic bytes for binary format confirmation
_MAGIC_BYTES: dict[str, tuple[bytes, ...]] = {
    "parquet": (b"PAR1",),
    "excel": (b"PK\x03\x04",),  # ZIP signature for xlsx
    "feather": (b"ARROW1",),
}

# File types that are text-based (need delimiter/encoding detection)
_TEXT_TYPES: frozenset[str] = frozenset({"csv", "tsv", "pipe", "semicolon"})

# Delimiter → file_type mapping
_DELIMITER_TYPE: dict[str, str] = {
    ",": "csv",
    "\t": "tsv",
    "|": "pipe",
    ";": "semicolon",
}

_TEXT_SIZE_LIMIT = 25 * 1024 * 1024  # 25 MB
_BINARY_SIZE_LIMIT = 100 * 1024 * 1024  # 100 MB


@dataclass(frozen=True)
class FormatInfo:
    """Result of format detection (Stage 1 output)."""

    file_type: str
    """Detected file type: csv, tsv, pipe, semicolon, excel, parquet, feather."""

    delimiter: str | None = None
    """Detected delimiter for text formats."""

    encoding: str = "utf-8"
    """Detected encoding for text formats."""

    file_size: int = 0
    """File size in bytes."""


def detect_format(
    path: Path,
    *,
    format_override: str | None = None,
    delimiter_override: str | None = None,
    encoding_override: str | None = None,
    no_size_limit: bool = False,
) -> FormatInfo:
    """Detect the file format and basic parameters.

    Args:
        path: Path to the file to detect.
        format_override: Explicit file type (skips detection).
        delimiter_override: Explicit delimiter (text formats only).
        encoding_override: Explicit encoding (text formats only).
        no_size_limit: If True, skip file size checks.

    Returns:
        FormatInfo with detected parameters.

    Raises:
        ValueError: If file type is unsupported or size limit exceeded.
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    file_size = path.stat().st_size

    # Determine file type
    if format_override:
        file_type = format_override
    else:
        file_type = _detect_type_from_extension(path)

    # Check magic bytes for binary formats
    if file_type in _MAGIC_BYTES and file_size >= 4:
        _verify_magic_bytes(path, file_type)

    # Enforce size guardrails
    if not no_size_limit:
        _check_size_limit(path, file_type, file_size)

    # For text formats, detect delimiter and encoding
    if file_type in _TEXT_TYPES or file_type in ("csv", "tsv", "pipe", "semicolon"):
        encoding = encoding_override or detect_encoding(path)
        if delimiter_override:
            delimiter = delimiter_override
            file_type = _DELIMITER_TYPE.get(delimiter, "csv")
        elif file_type == "csv":
            # CSV may actually be pipe or semicolon — sniff to confirm
            sample_lines = _read_sample_lines(path, encoding, n=20)
            delimiter = detect_delimiter(sample_lines)
            file_type = _DELIMITER_TYPE.get(delimiter, "csv")
        elif file_type == "tsv":
            delimiter = "\t"
        else:
            sample_lines = _read_sample_lines(path, encoding, n=20)
            delimiter = detect_delimiter(sample_lines)
            file_type = _DELIMITER_TYPE.get(delimiter, "csv")

        return FormatInfo(
            file_type=file_type,
            delimiter=delimiter,
            encoding=encoding,
            file_size=file_size,
        )

    return FormatInfo(file_type=file_type, file_size=file_size)


def _detect_type_from_extension(path: Path) -> str:
    """Map file extension to type, sniffing for ambiguous extensions.

    Args:
        path: File path.

    Returns:
        File type string.

    Raises:
        ValueError: If extension is not recognized.
    """
    suffix = path.suffix.lower()
    if suffix in _EXTENSION_MAP:
        return _EXTENSION_MAP[suffix]
    if suffix in (".txt", ".dat"):
        # Must sniff delimiter to determine type
        return "csv"  # Will be refined by delimiter detection
    raise ValueError(
        f"Unsupported file type: '{suffix}'. "
        f"Supported: .csv, .tsv, .tab, .txt, .dat, .xlsx, .parquet, .pq, "
        f".feather, .arrow, .ipc"
    )


def _verify_magic_bytes(path: Path, expected_type: str) -> None:
    """Verify magic bytes match the expected file type.

    Args:
        path: File path.
        expected_type: Expected file type from extension.
    """
    with open(path, "rb") as f:
        header = f.read(8)
    for magic in _MAGIC_BYTES.get(expected_type, ()):
        if header.startswith(magic):
            return
    logger.debug(
        f"Magic bytes for {path.name} don't match expected type "
        f"'{expected_type}' — proceeding with extension-based detection"
    )


def _check_size_limit(path: Path, file_type: str, file_size: int) -> None:
    """Enforce file size guardrails.

    Args:
        path: File path (for error message).
        file_type: Detected file type.
        file_size: File size in bytes.

    Raises:
        ValueError: If file exceeds size limit.
    """
    is_binary = file_type in ("excel", "parquet", "feather")
    limit = _BINARY_SIZE_LIMIT if is_binary else _TEXT_SIZE_LIMIT
    limit_mb = limit // (1024 * 1024)

    if file_size > limit:
        size_mb = file_size / (1024 * 1024)
        raise ValueError(
            f"File {path.name} is {size_mb:.1f} MB, exceeding the "
            f"{limit_mb} MB limit for {'binary' if is_binary else 'text'} "
            f"formats. Use --no-size-limit to override."
        )


def detect_delimiter(lines: list[str]) -> str:
    """Detect the most likely delimiter from sample lines.

    Tries comma, tab, pipe, semicolon. Picks the delimiter that produces
    the most consistent non-zero column count across sample rows.

    Args:
        lines: Sample lines from the file (typically first 20).

    Returns:
        Detected delimiter character. Defaults to comma.
    """
    candidates = [",", "\t", "|", ";"]
    best_delimiter = ","
    best_score = -1

    for delim in candidates:
        counts = [line.count(delim) for line in lines if line.strip()]
        if not counts or max(counts) == 0:
            continue
        # Score = consistency (low variance) × count
        avg = sum(counts) / len(counts)
        if avg == 0:
            continue
        variance = sum((c - avg) ** 2 for c in counts) / len(counts)
        # Prefer high count, low variance
        score = avg / (1 + variance)
        if score > best_score:
            best_score = score
            best_delimiter = delim

    return best_delimiter


def detect_encoding(path: Path) -> str:
    """Detect file encoding using charset-normalizer.

    Args:
        path: Path to the text file.

    Returns:
        Detected encoding string (e.g. "utf-8", "latin-1").
    """
    # Check for BOM first
    with open(path, "rb") as f:
        bom = f.read(4)
    if bom.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if bom.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if bom.startswith(b"\xfe\xff"):
        return "utf-16-be"

    # Try UTF-8 first (most common)
    try:
        with open(path, encoding="utf-8") as f:
            f.read(8192)
        return "utf-8"
    except UnicodeDecodeError:
        pass

    # Fall back to charset-normalizer
    from charset_normalizer import from_path

    result = from_path(path)
    best = result.best()
    if best and best.encoding:
        return best.encoding

    return "utf-8"  # Last resort fallback


def _read_sample_lines(path: Path, encoding: str, n: int = 20) -> list[str]:
    """Read the first N lines of a text file.

    Args:
        path: File path.
        encoding: File encoding.
        n: Number of lines to read.

    Returns:
        List of lines (without trailing newlines).
    """
    lines: list[str] = []
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                lines.append(line.rstrip("\n\r"))
    except Exception:
        logger.debug(f"Could not read sample lines from {path}", exc_info=True)
    return lines
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_format_detector.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/format_detector.py tests/moneybin/test_extractors/test_tabular/test_format_detector.py
git commit -m "feat: add Stage 1 format detection (extension, magic bytes, delimiter, encoding, size guardrails)"
```

---

## Phase 4: Readers (Stage 2)

Convert files into format-agnostic Polars DataFrames.

### Task 11: Implement file readers

**Files:**
- Create: `src/moneybin/extractors/tabular/readers.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_readers.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_readers.py
"""Tests for Stage 2 file readers."""

from pathlib import Path

import polars as pl
import pytest

from moneybin.extractors.tabular.format_detector import FormatInfo
from moneybin.extractors.tabular.readers import (
    ReadResult,
    read_file,
)


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(content)
    return path


class TestCSVReader:
    def test_basic_csv(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "basic.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 1
        assert list(result.df.columns) == ["Date", "Amount", "Description"]

    def test_skip_preamble_rows(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "preamble.csv",
            "Bank Summary Report\nGenerated: 2026-01-15\n\nDate,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 1
        assert "Date" in result.df.columns
        assert result.skip_rows > 0

    def test_trailing_total_row_removed(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "trailing.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n2026-01-02,10.00,Tea\nTotal,,52.50\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 2
        assert result.rows_skipped_trailing >= 1

    def test_bom_handled(self, tmp_path: Path) -> None:
        f = tmp_path / "bom.csv"
        f.write_bytes(b"\xef\xbb\xbfDate,Amount\n2026-01-01,42.50\n")
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8-sig")
        result = read_file(f, info)
        assert "Date" in result.df.columns

    def test_pipe_delimiter(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "pipe.txt",
            "Date|Amount|Description\n2026-01-01|42.50|Coffee\n",
        )
        info = FormatInfo(file_type="pipe", delimiter="|", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 1

    def test_row_limit_warning(self, tmp_path: Path) -> None:
        """Files over 10k rows produce a warning but proceed."""
        rows = "\n".join(f"2026-01-01,{i},Item{i}" for i in range(10_001))
        f = _write_csv(
            tmp_path / "big.csv",
            f"Date,Amount,Description\n{rows}\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 10_001
        assert result.row_count_warning is True

    def test_row_limit_refuse(self, tmp_path: Path) -> None:
        """Files over 50k rows are refused without override."""
        rows = "\n".join(f"2026-01-01,{i},Item{i}" for i in range(50_001))
        f = _write_csv(
            tmp_path / "huge.csv",
            f"Date,Amount,Description\n{rows}\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        with pytest.raises(ValueError, match="50,000"):
            read_file(f, info)

    def test_row_limit_refuse_with_override(self, tmp_path: Path) -> None:
        """Row limit override allows large files."""
        rows = "\n".join(f"2026-01-01,{i},Item{i}" for i in range(50_001))
        f = _write_csv(
            tmp_path / "huge.csv",
            f"Date,Amount,Description\n{rows}\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info, no_row_limit=True)
        assert len(result.df) == 50_001


class TestExcelReader:
    def test_basic_excel(self, tmp_path: Path) -> None:
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-01", 42.50, "Coffee"])
        path = tmp_path / "test.xlsx"
        wb.save(path)

        info = FormatInfo(file_type="excel")
        result = read_file(path, info)
        assert len(result.df) == 1
        assert "Date" in result.df.columns

    def test_multi_sheet_picks_largest(self, tmp_path: Path) -> None:
        import openpyxl

        wb = openpyxl.Workbook()
        # Default sheet with 1 row
        ws1 = wb.active
        ws1.title = "Summary"
        ws1.append(["Total", 100])
        # Second sheet with 3 rows
        ws2 = wb.create_sheet("Transactions")
        ws2.append(["Date", "Amount", "Desc"])
        ws2.append(["2026-01-01", 42.50, "Coffee"])
        ws2.append(["2026-01-02", 10.00, "Tea"])
        ws2.append(["2026-01-03", 5.00, "Water"])
        path = tmp_path / "multi.xlsx"
        wb.save(path)

        info = FormatInfo(file_type="excel")
        result = read_file(path, info)
        assert len(result.df) == 3


class TestParquetReader:
    def test_basic_parquet(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "date": ["2026-01-01"],
            "amount": [42.50],
            "description": ["Coffee"],
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)

        info = FormatInfo(file_type="parquet")
        result = read_file(path, info)
        assert len(result.df) == 1
        assert list(result.df.columns) == ["date", "amount", "description"]


class TestFeatherReader:
    def test_basic_feather(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "date": ["2026-01-01"],
            "amount": [42.50],
            "description": ["Coffee"],
        })
        path = tmp_path / "test.feather"
        df.write_ipc(path)

        info = FormatInfo(file_type="feather")
        result = read_file(path, info)
        assert len(result.df) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_readers.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement readers.py**

```python
# src/moneybin/extractors/tabular/readers.py
"""Stage 2: File readers producing format-agnostic Polars DataFrames.

Each reader converts a specific file type into a Polars DataFrame with
string column names. This is the format-agnostic boundary — everything
downstream operates on DataFrames regardless of source format.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from moneybin.extractors.tabular.format_detector import FormatInfo

logger = logging.getLogger(__name__)

_ROW_WARN_THRESHOLD = 10_000
_ROW_REFUSE_THRESHOLD = 50_000

DEFAULT_TRAILING_PATTERNS: list[str] = [
    r"^(Total|Grand Total|Sum|Totals)\b",
    r"^(Export(ed)?|Generated|Downloaded|Report) (Date|On|At)\b",
    r"^(Record Count|Row Count|Number of)",
    r"^(Opening|Closing|Beginning|Ending) Balance\b",
    r"^,{3,}$",
    r"^\s*$",
]


@dataclass
class ReadResult:
    """Output of a file reader."""

    df: pl.DataFrame
    """DataFrame with raw string column names."""

    skip_rows: int = 0
    """Number of preamble rows skipped before the header."""

    rows_skipped_trailing: int = 0
    """Number of trailing junk rows removed."""

    row_count_warning: bool = False
    """True if row count exceeded warning threshold."""

    sheet_used: str | None = None
    """Excel sheet name used (None for non-Excel)."""


def read_file(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    sheet: str | None = None,
    skip_trailing_patterns: list[str] | None = None,
    no_row_limit: bool = False,
) -> ReadResult:
    """Read a file into a format-agnostic Polars DataFrame.

    Args:
        path: File path.
        info: Format detection result from Stage 1.
        skip_rows: Explicit skip rows (overrides detection).
        sheet: Excel sheet name (overrides auto-selection).
        skip_trailing_patterns: Regex patterns for trailing junk.
            None = use defaults, [] = no patterns.
        no_row_limit: If True, skip row count limits.

    Returns:
        ReadResult with DataFrame and metadata.

    Raises:
        ValueError: If row count exceeds limit without override.
    """
    if info.file_type in ("csv", "tsv", "pipe", "semicolon"):
        result = _read_text(
            path, info,
            skip_rows=skip_rows,
            skip_trailing_patterns=skip_trailing_patterns,
        )
    elif info.file_type == "excel":
        result = _read_excel(path, info, skip_rows=skip_rows, sheet=sheet)
    elif info.file_type == "parquet":
        result = _read_parquet(path)
    elif info.file_type == "feather":
        result = _read_feather(path)
    else:
        raise ValueError(f"No reader for file type: {info.file_type}")

    # Row count guardrails
    row_count = len(result.df)
    if row_count > _ROW_REFUSE_THRESHOLD and not no_row_limit:
        raise ValueError(
            f"File has {row_count:,} rows, exceeding the 50,000 row limit. "
            f"Use --no-row-limit to override."
        )
    if row_count > _ROW_WARN_THRESHOLD:
        logger.warning(
            f"⚠️  File has {row_count:,} rows (warning threshold: "
            f"{_ROW_WARN_THRESHOLD:,}). Proceeding with import."
        )
        result.row_count_warning = True

    return result


def _read_text(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    skip_trailing_patterns: list[str] | None = None,
) -> ReadResult:
    """Read a text-based tabular file (CSV, TSV, pipe, semicolon).

    Handles header row detection, preamble skipping, and trailing row removal.
    """
    encoding = info.encoding
    delimiter = info.delimiter or ","

    # Detect header row if skip_rows not specified
    if skip_rows is None:
        skip_rows = _detect_header_row(path, encoding, delimiter)

    df = pl.read_csv(
        path,
        separator=delimiter,
        encoding=encoding if encoding != "utf-8-sig" else "utf-8",
        skip_rows=skip_rows,
        has_header=True,
        infer_schema_length=0,  # Read everything as strings
        truncate_ragged_lines=True,
    )

    # Remove trailing junk rows
    patterns = skip_trailing_patterns
    if patterns is None:
        patterns = DEFAULT_TRAILING_PATTERNS
    rows_removed = 0
    if patterns and len(df) > 0:
        df, rows_removed = _remove_trailing_rows(df, patterns)

    # Remove rows that are repeated headers
    if len(df) > 0:
        df = _remove_repeated_headers(df)

    return ReadResult(
        df=df,
        skip_rows=skip_rows,
        rows_skipped_trailing=rows_removed,
    )


def _detect_header_row(path: Path, encoding: str, delimiter: str) -> int:
    """Find the header row by scanning for the first row that looks like headers.

    Headers are characterized by: multiple short strings, low numeric ratio,
    high uniqueness among values.

    Args:
        path: File path.
        encoding: File encoding.
        delimiter: Column delimiter.

    Returns:
        Number of rows to skip before the header (0 if header is row 1).
    """
    enc = encoding if encoding != "utf-8-sig" else "utf-8"
    lines: list[str] = []
    try:
        with open(path, encoding=enc, errors="replace") as f:
            for i, line in enumerate(f):
                if i >= 30:
                    break
                lines.append(line.rstrip("\n\r"))
    except Exception:
        return 0

    for i, line in enumerate(lines):
        if not line.strip():
            continue
        parts = line.split(delimiter)
        if len(parts) < 2:
            continue
        # Check if this looks like a header: mostly non-numeric, short strings
        non_empty = [p.strip().strip('"').strip("'") for p in parts if p.strip()]
        if not non_empty:
            continue
        numeric_count = sum(1 for p in non_empty if _is_numeric(p))
        numeric_ratio = numeric_count / len(non_empty) if non_empty else 1.0
        # Headers have low numeric ratio
        if numeric_ratio < 0.5 and len(non_empty) >= 2:
            return i

    return 0


def _is_numeric(s: str) -> bool:
    """Check if a string looks numeric."""
    s = s.replace(",", "").replace("$", "").replace("€", "").strip()
    try:
        float(s)
        return True
    except ValueError:
        return False


def _remove_trailing_rows(
    df: pl.DataFrame, patterns: list[str]
) -> tuple[pl.DataFrame, int]:
    """Remove trailing rows matching regex patterns.

    Scans from the last row upward. Stops removing when a row doesn't match.

    Args:
        df: Input DataFrame.
        patterns: Regex patterns to match against the first column.

    Returns:
        Tuple of (filtered DataFrame, number of rows removed).
    """
    if len(df) == 0 or not patterns:
        return df, 0

    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    first_col = df.columns[0]
    values = df[first_col].to_list()

    remove_from = len(values)
    for i in range(len(values) - 1, -1, -1):
        val = str(values[i]) if values[i] is not None else ""
        # Also check the full row as CSV text for delimiter-only rows
        row_str = ",".join(
            str(df[col][i]) if df[col][i] is not None else ""
            for col in df.columns
        )
        if any(p.search(val) or p.search(row_str) for p in compiled):
            remove_from = i
        else:
            break

    if remove_from < len(values):
        removed = len(values) - remove_from
        return df.head(remove_from), removed
    return df, 0


def _remove_repeated_headers(df: pl.DataFrame) -> pl.DataFrame:
    """Remove rows that are duplicates of the header row.

    Some exports from paginated web views repeat the header mid-file.
    """
    if len(df) == 0:
        return df
    headers_lower = [c.lower() for c in df.columns]
    mask = pl.Series(
        [True] * len(df)
    )
    first_col = df.columns[0]
    first_col_values = df[first_col].cast(pl.Utf8).to_list()
    for i, val in enumerate(first_col_values):
        if val is not None and val.lower() == headers_lower[0]:
            # Check if this entire row matches the headers
            row_values = [
                str(df[col][i]).lower() if df[col][i] is not None else ""
                for col in df.columns
            ]
            if row_values == headers_lower:
                mask[i] = False
    return df.filter(mask)


def _read_excel(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    sheet: str | None = None,
) -> ReadResult:
    """Read an Excel (.xlsx) file."""
    import openpyxl

    sheet_used = sheet
    if sheet_used is None:
        # Pick the sheet with the most data rows
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            best_sheet = wb.sheetnames[0]
            best_rows = 0
            for name in wb.sheetnames:
                ws = wb[name]
                row_count = sum(1 for _ in ws.iter_rows(min_row=1))
                if row_count > best_rows:
                    best_rows = row_count
                    best_sheet = name
            sheet_used = best_sheet
        finally:
            wb.close()

    df = pl.read_excel(
        path,
        sheet_name=sheet_used,
        infer_schema_length=0,  # Read as strings for consistent handling
    )

    actual_skip = 0
    if skip_rows is not None and skip_rows > 0:
        df = df.slice(skip_rows)
        actual_skip = skip_rows

    return ReadResult(df=df, skip_rows=actual_skip, sheet_used=sheet_used)


def _read_parquet(path: Path) -> ReadResult:
    """Read a Parquet file."""
    df = pl.read_parquet(path)
    return ReadResult(df=df)


def _read_feather(path: Path) -> ReadResult:
    """Read a Feather/Arrow IPC file."""
    df = pl.read_ipc(path)
    return ReadResult(df=df)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_readers.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/readers.py tests/moneybin/test_extractors/test_tabular/test_readers.py
git commit -m "feat: add Stage 2 file readers (CSV, TSV, pipe, Excel, Parquet, Feather)"
```

---

## Phase 5: Date & Number Detection

Build the date format detection and number format detection engines used by Stage 3.

### Task 12: Implement date detection and number format detection

**Files:**
- Create: `src/moneybin/extractors/tabular/date_detection.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_date_detection.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_date_detection.py
"""Tests for date format detection and number format detection."""

import polars as pl
import pytest

from moneybin.extractors.tabular.date_detection import (
    detect_date_format,
    detect_number_format,
    parse_amount_str,
)


class TestDetectDateFormat:
    def test_us_format(self) -> None:
        values = ["01/15/2026", "02/20/2026", "03/31/2026"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%m/%d/%Y"

    def test_iso_format(self) -> None:
        values = ["2026-01-15", "2026-02-20", "2026-03-31"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%Y-%m-%d"

    def test_dd_mm_yyyy_with_day_over_12(self) -> None:
        """When position 1 has values >12, must be DD/MM."""
        values = ["15/01/2026", "20/02/2026", "31/03/2026"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%d/%m/%Y"

    def test_mm_dd_yyyy_with_day_over_12(self) -> None:
        """When position 2 has values >12, must be MM/DD."""
        values = ["01/15/2026", "02/20/2026", "03/31/2026"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%m/%d/%Y"

    def test_two_digit_year(self) -> None:
        values = ["01/15/26", "02/20/26", "03/31/26"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%m/%d/%y"

    def test_named_month(self) -> None:
        values = ["15-Mar-2026", "20-Apr-2026", "31-May-2026"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%d-%b-%Y"

    def test_long_month(self) -> None:
        values = ["Mar 15, 2026", "Apr 20, 2026", "May 31, 2026"]
        fmt, confidence = detect_date_format(values)
        assert fmt == "%b %d, %Y"

    def test_ambiguous_returns_medium_confidence(self) -> None:
        """All values ≤12 in both positions — truly ambiguous."""
        values = ["01/02/2026", "03/04/2026", "05/06/2026"]
        fmt, confidence = detect_date_format(values)
        assert confidence in ("medium", "high")

    def test_empty_values_handled(self) -> None:
        values = ["", None, "01/15/2026", "", "02/20/2026"]
        fmt, confidence = detect_date_format(values)
        assert fmt is not None


class TestDetectNumberFormat:
    def test_us_format(self) -> None:
        values = ["1,234.56", "42.50", "1,000.00"]
        assert detect_number_format(values) == "us"

    def test_european_format(self) -> None:
        values = ["1.234,56", "42,50", "1.000,00"]
        assert detect_number_format(values) == "european"

    def test_swiss_french_format(self) -> None:
        values = ["1 234,56", "42,50", "1 000,00"]
        assert detect_number_format(values) == "swiss_french"

    def test_zero_decimal(self) -> None:
        values = ["1,234", "42", "1,000"]
        assert detect_number_format(values) == "zero_decimal"

    def test_plain_numbers_default_us(self) -> None:
        values = ["42.50", "10.00", "100.25"]
        assert detect_number_format(values) == "us"


class TestParseAmountStr:
    def test_us_basic(self) -> None:
        assert parse_amount_str("1,234.56", "us") == 1234.56

    def test_european_basic(self) -> None:
        assert parse_amount_str("1.234,56", "european") == 1234.56

    def test_swiss_french_basic(self) -> None:
        assert parse_amount_str("1 234,56", "swiss_french") == 1234.56

    def test_zero_decimal(self) -> None:
        assert parse_amount_str("1,234", "zero_decimal") == 1234.0

    def test_currency_symbol_stripped(self) -> None:
        assert parse_amount_str("$1,234.56", "us") == 1234.56
        assert parse_amount_str("€1.234,56", "european") == 1234.56
        assert parse_amount_str("¥1,234", "zero_decimal") == 1234.0

    def test_parentheses_as_negative(self) -> None:
        assert parse_amount_str("(42.50)", "us") == -42.50

    def test_dr_suffix(self) -> None:
        assert parse_amount_str("42.50 DR", "us") == -42.50

    def test_cr_suffix(self) -> None:
        assert parse_amount_str("42.50 CR", "us") == 42.50

    def test_negative_sign(self) -> None:
        assert parse_amount_str("-42.50", "us") == -42.50

    def test_empty_returns_none(self) -> None:
        assert parse_amount_str("", "us") is None
        assert parse_amount_str("  ", "us") is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_date_detection.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement date_detection.py**

```python
# src/moneybin/extractors/tabular/date_detection.py
"""Date format detection, DD/MM disambiguation, and number format detection.

Handles the nuances of international date and number conventions that
trip up every CSV importer. Uses positional value analysis for date
disambiguation and convention scoring for number format detection.
"""

import re
from datetime import datetime

_CURRENCY_SYMBOLS = re.compile(
    r"[$€£¥₩₹₽₺₫kr\s]|CHF|R\$|kr\b|SEK|NOK|DKK", re.IGNORECASE
)

# Candidate date formats in priority order
_DATE_FORMATS: list[str] = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d-%b-%Y",
    "%d-%B-%Y",
    "%b %d, %Y",
]

# Minimum year for reasonable financial dates
_MIN_YEAR = 1970
# Maximum year = current year + 1
_MAX_YEAR = datetime.now().year + 1


def detect_date_format(
    values: list[str | None],
) -> tuple[str | None, str]:
    """Detect the date format from sample values.

    Tries each candidate format and scores on parse rate and date range
    reasonableness. Handles DD/MM vs MM/DD disambiguation.

    Args:
        values: Sample date strings (may include None/empty).

    Returns:
        Tuple of (format string, confidence: "high" | "medium" | "low").
        Format is None if no candidate passes the threshold.
    """
    clean = [v.strip() for v in values if v and v.strip()]
    if not clean:
        return None, "low"

    # Score each candidate
    scores: list[tuple[str, float, float]] = []  # (fmt, parse_rate, range_score)
    for fmt in _DATE_FORMATS:
        parse_count = 0
        reasonable_count = 0
        for val in clean:
            try:
                dt = datetime.strptime(val, fmt)
                parse_count += 1
                if _MIN_YEAR <= dt.year <= _MAX_YEAR:
                    reasonable_count += 1
            except ValueError:
                continue
        parse_rate = parse_count / len(clean) if clean else 0
        range_score = reasonable_count / max(parse_count, 1)
        if parse_rate >= 0.9:
            scores.append((fmt, parse_rate, range_score))

    if not scores:
        return None, "low"

    # Check for DD/MM vs MM/DD ambiguity
    dd_mm_fmts = {"%d/%m/%Y"}
    mm_dd_fmts = {"%m/%d/%Y"}
    has_dd_mm = any(s[0] in dd_mm_fmts for s in scores)
    has_mm_dd = any(s[0] in mm_dd_fmts for s in scores)

    if has_dd_mm and has_mm_dd:
        resolved_fmt, confidence = _disambiguate_dd_mm(clean, scores)
        if resolved_fmt:
            return resolved_fmt, confidence

    # Pick the best by combined score (parse_rate * range_score)
    scores.sort(key=lambda s: s[1] * s[2], reverse=True)
    best_fmt, best_parse, best_range = scores[0]

    confidence = "high" if best_parse >= 0.95 and best_range >= 0.95 else "medium"
    return best_fmt, confidence


def _disambiguate_dd_mm(
    values: list[str],
    scores: list[tuple[str, float, float]],
) -> tuple[str | None, str]:
    """Disambiguate DD/MM vs MM/DD using positional value analysis.

    Scans all values for numeric components >12 to determine which
    position holds the day vs month.

    Args:
        values: All date strings (not just sample).
        scores: Candidate scores to compare.

    Returns:
        Tuple of (resolved format, confidence). Returns (None, "") if
        unresolvable (caller falls back to best score).
    """
    sep_pattern = re.compile(r"[/\-.]")
    pos1_max = 0
    pos2_max = 0

    for val in values:
        parts = sep_pattern.split(val)
        if len(parts) >= 2:
            try:
                p1 = int(parts[0])
                p2 = int(parts[1])
                pos1_max = max(pos1_max, p1)
                pos2_max = max(pos2_max, p2)
            except ValueError:
                continue

    if pos1_max > 12 and pos2_max <= 12:
        return "%d/%m/%Y", "high"
    if pos2_max > 12 and pos1_max <= 12:
        return "%m/%d/%Y", "high"
    if pos1_max > 12 and pos2_max > 12:
        # Both have >12 — mixed formats, flag as low confidence
        return None, "low"

    # Neither exceeds 12 — use range reasonableness from scores
    dd_mm_score = next(
        (s[1] * s[2] for s in scores if s[0] == "%d/%m/%Y"), 0
    )
    mm_dd_score = next(
        (s[1] * s[2] for s in scores if s[0] == "%m/%d/%Y"), 0
    )
    if mm_dd_score > dd_mm_score:
        return "%m/%d/%Y", "medium"
    if dd_mm_score > mm_dd_score:
        return "%d/%m/%Y", "medium"
    # Truly ambiguous — default to MM/DD (US convention)
    return "%m/%d/%Y", "medium"


def detect_number_format(values: list[str | None]) -> str:
    """Detect the number format convention from sample values.

    Args:
        values: Sample amount strings.

    Returns:
        One of: "us", "european", "swiss_french", "zero_decimal".
    """
    clean = [v.strip() for v in values if v and v.strip()]
    if not clean:
        return "us"

    # Strip currency symbols for analysis
    stripped = [_CURRENCY_SYMBOLS.sub("", v).strip() for v in clean]
    stripped = [v.lstrip("-").strip("()").strip() for v in stripped]

    # Score each convention
    convention_scores: dict[str, int] = {
        "us": 0,
        "european": 0,
        "swiss_french": 0,
        "zero_decimal": 0,
    }

    for val in stripped:
        if not val:
            continue
        has_period = "." in val
        has_comma = "," in val
        has_space = " " in val

        if has_period and has_comma:
            # Determine which is the decimal separator
            last_period = val.rfind(".")
            last_comma = val.rfind(",")
            if last_period > last_comma:
                convention_scores["us"] += 1  # 1,234.56
            else:
                convention_scores["european"] += 1  # 1.234,56

        elif has_space and has_comma:
            convention_scores["swiss_french"] += 1  # 1 234,56

        elif has_period and not has_comma:
            # Could be US decimal or European thousands
            # Check if exactly 2 digits after the period
            after_period = val[val.rfind(".") + 1 :]
            if len(after_period) <= 3 and after_period.isdigit():
                convention_scores["us"] += 1
            else:
                convention_scores["european"] += 1

        elif has_comma and not has_period:
            # Could be US thousands or European decimal
            after_comma = val[val.rfind(",") + 1 :]
            if len(after_comma) == 2 and after_comma.isdigit():
                convention_scores["european"] += 1
            elif len(after_comma) == 3 and after_comma.isdigit():
                convention_scores["zero_decimal"] += 1
            else:
                convention_scores["us"] += 1

        else:
            # Plain integer — compatible with zero_decimal and us
            convention_scores["us"] += 1

    # Pick winner; tie goes to US
    best = max(convention_scores, key=lambda k: convention_scores[k])
    if convention_scores[best] == 0:
        return "us"
    return best


def parse_amount_str(value: str, number_format: str) -> float | None:
    """Parse an amount string using the specified number format convention.

    Handles currency symbols, parentheses-as-negative, DR/CR suffixes.

    Args:
        value: Raw amount string.
        number_format: Convention: us, european, swiss_french, zero_decimal.

    Returns:
        Parsed float, or None if the string is empty/unparseable.
    """
    if not value or not value.strip():
        return None

    s = value.strip()

    # Detect sign modifiers before stripping
    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1].strip()
    if s.startswith("-"):
        is_negative = True
        s = s[1:].strip()

    # DR/CR suffix
    s_upper = s.upper().rstrip()
    if s_upper.endswith(" DR"):
        is_negative = True
        s = s[:-3].strip()
    elif s_upper.endswith(" CR"):
        is_negative = False
        s = s[:-3].strip()

    # Strip currency symbols
    s = _CURRENCY_SYMBOLS.sub("", s).strip()

    if not s:
        return None

    # Apply number format convention
    if number_format == "european":
        s = s.replace(".", "").replace(",", ".")
    elif number_format == "swiss_french":
        s = s.replace(" ", "").replace(",", ".")
    elif number_format == "zero_decimal":
        s = s.replace(",", "")
    else:  # us
        s = s.replace(",", "")

    try:
        result = float(s)
        return -result if is_negative else result
    except ValueError:
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_date_detection.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/date_detection.py tests/moneybin/test_extractors/test_tabular/test_date_detection.py
git commit -m "feat: add date format detection, DD/MM disambiguation, and number format detection"
```

---

## Phase 6: Sign Convention Inference

### Task 13: Implement sign convention detection

**Files:**
- Create: `src/moneybin/extractors/tabular/sign_convention.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_sign_convention.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_sign_convention.py
"""Tests for sign convention inference."""

import polars as pl
import pytest

from moneybin.extractors.tabular.sign_convention import (
    SignConventionResult,
    infer_sign_convention,
)


class TestInferSignConvention:
    def test_negative_is_expense(self) -> None:
        """Mixed positive/negative values in single column → negative_is_expense."""
        result = infer_sign_convention(
            amount_values=["-42.50", "100.00", "-8.99", "1250.00"],
            debit_values=None,
            credit_values=None,
        )
        assert result.convention == "negative_is_expense"
        assert result.needs_confirmation is False

    def test_all_positive_flagged(self) -> None:
        """All-positive single column → flagged for confirmation."""
        result = infer_sign_convention(
            amount_values=["42.50", "100.00", "8.99"],
            debit_values=None,
            credit_values=None,
        )
        assert result.convention == "negative_is_expense"
        assert result.needs_confirmation is True

    def test_split_debit_credit(self) -> None:
        """When debit/credit columns have exclusive non-null values."""
        result = infer_sign_convention(
            amount_values=None,
            debit_values=["42.50", None, "8.99"],
            credit_values=[None, "100.00", None],
        )
        assert result.convention == "split_debit_credit"
        assert result.needs_confirmation is False

    def test_negative_is_income(self) -> None:
        """Credit card statement convention where negative = payment/credit."""
        result = infer_sign_convention(
            amount_values=["42.50", "8.99", "-500.00"],
            debit_values=None,
            credit_values=None,
            header_context="credit",
        )
        assert result.convention == "negative_is_income"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_sign_convention.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement sign_convention.py**

```python
# src/moneybin/extractors/tabular/sign_convention.py
"""Sign convention inference for amount columns.

Determines how the source represents expenses vs income:
- negative_is_expense: negative = expense, positive = income (MoneyBin native)
- negative_is_income: inverted (credit card statements)
- split_debit_credit: separate debit and credit columns
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SignConventionResult:
    """Result of sign convention inference."""

    convention: str
    """One of: negative_is_expense, negative_is_income, split_debit_credit."""

    needs_confirmation: bool = False
    """True if the convention is ambiguous and needs user confirmation."""

    reason: str = ""
    """Human-readable explanation of the inference."""


def infer_sign_convention(
    amount_values: list[str | None] | None,
    debit_values: list[str | None] | None,
    credit_values: list[str | None] | None,
    *,
    header_context: str = "",
) -> SignConventionResult:
    """Infer the sign convention from sample values.

    Args:
        amount_values: Values from a single amount column (if present).
        debit_values: Values from a debit column (if present).
        credit_values: Values from a credit column (if present).
        header_context: Lowercase header text for context clues
            (e.g., "credit" suggests credit card statement).

    Returns:
        SignConventionResult with the inferred convention.
    """
    # Split debit/credit columns
    if debit_values is not None and credit_values is not None:
        return SignConventionResult(
            convention="split_debit_credit",
            reason="Separate debit and credit columns detected",
        )

    if amount_values is None:
        return SignConventionResult(
            convention="negative_is_expense",
            needs_confirmation=True,
            reason="No amount column provided",
        )

    # Analyze the single amount column
    clean = [v.strip() for v in amount_values if v and v.strip()]
    if not clean:
        return SignConventionResult(
            convention="negative_is_expense",
            needs_confirmation=True,
            reason="No non-empty amount values",
        )

    has_negative = any(
        v.startswith("-") or (v.startswith("(") and v.endswith(")"))
        for v in clean
    )
    has_positive = any(
        not v.startswith("-") and not (v.startswith("(") and v.endswith(")"))
        for v in clean
    )

    if not has_negative:
        # All positive — ambiguous
        return SignConventionResult(
            convention="negative_is_expense",
            needs_confirmation=True,
            reason="All amounts are positive — sign convention is ambiguous",
        )

    # Check for credit card convention (negative = income/payment)
    if "credit" in header_context.lower():
        return SignConventionResult(
            convention="negative_is_income",
            reason="Credit card context detected — negative values are payments/credits",
        )

    return SignConventionResult(
        convention="negative_is_expense",
        reason="Mixed positive/negative values — standard convention",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_sign_convention.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/sign_convention.py tests/moneybin/test_extractors/test_tabular/test_sign_convention.py
git commit -m "feat: add sign convention inference (negative-is-expense, split debit/credit, credit card)"
```

---

## Phase 7: Column Mapping Engine (Stage 3)

The core intelligence — header matching, content validation, format lookup, and confidence tiers.

### Task 14: Implement column mapping engine

**Files:**
- Create: `src/moneybin/extractors/tabular/column_mapper.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_column_mapper.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_column_mapper.py
"""Tests for the column mapping engine (Stage 3)."""

import polars as pl
import pytest

from moneybin.extractors.tabular.column_mapper import (
    MappingResult,
    map_columns,
)


def _make_df(columns: dict[str, list[str]]) -> pl.DataFrame:
    """Helper to create a DataFrame from string columns."""
    return pl.DataFrame(columns)


class TestMapColumnsHighConfidence:
    def test_standard_headers(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        result = map_columns(df)
        assert result.confidence == "high"
        assert result.field_mapping["transaction_date"] == "Transaction Date"
        assert result.field_mapping["amount"] == "Amount"
        assert result.field_mapping["description"] == "Description"

    def test_chase_like_headers(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026"],
            "Post Date": ["01/16/2026"],
            "Description": ["KROGER"],
            "Category": ["Groceries"],
            "Type": ["Sale"],
            "Amount": ["-42.50"],
            "Memo": [""],
        })
        result = map_columns(df)
        assert result.confidence == "high"
        assert result.field_mapping["post_date"] == "Post Date"
        assert result.field_mapping["category"] == "Category"

    def test_debit_credit_columns(self) -> None:
        df = _make_df({
            "Date": ["01/15/2026"],
            "Description": ["Payment"],
            "Debit": ["42.50"],
            "Credit": [""],
        })
        result = map_columns(df)
        assert result.field_mapping["debit_amount"] == "Debit"
        assert result.field_mapping["credit_amount"] == "Credit"
        assert result.sign_convention == "split_debit_credit"


class TestMapColumnsMediumConfidence:
    def test_generic_headers_with_content_match(self) -> None:
        """Generic column names detected via content analysis."""
        df = _make_df({
            "Col1": ["01/15/2026", "02/20/2026"],
            "Col2": ["KROGER #1234", "WALMART"],
            "Col3": ["Groceries", "Groceries"],
            "Col4": ["-42.50", "100.00"],
        })
        result = map_columns(df)
        assert result.confidence in ("medium", "high")
        # Should detect Col1 as date, Col4 as amount, Col2 as description
        assert "transaction_date" in result.field_mapping
        assert "amount" in result.field_mapping
        assert "description" in result.field_mapping


class TestMapColumnsLowConfidence:
    def test_no_date_column(self) -> None:
        """No column parses as dates → low confidence."""
        df = _make_df({
            "Name": ["Alice", "Bob"],
            "Score": ["95", "87"],
            "Grade": ["A", "B"],
        })
        result = map_columns(df)
        assert result.confidence == "low"


class TestMultiAccountDetection:
    def test_account_column_detected(self) -> None:
        df = _make_df({
            "Date": ["01/15/2026"],
            "Amount": ["-42.50"],
            "Description": ["KROGER"],
            "Account": ["Chase Checking"],
        })
        result = map_columns(df)
        assert result.is_multi_account is True
        assert result.field_mapping.get("account_name") == "Account"

    def test_no_account_column(self) -> None:
        df = _make_df({
            "Date": ["01/15/2026"],
            "Amount": ["-42.50"],
            "Description": ["KROGER"],
        })
        result = map_columns(df)
        assert result.is_multi_account is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_column_mapper.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement column_mapper.py**

```python
# src/moneybin/extractors/tabular/column_mapper.py
"""Stage 3: Column mapping engine.

Takes a DataFrame (headers + sample rows), produces a field mapping with
a confidence tier. This is the core intelligence of the smart importer.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime

import polars as pl

from moneybin.extractors.tabular.date_detection import (
    detect_date_format,
    detect_number_format,
)
from moneybin.extractors.tabular.field_aliases import (
    ACCOUNT_IDENTIFYING_FIELDS,
    REQUIRED_FIELDS,
    match_header_to_field,
    normalize_header,
)
from moneybin.extractors.tabular.sign_convention import (
    infer_sign_convention,
)

logger = logging.getLogger(__name__)

_SAMPLE_SIZE = 20


@dataclass
class MappingResult:
    """Result of column mapping (Stage 3 output)."""

    field_mapping: dict[str, str]
    """Destination field → source column name."""

    confidence: str
    """Confidence tier: high, medium, low."""

    date_format: str | None = None
    """Detected date format string."""

    number_format: str = "us"
    """Detected number format convention."""

    sign_convention: str = "negative_is_expense"
    """Detected sign convention."""

    sign_needs_confirmation: bool = False
    """True if sign convention is ambiguous."""

    is_multi_account: bool = False
    """True if account-identifying columns were detected."""

    unmapped_columns: list[str] = field(default_factory=list)
    """Source columns with no destination field match."""

    flagged_fields: list[str] = field(default_factory=list)
    """Fields matched with low confidence (content-only)."""

    sample_values: dict[str, list[str]] = field(default_factory=dict)
    """Sample values for each mapped field (for confirmation display)."""


def map_columns(
    df: pl.DataFrame,
    *,
    overrides: dict[str, str] | None = None,
) -> MappingResult:
    """Map source columns to destination fields.

    Executes Steps 2–7 of Stage 3:
    1. Header-to-destination matching via alias table
    2. Content validation on matched fields
    3. Fallback discovery for unclaimed columns
    4. Sign convention inference
    5. Multi-account detection
    6. Confidence tier assignment

    Args:
        df: Source DataFrame from Stage 2.
        overrides: Explicit field→column overrides from user.

    Returns:
        MappingResult with mapping, confidence, and metadata.
    """
    mapping: dict[str, str] = {}
    claimed: set[str] = set()
    flagged: list[str] = []
    sample_values: dict[str, list[str]] = {}

    # Apply overrides first
    if overrides:
        for dest_field, src_col in overrides.items():
            if src_col in df.columns:
                mapping[dest_field] = src_col
                claimed.add(src_col)

    # Step 2: Header matching
    for col in df.columns:
        if col in claimed:
            continue
        dest = match_header_to_field(col)
        if dest and dest not in mapping:
            mapping[dest] = col
            claimed.add(col)

    # Collect sample values for mapped fields
    for dest, src in mapping.items():
        vals = df[src].head(_SAMPLE_SIZE).cast(pl.Utf8).to_list()
        sample_values[dest] = [str(v) if v is not None else "" for v in vals]

    # Step 3: Content validation on date and amount fields
    date_format = None
    if "transaction_date" in mapping:
        date_vals = sample_values.get("transaction_date", [])
        date_format, date_confidence = detect_date_format(date_vals)
        if date_format is None:
            # Header matched but content doesn't look like dates — demote
            flagged.append("transaction_date")

    # Detect number format from amount values
    amount_vals = sample_values.get(
        "amount",
        sample_values.get("debit_amount", []),
    )
    number_format = detect_number_format(amount_vals) if amount_vals else "us"

    # Step 4: Fallback discovery for required fields not yet mapped
    for req_field in REQUIRED_FIELDS:
        if req_field not in mapping:
            candidate = _discover_by_content(df, req_field, claimed)
            if candidate:
                mapping[req_field] = candidate
                claimed.add(candidate)
                flagged.append(req_field)
                # Get sample values
                vals = df[candidate].head(_SAMPLE_SIZE).cast(pl.Utf8).to_list()
                sample_values[req_field] = [
                    str(v) if v is not None else "" for v in vals
                ]
                # Detect date format if this is the date field
                if req_field == "transaction_date" and date_format is None:
                    date_format, _ = detect_date_format(
                        sample_values[req_field]
                    )

    # Handle amount as debit+credit if no single amount column
    if "amount" not in mapping and (
        "debit_amount" in mapping or "credit_amount" in mapping
    ):
        pass  # Split debit/credit handled in sign convention

    # Step 5: Sign convention inference
    sign_result = infer_sign_convention(
        amount_values=sample_values.get("amount"),
        debit_values=sample_values.get("debit_amount"),
        credit_values=sample_values.get("credit_amount"),
    )

    # Step 6: Multi-account detection
    is_multi_account = bool(
        set(mapping.keys()) & ACCOUNT_IDENTIFYING_FIELDS
    )

    # Step 7: Confidence tier
    unmapped = [c for c in df.columns if c not in claimed]
    confidence = _assign_confidence(mapping, flagged, date_format)

    return MappingResult(
        field_mapping=mapping,
        confidence=confidence,
        date_format=date_format,
        number_format=number_format,
        sign_convention=sign_result.convention,
        sign_needs_confirmation=sign_result.needs_confirmation,
        is_multi_account=is_multi_account,
        unmapped_columns=unmapped,
        flagged_fields=flagged,
        sample_values=sample_values,
    )


def _discover_by_content(
    df: pl.DataFrame,
    target_field: str,
    claimed: set[str],
) -> str | None:
    """Discover a destination field from column content analysis.

    Used for columns with non-descriptive headers (Col1, Col2, etc.).

    Args:
        df: Source DataFrame.
        target_field: Destination field to find.
        claimed: Already-claimed column names.

    Returns:
        Best candidate column name, or None.
    """
    candidates: list[tuple[str, float]] = []

    for col in df.columns:
        if col in claimed:
            continue
        vals = df[col].head(_SAMPLE_SIZE).cast(pl.Utf8).to_list()
        clean = [v for v in vals if v is not None and v.strip()]
        if not clean:
            continue

        score = _score_column_for_field(clean, target_field)
        if score > 0:
            candidates.append((col, score))

    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]
    return None


def _score_column_for_field(values: list[str], field: str) -> float:
    """Score how well a column's content matches a target field type.

    Args:
        values: Non-empty string values from the column.
        field: Target destination field name.

    Returns:
        Score between 0.0 (no match) and 1.0 (perfect match).
    """
    if field == "transaction_date":
        # Check if values parse as dates
        date_fmt, confidence = detect_date_format(values)
        if date_fmt:
            return 0.9 if confidence == "high" else 0.6
        return 0.0

    if field == "amount":
        # Check if values are numeric
        numeric_count = sum(1 for v in values if _is_amount(v))
        ratio = numeric_count / len(values) if values else 0
        return ratio * 0.9 if ratio >= 0.8 else 0.0

    if field == "description":
        # Descriptions: high cardinality, mostly text, average length > 5
        unique_ratio = len(set(values)) / len(values) if values else 0
        avg_len = sum(len(v) for v in values) / len(values) if values else 0
        numeric_count = sum(1 for v in values if _is_amount(v))
        numeric_ratio = numeric_count / len(values) if values else 0
        if unique_ratio > 0.5 and avg_len > 5 and numeric_ratio < 0.3:
            return 0.7
        return 0.0

    return 0.0


def _is_amount(s: str) -> bool:
    """Check if a string looks like a financial amount."""
    s = s.strip().lstrip("-").strip("$€£¥").replace(",", "").strip("()")
    try:
        float(s)
        return True
    except ValueError:
        return False


def _assign_confidence(
    mapping: dict[str, str],
    flagged: list[str],
    date_format: str | None,
) -> str:
    """Assign confidence tier based on mapping quality.

    Args:
        mapping: Current field mapping.
        flagged: Fields matched with low confidence.
        date_format: Detected date format (None = no dates found).

    Returns:
        Confidence tier: high, medium, low.
    """
    has_date = "transaction_date" in mapping
    has_amount = "amount" in mapping or (
        "debit_amount" in mapping and "credit_amount" in mapping
    )
    has_description = "description" in mapping

    if not (has_date and has_amount and has_description):
        return "low"

    if flagged or date_format is None:
        return "medium"

    return "high"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_column_mapper.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/column_mapper.py tests/moneybin/test_extractors/test_tabular/test_column_mapper.py
git commit -m "feat: add Stage 3 column mapping engine (header matching, content validation, confidence tiers)"
```

---

## Phase 8: Transform & Validate (Stage 4)

Parse dates, normalize amounts, generate IDs, validate data.

### Task 15: Implement transform and validate

**Files:**
- Create: `src/moneybin/extractors/tabular/transforms.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_transforms.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_transforms.py
"""Tests for Stage 4 transform and validation."""

import polars as pl
import pytest

from moneybin.extractors.tabular.transforms import (
    TransformResult,
    transform_dataframe,
)


def _make_df(**columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(columns)


class TestTransformBasic:
    def test_basic_transform(self) -> None:
        df = _make_df(
            Date=["01/15/2026", "02/20/2026"],
            Amount=["-42.50", "100.00"],
            Description=["KROGER #1234", "DIRECT DEPOSIT"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={"transaction_date": "Date", "amount": "Amount", "description": "Description"},
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test-checking",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test_bank",
            import_id="test-import-123",
        )
        assert len(result.transactions) == 2
        assert result.transactions["amount"][0] == -42.50
        assert result.transactions["amount"][1] == 100.00
        assert result.transactions["description"][0] == "KROGER #1234"

    def test_original_values_preserved(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["-42.50"],
            Description=["Test"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={"transaction_date": "Date", "amount": "Amount", "description": "Description"},
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.transactions["original_amount"][0] == "-42.50"
        assert result.transactions["original_date_str"][0] == "01/15/2026"

    def test_row_numbers_assigned(self) -> None:
        df = _make_df(
            Date=["01/15/2026", "01/16/2026", "01/17/2026"],
            Amount=["-10", "-20", "-30"],
            Description=["A", "B", "C"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={"transaction_date": "Date", "amount": "Amount", "description": "Description"},
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.transactions["row_number"].to_list() == [1, 2, 3]

    def test_transaction_id_deterministic(self) -> None:
        """Same input produces same transaction_id."""
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["-42.50"],
            Description=["KROGER"],
        )
        kwargs = dict(
            field_mapping={"transaction_date": "Date", "amount": "Amount", "description": "Description"},
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        r1 = transform_dataframe(df=df, **kwargs)
        r2 = transform_dataframe(df=df, **kwargs)
        assert r1.transactions["transaction_id"][0] == r2.transactions["transaction_id"][0]

    def test_source_transaction_id_used_when_present(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["-42.50"],
            Description=["KROGER"],
            TxnID=["TXN90812"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "source_transaction_id": "TxnID",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.transactions["transaction_id"][0] == "test:TXN90812"


class TestSignConventionTransform:
    def test_negative_is_income_inverts(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["42.50"],
            Description=["PURCHASE"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={"transaction_date": "Date", "amount": "Amount", "description": "Description"},
            date_format="%m/%d/%Y",
            sign_convention="negative_is_income",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        # positive in source + negative_is_income → expense → negative
        assert result.transactions["amount"][0] == -42.50

    def test_split_debit_credit(self) -> None:
        df = _make_df(
            Date=["01/15/2026", "01/16/2026"],
            Debit=["42.50", ""],
            Credit=["", "100.00"],
            Description=["KROGER", "DEPOSIT"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "debit_amount": "Debit",
                "credit_amount": "Credit",
                "description": "Description",
            },
            date_format="%m/%d/%Y",
            sign_convention="split_debit_credit",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.transactions["amount"][0] == -42.50  # debit = expense
        assert result.transactions["amount"][1] == 100.00  # credit = income
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_transforms.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement transforms.py**

```python
# src/moneybin/extractors/tabular/transforms.py
"""Stage 4: Transform and validate.

Applies the confirmed column mapping to produce the canonical raw schema
shape. Parses dates, normalizes amounts, generates transaction IDs, and
validates structural integrity.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime

import polars as pl

from moneybin.extractors.tabular.date_detection import parse_amount_str

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    """Output of the transform stage."""

    transactions: pl.DataFrame
    """Transformed transactions ready for raw.tabular_transactions."""

    rows_rejected: int = 0
    """Number of rows that failed validation."""

    rejection_details: list[dict[str, str]] = field(default_factory=list)
    """Per-rejected-row details: [{row_number, reason}]."""

    balance_validated: bool | None = None
    """Whether running balance validation passed (None if no balance)."""


def transform_dataframe(
    df: pl.DataFrame,
    *,
    field_mapping: dict[str, str],
    date_format: str,
    sign_convention: str,
    number_format: str,
    account_id: str,
    source_file: str,
    source_type: str,
    source_origin: str,
    import_id: str,
) -> TransformResult:
    """Transform a mapped DataFrame into the raw schema shape.

    Args:
        df: Source DataFrame from Stage 2.
        field_mapping: Destination field → source column name.
        date_format: strftime format string for date parsing.
        sign_convention: How amounts are represented.
        number_format: Number convention (us, european, etc.).
        account_id: Account identifier for all rows.
        source_file: Path to the source file.
        source_type: File type (csv, tsv, excel, etc.).
        source_origin: Format/institution identifier.
        import_id: UUID for this import batch.

    Returns:
        TransformResult with transformed DataFrame.
    """
    rows = len(df)
    rejection_details: list[dict[str, str]] = []

    # Assign 1-based row numbers
    row_numbers = list(range(1, rows + 1))

    # Extract and parse dates
    date_col = field_mapping.get("transaction_date")
    date_values = df[date_col].cast(pl.Utf8).to_list() if date_col else []
    original_date_strs = list(date_values)
    parsed_dates: list[str | None] = []
    for i, val in enumerate(date_values):
        if val and val.strip():
            try:
                dt = datetime.strptime(val.strip(), date_format)
                parsed_dates.append(dt.strftime("%Y-%m-%d"))
            except ValueError:
                parsed_dates.append(None)
                rejection_details.append({
                    "row_number": str(row_numbers[i]),
                    "reason": f"Date parse failed: '{val}'",
                })
        else:
            parsed_dates.append(None)
            rejection_details.append({
                "row_number": str(row_numbers[i]),
                "reason": "Missing date",
            })

    # Parse post_date if present
    post_date_col = field_mapping.get("post_date")
    post_dates: list[str | None] = [None] * rows
    if post_date_col and post_date_col in df.columns:
        post_vals = df[post_date_col].cast(pl.Utf8).to_list()
        for i, val in enumerate(post_vals):
            if val and val.strip():
                try:
                    dt = datetime.strptime(val.strip(), date_format)
                    post_dates[i] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

    # Extract and normalize amounts
    amounts, original_amounts = _extract_amounts(
        df, field_mapping, sign_convention, number_format, row_numbers, rejection_details
    )

    # Extract description
    desc_col = field_mapping.get("description")
    descriptions = (
        df[desc_col].cast(pl.Utf8).to_list()
        if desc_col and desc_col in df.columns
        else [None] * rows
    )

    # Generate transaction IDs
    source_txn_col = field_mapping.get("source_transaction_id")
    source_txn_ids: list[str | None] = [None] * rows
    if source_txn_col and source_txn_col in df.columns:
        source_txn_ids = df[source_txn_col].cast(pl.Utf8).to_list()

    transaction_ids = _generate_transaction_ids(
        parsed_dates, amounts, descriptions, account_id, row_numbers, source_txn_ids
    )

    # Extract optional fields
    def _get_col(field_name: str) -> list[str | None]:
        col = field_mapping.get(field_name)
        if col and col in df.columns:
            return df[col].cast(pl.Utf8).to_list()
        return [None] * rows

    # Filter valid rows (non-None date and amount)
    valid_mask = [
        parsed_dates[i] is not None and amounts[i] is not None
        for i in range(rows)
    ]
    rows_rejected = sum(1 for v in valid_mask if not v)

    # Build output DataFrame
    result_df = pl.DataFrame({
        "transaction_id": transaction_ids,
        "account_id": [account_id] * rows,
        "transaction_date": parsed_dates,
        "post_date": post_dates,
        "amount": amounts,
        "original_amount": original_amounts,
        "original_date_str": original_date_strs if date_values else [None] * rows,
        "description": descriptions,
        "memo": _get_col("memo"),
        "category": _get_col("category"),
        "subcategory": _get_col("subcategory"),
        "transaction_type": _get_col("transaction_type"),
        "status": _get_col("status"),
        "check_number": _get_col("check_number"),
        "source_transaction_id": source_txn_ids,
        "reference_number": _get_col("reference_number"),
        "balance": [None] * rows,  # Populated below if balance column present
        "currency": _get_col("currency"),
        "member_name": _get_col("member_name"),
        "source_file": [source_file] * rows,
        "source_type": [source_type] * rows,
        "source_origin": [source_origin] * rows,
        "import_id": [import_id] * rows,
        "row_number": row_numbers,
    })

    # Parse balance column if present
    balance_col = field_mapping.get("balance")
    if balance_col and balance_col in df.columns:
        balance_strs = df[balance_col].cast(pl.Utf8).to_list()
        balance_vals = [
            parse_amount_str(v, number_format) if v else None
            for v in balance_strs
        ]
        result_df = result_df.with_columns(
            pl.Series("balance", balance_vals, dtype=pl.Float64)
        )

    # Filter to valid rows only
    result_df = result_df.filter(pl.Series(valid_mask))

    # Cast types
    result_df = result_df.with_columns([
        pl.col("transaction_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("post_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col("amount").cast(pl.Decimal(precision=18, scale=2)),
        pl.col("balance").cast(pl.Decimal(precision=18, scale=2), strict=False),
        pl.col("row_number").cast(pl.Int32),
    ])

    return TransformResult(
        transactions=result_df,
        rows_rejected=rows_rejected,
        rejection_details=rejection_details,
    )


def _extract_amounts(
    df: pl.DataFrame,
    field_mapping: dict[str, str],
    sign_convention: str,
    number_format: str,
    row_numbers: list[int],
    rejection_details: list[dict[str, str]],
) -> tuple[list[float | None], list[str | None]]:
    """Extract and normalize amounts from the DataFrame.

    Returns:
        Tuple of (normalized amounts, original amount strings).
    """
    rows = len(df)

    if sign_convention == "split_debit_credit":
        debit_col = field_mapping.get("debit_amount")
        credit_col = field_mapping.get("credit_amount")
        debit_vals = (
            df[debit_col].cast(pl.Utf8).to_list()
            if debit_col and debit_col in df.columns
            else [None] * rows
        )
        credit_vals = (
            df[credit_col].cast(pl.Utf8).to_list()
            if credit_col and credit_col in df.columns
            else [None] * rows
        )

        amounts: list[float | None] = []
        originals: list[str | None] = []
        for i in range(rows):
            d = parse_amount_str(debit_vals[i] or "", number_format)
            c = parse_amount_str(credit_vals[i] or "", number_format)
            if d is not None and d != 0:
                amounts.append(-abs(d))
                originals.append(debit_vals[i])
            elif c is not None and c != 0:
                amounts.append(abs(c))
                originals.append(credit_vals[i])
            else:
                amounts.append(None)
                originals.append(None)
                rejection_details.append({
                    "row_number": str(row_numbers[i]),
                    "reason": "Both debit and credit are empty",
                })
        return amounts, originals

    # Single amount column
    amount_col = field_mapping.get("amount")
    if not amount_col or amount_col not in df.columns:
        return [None] * rows, [None] * rows

    raw_vals = df[amount_col].cast(pl.Utf8).to_list()
    amounts = []
    originals = list(raw_vals)

    for i, val in enumerate(raw_vals):
        parsed = parse_amount_str(val or "", number_format)
        if parsed is None and val and val.strip():
            rejection_details.append({
                "row_number": str(row_numbers[i]),
                "reason": f"Amount parse failed: '{val}'",
            })
        if parsed is not None and sign_convention == "negative_is_income":
            parsed = -parsed
        amounts.append(parsed)

    return amounts, originals


def _generate_transaction_ids(
    dates: list[str | None],
    amounts: list[float | None],
    descriptions: list[str | None],
    account_id: str,
    row_numbers: list[int],
    source_txn_ids: list[str | None],
) -> list[str]:
    """Generate deterministic transaction IDs.

    If source_transaction_id is available, uses account_id:source_transaction_id.
    Otherwise, SHA-256 hash of date|amount|description|account_id|row_number.

    Args:
        dates: Parsed date strings (YYYY-MM-DD).
        amounts: Parsed amounts.
        descriptions: Description strings.
        account_id: Account identifier.
        row_numbers: 1-based row numbers.
        source_txn_ids: Institution-assigned IDs (may be None).

    Returns:
        List of transaction ID strings.
    """
    ids: list[str] = []
    for i in range(len(dates)):
        if source_txn_ids[i] and source_txn_ids[i].strip():
            ids.append(f"{account_id}:{source_txn_ids[i].strip()}")
        else:
            raw = (
                f"{dates[i] or ''}|{amounts[i] or ''}|"
                f"{descriptions[i] or ''}|{account_id}|{row_numbers[i]}"
            )
            digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
            ids.append(digest)
    return ids
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_transforms.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/transforms.py tests/moneybin/test_extractors/test_tabular/test_transforms.py
git commit -m "feat: add Stage 4 transform and validate (date parsing, amount normalization, ID generation)"
```

---

## Phase 9: Loader (Stage 5)

Import batch tracking, raw table writes, format save, import reverting.

### Task 16: Implement tabular loader

**Files:**
- Create: `src/moneybin/loaders/tabular_loader.py`
- Test: `tests/moneybin/test_loaders/test_tabular_loader.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_loaders/test_tabular_loader.py
"""Tests for the tabular loader (Stage 5)."""

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from moneybin.loaders.tabular_loader import TabularLoader


@pytest.fixture()
def mock_db() -> MagicMock:
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.fetchall.return_value = []
    return db


class TestCreateImportBatch:
    def test_creates_import_log_entry(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        import_id = loader.create_import_batch(
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test_bank",
            account_names=["Test Checking"],
        )
        assert import_id  # Non-empty UUID
        assert len(import_id) == 36  # UUID format
        # Verify insert was called
        assert mock_db.execute.called

    def test_import_id_is_unique(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        id1 = loader.create_import_batch(
            source_file="/tmp/test1.csv",
            source_type="csv",
            source_origin="test_bank",
            account_names=["Test"],
        )
        id2 = loader.create_import_batch(
            source_file="/tmp/test2.csv",
            source_type="csv",
            source_origin="test_bank",
            account_names=["Test"],
        )
        assert id1 != id2


class TestFinalizeImportBatch:
    def test_finalize_sets_complete_status(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        loader.finalize_import_batch(
            import_id="test-123",
            rows_total=100,
            rows_imported=95,
            rows_rejected=5,
            rows_skipped_trailing=2,
            detection_confidence="high",
            number_format="us",
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            balance_validated=True,
        )
        # Verify update was called with correct status
        call_args = mock_db.execute.call_args
        assert "complete" in str(call_args) or "partial" in str(call_args)


class TestRevertImport:
    def test_revert_deletes_rows(self, mock_db: MagicMock) -> None:
        mock_db.execute.return_value.fetchone.return_value = ("test-123", "complete")
        loader = TabularLoader(mock_db)
        result = loader.revert_import("test-123")
        assert result["status"] == "reverted"

    def test_revert_already_reverted(self, mock_db: MagicMock) -> None:
        mock_db.execute.return_value.fetchone.return_value = ("test-123", "reverted")
        loader = TabularLoader(mock_db)
        result = loader.revert_import("test-123")
        assert result["status"] == "already_reverted"

    def test_revert_not_found(self, mock_db: MagicMock) -> None:
        mock_db.execute.return_value.fetchone.return_value = None
        loader = TabularLoader(mock_db)
        result = loader.revert_import("nonexistent")
        assert result["status"] == "not_found"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_loaders/test_tabular_loader.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement tabular_loader.py**

```python
# src/moneybin/loaders/tabular_loader.py
"""Stage 5: Tabular data loader.

Handles import batch tracking, raw table writes via Database.ingest_dataframe(),
format persistence, and import reverting.
"""

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

import polars as pl

from moneybin.database import Database

logger = logging.getLogger(__name__)


class TabularLoader:
    """Load tabular data into DuckDB raw tables with batch tracking."""

    def __init__(self, db: Database) -> None:
        """Initialize the tabular loader.

        Args:
            db: Database instance for all database operations.
        """
        self.db = db

    def create_import_batch(
        self,
        *,
        source_file: str,
        source_type: str,
        source_origin: str,
        account_names: list[str],
        format_name: str | None = None,
        format_source: str | None = None,
    ) -> str:
        """Create an import batch record in raw.import_log.

        Args:
            source_file: Absolute path to the imported file.
            source_type: File format (csv, tsv, excel, etc.).
            source_origin: Format/institution identifier.
            account_names: List of account names in this import.
            format_name: Matched or saved format name (if any).
            format_source: How the format was resolved.

        Returns:
            UUID import_id for this batch.
        """
        import_id = str(uuid.uuid4())
        self.db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                format_name, format_source, account_names, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'importing')
            """,
            [
                import_id,
                source_file,
                source_type,
                source_origin,
                format_name,
                format_source,
                json.dumps(account_names),
            ],
        )
        logger.info(f"Created import batch: {import_id[:8]}...")
        return import_id

    def load_transactions(self, df: pl.DataFrame) -> int:
        """Load transactions into raw.tabular_transactions.

        Args:
            df: Transformed transactions DataFrame.

        Returns:
            Number of rows loaded.
        """
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe(
            "raw.tabular_transactions", df, on_conflict="upsert"
        )
        logger.info(f"Loaded {len(df)} transactions")
        return len(df)

    def load_accounts(self, df: pl.DataFrame) -> int:
        """Load accounts into raw.tabular_accounts.

        Args:
            df: Accounts DataFrame.

        Returns:
            Number of rows loaded.
        """
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe(
            "raw.tabular_accounts", df, on_conflict="upsert"
        )
        logger.info(f"Loaded {len(df)} accounts")
        return len(df)

    def finalize_import_batch(
        self,
        *,
        import_id: str,
        rows_total: int,
        rows_imported: int,
        rows_rejected: int = 0,
        rows_skipped_trailing: int = 0,
        rejection_details: list[dict[str, str]] | None = None,
        detection_confidence: str | None = None,
        number_format: str | None = None,
        date_format: str | None = None,
        sign_convention: str | None = None,
        balance_validated: bool | None = None,
    ) -> None:
        """Finalize an import batch with results.

        Args:
            import_id: UUID of the import batch.
            rows_total: Total rows in source file.
            rows_imported: Rows successfully imported.
            rows_rejected: Rows that failed validation.
            rows_skipped_trailing: Trailing junk rows removed.
            rejection_details: Per-rejected-row details.
            detection_confidence: Confidence tier used.
            number_format: Number convention used.
            date_format: Date format string used.
            sign_convention: Sign convention applied.
            balance_validated: Whether balance validation passed.
        """
        status = "complete" if rows_rejected == 0 else "partial"
        self.db.execute(
            """
            UPDATE raw.import_log SET
                status = ?,
                rows_total = ?,
                rows_imported = ?,
                rows_rejected = ?,
                rows_skipped_trailing = ?,
                rejection_details = ?,
                detection_confidence = ?,
                number_format = ?,
                date_format = ?,
                sign_convention = ?,
                balance_validated = ?,
                completed_at = CURRENT_TIMESTAMP
            WHERE import_id = ?
            """,
            [
                status,
                rows_total,
                rows_imported,
                rows_rejected,
                rows_skipped_trailing,
                json.dumps(rejection_details) if rejection_details else None,
                detection_confidence,
                number_format,
                date_format,
                sign_convention,
                balance_validated,
                import_id,
            ],
        )
        logger.info(
            f"Import {import_id[:8]}... finalized: {status} "
            f"({rows_imported} imported, {rows_rejected} rejected)"
        )

    def revert_import(self, import_id: str) -> dict[str, str | int]:
        """Revert an import batch by deleting all its rows.

        Args:
            import_id: UUID of the import to revert.

        Returns:
            Dict with status and details.
        """
        # Check import exists and current status
        row = self.db.execute(
            "SELECT import_id, status FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()

        if row is None:
            return {"status": "not_found", "reason": f"No import with ID {import_id}"}

        if row[1] == "reverted":
            return {"status": "already_reverted"}

        # Delete transaction and account rows
        txn_count = self.db.execute(
            "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
            [import_id],
        ).fetchone()
        txn_deleted = txn_count[0] if txn_count else 0

        self.db.execute(
            "DELETE FROM raw.tabular_transactions WHERE import_id = ?",
            [import_id],
        )
        self.db.execute(
            "DELETE FROM raw.tabular_accounts WHERE import_id = ?",
            [import_id],
        )

        # Mark as reverted
        self.db.execute(
            """
            UPDATE raw.import_log SET
                status = 'reverted',
                reverted_at = CURRENT_TIMESTAMP
            WHERE import_id = ?
            """,
            [import_id],
        )

        logger.info(
            f"Reverted import {import_id[:8]}...: {txn_deleted} rows deleted"
        )
        return {"status": "reverted", "rows_deleted": txn_deleted}

    def get_import_history(
        self,
        *,
        limit: int = 20,
        import_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        """Query import history.

        Args:
            limit: Maximum number of records to return.
            import_id: Filter to a specific import ID.

        Returns:
            List of import log records.
        """
        if import_id:
            rows = self.db.execute(
                """
                SELECT import_id, source_file, source_type, source_origin,
                       format_name, status, rows_imported, rows_rejected,
                       detection_confidence, started_at, completed_at
                FROM raw.import_log
                WHERE import_id = ?
                """,
                [import_id],
            ).fetchall()
        else:
            rows = self.db.execute(
                """
                SELECT import_id, source_file, source_type, source_origin,
                       format_name, status, rows_imported, rows_rejected,
                       detection_confidence, started_at, completed_at
                FROM raw.import_log
                ORDER BY started_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()

        columns = [
            "import_id", "source_file", "source_type", "source_origin",
            "format_name", "status", "rows_imported", "rows_rejected",
            "detection_confidence", "started_at", "completed_at",
        ]
        return [dict(zip(columns, row)) for row in rows]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_loaders/test_tabular_loader.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/loaders/tabular_loader.py tests/moneybin/test_loaders/test_tabular_loader.py
git commit -m "feat: add tabular loader with import batch tracking, reverting, and history"
```

---

## Phase 10: Service Layer

Wire all pipeline stages together into the service layer that CLI and MCP share.

### Task 17: Update import service for tabular pipeline

**Files:**
- Modify: `src/moneybin/services/import_service.py`
- Test: `tests/moneybin/test_services/test_tabular_import_service.py`

- [ ] **Step 1: Write failing test for the tabular import path**

```python
# tests/moneybin/test_services/test_tabular_import_service.py
"""Tests for the tabular import service layer."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.services.import_service import import_file, ImportResult


class TestDetectFileType:
    """Test that tabular file extensions are detected correctly."""

    def test_csv_detected(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.csv")) == "tabular"

    def test_tsv_detected(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.tsv")) == "tabular"

    def test_xlsx_detected(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.xlsx")) == "tabular"

    def test_parquet_detected(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.parquet")) == "tabular"

    def test_feather_detected(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.feather")) == "tabular"

    def test_txt_detected(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.txt")) == "tabular"

    def test_ofx_still_works(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.ofx")) == "ofx"

    def test_pdf_still_works(self) -> None:
        from moneybin.services.import_service import _detect_file_type
        assert _detect_file_type(Path("test.pdf")) == "w2"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_services/test_tabular_import_service.py -v
```

Expected: Assertion failures — current `_detect_file_type` returns "csv" not "tabular".

- [ ] **Step 3: Update _detect_file_type in import_service.py**

Replace the `_detect_file_type` function in `src/moneybin/services/import_service.py`:

```python
def _detect_file_type(file_path: Path) -> str:
    """Detect file type from extension.

    Args:
        file_path: Path to the file.

    Returns:
        File type string: 'ofx', 'w2', or 'tabular'.

    Raises:
        ValueError: If extension is not recognized.
    """
    suffix = file_path.suffix.lower()
    if suffix in (".ofx", ".qfx"):
        return "ofx"
    if suffix == ".pdf":
        return "w2"
    if suffix in (
        ".csv", ".tsv", ".tab", ".txt", ".dat",
        ".xlsx", ".xls",
        ".parquet", ".pq",
        ".feather", ".arrow", ".ipc",
    ):
        return "tabular"
    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx, .csv, .tsv, .xlsx, .parquet, .feather, .pdf"
    )
```

- [ ] **Step 4: Add `_import_tabular()` function to import_service.py**

Add a new `_import_tabular()` function and update the `import_file()` dispatcher to call it instead of `_import_csv()`. This function orchestrates the full five-stage pipeline. The detailed implementation wires together all the modules built in Phases 3–9.

```python
def _import_tabular(
    db: Database,
    file_path: Path,
    *,
    account_name: str | None = None,
    account_id: str | None = None,
    format_name: str | None = None,
    overrides: dict[str, str] | None = None,
    save_format: bool = True,
    auto_confirm: bool = False,
    sheet: str | None = None,
    delimiter: str | None = None,
    encoding: str | None = None,
    skip_transform: bool = False,
    no_row_limit: bool = False,
    no_size_limit: bool = False,
) -> ImportResult:
    """Import a tabular file through the five-stage pipeline.

    Args:
        db: Database instance.
        file_path: Path to the file.
        account_name: Account name for single-account files.
        account_id: Explicit account ID (bypass matching).
        format_name: Explicit format name (bypass detection).
        overrides: Field→column overrides.
        save_format: Whether to auto-save detected format.
        auto_confirm: Whether to auto-confirm high-confidence detections.
        sheet: Excel sheet name.
        delimiter: Explicit delimiter.
        encoding: Explicit encoding.
        skip_transform: Skip SQLMesh transforms.
        no_row_limit: Override row count limit.
        no_size_limit: Override file size limit.

    Returns:
        ImportResult with summary.
    """
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.formats import (
        TabularFormat,
        load_builtin_formats,
    )
    from moneybin.extractors.tabular.readers import read_file
    from moneybin.extractors.tabular.transforms import transform_dataframe
    from moneybin.loaders.tabular_loader import TabularLoader

    result = ImportResult(file_path=str(file_path), file_type="tabular")

    # Stage 1: Format detection
    format_info = detect_format(
        file_path,
        format_override=None,
        delimiter_override=delimiter,
        encoding_override=encoding,
        no_size_limit=no_size_limit,
    )

    # Stage 2: Read file
    read_result = read_file(
        file_path,
        format_info,
        sheet=sheet,
        no_row_limit=no_row_limit,
    )
    df = read_result.df

    if len(df) == 0:
        raise ValueError(f"No data rows found in {file_path.name}")

    # Stage 3: Column mapping
    # Check for format match first
    matched_format: TabularFormat | None = None
    if format_name:
        # Explicit format specified
        builtin = load_builtin_formats()
        if format_name in builtin:
            matched_format = builtin[format_name]
        # TODO: also check DB formats when DB format loading is implemented
    else:
        # Try auto-detection against known formats
        builtin = load_builtin_formats()
        headers = list(df.columns)
        for fmt in builtin.values():
            if fmt.matches_headers(headers):
                matched_format = fmt
                break

    if matched_format:
        mapping_result_mapping = matched_format.field_mapping
        mapping_result_date_format = matched_format.date_format
        mapping_result_sign_convention = matched_format.sign_convention
        mapping_result_number_format = matched_format.number_format
        mapping_result_is_multi_account = matched_format.multi_account
        mapping_result_confidence = "high"
        format_source = "built-in"
    else:
        # Heuristic detection
        mapping_result = map_columns(df, overrides=overrides)
        mapping_result_mapping = mapping_result.field_mapping
        mapping_result_date_format = mapping_result.date_format or "%Y-%m-%d"
        mapping_result_sign_convention = mapping_result.sign_convention
        mapping_result_number_format = mapping_result.number_format
        mapping_result_is_multi_account = mapping_result.is_multi_account
        mapping_result_confidence = mapping_result.confidence
        format_source = "detected"

        if mapping_result.confidence == "low":
            raise ValueError(
                f"Could not reliably detect column mapping for "
                f"{file_path.name}. Use --override to specify columns manually."
            )

    # Determine account info
    source_type = format_info.file_type
    # Normalize xlsx → excel
    if source_type == "semicolon":
        source_type = "csv"

    if account_id:
        acct_id = account_id
    elif account_name:
        # Generate deterministic slug
        import re
        acct_id = re.sub(r"[^a-z0-9]+", "-", account_name.lower()).strip("-")
    elif mapping_result_is_multi_account:
        acct_id = "multi-account"  # Placeholder — per-row extraction below
    else:
        raise ValueError(
            "Single-account files require --account-name or --account-id"
        )

    source_origin = (
        matched_format.name
        if matched_format
        else (
            re.sub(r"[^a-z0-9]+", "-", (account_name or "unknown").lower()).strip("-")
            if account_name
            else "unknown"
        )
    )

    # Create import batch
    loader = TabularLoader(db)
    import_id = loader.create_import_batch(
        source_file=str(file_path),
        source_type=source_type,
        source_origin=source_origin,
        account_names=[account_name or acct_id],
        format_name=matched_format.name if matched_format else None,
        format_source=format_source,
    )

    # Stage 4: Transform
    try:
        transform_result = transform_dataframe(
            df=df,
            field_mapping=mapping_result_mapping,
            date_format=mapping_result_date_format,
            sign_convention=mapping_result_sign_convention,
            number_format=mapping_result_number_format,
            account_id=acct_id,
            source_file=str(file_path),
            source_type=source_type,
            source_origin=source_origin,
            import_id=import_id,
        )
    except Exception as e:
        # Mark import as failed
        loader.finalize_import_batch(
            import_id=import_id,
            rows_total=len(df),
            rows_imported=0,
            rows_rejected=len(df),
        )
        raise ValueError(f"Transform failed: {e}") from e

    # Stage 5: Load
    # Build account DataFrame
    import polars as pl
    account_df = pl.DataFrame({
        "account_id": [acct_id],
        "account_name": [account_name or acct_id],
        "account_number": [None],
        "account_number_masked": [None],
        "account_type": [None],
        "institution_name": [
            matched_format.institution_name if matched_format else None
        ],
        "currency": [None],
        "source_file": [str(file_path)],
        "source_type": [source_type],
        "source_origin": [source_origin],
        "import_id": [import_id],
    })

    rows_imported = loader.load_transactions(transform_result.transactions)
    loader.load_accounts(account_df)

    loader.finalize_import_batch(
        import_id=import_id,
        rows_total=len(df),
        rows_imported=rows_imported,
        rows_rejected=transform_result.rows_rejected,
        rows_skipped_trailing=read_result.rows_skipped_trailing,
        detection_confidence=mapping_result_confidence,
        number_format=mapping_result_number_format,
        date_format=mapping_result_date_format,
        sign_convention=mapping_result_sign_convention,
        balance_validated=transform_result.balance_validated,
    )

    result.accounts = 1
    result.transactions = rows_imported
    result.details = {"transactions": rows_imported, "accounts": 1}

    if rows_imported > 0:
        result.date_range = _query_date_range(
            db, "raw.tabular_transactions", "transaction_date", file_path
        )

    return result
```

Update the `import_file()` dispatcher to use `_import_tabular()`:

```python
    if file_type == "ofx":
        result = _import_ofx(db, path, institution=institution)
    elif file_type == "w2":
        result = _import_w2(db, path)
    elif file_type == "tabular":
        result = _import_tabular(
            db, path,
            account_name=account_name,
            account_id=account_id,
        )
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
```

Also add `account_name` parameter to `import_file()` signature.

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_services/test_tabular_import_service.py -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/services/import_service.py tests/moneybin/test_services/test_tabular_import_service.py
git commit -m "feat: wire tabular pipeline into import service (detect → read → map → transform → load)"
```

---

## Phase 11: SQLMesh Models

New staging views and updated core models.

### Task 18: Create staging views

**Files:**
- Create: `sqlmesh/models/prep/stg_tabular__transactions.sql`
- Create: `sqlmesh/models/prep/stg_tabular__accounts.sql`

- [ ] **Step 1: Create stg_tabular__transactions.sql**

```sql
MODEL (
  name prep.stg_tabular__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    transaction_id,
    account_id,
    transaction_date,
    post_date,
    amount,
    original_amount,
    original_date_str,
    TRIM(description) AS description,
    TRIM(memo) AS memo,
    category,
    subcategory,
    transaction_type,
    status,
    check_number,
    source_transaction_id,
    reference_number,
    balance,
    currency,
    member_name,
    source_file,
    source_type,
    source_origin,
    import_id,
    row_number,
    extracted_at,
    loaded_at,
    ROW_NUMBER() OVER (PARTITION BY transaction_id, account_id ORDER BY loaded_at DESC) AS _row_num
  FROM raw.tabular_transactions
)
SELECT
  transaction_id, -- Deterministic ID: source-provided or SHA-256 hash
  account_id, -- Source-system account identifier
  transaction_date, -- Parsed date from source
  post_date, -- Settlement date when available
  amount, -- Normalized: negative = expense, positive = income
  original_amount, -- Raw amount string for audit
  original_date_str, -- Raw date string for audit
  description, -- Trimmed transaction description
  memo, -- Trimmed supplementary details
  category, -- Source-provided category (for migration bootstrap)
  subcategory, -- Source-provided subcategory
  transaction_type, -- Source-provided type code
  status, -- Source-provided status
  check_number, -- Check number when applicable
  source_transaction_id, -- Institution-assigned unique ID
  reference_number, -- Institution reference number
  balance, -- Running balance after this transaction
  currency, -- ISO 4217 currency code
  member_name, -- Account holder name
  source_file, -- Path to source file
  source_type, -- Import pathway: csv, tsv, excel, parquet, feather, pipe
  source_origin, -- Institution/format that produced this data
  import_id, -- UUID linking to import batch
  row_number, -- 1-based source file row number
  extracted_at,
  loaded_at
FROM ranked
WHERE
  _row_num = 1
```

- [ ] **Step 2: Create stg_tabular__accounts.sql**

```sql
MODEL (
  name prep.stg_tabular__accounts,
  kind VIEW
);

SELECT
  account_id, -- Source-system account identifier
  account_name, -- Human-readable label
  account_number, -- Full account number (encrypted at rest)
  account_number_masked, -- Last 4 digits for display
  account_type, -- Account classification
  institution_name, -- Financial institution name
  currency, -- Default currency
  NULL::TEXT AS routing_number, -- Not available from tabular imports
  NULL::TEXT AS institution_fid, -- Not available from tabular imports
  source_file,
  source_type,
  source_origin,
  import_id,
  extracted_at,
  loaded_at
FROM raw.tabular_accounts
```

- [ ] **Step 3: Commit**

```bash
git add sqlmesh/models/prep/stg_tabular__transactions.sql sqlmesh/models/prep/stg_tabular__accounts.sql
git commit -m "feat: add SQLMesh staging views for tabular transactions and accounts"
```

### Task 19: Update core models (source_system → source_type)

**Files:**
- Modify: `sqlmesh/models/core/dim_accounts.sql`
- Modify: `sqlmesh/models/core/fct_transactions.sql`

- [ ] **Step 1: Update dim_accounts.sql**

Replace the `csv_accounts` CTE with `tabular_accounts` and rename `source_system` → `source_type`:

```sql
/* Canonical accounts dimension; deduplicated accounts from all sources, keeping the most recently extracted record per account_id */
MODEL (
  name core.dim_accounts,
  kind FULL,
  grain account_id
);

WITH ofx_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_org AS institution_name,
    institution_fid,
    'ofx' AS source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_ofx__accounts
), tabular_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_name,
    institution_fid,
    source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_tabular__accounts
), all_accounts AS (
  SELECT
    *
  FROM ofx_accounts
  UNION ALL
  SELECT
    *
  FROM tabular_accounts
), deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY extracted_at DESC) AS _row_num
  FROM all_accounts
)
SELECT
  account_id, -- Unique account identifier; stable across imports; foreign key in fct_transactions
  routing_number, -- ABA bank routing number; NULL when not provided by source
  account_type, -- Account classification from source, e.g. CHECKING, SAVINGS, CREDITLINE
  institution_name, -- Human-readable name of the financial institution
  institution_fid, -- OFX financial institution identifier; NULL for tabular sources
  source_type, -- Origin of the winning record after deduplication: ofx, csv, tsv, excel, etc.
  source_file, -- Path to the source file from which this record was loaded
  extracted_at, -- When the data was parsed from the source file
  loaded_at, -- When the record was written to the raw table
  CURRENT_TIMESTAMP AS updated_at -- When this core record was last refreshed by SQLMesh
FROM deduplicated
WHERE
  _row_num = 1
```

- [ ] **Step 2: Update fct_transactions.sql**

Replace the `csv_transactions` CTE with `tabular_transactions` and rename `source_system` → `source_type`. Key changes:

- CTE name: `csv_transactions` → `tabular_transactions`
- Source: `prep.stg_csv__transactions` → `prep.stg_tabular__transactions`
- Column: `'csv' AS source_system` → `source_type` (passes through from staging)
- Column in final SELECT: `source_system` → `source_type`
- OFX CTE: `'ofx' AS source_system` → `'ofx' AS source_type`

The full replacement follows the same structure as the existing file but with these substitutions throughout. Replace `source_system` with `source_type` in all CTEs, the `standardized` CTE, and the final SELECT with its comments.

- [ ] **Step 3: Format SQL models**

```bash
uv run sqlmesh -p sqlmesh format
```

- [ ] **Step 4: Commit**

```bash
git add sqlmesh/models/core/dim_accounts.sql sqlmesh/models/core/fct_transactions.sql
git commit -m "feat: update core models — tabular CTEs, source_system → source_type"
```

---

## Phase 12: CLI Commands

Update the import command with new options and add format management subcommands.

### Task 20: Update import file command

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py`

- [ ] **Step 1: Update the `import file` command with new options**

Add `--account-name`, `--yes`, `--override`, `--save-format`/`--no-save-format`, `--sheet`, `--delimiter`, `--encoding`, `--no-row-limit`, `--no-size-limit`, `--skip-transform`, `--format`, `--sign`, `--date-format`, `--number-format` options. Update the function body to call `import_file()` with the new tabular parameters.

- [ ] **Step 2: Add `import history` subcommand**

```python
@app.command("history")
def import_history(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
    import_id: str = typer.Option(None, "--import-id", help="Show details for a specific import"),
) -> None:
    """List recent imports with batch details."""
    # Query raw.import_log and display as table
```

- [ ] **Step 3: Add `import revert` subcommand**

```python
@app.command("revert")
def import_revert(
    import_id: str = typer.Argument(..., help="Import batch ID to revert"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Revert an import — deletes all rows from that batch."""
    # Call TabularLoader.revert_import()
```

- [ ] **Step 4: Add `import preview` subcommand**

```python
@app.command("preview")
def import_preview(
    file_path: str = typer.Argument(..., help="File to preview"),
) -> None:
    """Preview file structure without importing."""
    # Stage 1 + 2 + 3 (detect, read, map) without loading
```

- [ ] **Step 5: Add format management subcommands**

```python
@app.command("list-formats")
def list_formats() -> None:
    """List all formats (built-in + user-saved)."""

@app.command("show-format")
def show_format(name: str = typer.Argument(...)) -> None:
    """Show format details."""

@app.command("delete-format")
def delete_format(name: str = typer.Argument(...)) -> None:
    """Delete a user format."""
```

- [ ] **Step 6: Run linting and type checks**

```bash
uv run ruff check src/moneybin/cli/commands/import_cmd.py
uv run pyright src/moneybin/cli/commands/import_cmd.py
```

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py
git commit -m "feat: update CLI — new import options, history, revert, preview, format management"
```

### Task 21: Write CLI tests

**Files:**
- Create: `tests/moneybin/test_cli/test_import_cmd_tabular.py`

- [ ] **Step 1: Write CLI tests**

Test argument parsing, exit codes, and error messages (business logic tested in service tests). Focus on:
- `import file` with `--account-name` (required for single-account)
- `import file` with `--yes` (auto-confirm)
- `import file` missing required args (exit code 1)
- `import history` output format
- `import revert` with confirmation
- `import preview` output

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/moneybin/test_cli/test_import_cmd_tabular.py -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_cli/test_import_cmd_tabular.py
git commit -m "test: add CLI tests for tabular import commands"
```

---

## Phase 13: MCP Tools

Update MCP tools to use the new service layer.

### Task 22: Update MCP write tools

**Files:**
- Modify: `src/moneybin/mcp/write_tools.py`

- [ ] **Step 1: Update `import_file` tool**

Update the existing `import_file` tool to accept `account_name`, `format_name`, `overrides`, `save_format`, `auto_confirm`, `sheet`, `delimiter` parameters. Update the docstring to describe the new tabular import capabilities.

- [ ] **Step 2: Add `import_preview` tool**

```python
@mcp.tool()
def import_preview(file_path: str) -> str:
    """Preview a file's structure and detected column mapping without importing."""
```

- [ ] **Step 3: Add `import_history` tool**

```python
@mcp.tool()
def import_history(limit: int = 20, import_id: str | None = None) -> str:
    """List past imports with batch details."""
```

- [ ] **Step 4: Add `import_revert` tool**

```python
@mcp.tool()
def import_revert(import_id: str, auto_confirm: bool = False) -> str:
    """Undo an import batch — deletes all rows from that batch."""
```

- [ ] **Step 5: Add `list_formats` tool**

```python
@mcp.tool()
def list_formats() -> str:
    """List all available tabular formats (built-in + user-saved)."""
```

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/mcp/write_tools.py
git commit -m "feat: update MCP tools — tabular import, preview, history, revert, list-formats"
```

---

## Phase 14: Metrics

### Task 23: Add tabular import metrics

**Files:**
- Modify: `src/moneybin/metrics/registry.py`

- [ ] **Step 1: Add new metrics**

```python
# Tabular import metrics
TABULAR_FORMAT_MATCHES = Counter(
    "moneybin_tabular_format_matches_total",
    "Tabular format matches by format name and source",
    ["format_name", "format_source"],
)
TABULAR_DETECTION_CONFIDENCE = Counter(
    "moneybin_tabular_detection_confidence_total",
    "Column mapping detection confidence distribution",
    ["confidence"],
)
TABULAR_IMPORT_BATCHES = Counter(
    "moneybin_tabular_import_batches_total",
    "Import batch lifecycle events",
    ["status"],
)
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/metrics/registry.py
git commit -m "feat: add tabular import metrics (format matches, detection confidence, batch lifecycle)"
```

---

## Phase 15: Remove Old CSV System

After the new system is working end-to-end, remove the old CSV-specific files.

### Task 24: Remove old CSV extractor, profiles, loader, and schemas

**Files:**
- Remove: `src/moneybin/extractors/csv_extractor.py`
- Remove: `src/moneybin/extractors/csv_profiles.py`
- Remove: `src/moneybin/loaders/csv_loader.py`
- Remove: `src/moneybin/data/csv_profiles/` (directory)
- Remove: `src/moneybin/sql/schema/raw_csv_transactions.sql`
- Remove: `src/moneybin/sql/schema/raw_csv_accounts.sql`
- Remove: `sqlmesh/models/prep/stg_csv__transactions.sql`
- Remove: `sqlmesh/models/prep/stg_csv__accounts.sql`
- Remove: `tests/moneybin/test_extractors/test_csv_extractor.py`
- Remove: `tests/moneybin/test_extractors/test_csv_profiles.py`
- Modify: `src/moneybin/schema.py` — remove `raw_csv_accounts.sql`, `raw_csv_transactions.sql` from `_SCHEMA_FILES`
- Modify: `src/moneybin/tables.py` — remove `CSV_ACCOUNTS`, `CSV_TRANSACTIONS`

- [ ] **Step 1: Remove old files**

```bash
rm src/moneybin/extractors/csv_extractor.py
rm src/moneybin/extractors/csv_profiles.py
rm src/moneybin/loaders/csv_loader.py
rm -rf src/moneybin/data/csv_profiles/
rm src/moneybin/sql/schema/raw_csv_transactions.sql
rm src/moneybin/sql/schema/raw_csv_accounts.sql
rm sqlmesh/models/prep/stg_csv__transactions.sql
rm sqlmesh/models/prep/stg_csv__accounts.sql
rm tests/moneybin/test_extractors/test_csv_extractor.py
rm tests/moneybin/test_extractors/test_csv_profiles.py
```

- [ ] **Step 2: Update schema.py and tables.py**

Remove references to the old CSV files from `_SCHEMA_FILES` in `schema.py` and the old `CSV_ACCOUNTS` / `CSV_TRANSACTIONS` constants from `tables.py`.

- [ ] **Step 3: Clean up any remaining imports**

Search for imports of `csv_extractor`, `csv_profiles`, `csv_loader` across the codebase and remove or update them.

```bash
uv run ruff check . && uv run pyright src/moneybin/
```

- [ ] **Step 4: Run full test suite**

```bash
make test
```

Expected: All tests PASS with no references to old CSV system.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: remove old CSV import system (replaced by tabular importer)"
```

---

## Phase 16: Integration Testing & Fixtures

### Task 25: Create core test fixtures

**Files:**
- Create: `tests/fixtures/tabular/` directory with fixture files

- [ ] **Step 1: Create fixture directory and core format files**

Create the 7 core format fixtures (standard CSV, TSV, pipe, semicolon, Excel, Parquet, Feather) plus the 6 built-in format fixtures (Chase, Citi, Tiller, Mint, YNAB, Maybe). Each fixture is 5–10 rows of realistic data.

- [ ] **Step 2: Create edge case fixtures**

Prioritize the highest-value fixtures from the spec: preamble rows, trailing totals, BOM, Latin-1 encoding, debit/credit split, DD/MM dates, European amounts, all-positive amounts, generic headers.

- [ ] **Step 3: Write end-to-end integration test**

```python
# tests/moneybin/test_integration/test_tabular_e2e.py
"""End-to-end integration test for the tabular import pipeline."""

@pytest.mark.integration
class TestTabularEndToEnd:
    def test_csv_import_roundtrip(self, tmp_path, mock_db):
        """File → detect → read → map → transform → load → verify."""

    def test_format_save_and_reuse(self, tmp_path, mock_db):
        """Import unknown file → format saved → re-import → format matched."""

    def test_import_and_revert(self, tmp_path, mock_db):
        """Import → verify rows → revert → verify rows deleted."""
```

- [ ] **Step 4: Run integration tests**

```bash
uv run pytest tests/moneybin/test_integration/test_tabular_e2e.py -v -m integration
```

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/tabular/ tests/moneybin/test_integration/
git commit -m "test: add tabular import fixtures and end-to-end integration tests"
```

---

## Phase 17: Documentation & Spec Updates

### Task 26: Update spec status and README

**Files:**
- Modify: `docs/specs/smart-import-tabular.md` (status → `in-progress`)
- Modify: `docs/specs/INDEX.md` (update status)
- Modify: `README.md` (update roadmap)

- [ ] **Step 1: Update spec status to in-progress**

Change `ready` to `in-progress` in `smart-import-tabular.md` line 8.

- [ ] **Step 2: Update INDEX.md**

Update the smart-import-tabular entry status to `in-progress`.

- [ ] **Step 3: Update README roadmap**

Change the smart tabular import icon from 📐 to 🗓️ (or whatever the current icon is) to reflect active development.

- [ ] **Step 4: Commit**

```bash
git add docs/specs/smart-import-tabular.md docs/specs/INDEX.md README.md
git commit -m "docs: mark smart tabular import as in-progress"
```

---

## Phase 18: Pre-Commit Quality

### Task 27: Format, lint, type-check, and full test suite

**Files:**
- All modified files

- [ ] **Step 1: Format and lint**

```bash
make format && make lint
```

- [ ] **Step 2: Type check**

```bash
uv run pyright src/moneybin/extractors/tabular/ src/moneybin/loaders/tabular_loader.py
```

- [ ] **Step 3: Full test suite**

```bash
make test
```

- [ ] **Step 4: Format SQL models**

```bash
uv run sqlmesh -p sqlmesh format
```

- [ ] **Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "chore: format, lint, and type-check fixes"
```

---

## Phase 19: Running Balance Validation (Spec Req 30)

### Task 28: Implement running balance validation

**Files:**
- Modify: `src/moneybin/extractors/tabular/transforms.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_transforms.py`

The spec requires that when a `balance` column is present, the transform stage validates internal consistency by checking sequential balance deltas: `balance[n] - balance[n-1]` should equal `amount[n]`. This is a high-confidence signal that catches sign convention errors.

- [ ] **Step 1: Write failing test for balance validation**

Add to `test_transforms.py`:

```python
class TestRunningBalanceValidation:
    def test_balance_validates_amounts(self) -> None:
        """Sequential balance deltas match amounts → balance_validated=True."""
        df = _make_df(
            Date=["01/15/2026", "01/16/2026", "01/17/2026"],
            Amount=["-42.50", "100.00", "-10.00"],
            Description=["A", "B", "C"],
            Balance=["957.50", "1057.50", "1047.50"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "balance": "Balance",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.balance_validated is True

    def test_balance_detects_wrong_sign(self) -> None:
        """Balance validates after sign inversion → auto-correct sign convention."""
        df = _make_df(
            Date=["01/15/2026", "01/16/2026"],
            Amount=["42.50", "-100.00"],
            Description=["A", "B"],
            Balance=["957.50", "1057.50"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "balance": "Balance",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        # Should auto-correct the sign and validate
        assert result.balance_validated is True

    def test_balance_inconsistent_warns(self) -> None:
        """Balance doesn't match in either direction → balance_validated=False."""
        df = _make_df(
            Date=["01/15/2026", "01/16/2026"],
            Amount=["-42.50", "100.00"],
            Description=["A", "B"],
            Balance=["500.00", "999.99"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "balance": "Balance",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.balance_validated is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_transforms.py::TestRunningBalanceValidation -v
```

- [ ] **Step 3: Implement balance validation in transforms.py**

Add a `_validate_running_balance()` function that:
1. Parses the balance column values
2. Computes `balance[n] - balance[n-1]` for consecutive pairs
3. Compares each delta to `amount[n]` within a tolerance of ±0.01
4. If ≥90% pass → `balance_validated = True`
5. If <90% pass but ≥90% pass after inverting amounts → auto-correct sign, `balance_validated = True`
6. If neither → `balance_validated = False`, log warning

Call this function at the end of `transform_dataframe()` when a balance column is mapped.

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_transforms.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/extractors/tabular/transforms.py tests/moneybin/test_extractors/test_tabular/test_transforms.py
git commit -m "feat: add running balance validation (sequential delta check, auto-correct sign convention)"
```

---

## Phase 20: Format DB Persistence (Spec Reqs 10, 12)

### Task 29: Implement format save/load from database

**Files:**
- Modify: `src/moneybin/extractors/tabular/formats.py`
- Modify: `src/moneybin/loaders/tabular_loader.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_formats.py`

The spec requires formats be saved to `app.tabular_formats` (DB) and loaded from DB with user formats overriding built-ins.

- [ ] **Step 1: Write failing tests**

Add to `test_formats.py`:

```python
class TestFormatDBOperations:
    def test_save_format_to_db(self, mock_db: MagicMock) -> None:
        from moneybin.extractors.tabular.formats import save_format_to_db
        fmt = TabularFormat(
            name="test_bank",
            institution_name="Test Bank",
            header_signature=["Date", "Amount"],
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            sign_convention="negative_is_expense",
            date_format="%m/%d/%Y",
        )
        save_format_to_db(mock_db, fmt)
        assert mock_db.execute.called

    def test_load_formats_from_db(self, mock_db: MagicMock) -> None:
        from moneybin.extractors.tabular.formats import load_formats_from_db
        mock_db.execute.return_value.fetchall.return_value = []
        formats = load_formats_from_db(mock_db)
        assert isinstance(formats, dict)

    def test_user_format_overrides_builtin(self) -> None:
        from moneybin.extractors.tabular.formats import (
            merge_formats,
            load_builtin_formats,
        )
        builtins = load_builtin_formats()
        user = {"chase_credit": TabularFormat(
            name="chase_credit",
            institution_name="Chase (custom)",
            header_signature=["Custom Date", "Custom Amount"],
            field_mapping={"transaction_date": "Custom Date", "amount": "Custom Amount"},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )}
        merged = merge_formats(builtins, user)
        assert merged["chase_credit"].institution_name == "Chase (custom)"
```

- [ ] **Step 2: Implement DB save/load functions in formats.py**

Add `save_format_to_db(db, format)`, `load_formats_from_db(db)`, and `merge_formats(builtins, user_formats)` functions.

`save_format_to_db`: INSERT OR REPLACE into `app.tabular_formats` using parameterized SQL. Serialize `header_signature`, `field_mapping`, `skip_trailing_patterns` as JSON.

`load_formats_from_db`: SELECT all from `app.tabular_formats`, deserialize JSON columns, return dict of `TabularFormat` instances.

`merge_formats`: Combine built-in and user formats, with user formats overriding built-ins of the same name.

- [ ] **Step 3: Update tabular_loader.py to save format after successful import**

Add a `save_detected_format()` method to `TabularLoader` that calls `save_format_to_db()` when `save_format=True`.

- [ ] **Step 4: Update service layer to load from DB + built-in**

In `_import_tabular()` in `import_service.py`, replace the YAML-only format loading with `merge_formats(load_builtin_formats(), load_formats_from_db(db))`.

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_formats.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/extractors/tabular/formats.py src/moneybin/loaders/tabular_loader.py src/moneybin/services/import_service.py tests/moneybin/test_extractors/test_tabular/test_formats.py
git commit -m "feat: add format DB persistence (save/load/merge, user overrides built-in)"
```

---

## Phase 21: Account Matching (Spec Req 15)

### Task 30: Implement account matching with fuzzy names

**Files:**
- Create: `src/moneybin/extractors/tabular/account_matching.py`
- Test: `tests/moneybin/test_extractors/test_tabular/test_account_matching.py`

The spec requires cross-source account matching by account number (strongest), name with fuzzy matching, or explicit `--account-id` bypass.

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_extractors/test_tabular/test_account_matching.py
"""Tests for account matching across source types."""

from unittest.mock import MagicMock

import pytest

from moneybin.extractors.tabular.account_matching import (
    AccountMatch,
    match_account,
)


class TestMatchAccount:
    def test_exact_slug_match(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking"},
        ]
        result = match_account("Chase Checking", existing_accounts=existing)
        assert result.matched is True
        assert result.account_id == "chase-checking"

    def test_fuzzy_match_candidates(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking"},
            {"account_id": "chase-credit", "account_name": "Chase Credit Card"},
        ]
        result = match_account("Chase Check", existing_accounts=existing)
        assert result.matched is False
        assert len(result.candidates) > 0
        assert result.candidates[0]["account_name"] == "Chase Checking"

    def test_account_number_match(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking",
             "account_number": "1234567890"},
        ]
        result = match_account(
            "Chase Checking",
            account_number="1234567890",
            existing_accounts=existing,
        )
        assert result.matched is True
        assert result.account_id == "chase-checking"

    def test_no_match_returns_new(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking"},
        ]
        result = match_account("Ally Savings", existing_accounts=existing)
        assert result.matched is False
        assert len(result.candidates) == 0

    def test_explicit_account_id_bypasses_matching(self) -> None:
        result = match_account(
            "Anything",
            explicit_account_id="my-custom-id",
            existing_accounts=[],
        )
        assert result.matched is True
        assert result.account_id == "my-custom-id"
```

- [ ] **Step 2: Implement account_matching.py**

```python
# src/moneybin/extractors/tabular/account_matching.py
"""Account matching across source types.

Matches imported accounts against the full account registry using:
1. Account number (strongest signal)
2. Exact slug match on account name
3. Fuzzy name matching (difflib.SequenceMatcher)
4. Explicit --account-id bypass
"""

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher


@dataclass
class AccountMatch:
    """Result of account matching."""

    matched: bool
    """Whether a match was found."""

    account_id: str | None = None
    """Matched or generated account ID."""

    candidates: list[dict[str, str]] = field(default_factory=list)
    """Fuzzy match candidates for "did you mean?" prompt."""


def _slugify(name: str) -> str:
    """Generate deterministic slug from account name."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def match_account(
    account_name: str,
    *,
    account_number: str | None = None,
    explicit_account_id: str | None = None,
    existing_accounts: list[dict[str, str | None]] | None = None,
) -> AccountMatch:
    """Match an account against the existing account registry.

    Args:
        account_name: Account name to match.
        account_number: Account number for strongest match.
        explicit_account_id: Explicit ID (bypasses matching).
        existing_accounts: List of existing account dicts with
            account_id, account_name, and optionally account_number.

    Returns:
        AccountMatch with match result and candidates.
    """
    if explicit_account_id:
        return AccountMatch(matched=True, account_id=explicit_account_id)

    existing = existing_accounts or []

    # 1. Account number match (strongest)
    if account_number:
        for acct in existing:
            if acct.get("account_number") == account_number:
                return AccountMatch(
                    matched=True, account_id=acct["account_id"]
                )

    # 2. Exact slug match
    target_slug = _slugify(account_name)
    for acct in existing:
        if acct.get("account_id") == target_slug:
            return AccountMatch(matched=True, account_id=target_slug)
        if _slugify(acct.get("account_name", "")) == target_slug:
            return AccountMatch(
                matched=True, account_id=acct["account_id"]
            )

    # 3. Fuzzy matching
    candidates: list[tuple[float, dict[str, str]]] = []
    for acct in existing:
        name = acct.get("account_name", "")
        ratio = SequenceMatcher(
            None, account_name.lower(), name.lower()
        ).ratio()
        if ratio >= 0.6:
            candidates.append((ratio, {
                "account_id": acct.get("account_id", ""),
                "account_name": name,
            }))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return AccountMatch(
            matched=False,
            candidates=[c[1] for c in candidates[:5]],
        )

    # No match — caller should create new account
    return AccountMatch(matched=False, account_id=None)
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/moneybin/test_extractors/test_tabular/test_account_matching.py -v
```

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/extractors/tabular/account_matching.py tests/moneybin/test_extractors/test_tabular/test_account_matching.py
git commit -m "feat: add cross-source account matching (number, slug, fuzzy name)"
```

---

## Execution Summary

| Phase | Tasks | Description |
|---|---|---|
| 1 | 1–6 | Data model & infrastructure (schemas, constants, dependencies) |
| 2 | 7–9 | Format system (aliases, TabularFormat model, YAML files) |
| 3 | 10 | Format detection (Stage 1) |
| 4 | 11 | File readers (Stage 2) |
| 5 | 12 | Date & number detection |
| 6 | 13 | Sign convention inference |
| 7 | 14 | Column mapping engine (Stage 3) |
| 8 | 15 | Transform & validate (Stage 4) |
| 9 | 16 | Tabular loader (Stage 5) |
| 10 | 17 | Service layer wiring |
| 11 | 18–19 | SQLMesh staging views + core model updates |
| 12 | 20–21 | CLI commands |
| 13 | 22 | MCP tools |
| 14 | 23 | Metrics |
| 15 | 24 | Remove old CSV system |
| 16 | 25 | Integration tests & fixtures |
| 17 | 26 | Documentation updates |
| 18 | 27 | Quality pass |
| 19 | 28 | Running balance validation |
| 20 | 29 | Format DB persistence (save/load/merge) |
| 21 | 30 | Account matching (number, slug, fuzzy) |

**Total: 30 tasks across 21 phases.**

Each phase produces working, testable code with its own commit. Phases 1–9 build bottom-up (data model → detection → reading → mapping → transform → load). Phases 10–13 wire the pipeline into the application surfaces. Phases 14–18 are cleanup and quality. Phases 19–21 fill in spec requirements that cross-cut multiple pipeline stages.
