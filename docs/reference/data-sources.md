# Data Sources

MoneyBin supports importing financial data from multiple source types. The priority order follows the [privacy tiers](../decisions/002-privacy-tiers.md) -- local file imports are first-class, API-based aggregation is available through the Encrypted Sync tier.

All data sources flow through the same pipeline:

```mermaid
flowchart LR
    Source --> Extractor --> Raw[Raw Tables] --> Staging[SQLMesh Staging] --> Core[Core Tables] --> MCP[MCP Server]
```

## Integration matrix

| Source | Privacy Tier | Status | Raw Tables | Import Command |
|--------|-------------|--------|------------|----------------|
| OFX/QFX files | Local Only | Implemented | `raw.ofx_*` | `moneybin data extract ofx <file>` |
| W-2 PDFs | Local Only | Implemented | `raw.w2_forms` | `moneybin data extract w2 <file>` |
| CSV files | Local Only | Planned | `raw.csv_*` | `moneybin data extract csv <file>` |
| Other tax PDFs | Local Only | Planned | `raw.tax_*` | -- |
| Bank statement PDFs | Local Only | Planned | `raw.pdf_*` | -- |
| Plaid API | Encrypted Sync | Planned | `raw.plaid_*` | Automatic sync |

All sources feed into the same core tables (`core.dim_accounts`, `core.fct_transactions`) via SQLMesh staging models.

## Priority 1: OFX/QFX files (Implemented)

The primary data source for the Local Only tier. Most US banks support OFX/QFX file exports.

```bash
moneybin data extract ofx path/to/downloads/*.qfx
```

Extracts institutions, accounts, transactions, and balances. Supports both SGML and XML OFX formats. Idempotent -- safe to re-import the same file.

**How to get files**: Most banks offer "Download transactions" in QFX/OFX format. Look for "Quicken" or "Money" export options.

See [OFX Import Spec](../specs/archived/ofx-import.md) for details.

## Priority 2: W-2 PDF extraction (Implemented)

Dual extraction strategy (text + OCR) for W-2 tax forms.

```bash
moneybin data extract w2 path/to/w2.pdf --year 2024
```

Extracts tax year, employer info, wages, federal/state/FICA taxes. Confidence scoring validates extraction quality.

See [W-2 Extraction Spec](../specs/archived/w2-extraction.md) for details.

## Priority 3: CSV import (Planned)

Manual CSV import for banks that don't support OFX, or for users who prefer CSV workflows.

```bash
moneybin data extract csv path/to/transactions.csv --bank=chase
```

Bank-specific parsers for common formats plus a generic parser with column mapping. See [CSV Import Spec](../specs/csv-import.md).

### CSV export locations by bank

| Bank | Where to Find CSV Export |
|------|------------------------|
| Wells Fargo | Account Activity > Export > Comma Delimited |
| Chase | Account Details > Download Activity > CSV |
| Capital One | Account Details > Download Transactions > CSV |
| Fidelity | Portfolio > History > Download > CSV |

## Priority 4: PDF statement processing (Planned)

For institutions without OFX or CSV export, or for tax forms beyond W-2.

**Primary tool**: pdfplumber (with pytesseract OCR fallback)

**Planned extractors**: Form 1040, 1099 forms, bank statements, investment statements, state tax forms.

## Priority 5: Plaid API (Planned -- Encrypted Sync tier)

Automatic bank sync with E2E encryption.

1. User connects bank accounts via Plaid Link
2. Encrypted Sync server fetches data from Plaid
3. Data encrypted immediately to user's device key
4. Encrypted payload synced to user's machine
5. Client decrypts and loads into `raw.plaid_*` tables
6. SQLMesh transforms into core tables alongside OFX/CSV data

See [Plaid Integration Spec](../specs/sync-plaid.md) and [ADR-004: E2E Encryption](../decisions/004-e2e-encryption.md).

## Fallback strategy

For any given institution:

1. **OFX/QFX** -- Try first. Most banks support it and it's the cleanest format.
2. **CSV** -- If OFX isn't available, most banks offer CSV export.
3. **PDF** -- For institutions with no export options (Goldman Sachs Wealth, tax forms).
4. **Plaid** -- For automatic ongoing sync (requires Encrypted Sync tier).

The goal is full functionality with **zero paid services** -- everything works in Local Only with manual file imports.
