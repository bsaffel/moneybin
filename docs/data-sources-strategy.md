# Data Sources Strategy

## Overview

MoneyBin supports importing financial data from multiple source types. The priority order follows the [privacy tiers](privacy-tiers-architecture.md) -- local file imports are first-class, and API-based aggregation is available through the Encrypted Sync tier.

All data sources flow through the same pipeline:

```text
Source ──→ Extractor ──→ Raw Tables ──→ dbt Staging ──→ Core Tables ──→ MCP Server
```

---

## Priority 1: OFX/QFX Files (Local Only -- Implemented)

The primary data source for the Local Only tier. Most US banks support OFX/QFX file exports.

**Status**: Implemented

**Import command**:

```bash
moneybin extract ofx path/to/downloads/*.qfx
```

**What's extracted**: Institutions, accounts, transactions, balances

**Supported institutions** (any bank that supports OFX/QFX export):

- Wells Fargo, Chase, Capital One, Bank of America, etc.
- Credit unions and smaller banks
- Credit card issuers (Amex, Discover, etc.)

**How to get OFX/QFX files**: Most banks offer "Download transactions" in QFX/OFX format from their online banking portal. Look for "Quicken" or "Money" export options.

See [`ofx-import-guide.md`](ofx-import-guide.md) for the complete guide.

---

## Priority 2: W-2 PDF Extraction (Local Only -- Implemented)

Extract W-2 Wage and Tax Statement data from PDF files using dual extraction (text + OCR).

**Status**: Implemented

**Import command**:

```bash
moneybin extract w2 path/to/w2.pdf
```

**What's extracted**: Tax year, employer info, wages (Box 1), federal/state/FICA taxes, state/local details

See [`w2-extraction-architecture.md`](w2-extraction-architecture.md) for the technical design.

---

## Priority 3: CSV Import (Local Only -- Planned)

Manual CSV import for banks that don't support OFX, or for users who prefer CSV workflows.

**Status**: Planned

**Planned command**:

```bash
moneybin extract csv path/to/transactions.csv --bank=chase
```

**Implementation approach**:
- Bank-specific parsers for common formats (Chase, Capital One, Wells Fargo, Fidelity, etc.)
- Generic parser with column mapping for unknown formats
- Raw tables: `raw.csv_transactions`, `raw.csv_accounts`

### CSV Export Locations by Bank

| Bank | Path to CSV Export |
|------|-------------------|
| Wells Fargo | Account Activity -> Export -> Comma Delimited |
| Chase | Account Details -> Download Activity -> CSV |
| Capital One | Account Details -> Download Transactions -> CSV |
| Fidelity | Portfolio -> History -> Download -> CSV |
| E*TRADE | Accounts -> History -> Export -> CSV |

---

## Priority 4: PDF Statement Processing (Local Only -- Planned)

For institutions without OFX or CSV export, or for tax forms beyond W-2.

**Status**: Partial (W-2 implemented; other forms planned)

**Primary tool**: pdfplumber (with pytesseract OCR fallback)

**Planned extractors**:
- Form 1040 (Individual Income Tax Return)
- 1099 forms (1099-INT, 1099-DIV, 1099-MISC, etc.)
- Bank statements (multi-bank PDF processing)
- Investment statements (Fidelity, E*TRADE)
- State tax forms (Georgia Form 500, etc.)

See [`modern-ocr-strategy.md`](modern-ocr-strategy.md) for the complete PDF processing strategy.

---

## Priority 5: Plaid API (Encrypted Sync Tier -- Planned)

Automatic bank transaction sync via Plaid, with E2E encryption.

**Status**: Planned (requires Encrypted Sync service)

**Supported via Plaid**:
- Wells Fargo, Chase, Capital One (checking, savings, credit cards)
- Fidelity, E*TRADE (investment accounts)
- Most US banks and credit unions

**Not supported via Plaid**:
- Goldman Sachs Wealth Management (no retail API access)
- QuickBooks (separate API)

**How it works**:
1. User connects bank accounts via Plaid Link
2. Encrypted Sync server fetches data from Plaid
3. Data is encrypted immediately to user's device key
4. Encrypted payload synced to user's machine
5. Client decrypts and loads into `raw.plaid_*` tables
6. dbt transforms into core tables alongside OFX/CSV data

See [`architecture/e2e-encryption.md`](architecture/e2e-encryption.md) for the encryption design and [`architecture/security-tradeoffs.md`](architecture/security-tradeoffs.md) for the security analysis.

---

## Data Source Integration Matrix

| Source | Privacy Tier | Status | Raw Tables |
|--------|-------------|--------|------------|
| OFX/QFX files | Local Only | Implemented | `raw.ofx_*` |
| W-2 PDFs | Local Only | Implemented | `raw.w2_forms` |
| CSV files | Local Only | Planned | `raw.csv_*` |
| Other tax PDFs | Local Only | Planned | `raw.tax_*` |
| Bank statement PDFs | Local Only | Planned | `raw.pdf_*` |
| Plaid API | Encrypted Sync | Planned | `raw.plaid_*` |

All sources feed into the same core tables (`core.dim_accounts`, `core.fct_transactions`) via dbt staging models, so the MCP server and data toolkit work identically regardless of how data was imported.

---

## Fallback Strategy

For any given institution, the recommended approach:

1. **OFX/QFX** -- Try this first. Most banks support it and it's the cleanest format.
2. **CSV** -- If OFX isn't available, most banks offer CSV export.
3. **PDF** -- For institutions with no export options (Goldman Sachs Wealth, tax forms).
4. **Plaid** -- For automatic ongoing sync (requires Encrypted Sync tier).

The goal is to ensure MoneyBin is fully functional with **zero paid services** -- everything works in the Local Only tier with manual file imports.
