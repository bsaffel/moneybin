---
description: "Data extraction and loading: day-boundary extraction, incremental sync, dedup strategy, parameter design"
paths: ["src/moneybin/extractors/**", "src/moneybin/connectors/**"]
---

# Data Extraction & Loading

## Day-Boundary Extraction

Only extract complete days: the range runs from the day after the last extraction through the last complete day, and is empty when no new complete day exists. Partial-day extraction breaks idempotency — re-running the same extraction later in the day would pick up new transactions, producing different record counts and content hashes that defeat dedup. Resolve "complete day" per `design-principles.md` → "Time": never from the host clock's local date.

## Incremental by Default

- Default to incremental extraction (skip if no new complete days).
- Provide `--force` / `-f` flag for full lookback override.
- Track extraction metadata in DuckDB.

## Dedup Strategy

| Level | Method |
|-------|--------|
| Extraction | Track last extraction dates, skip redundant API calls |
| Loading | Unique IDs (`transaction_id`) prevent duplicate inserts |
| Storage | Allow duplicate raw files; deduplicate at database level |

## Parameter Design

Only expose parameters for values that **cannot be reliably determined from the document content itself**. If a value (e.g., tax year, account number, institution name) is present in or derivable from the source file, the parser should extract it — not require the caller to provide it.

- **Extractors**: Parse all available metadata from the document. Use multi-tier fallback strategies (content → filename → file metadata).
- **CLI/MCP callers**: Do not expose options for extractor-derivable fields. Only surface parameters for truly external context (e.g., `account_id` for CSV files that lack one).
- **MCP prompts**: When a workflow requires values that can't be extracted, the prompt template should explicitly ask the user for them rather than silently defaulting.

For example, CSV `account_id` is justified — CSVs genuinely don't contain account identifiers.
