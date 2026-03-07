# ADR-001: Medallion Data Layers

## Status
accepted

## Context

MoneyBin ingests financial data from multiple sources (OFX, W-2, CSV, Plaid) with different schemas, naming conventions, and data quality levels. We need a consistent architecture to organize data from raw ingestion through to analytical consumption.

The medallion architecture (raw/staging/core) is a well-established pattern in data engineering that provides clear separation of concerns at each layer.

## Decision

Adopt a three-layer medallion architecture with distinct schemas:

| Layer | Schema | Materialization | Purpose |
|-------|--------|-----------------|---------|
| Raw | `raw` | Table | Untouched data from loaders (Python extractors). Source-specific tables preserved exactly as extracted. |
| Staging | `prep` | View | Light cleaning, type casting, column renaming (dbt `stg_*` models). |
| Core | `core` | Table | Canonical, deduplicated, multi-source fact and dimension tables. |

### Key principles

1. **One canonical table per entity** -- `dim_accounts`, `fct_transactions`, etc. All consumers read from core only.
2. **Multi-source union** -- Core models `UNION ALL` from every staging source with a `source_system` column.
3. **Dedup in core** -- `ROW_NUMBER()` windows for duplicate records; mapping tables for cross-source dedup.
4. **Accounting sign convention** -- negative = expense, positive = income. Amounts are `DECIMAL(18,2)`, dates are `DATE`.
5. **Source-agnostic consumers** -- MCP server, CLI, dashboards use core `TableRef` constants, never source-specific logic.

### Adding a new data source

1. Create staging models in `dbt/models/<source>/` (views in `prep` schema)
2. Add a CTE to the relevant core model and `UNION ALL` into the `all_*` CTE
3. No changes needed to consumers

## Consequences

- Clear data lineage from source to consumption.
- Raw data is never modified, enabling full replay and auditing.
- New sources can be added without changing downstream consumers.
- dbt handles all transformation logic, keeping Python extractors focused on extraction.
- The `prep` schema uses views (not tables) to avoid data duplication.
- Core tables must be rebuilt (`dbt run`) after raw data changes.

## References

- [Data Model](../reference/data-model.md) -- Schema definitions and ER diagram
- [Data Sources](../reference/data-sources.md) -- Source roadmap and priorities
