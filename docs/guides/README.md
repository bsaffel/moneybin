# MoneyBin Feature Guide

Detailed documentation for everything MoneyBin can do today. Each feature has its own page with full usage examples and reference material.

For a quick overview, see the [README](../../README.md). For design specs, see the [Spec Index](../specs/INDEX.md).

## Guides

| Guide | Description |
|-------|-------------|
| [Data Import](data-import.md) | OFX/QFX, CSV, TSV, Excel, Parquet, Feather, W-2 PDF — formats, detection, batch management |
| [Data Pipeline](data-pipeline.md) | Three-layer architecture (raw/staging/core), SQLMesh transforms, adding data sources |
| [Categorization](categorization.md) | Rule engine, merchant normalization, bulk operations, category taxonomy |
| [Database & Security](database-security.md) | AES-256-GCM encryption, key management, database tools, schema migrations |
| [Multi-Profile Support](profiles.md) | Isolation boundaries, profile lifecycle, per-profile config |
| [MCP Server](mcp-server.md) | AI assistant integration — tools, prompts, client setup |
| [CLI Reference](cli-reference.md) | Complete command tree with all options |
| [Observability](observability.md) | Structured logging, metrics, instrumentation |
| [Synthetic Data](synthetic-data.md) | Test data generation — personas, merchants, ground truth |
| [Direct SQL Access](sql-access.md) | Query your data with DuckDB — shell, UI, key tables, example queries |
