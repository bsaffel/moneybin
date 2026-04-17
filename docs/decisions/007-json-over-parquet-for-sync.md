# ADR-007: JSON Over Parquet for Server-Client Sync Transfer

**Status**: accepted

## Context

The original moneybin-server design (Phase 3 spec) called for the server to convert Plaid API responses into Parquet files, package them as a tar archive, and serve them for download. The client would unpack the archive and load Parquet files into DuckDB via `read_parquet()`.

This required a heavy server-side dependency (`@duckdb/node` ~50MB native binary, or `parquet-wasm`) solely to produce Parquet output. It also introduced temp file management, tar archive packaging/unpacking, TTL-based cleanup of in-memory Parquet buffers, and an entire test suite for Parquet schema validation.

Meanwhile, the actual data volumes are small -- a typical sync produces hundreds to low thousands of transactions (KB of JSON). Parquet's strengths (columnar compression, predicate pushdown, efficient scanning of wide tables) provide no benefit at this scale.

## Decision

The server delivers sync data as typed JSON responses over standard REST endpoints. The client loads JSON into DuckDB raw tables using `read_json()` or direct parameterized INSERTs.

The `/sync/data` endpoint returns:

```json
{
  "accounts": [...],
  "transactions": [...],
  "balances": [...],
  "removed_transactions": [...]
}
```

The data contract is enforced by Zod schemas on the server and Pydantic models on the client, not by Parquet column schemas.

## Alternatives Considered

1. **`@duckdb/node` Parquet generation (original plan)** -- Guarantees DuckDB-readable Parquet but adds a heavy native dependency to a server that is otherwise a lightweight REST API. Deployment complications with native binaries.
2. **`parquet-wasm`** -- Lighter than DuckDB but still an extra dependency and build complexity for a format that adds no value at these data volumes.
3. **Server returns JSON, client writes Parquet locally** -- Moves complexity to the client. Unnecessary intermediate step when DuckDB can read JSON directly.

## Consequences

- Server has no Parquet dependency. No `src/services/parquet.ts`, no temp file management, no tar packaging.
- Phases 2 and 3 of the server implementation plan can be merged since the sync engine becomes "call Plaid, return JSON" rather than "call Plaid, generate Parquet, package archive."
- The sync data response is human-readable and debuggable with `curl`.
- Client-side `PlaidLoader` uses `read_json()` or direct INSERTs instead of `read_parquet()`.
- Raw table DDL in the client (`raw_plaid_*.sql`) is unchanged -- the schema contract is the same, only the loading mechanism changes.
- If data volumes ever grow to the point where Parquet matters (millions of rows, multi-MB payloads), this decision can be revisited. The JSON schema maps 1:1 to the Parquet schema, so migration would be additive.
- The `plaid-integration.md` spec in this project should be updated to reflect JSON loading instead of Parquet loading in the raw table pipeline.
