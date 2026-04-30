# Feature: `categorize bulk` CLI + bulk-loop performance

## Status
draft

## Goal

Add a `moneybin categorize bulk` CLI command that mirrors the existing `categorize.bulk` MCP tool, and eliminate per-item duplicate DB lookups inside `CategorizationService.bulk_categorize` by threading a shared `BulkRecordingContext` through `AutoRuleService.record_categorization`. Tighten the bulk-categorize input contract by replacing untyped dicts with a shared Pydantic model validated at every boundary.

## Background

- `mcp-architecture.md` §5 (CLI Symmetry) requires every MCP tool to have a CLI equivalent. `categorize.bulk` is the largest remaining gap.
- `private/followups.md` items: "No `categorize bulk` CLI command", "Cache active-rule patterns and merchant pairs across `bulk_categorize` loop", "Avoid duplicate description SELECT in `bulk_categorize`".
- Auto-rule learning is *triggered* by `bulk_categorize`. Without a CLI surface, there is no honest end-to-end CLI path through the auto-rule pipeline. `tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline::test_import_then_promote_proposal` currently seeds `app.proposed_rules` via raw `db query` SQL as a workaround.
- Today's hot path: `AutoRuleService.record_categorization` runs ~5 DB queries per item (description SELECT, rule-engine evaluation queries, merchants table SELECT) — many of which duplicate state the bulk loop already fetched.

## Requirements

1. New CLI command `moneybin categorize bulk` accepts a JSON array of categorization items from a file (`--input <path>`) or stdin (sentinel `-`), with `--output {table,json}` mirroring sibling commands.
2. `CategorizationService.bulk_categorize` requires `Sequence[BulkCategorizationItem]` (Pydantic model). No untyped-dict input path.
3. The Pydantic model is shared between the CLI command and the `categorize.bulk` MCP tool. Both surfaces validate per-item, accumulate validation failures into the existing `BulkCategorizationResult.error_details`, and never short-circuit a partially-valid batch.
4. CLI exit code is `1` when any item failed to apply (`errors > 0` or `skipped > 0`), `0` otherwise.
5. `bulk_categorize` builds one `BulkRecordingContext` (txn-row map with `description`/`amount`/`account_id`, active-rule rows, merchant rows) before the per-item loop and threads it into every `record_categorization` call. Context owns merchant-cache invalidation when the loop creates a new merchant.
6. `AutoRuleService.record_categorization` accepts an optional `context: BulkRecordingContext | None`. When provided, helpers consult the context instead of issuing DB queries. When `None`, behavior is unchanged for non-bulk callers.
7. Query count for an N-item bulk drops from ~`3 + 5N` to ~`3 + N` (one batch description fetch, one merchants fetch, one rules fetch, one INSERT per item, plus one post-loop override check).
8. `tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline::test_import_then_promote_proposal` is rewritten to drive the auto-rule pipeline through the new CLI: import → `categorize bulk` → `auto-review` → `auto-confirm` → re-import → assert `categorized_by='auto_rule'` rows exist.
9. Observability: new metrics `categorize_bulk_items_total`, `categorize_bulk_duration_seconds`, `categorize_bulk_errors_total` registered in `src/moneybin/metrics/registry.py` and emitted by the service.

## Data Model

No schema changes. New in-memory types only:

- `BulkCategorizationItem` (Pydantic) in `src/moneybin/services/categorization_service.py` next to `BulkCategorizationResult`.
- `BulkRecordingContext` (dataclass) in `src/moneybin/services/auto_rule_service.py`.

## Implementation Plan

### Files to Create

- `tests/moneybin/test_bulk_recording_context.py` — context unit tests: construction, `register_new_merchant` ordering, in-Python rule and merchant matching parity with the existing SQL paths.
- `tests/integration/test_categorize_bulk_cli.py` — CLI integration: file input, stdin input, `--output json`, exit code on partial failure, malformed top-level JSON.

### Files to Modify

- `src/moneybin/services/categorization_service.py`
  - Add `BulkCategorizationItem` Pydantic model.
  - Tighten `bulk_categorize` signature to `Sequence[BulkCategorizationItem]`.
  - Replace Phase 1 dict validation with model unwrapping (input items already validated at the boundary).
  - Build `BulkRecordingContext` from the existing Phase 2/3 fetches plus a new active-rules fetch; pass into `record_categorization`.
  - Replace direct `cached_merchants.insert(...)` with `ctx.register_new_merchant(...)`.
- `src/moneybin/services/auto_rule_service.py`
  - Add `BulkRecordingContext` dataclass with: `txn_rows: dict[str, TxnRow]` (description + amount + account_id, populated by widening the existing batch description fetch), active-rule rows, merchant rows, ordering-aware `register_new_merchant`, `txn_row_for`, `description_for`, `active_rule_match`, `merchant_mapping_covers`.
  - Add optional `context` param to `record_categorization`, `_extract_pattern`, `_active_rule_covers_transaction`, `_merchant_mapping_covers`. When present, route through context.
- `src/moneybin/services/categorization_service.py::find_matching_rule`
  - Optional `rules_override: list[ActiveRuleRow] | None` and `txn_row_override: TxnRow | None` to evaluate against pre-loaded data. Keeps rule-engine semantics (`match_first_rule`) in one place.
- `src/moneybin/cli/commands/categorize.py`
  - New `bulk` Typer command. Reads file or stdin (sentinel `-`). Calls a shared `_validate_items()` helper that returns `(items, parse_errors)`.
  - Merges `parse_errors` into `BulkCategorizationResult.error_details`.
  - Exit code per Requirement 4.
- `src/moneybin/mcp/tools/categorize.py`
  - Replace inline dict-passing with the shared `_validate_items()` helper. Same partial-success envelope.
- `src/moneybin/metrics/registry.py`
  - Register the three new metrics.
- `tests/e2e/test_e2e_workflows.py`
  - Rewrite `TestAutoRulePipeline::test_import_then_promote_proposal` to use real CLI flow.
- `tests/moneybin/test_categorization_service.py`, `tests/moneybin/test_auto_rule_service.py`
  - Migrate dict-based bulk tests to construct `BulkCategorizationItem`. Add coverage for context-routed paths and assertions that DB queries are not issued when the context is provided.
- `docs/specs/INDEX.md` — add this spec under Categorization.
- `docs/specs/mcp-architecture.md` §5 — note that `categorize.bulk` now has CLI parity.
- `private/followups.md` — remove the three resolved items.
- `README.md` — per `.claude/rules/shipping.md`: update CLI section / categorization roadmap.

### Key Decisions

- **Pydantic at every boundary, not in the service.** The service expects validated typed items. Boundaries (CLI, MCP) own validation and error accumulation. This avoids duplicating validation logic and keeps the service signature honest.
- **Per-item validation, not array-level.** Both surfaces accumulate per-row validation errors into the existing `error_details` envelope rather than rejecting the whole batch on the first bad row. Critical for the MCP path (LLM-generated input) and harmless for the CLI path.
- **Single context object, not three optional kwargs.** Caches that get invalidated together live and update together. Adding a fourth cache or changing the merchant-list shape later touches only the context's internals.
- **`find_matching_rule` accepts an in-memory rule list.** Keeps rule-engine semantics (contains/regex/exact + amount/account filters) in one place; the context just supplies the rows.
- **No backwards compatibility shim.** Existing tests passing dicts to `bulk_categorize` get migrated in the same PR.

## CLI Interface

```bash
# From a JSON file
moneybin categorize bulk --input categorizations.json

# From stdin (pipe-friendly)
cat categorizations.json | moneybin categorize bulk -

# JSON output for scripts
moneybin categorize bulk --input cats.json --output json
```

Input format (bare JSON array):

```json
[
  {"transaction_id": "csv_abc123", "category": "Food", "subcategory": "Groceries"},
  {"transaction_id": "csv_def456", "category": "Transport"}
]
```

Field constraints (`BulkCategorizationItem`):

| Field | Type | Constraint |
|---|---|---|
| `transaction_id` | `str` | 1–64 chars, stripped |
| `category` | `str` | 1–100 chars, stripped |
| `subcategory` | `str \| None` | 1–100 chars when present, stripped |

`extra="forbid"` rejects unknown fields. Validation failures become `error_details` entries with the row index and Pydantic's error summary.

Exit code: `0` if every item applied cleanly, `1` if any item failed parse, validation, or apply.

## MCP Interface

`categorize.bulk` tool — no signature change at the protocol level. Internally:

- Same `_validate_items()` helper as the CLI; validation failures accumulate to `error_details`.
- Result envelope unchanged: `{summary, data, actions}` with the existing `BulkCategorizationResult` fields under `data`.
- Sensitivity tier unchanged (`medium`).

## Testing Strategy

| Layer | Test |
|---|---|
| Unit | `bulk_categorize` migrated to `BulkCategorizationItem` input; assertions on per-item validation accumulation |
| Unit | `BulkRecordingContext` construction, ordering-preserving `register_new_merchant`, merchant/rule matching parity |
| Unit | `record_categorization(context=...)` issues no DB queries for description/rules/merchants; falls back when `context=None` |
| Unit | `find_matching_rule(rules_override=...)` returns identical results to the DB-backed path |
| Integration | CLI: file input, stdin input, `--output json`, partial-failure exit code, malformed JSON, unknown field rejected |
| E2E | `TestAutoRulePipeline::test_import_then_promote_proposal` rewritten to drive real CLI |

Performance check: in the bulk-path unit test, mock the `Database` and assert query count is `O(items)` rather than `O(5 * items)` by counting `db.execute(...)` calls.

## Synthetic Data Requirements

None. The synthetic generator already produces tabular transactions that exercise the bulk path; the rewritten E2E uses existing fixtures.

## Dependencies

- Pydantic (already a project dependency).
- No new packages.

## Out of Scope

- CSV input format for the CLI (followup if user demand exists).
- Idempotency tokens / re-run protection. `INSERT OR REPLACE` masks duplicate writes for `transaction_categories`; auto-rule observation counters may inflate on re-runs of identical files. Auto-rule threshold logic absorbs the noise.
- Streaming JSON parse for very large inputs. Personal-finance scale makes this academic.
- Async / concurrent bulk processing.
