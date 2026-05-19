# Feature: `categorize commit` CLI + bulk-loop performance

## Status
implemented

> **Naming notes:**
> - **2026-05-10:** Internal class and method names renamed as part of the de-bulking sweep — `BulkCategorizationResult` → `CategorizationResult`, `bulk_categorize` → `categorize_items`, `BulkCategorizationItem` → `CategorizationItem`, `BulkRecordingContext` → `RecordingContext`.
> - **2026-05-15 (PR #171):** Public surface renamed — MCP tool `transactions_categorize_bulk_apply` → `transactions_categorize_commit`; CLI command `moneybin transactions categorize bulk` → `moneybin transactions categorize commit`. The shape-3 `_commit` verb (per `.claude/rules/surface-design.md`) replaces the bulk-suffixed names throughout this spec.
> - **2026-05-17 (PR #155):** `CategorizationService` was split into a facade + collaborators under `src/moneybin/services/categorization/`; the prior single-file `categorization_service.py` no longer exists. `CategorizationItem`, `CategorizationResult`, and `categorize_items()` are re-exported from the package `__init__.py`; `RecordingContext` lives in `src/moneybin/services/auto_rule_service.py`.

## Goal

Add a `moneybin transactions categorize commit` CLI command (originally shipped as `categorize bulk`; renamed in PR #171) that mirrors the `transactions_categorize_commit` MCP tool (originally `transactions_categorize_bulk_apply`), and eliminate per-item duplicate DB lookups inside `CategorizationService.categorize_items` by threading a shared `RecordingContext` through `AutoRuleService.record_categorization`. Tighten the bulk-categorize input contract by replacing untyped dicts with a shared Pydantic model (`CategorizationItem`) validated at every boundary.

## Background

- `mcp-architecture.md` §5 (CLI Symmetry) requires every MCP tool to have a CLI equivalent. `transactions_categorize_commit` (then `transactions_categorize_bulk_apply`) was the largest remaining gap.
- Tracked deferred work: a CLI commit command, caching of active-rule patterns and merchant pairs across the `categorize_items` loop, and removing the duplicate description SELECT.
- Auto-rule learning is *triggered* by `categorize_items`. Without a CLI surface, there was no honest end-to-end CLI path through the auto-rule pipeline. `tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline::test_import_then_promote_proposal` previously seeded `app.proposed_rules` via raw `db query` SQL as a workaround.
- Today's hot path: `AutoRuleService.record_categorization` runs ~5 DB queries per item (description SELECT, rule-engine evaluation queries, merchants table SELECT) — many of which duplicate state the bulk loop already fetched.

## Requirements

1. New CLI command `moneybin transactions categorize commit` accepts a JSON array of categorization items from a file (`--input <path>`) or stdin (sentinel `-`), with `--output {table,json}` mirroring sibling commands.
2. `CategorizationService.categorize_items` requires `Sequence[CategorizationItem]` (Pydantic model). No untyped-dict input path.
3. The Pydantic model is shared between the CLI command and the `transactions_categorize_commit` MCP tool. Both surfaces validate per-item, accumulate validation failures into the existing `CategorizationResult.error_details`, and never short-circuit a partially-valid batch.
4. CLI exit code is `1` when any item failed to apply (`errors > 0` or `skipped > 0`), `0` otherwise.
5. `categorize_items` builds one `RecordingContext` (txn-row map with `description`/`amount`/`account_id`, active-rule rows, merchant rows) before the per-item loop and threads it into every `record_categorization` call. Context owns merchant-cache invalidation when the loop creates a new merchant.
6. `AutoRuleService.record_categorization` accepts an optional `context: RecordingContext | None`. When provided, helpers consult the context instead of issuing DB queries. When `None`, behavior is unchanged for non-bulk callers.
7. Query count for an N-item bulk drops from ~`3 + 5N` to ~`3 + N` (one batch description fetch, one merchants fetch, one rules fetch, one INSERT per item, plus one post-loop override check).
8. `tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline::test_import_then_promote_proposal` is rewritten to drive the auto-rule pipeline through the new CLI: import → `categorize commit` → `auto review` → `auto accept` → re-import → assert `categorized_by='auto_rule'` rows exist.
9. Observability: new metrics `categorize_items_total`, `categorize_duration_seconds`, `categorize_errors_total` registered in `src/moneybin/metrics/registry.py` and emitted by the service. (Originally proposed as `categorize_bulk_*`; renamed alongside the de-bulking sweep.)

## Data Model

No schema changes. New in-memory types only (current locations after PR #155 service split):

- `CategorizationItem` (Pydantic) in `src/moneybin/services/categorization/_shared.py`, re-exported from `src/moneybin/services/categorization/__init__.py` alongside `CategorizationResult`.
- `RecordingContext` (dataclass) in `src/moneybin/services/auto_rule_service.py`.

## Implementation Plan

> **Historical note.** This plan describes the work as it shipped. Current paths reflect the PR #155 service split (`categorization_service.py` → `services/categorization/` package) and the PR #171 rename (`bulk` → `commit`). The plan is preserved here for traceability; live file paths are restated inline below.

### Files Created

- `tests/moneybin/test_recording_context.py` (originally `test_bulk_recording_context.py`) — context unit tests: construction, `register_new_merchant` ordering, in-Python rule and merchant matching parity with the existing SQL paths.
- `tests/integration/test_categorize_commit_cli.py` (originally `test_categorize_bulk_cli.py`) — CLI integration: file input, stdin input, `--output json`, exit code on partial failure, malformed top-level JSON.

### Files Modified (canonical paths after PR #155)

- `src/moneybin/services/categorization/_shared.py` and `__init__.py`
  - `CategorizationItem` Pydantic model (originally `BulkCategorizationItem`).
  - `categorize_items` signature `Sequence[CategorizationItem]` (originally `bulk_categorize`). No untyped-dict path.
  - Model unwrapping replaces Phase 1 dict validation (input items already validated at the boundary).
- `src/moneybin/services/categorization/orchestrator.py`
  - Builds the `RecordingContext` from the existing Phase 2/3 fetches plus the active-rules fetch; threads into `record_categorization`.
  - Uses `ctx.register_new_merchant(...)` for merchant-cache invalidation.
- `src/moneybin/services/auto_rule_service.py`
  - `RecordingContext` dataclass: `txn_rows: dict[str, TxnRow]` (description + amount + account_id + memo + source_type), active-rule rows, merchant rows, ordering-aware `register_new_merchant`, `txn_row_for`, `description_for`, `active_rule_match`, `merchant_mapping_covers`.
  - Optional `context` param on `record_categorization`, `_extract_pattern`, `_active_rule_covers_transaction`, `_merchant_mapping_covers`.
- `src/moneybin/services/categorization/matcher.py::find_matching_rule`
  - Accepts pre-loaded data overrides so `match_first_rule` semantics stay in one place.
- `src/moneybin/cli/commands/transactions/categorize/__init__.py` + `commit_from_file.py`
  - `commit` Typer command (originally `bulk`). Reads file or stdin (sentinel `-`). Uses shared `validate_items()` helper.
  - Exit code per Requirement 4.
- `src/moneybin/mcp/tools/transactions_categorize.py`
  - `transactions_categorize_commit` tool calls the shared `validate_items()` helper. Same partial-success envelope.
- `src/moneybin/metrics/registry.py`
  - `moneybin_categorize_items_total`, `moneybin_categorize_duration_seconds`, `moneybin_categorize_errors_total` (originally proposed as `_bulk_*`).
- `tests/e2e/test_e2e_workflows.py`
  - `TestAutoRulePipeline::test_import_then_promote_proposal` rewritten to drive real CLI.
- `docs/specs/INDEX.md` — added under Categorization.
- `docs/specs/moneybin-mcp.md` / `moneybin-cli.md` / `moneybin-capabilities.md` — parity noted (the cross-surface capabilities map post-dates the bulk spec).
- `README.md` / `CHANGELOG.md` per `.claude/rules/shipping.md`.

### Key Decisions

- **Pydantic at every boundary, not in the service.** The service expects validated typed items. Boundaries (CLI, MCP) own validation and error accumulation. This avoids duplicating validation logic and keeps the service signature honest.
- **Per-item validation, not array-level.** Both surfaces accumulate per-row validation errors into the existing `error_details` envelope rather than rejecting the whole batch on the first bad row. Critical for the MCP path (LLM-generated input) and harmless for the CLI path.
- **Single context object, not three optional kwargs.** Caches that get invalidated together live and update together. Adding a fourth cache or changing the merchant-list shape later touches only the context's internals.
- **`find_matching_rule` accepts an in-memory rule list.** Keeps rule-engine semantics (contains/regex/exact + amount/account filters) in one place; the context just supplies the rows.
- **No backwards compatibility shim.** Existing tests passing dicts to `bulk_categorize` get migrated in the same PR.

## CLI Interface

```bash
# From a JSON file
moneybin transactions categorize commit --input categorizations.json

# From stdin (pipe-friendly)
cat categorizations.json | moneybin transactions categorize commit -

# JSON output for scripts
moneybin transactions categorize commit --input cats.json --output json
```

Input format (bare JSON array):

```json
[
  {"transaction_id": "csv_abc123", "category": "Food", "subcategory": "Groceries"},
  {"transaction_id": "csv_def456", "category": "Transport"}
]
```

Field constraints (`CategorizationItem`):

| Field | Type | Constraint |
|---|---|---|
| `transaction_id` | `str` | 1–64 chars, stripped |
| `category` | `str` | 1–100 chars, stripped |
| `subcategory` | `str \| None` | 1–100 chars when present, stripped |

`extra="forbid"` rejects unknown fields. Validation failures become `error_details` entries with the row index and Pydantic's error summary.

Exit code: `0` if every item applied cleanly, `1` if any item failed parse, validation, or apply.

## MCP Interface

`transactions_categorize_commit` tool (renamed from `transactions_categorize_bulk_apply` in PR #171). Internally:

- Same `validate_items()` helper as the CLI; validation failures accumulate to `error_details`.
- Result envelope: `{summary, data, actions}` with the `CategorizationResult` fields under `data`.
- Sensitivity tier `medium`.

## Testing Strategy

| Layer | Test |
|---|---|
| Unit | `categorize_items` accepts `CategorizationItem`; assertions on per-item validation accumulation |
| Unit | `RecordingContext` construction, ordering-preserving `register_new_merchant`, merchant/rule matching parity |
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
