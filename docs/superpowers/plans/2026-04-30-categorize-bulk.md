# `categorize bulk` CLI + Bulk-Loop Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `moneybin categorize bulk` CLI command that mirrors the existing `categorize.bulk` MCP tool, eliminate per-item duplicate DB lookups in `bulk_categorize` via a shared `BulkRecordingContext`, and tighten the input contract to a Pydantic model validated at every boundary.

**Architecture:** A shared `BulkCategorizationItem` Pydantic model lives in `categorization_service.py` and is the only accepted input shape for `bulk_categorize`. CLI and MCP both validate per-item via a shared `_validate_items` helper, accumulating row failures into the existing `BulkCategorizationResult.error_details` envelope. A new `BulkRecordingContext` dataclass in `auto_rule_service.py` carries pre-loaded txn rows, active rules, and merchant mappings into `record_categorization`, replacing five DB queries per item with Python-side membership checks. The auto-rule E2E test gets rewritten to drive the auto-rule pipeline through the new CLI rather than seeding `app.proposed_rules` via raw SQL.

**Tech Stack:** Python 3.12, Typer, Pydantic v2, DuckDB, pytest (`@pytest.mark.integration`, `@pytest.mark.e2e`), the project's `Database` + `CategorizationService` + `AutoRuleService` plumbing.

**Reference spec:** `docs/specs/categorize-bulk.md`.

**Related rules:**
- `.claude/rules/cli.md` (Typer command structure, `handle_cli_errors`, `--output json`)
- `.claude/rules/security.md` (Pydantic at boundaries, parameterized SQL, PII in logs)
- `.claude/rules/testing.md` (test layers, fixtures, query-count assertions)
- `.claude/rules/mcp-server.md` (response envelope, sensitivity tier)
- `.claude/rules/shipping.md` (README updates, `/simplify` pre-push pass)
- `.claude/rules/branching.md` (branch is `feat/categorize-bulk` — primary intent is the new CLI feature)

---

## File Structure

| File | Status | Responsibility |
|------|--------|---------------|
| `src/moneybin/services/categorization_service.py` | Modify | Add `BulkCategorizationItem` Pydantic model. Tighten `bulk_categorize` to `Sequence[BulkCategorizationItem]`. Widen Phase 2 batch fetch to include amount + account_id. Build `BulkRecordingContext` and pass into `record_categorization`. Add `rules_override` and `txn_row_override` params to `find_matching_rule`. Add module-level `_validate_items` helper shared by CLI and MCP. |
| `src/moneybin/services/auto_rule_service.py` | Modify | Add `BulkRecordingContext` dataclass + `TxnRow` dataclass. Add optional `context` to `record_categorization`, `_extract_pattern`, `_active_rule_covers_transaction`, `_merchant_mapping_covers`. Route through context when present. |
| `src/moneybin/cli/commands/categorize.py` | Modify | Add `bulk` Typer command: file or stdin (`-` sentinel) input, `--output {table,json}`, exit 1 on partial failure. |
| `src/moneybin/mcp/tools/categorize.py` | Modify | Replace dict-typed `categorize_bulk` arg with shared `_validate_items` boundary. |
| `src/moneybin/metrics/registry.py` | Modify | Register `categorize_bulk_items_total`, `categorize_bulk_duration_seconds`, `categorize_bulk_errors_total`. |
| `tests/moneybin/test_bulk_recording_context.py` | Create | Unit tests: context construction, `register_new_merchant` ordering, in-Python merchant + rule matching parity. |
| `tests/moneybin/test_categorization_service_bulk.py` | Create | Unit tests: `BulkCategorizationItem` validation, query-count assertion, `_validate_items` per-item accumulation. |
| `tests/moneybin/test_auto_rule_service_context.py` | Create | Unit tests: `record_categorization(context=...)` issues no description/rules/merchants queries; falls back when `context=None`. |
| `tests/integration/test_categorize_bulk_cli.py` | Create | Integration tests: file input, stdin input, JSON output, partial failure exit code, malformed input. |
| `tests/e2e/test_e2e_workflows.py` | Modify | Rewrite `TestAutoRulePipeline::test_import_then_promote_proposal` to drive the real CLI flow. |
| `tests/moneybin/test_categorization_service.py` | Modify | Migrate existing `bulk_categorize` dict-based fixtures to construct `BulkCategorizationItem`. |
| `docs/specs/INDEX.md` | Modify | Update `categorize-bulk` status to `implemented` post-merge (Task 11 leaves it at `in-progress` until merge). |
| `docs/specs/categorize-bulk.md` | Modify | Status transitions per workflow. |
| `docs/specs/mcp-architecture.md` | Modify | Note CLI parity for `categorize.bulk` is shipped. |
| `private/followups.md` | Modify | Remove the three resolved items: "No `categorize bulk` CLI command", "Cache active-rule patterns and merchant pairs", "Avoid duplicate description SELECT". |
| `README.md` | Modify | Add `categorize bulk` to the CLI section. Update Categorization roadmap icon if present. |

---

## Task 1: Add `BulkCategorizationItem` model and `_validate_items` helper

**Files:**
- Modify: `src/moneybin/services/categorization_service.py`
- Create: `tests/moneybin/test_categorization_service_bulk.py`

The shared input boundary. Validation lives at the model; both CLI and MCP call `_validate_items` to convert raw input into `(items, parse_errors)` and merge `parse_errors` into `BulkCategorizationResult.error_details`.

- [ ] **Step 1.1: Write failing tests for the model and the helper**

Create `tests/moneybin/test_categorization_service_bulk.py`:

```python
"""Unit tests for BulkCategorizationItem and _validate_items."""

from __future__ import annotations

import pytest

from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    _validate_items,
)


class TestBulkCategorizationItem:
    def test_valid_item_with_subcategory(self) -> None:
        item = BulkCategorizationItem(
            transaction_id="csv_abc123",
            category="Food",
            subcategory="Groceries",
        )
        assert item.transaction_id == "csv_abc123"
        assert item.category == "Food"
        assert item.subcategory == "Groceries"

    def test_subcategory_optional(self) -> None:
        item = BulkCategorizationItem(transaction_id="csv_abc", category="Food")
        assert item.subcategory is None

    def test_strips_whitespace(self) -> None:
        item = BulkCategorizationItem(
            transaction_id="  csv_abc  ",
            category="  Food  ",
            subcategory="  Groceries  ",
        )
        assert item.transaction_id == "csv_abc"
        assert item.category == "Food"
        assert item.subcategory == "Groceries"

    def test_empty_transaction_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="", category="Food")

    def test_empty_category_rejected(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="csv_abc", category="")

    def test_empty_subcategory_rejected(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(
                transaction_id="csv_abc", category="Food", subcategory=""
            )

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(
                transaction_id="csv_abc",
                category="Food",
                notes="hallucinated by an LLM",  # type: ignore[call-arg]
            )

    def test_transaction_id_max_length(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="x" * 65, category="Food")

    def test_category_max_length(self) -> None:
        with pytest.raises(ValueError):
            BulkCategorizationItem(transaction_id="csv_abc", category="x" * 101)


class TestValidateItems:
    def test_all_valid_returns_items_no_errors(self) -> None:
        raw = [
            {"transaction_id": "csv_abc", "category": "Food"},
            {
                "transaction_id": "csv_def",
                "category": "Transport",
                "subcategory": "Gas",
            },
        ]
        items, parse_errors = _validate_items(raw)
        assert len(items) == 2
        assert items[0].transaction_id == "csv_abc"
        assert items[1].subcategory == "Gas"
        assert parse_errors == []

    def test_per_item_validation_accumulates_errors(self) -> None:
        raw = [
            {"transaction_id": "csv_abc", "category": "Food"},
            {"transaction_id": "", "category": "Transport"},  # invalid
            {"transaction_id": "csv_def", "category": ""},  # invalid
            {"transaction_id": "csv_ghi", "category": "Shopping"},
        ]
        items, parse_errors = _validate_items(raw)
        assert len(items) == 2
        assert {i.transaction_id for i in items} == {"csv_abc", "csv_ghi"}
        assert len(parse_errors) == 2
        assert parse_errors[0]["transaction_id"] == "(missing)"
        assert "transaction_id" in parse_errors[0]["reason"]
        assert parse_errors[1]["transaction_id"] == "csv_def"
        assert "category" in parse_errors[1]["reason"]

    def test_unknown_field_accumulates(self) -> None:
        raw = [{"transaction_id": "csv_abc", "category": "Food", "notes": "no"}]
        items, parse_errors = _validate_items(raw)
        assert items == []
        assert len(parse_errors) == 1
        assert "notes" in parse_errors[0]["reason"]

    def test_non_dict_row_accumulates(self) -> None:
        raw = [{"transaction_id": "csv_abc", "category": "Food"}, "not a dict"]
        items, parse_errors = _validate_items(raw)
        assert len(items) == 1
        assert len(parse_errors) == 1

    def test_top_level_not_a_list_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON array"):
            _validate_items({"items": []})  # type: ignore[arg-type]
```

- [ ] **Step 1.2: Run the tests and confirm they fail**

```bash
uv run pytest tests/moneybin/test_categorization_service_bulk.py -v
```

Expected: ImportError or collection error — `BulkCategorizationItem` and `_validate_items` don't exist yet.

- [ ] **Step 1.3: Implement the model and helper**

In `src/moneybin/services/categorization_service.py`, add at the top of the file (after existing imports):

```python
from pydantic import BaseModel, ConfigDict, Field, ValidationError
```

Then add immediately after the existing `BulkCategorizationResult` dataclass (around line 92):

```python
class BulkCategorizationItem(BaseModel):
    """One row of input for ``CategorizationService.bulk_categorize``.

    Validated at every boundary (CLI, MCP). The service refuses untyped dicts.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    transaction_id: str = Field(min_length=1, max_length=64)
    category: str = Field(min_length=1, max_length=100)
    subcategory: str | None = Field(default=None, min_length=1, max_length=100)


def _validate_items(
    raw: object,
) -> tuple[list[BulkCategorizationItem], list[dict[str, str]]]:
    """Validate a raw decoded JSON array into typed items + per-row errors.

    Per-item validation: a malformed row contributes an ``error_details`` entry
    but does not abort the batch. Callers merge ``parse_errors`` into the
    final ``BulkCategorizationResult.error_details`` so the response envelope
    surfaces every failure together.
    """
    if not isinstance(raw, list):
        raise ValueError("Input must be a JSON array of categorization items")

    items: list[BulkCategorizationItem] = []
    errors: list[dict[str, str]] = []
    for index, row in enumerate(raw):
        if not isinstance(row, dict):
            errors.append({
                "transaction_id": "(missing)",
                "reason": f"Row {index} is not an object",
            })
            continue
        try:
            items.append(BulkCategorizationItem.model_validate(row))
        except ValidationError as e:
            txn_id = str(row.get("transaction_id") or "").strip() or "(missing)"
            reason = "; ".join(
                f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
                for err in e.errors()
            )
            errors.append({"transaction_id": txn_id, "reason": reason})
    return items, errors
```

- [ ] **Step 1.4: Run the tests and confirm they pass**

```bash
uv run pytest tests/moneybin/test_categorization_service_bulk.py -v
uv run pyright src/moneybin/services/categorization_service.py tests/moneybin/test_categorization_service_bulk.py
```

Expected: all tests PASS. Pyright clean.

- [ ] **Step 1.5: Commit**

```bash
git add src/moneybin/services/categorization_service.py tests/moneybin/test_categorization_service_bulk.py
git commit -m "Add BulkCategorizationItem model and _validate_items helper"
```

---

## Task 2: Tighten `bulk_categorize` signature to typed items

**Files:**
- Modify: `src/moneybin/services/categorization_service.py:276-460` (`bulk_categorize`)
- Modify: `tests/moneybin/test_categorization_service.py` (existing dict-based tests)

The service stops accepting untyped dicts. This is a breaking change with no shim. The behavior of the function does not change yet — we are only swapping the input shape.

- [ ] **Step 2.1: Modify `bulk_categorize` signature and Phase 1 logic**

In `src/moneybin/services/categorization_service.py` change the signature and Phase 1:

Old:
```python
def bulk_categorize(
    self, items: Sequence[Mapping[str, str | None]]
) -> BulkCategorizationResult:
    ...
    # Phase 1 — validate and partition input
    valid_items: list[tuple[str, str, str | None]] = []
    for item in items:
        txn_id = (item.get("transaction_id") or "").strip()
        category = (item.get("category") or "").strip()
        if not txn_id or not category:
            skipped += 1
            error_details.append({
                "transaction_id": txn_id or "(missing)",
                "reason": "Missing transaction_id or category",
            })
            continue
        subcategory = (item.get("subcategory") or "").strip() or None
        valid_items.append((txn_id, category, subcategory))
```

New:
```python
def bulk_categorize(
    self, items: Sequence[BulkCategorizationItem]
) -> BulkCategorizationResult:
    ...
    # Phase 1 — items are already validated by the boundary (CLI/MCP).
    valid_items: list[tuple[str, str, str | None]] = [
        (i.transaction_id, i.category, i.subcategory) for i in items
    ]
```

The `Sequence[Mapping[str, str | None]]` import (`Mapping`) becomes unused — remove from imports if no other usage remains in the file.

- [ ] **Step 2.2: Migrate existing tests**

In `tests/moneybin/test_categorization_service.py`, find every call to `bulk_categorize(...)` that passes literal dicts. Replace each dict with `BulkCategorizationItem(...)`. Add the import at the top:

```python
from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    CategorizationService,
)
```

For tests that previously asserted dict-validation behavior (e.g., a test passing `{"transaction_id": "", ...}` and expecting `skipped += 1`), delete those tests — that responsibility now lives in `_validate_items` (covered by Task 1's tests). The service no longer skips invalid rows; it trusts its input.

Run `grep -n "bulk_categorize" tests/moneybin/test_categorization_service.py` to enumerate the call sites before editing.

- [ ] **Step 2.3: Run the migrated tests and confirm they pass**

```bash
uv run pytest tests/moneybin/test_categorization_service.py -v
```

Expected: PASS. Any test that previously expected the service to skip-and-continue on a missing field should be deleted, not adapted — that contract moved.

- [ ] **Step 2.4: Type-check**

```bash
uv run pyright src/moneybin/services/categorization_service.py tests/moneybin/test_categorization_service.py
```

Expected: clean.

- [ ] **Step 2.5: Commit**

```bash
git add src/moneybin/services/categorization_service.py tests/moneybin/test_categorization_service.py
git commit -m "Require BulkCategorizationItem input to bulk_categorize"
```

---

## Task 3: Add `rules_override` and `txn_row_override` to `find_matching_rule`

**Files:**
- Modify: `src/moneybin/services/categorization_service.py:586-616` (`find_matching_rule`)
- Modify: `tests/moneybin/test_categorization_service.py`

Lets the bulk path skip the per-item txn-row SELECT and the per-item active-rules SELECT. Pure refactor — no behavior change when the new params are `None`.

- [ ] **Step 3.1: Write failing tests for the override params**

Append to `tests/moneybin/test_categorization_service.py` (adjust imports at top):

```python
def test_find_matching_rule_uses_rules_override(
    db_with_categorization: Database,
) -> None:
    """When rules_override is provided, the rules table is not queried."""
    svc = CategorizationService(db_with_categorization)
    # Insert a transaction we want to match, but the rules table is empty.
    db_with_categorization.execute(
        f"""
        INSERT INTO {FCT_TRANSACTIONS.full_name}
        (transaction_id, account_id, posted_date, description, amount, source_system)
        VALUES ('csv_test', 'acct_1', '2026-01-01', 'STARBUCKS COFFEE', -5.0, 'csv')
        """
    )
    # Rule passed as override only.
    override_rules = [
        ("rule_1", "STARBUCKS", "contains", None, None, None, "Food", "Coffee", "user")
    ]
    match = svc.find_matching_rule("csv_test", rules_override=override_rules)
    assert match is not None
    assert match[1] == "Food"
    assert match[2] == "Coffee"


def test_find_matching_rule_uses_txn_row_override(
    db_with_categorization: Database,
) -> None:
    """When txn_row_override is provided, fct_transactions is not queried."""
    svc = CategorizationService(db_with_categorization)
    # Note: no INSERT into fct_transactions for this id.
    override_rules = [
        ("rule_1", "AMZN", "contains", None, None, None, "Shopping", None, "user")
    ]
    match = svc.find_matching_rule(
        "ghost_txn",
        rules_override=override_rules,
        txn_row_override=("AMZN MARKETPLACE", -42.0, "acct_1"),
    )
    assert match is not None
    assert match[1] == "Shopping"
```

(Use whichever fixture name is canonical in this test file for a DuckDB-backed `Database`. Check the file's existing fixtures with `grep "def db_" tests/moneybin/test_categorization_service.py` — adapt names to match.)

- [ ] **Step 3.2: Run the tests and confirm they fail**

```bash
uv run pytest tests/moneybin/test_categorization_service.py -k find_matching_rule -v
```

Expected: FAIL with TypeError on unexpected keyword argument.

- [ ] **Step 3.3: Implement the overrides**

Edit `find_matching_rule` in `src/moneybin/services/categorization_service.py`:

```python
def find_matching_rule(
    self,
    transaction_id: str,
    *,
    rules_override: list[tuple[Any, ...]] | None = None,
    txn_row_override: tuple[str, float | None, str | None] | None = None,
) -> tuple[str, str, str | None, str] | None:
    """Return the first active rule matching this transaction, or ``None``.

    Result tuple is ``(rule_id, category, subcategory, created_by)``.
    Single-transaction variant of :meth:`apply_rules`.

    The bulk path supplies pre-loaded rule rows and txn metadata via
    ``rules_override`` and ``txn_row_override`` so this function issues no
    queries during a bulk loop. Both default to ``None`` for non-bulk callers.
    """
    if txn_row_override is not None:
        description, amount, account_id = txn_row_override
    else:
        try:
            txn_row = self._db.execute(
                f"SELECT description, amount, account_id "
                f"FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        if not txn_row or not txn_row[0]:
            return None
        description, amount, account_id = txn_row
    if not description:
        return None

    rules = rules_override if rules_override is not None else self.fetch_active_rules()
    if not rules:
        return None
    return self.match_first_rule(
        rules,
        str(description),
        float(amount) if amount is not None else None,
        str(account_id) if account_id is not None else None,
    )
```

- [ ] **Step 3.4: Run the tests and confirm they pass**

```bash
uv run pytest tests/moneybin/test_categorization_service.py -k find_matching_rule -v
uv run pyright src/moneybin/services/categorization_service.py
```

Expected: PASS. Pyright clean.

- [ ] **Step 3.5: Commit**

```bash
git add src/moneybin/services/categorization_service.py tests/moneybin/test_categorization_service.py
git commit -m "Add rules_override and txn_row_override to find_matching_rule"
```

---

## Task 4: Add `BulkRecordingContext` dataclass

**Files:**
- Modify: `src/moneybin/services/auto_rule_service.py`
- Create: `tests/moneybin/test_bulk_recording_context.py`

The context owns pre-loaded txn rows, active rules, merchant mappings, and ordering-aware merchant invalidation. No `record_categorization` plumbing yet — that's Task 5.

- [ ] **Step 4.1: Write failing tests**

Create `tests/moneybin/test_bulk_recording_context.py`:

```python
"""Unit tests for BulkRecordingContext."""

from __future__ import annotations

from moneybin.services.auto_rule_service import BulkRecordingContext, TxnRow


def _merchant(
    merchant_id: str,
    raw_pattern: str,
    match_type: str,
    canonical: str,
    category: str,
    subcategory: str | None = None,
) -> tuple[str, str, str, str, str, str | None]:
    return (merchant_id, raw_pattern, match_type, canonical, category, subcategory)


class TestTxnRowLookup:
    def test_txn_row_for_returns_loaded_row(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={
                "csv_a": TxnRow(
                    description="STARBUCKS", amount=-5.0, account_id="acct_1"
                )
            },
            active_rules=[],
            merchant_mappings=[],
        )
        row = ctx.txn_row_for("csv_a")
        assert row is not None
        assert row.description == "STARBUCKS"

    def test_description_for_returns_description(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={
                "csv_a": TxnRow(description="STARBUCKS", amount=-5.0, account_id=None)
            },
            active_rules=[],
            merchant_mappings=[],
        )
        assert ctx.description_for("csv_a") == "STARBUCKS"

    def test_description_for_returns_none_when_missing(self) -> None:
        ctx = BulkRecordingContext(txn_rows={}, active_rules=[], merchant_mappings=[])
        assert ctx.description_for("missing") is None


class TestRegisterNewMerchant:
    def test_inserts_before_first_regex(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "amzn", "exact", "AMZN", "Shopping"),
                _merchant("m2", "amzn", "contains", "AMZN", "Shopping"),
                _merchant("m3", ".*coffee.*", "regex", "Coffee", "Food"),
            ],
        )
        new = _merchant("m4", "starbucks", "contains", "Starbucks", "Food", "Coffee")
        ctx.register_new_merchant(new)
        # Inserted before the first regex entry (index 2).
        assert ctx.merchant_mappings[2] == new
        assert ctx.merchant_mappings[3][0] == "m3"

    def test_appends_when_no_regex(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "amzn", "exact", "AMZN", "Shopping"),
            ],
        )
        new = _merchant("m2", "starbucks", "contains", "Starbucks", "Food")
        ctx.register_new_merchant(new)
        assert ctx.merchant_mappings[-1] == new


class TestMerchantMappingCovers:
    def test_returns_true_on_contains_category_match(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "AMZN", "contains", "AMZN", "Shopping", None),
            ],
        )
        assert ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Shopping", None)

    def test_returns_false_on_category_mismatch(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "AMZN", "contains", "AMZN", "Shopping", None),
            ],
        )
        assert not ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Food", None)

    def test_subcategory_mismatch_means_no_cover(self) -> None:
        ctx = BulkRecordingContext(
            txn_rows={},
            active_rules=[],
            merchant_mappings=[
                _merchant("m1", "AMZN", "contains", "AMZN", "Shopping", "Books"),
            ],
        )
        assert not ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Shopping", None)
        assert ctx.merchant_mapping_covers("AMZN MARKETPLACE", "Shopping", "Books")
```

- [ ] **Step 4.2: Run the tests and confirm they fail**

```bash
uv run pytest tests/moneybin/test_bulk_recording_context.py -v
```

Expected: ImportError — `BulkRecordingContext` and `TxnRow` don't exist yet.

- [ ] **Step 4.3: Implement `BulkRecordingContext` and `TxnRow`**

In `src/moneybin/services/auto_rule_service.py`, add near the top of the file (after the existing imports / dataclasses):

```python
from dataclasses import dataclass, field

from moneybin.services._text import normalize_description
from moneybin.services._matching import (
    matches_pattern,
)  # adjust if matches_pattern lives elsewhere — confirm path before writing
```

(If `matches_pattern` is imported from a different module, use that path. Inspect with `grep -rn "def matches_pattern" src/moneybin/`.)

Add the two dataclasses:

```python
@dataclass(slots=True, frozen=True)
class TxnRow:
    """Pre-loaded transaction columns needed by the bulk path."""

    description: str | None
    amount: float | None
    account_id: str | None


@dataclass(slots=True)
class BulkRecordingContext:
    """In-memory caches threaded through ``record_categorization`` during a bulk loop.

    Owns the data that today's per-item helpers re-fetch from the database.
    Mutators (``register_new_merchant``) preserve the same ordering invariants
    that ``_fetch_merchants`` produces (exact → contains → regex), so ``cover``
    checks see new merchants in their canonical match position.
    """

    txn_rows: dict[str, TxnRow]
    active_rules: list[tuple[Any, ...]]
    merchant_mappings: list[tuple[Any, ...]]
    new_merchant_count: int = field(default=0)

    def txn_row_for(self, transaction_id: str) -> TxnRow | None:
        return self.txn_rows.get(transaction_id)

    def description_for(self, transaction_id: str) -> str | None:
        row = self.txn_rows.get(transaction_id)
        return row.description if row else None

    def register_new_merchant(self, merchant_row: tuple[Any, ...]) -> None:
        """Insert at the canonical match-order position (before the first regex)."""
        insert_at = next(
            (i for i, m in enumerate(self.merchant_mappings) if m[2] == "regex"),
            len(self.merchant_mappings),
        )
        self.merchant_mappings.insert(insert_at, merchant_row)
        self.new_merchant_count += 1

    def merchant_mapping_covers(
        self, pattern: str, category: str, subcategory: str | None
    ) -> bool:
        """Mirror of ``AutoRuleService._merchant_mapping_covers`` against the cached list."""
        for merchant in self.merchant_mappings:
            _mid, raw_pattern, match_type, _canonical, m_cat, m_subcat = merchant
            if str(m_cat) != category:
                continue
            if (m_subcat if m_subcat is None else str(m_subcat)) != subcategory:
                continue
            if matches_pattern(
                pattern, str(raw_pattern), str(match_type or "contains")
            ):
                return True
        return False
```

The `Any` import comes from `typing` — verify the file already imports it; add if not.

- [ ] **Step 4.4: Run the tests and confirm they pass**

```bash
uv run pytest tests/moneybin/test_bulk_recording_context.py -v
uv run pyright src/moneybin/services/auto_rule_service.py tests/moneybin/test_bulk_recording_context.py
```

Expected: PASS. Pyright clean.

- [ ] **Step 4.5: Commit**

```bash
git add src/moneybin/services/auto_rule_service.py tests/moneybin/test_bulk_recording_context.py
git commit -m "Add BulkRecordingContext for bulk-loop cache + invalidation"
```

---

## Task 5: Thread context through `record_categorization` and helpers

**Files:**
- Modify: `src/moneybin/services/auto_rule_service.py:_extract_pattern, _active_rule_covers_transaction, _merchant_mapping_covers, record_categorization`
- Create: `tests/moneybin/test_auto_rule_service_context.py`

When `context` is provided, helpers consult it instead of issuing queries. When `context=None`, current behavior is preserved.

- [ ] **Step 5.1: Write failing tests**

Create `tests/moneybin/test_auto_rule_service_context.py`:

```python
"""Unit tests for AutoRuleService context-aware helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin.services.auto_rule_service import (
    AutoRuleService,
    BulkRecordingContext,
    TxnRow,
)


@pytest.fixture
def db_mock() -> MagicMock:
    """A mock Database whose ``execute`` records every call."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.fetchall.return_value = []
    return db


def _merchant(
    merchant_id: str,
    raw_pattern: str,
    match_type: str,
    canonical: str,
    category: str,
    subcategory: str | None = None,
) -> tuple[str, str, str, str, str, str | None]:
    return (merchant_id, raw_pattern, match_type, canonical, category, subcategory)


class TestRecordCategorizationWithContext:
    def test_no_db_queries_when_context_provided(self, db_mock: MagicMock) -> None:
        """The bulk path issues zero pattern/coverage queries when context is set."""
        ctx = BulkRecordingContext(
            txn_rows={
                "csv_a": TxnRow(description="STARBUCKS", amount=-5.0, account_id=None)
            },
            active_rules=[],  # no rule will cover the txn
            merchant_mappings=[],  # no merchant will cover the txn
        )
        svc = AutoRuleService(db_mock)
        # Stub away the proposal-side writes so we can isolate the read paths.
        svc._upsert_proposal = MagicMock()  # type: ignore[method-assign]

        svc.record_categorization("csv_a", "Food", subcategory=None, context=ctx)

        # No SELECT against transaction_categories, fct_transactions, merchants, or
        # categorization_rules from the read paths in record_categorization.
        for call in db_mock.execute.call_args_list:
            sql = str(call.args[0]).lower()
            assert "from fct_transactions" not in sql
            assert "from merchants" not in sql
            assert "from categorization_rules" not in sql
            assert "select merchant_id from " not in sql

    def test_falls_back_when_context_none(self, db_mock: MagicMock) -> None:
        svc = AutoRuleService(db_mock)
        svc._upsert_proposal = MagicMock()  # type: ignore[method-assign]
        svc.record_categorization("csv_a", "Food", subcategory=None, context=None)
        # At least one DB query was issued.
        assert db_mock.execute.called
```

- [ ] **Step 5.2: Run the tests and confirm they fail**

```bash
uv run pytest tests/moneybin/test_auto_rule_service_context.py -v
```

Expected: FAIL — `record_categorization` does not yet accept `context`, and even when ignored, the helpers still issue DB queries.

- [ ] **Step 5.3: Add `context` to all four methods**

In `src/moneybin/services/auto_rule_service.py`:

(a) `record_categorization` signature gains `*, context: BulkRecordingContext | None = None`. Pass `context` into `_extract_pattern`, `_active_rule_covers_transaction`, `_merchant_mapping_covers`.

(b) `_extract_pattern`:

```python
def _extract_pattern(
    self,
    transaction_id: str,
    *,
    merchant_id: str | None = None,
    context: BulkRecordingContext | None = None,
) -> tuple[str, str] | None:
    if merchant_id is None:
        row = self._db.execute(
            f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
        merchant_id = str(row[0]) if row and row[0] else None
    if merchant_id:
        m = self._db.execute(
            f"SELECT raw_pattern, match_type FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        if m and m[0]:
            return str(m[0]), str(m[1] or "contains")

    if context is not None:
        description = context.description_for(transaction_id)
    else:
        desc_row = self._db.execute(
            f"SELECT description FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
        description = str(desc_row[0]) if desc_row and desc_row[0] else None
    if not description:
        return None
    cleaned = normalize_description(description)
    if not cleaned:
        return None
    return cleaned, "contains"
```

(c) `_active_rule_covers_transaction`:

```python
def _active_rule_covers_transaction(
    self,
    transaction_id: str,
    category: str,
    subcategory: str | None,
    *,
    context: BulkRecordingContext | None = None,
) -> bool:
    rules_override = context.active_rules if context is not None else None
    txn_row = context.txn_row_for(transaction_id) if context is not None else None
    txn_row_override = (
        (txn_row.description or "", txn_row.amount, txn_row.account_id)
        if txn_row is not None and txn_row.description
        else None
    )
    match = CategorizationService(self._db).find_matching_rule(
        transaction_id,
        rules_override=rules_override,
        txn_row_override=txn_row_override,
    )
    if match is None:
        return False
    _rule_id, matched_category, matched_subcategory, _created_by = match
    return matched_category == category and matched_subcategory == subcategory
```

(d) `_merchant_mapping_covers`:

```python
def _merchant_mapping_covers(
    self,
    pattern: str,
    category: str,
    subcategory: str | None,
    *,
    context: BulkRecordingContext | None = None,
) -> bool:
    if context is not None:
        return context.merchant_mapping_covers(pattern, category, subcategory)
    try:
        rows = self._db.execute(
            f"""
            SELECT raw_pattern, match_type, category, subcategory
            FROM {MERCHANTS.full_name}
            """
        ).fetchall()
    except duckdb.CatalogException:
        return False
    for raw_pattern, m_type, m_cat, m_subcat in rows:
        if str(m_cat) != category:
            continue
        if (m_subcat if m_subcat is None else str(m_subcat)) != subcategory:
            continue
        if matches_pattern(pattern, str(raw_pattern), str(m_type or "contains")):
            return True
    return False
```

Update the call sites inside `record_categorization` to pass `context=context` to all three helpers.

- [ ] **Step 5.4: Run all auto_rule_service tests**

```bash
uv run pytest tests/moneybin/test_auto_rule_service_context.py tests/moneybin/test_auto_rule_service.py -v
uv run pyright src/moneybin/services/auto_rule_service.py
```

Expected: PASS. Existing `test_auto_rule_service.py` keeps passing because `context=None` is the default and behavior is preserved.

- [ ] **Step 5.5: Commit**

```bash
git add src/moneybin/services/auto_rule_service.py tests/moneybin/test_auto_rule_service_context.py
git commit -m "Thread BulkRecordingContext through record_categorization helpers"
```

---

## Task 6: Wire context into `bulk_categorize`

**Files:**
- Modify: `src/moneybin/services/categorization_service.py:bulk_categorize`
- Modify: `tests/moneybin/test_categorization_service_bulk.py` (add query-count test)

`bulk_categorize` builds the context once and threads it into every `record_categorization` call. The existing per-item duplicate `cached_merchants.insert(...)` site moves into `ctx.register_new_merchant(...)`.

- [ ] **Step 6.1: Write a failing query-count regression test**

Append to `tests/moneybin/test_categorization_service_bulk.py`:

```python
from unittest.mock import MagicMock

from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    CategorizationService,
)


class TestBulkQueryCount:
    """Bulk loop must issue O(items) queries, not O(5 * items).

    See docs/specs/categorize-bulk.md §Requirements item 7.
    """

    def test_per_item_path_does_not_query_rules_or_merchants(
        self, db_mock_bulk_friendly: MagicMock
    ) -> None:
        items = [
            BulkCategorizationItem(transaction_id=f"csv_{i}", category="Food")
            for i in range(5)
        ]
        svc = CategorizationService(db_mock_bulk_friendly)
        result = svc.bulk_categorize(items)
        assert result.applied + result.errors + result.skipped == len(items)

        rule_queries = sum(
            1
            for call in db_mock_bulk_friendly.execute.call_args_list
            if "from categorization_rules" in str(call.args[0]).lower()
        )
        merchant_queries = sum(
            1
            for call in db_mock_bulk_friendly.execute.call_args_list
            if "from merchants" in str(call.args[0]).lower()
        )
        # Exactly one rules fetch + one merchants fetch for the whole batch.
        assert rule_queries == 1
        assert merchant_queries == 1
```

The `db_mock_bulk_friendly` fixture lives in `tests/moneybin/conftest.py` if it exists, or this test file. If it does not exist, add at the top of the test file:

```python
@pytest.fixture
def db_mock_bulk_friendly() -> MagicMock:
    """Mock Database that returns plausible empty results for bulk_categorize."""
    db = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None
    db.execute.return_value = cursor
    return db
```

- [ ] **Step 6.2: Run the test and confirm it fails**

```bash
uv run pytest tests/moneybin/test_categorization_service_bulk.py::TestBulkQueryCount -v
```

Expected: FAIL — current code issues N merchant SELECTs (one per item via `_merchant_mapping_covers`) and N rule SELECTs (via `find_matching_rule`).

- [ ] **Step 6.3: Refactor `bulk_categorize` to build and use the context**

In `src/moneybin/services/categorization_service.py` change Phase 2/3/4:

Old Phase 2/3:
```python
# Phase 2 — batch-fetch descriptions
txn_ids = [v[0] for v in valid_items]
placeholders = ",".join(["?"] * len(txn_ids))
descriptions: dict[str, str | None] = {}
try:
    rows = self._db.execute(
        f"""
        SELECT transaction_id, description
        FROM {FCT_TRANSACTIONS.full_name}
        WHERE transaction_id IN ({placeholders})
        """,
        txn_ids,
    ).fetchall()
    descriptions = {row[0]: row[1] for row in rows}
except Exception:  # noqa: BLE001
    logger.warning("Could not batch-fetch descriptions", exc_info=True)

# Phase 3 — fetch merchants once
try:
    cached_merchants = _fetch_merchants(self._db)
except Exception:  # noqa: BLE001
    logger.warning("Could not batch-fetch merchants", exc_info=True)
    cached_merchants = None
```

New Phase 2/3 (build the context):
```python
from moneybin.services.auto_rule_service import (
    AutoRuleService,
    BulkRecordingContext,
    TxnRow,
)

# Phase 2 — batch-fetch txn rows (description + amount + account_id)
txn_ids = [v[0] for v in valid_items]
placeholders = ",".join(["?"] * len(txn_ids))
txn_rows: dict[str, TxnRow] = {}
try:
    rows = self._db.execute(
        f"""
        SELECT transaction_id, description, amount, account_id
        FROM {FCT_TRANSACTIONS.full_name}
        WHERE transaction_id IN ({placeholders})
        """,  # noqa: S608 — FCT_TRANSACTIONS is a compile-time TableRef constant; values are parameterized
        txn_ids,
    ).fetchall()
    txn_rows = {
        row[0]: TxnRow(
            description=row[1],
            amount=float(row[2]) if row[2] is not None else None,
            account_id=str(row[3]) if row[3] is not None else None,
        )
        for row in rows
    }
except Exception:  # noqa: BLE001 — best-effort; degrades to no merchant resolution
    logger.warning("Could not batch-fetch transaction rows", exc_info=True)

# Phase 3 — fetch merchants and active rules once for the whole batch
try:
    cached_merchants = _fetch_merchants(self._db)
except Exception:  # noqa: BLE001 — best-effort
    logger.warning("Could not batch-fetch merchants", exc_info=True)
    cached_merchants = []
try:
    cached_rules = self.fetch_active_rules()
except Exception:  # noqa: BLE001 — best-effort
    logger.warning("Could not batch-fetch active rules", exc_info=True)
    cached_rules = []

ctx = BulkRecordingContext(
    txn_rows=txn_rows,
    active_rules=cached_rules,
    merchant_mappings=list(cached_merchants),
)
```

(`_fetch_merchants` already returns a list of tuples in canonical order — wrap with `list(...)` to ensure mutability.)

In Phase 4, replace per-item logic. Old:
```python
description = descriptions.get(txn_id)
if description and cached_merchants is not None:
    try:
        existing = _match_description(description, cached_merchants)
        ...

AutoRuleService(self._db).record_categorization(
    txn_id, category, subcategory=subcategory, merchant_id=merchant_id,
)
```

New:
```python
description = ctx.description_for(txn_id)
existing: dict[str, Any] | None = None
if description and ctx.merchant_mappings:
    try:
        existing = _match_description(description, ctx.merchant_mappings)
        if existing:
            merchant_id = existing["merchant_id"]
    except Exception:  # noqa: BLE001 — merchant lookup is best-effort
        logger.debug(f"Could not resolve merchant for {txn_id}", exc_info=True)

try:
    AutoRuleService(self._db).record_categorization(
        txn_id,
        category,
        subcategory=subcategory,
        merchant_id=merchant_id,
        context=ctx,
    )
except Exception:  # noqa: BLE001 — auto-rule learning is best-effort
    logger.warning("auto-rule recording failed", exc_info=True)
```

The new-merchant branch becomes:
```python
if merchant_id is None and description and ctx.merchant_mappings is not None:
    try:
        normalized = normalize_description(description)
        if normalized:
            merchant_id = self.create_merchant(
                normalized,
                normalized,
                match_type="contains",
                category=category,
                subcategory=subcategory,
                created_by="ai",
            )
            merchants_created += 1
            new_row = (
                merchant_id,
                normalized,
                "contains",
                normalized,
                category,
                subcategory,
            )
            ctx.register_new_merchant(new_row)
    except Exception:  # noqa: BLE001 — merchant resolution is best-effort
        logger.debug(f"Could not create merchant for {txn_id}", exc_info=True)
```

The hand-coded `cached_merchants.insert(insert_at, new_row)` block goes away — the context owns that logic now.

- [ ] **Step 6.4: Run the bulk tests**

```bash
uv run pytest tests/moneybin/test_categorization_service_bulk.py tests/moneybin/test_categorization_service.py -v
uv run pyright src/moneybin/services/categorization_service.py
```

Expected: query-count test PASSES. All previously-passing tests still pass.

- [ ] **Step 6.5: Run the full integration suite to catch any regression**

```bash
uv run pytest tests/integration/ -v
```

Expected: PASS. Pay attention to anything touching `categorize.bulk` end-to-end.

- [ ] **Step 6.6: Commit**

```bash
git add src/moneybin/services/categorization_service.py tests/moneybin/test_categorization_service_bulk.py
git commit -m "Build BulkRecordingContext once and thread into bulk_categorize"
```

---

## Task 7: Add observability metrics

**Files:**
- Modify: `src/moneybin/metrics/registry.py`
- Modify: `src/moneybin/services/categorization_service.py:bulk_categorize`

Per `docs/specs/observability.md` and AGENTS.md, specs touching app code must wire metrics.

- [ ] **Step 7.1: Inspect existing metric definitions**

Run `grep -n "Counter\|Histogram\|Gauge" src/moneybin/metrics/registry.py | head -20` and read enough of the file to mirror the existing pattern (label sets, naming, units).

- [ ] **Step 7.2: Register the three new metrics**

In `src/moneybin/metrics/registry.py`, follow the file's existing pattern. Add (adapt names if the file uses a different naming convention — the goal is consistency with neighbors, not the exact strings below):

```python
categorize_bulk_items_total = Counter(
    "moneybin_categorize_bulk_items_total",
    "Number of items processed by bulk_categorize, by outcome",
    ["outcome"],  # applied | skipped | error
)

categorize_bulk_duration_seconds = Histogram(
    "moneybin_categorize_bulk_duration_seconds",
    "Wall-clock duration of CategorizationService.bulk_categorize calls",
)

categorize_bulk_errors_total = Counter(
    "moneybin_categorize_bulk_errors_total",
    "Number of bulk_categorize calls that raised before returning a result",
)
```

- [ ] **Step 7.3: Wire emission in `bulk_categorize`**

At the top of `bulk_categorize`, wrap the body with the histogram timer and emit counters before returning. Use the `track_duration`/`@tracked` helpers from `observability.md` if they exist; otherwise:

```python
from time import perf_counter
from moneybin.metrics.registry import (
    categorize_bulk_items_total,
    categorize_bulk_duration_seconds,
    categorize_bulk_errors_total,
)


def bulk_categorize(
    self, items: Sequence[BulkCategorizationItem]
) -> BulkCategorizationResult:
    start = perf_counter()
    try:
        # ... existing body ...
        result = BulkCategorizationResult(
            applied=applied,
            skipped=skipped,
            errors=errors,
            error_details=error_details,
            merchants_created=merchants_created,
        )
    except Exception:
        categorize_bulk_errors_total.inc()
        raise
    finally:
        categorize_bulk_duration_seconds.observe(perf_counter() - start)
    categorize_bulk_items_total.labels(outcome="applied").inc(applied)
    categorize_bulk_items_total.labels(outcome="skipped").inc(skipped)
    categorize_bulk_items_total.labels(outcome="error").inc(errors)
    return result
```

(If the registry uses a `@tracked` decorator pattern, prefer that — match the surrounding code's style.)

- [ ] **Step 7.4: Run tests**

```bash
uv run pytest tests/moneybin/test_categorization_service.py tests/moneybin/test_categorization_service_bulk.py -v
```

Expected: PASS.

- [ ] **Step 7.5: Commit**

```bash
git add src/moneybin/metrics/registry.py src/moneybin/services/categorization_service.py
git commit -m "Wire bulk_categorize observability metrics"
```

---

## Task 8: New `categorize bulk` CLI command

**Files:**
- Modify: `src/moneybin/cli/commands/categorize.py`
- Create: `tests/integration/test_categorize_bulk_cli.py`

Reads JSON from `--input <path>` or stdin (`-` sentinel), validates per-item via `_validate_items`, calls the service, prints either a human summary or the response envelope as JSON, exits non-zero on partial failure.

- [ ] **Step 8.1: Inspect a similar existing CLI command for the table/json pattern**

Read `auto_review_cmd` in `src/moneybin/cli/commands/categorize.py` (around line 107) for the `--output table|json` shape. Mirror it.

- [ ] **Step 8.2: Write failing integration tests**

Create `tests/integration/test_categorize_bulk_cli.py`:

```python
"""Integration tests for `moneybin categorize bulk` CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app

pytestmark = pytest.mark.integration

runner = CliRunner(mix_stderr=False)


def _seed_one_transaction(db_path: Path) -> str:
    """Insert one fct_transactions row and return its id. Uses moneybin's Database."""
    from moneybin.database import Database
    from moneybin.tables import FCT_TRANSACTIONS

    db = Database(db_path, no_auto_upgrade=False)
    txn_id = "csv_test_001"
    db.execute(
        f"""
        INSERT INTO {FCT_TRANSACTIONS.full_name}
        (transaction_id, account_id, posted_date, description, amount, source_system)
        VALUES (?, 'acct_1', '2026-01-01', 'STARBUCKS COFFEE', -5.0, 'csv')
        """,
        [txn_id],
    )
    return txn_id


class TestCategorizeBulkCLI:
    def test_file_input_applies_categorizations(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setenv("MONEYBIN_DATABASE__PATH", str(db_path))
        txn_id = _seed_one_transaction(db_path)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(
            json.dumps([
                {"transaction_id": txn_id, "category": "Food", "subcategory": "Coffee"},
            ])
        )

        result = runner.invoke(app, ["categorize", "bulk", "--input", str(cats_file)])
        assert result.exit_code == 0, result.stderr

    def test_stdin_input(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setenv("MONEYBIN_DATABASE__PATH", str(db_path))
        txn_id = _seed_one_transaction(db_path)

        payload = json.dumps([{"transaction_id": txn_id, "category": "Food"}])
        result = runner.invoke(app, ["categorize", "bulk", "-"], input=payload)
        assert result.exit_code == 0, result.stderr

    def test_json_output_returns_envelope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setenv("MONEYBIN_DATABASE__PATH", str(db_path))
        txn_id = _seed_one_transaction(db_path)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(
            json.dumps([
                {"transaction_id": txn_id, "category": "Food"},
            ])
        )

        result = runner.invoke(
            app,
            ["categorize", "bulk", "--input", str(cats_file), "--output", "json"],
        )
        assert result.exit_code == 0
        envelope = json.loads(result.stdout)
        assert envelope["data"]["applied"] == 1
        assert envelope["data"]["error_details"] == []

    def test_partial_failure_exits_nonzero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setenv("MONEYBIN_DATABASE__PATH", str(db_path))
        txn_id = _seed_one_transaction(db_path)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(
            json.dumps([
                {"transaction_id": txn_id, "category": "Food"},
                {"transaction_id": "", "category": "X"},  # invalid
            ])
        )

        result = runner.invoke(
            app, ["categorize", "bulk", "--input", str(cats_file), "--output", "json"]
        )
        assert result.exit_code == 1
        envelope = json.loads(result.stdout)
        assert any(
            "transaction_id" in e["reason"] for e in envelope["data"]["error_details"]
        )

    def test_malformed_top_level_exits_nonzero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setenv("MONEYBIN_DATABASE__PATH", str(db_path))
        _seed_one_transaction(db_path)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(json.dumps({"items": []}))  # not a list

        result = runner.invoke(app, ["categorize", "bulk", "--input", str(cats_file)])
        assert result.exit_code == 1

    def test_missing_file_exits_two(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = tmp_path / "test.duckdb"
        monkeypatch.setenv("MONEYBIN_DATABASE__PATH", str(db_path))
        _seed_one_transaction(db_path)

        result = runner.invoke(
            app, ["categorize", "bulk", "--input", str(tmp_path / "missing.json")]
        )
        assert result.exit_code == 2
```

- [ ] **Step 8.3: Run the tests and confirm they fail**

```bash
uv run pytest tests/integration/test_categorize_bulk_cli.py -v
```

Expected: FAIL — the `bulk` command does not exist.

- [ ] **Step 8.4: Add the command**

In `src/moneybin/cli/commands/categorize.py`, add the new command (placement: alphabetical or grouped with other write commands — match neighbors):

```python
@app.command("bulk")
def bulk_cmd(
    input_path: str | None = typer.Option(
        None,
        "--input",
        help="Path to a JSON file containing categorization items. Use '-' on stdin instead.",
    ),
    stdin_sentinel: str | None = typer.Argument(
        None,
        help="Pass '-' to read JSON from stdin.",
    ),
    output: str = typer.Option(
        "table", "--output", help="Output format: table or json"
    ),
) -> None:
    """Bulk-assign categories to transactions from a JSON array.

    Read from a file:

      moneybin categorize bulk --input cats.json

    Or from stdin:

      cat cats.json | moneybin categorize bulk -

    The input is a JSON array of objects: ``transaction_id`` (required),
    ``category`` (required), and optional ``subcategory``. Per-item
    validation accumulates failures into the result rather than aborting
    the batch. Exit code is 1 if any item failed.
    """
    import json
    import sys

    from moneybin.services.categorization_service import (
        CategorizationService,
        _validate_items,
    )

    if (input_path is None) == (stdin_sentinel != "-"):
        typer.echo(
            "Provide either --input <path> or '-' to read from stdin (not both).",
            err=True,
        )
        raise typer.Exit(2)

    try:
        if input_path is not None:
            with open(input_path, encoding="utf-8") as f:
                raw = json.load(f)
        else:
            raw = json.load(sys.stdin)
    except FileNotFoundError as e:
        typer.echo(f"❌ File not found: {input_path}", err=True)
        raise typer.Exit(2) from e
    except json.JSONDecodeError as e:
        typer.echo(f"❌ Invalid JSON: {e}", err=True)
        raise typer.Exit(1) from e

    try:
        items, parse_errors = _validate_items(raw)
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    with handle_cli_errors() as db:
        result = CategorizationService(db).bulk_categorize(items)

    # Merge parse-time errors into the envelope's error_details.
    result.error_details = parse_errors + result.error_details
    result.skipped += len(parse_errors)

    if output == "json":
        typer.echo(
            json.dumps(result.to_envelope(len(items) + len(parse_errors)).to_dict())
        )
    else:
        logger.info(
            f"✅ Applied {result.applied} | skipped {result.skipped} | errors {result.errors}"
        )
        if result.merchants_created:
            logger.info(f"   Created {result.merchants_created} merchant mappings")
        for err in result.error_details:
            logger.warning(f"⚠️  {err['transaction_id']}: {err['reason']}")

    if result.errors > 0 or result.skipped > 0:
        raise typer.Exit(1)
```

- [ ] **Step 8.5: Run the tests**

```bash
uv run pytest tests/integration/test_categorize_bulk_cli.py -v
uv run pyright src/moneybin/cli/commands/categorize.py
```

Expected: PASS. Pyright clean.

- [ ] **Step 8.6: Verify CLI help text**

```bash
uv run moneybin categorize bulk --help
```

Expected: help text shows `--input`, `--output`, and the `-` stdin sentinel argument.

- [ ] **Step 8.7: Commit**

```bash
git add src/moneybin/cli/commands/categorize.py tests/integration/test_categorize_bulk_cli.py
git commit -m "Add 'moneybin categorize bulk' CLI command"
```

---

## Task 9: Migrate MCP `categorize_bulk` tool to shared `_validate_items`

**Files:**
- Modify: `src/moneybin/mcp/tools/categorize.py:284-306`
- Modify: tests for the MCP tool (find with `grep -n "categorize_bulk\|categorize.bulk" tests/`)

Same partial-success semantics as the CLI; the tool stops accepting raw dicts internally — it routes through `_validate_items` and merges parse errors into `error_details`.

- [ ] **Step 9.1: Read existing MCP tool tests**

Run `grep -rn "categorize_bulk" tests/` and read the relevant test file(s). Identify any test that passes a dict missing required fields and asserts the previous skip-and-continue behavior — those tests still pass because the skip behavior is preserved (now via `_validate_items` instead of inline checks).

- [ ] **Step 9.2: Rewrite the MCP tool**

In `src/moneybin/mcp/tools/categorize.py`, replace the body of `categorize_bulk`:

```python
@mcp_tool(sensitivity="medium")
def categorize_bulk(
    items: list[dict[str, str | None]],
) -> ResponseEnvelope:
    """Assign categories to multiple transactions in one call.

    Each item should have ``transaction_id``, ``category``, and optionally
    ``subcategory``. Validation is per-item: rows that fail validation appear
    in ``error_details`` while valid rows are still applied.

    Args:
        items: List of dicts with transaction_id, category, subcategory.
    """
    from moneybin.services.categorization_service import _validate_items

    if not items:
        return BulkCategorizationResult(
            applied=0, skipped=0, errors=0, error_details=[]
        ).to_envelope(0)

    try:
        validated, parse_errors = _validate_items(items)
    except ValueError as e:
        return BulkCategorizationResult(
            applied=0,
            skipped=len(items),
            errors=0,
            error_details=[{"transaction_id": "(input)", "reason": str(e)}],
        ).to_envelope(len(items))

    result = CategorizationService(get_database()).bulk_categorize(validated)
    result.error_details = parse_errors + result.error_details
    result.skipped += len(parse_errors)
    return result.to_envelope(len(items))
```

- [ ] **Step 9.3: Run MCP tests and confirm they pass**

```bash
uv run pytest -v -k "categorize" tests/
uv run pyright src/moneybin/mcp/tools/categorize.py
```

Expected: PASS. Pyright clean.

- [ ] **Step 9.4: Commit**

```bash
git add src/moneybin/mcp/tools/categorize.py
git commit -m "Route MCP categorize.bulk through shared _validate_items"
```

---

## Task 10: Rewrite `TestAutoRulePipeline::test_import_then_promote_proposal` to use the real CLI

**Files:**
- Modify: `tests/e2e/test_e2e_workflows.py`

Drives the auto-rule pipeline end-to-end: synthetic import → `categorize bulk` → `auto-review` → `auto-confirm` → re-import → assert categorizations were back-filled by the auto-rule.

- [ ] **Step 10.1: Read the existing test**

Run `grep -n "TestAutoRulePipeline\|test_import_then_promote_proposal" tests/e2e/test_e2e_workflows.py` and read enough lines of context to understand the existing setup, fixtures, and where it seeds proposals via raw SQL.

- [ ] **Step 10.2: Rewrite the test body**

Replace the seed-via-`db query` block with a `categorize bulk` subprocess call. The shape (adapt to the existing fixture / helper conventions in this file):

```python
def test_import_then_promote_proposal(self, workflow_env: WorkflowEnv) -> None:
    """Drive the auto-rule pipeline through the real CLI surface.

    Imports synthetic data, bulk-categorizes some transactions, lets the
    auto-rule pipeline propose a rule, approves it, re-imports, and asserts
    that auto-rule categorizations were applied to the new rows.
    """
    workflow_env.run("import", "synthetic")  # or whatever the existing import call is

    # 1. Pick a few transactions to bulk-categorize so the auto-rule pipeline
    #    sees enough trigger events to propose a rule.
    rows = workflow_env.db.execute(
        f"""
        SELECT transaction_id FROM {FCT_TRANSACTIONS.full_name}
        WHERE description ILIKE '%STARBUCKS%'
        LIMIT 5
        """
    ).fetchall()
    assert len(rows) >= 3, "synthetic data must include enough STARBUCKS rows"

    cats = [
        {"transaction_id": r[0], "category": "Food", "subcategory": "Coffee"}
        for r in rows
    ]
    cats_file = workflow_env.tmp_path / "cats.json"
    cats_file.write_text(json.dumps(cats))

    workflow_env.run("categorize", "bulk", "--input", str(cats_file))

    # 2. Review and approve every pending proposal.
    review_out = workflow_env.run_json("categorize", "auto-review", "--output", "json")
    proposals = review_out["data"]["proposals"]
    assert proposals, "auto-rule pipeline did not propose any rule"
    workflow_env.run("categorize", "auto-confirm", "--approve-all")

    # 3. Re-import to replay the now-active auto-rule across the same data.
    workflow_env.run(
        "import", "synthetic", "--force"
    )  # adjust flag to whatever forces re-categorization

    # 4. Assert the auto-rule back-filled categorizations on at least the matching rows.
    auto_hits = workflow_env.db.execute(
        f"""
        SELECT COUNT(*) FROM {TRANSACTION_CATEGORIES.full_name}
        WHERE categorized_by = 'auto_rule' AND category = 'Food'
        """
    ).fetchone()[0]
    assert auto_hits > 0, (
        "promoted auto-rule should back-fill at least one transaction; "
        "check that bulk_categorize → auto-review → auto-confirm wired up correctly"
    )
```

If the existing test uses subprocess invocation rather than a `workflow_env.run` helper, mirror that style instead. The only behavior changes are:
1. Replace the `INSERT INTO app.proposed_rules ...` step with `categorize bulk`.
2. Approve via `auto-confirm` (already exists) instead of seeding-and-approving.

- [ ] **Step 10.3: Run the rewritten test**

```bash
uv run pytest tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline -v
```

Expected: PASS. If it fails on "no proposals were created," check that the CLI bulk path is reaching `record_categorization` (Phase 4 of `bulk_categorize`) and that the synthetic dataset includes enough matching descriptions to trip the auto-rule threshold.

- [ ] **Step 10.4: Commit**

```bash
git add tests/e2e/test_e2e_workflows.py
git commit -m "Drive auto-rule E2E through real CLI instead of seeded SQL"
```

---

## Task 11: Documentation cleanup

**Files:**
- Modify: `private/followups.md` — remove the three resolved items
- Modify: `docs/specs/categorize-bulk.md` — status `draft` → `in-progress`
- Modify: `docs/specs/INDEX.md` — `draft` → `in-progress`
- Modify: `docs/specs/mcp-architecture.md` — note CLI parity for `categorize.bulk`
- Modify: `README.md` — `What Works Today` and roadmap

Per `.claude/rules/shipping.md`, `INDEX.md` updates to `in-progress` when work begins (it's already begun by Task 1) and flips to `implemented` only after merge. We do the `in-progress` flip here; `implemented` happens after PR merges.

- [ ] **Step 11.1: Update spec status**

Edit `docs/specs/categorize-bulk.md`:

```markdown
## Status
in-progress
```

- [ ] **Step 11.2: Update INDEX.md status**

Edit the new line in `docs/specs/INDEX.md`:

```markdown
| [Categorize Bulk](categorize-bulk.md) | Feature | in-progress | `moneybin categorize bulk` CLI parity for `categorize.bulk` MCP tool; shared Pydantic input model with per-item validation; `BulkRecordingContext` to drop per-item duplicate DB lookups in the bulk loop |
```

- [ ] **Step 11.3: Update mcp-architecture.md §5**

Find §5 (CLI Symmetry). After the existing language declaring the principle, add a sentence acknowledging `categorize.bulk` now has CLI parity. Use existing formatting conventions in that file.

```bash
grep -n "CLI Symmetry\|^## .*5\b" docs/specs/mcp-architecture.md
```

- [ ] **Step 11.4: Update README.md**

Per `.claude/rules/shipping.md`:
- Add `categorize bulk` to the categorization CLI examples (look for an existing categorize section; mirror the `apply-rules` / `auto-review` examples).
- Update the categorization roadmap entry from 📐/🗓️ to ✅ if a planned line for "bulk categorization CLI" exists.

Run `grep -n "categorize\|bulk" README.md` first to find the right insertion point.

- [ ] **Step 11.5: Remove resolved followups**

Edit `private/followups.md`. Delete three sections:
- `### No `categorize bulk` CLI command` (around lines 15–50)
- `### Cache active-rule patterns and merchant pairs across `bulk_categorize` loop` (around lines 101–110)
- `### Avoid duplicate description SELECT in `bulk_categorize`` (the next adjacent section)

Confirm exact line numbers before deletion:
```bash
grep -n "^### " private/followups.md
```

- [ ] **Step 11.6: Run docs-touching checks (markdown lint if configured)**

```bash
make format && make lint
```

Expected: clean.

- [ ] **Step 11.7: Commit**

```bash
git add docs/specs/categorize-bulk.md docs/specs/INDEX.md docs/specs/mcp-architecture.md README.md private/followups.md
git commit -m "Update specs, README, and followups for categorize bulk"
```

---

## Task 12: Pre-push quality pass

**Files:** All changed in this branch.

Per `.claude/rules/shipping.md`, run `/simplify` against the changed code before the final commit, then `make check test`.

- [ ] **Step 12.1: Run /simplify**

In Claude Code: invoke the `simplify` skill against the diff. Apply any fixes it surfaces inline.

- [ ] **Step 12.2: Run the full pre-commit check**

```bash
make check test
```

Expected: format, lint, type-check, all tests pass.

- [ ] **Step 12.3: Commit any /simplify fixes**

```bash
git add -A
git diff --cached  # review before committing
git commit -m "Apply /simplify fixes before push"
```

(Skip this commit if `/simplify` made no changes.)

- [ ] **Step 12.4: Push and open PR**

```bash
git push -u origin feat/categorize-bulk
gh pr create --title "Add 'categorize bulk' CLI + bulk-loop perf" --body "$(cat <<'EOF'
## Summary
- New `moneybin categorize bulk` CLI command with file/stdin input and `--output {table,json}`, mirroring the `categorize.bulk` MCP tool. Closes the largest CLI symmetry gap.
- Shared `BulkCategorizationItem` Pydantic model validated at every boundary; per-item validation accumulates failures into the existing `BulkCategorizationResult.error_details` envelope (no fail-fast).
- New `BulkRecordingContext` threaded through `AutoRuleService.record_categorization` eliminates per-item duplicate description / merchant / rule SELECTs. Query count drops from `~3 + 5N` to `~3 + N`.
- Auto-rule E2E test now drives the real CLI flow (`import → categorize bulk → auto-review → auto-confirm → re-import`) instead of seeding `app.proposed_rules` via raw SQL.
- New observability metrics: `categorize_bulk_items_total`, `categorize_bulk_duration_seconds`, `categorize_bulk_errors_total`.

Resolves three items in `private/followups.md`: the missing CLI command, the rule/merchant pattern cache, and the duplicate description SELECT.

## Test plan
- [x] `uv run pytest tests/moneybin/test_categorization_service_bulk.py tests/moneybin/test_bulk_recording_context.py tests/moneybin/test_auto_rule_service_context.py -v`
- [x] `uv run pytest tests/integration/test_categorize_bulk_cli.py -v`
- [x] `uv run pytest tests/e2e/test_e2e_workflows.py::TestAutoRulePipeline -v`
- [x] `make check test`

Spec: `docs/specs/categorize-bulk.md`.
EOF
)"
```

Expected: PR opens with the `enhancement` label (per `.claude/rules/branching.md` `feat/` → `enhancement`).

---

## Self-Review Notes (writing-plans skill)

- **Spec coverage:** Every numbered requirement in `docs/specs/categorize-bulk.md` maps to a task: Req 1 → Task 8; Req 2 → Task 2; Req 3 → Tasks 1, 8, 9; Req 4 → Task 8 (`raise typer.Exit(1)` on partial fail); Req 5 → Task 6 (build context); Req 6 → Task 5; Req 7 → Task 6 (query-count test pins it); Req 8 → Task 10; Req 9 → Task 7.
- **Placeholder scan:** No "TBD"/"add appropriate handling"/"similar to". Tasks involving discovery (existing test fixtures, metric naming style) name an exact `grep` command to pin the convention before writing code, with the full code body included for the substantive part.
- **Type consistency:** `BulkCategorizationItem`, `BulkRecordingContext`, `TxnRow`, and the `find_matching_rule(rules_override=, txn_row_override=)` keyword names are consistent across Tasks 1–6 and the test files. Merchant tuple shape `(merchant_id, raw_pattern, match_type, canonical, category, subcategory)` matches `_fetch_merchants` output and is used identically in Tasks 4, 5, 6.
- **Scope check:** Single PR, single feature, with one perf bundle inline. Branch convention (`feat/`) follows primary intent.
- **Untouched assumptions:** `matches_pattern` import path in Task 4 is asserted-to-be-confirmed via `grep` before writing — the file may import it from `_text` or a sibling. Task 7 metric naming says "match neighbors" rather than dictating an exact prefix because `metrics/registry.py` may use a project-wide convention not visible from the spec alone.
