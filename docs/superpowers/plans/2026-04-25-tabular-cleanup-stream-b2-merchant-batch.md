# Tabular Import Cleanup — Stream B2 (Batch Merchant Resolution) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the N+1 merchant lookup in `bulk_categorize`. Today every item triggers an individual `SELECT description` and an individual `match_merchant` (which itself queries `MERCHANTS` once per call). After this plan, those queries collapse to two batch queries plus an in-memory match.

**Architecture:** `bulk_categorize` already calls `match_merchant(db, description)` per item. `match_merchant` is a thin wrapper over `_fetch_merchants(db)` (one query) + `_match_description(...)` (in-memory). The fix is to:

1. Batch-fetch all `(transaction_id, description)` pairs once.
2. Cache `_fetch_merchants(db)` once.
3. Reuse `_match_description(...)` per item against the cached list.
4. Batch-insert new merchants and category assignments at the end.

**Tech Stack:** Python 3.12, DuckDB, Polars (not strictly needed here).

**Branch:** `perf/categorize-bulk-batch-merchants`

**Spec:** [docs/specs/tabular-import-cleanup.md](../../specs/tabular-import-cleanup.md) §Stream B2

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/moneybin/services/categorization_service.py:307-401` | `bulk_categorize()` — per-item resolution loop | **Modify** — replace per-item DB calls with two batch queries + in-memory match |
| `src/moneybin/services/categorization_service.py:181-258` | `_fetch_merchants` / `_match_description` / `match_merchant` | **No change** — already factored, just reused |
| `tests/moneybin/test_services/test_categorization_service.py` | Existing categorization service tests | **Modify** — add a perf-shape test (counts DB calls) and a behavior test (auto-creation still happens) |

The batch path is local to `bulk_categorize`. No other callers need to change. The MCP tool `categorize_bulk` (`src/moneybin/mcp/tools/categorize.py:286-309`) already delegates here and stays a one-liner.

---

## Task 1: Write the perf-shape test

**Files:**
- Test: `tests/moneybin/test_services/test_categorization_service.py` (existing — append)

The motivating concern is round-trip count, not wall time. Asserting wall-time bounds is flaky; instead, count `db.execute` calls.

- [ ] **Step 1.1: Read the existing test to find a fixture pattern**

Run: `Read tests/moneybin/test_services/test_categorization_service.py` and locate any existing `bulk_categorize` test. Note the fixture used to set up a Database with seeded transactions — most likely a `tmp_path` + `Database(..., no_auto_upgrade=True)` pattern with helper inserts. Reuse that scaffolding.

- [ ] **Step 1.2: Append the perf-shape test**

```python
def test_bulk_categorize_uses_constant_number_of_db_calls(
    monkeypatch, mock_secret_store, tmp_path
) -> None:
    """bulk_categorize should not scale DB round-trips with item count.

    With N items, the number of read queries (description fetch + merchant
    fetch) must be O(1), not O(N).
    """
    from moneybin.database import Database
    from moneybin.services.categorization_service import bulk_categorize
    from moneybin.tables import FCT_TRANSACTIONS

    db = Database(
        tmp_path / "perf.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    # Seed 25 transactions and 25 corresponding category items.
    db.execute(f"""
        CREATE OR REPLACE TABLE {FCT_TRANSACTIONS.full_name} AS
        SELECT
            'txn_' || i AS transaction_id,
            CAST('2025-01-01' AS DATE) AS transaction_date,
            CAST(-10.00 AS DECIMAL(18,2)) AS amount,
            'Coffee shop ' || i AS description,
            CAST(NULL AS VARCHAR) AS memo,
            'acct1' AS account_id
        FROM range(25) t(i)
    """)
    items = [
        {"transaction_id": f"txn_{i}", "category": "Food", "subcategory": "Coffee"}
        for i in range(25)
    ]

    real_execute = db.execute
    select_calls: list[str] = []

    def counting_execute(query, *args, **kwargs):
        if query.strip().upper().startswith("SELECT"):
            select_calls.append(query)
        return real_execute(query, *args, **kwargs)

    monkeypatch.setattr(db, "execute", counting_execute)

    result = bulk_categorize(db, items)

    assert result.applied == 25
    # Expected reads: 1 batch description fetch + 1 merchant fetch +
    # any small bookkeeping. Generous upper bound = 5; previous impl was 50+.
    assert len(select_calls) <= 5, (
        f"Expected ≤5 SELECTs, got {len(select_calls)}:\n" + "\n".join(select_calls)
    )
```

- [ ] **Step 1.3: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::test_bulk_categorize_uses_constant_number_of_db_calls -v`
Expected: FAIL — current implementation issues 25+ SELECTs (one per item plus one merchants fetch per `match_merchant` call).

---

## Task 2: Refactor `bulk_categorize` to batch reads and reuse merchant cache

**Files:**
- Modify: `src/moneybin/services/categorization_service.py:307-401`

The new flow:

1. Filter items to those with non-empty `transaction_id` and `category`. Stash the rest in `error_details` with `skipped += 1`.
2. Single batch query: `SELECT transaction_id, description FROM fct_transactions WHERE transaction_id IN (?, ?, ...)` — build a `dict[txn_id, description]`.
3. Single `_fetch_merchants(db)` call → cached merchant list (may be `None` if table missing).
4. Per item, call `_match_description(description, cached_merchants)` for resolution. If no match and `normalized` is non-empty, queue a `create_merchant` **inside the loop** (still single-row INSERTs — DuckDB's overhead is per-statement, not per-round-trip; further batching is overkill until profiled).
5. Per item, perform the existing `INSERT OR REPLACE INTO transaction_categories` — this is unavoidable per-item but is a write, not a read, and is what the user is paying for.

The behavior must not change: `merchants_created` count, `applied` count, `error_details` content, and the relative ordering of error reporting must all match.

- [ ] **Step 2.1: Replace the function body**

Read `src/moneybin/services/categorization_service.py:307-401` first to confirm the exact current implementation. Then replace the body of `bulk_categorize` with:

```python
def bulk_categorize(
    db: Database,
    items: list[dict[str, str]],
) -> BulkCategorizationResult:
    """Assign categories to multiple transactions with merchant auto-creation.

    For each item, looks up the transaction description, resolves or creates
    a merchant mapping, then inserts/replaces the category assignment.
    Merchant resolution is best-effort — failures do not prevent categorization.

    Read-side cost is O(1) in the number of items: one batch description
    fetch and one merchant-table fetch, regardless of input size.

    Args:
        db: Database instance (read-write).
        items: List of dicts with transaction_id, category, and optional subcategory.

    Returns:
        BulkCategorizationResult with applied/skipped/error counts.
    """
    applied = 0
    skipped = 0
    errors = 0
    merchants_created = 0
    error_details: list[dict[str, str]] = []

    # Phase 1 — validate and partition input
    valid_items: list[tuple[str, str, str | None]] = []
    for item in items:
        txn_id = item.get("transaction_id", "").strip()
        category = item.get("category", "").strip()
        if not txn_id or not category:
            skipped += 1
            error_details.append({
                "transaction_id": txn_id or "(missing)",
                "reason": "Missing transaction_id or category",
            })
            continue
        subcategory = item.get("subcategory", "").strip() or None
        valid_items.append((txn_id, category, subcategory))

    if not valid_items:
        return BulkCategorizationResult(
            applied=applied,
            skipped=skipped,
            errors=errors,
            error_details=error_details,
            merchants_created=merchants_created,
        )

    # Phase 2 — batch-fetch descriptions
    txn_ids = [v[0] for v in valid_items]
    placeholders = ",".join(["?"] * len(txn_ids))
    descriptions: dict[str, str | None] = {}
    try:
        rows = db.execute(
            f"""
            SELECT transaction_id, description
            FROM {FCT_TRANSACTIONS.full_name}
            WHERE transaction_id IN ({placeholders})
            """,  # noqa: S608 — placeholders count is bounded by len(valid_items); values are parameterized
            txn_ids,
        ).fetchall()
        descriptions = {row[0]: row[1] for row in rows}
    except Exception:  # noqa: BLE001 — best-effort; missing table → all merchants resolve to None
        logger.debug("Could not batch-fetch descriptions", exc_info=True)

    # Phase 3 — fetch merchants once, then match in memory
    cached_merchants = _fetch_merchants(db)

    # Phase 4 — per-item categorization (writes only)
    for txn_id, category, subcategory in valid_items:
        merchant_id: str | None = None
        description = descriptions.get(txn_id)
        if description and cached_merchants is not None:
            try:
                existing = _match_description(description, cached_merchants)
                if existing:
                    merchant_id = existing["merchant_id"]
                else:
                    normalized = normalize_description(description)
                    if normalized:
                        merchant_id = create_merchant(
                            db,
                            normalized,
                            normalized,
                            match_type="contains",
                            category=category,
                            subcategory=subcategory,
                            created_by="ai",
                        )
                        merchants_created += 1
                        # Append to cache so subsequent items in this batch
                        # find the just-created merchant instead of re-creating.
                        cached_merchants.append((
                            merchant_id,
                            normalized,
                            "contains",
                            normalized,
                            category,
                            subcategory,
                        ))
            except Exception:  # noqa: BLE001 — merchant resolution is best-effort; categorization proceeds without it
                logger.debug(
                    f"Could not resolve merchant for {txn_id}",
                    exc_info=True,
                )

        try:
            db.execute(
                f"""
                INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory,
                 categorized_at, categorized_by, merchant_id)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ai', ?)
                """,
                [txn_id, category, subcategory, merchant_id],
            )
            applied += 1
        except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
            errors += 1
            logger.exception(f"bulk_categorize failed for transaction {txn_id!r}")
            error_details.append({
                "transaction_id": txn_id,
                "reason": "Failed to apply category — check logs for details.",
            })

    return BulkCategorizationResult(
        applied=applied,
        skipped=skipped,
        errors=errors,
        error_details=error_details,
        merchants_created=merchants_created,
    )
```

Behavior preserved:

- Validation order and `error_details` content for missing `transaction_id`/`category` — unchanged.
- For valid items where the description fetch failed or returned nothing → `merchant_id = None`, category still applied (matches existing best-effort behavior).
- Merchant auto-create still happens with the same `match_type="contains"`, `created_by="ai"`, and `category`/`subcategory` from the item.
- Insert-cache trick prevents creating the same merchant twice when two items in the same batch share a normalized description.

- [ ] **Step 2.2: Run the perf-shape test**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::test_bulk_categorize_uses_constant_number_of_db_calls -v`
Expected: PASS.

- [ ] **Step 2.3: Run the full categorization test suite**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v`
Expected: all green. Pay special attention to any test that asserts `merchants_created == N` — the in-batch dedup might change a count if two test items share a description (unlikely in existing tests, but verify).

- [ ] **Step 2.4: Run the MCP categorize tests**

Run: `uv run pytest tests/moneybin/test_mcp/ -v -k categorize`
Expected: all green.

- [ ] **Step 2.5: Run pyright**

Run: `uv run pyright src/moneybin/services/categorization_service.py`
Expected: 0 errors.

- [ ] **Step 2.6: Commit**

```bash
git add src/moneybin/services/categorization_service.py \
        tests/moneybin/test_services/test_categorization_service.py
git commit -m "Batch description and merchant lookups in bulk_categorize"
```

---

## Task 3: Add a behavior test for in-batch merchant dedup

**Files:**
- Test: `tests/moneybin/test_services/test_categorization_service.py` (append)

This guards against a subtle regression: if a future change drops the cache-append in Phase 4, two items with the same normalized description would create the merchant twice (or, worse, fail on a uniqueness constraint).

- [ ] **Step 3.1: Write the test**

```python
def test_bulk_categorize_dedupes_merchant_creation_within_batch(
    mock_secret_store, tmp_path
) -> None:
    """Two items with the same description create exactly one merchant."""
    from moneybin.database import Database
    from moneybin.services.categorization_service import bulk_categorize
    from moneybin.tables import FCT_TRANSACTIONS, MERCHANTS

    db = Database(
        tmp_path / "dedup.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    db.execute(f"""
        CREATE OR REPLACE TABLE {FCT_TRANSACTIONS.full_name} AS
        SELECT
            'txn_' || i AS transaction_id,
            CAST('2025-01-01' AS DATE) AS transaction_date,
            CAST(-10.00 AS DECIMAL(18,2)) AS amount,
            'IDENTICAL VENDOR' AS description,
            CAST(NULL AS VARCHAR) AS memo,
            'acct1' AS account_id
        FROM range(3) t(i)
    """)

    items = [
        {"transaction_id": f"txn_{i}", "category": "Food", "subcategory": "Coffee"}
        for i in range(3)
    ]

    result = bulk_categorize(db, items)

    assert result.applied == 3
    assert result.merchants_created == 1, (
        f"Expected 1 merchant created across 3 identical-description items, "
        f"got {result.merchants_created}"
    )

    merchant_count = db.execute(
        f"SELECT COUNT(*) FROM {MERCHANTS.full_name}"
    ).fetchone()
    assert merchant_count is not None
    assert merchant_count[0] == 1
```

- [ ] **Step 3.2: Run the test**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::test_bulk_categorize_dedupes_merchant_creation_within_batch -v`
Expected: PASS (the cache-append in Phase 4 already ensures this).

- [ ] **Step 3.3: Commit**

```bash
git add tests/moneybin/test_services/test_categorization_service.py
git commit -m "Test in-batch merchant dedup in bulk_categorize"
```

---

## Task 4: Pre-push quality pass

- [ ] **Step 4.1: Full check**

Run: `make check test`
Expected: green.

- [ ] **Step 4.2: E2E**

Run: `uv run pytest tests/e2e/ -v -m "e2e and not slow"`
Expected: green.

- [ ] **Step 4.3: Push and open PR**

```bash
git push -u origin perf/categorize-bulk-batch-merchants
gh pr create --title "Batch merchant resolution in categorize.bulk" --body "$(cat <<'EOF'
## Summary
- Replace per-item description fetch with a single `IN (...)` batch query
- Cache `_fetch_merchants(db)` once per call, reuse `_match_description` in memory
- Append newly-created merchants to the cache so duplicate descriptions in a batch don't double-create
- Drops read round-trips from O(N) to O(1) per `bulk_categorize` call

Spec: docs/specs/tabular-import-cleanup.md (Stream B2). No behavior change beyond perf and within-batch dedup (which existed by accident before — now explicit and tested).

## Test plan
- [x] `make check test`
- [x] New perf-shape test asserts ≤5 SELECTs for 25 items
- [x] New dedup test asserts 1 merchant created for 3 identical-description items
EOF
)"
```
