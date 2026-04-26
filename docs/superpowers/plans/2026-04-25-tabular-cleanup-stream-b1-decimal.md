# Tabular Import Cleanup — Stream B1 (Decimal Correctness) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `float` with `Decimal` for all monetary values that currently leak through `float` on the way to DuckDB `DECIMAL(18,2)` columns. Tabular pipeline was fixed; this plan covers OFX, W-2, and the synthetic data writer.

**Architecture:** Each module already gets a `Decimal` from upstream (OFX library, Pydantic schema) and then *casts to `float`* before handing off to Polars. The fix is to keep the value as `Decimal` end-to-end and use `pl.Decimal(precision=18, scale=2)` in the Polars schemas instead of `pl.Float64`. The DuckDB tables are already `DECIMAL(18,2)`.

**Tech Stack:** Python 3.12, Polars, DuckDB, Pydantic v2, ofxparse.

**Branch:** `fix/decimal-correctness-extractors`

**Spec:** [docs/specs/tabular-import-cleanup.md](../../specs/tabular-import-cleanup.md) §Stream B1

---

## Pre-Plan Verification

The spec lists four locations that *may* still use `float`. Two were verified during plan-writing — they do, and are fixed below:

- `src/moneybin/extractors/ofx_extractor.py:337,358,384,390` — calls `float(...)` on Decimal values, declares `pl.Float64` in schema. **CONFIRMED.**
- `src/moneybin/extractors/w2_extractor.py:997-1020` — calls `float(...)` on every Decimal Box value before building the dict. **CONFIRMED.**
- `src/moneybin/testing/synthetic/writer.py:155,180,238,241` — calls `float(...)` on amounts/balances when building dicts. **CONFIRMED.** (Spec wrongly pointed at `tests/moneybin/test_services/test_synthetic_data.py` — the source code is in `src/moneybin/testing/synthetic/writer.py`. Tests live in `tests/moneybin/test_synthetic/`.)
- `src/moneybin/mcp/` — spec says "MCP tool responses with float amounts." A grep for `float(` and `: float` across `src/moneybin/mcp/` returned **no matches** for monetary fields. The MCP tools read amounts from DuckDB `DECIMAL(18,2)` and return them as-is. **NO ACTION REQUIRED** — but verify in Task 4 below before closing the plan.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/moneybin/extractors/ofx_extractor.py` | OFX → Polars DataFrame builder | Modify — drop `float(...)` casts on `amount`, `ledger_balance`, `available_balance`; switch `pl.Float64` to `pl.Decimal(18, 2)` in the empty-frame schema |
| `src/moneybin/extractors/w2_extractor.py` | W-2 → dict builder | Modify — drop `float(...)` casts on every Box value |
| `src/moneybin/testing/synthetic/writer.py` | Synthetic data → CSV/Parquet writer | Modify — drop `float(...)` casts on `amount` / `balance` / `ledger_balance` |
| `src/moneybin/loaders/ofx_loader.py` | DuckDB ingest from OFX DataFrames | Read & adjust if it casts to/from float on the boundary |
| `src/moneybin/loaders/w2_loader.py` | DuckDB ingest from W-2 dicts | Read & adjust if it casts to/from float on the boundary |
| `tests/moneybin/test_extractors/test_ofx_extractor.py` | Existing OFX extractor tests | Update assertions from `float` literals to `Decimal("...")` |
| `tests/moneybin/test_extractors/test_w2_extractor.py` | Existing W-2 extractor tests | Update assertions from `float` literals to `Decimal("...")` |
| `tests/moneybin/test_synthetic/test_writer.py` | Existing synthetic writer tests | Update assertions to `Decimal("...")` |

---

## Task 1: Fix OFX extractor — keep `Decimal` to the DataFrame boundary

**Files:**
- Modify: `src/moneybin/extractors/ofx_extractor.py:320-365` (transactions DataFrame builder)
- Modify: `src/moneybin/extractors/ofx_extractor.py:367-405` (balances dict builder)
- Test: `tests/moneybin/test_extractors/test_ofx_extractor.py` (existing)

The schema already uses `Decimal` (`OFXTransactionSchema.amount: Decimal` at line 53). The extractor converts to `float` for the dict, then declares the empty-frame schema as `pl.Float64`. Both must change.

- [ ] **Step 1.1: Read the existing test to understand current assertions**

Run: `Read tests/moneybin/test_extractors/test_ofx_extractor.py` and note any assertion that uses `float` literals on amount/balance — those will need updating.

- [ ] **Step 1.2: Write a failing test asserting Decimal preservation**

Append to `tests/moneybin/test_extractors/test_ofx_extractor.py`:

```python
def test_extracted_transaction_amount_is_decimal(tmp_path) -> None:
    """OFX transactions DataFrame uses Decimal, not Float64, for amount."""
    import polars as pl

    from moneybin.extractors.ofx_extractor import OFXExtractor

    # Reuse any existing OFX fixture in this test file. If a fixture path
    # constant exists (e.g. SAMPLE_OFX), use it. Otherwise, copy the smallest
    # existing OFX-from-file test setup here.
    ofx_path = SAMPLE_OFX  # noqa: F821 — replace with the file's existing fixture
    extractor = OFXExtractor()
    data = extractor.extract_from_file(ofx_path)

    txn_df: pl.DataFrame = data.transactions
    assert txn_df.schema["amount"] == pl.Decimal(precision=18, scale=2)
```

If `SAMPLE_OFX` doesn't exist in the file, replace with whatever fixture path/constant the existing tests already use.

- [ ] **Step 1.3: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_extractors/test_ofx_extractor.py::test_extracted_transaction_amount_is_decimal -v`
Expected: FAIL with an `AssertionError` reporting `pl.Float64` ≠ `pl.Decimal(...)`.

- [ ] **Step 1.4: Update the transactions DataFrame builder**

Edit `src/moneybin/extractors/ofx_extractor.py`. At line 337, change:

```python
                    "amount": float(tx_schema.amount),
```

to:

```python
                    "amount": tx_schema.amount,
```

At line 358 (inside `_build_empty_transactions_df`), change:

```python
                "amount": pl.Float64,
```

to:

```python
                "amount": pl.Decimal(precision=18, scale=2),
```

If `pl.DataFrame(transactions_data)` later in the same function infers a non-Decimal dtype because the inferred Python type is `Decimal`, force the schema explicitly:

```python
        if transactions_data:
            return pl.DataFrame(
                transactions_data,
                schema_overrides={"amount": pl.Decimal(precision=18, scale=2)},
            )
```

- [ ] **Step 1.5: Update the balances builder**

At line 384, change:

```python
                    "ledger_balance": float(statement.balance)
```

to:

```python
                    "ledger_balance": statement.balance
```

(Note: `statement.balance` from `ofxparse` is already a `Decimal`. If pyright complains, add a type-narrowing `Decimal(...)` wrapper.)

At line 390, change:

```python
                    "available_balance": float(statement.available_balance)
```

to:

```python
                    "available_balance": statement.available_balance
```

If the file builds an empty balances DataFrame with `pl.Float64` for those columns, switch them to `pl.Decimal(precision=18, scale=2)` too. Use Read on lines ~400-440 to find that constructor and update its schema.

- [ ] **Step 1.6: Update the loader if necessary**

Read `src/moneybin/loaders/ofx_loader.py`. Look for any `pl.Float64` in schema declarations or `.cast(pl.Float64)` calls touching `amount`, `ledger_balance`, or `available_balance`. Replace with `pl.Decimal(precision=18, scale=2)`. If the loader simply forwards the DataFrame to `db.ingest_dataframe(...)`, no change is needed — DuckDB will cast `Decimal` → `DECIMAL(18,2)` directly.

- [ ] **Step 1.7: Update existing test assertions**

Re-read `tests/moneybin/test_extractors/test_ofx_extractor.py`. For any assertion of the form `assert df["amount"][0] == 12.34` or `pytest.approx(12.34)`, change to:

```python
from decimal import Decimal
assert df["amount"][0] == Decimal("12.34")
```

Use exact string literals — never `Decimal(12.34)` (captures float imprecision per `.claude/rules/database.md`).

- [ ] **Step 1.8: Run the OFX extractor tests**

Run: `uv run pytest tests/moneybin/test_extractors/test_ofx_extractor.py -v`
Expected: all green.

- [ ] **Step 1.9: Run the OFX loader and end-to-end OFX tests**

Run: `uv run pytest tests/moneybin/test_loaders/ -v -k ofx && uv run pytest tests/e2e/ -v -k ofx`
Expected: all green. (If the loader tests check schema, they should now expect `Decimal` too — update accordingly.)

- [ ] **Step 1.10: Run pyright**

Run: `uv run pyright src/moneybin/extractors/ofx_extractor.py src/moneybin/loaders/ofx_loader.py`
Expected: 0 errors.

- [ ] **Step 1.11: Commit**

```bash
git add src/moneybin/extractors/ofx_extractor.py \
        src/moneybin/loaders/ofx_loader.py \
        tests/moneybin/test_extractors/test_ofx_extractor.py \
        tests/moneybin/test_loaders/
git commit -m "Use Decimal end-to-end in OFX extractor and loader"
```

---

## Task 2: Fix W-2 extractor — keep `Decimal` for all Box values

**Files:**
- Modify: `src/moneybin/extractors/w2_extractor.py:990-1025` (final dict builder — adjust line range after reading)
- Modify (if needed): `src/moneybin/loaders/w2_loader.py`
- Test: `tests/moneybin/test_extractors/test_w2_extractor.py` (existing)

W-2 boxes are stored in `raw_w2_forms` as `DECIMAL(18,2)`. The extractor casts every box value to `float` when building its return dict. Drop those casts.

- [ ] **Step 2.1: Read the W-2 extractor return dict**

Read `src/moneybin/extractors/w2_extractor.py:980-1030` to confirm the exact line numbers and the structure. There are at least 11 `float(...)` calls in this region — list them.

- [ ] **Step 2.2: Write a failing test**

Append to `tests/moneybin/test_extractors/test_w2_extractor.py`:

```python
def test_extracted_w2_wages_are_decimal() -> None:
    """W-2 extractor returns Decimal — never float — for monetary fields."""
    from decimal import Decimal

    # Use whatever extractor fixture the file already uses.
    extractor = W2Extractor()  # noqa: F821 — already imported in this file
    data = extractor.extract_from_file(SAMPLE_W2_PDF)  # noqa: F821 — replace with file's fixture
    record = data[0] if isinstance(data, list) else data

    for field in (
        "wages",
        "federal_income_tax",
        "social_security_wages",
        "social_security_tax",
        "medicare_wages",
        "medicare_tax",
    ):
        if record.get(field) is not None:
            assert isinstance(record[field], Decimal), (
                f"{field} should be Decimal, got {type(record[field]).__name__}"
            )
```

If the extractor returns a Pydantic model rather than a dict, adapt the indexing.

- [ ] **Step 2.3: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_extractors/test_w2_extractor.py::test_extracted_w2_wages_are_decimal -v`
Expected: FAIL on the first field (`wages`) — gets `float` not `Decimal`.

- [ ] **Step 2.4: Drop every `float(...)` cast in the extractor return dict**

Edit `src/moneybin/extractors/w2_extractor.py`. For each line in the 990-1025 block, change `float(w2_schema.<box>)` to `w2_schema.<box>`. Example:

```python
            "wages": w2_schema.wages,
            "federal_income_tax": w2_schema.federal_income_tax,
            "social_security_wages": w2_schema.social_security_wages
            ...,
```

(Keep any surrounding `if X is not None else None` ternaries intact.)

- [ ] **Step 2.5: Update existing test assertions**

Find any assertion in `test_w2_extractor.py` that compares to a bare float literal. Convert to `Decimal("…")`. Example: `assert record["wages"] == 50000.00` → `assert record["wages"] == Decimal("50000.00")`.

- [ ] **Step 2.6: Update the loader if necessary**

Read `src/moneybin/loaders/w2_loader.py`. If it casts to/from float anywhere, switch to `Decimal` (or remove the cast — the values are already Decimal). If it builds a Polars DataFrame internally with `pl.Float64`, switch to `pl.Decimal(precision=18, scale=2)`.

- [ ] **Step 2.7: Run the W-2 test suite**

Run: `uv run pytest tests/moneybin/test_extractors/test_w2_extractor.py tests/moneybin/test_loaders/ -v -k w2 && uv run pytest tests/e2e/ -v -k w2`
Expected: all green.

- [ ] **Step 2.8: Run pyright**

Run: `uv run pyright src/moneybin/extractors/w2_extractor.py src/moneybin/loaders/w2_loader.py`
Expected: 0 errors.

- [ ] **Step 2.9: Commit**

```bash
git add src/moneybin/extractors/w2_extractor.py \
        src/moneybin/loaders/w2_loader.py \
        tests/moneybin/test_extractors/test_w2_extractor.py \
        tests/moneybin/test_loaders/
git commit -m "Use Decimal end-to-end in W-2 extractor and loader"
```

---

## Task 3: Fix synthetic-data writer — keep `Decimal` end-to-end

**Files:**
- Modify: `src/moneybin/testing/synthetic/writer.py:150-250` (read first, then patch the four `float(...)` sites at ~155, ~180, ~238, ~241)
- Modify: `src/moneybin/testing/synthetic/models.py:29-134` (consider whether `amount: float` and `opening_balance: float` should become `Decimal`)
- Test: `tests/moneybin/test_synthetic/test_writer.py` (existing)

This is the most invasive sub-task. Synthetic *generation* happens in `float` space (random distributions, raise calculations) but *output* must be `Decimal` to round-trip cleanly through the import pipeline. Decision: keep generators in `float`, convert at the writer boundary using `Decimal(str(value))` (per `.claude/rules/database.md`).

- [ ] **Step 3.1: Read the writer to confirm the four sites**

Read `src/moneybin/testing/synthetic/writer.py:140-260`. Confirm the four `float(...)` casts are at the lines noted in the spec, and identify the surrounding dict structures.

- [ ] **Step 3.2: Write a failing test**

Append to `tests/moneybin/test_synthetic/test_writer.py`:

```python
def test_writer_emits_decimal_amounts(tmp_path) -> None:
    """Synthetic writer outputs Decimal amounts in the produced DataFrames."""
    from decimal import Decimal

    import polars as pl

    # Use whatever the existing tests use to build a small ledger and run
    # the writer. Adapt this skeleton to the file's existing helpers.
    ledger = _build_minimal_ledger()  # noqa: F821 — implement using the file's existing test helpers

    output_dir = tmp_path / "out"
    output_dir.mkdir()
    writer = SyntheticWriter(output_dir)  # noqa: F821 — already imported in this test file
    writer.write(ledger)

    # Read back one of the produced CSVs and assert amount column is Decimal-coercible
    csv_path = next(output_dir.rglob("*.csv"))
    df = pl.read_csv(csv_path)
    if "amount" in df.columns:
        # Polars read_csv infers Float — what matters is the on-disk value
        # round-trips through Decimal without precision loss.
        for raw in df["amount"].to_list():
            assert Decimal(str(raw)) == Decimal(f"{raw:.2f}"), (
                f"Amount {raw!r} would lose precision in a Decimal round-trip"
            )
```

If the synthetic writer returns DataFrames in-memory rather than writing files, adapt the test accordingly.

- [ ] **Step 3.3: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_synthetic/test_writer.py::test_writer_emits_decimal_amounts -v`
Expected: FAIL — values currently round-trip as floats with imprecision (e.g. `123.4500000001`).

- [ ] **Step 3.4: Convert at the writer boundary**

Edit `src/moneybin/testing/synthetic/writer.py`. At each of the four sites:

```python
                "ledger_balance": float(acct.opening_balance),
```

becomes:

```python
                "ledger_balance": Decimal(str(round(acct.opening_balance, 2))),
```

And:

```python
                "amount": float(txn.amount),
```

becomes:

```python
                "amount": Decimal(str(round(txn.amount, 2))),
```

The `round(..., 2)` ensures the random-generator floats are quantized to two decimals before string-conversion. `Decimal(str(...))` is the only safe constructor (per `.claude/rules/database.md`).

Add `from decimal import Decimal` to the file's imports if not already present.

If any DataFrame in this file declares a schema with `pl.Float64` for these columns, switch to `pl.Decimal(precision=18, scale=2)`. Use Grep to find them.

- [ ] **Step 3.5: Run the synthetic writer tests**

Run: `uv run pytest tests/moneybin/test_synthetic/ -v`
Expected: all green.

- [ ] **Step 3.6: Run the synthetic E2E test**

Run: `uv run pytest tests/e2e/ -v -k synthetic`
Expected: all green.

- [ ] **Step 3.7: Run pyright**

Run: `uv run pyright src/moneybin/testing/synthetic/`
Expected: 0 errors.

- [ ] **Step 3.8: Commit**

```bash
git add src/moneybin/testing/synthetic/ tests/moneybin/test_synthetic/
git commit -m "Convert synthetic writer amounts to Decimal at the output boundary"
```

---

## Task 4: Verify MCP layer is already Decimal-clean

**Files:**
- Inspect: `src/moneybin/mcp/tools/` (no edits expected)

Pre-plan grep showed no `float(` casts on amount fields in the MCP layer. Confirm before closing the plan.

- [ ] **Step 4.1: Repo-wide grep for monetary float casts**

Run:

```bash
grep -rn "float(.*amount\|float(.*balance\|float(.*wage" src/moneybin/mcp/
```

Expected: no matches. If matches appear, file a follow-up issue rather than expanding this PR's scope.

- [ ] **Step 4.2: Spot-check `spending.py`, `accounts.py`, `transactions.py`**

For each, scan the response builder. The amount/balance values should be passed through unchanged from the DuckDB query result. If you see `float(...)` or `: float` annotations on monetary columns, file a follow-up.

- [ ] **Step 4.3: No commit needed if no edits**

Document the verification in the PR description.

---

## Task 5: Pre-push quality pass

- [ ] **Step 5.1: Run the broader test surface**

Run: `make check test`
Expected: all green.

- [ ] **Step 5.2: Run all E2E tests**

Run: `uv run pytest tests/e2e/ -v -m "e2e and not slow"`
Expected: all green.

- [ ] **Step 5.3: Push and open PR**

```bash
git push -u origin fix/decimal-correctness-extractors
gh pr create --title "Use Decimal end-to-end for monetary values" --body "$(cat <<'EOF'
## Summary
- OFX extractor and loader: keep `Decimal` from Pydantic schema to Polars DataFrame
- W-2 extractor: drop all `float(...)` casts on Box monetary fields
- Synthetic writer: quantize generator floats with `Decimal(str(round(x, 2)))` at the output boundary
- MCP layer verified clean — no float casts on amount/balance/wage fields

Spec: docs/specs/tabular-import-cleanup.md (Stream B1).

## Test plan
- [x] `make check test`
- [x] `uv run pytest tests/e2e/ -m "e2e and not slow"`
- [x] Round-trip a real OFX file through `moneybin import file` and confirm `raw.ofx_transactions.amount` matches the source to the cent
EOF
)"
```
