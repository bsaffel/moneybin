# Tabular Import Cleanup — Stream A (Internal Refactor) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tighten internals of the tabular import pipeline — extract a `ResolvedMapping` dataclass, thread `Literal` types through service layer, move balance-validation tunables to config, and extract the repeated `DatabaseKeyError` handler — all with no behavior change.

**Architecture:** Pure refactor inside the existing five-stage pipeline. Three of the four sub-tasks are local to `services/import_service.py`, `extractors/tabular/`, and `cli/commands/`. One (`A4`) introduces a small new module under `cli/`.

**Tech Stack:** Python 3.12, Pydantic v2, Typer, Polars, DuckDB. Tests: pytest.

**Branch:** `refactor/tabular-import-cleanup-stream-a`

**Spec:** [docs/specs/tabular-import-cleanup.md](../../specs/tabular-import-cleanup.md) §Stream A

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/moneybin/extractors/tabular/column_mapper.py` | Column mapping with `MappingResult` dataclass | Modify — change `MappingResult.sign_convention` and `MappingResult.number_format` to `SignConventionType` / `NumberFormatType` |
| `src/moneybin/services/import_service.py` | `_import_tabular()` — resolves mapping, runs transform, loads | Modify — replace 6 unpacked locals with `ResolvedMapping` instance |
| `src/moneybin/extractors/tabular/transforms.py` | `transform_dataframe()` and `_validate_running_balance()` | Modify — accept `Literal` types; thread `pass_threshold` and `tolerance` from config |
| `src/moneybin/config.py` | `TabularConfig` Pydantic model | Modify — add `balance_pass_threshold` and `balance_tolerance_cents` fields |
| `src/moneybin/cli/utils.py` | New: shared CLI helpers | **Create** — house `handle_database_errors` context manager |
| `src/moneybin/cli/commands/{import_cmd,stats,matches,categorize,synthetic,mcp,db,migrate,transform}.py` | CLI commands using `get_database()` | Modify — call sites replace 5-line except blocks with `with handle_database_errors() as db:` |
| `tests/moneybin/test_services/test_tabular_import_service.py` | Existing tabular import service test | Modify — add `ResolvedMapping` round-trip test |
| `tests/moneybin/test_extractors/test_tabular/test_transforms.py` | Existing balance-validation test (or new file) | Modify — assert config-driven thresholds |
| `tests/moneybin/test_cli/test_handle_database_errors.py` | New: tests for the context manager | **Create** |

The new `ResolvedMapping` dataclass is local to `import_service.py` (private — only one caller). The `handle_database_errors` helper goes in a new `cli/utils.py` module so all CLI command modules can import it without coupling to any specific command.

---

## Task 1: Add `balance_pass_threshold` and `balance_tolerance_cents` to `TabularConfig`

**Files:**
- Modify: `src/moneybin/config.py:119-139` (`TabularConfig` body)
- Test: `tests/moneybin/test_config_profiles.py` (existing) — append a unit test asserting defaults

This task lands the config plumbing first so later tasks can consume it.

- [ ] **Step 1.1: Write the failing test**

Append to `tests/moneybin/test_config_profiles.py`:

```python
def test_tabular_config_balance_validation_defaults() -> None:
    """TabularConfig exposes balance validation tunables with safe defaults."""
    from moneybin.config import TabularConfig

    cfg = TabularConfig()
    assert cfg.balance_pass_threshold == 0.90
    assert cfg.balance_tolerance_cents == 1


def test_tabular_config_balance_validation_overrides() -> None:
    """Caller can override balance validation tunables."""
    from moneybin.config import TabularConfig

    cfg = TabularConfig(balance_pass_threshold=0.95, balance_tolerance_cents=5)
    assert cfg.balance_pass_threshold == 0.95
    assert cfg.balance_tolerance_cents == 5
```

- [ ] **Step 1.2: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_config_profiles.py::test_tabular_config_balance_validation_defaults -v`
Expected: FAIL with `AttributeError: 'TabularConfig' object has no attribute 'balance_pass_threshold'`

- [ ] **Step 1.3: Add the fields to `TabularConfig`**

Edit `src/moneybin/config.py`, after the `row_refuse_threshold` field inside `TabularConfig` (line 136-139):

```python
    balance_pass_threshold: float = Field(
        default=0.90,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum fraction of balance deltas that must match "
            "for balance validation to pass"
        ),
    )
    balance_tolerance_cents: int = Field(
        default=1,
        ge=0,
        description="Per-delta tolerance in cents for balance validation",
    )
```

- [ ] **Step 1.4: Run the tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_config_profiles.py -v -k balance_validation`
Expected: 2 passed

- [ ] **Step 1.5: Run type check**

Run: `uv run pyright src/moneybin/config.py`
Expected: 0 errors

- [ ] **Step 1.6: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_config_profiles.py
git commit -m "Add balance validation tunables to TabularConfig"
```

---

## Task 2: Thread balance-validation config into `_validate_running_balance`

**Files:**
- Modify: `src/moneybin/extractors/tabular/transforms.py:94-326` (`transform_dataframe`)
- Modify: `src/moneybin/extractors/tabular/transforms.py:463-557` (`_validate_running_balance`)
- Test: `tests/moneybin/test_extractors/test_tabular/test_transforms.py` (existing or create)

The current implementation hardcodes `_balance_tolerance = Decimal("0.01")` and `_pass_threshold = 0.90` inside the function body. After this task, `transform_dataframe` accepts these as keyword arguments with the same defaults, and `_import_tabular` will pass them in from `get_settings().data.tabular`.

- [ ] **Step 2.1: Write the failing test**

If `tests/moneybin/test_extractors/test_tabular/test_transforms.py` does not exist, create it. Append (or write) this test:

```python
"""Tests for the tabular transform stage."""

from decimal import Decimal

import polars as pl

from moneybin.extractors.tabular.transforms import transform_dataframe


def _make_df() -> pl.DataFrame:
    """Three rows with a perfectly consistent running balance."""
    return pl.DataFrame({
        "Date": ["2025-01-01", "2025-01-02", "2025-01-03"],
        "Description": ["a", "b", "c"],
        "Amount": ["-10.00", "-20.00", "5.00"],
        "Balance": ["100.00", "80.00", "85.00"],
    })


def test_transform_uses_custom_balance_tolerance_cents() -> None:
    """A high tolerance accepts deltas that the default would reject."""
    df = pl.DataFrame({
        "Date": ["2025-01-01", "2025-01-02"],
        "Description": ["a", "b"],
        # Amount is off by 10 cents from the balance delta
        "Amount": ["-10.00", "-19.90"],
        "Balance": ["100.00", "80.00"],
    })
    field_mapping = {
        "transaction_date": "Date",
        "description": "Description",
        "amount": "Amount",
        "balance": "Balance",
    }
    result = transform_dataframe(
        df=df,
        field_mapping=field_mapping,
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        account_id="acct1",
        source_file="t.csv",
        source_type="csv",
        source_origin="t",
        import_id="imp1",
        balance_pass_threshold=0.90,
        balance_tolerance_cents=20,  # 0.20 — accepts the 0.10 mismatch
    )
    assert result.balance_validated is True


def test_transform_default_tolerance_rejects_off_by_ten_cents() -> None:
    """Default 1-cent tolerance rejects a 10-cent delta mismatch."""
    df = pl.DataFrame({
        "Date": ["2025-01-01", "2025-01-02"],
        "Description": ["a", "b"],
        "Amount": ["-10.00", "-19.90"],
        "Balance": ["100.00", "80.00"],
    })
    field_mapping = {
        "transaction_date": "Date",
        "description": "Description",
        "amount": "Amount",
        "balance": "Balance",
    }
    result = transform_dataframe(
        df=df,
        field_mapping=field_mapping,
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        account_id="acct1",
        source_file="t.csv",
        source_type="csv",
        source_origin="t",
        import_id="imp1",
    )
    # forward 0/1, inverted 0/1 — neither passes
    assert result.balance_validated is False
```

- [ ] **Step 2.2: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_extractors/test_tabular/test_transforms.py -v`
Expected: FAIL with `TypeError: transform_dataframe() got an unexpected keyword argument 'balance_pass_threshold'`

- [ ] **Step 2.3: Add the new parameters to `transform_dataframe`**

Edit `src/moneybin/extractors/tabular/transforms.py`. In the `transform_dataframe` signature (around line 94-106), add two keyword-only parameters with defaults:

```python
def transform_dataframe(
    *,
    df: pl.DataFrame,
    field_mapping: dict[str, str],
    date_format: str,
    sign_convention: str,
    number_format: str,
    account_id: str | list[str],
    source_file: str,
    source_type: str,
    source_origin: str,
    import_id: str,
    balance_pass_threshold: float = 0.90,
    balance_tolerance_cents: int = 1,
) -> TransformResult:
```

In the same function, find the `_validate_running_balance(...)` call near the end (around line 324). Pass the new arguments through:

```python
            result = _validate_running_balance(
                result,
                balance_strs,
                number_format,
                pass_threshold=balance_pass_threshold,
                tolerance_cents=balance_tolerance_cents,
            )
```

Update `_validate_running_balance` signature (around line 463-468):

```python
def _validate_running_balance(
    result: TransformResult,
    balance_strs: list[str],
    number_format: str,
    *,
    pass_threshold: float = 0.90,
    tolerance_cents: int = 1,
) -> TransformResult:
```

Inside the function body, replace:

```python
    _balance_tolerance = Decimal("0.01")
    _pass_threshold = 0.90
```

with:

```python
_balance_tolerance = (Decimal(tolerance_cents) / Decimal(100)).quantize(Decimal("0.01"))
_pass_threshold = pass_threshold
```

Leave the rest of the function unchanged — `_balance_tolerance` and `_pass_threshold` are still referenced below.

- [ ] **Step 2.4: Run the tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_extractors/test_tabular/test_transforms.py -v`
Expected: 2 passed

- [ ] **Step 2.5: Run the full transforms test suite to verify no regression**

Run: `uv run pytest tests/moneybin/test_extractors/test_tabular/ -v`
Expected: all green

- [ ] **Step 2.6: Wire `_import_tabular` to read from config**

Edit `src/moneybin/services/import_service.py`. In `_import_tabular`, before the `transform_dataframe(...)` call (around line 451), add:

```python
    from moneybin.config import get_settings

    tabular_cfg = get_settings().data.tabular
```

(If `get_settings` is already imported in another scope inside the function via `_run_matching`, leave that alone — keep the local import for clarity.)

Then update the `transform_dataframe(...)` call to pass through the two new kwargs:

```python
        transform_result = transform_dataframe(
            df=df,
            field_mapping=mapping_result_mapping,
            date_format=mapping_result_date_format,
            sign_convention=mapping_result_sign_convention,
            number_format=mapping_result_number_format,
            account_id=account_ids,
            source_file=str(file_path),
            source_type=source_type,
            source_origin=source_origin,
            import_id=import_id,
            balance_pass_threshold=tabular_cfg.balance_pass_threshold,
            balance_tolerance_cents=tabular_cfg.balance_tolerance_cents,
        )
```

- [ ] **Step 2.7: Run the broader test suite**

Run: `uv run pytest tests/moneybin/test_services/test_tabular_import_service.py tests/moneybin/test_extractors/test_tabular/ -v`
Expected: all green

- [ ] **Step 2.8: Commit**

```bash
git add src/moneybin/extractors/tabular/transforms.py \
        src/moneybin/services/import_service.py \
        tests/moneybin/test_extractors/test_tabular/test_transforms.py
git commit -m "Move tabular balance-validation tunables to TabularConfig"
```

---

## Task 3: Tighten `Literal` types through `column_mapper` → `transforms`

**Files:**
- Modify: `src/moneybin/extractors/tabular/column_mapper.py:44-77` (`MappingResult` dataclass)
- Modify: `src/moneybin/extractors/tabular/transforms.py:94-106` (`transform_dataframe` signature)
- Modify: `src/moneybin/extractors/tabular/transforms.py:362-368` (`_extract_amounts` signature)

`SignConventionType` and `NumberFormatType` already exist in `extractors/tabular/formats.py:28-31`. This task just imports them where they belong.

- [ ] **Step 3.1: Update `MappingResult` to use the literal types**

Edit `src/moneybin/extractors/tabular/column_mapper.py`. Add to the imports (around line 12):

```python
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)
```

Update the `MappingResult` dataclass (around line 44-77):

```python
@dataclass
class MappingResult:
    """Result of column mapping (Stage 3 output)."""

    field_mapping: dict[str, str]
    """Destination field → source column name."""

    confidence: str
    """Confidence tier: high, medium, low."""

    date_format: str | None = None
    """Detected date format string."""

    number_format: NumberFormatType = "us"
    """Detected number format convention."""

    sign_convention: SignConventionType = "negative_is_expense"
    """Detected sign convention."""

    sign_needs_confirmation: bool = False
    """True if sign convention is ambiguous."""

    is_multi_account: bool = False
    """True if account-identifying columns were detected."""

    unmapped_columns: list[str] = field(default_factory=list)
    """Source columns with no destination field match."""

    flagged_fields: list[str] = field(default_factory=list)
    """Fields matched with low confidence (content-only)."""

    sample_values: dict[str, list[str]] = field(default_factory=dict)
    """Sample values for each mapped field."""
```

- [ ] **Step 3.2: Update `transform_dataframe` signature**

Edit `src/moneybin/extractors/tabular/transforms.py`. Add to imports (around line 17-22):

```python
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)
```

Update the signature so `sign_convention` and `number_format` use literals:

```python
def transform_dataframe(
    *,
    df: pl.DataFrame,
    field_mapping: dict[str, str],
    date_format: str,
    sign_convention: SignConventionType,
    number_format: NumberFormatType,
    account_id: str | list[str],
    source_file: str,
    source_type: str,
    source_origin: str,
    import_id: str,
    balance_pass_threshold: float = 0.90,
    balance_tolerance_cents: int = 1,
) -> TransformResult:
```

Apply the same change to `_extract_amounts` (around line 362-368):

```python
def _extract_amounts(
    *,
    df: pl.DataFrame,
    field_mapping: dict[str, str],
    sign_convention: SignConventionType,
    number_format: NumberFormatType,
) -> tuple[list[Decimal | None], dict[int, str]]:
```

- [ ] **Step 3.3: Run pyright on touched files**

Run: `uv run pyright src/moneybin/extractors/tabular/transforms.py src/moneybin/extractors/tabular/column_mapper.py src/moneybin/services/import_service.py`
Expected: 0 errors. (`_import_tabular` already reads from `TabularFormat`'s typed fields and validates CLI overrides against the literal sets — the call sites are already type-correct, so no changes needed there.)

- [ ] **Step 3.4: Run the affected test suites**

Run: `uv run pytest tests/moneybin/test_extractors/test_tabular/ tests/moneybin/test_services/test_tabular_import_service.py -v`
Expected: all green.

- [ ] **Step 3.5: Commit**

```bash
git add src/moneybin/extractors/tabular/column_mapper.py \
        src/moneybin/extractors/tabular/transforms.py
git commit -m "Use Literal types for sign_convention and number_format in tabular pipeline"
```

---

## Task 4: Extract `ResolvedMapping` dataclass in `_import_tabular`

**Files:**
- Modify: `src/moneybin/services/import_service.py:239-557` (`_import_tabular`)
- Test: `tests/moneybin/test_services/test_tabular_import_service.py` — add a focused unit test

The current code unpacks 6 locals (`mapping_result_mapping`, `_date_format`, `_sign_convention`, `_number_format`, `_is_multi_account`, `_confidence`, `format_source`) in two branches around lines 355-373 of `import_service.py`. This task replaces them with a single `ResolvedMapping` instance.

- [ ] **Step 4.1: Write the failing test**

Add to `tests/moneybin/test_services/test_tabular_import_service.py`:

```python
def test_resolved_mapping_round_trip() -> None:
    """ResolvedMapping is constructible and exposes the resolved tabular fields."""
    from moneybin.services.import_service import ResolvedMapping

    rm = ResolvedMapping(
        field_mapping={"transaction_date": "Date", "amount": "Amt"},
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        is_multi_account=False,
        confidence="high",
    )
    assert rm.field_mapping["amount"] == "Amt"
    assert rm.sign_convention == "negative_is_expense"
    # Frozen — assignment must raise
    import dataclasses

    try:
        rm.confidence = "low"  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        pass
    else:
        raise AssertionError("ResolvedMapping must be frozen")
```

- [ ] **Step 4.2: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_tabular_import_service.py::test_resolved_mapping_round_trip -v`
Expected: FAIL with `ImportError: cannot import name 'ResolvedMapping'`

- [ ] **Step 4.3: Define `ResolvedMapping` in `import_service.py`**

Edit `src/moneybin/services/import_service.py`. Near the top of the file, after the existing `ImportResult` dataclass (after line 60), insert:

```python
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)


@dataclass(frozen=True)
class ResolvedMapping:
    """Final per-import mapping from the matched format or auto-detection.

    Both the matched-format branch and the auto-detect branch in
    ``_import_tabular`` produce one of these. Downstream code reads from
    the instance instead of six unpacked local variables.
    """

    field_mapping: dict[str, str]
    date_format: str
    sign_convention: SignConventionType
    number_format: NumberFormatType
    is_multi_account: bool
    confidence: str
```

- [ ] **Step 4.4: Refactor the two branches in `_import_tabular`**

In `_import_tabular`, replace the block at lines 355-386 (matched-format vs auto-detect) with:

```python
if matched_format:
    resolved = ResolvedMapping(
        field_mapping=matched_format.field_mapping,
        date_format=matched_format.date_format,
        sign_convention=matched_format.sign_convention,
        number_format=matched_format.number_format,
        is_multi_account=matched_format.multi_account,
        confidence="high",
    )
    format_source = "built-in" if matched_format.name in builtin_formats else "saved"
else:
    mapping_result = map_columns(df, overrides=overrides)
    resolved = ResolvedMapping(
        field_mapping=mapping_result.field_mapping,
        date_format=mapping_result.date_format or "%Y-%m-%d",
        sign_convention=mapping_result.sign_convention,
        number_format=mapping_result.number_format,
        is_multi_account=mapping_result.is_multi_account,
        confidence=mapping_result.confidence,
    )
    format_source = "detected"

    if mapping_result.sign_needs_confirmation and not sign:
        logger.warning(
            "⚠️  Sign convention is ambiguous (all amounts appear positive). "
            f"Proceeding with '{resolved.sign_convention}' — "
            "use --sign to override if expense amounts look wrong."
        )

    if mapping_result.confidence == "low":
        raise ValueError(
            f"Could not reliably detect column mapping for "
            f"{file_path.name}. Use --override to specify columns manually."
        )
```

The CLI overrides block (currently around lines 395-401) becomes:

```python
    # Apply CLI overrides — rebuild a new ResolvedMapping (frozen)
    if sign or date_format_override or number_format_override:
        resolved = dataclasses.replace(
            resolved,
            sign_convention=sign or resolved.sign_convention,  # type: ignore[arg-type]  # CLI validates against the literal set
            date_format=date_format_override or resolved.date_format,
            number_format=number_format_override or resolved.number_format,  # type: ignore[arg-type]  # CLI validates against the literal set
        )
```

Add `import dataclasses` to the top-level imports if not already present.

Replace every remaining reference to `mapping_result_mapping`, `mapping_result_date_format`, `mapping_result_sign_convention`, `mapping_result_number_format`, `mapping_result_is_multi_account`, `mapping_result_confidence` with `resolved.field_mapping`, `resolved.date_format`, `resolved.sign_convention`, `resolved.number_format`, `resolved.is_multi_account`, `resolved.confidence`. Pay attention to:

- `TABULAR_DETECTION_CONFIDENCE.labels(...)` (around line 393)
- `acct_name_col = mapping_result_mapping.get("account_name")` (around line 409)
- the `mapping_result_is_multi_account` check (around line 420)
- the `transform_dataframe(...)` keyword arguments (around lines 451-463)
- the `loader.finalize_import_batch(...)` call (around lines 496-512)
- the `auto-save format` block (around lines 530-551) — including the `confidence in ("high", "medium")` check and `field_mapping=resolved.field_mapping`, etc.

The `# type: ignore[reportArgumentType]` comments on `sign_convention` and `number_format` in the `TabularFormat(...)` construction (around lines 545, 547) can now be removed — both are already `SignConventionType` / `NumberFormatType`.

- [ ] **Step 4.5: Run the failing test to verify it passes**

Run: `uv run pytest tests/moneybin/test_services/test_tabular_import_service.py::test_resolved_mapping_round_trip -v`
Expected: PASS

- [ ] **Step 4.6: Run the full tabular test suite**

Run: `uv run pytest tests/moneybin/test_services/test_tabular_import_service.py tests/moneybin/test_extractors/test_tabular/ tests/moneybin/test_loaders/ -v`
Expected: all green.

- [ ] **Step 4.7: Run pyright on the modified file**

Run: `uv run pyright src/moneybin/services/import_service.py`
Expected: 0 errors.

- [ ] **Step 4.8: Run the E2E import workflow as a smoke test**

Run: `uv run pytest tests/e2e/test_e2e_workflows.py -v -k tabular`
Expected: all green. (If no tabular E2E test exists, run `tests/e2e/test_e2e_workflows.py -v` and confirm nothing regresses.)

- [ ] **Step 4.9: Commit**

```bash
git add src/moneybin/services/import_service.py \
        tests/moneybin/test_services/test_tabular_import_service.py
git commit -m "Extract ResolvedMapping dataclass in _import_tabular"
```

---

## Task 5: Extract `handle_database_errors` context manager

**Files:**
- Create: `src/moneybin/cli/utils.py`
- Test: `tests/moneybin/test_cli/test_handle_database_errors.py` (create)
- Modify (call sites — see step 5.4 for the full list): `src/moneybin/cli/commands/{import_cmd,stats,matches,categorize,synthetic,mcp,db,migrate,transform}.py`

The existing pattern (verified via grep) is repeated **30+ times** across 9 CLI command modules. Each occurrence is the same 5-line block:

```python
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e
```

Per `.claude/rules/cli.md`: "Recovery messages containing keys, tokens, or credentials must go to stderr via `typer.echo(..., err=True)` — **never through `logger.*()`**." But `database_key_error_hint()` returns a generic instructional message ("run `moneybin db unlock`") with no secret values, so the existing `logger.info` is acceptable. Keep that behavior — refactor only the structure, not the channel. Verify by reading `database_key_error_hint()` in `src/moneybin/database.py` before continuing.

- [ ] **Step 5.1: Write the failing tests**

Create `tests/moneybin/test_cli/test_handle_database_errors.py`:

```python
"""Tests for the shared CLI database-error handler."""

from unittest.mock import MagicMock, patch

import pytest
import typer

from moneybin.database import DatabaseKeyError


def test_handle_database_errors_yields_database() -> None:
    """When get_database succeeds, the context manager yields the Database."""
    from moneybin.cli.utils import handle_database_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with handle_database_errors() as db:
            assert db is fake_db


def test_handle_database_errors_translates_key_error_to_exit(caplog) -> None:
    """DatabaseKeyError is caught, logged, and converted to typer.Exit(1)."""
    from moneybin.cli.utils import handle_database_errors

    with patch(
        "moneybin.cli.utils.get_database",
        side_effect=DatabaseKeyError("locked"),
    ):
        with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exc_info:
            with handle_database_errors():
                pass
    assert exc_info.value.exit_code == 1
    assert "locked" in caplog.text


def test_handle_database_errors_lets_other_exceptions_propagate() -> None:
    """Non-DatabaseKeyError exceptions raised inside the block pass through."""
    from moneybin.cli.utils import handle_database_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with pytest.raises(RuntimeError, match="boom"):
            with handle_database_errors():
                raise RuntimeError("boom")
```

- [ ] **Step 5.2: Run the tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_handle_database_errors.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'moneybin.cli.utils'`

- [ ] **Step 5.3: Create `src/moneybin/cli/utils.py`**

```python
"""Shared helpers for CLI commands."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import typer

from moneybin.database import (
    Database,
    DatabaseKeyError,
    database_key_error_hint,
    get_database,
)

logger = logging.getLogger(__name__)


@contextmanager
def handle_database_errors() -> Iterator[Database]:
    """Get the active database with standard CLI error handling.

    Catches ``DatabaseKeyError`` (raised when the encryption key is not
    available — e.g. database is locked), logs a user-facing error and the
    standard ``moneybin db unlock`` hint, and exits with code 1. All other
    exceptions propagate unchanged so callers can handle them.
    """
    try:
        db = get_database()
    except DatabaseKeyError as e:
        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    yield db
```

- [ ] **Step 5.4: Run the tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_cli/test_handle_database_errors.py -v`
Expected: 3 passed.

- [ ] **Step 5.5: Run pyright**

Run: `uv run pyright src/moneybin/cli/utils.py`
Expected: 0 errors.

- [ ] **Step 5.6: Commit**

```bash
git add src/moneybin/cli/utils.py tests/moneybin/test_cli/test_handle_database_errors.py
git commit -m "Add handle_database_errors context manager for CLI commands"
```

---

## Task 6: Migrate CLI command call sites to `handle_database_errors`

**Files (call sites to refactor — verify each line range against the current file before editing):**

Use grep to enumerate every match before editing:

```bash
grep -rn "except DatabaseKeyError" src/moneybin/cli/commands/
```

Expect ~30 occurrences across these modules:

- `src/moneybin/cli/commands/import_cmd.py` (5 sites: lines ~219, ~260, ~330, ~604, ~639)
- `src/moneybin/cli/commands/stats.py` (1 site)
- `src/moneybin/cli/commands/matches.py` (5 sites)
- `src/moneybin/cli/commands/categorize.py` (4 sites)
- `src/moneybin/cli/commands/synthetic.py` — check
- `src/moneybin/cli/commands/mcp.py` — check
- `src/moneybin/cli/commands/db.py` — check
- `src/moneybin/cli/commands/migrate.py` — check
- `src/moneybin/cli/commands/transform.py` — check

For each call site, the typical *before* shape is:

```python
def some_command(...) -> None:
    setup_logging(cli_mode=True)
    try:
        db = get_database()
        # ... work using db ...
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e
    except ValueError as e:
        ...
```

The *after* shape:

```python
def some_command(...) -> None:
    setup_logging(cli_mode=True)
    try:
        with handle_database_errors() as db:
            # ... work using db ...
    except ValueError as e:
        ...
```

Subtleties:

1. The original handler runs *before* other excepts because `get_database()` is the first call inside `try`. The new shape puts `handle_database_errors()` *outside* the rest of the try, so other excepts must remain at the outer level. The shape above (outer `try` wrapping `with handle_database_errors() as db:`) preserves that order.
2. If a function only catches `DatabaseKeyError` (no other excepts), the shape simplifies to just `with handle_database_errors() as db: ...` — no outer try needed.
3. Remove the now-unused `from moneybin.database import DatabaseKeyError, get_database` import — replace with `from moneybin.cli.utils import handle_database_errors`. If `get_database` is still used elsewhere in the same file (e.g., outside any handler), keep that part of the import.
4. Remove the inline `from moneybin.database import database_key_error_hint` — it's unused after the refactor.

- [ ] **Step 6.1: Refactor `src/moneybin/cli/commands/import_cmd.py`**

Read the file to confirm the exact shape of each of the 5 sites at lines ~170, ~219, ~253, ~314, ~330, ~583, ~604, ~627, ~639. For each command (`import_file`, `import_history`, `import_revert`, `delete_format`, `import_status`), apply the *before*/*after* transformation above.

Verify after edits: `grep -n "except DatabaseKeyError\|database_key_error_hint" src/moneybin/cli/commands/import_cmd.py` — expect zero matches.

Run the relevant tests:

```bash
uv run pytest tests/moneybin/test_cli/ -v -k import
uv run pyright src/moneybin/cli/commands/import_cmd.py
```

Expected: green.

- [ ] **Step 6.2: Refactor `src/moneybin/cli/commands/stats.py`**

Apply the same pattern to the single site at line ~42.

Verify: `grep -n "except DatabaseKeyError" src/moneybin/cli/commands/stats.py` — expect zero matches.

Run: `uv run pytest tests/moneybin/test_stats_command.py -v && uv run pyright src/moneybin/cli/commands/stats.py`
Expected: green.

- [ ] **Step 6.3: Refactor `src/moneybin/cli/commands/matches.py`**

Apply the same pattern to the 5 sites at lines ~52, ~214, ~260, ~287, ~330. Verify and run:

```bash
grep -n "except DatabaseKeyError" src/moneybin/cli/commands/matches.py  # expect 0
uv run pytest tests/moneybin/test_cli/ -v -k matches
uv run pyright src/moneybin/cli/commands/matches.py
```

- [ ] **Step 6.4: Refactor `src/moneybin/cli/commands/categorize.py`**

Apply to the 4 sites at lines ~42, ~67, ~87, ~131. Verify and run:

```bash
grep -n "except DatabaseKeyError" src/moneybin/cli/commands/categorize.py  # expect 0
uv run pytest tests/moneybin/test_cli/ -v -k categorize
uv run pyright src/moneybin/cli/commands/categorize.py
```

- [ ] **Step 6.5: Refactor `src/moneybin/cli/commands/{synthetic,mcp,db,migrate,transform}.py`**

For each file in `synthetic.py mcp.py db.py migrate.py transform.py`:

```bash
grep -n "except DatabaseKeyError" src/moneybin/cli/commands/<file>
```

Apply the same transformation to each site. After editing each file, verify there are no remaining matches and run pyright on the file.

- [ ] **Step 6.6: Final verification — no regressions in repo-wide grep**

Run: `grep -rn "except DatabaseKeyError" src/moneybin/cli/`
Expected: no matches.

Run: `grep -rn "database_key_error_hint" src/moneybin/cli/`
Expected: no matches in `cli/commands/`. (Matches inside `cli/utils.py` and `database.py` are fine.)

- [ ] **Step 6.7: Full unit-test run**

Run: `make test`
Expected: green.

- [ ] **Step 6.8: Full E2E run**

Run: `uv run pytest tests/e2e/ -v -m "e2e and not slow"`
Expected: green. (Lock/unlock paths are exercised — verify especially `tests/e2e/test_e2e_mutating.py` and `test_e2e_workflows.py` still pass.)

- [ ] **Step 6.9: Commit**

```bash
git add src/moneybin/cli/commands/
git commit -m "Migrate CLI commands to handle_database_errors context manager"
```

---

## Task 7: Pre-push quality pass

- [ ] **Step 7.1: Run `make check test`**

Run: `make check test`
Expected: format clean, lint clean, pyright clean, all tests green.

- [ ] **Step 7.2: Self-review the diff**

Run: `git diff main..HEAD --stat`
Confirm: only the files listed in this plan changed. No drive-by edits.

- [ ] **Step 7.3: Push and open PR**

```bash
git push -u origin refactor/tabular-import-cleanup-stream-a
gh pr create --title "Refactor tabular import internals" --body "$(cat <<'EOF'
## Summary
- Extract `ResolvedMapping` dataclass in `_import_tabular` (Stream A1)
- Thread `SignConventionType` / `NumberFormatType` through `column_mapper` and `transforms` (A2)
- Move `balance_pass_threshold` and `balance_tolerance_cents` to `TabularConfig` (A3)
- Extract `handle_database_errors` context manager and adopt across 9 CLI command modules (A4)

No behavior change. Spec: docs/specs/tabular-import-cleanup.md (Stream A).

## Test plan
- [x] `make check test`
- [x] `uv run pytest tests/e2e/ -m "e2e and not slow"`
- [ ] Manual smoke: `moneybin import file <fixture.csv>` succeeds
EOF
)"
```
