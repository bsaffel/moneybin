# Tabular Import Cleanup

**Status**: draft
**Type**: Feature (refactor)
**Parent**: [Tabular Import](smart-import-tabular.md)

Post-ship cleanup for the tabular import pipeline. Three independent work
streams: a refactor PR for internal code quality, a fix PR for Decimal
correctness, and a behavior change for account matching.

---

## Stream A: Internal refactoring (one `refactor/` PR)

All mechanical changes with no behavior change. Can land in a single PR.

### A1. Extract `ResolvedMapping` dataclass

**Problem**: `_import_tabular()` in `import_service.py` unpacks column mapping
results into 5+ local variables (`mapping_result_mapping`,
`mapping_result_date_format`, `mapping_result_sign_convention`, etc.) in two
branches (matched-format vs. auto-detected). The repetition makes it easy to
forget a field when adding new mapping properties.

**Fix**: Create a `ResolvedMapping` dataclass (or frozen Pydantic model) that
holds `field_mapping`, `date_format`, `sign_convention`, `number_format`,
`is_multi_account`, and `confidence`. Both branches construct one instance, and
the rest of the function reads from it.

```python
@dataclass(frozen=True)
class ResolvedMapping:
    field_mapping: dict[str, str]
    date_format: str
    sign_convention: str
    number_format: str
    is_multi_account: bool
    confidence: str
```

### A2. Use `Literal` types through the service layer

**Problem**: `sign_convention` and `number_format` are `Literal` types on
`TabularFormat` but degrade to bare `str` the moment they leave the model.
`_import_tabular()`, `transform_dataframe()`, and `_extract_amounts()` all
accept plain `str`, bypassing type checking.

**Fix**: Use `SignConventionType` and `NumberFormatType` (already defined in
`formats.py`) in function signatures. The `ResolvedMapping` dataclass from A1
should use these types too. CLI override validation already checks membership;
this just threads the types through.

**Files**: `import_service.py`, `transforms.py`, `column_mapper.py` (return
type of `map_columns`).

### A3. Move balance validation parameters to config

**Problem**: `_balance_tolerance = Decimal("0.01")` and `_pass_threshold = 0.90`
in `transforms.py` are algorithm parameters that affect import correctness.
Per CLAUDE.md, tunable parameters belong in config.

**Fix**: Add to `TabularConfig` in `config.py`:

```python
balance_pass_threshold: float = Field(
    default=0.90,
    description=(
        "Minimum fraction of balance deltas that must match "
        "for balance validation to pass"
    ),
)
balance_tolerance_cents: int = Field(
    default=1,
    description="Per-delta tolerance in cents for balance validation",
)
```

Thread through `transform_dataframe()` → `_validate_running_balance()`. The
fuzzy match threshold `0.6` in `account_matching.py:81` should also move here
(as `account_match_threshold: float = 0.6`), but can wait until account
matching is wired in (Stream C).

### A4. Extract `DatabaseKeyError` handler

**Problem**: Every CLI command that calls `get_database()` repeats the same
5-line `except DatabaseKeyError` block with a hint message. This pattern
appears in `import_file`, `import_history`, `import_revert`, `delete_format`,
`import_status`, and will appear in every future command.

**Fix**: Extract a context manager or decorator:

```python
@contextmanager
def handle_database_errors() -> Iterator[Database]:
    """Get database with standard error handling for CLI commands."""
    try:
        yield get_database()
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e
```

This applies to **all** CLI commands project-wide, not just import. Consider
placing it in `src/moneybin/cli/utils.py` or similar.

---

## Stream B: Decimal correctness (one `fix/` PR)

### B1. Replace `float` with `Decimal` for monetary values

**Problem**: Several modules outside the tabular pipeline still use `float`
for currency amounts, violating the project rule in `database.md`. The tabular
pipeline was fixed in this PR, but the issue exists elsewhere.

**Known locations** (verify before fixing — these were identified during the
tabular import review and may have changed):

- `src/moneybin/extractors/ofx_extractor.py` — OFX amounts parsed as float
- `src/moneybin/extractors/w2_extractor.py` — W-2 wage/tax amounts as float
- `src/moneybin/mcp/` — MCP tool responses with float amounts
- `tests/moneybin/test_services/test_synthetic_data.py` — synthetic data
  generator uses float for amounts

**Fix**: For each file:
1. Import `Decimal` and use `Decimal(str(value))` for parsed amounts
2. Use `pl.Decimal(precision=18, scale=2)` in Polars schemas
3. Update test assertions to use `Decimal("...")` literals

---

## Stream C: Account matching (one `feat/` or `fix/` PR)

### C1. Wire `match_account()` into the import pipeline

**Problem**: `account_matching.py` defines a multi-tier matching function
(account number → slug → fuzzy name) with full test coverage, but
`_import_tabular()` in `import_service.py` never calls it. It goes straight
to `slugify(account_name)`. Re-importing the same account under a slightly
different name (e.g., "Chase Checking" vs "CHASE CHECKING ACCT") creates a
duplicate account record.

**Current TODO**: Lines 31-33 of `account_matching.py`.

**Fix**:
1. Query existing accounts from `raw.tabular_accounts` before the account
   ID assignment block in `_import_tabular()`
2. Call `match_account()` with the account name and any detected account number
3. Handle the three outcomes:
   - `matched=True` → use `result.account_id`
   - `matched=False` with candidates → log a warning listing candidates,
     proceed with `slugify()` (non-interactive mode), or prompt for selection
     (interactive mode)
   - `matched=False`, no candidates → create new account via `slugify()`
4. Re-add `--yes` / `-y` to `import_file` to auto-accept fuzzy matches
5. Move the fuzzy threshold `0.6` to `TabularConfig` (see A3)

This is the only stream with a user-visible behavior change. It should have
its own PR with targeted tests for the matching-in-pipeline flow.

---

## Sequencing

Streams are independent and can land in any order:

1. **Stream A** first — purely internal, no risk, makes future work cleaner
2. **Stream B** next — correctness fix, low risk but touches more files
3. **Stream C** last — behavior change, needs the most testing
