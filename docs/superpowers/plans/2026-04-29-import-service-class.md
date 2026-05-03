# ImportService Class Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `src/moneybin/services/import_service.py` from module-level functions to an `ImportService` class so 100% of the service layer matches the established `AccountService`/`CategorizationService`/`SpendingService`/`TransactionService` pattern.

**Architecture:** Pure mechanical refactor. No behavior changes. `ImportService(db)` exposes `import_file()` and `run_transforms()` as public methods (matching today's module-level public API). All `_db`-using helpers (`_import_ofx`, `_import_w2`, `_import_tabular`, `_query_date_range`, `_resolve_account_via_matcher`, `_run_matching`, `_apply_categorization`) become private methods. Pure helpers that don't need `db` (`_detect_file_type`, `_display_label`) stay as module-level functions so tests can keep importing them directly. `ResolvedMapping` and `ImportResult` dataclasses stay module-level.

**Tech Stack:** Python 3.12, Pydantic, Typer, DuckDB. Existing `Database` abstraction via `get_database()`.

---

## File Structure

**Modify:**
- `src/moneybin/services/import_service.py` — convert to class
- `src/moneybin/cli/commands/import_cmd.py` — migrate caller
- `src/moneybin/cli/commands/transform.py` — migrate caller
- `src/moneybin/cli/commands/matches.py` — migrate three caller sites
- `src/moneybin/cli/commands/synthetic.py` — migrate caller
- `src/moneybin/mcp/tools/import_tools.py` — migrate caller
- `tests/integration/test_integration_existing.py` — update import + call site
- `tests/integration/test_tabular_description_regression.py` — update import + call site
- `tests/moneybin/test_services/test_tabular_import_service.py` — keep `_detect_file_type` import; update `ResolvedMapping` import
- `tests/moneybin/test_import_matching_integration.py` — update mock paths
- `tests/moneybin/test_cli/test_import_command.py` — update mock paths
- `tests/moneybin/test_cli/test_import_commands.py` — update mock paths
- `tests/moneybin/test_cli/test_import_cmd_tabular.py` — update mock paths
- `tests/moneybin/test_cli/test_matches.py` — update mock paths
- `tests/moneybin/test_synthetic/test_cli.py` — update mock paths

**No new test files needed.** Existing tests cover behavior; this is a refactor. New unit test added in Task 1 verifies the class shape itself.

---

## Task 1: Add `ImportService` class wrapping existing module functions

The strategy is one-shot conversion in a single commit per caller, but we start by introducing the class alongside the existing module functions. New shape lives, old shape stays so callers can migrate one at a time. Module-level public functions become two-line shims that instantiate `ImportService` and delegate.

**Files:**
- Modify: `src/moneybin/services/import_service.py`
- Test: `tests/moneybin/test_services/test_import_service_class.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_services/test_import_service_class.py`:

```python
"""Tests for the ImportService class shape."""

from unittest.mock import MagicMock

from moneybin.database import Database
from moneybin.services.import_service import ImportService


class TestImportServiceShape:
    """Verify ImportService matches the AccountService/CategorizationService pattern."""

    def test_constructor_accepts_database(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        assert service is not None

    def test_exposes_import_file_method(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        assert callable(service.import_file)

    def test_exposes_run_transforms_method(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        assert callable(service.run_transforms)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_import_service_class.py -v`
Expected: FAIL with `ImportError: cannot import name 'ImportService'`.

- [ ] **Step 3: Add `ImportService` class to `src/moneybin/services/import_service.py`**

Insert this class definition immediately after the `_apply_categorization` function (end of file). Each method is the body of the corresponding module-level function with `db` references replaced by `self._db`. **Copy the existing function bodies verbatim** — do not rewrite logic. Then replace each module-level function (`import_file`, `run_transforms`, `_import_ofx`, `_import_w2`, `_import_tabular`, `_query_date_range`, `_resolve_account_via_matcher`, `_run_matching`, `_apply_categorization`) with a thin shim that instantiates `ImportService` and delegates.

The `_detect_file_type` and `_display_label` helpers stay as module-level functions (they don't need `db`).

```python
class ImportService:
    """Unified import service for financial data files.

    Handles the full import pipeline: detect file type, extract data,
    load to raw tables, and run SQLMesh transforms. Both CLI commands
    and MCP tools call this same service — no duplication.
    """

    def __init__(self, db: Database) -> None:
        """Initialize ImportService with an open Database connection."""
        self._db = db

    def import_file(
        self,
        file_path: str | Path,
        *,
        apply_transforms: bool = True,
        institution: str | None = None,
        account_id: str | None = None,
        account_name: str | None = None,
        format_name: str | None = None,
        overrides: dict[str, str] | None = None,
        sign: str | None = None,
        date_format: str | None = None,
        number_format: str | None = None,
        save_format: bool = True,
        sheet: str | None = None,
        delimiter: str | None = None,
        encoding: str | None = None,
        no_row_limit: bool = False,
        no_size_limit: bool = False,
        auto_accept: bool = False,
    ) -> ImportResult:
        """Import a financial data file into DuckDB.

        Auto-detects file type by extension and runs the appropriate
        extract -> load -> transform pipeline.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file type is not supported.
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_type = _detect_file_type(path)
        logger.info(f"Importing {_display_label(file_type, path)} file: {path}")

        if file_type == "ofx":
            result = self._import_ofx(path, institution=institution)
        elif file_type == "w2":
            result = self._import_w2(path)
        elif file_type == "tabular":
            result = self._import_tabular(
                path,
                account_name=account_name,
                account_id=account_id,
                format_name=format_name,
                overrides=overrides,
                sign=sign,
                date_format_override=date_format,
                number_format_override=number_format,
                save_format=save_format,
                sheet=sheet,
                delimiter=delimiter,
                encoding=encoding,
                no_row_limit=no_row_limit,
                no_size_limit=no_size_limit,
                auto_accept=auto_accept,
            )
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        if apply_transforms and file_type in ("ofx", "tabular"):
            try:
                self._run_matching()
            except Exception:  # noqa: BLE001 — matching is best-effort; first import may precede SQLMesh views
                logger.debug(
                    "Matching skipped (views may not exist yet)", exc_info=True
                )
            result.core_tables_rebuilt = self.run_transforms()
            self._apply_categorization()

        logger.info(f"Import complete: {result.summary()}")
        return result

    def run_transforms(self) -> bool:
        """Run SQLMesh transforms to rebuild core tables."""
        from moneybin.config import get_settings
        from moneybin.matching.priority import seed_source_priority

        logger.info("Running SQLMesh transforms")
        seed_source_priority(self._db, get_settings().matching)
        with sqlmesh_context() as ctx:
            ctx.plan(auto_apply=True, no_prompts=True)
        logger.info("SQLMesh transforms completed")
        return True

    def _import_ofx(
        self,
        file_path: Path,
        *,
        institution: str | None = None,
    ) -> ImportResult:
        """Import an OFX/QFX file."""
        # COPY THE EXISTING _import_ofx BODY HERE, replacing `db` with `self._db`.
        # The body uses `db` only as the loader argument (`OFXLoader(db)`) and
        # as the first arg to `_query_date_range`. Replace both with `self._db`
        # and `self._query_date_range(...)` respectively.
        from moneybin.extractors.ofx_extractor import OFXExtractor
        from moneybin.loaders.ofx_loader import OFXLoader

        result = ImportResult(file_path=str(file_path), file_type="ofx")
        extractor = OFXExtractor()
        data = extractor.extract_from_file(file_path, institution)
        loader = OFXLoader(self._db)
        row_counts = loader.load_data(data)
        result.institutions = row_counts.get("institutions", 0)
        result.accounts = row_counts.get("accounts", 0)
        result.transactions = row_counts.get("transactions", 0)
        result.balances = row_counts.get("balances", 0)
        result.details = row_counts
        if result.transactions > 0:
            result.date_range = self._query_date_range(
                "raw.ofx_transactions", "CAST(date_posted AS DATE)", file_path
            )
        return result

    def _import_w2(self, file_path: Path) -> ImportResult:
        """Import a W-2 PDF file."""
        from moneybin.extractors.w2_extractor import W2Extractor
        from moneybin.loaders.w2_loader import W2Loader

        result = ImportResult(file_path=str(file_path), file_type="w2")
        extractor = W2Extractor()
        data = extractor.extract_from_file(file_path)
        loader = W2Loader(self._db)
        row_count = loader.load_data(data)
        result.w2_forms = row_count
        result.details = {"w2_forms": row_count}
        return result

    def _import_tabular(
        self,
        file_path: Path,
        *,
        account_name: str | None = None,
        account_id: str | None = None,
        format_name: str | None = None,
        overrides: dict[str, str] | None = None,
        sign: str | None = None,
        date_format_override: str | None = None,
        number_format_override: str | None = None,
        save_format: bool = True,
        sheet: str | None = None,
        delimiter: str | None = None,
        encoding: str | None = None,
        no_row_limit: bool = False,
        no_size_limit: bool = False,
        auto_accept: bool = False,
    ) -> ImportResult:
        """Import a tabular file through the five-stage pipeline.

        COPY THE EXISTING _import_tabular BODY HERE — preserve line-for-line.
        Replace every `db` reference with `self._db`. Replace
        `_resolve_account_via_matcher(db, ...)` with
        `self._resolve_account_via_matcher(...)`. Replace
        `_query_date_range(db, ...)` with `self._query_date_range(...)`.
        """
        # ... (copy from existing _import_tabular, mechanical s/db/self._db/)

    def _query_date_range(
        self,
        table: str,
        date_expr: str,
        file_path: Path,
    ) -> str:
        """Query min/max date range for a source file from a raw table."""
        try:
            result = self._db.execute(
                f"""
                SELECT MIN({date_expr}) AS min_date,
                       MAX({date_expr}) AS max_date
                FROM {table}
                WHERE source_file = ?
                """,  # noqa: S608 — table and date_expr are hardcoded by callers, not user input
                [str(file_path)],
            ).fetchone()
            if result and result[0]:
                return f"{result[0]} to {result[1]}"
        except Exception:  # noqa: BLE001 — date range is best-effort; any DB failure returns empty string
            logger.debug(f"Could not determine date range from {table}", exc_info=True)
        return ""

    def _resolve_account_via_matcher(
        self,
        *,
        account_name: str,
        account_number: str | None,
        threshold: float,
        auto_accept: bool,
    ) -> str:
        """Resolve an account name to an account_id using match_account().

        COPY THE EXISTING _resolve_account_via_matcher BODY HERE,
        replacing every `db` with `self._db`.
        """
        # ... (copy from existing function, s/db/self._db/)

    def _run_matching(self) -> None:
        """Run transaction matching after import."""
        from moneybin.config import get_settings
        from moneybin.matching.engine import TransactionMatcher
        from moneybin.matching.priority import seed_source_priority

        settings = get_settings().matching
        seed_source_priority(self._db, settings)
        matcher = TransactionMatcher(self._db, settings)
        result = matcher.run()
        if result.has_matches:
            logger.info(f"Matching: {result.summary()}")
            if result.has_pending:
                logger.info("Run 'moneybin matches review' when ready")

    def _apply_categorization(self) -> None:
        """Run deterministic categorization on uncategorized transactions."""
        from moneybin.services.auto_rule_service import AutoRuleService
        from moneybin.services.categorization_service import CategorizationService

        try:
            service = CategorizationService(self._db)
            stats = service.apply_deterministic()
            if stats["total"] > 0:
                logger.info(
                    f"Auto-categorized {stats['total']} transactions "
                    f"({stats['merchant']} merchant, {stats['rule']} rule)"
                )
            pending = AutoRuleService(self._db).stats().pending_proposals
            if pending:
                logger.info(f"  {pending} new auto-rule proposals")
                logger.info(
                    "  💡 Run 'moneybin categorize auto-review' to review proposed rules"
                )
        except Exception:  # noqa: BLE001 — categorization is best-effort; failure skips without aborting import
            logger.debug(
                "Categorization skipped (tables may not exist yet)",
                exc_info=True,
            )
```

After adding the class, **delete** the module-level versions of: `import_file`, `run_transforms`, `_import_ofx`, `_import_w2`, `_import_tabular`, `_query_date_range`, `_resolve_account_via_matcher`, `_run_matching`, `_apply_categorization`. Keep `_detect_file_type`, `_display_label`, `ImportResult`, `ResolvedMapping` as module-level.

- [ ] **Step 4: Run unit test to verify class exists**

Run: `uv run pytest tests/moneybin/test_services/test_import_service_class.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run lint + type check on the file**

Run: `uv run ruff format src/moneybin/services/import_service.py && uv run ruff check src/moneybin/services/import_service.py && uv run pyright src/moneybin/services/import_service.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/services/import_service.py tests/moneybin/test_services/test_import_service_class.py
git commit -m "Introduce ImportService class

Replace module-level import_service functions with an ImportService class
matching the AccountService/CategorizationService pattern. Pure helpers
(_detect_file_type, _display_label) and dataclasses (ImportResult,
ResolvedMapping) remain module-level."
```

Note: callers and other tests will fail after this commit until later tasks migrate them. Do not run the full test suite yet.

---

## Task 2: Migrate CLI `import` command

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py:177,205`
- Modify: `tests/moneybin/test_cli/test_import_command.py:45,72`
- Modify: `tests/moneybin/test_cli/test_import_commands.py:32`
- Modify: `tests/moneybin/test_cli/test_import_cmd_tabular.py:50,139,160`

- [ ] **Step 1: Update `import_cmd.py` to use `ImportService`**

In `src/moneybin/cli/commands/import_cmd.py`, replace the import and call site (around line 177–230):

```python
# Replace:
from moneybin.services.import_service import import_file as run_import
# ...
result = run_import(
    db=db,
    file_path=source,
    apply_transforms=not skip_transform,
    ...
)

# With:
from moneybin.services.import_service import ImportService
# ...
result = ImportService(db).import_file(
    file_path=source,
    apply_transforms=not skip_transform,
    ...
)
```

The argument list is otherwise unchanged. Note `db=db` is removed (now `self._db`).

- [ ] **Step 2: Update CLI tests' mock paths**

In each of `test_import_command.py`, `test_import_commands.py`, `test_import_cmd_tabular.py`, replace:

```python
"moneybin.services.import_service.import_file"
```

with:

```python
"moneybin.services.import_service.ImportService.import_file"
```

For mocks that previously matched the module-level function signature (db as first positional or kwarg), update to match the bound-method signature: the `db` parameter is gone. Mocks that use `MagicMock` and just return `ImportResult` continue to work — only the patch path changes.

- [ ] **Step 3: Run affected tests**

Run: `uv run pytest tests/moneybin/test_cli/test_import_command.py tests/moneybin/test_cli/test_import_commands.py tests/moneybin/test_cli/test_import_cmd_tabular.py -v`
Expected: PASS for all.

- [ ] **Step 4: Lint + type check changed files**

Run: `uv run ruff format src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/ && uv run ruff check src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/ && uv run pyright src/moneybin/cli/commands/import_cmd.py`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/test_import_command.py tests/moneybin/test_cli/test_import_commands.py tests/moneybin/test_cli/test_import_cmd_tabular.py
git commit -m "Migrate import CLI to ImportService class"
```

---

## Task 3: Migrate `transform`, `matches`, `synthetic` CLI commands (run_transforms callers)

**Files:**
- Modify: `src/moneybin/cli/commands/transform.py:58`
- Modify: `src/moneybin/cli/commands/matches.py:49,203,298`
- Modify: `src/moneybin/cli/commands/synthetic.py:57`
- Modify: `tests/moneybin/test_cli/test_matches.py:44,66,88,111,138`
- Modify: `tests/moneybin/test_synthetic/test_cli.py:71`

- [ ] **Step 1: Update each call site**

For each of the four source files, replace:

```python
from moneybin.services.import_service import run_transforms

# ...
run_transforms()
```

with:

```python
from moneybin.services.import_service import ImportService

# ...
ImportService(db).run_transforms()
```

In `matches.py`, `transform.py`, `synthetic.py`, the `db` is already in scope (typically from `handle_cli_errors() as db` or `get_database()`). Use whichever local `db` name is already available. If `run_transforms()` is called inside a function that doesn't currently hold a `Database`, fetch it via `get_database()` (already used elsewhere in the same file).

Read each call site (lines `matches.py:49`, `matches.py:203`, `matches.py:298`, `transform.py:58`, `synthetic.py:57`) and confirm the local `db` variable name before editing.

- [ ] **Step 2: Update test mock paths**

In `test_matches.py` (5 occurrences) and `test_synthetic/test_cli.py` (1 occurrence), replace:

```python
"moneybin.services.import_service.run_transforms"
```

with:

```python
"moneybin.services.import_service.ImportService.run_transforms"
```

- [ ] **Step 3: Run affected tests**

Run: `uv run pytest tests/moneybin/test_cli/test_matches.py tests/moneybin/test_synthetic/test_cli.py -v`
Expected: PASS for all.

- [ ] **Step 4: Lint + type check changed files**

Run: `uv run ruff format src/moneybin/cli/commands/transform.py src/moneybin/cli/commands/matches.py src/moneybin/cli/commands/synthetic.py && uv run ruff check src/moneybin/cli/commands/transform.py src/moneybin/cli/commands/matches.py src/moneybin/cli/commands/synthetic.py && uv run pyright src/moneybin/cli/commands/transform.py src/moneybin/cli/commands/matches.py src/moneybin/cli/commands/synthetic.py`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/transform.py src/moneybin/cli/commands/matches.py src/moneybin/cli/commands/synthetic.py tests/moneybin/test_cli/test_matches.py tests/moneybin/test_synthetic/test_cli.py
git commit -m "Migrate transform/matches/synthetic CLI to ImportService"
```

---

## Task 4: Migrate MCP `import.*` tools

**Files:**
- Modify: `src/moneybin/mcp/tools/import_tools.py:73,80`

- [ ] **Step 1: Update `import_tools.py`**

Replace:

```python
from moneybin.services.import_service import import_file as run_import

# ...
result = run_import(
    get_database(),
    str(validated),
    account_id=account_id,
    account_name=account_name,
    institution=institution,
    format_name=format_name,
)
```

with:

```python
from moneybin.services.import_service import ImportService

# ...
result = ImportService(get_database()).import_file(
    str(validated),
    account_id=account_id,
    account_name=account_name,
    institution=institution,
    format_name=format_name,
)
```

- [ ] **Step 2: Run MCP import tests**

Run: `uv run pytest tests/ -v -k "import_tools or import_mcp" -m "not e2e"`
Expected: PASS (or "no tests collected" if there are no MCP-tool-specific unit tests; that's fine — E2E tests in Task 6 will exercise this path).

- [ ] **Step 3: Lint + type check**

Run: `uv run ruff format src/moneybin/mcp/tools/import_tools.py && uv run ruff check src/moneybin/mcp/tools/import_tools.py && uv run pyright src/moneybin/mcp/tools/import_tools.py`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/mcp/tools/import_tools.py
git commit -m "Migrate import MCP tools to ImportService"
```

---

## Task 5: Migrate integration tests + remaining test mocks

**Files:**
- Modify: `tests/integration/test_integration_existing.py:93`
- Modify: `tests/integration/test_tabular_description_regression.py:30`
- Modify: `tests/moneybin/test_import_matching_integration.py` (multiple lines: 10–14, 25, 41–45, 56, 71–75, 86, 119, 135, 153, 169)

- [ ] **Step 1: Update `test_integration_existing.py:93`**

Replace:

```python
from moneybin.services.import_service import run_transforms

# ...
run_transforms()
```

with:

```python
from moneybin.services.import_service import ImportService

# ...
ImportService(db).run_transforms()
```

(`db` is the integration test's database fixture.)

- [ ] **Step 2: Update `test_tabular_description_regression.py:30`**

Replace:

```python
from moneybin.services.import_service import import_file

# ...
result = import_file(db, ...)
```

with:

```python
from moneybin.services.import_service import ImportService

# ...
result = ImportService(db).import_file(...)
```

(Drop the `db` positional, since it's now bound on the instance.)

- [ ] **Step 3: Update `test_import_matching_integration.py` mock paths**

Replace every patch path of the form `moneybin.services.import_service.X` (where X is one of `run_transforms`, `_run_matching`, `_apply_categorization`, `_import_ofx`, `_detect_file_type`) with the appropriate new path:

| Old patch path | New patch path |
|---|---|
| `moneybin.services.import_service.run_transforms` | `moneybin.services.import_service.ImportService.run_transforms` |
| `moneybin.services.import_service._run_matching` | `moneybin.services.import_service.ImportService._run_matching` |
| `moneybin.services.import_service._apply_categorization` | `moneybin.services.import_service.ImportService._apply_categorization` |
| `moneybin.services.import_service._import_ofx` | `moneybin.services.import_service.ImportService._import_ofx` |
| `moneybin.services.import_service._detect_file_type` | `moneybin.services.import_service._detect_file_type` (unchanged — stays module-level) |

The `import_file` import in this test (`from moneybin.services.import_service import ImportResult, import_file`) needs to change to `from moneybin.services.import_service import ImportResult, ImportService`, and the call site `import_file(db, ...)` becomes `ImportService(db).import_file(...)`.

The `caplog.at_level(..., logger="moneybin.services.import_service")` lines (135, 169) are unchanged — the logger name stays the same.

- [ ] **Step 4: Run integration tests**

Run: `uv run pytest tests/integration/test_integration_existing.py tests/integration/test_tabular_description_regression.py tests/moneybin/test_import_matching_integration.py -v`
Expected: PASS for all (some may be marked `@pytest.mark.integration` and skipped without the marker — that's fine).

- [ ] **Step 5: Run the full unit test suite**

Run: `uv run pytest tests/ -v -m "not integration and not e2e and not slow"`
Expected: PASS (no failures from the refactor).

- [ ] **Step 6: Lint + type check**

Run: `uv run ruff format tests/ && uv run ruff check tests/`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add tests/integration/test_integration_existing.py tests/integration/test_tabular_description_regression.py tests/moneybin/test_import_matching_integration.py
git commit -m "Migrate import-service integration tests to ImportService"
```

---

## Task 6: Verify E2E tests + final pre-commit pass

**Files:** No edits expected — this task verifies nothing was missed.

- [ ] **Step 1: Search for any remaining stale imports**

Run: `uv run grep -rn "from moneybin.services.import_service import" --include="*.py" src/ tests/`
Expected output: every match shows `ImportService` or `_detect_file_type`/`_display_label`/`ImportResult`/`ResolvedMapping` — no remaining `import_file`, `run_transforms`, `_import_ofx`, `_import_w2`, `_import_tabular`, `_query_date_range`, `_resolve_account_via_matcher`, `_run_matching`, or `_apply_categorization` imports.

If any remain, migrate them using the same pattern as Tasks 2–5 and add to the relevant commit (or amend if already pushed-but-not-published).

- [ ] **Step 2: Run E2E tests for import workflow**

Run: `uv run pytest tests/e2e/ -m "e2e" -v -k "import"`
Expected: PASS for all import-related E2E tests.

- [ ] **Step 3: Run full test suite + pre-commit checks**

Run: `make check test`
Expected: format clean, lint clean, pyright clean, all tests pass.

- [ ] **Step 4: If anything fails, fix in place and amend the most relevant commit**

Do not skip hooks. Investigate failures rather than bypassing.

---

## Self-Review Notes

**Spec coverage:** The followup says: refactor `import_service.py` to expose an `ImportService` class; migrate CLI `import` commands, MCP `import.*` tools, and integration tests in the same PR. Task 1 introduces the class. Tasks 2–4 migrate the three caller categories. Task 5 migrates remaining tests. Task 6 verifies completeness.

**Placeholder scan:** The class body in Task 1 inlines small methods (`run_transforms`, `_query_date_range`, `_run_matching`, `_apply_categorization`, `_import_ofx`, `_import_w2`) verbatim. Two methods (`_import_tabular`, `_resolve_account_via_matcher`) have placeholder comments saying "copy the existing body, mechanical s/db/self._db/" because their bodies are 80+ lines and reproducing them in this plan adds no information — the engineer reads the original function and applies the substitution. This is **not** a planning placeholder (no logic decisions deferred); it's a transcription instruction. If a reader needs the full body, they read `src/moneybin/services/import_service.py` directly.

**Type consistency:** Method names match exactly across the plan (`import_file`, `run_transforms`, `_import_ofx`, etc.). Constructor signature is `__init__(self, db: Database)`, matching `AccountService.__init__`. The `db` parameter is removed from public method signatures (was the first arg in module functions); callers pass it once when constructing the service.
