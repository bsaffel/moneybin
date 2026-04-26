# Tabular Import Cleanup — Stream C (Wire Account Matching) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing `match_account()` (in `extractors/tabular/account_matching.py`) into `_import_tabular()` so re-imports under varying account names ("Chase Checking" vs "CHASE CHECKING ACCT") map to the same account ID instead of creating duplicates. Surface the fuzzy threshold via `TabularConfig` and add a non-interactive `--yes` flag to auto-accept fuzzy matches.

**Architecture:** `_import_tabular()` currently does `slugify(account_name)` to derive an account ID. The new path queries existing accounts from `raw.tabular_accounts`, calls `match_account()`, and branches on its outcome (matched / fuzzy candidates / no candidates). The fuzzy threshold (currently hardcoded `0.6`) becomes `TabularConfig.account_match_threshold`. Interactive prompts are gated by `sys.stdin.isatty()` and an explicit `--yes` flag for non-interactive parity (per `.claude/rules/cli.md`).

**Tech Stack:** Python 3.12, DuckDB, Typer, Polars.

**Branch:** `feat/tabular-account-matching`

**Spec:** [docs/specs/tabular-import-cleanup.md](../../specs/tabular-import-cleanup.md) §Stream C

**Depends on:** Stream A (Task 1 — `TabularConfig` lives there). If Stream A has not landed, the `account_match_threshold` config addition in Task 1 here can stand alone — it just adds one more field to the same model.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/moneybin/config.py` | `TabularConfig` Pydantic model | **Modify** — add `account_match_threshold: float = 0.6` |
| `src/moneybin/extractors/tabular/account_matching.py` | `match_account()` and threshold | **Modify** — accept `threshold` kwarg; remove the TODO comment |
| `src/moneybin/services/import_service.py` | `_import_tabular()` account ID assignment | **Modify** — query existing accounts, call `match_account`, branch on outcome |
| `src/moneybin/cli/commands/import_cmd.py` | `import_file` Typer command | **Modify** — add `--yes` / `-y` flag, pipe through |
| `tests/moneybin/test_extractors/test_tabular/test_account_matching.py` | Existing matcher tests | **Modify** — assert configurable threshold |
| `tests/moneybin/test_services/test_tabular_import_service.py` | Existing import-service tests | **Modify** — add 3 tests for the three matching outcomes |
| `tests/moneybin/test_cli/test_import_command.py` (or existing) | CLI tests | **Modify** — assert `--yes` is accepted and forwarded |

The matching logic lives in the existing extractor module. The branching policy (interactive vs auto-accept vs new-account) lives in `_import_tabular` because it depends on CLI state (`auto_accept`, `sys.stdin.isatty()`).

---

## Task 1: Add `account_match_threshold` to `TabularConfig`

**Files:**
- Modify: `src/moneybin/config.py:119-139`
- Test: `tests/moneybin/test_config_profiles.py`

If Stream A landed first, this slots between `balance_tolerance_cents` and the closing of `TabularConfig`. If Stream A did not land, just add the field.

- [ ] **Step 1.1: Write the failing test**

Append to `tests/moneybin/test_config_profiles.py`:

```python
def test_tabular_config_account_match_threshold_default() -> None:
    """TabularConfig exposes the fuzzy account-match threshold with default 0.6."""
    from moneybin.config import TabularConfig

    cfg = TabularConfig()
    assert cfg.account_match_threshold == 0.6


def test_tabular_config_account_match_threshold_override() -> None:
    """Caller can tighten or loosen the fuzzy match threshold."""
    from moneybin.config import TabularConfig

    cfg = TabularConfig(account_match_threshold=0.85)
    assert cfg.account_match_threshold == 0.85
```

- [ ] **Step 1.2: Run the tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_config_profiles.py -v -k account_match_threshold`
Expected: FAIL — `account_match_threshold` does not exist.

- [ ] **Step 1.3: Add the field**

Edit `src/moneybin/config.py`. Inside `TabularConfig`, after `row_refuse_threshold` (or, if Stream A landed, after `balance_tolerance_cents`):

```python
    account_match_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description=(
            "Fuzzy-match similarity threshold (difflib.SequenceMatcher.ratio) "
            "for account-name matching. Below this threshold, candidates are "
            "treated as 'no match'."
        ),
    )
```

- [ ] **Step 1.4: Run the tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_config_profiles.py -v -k account_match_threshold`
Expected: 2 passed.

- [ ] **Step 1.5: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_config_profiles.py
git commit -m "Add account_match_threshold to TabularConfig"
```

---

## Task 2: Make `match_account` accept a configurable threshold

**Files:**
- Modify: `src/moneybin/extractors/tabular/account_matching.py:31-99`
- Test: `tests/moneybin/test_extractors/test_tabular/test_account_matching.py` (existing — read first)

The current implementation hardcodes `0.6` at line 81. After this task, the threshold is a keyword argument with the same default.

- [ ] **Step 2.1: Read the existing test file**

Run: `Read tests/moneybin/test_extractors/test_tabular/test_account_matching.py` (path may differ — Glob if needed). Note the existing test fixtures so the new tests reuse the same `existing_accounts` shape.

- [ ] **Step 2.2: Write the failing tests**

Append to the existing test file (or create one if missing):

```python
def test_match_account_respects_custom_threshold() -> None:
    """A high threshold rejects fuzzy candidates that the default would accept."""
    from moneybin.extractors.tabular.account_matching import match_account

    existing = [{"account_id": "acct1", "account_name": "Chase Checking"}]
    # "Chase Visa" vs "Chase Checking" — SequenceMatcher.ratio ≈ 0.65
    result = match_account(
        "Chase Visa",
        existing_accounts=existing,
        threshold=0.95,
    )
    assert result.matched is False
    assert result.candidates == []  # below threshold → no candidates


def test_match_account_default_threshold_returns_candidates() -> None:
    """The default 0.6 threshold surfaces near-misses as candidates."""
    from moneybin.extractors.tabular.account_matching import match_account

    existing = [{"account_id": "acct1", "account_name": "Chase Checking"}]
    result = match_account("Chase Visa", existing_accounts=existing)
    assert result.matched is False
    assert any(c["account_id"] == "acct1" for c in result.candidates)
```

- [ ] **Step 2.3: Run the tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_extractors/test_tabular/test_account_matching.py -v -k threshold`
Expected: FAIL with `TypeError: match_account() got an unexpected keyword argument 'threshold'`.

- [ ] **Step 2.4: Patch `match_account`**

Edit `src/moneybin/extractors/tabular/account_matching.py`. Remove the TODO comment at lines 31-33 (the work is done by this PR). Update the signature and the fuzzy block:

```python
def match_account(
    account_name: str,
    *,
    account_number: str | None = None,
    explicit_account_id: str | None = None,
    existing_accounts: Sequence[Mapping[str, str | None]] | None = None,
    threshold: float = 0.6,
) -> AccountMatch:
    """Match an account against the existing account registry.

    Args:
        account_name: Account name to match.
        account_number: Account number for strongest match.
        explicit_account_id: Explicit ID (bypasses matching).
        existing_accounts: List of existing account dicts with
            account_id, account_name, and optionally account_number.
        threshold: Minimum SequenceMatcher.ratio for a name to count as a
            fuzzy candidate. Defaults to 0.6.

    Returns:
        AccountMatch with match result and candidates.
    """
```

In the fuzzy section (around line 81), change:

```python
        if ratio >= 0.6:
```

to:

```python
        if ratio >= threshold:
```

Update the docstring above the function — it can also lose the "TODO: wire into import pipeline" comment block (lines 31-33).

- [ ] **Step 2.5: Run the tests**

Run: `uv run pytest tests/moneybin/test_extractors/test_tabular/test_account_matching.py -v`
Expected: all green (existing tests still pass with the default).

- [ ] **Step 2.6: Run pyright**

Run: `uv run pyright src/moneybin/extractors/tabular/account_matching.py`
Expected: 0 errors.

- [ ] **Step 2.7: Commit**

```bash
git add src/moneybin/extractors/tabular/account_matching.py \
        tests/moneybin/test_extractors/test_tabular/test_account_matching.py
git commit -m "Make account_matching threshold configurable"
```

---

## Task 3: Wire `match_account` into `_import_tabular`

**Files:**
- Modify: `src/moneybin/services/import_service.py:239-557` (`_import_tabular`)
- Modify: `src/moneybin/services/import_service.py:560-660` (`import_file` — accept and forward `auto_accept`)
- Test: `tests/moneybin/test_services/test_tabular_import_service.py`

This is the behavior change. The current single-account path (around lines 412-433) does:

```python
    if account_id:
        account_ids: str | list[str] = account_id
        ...
    elif account_name:
        aid = slugify(account_name)
        account_ids = aid
        ...
    elif (mapping_result_is_multi_account and ...):
        ...
    else:
        raise ValueError(...)
```

After this task, the `elif account_name:` branch routes through `match_account()`. The multi-account branch (per-row) and the explicit `account_id` branch are unchanged — both already bypass the matcher correctly.

**Important:** the new query reads from `raw.tabular_accounts`. That table includes `account_id`, `account_name`, `account_number`, `account_number_masked` (per `src/moneybin/sql/schema/raw_tabular_accounts.sql:5-19`). The PRIMARY KEY is `(account_id, source_file)` — meaning the same account can appear under multiple files. Dedup with `SELECT DISTINCT` on `(account_id, account_name, account_number)`.

- [ ] **Step 3.1: Write the three behavior tests**

Append to `tests/moneybin/test_services/test_tabular_import_service.py`. These are unit-shape tests against the helper introduced in step 3.2 (`_resolve_account_via_matcher`), which keeps the test surface narrow.

```python
def test_resolve_account_via_matcher_uses_existing_id_on_match(
    mock_secret_store, tmp_path
) -> None:
    """Matched account name → reuses the existing account_id."""
    from moneybin.database import Database
    from moneybin.services.import_service import _resolve_account_via_matcher

    db = Database(
        tmp_path / "match.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    db.execute("""
        INSERT INTO raw.tabular_accounts
        (account_id, account_name, account_number, account_number_masked,
         account_type, institution_name, currency, source_file, source_type,
         source_origin, import_id)
        VALUES
        ('chase-checking', 'Chase Checking', NULL, NULL, NULL, NULL, NULL,
         'old.csv', 'csv', 'chase', 'imp1')
    """)
    aid = _resolve_account_via_matcher(
        db,
        account_name="Chase Checking",
        account_number=None,
        threshold=0.6,
        auto_accept=False,
    )
    assert aid == "chase-checking"


def test_resolve_account_via_matcher_creates_new_when_no_candidates(
    mock_secret_store, tmp_path
) -> None:
    """No fuzzy candidates → fall back to slugify (creates a new account)."""
    from moneybin.database import Database
    from moneybin.services.import_service import _resolve_account_via_matcher

    db = Database(
        tmp_path / "new.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    aid = _resolve_account_via_matcher(
        db,
        account_name="Brand New Account",
        account_number=None,
        threshold=0.6,
        auto_accept=False,
    )
    assert aid == "brand-new-account"


def test_resolve_account_via_matcher_auto_accepts_top_candidate(
    mock_secret_store, tmp_path, caplog
) -> None:
    """With auto_accept=True, a fuzzy candidate is taken without prompting."""
    from moneybin.database import Database
    from moneybin.services.import_service import _resolve_account_via_matcher

    db = Database(
        tmp_path / "fuzzy.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    db.execute("""
        INSERT INTO raw.tabular_accounts
        (account_id, account_name, account_number, account_number_masked,
         account_type, institution_name, currency, source_file, source_type,
         source_origin, import_id)
        VALUES
        ('chase-checking', 'Chase Checking Acct', NULL, NULL, NULL, NULL, NULL,
         'old.csv', 'csv', 'chase', 'imp1')
    """)
    with caplog.at_level("INFO"):
        aid = _resolve_account_via_matcher(
            db,
            account_name="Chase Checking",
            account_number=None,
            threshold=0.6,
            auto_accept=True,
        )
    assert aid == "chase-checking"
    assert "auto-accepting" in caplog.text.lower()


def test_resolve_account_via_matcher_warns_and_falls_back_when_not_auto(
    mock_secret_store, tmp_path, caplog
) -> None:
    """Without auto_accept, fuzzy candidates trigger a warning + slugify fallback."""
    from moneybin.database import Database
    from moneybin.services.import_service import _resolve_account_via_matcher

    db = Database(
        tmp_path / "fuzzy.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    db.execute("""
        INSERT INTO raw.tabular_accounts
        (account_id, account_name, account_number, account_number_masked,
         account_type, institution_name, currency, source_file, source_type,
         source_origin, import_id)
        VALUES
        ('chase-checking', 'Chase Checking Acct', NULL, NULL, NULL, NULL, NULL,
         'old.csv', 'csv', 'chase', 'imp1')
    """)
    with caplog.at_level("WARNING"):
        aid = _resolve_account_via_matcher(
            db,
            account_name="Chase Checking",
            account_number=None,
            threshold=0.6,
            auto_accept=False,
        )
    assert aid == "chase-checking"  # slugify("Chase Checking") happens to match
    assert "fuzzy" in caplog.text.lower() or "candidate" in caplog.text.lower()
```

The fourth test exists to prove the *non-auto-accept* path still completes (warning + slug fallback) — important because it's the default path and must not block import.

- [ ] **Step 3.2: Run the tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_tabular_import_service.py -v -k resolve_account_via_matcher`
Expected: FAIL with `ImportError: cannot import name '_resolve_account_via_matcher'`.

- [ ] **Step 3.3: Add the helper to `import_service.py`**

Edit `src/moneybin/services/import_service.py`. After the existing helpers (after `_detect_file_type` around line 142), add:

```python
def _resolve_account_via_matcher(
    db: Database,
    *,
    account_name: str,
    account_number: str | None,
    threshold: float,
    auto_accept: bool,
) -> str:
    """Resolve an account name to an account_id using match_account().

    Three outcomes:
      1. Matched (number or exact slug) → return the existing account_id.
      2. Fuzzy candidates and auto_accept=True → take the top candidate, log it.
      3. Fuzzy candidates and auto_accept=False → log a warning listing
         candidates, fall back to slugify(account_name).
      4. No candidates → fall back to slugify(account_name) (creates new).

    Args:
        db: Database instance.
        account_name: Account name from CLI/file.
        account_number: Account number if available, for strongest match.
        threshold: Minimum SequenceMatcher.ratio for a fuzzy candidate.
        auto_accept: True if the user passed --yes (or stdin is non-interactive).

    Returns:
        The resolved account_id (existing or freshly slugified).
    """
    from moneybin.extractors.tabular.account_matching import match_account
    from moneybin.utils import slugify

    try:
        rows = db.execute(
            """
            SELECT DISTINCT account_id, account_name, account_number
            FROM raw.tabular_accounts
            """
        ).fetchall()
    except Exception:  # noqa: BLE001 — table missing on first import; fall back cleanly
        logger.debug("raw.tabular_accounts unavailable; skipping account match")
        return slugify(account_name)

    existing = [
        {"account_id": r[0], "account_name": r[1], "account_number": r[2]}
        for r in rows
    ]

    result = match_account(
        account_name,
        account_number=account_number,
        existing_accounts=existing,
        threshold=threshold,
    )

    if result.matched and result.account_id:
        logger.info(
            f"Matched account {account_name!r} → existing id {result.account_id!r}"
        )
        return result.account_id

    if result.candidates:
        if auto_accept:
            top = result.candidates[0]
            logger.info(
                f"⚙️  Auto-accepting fuzzy match for {account_name!r}: "
                f"{top['account_name']!r} → {top['account_id']!r}"
            )
            return top["account_id"]
        logger.warning(
            f"⚠️  Account {account_name!r} did not match exactly. Fuzzy candidates: "
            + ", ".join(
                f"{c['account_name']!r} ({c['account_id']})"
                for c in result.candidates
            )
            + ". Use --yes to auto-accept the top candidate, "
            "or --account-id to pick explicitly."
        )

    return slugify(account_name)
```

- [ ] **Step 3.4: Wire the helper into `_import_tabular`**

Still in `src/moneybin/services/import_service.py`. Update the `_import_tabular` signature to accept `auto_accept` (around line 256):

```python
def _import_tabular(
    db: Database,
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
```

In the account-resolution block (currently around lines 412-433), change the `elif account_name:` branch from:

```python
    elif account_name:
        aid = slugify(account_name)
        account_ids = aid
        acct_id_to_name[aid] = account_name
```

to:

```python
    elif account_name:
        from moneybin.config import get_settings

        threshold = get_settings().data.tabular.account_match_threshold
        aid = _resolve_account_via_matcher(
            db,
            account_name=account_name,
            account_number=None,
            threshold=threshold,
            auto_accept=auto_accept,
        )
        account_ids = aid
        acct_id_to_name[aid] = account_name
```

(`account_number=None` for now — when Stream-D-style account-number extraction lands, the matcher already accepts it and will tighten the match.)

Update `import_file` (around line 560) to accept and forward `auto_accept`. Add the parameter at the end of the keyword-args:

```python
def import_file(
    db: Database,
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
```

In the `_import_tabular(...)` call inside `import_file` (around lines 627-644), pass `auto_accept=auto_accept`:

```python
        result = _import_tabular(
            db,
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
```

Update the `import_file` docstring's Args section to mention `auto_accept`:

```python
        auto_accept: Auto-accept the top fuzzy account match without prompting
            (CLI: --yes / -y). Defaults to False.
```

- [ ] **Step 3.5: Run the new tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_tabular_import_service.py -v -k resolve_account_via_matcher`
Expected: 4 passed.

- [ ] **Step 3.6: Run the broader tabular suites**

Run: `uv run pytest tests/moneybin/test_services/ tests/moneybin/test_extractors/test_tabular/ -v`
Expected: all green.

- [ ] **Step 3.7: Run pyright**

Run: `uv run pyright src/moneybin/services/import_service.py`
Expected: 0 errors.

- [ ] **Step 3.8: Commit**

```bash
git add src/moneybin/services/import_service.py \
        tests/moneybin/test_services/test_tabular_import_service.py
git commit -m "Wire match_account() into tabular import pipeline"
```

---

## Task 4: Add `--yes` / `-y` flag to `import file` CLI

**Files:**
- Modify: `src/moneybin/cli/commands/import_cmd.py:76-234` (`import_file`)
- Test: `tests/moneybin/test_cli/test_import_command.py` (or wherever the import-CLI tests live — Glob first)

Per `.claude/rules/cli.md`, every interactive choice must have a single-flag equivalent. `--yes` / `-y` is the standard auto-accept flag.

- [ ] **Step 4.1: Find the existing CLI test file**

Run: `Glob tests/**/test_import*.py` and `Glob tests/moneybin/test_cli/*.py`. Identify the test that exercises `import_file` via the Typer `CliRunner`.

- [ ] **Step 4.2: Write a failing test**

Append (to whichever file holds the import CLI tests):

```python
def test_import_file_passes_yes_flag_through(monkeypatch, tmp_path) -> None:
    """--yes is parsed and forwarded as auto_accept=True to import_file()."""
    from typer.testing import CliRunner

    from moneybin.cli.commands.import_cmd import app

    captured: dict[str, object] = {}

    def fake_import_file(*, db, file_path, **kwargs):
        captured.update(kwargs)
        from moneybin.services.import_service import ImportResult
        return ImportResult(file_path=str(file_path), file_type="tabular")

    monkeypatch.setattr(
        "moneybin.services.import_service.import_file", fake_import_file
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.import_cmd.get_database",
        lambda: object(),
        raising=False,
    )

    csv = tmp_path / "x.csv"
    csv.write_text("Date,Amount,Description\n2025-01-01,1.00,X\n")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["file", str(csv), "--account-name", "Test", "--yes"],
    )
    assert result.exit_code == 0, result.output
    assert captured.get("auto_accept") is True
```

If the existing tests use a different mocking pattern (e.g., patching `run_import` aliased inside `import_cmd.py`), adapt the monkeypatch target. The check that matters is `captured["auto_accept"] is True` after `--yes`.

- [ ] **Step 4.3: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_cli/test_import_command.py -v -k yes_flag`
Expected: FAIL — either CLI rejects `--yes` (no such option) or `captured["auto_accept"]` is missing.

- [ ] **Step 4.4: Add the flag**

Edit `src/moneybin/cli/commands/import_cmd.py`. In the `import_file` function signature (after line 152, before the closing `)`):

```python
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Auto-accept the top fuzzy account match without prompting",
    ),
```

In the call to `run_import(...)` (around line 199), append:

```python
            auto_accept=yes,
```

- [ ] **Step 4.5: Run the test to verify it passes**

Run: `uv run pytest tests/moneybin/test_cli/test_import_command.py -v -k yes_flag`
Expected: PASS.

- [ ] **Step 4.6: Run the full CLI test suite**

Run: `uv run pytest tests/moneybin/test_cli/ -v -k import`
Expected: all green.

- [ ] **Step 4.7: Run E2E import help and workflow**

Run: `uv run pytest tests/e2e/test_e2e_help.py tests/e2e/test_e2e_workflows.py -v -k import`
Expected: all green. (`test_e2e_help.py` calls `--help` for every command — the new `--yes` flag will appear there automatically.)

- [ ] **Step 4.8: Run pyright**

Run: `uv run pyright src/moneybin/cli/commands/import_cmd.py`
Expected: 0 errors.

- [ ] **Step 4.9: Commit**

```bash
git add src/moneybin/cli/commands/import_cmd.py \
        tests/moneybin/test_cli/test_import_command.py
git commit -m "Add --yes/-y flag to import file for auto-accepting fuzzy matches"
```

---

## Task 5: Update spec status and roadmap

Per `.claude/rules/shipping.md`, when a feature ships:

- [ ] **Step 5.1: Update spec status**

Edit `docs/specs/tabular-import-cleanup.md`. Change the `**Status**: draft` line at the top to `**Status**: implemented` (only after all Stream A/B1/B2/C PRs land — for this PR alone, leave at `draft` and update only the §Stream C heading status if a sub-status convention exists in the file).

Edit `docs/specs/INDEX.md`. Bump the `tabular-import-cleanup` row's status accordingly.

- [ ] **Step 5.2: Update README roadmap**

Read `README.md`. If a roadmap row mentions "account matching" with a 📐 or 🗓️ icon, change to ✅. If the "What Works Today" section mentions tabular import, add a sentence: "Re-importing the same account under a different name now matches against the existing account ID (with `--yes` to auto-accept fuzzy matches non-interactively)."

- [ ] **Step 5.3: Commit docs**

```bash
git add docs/specs/tabular-import-cleanup.md docs/specs/INDEX.md README.md
git commit -m "Mark account matching shipped; update README"
```

---

## Task 6: Pre-push quality pass

- [ ] **Step 6.1: Run `/simplify` on changed code**

Per `.claude/rules/shipping.md`, run a simplification pass before the final commit. Focus areas:
- Is `_resolve_account_via_matcher` doing more than one thing? (DB read + matcher call + branching + logging — this is intentional; one helper, one caller.)
- Could the `slugify` fallback in 3 of the 4 paths factor out? (No — each fallback path has a different log message; collapsing would lose user feedback.)

Make any small simplifications and commit them as a separate "Simplify" commit if non-trivial.

- [ ] **Step 6.2: Run `make check test`**

Run: `make check test`
Expected: green.

- [ ] **Step 6.3: Run all E2E**

Run: `uv run pytest tests/e2e/ -v -m "e2e and not slow"`
Expected: green.

- [ ] **Step 6.4: Manual smoke test**

```bash
# 1. First import — creates account
uv run moneybin import file <fixture.csv> --account-name "Chase Checking"

# 2. Re-import under a slightly different name — should match
uv run moneybin import file <fixture2.csv> --account-name "Chase Checking Acct" --yes

# 3. Verify both imports landed under the same account_id
uv run moneybin data sql "SELECT account_id, account_name FROM raw.tabular_accounts ORDER BY account_id"
```

Expected: both imports use `account_id = 'chase-checking'`. Without `--yes`, step 2 would warn and create a new `chase-checking-acct` account.

- [ ] **Step 6.5: Push and open PR**

```bash
git push -u origin feat/tabular-account-matching
gh pr create --title "Wire account matching into tabular import pipeline" --body "$(cat <<'EOF'
## Summary
- `match_account()` is now called from `_import_tabular()` for single-account files
- Three outcomes handled: exact match (reuse id), fuzzy candidates (auto-accept with `--yes` or warn + fallback), no candidates (slugify a new id)
- Fuzzy threshold moved to `TabularConfig.account_match_threshold` (default 0.6)
- New CLI flag: `--yes` / `-y` for non-interactive auto-accept (per cli.md non-interactive parity rule)

User-visible behavior change: re-importing the same account under varying names no longer creates duplicate account records. Spec: docs/specs/tabular-import-cleanup.md (Stream C).

## Test plan
- [x] `make check test`
- [x] 4 new unit tests on `_resolve_account_via_matcher` (matched / no-match / auto-accept / warn-and-fallback)
- [x] CLI test asserts `--yes` propagates as `auto_accept=True`
- [x] E2E `test_e2e_help.py` picks up the new flag automatically
- [x] Manual smoke: re-import under varying name reuses account_id
EOF
)"
```
