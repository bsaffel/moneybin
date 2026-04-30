# FastMCP 3.x Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade MoneyBin from the FastMCP v1 snapshot bundled in `mcp[cli]` to the standalone `fastmcp` 3.x package, replacing hand-rolled error handling and the custom `NamespaceRegistry` with built-in primitives, while extracting cross-transport types out of `mcp/` to prepare for a future HTTP/FastAPI surface.

**Architecture:** Single PR sequenced in six phases. Foundation first (relocate `ResponseEnvelope` out of `mcp/`), then SDK swap, then layered migrations (error handler → service-layer adapter extraction → visibility-based progressive disclosure → test fixtures → docs). Each phase ends with a passing test suite. Implementation is server-side only; CLI command code is untouched except where it imports the relocated envelope.

**Tech Stack:** Python 3.12, `fastmcp` 3.1.x (replacing `mcp[cli]>=1.9.0`), DuckDB via `Database` singleton, pytest, Typer. Tooling per AGENTS.md: `uv` for installs, `make format && make lint`, `uv run pyright`, `uv run pytest`.

**ADR:** [`docs/decisions/008-fastmcp-3x-sdk.md`](../../decisions/008-fastmcp-3x-sdk.md).

**Branch:** `refactor/migrate-fastmcp-3x` (already created in worktree `.worktrees/migrate-fastmcp-3x`).

---

## Pre-flight (one-time, before Task 1)

- [ ] **Sync the worktree's environment.**

Run:
```bash
cd /Users/bsaffel/Workspace/moneybin/.worktrees/migrate-fastmcp-3x
uv sync
```

Expected: virtualenv populated; `uv run python -c "import mcp.server.fastmcp; print('ok')"` succeeds.

- [ ] **Capture baseline test results.**

Run: `uv run pytest -x --ignore=tests/e2e -q`
Expected: PASS (record total count for later comparison).
Then: `uv run pytest tests/e2e -q`
Expected: PASS or document any pre-existing skips/xfails.

If anything fails before changes start, stop and investigate — don't proceed into the migration on a broken baseline.

---

## Phase 1: Foundation

### Task 1: Spike — verify `fastmcp` 3.x API surface

**Files:**
- Read: official `fastmcp` 3.x docs at https://gofastmcp.com and https://github.com/PrefectHQ/fastmcp
- Create: `docs/superpowers/plans/2026-04-29-fastmcp-3x-spike-notes.md` (working notes, will be deleted at end of migration)

This task answers questions the rest of the plan depends on. Do not skip — later tasks reference these answers.

- [ ] **Step 1: Install `fastmcp` 3.x in the worktree's venv as a side-by-side dependency.**

```bash
uv add 'fastmcp>=3.1,<4'
uv sync
```

Verify both packages co-exist: `uv run python -c "import fastmcp; import mcp.server.fastmcp; print(fastmcp.__version__)"` should print a 3.x version.

- [ ] **Step 2: Verify the `FastMCP` constructor signature.**

Compare today's `from mcp.server.fastmcp import FastMCP` constructor (used at `src/moneybin/mcp/server.py:30`) against `from fastmcp import FastMCP`. Confirm or document differences in:
- Constructor positional/keyword arguments (does it still accept `(name, instructions=...)`?)
- Lifespan/startup hooks
- `mcp.run(transport="stdio")` invocation shape
- `mcp.tool(name=..., description=...)` decorator shape

Write findings to the spike notes file under a heading "FastMCP constructor".

- [ ] **Step 3: Verify the error-handling primitive name and shape.**

Locate `@handle_tool_errors` (or its actual name in 3.x) in fastmcp source/docs. Confirm:
- Exact import path
- Whether it's a decorator applied per-tool, or a server-level configuration
- How `mask_error_details=True` is enabled (constructor arg? per-tool override?)
- What exception types/shapes it handles vs lets propagate

Write findings under "Error handling".

- [ ] **Step 4: Verify the visibility / progressive-disclosure primitive.**

Locate the visibility system. Confirm:
- Exact name of the decorator field that marks a tool as initially hidden (might be `enabled=False`, `initially_hidden=True`, `hidden=True`, etc.)
- Exact signature of `enable_components` (does it take `*names: str`, `names: list[str]`, glob patterns, component objects?)
- How to access `Context` inside a tool handler in 3.x — is it `ctx: Context` parameter, `from fastmcp import Context`, or accessed via decorator?
- **Critical security check:** confirm hidden tools return an error from `tools/call`, not just absent from `tools/list`. If a client knows a tool name and calls it directly while hidden, it must be rejected.

Write findings under "Visibility system". Flag the security check finding prominently — if hidden ≠ uncallable, the plan needs an additional gate.

- [ ] **Step 5: Verify Pydantic / structured-output handling.**

Today, `mcp_tool` decorator serializes `ResponseEnvelope` to JSON via `result.to_json()` (`src/moneybin/mcp/decorator.py:50`). Determine whether 3.x:
- Accepts a string return value as a tool's content (current pattern), OR
- Requires returning a Pydantic model / dict that 3.x serializes itself, OR
- Both work but one is preferred.

Write findings under "Structured outputs".

- [ ] **Step 6: Commit the spike notes.**

```bash
cd /Users/bsaffel/Workspace/moneybin/.worktrees/migrate-fastmcp-3x
git add docs/superpowers/plans/2026-04-29-fastmcp-3x-spike-notes.md pyproject.toml uv.lock
git commit -m "Add fastmcp 3.x as dependency and capture API spike notes"
```

The `mcp[cli]` dependency stays for now — we'll remove it in Task 4.

---

### Task 2: Create top-level `protocol/` module and relocate `ResponseEnvelope`

**Files:**
- Create: `src/moneybin/protocol/__init__.py`
- Create: `src/moneybin/protocol/envelope.py` (moved from `src/moneybin/mcp/envelope.py`)
- Modify: every file importing from `moneybin.mcp.envelope` (search the codebase)
- Delete: `src/moneybin/mcp/envelope.py`

The relocation makes explicit that the envelope is the cross-transport response shape, not an MCP-internal type.

- [ ] **Step 1: Find all current importers.**

Run:
```bash
cd /Users/bsaffel/Workspace/moneybin/.worktrees/migrate-fastmcp-3x
grep -rn "moneybin.mcp.envelope" src tests --include="*.py" | sort -u
```

Capture the file list. Expect ~20–30 files: most MCP tool modules, the auto-rule service, decorator, error_handler, CLI commands using `--output json`, tests.

- [ ] **Step 2: Create the new module with empty `__init__`.**

```python
# src/moneybin/protocol/__init__.py
"""Cross-transport protocol types shared across MCP, CLI, and future HTTP."""
```

- [ ] **Step 3: Move the file.**

```bash
git mv src/moneybin/mcp/envelope.py src/moneybin/protocol/envelope.py
```

- [ ] **Step 4: Update the docstring at the top of the moved file.**

Open `src/moneybin/protocol/envelope.py` and replace the module docstring with one that does not reference MCP exclusively:

```python
"""Cross-transport response envelope.

Every MCP tool and every CLI command with ``--output json`` returns this
shape: ``{summary, data, actions}``. A future HTTP/FastAPI surface will
use the same envelope. The shape gives consumers consistent metadata
(counts, truncation, sensitivity, currency) and contextual next-step hints.

See ``mcp-architecture.md`` section 4 for design rationale.
"""
```

- [ ] **Step 5: Update all importers.**

Replace every occurrence of `from moneybin.mcp.envelope import` with `from moneybin.protocol.envelope import`:

```bash
grep -rl "moneybin.mcp.envelope" src tests --include="*.py" | xargs sed -i '' 's|moneybin\.mcp\.envelope|moneybin.protocol.envelope|g'
```

(macOS `sed` syntax. On Linux use `sed -i 's|...|...|g'`.)

- [ ] **Step 6: Verify the module import works and pyright is clean on changed files.**

```bash
uv run python -c "from moneybin.protocol.envelope import ResponseEnvelope, build_envelope, build_error_envelope; print('ok')"
uv run pyright src/moneybin/protocol src/moneybin/mcp src/moneybin/services src/moneybin/cli
```

Expected: import works; pyright reports no errors on changed paths.

- [ ] **Step 7: Run the full test suite.**

```bash
uv run pytest -x -q
```

Expected: PASS at the same count as the baseline. The relocation is pure rename — no behavior change.

- [ ] **Step 8: Commit.**

```bash
git add -A
git commit -m "Relocate ResponseEnvelope to moneybin.protocol

ResponseEnvelope is the shared response shape across MCP, CLI --output
json, and the future HTTP surface. Moving it out of moneybin.mcp makes
the cross-transport contract explicit and prevents weird coupling when
non-MCP transports need it.

Pure rename; no behavior change."
```

---

## Phase 2: SDK Swap

### Task 3: Replace `FastMCP` import and validate server boots

**Files:**
- Modify: `src/moneybin/mcp/server.py:19` (the import and constructor)
- Modify: `pyproject.toml` (remove `mcp[cli]>=1.9.0`, keep `fastmcp>=3.1,<4`)
- Modify: any other file importing from `mcp.server.fastmcp`

This task gets the server starting on 3.x. Tool decorators and error handlers may still be in old shapes — those are addressed in later tasks.

- [ ] **Step 1: Find all `mcp.server.fastmcp` importers.**

```bash
grep -rn "mcp\.server\.fastmcp" src tests --include="*.py"
```

Expected: probably just `src/moneybin/mcp/server.py:19`. Note any other hits.

- [ ] **Step 2: Replace the import.**

In `src/moneybin/mcp/server.py:19`:

```python
# OLD
from mcp.server.fastmcp import FastMCP

# NEW
from fastmcp import FastMCP
```

- [ ] **Step 3: Enable `mask_error_details=True` on the constructor.**

Per spike notes, the constructor accepts `(name, instructions=..., mask_error_details=...)` unchanged from today. Add the masking flag:

```python
mcp = FastMCP(
    "MoneyBin",
    instructions=(...),
    mask_error_details=True,  # Per ADR-008: masks unclassified exceptions to a generic
                              # ToolError. Domain exceptions are caught by the mcp_tool
                              # decorator (Task 4) and converted to error envelopes
                              # before they reach the server boundary.
)
```

- [ ] **Step 4: Remove the bundled SDK from `pyproject.toml`.**

Open `pyproject.toml`, find the `mcp[cli]>=1.9.0` line in dependencies, delete it. The `fastmcp` line added in Task 1 stays. Run:

```bash
uv sync
```

Expected: removal of `mcp` happens transitively unless `fastmcp` pulls it. That's fine — we're just removing the direct import surface.

- [ ] **Step 5: Verify the server module imports without error.**

```bash
uv run python -c "from moneybin.mcp.server import mcp; print(type(mcp).__module__, type(mcp).__name__)"
```

Expected: prints `fastmcp.<...>` `FastMCP` (or whatever the 3.x module path is per spike notes).

If this raises an `AttributeError`, `TypeError`, or import error, consult spike notes — likely a constructor arg or lifespan hook needs adjustment. Fix and re-run.

- [ ] **Step 6: Run the unit test suite (skipping e2e for now).**

```bash
uv run pytest -x --ignore=tests/e2e -q
```

Expected: PASS. Unit tests don't boot the server — they exercise services and tool functions directly. If anything fails, the failure points to a stale import or transitive type mismatch.

- [ ] **Step 7: Run the e2e MCP test once to surface server-boot issues.**

```bash
uv run pytest tests/e2e/test_e2e_mcp.py -x -v
```

Expected behavior is uncertain — this is the first e2e run on 3.x. Likely outcomes:
- PASS: SDK swap is clean. Move on.
- FAIL on tool registration / call: tool decorator shape changed. Note the failure but DO NOT fix yet — the next phases address tool decorators systematically. Mark the failing test with `pytest.mark.xfail(strict=False, reason="Pending Phase 3-5 migration")` and move on.
- FAIL on server boot: lifespan or transport API differs. This must be fixed before proceeding. Consult spike notes and adjust.

If e2e tests are xfailed here, track them in a "fix in Phase X" list at the bottom of the spike notes. They get unmarked in the appropriate later task.

- [ ] **Step 8: Commit.**

```bash
git add -A
git commit -m "Swap FastMCP import from bundled mcp SDK to standalone fastmcp 3.x

Replaces 'from mcp.server.fastmcp import FastMCP' with 'from fastmcp
import FastMCP'. Enables mask_error_details=True at server construction
to prevent SQL fragments and file paths leaking through unhandled
exceptions.

Drops mcp[cli] direct dependency; fastmcp pulls mcp transitively for
protocol primitives. Tool decorators and error handler still on legacy
shapes — addressed in subsequent commits."
```

---

## Phase 3: Error Handling

### Task 4: Fold envelope-on-error into `mcp_tool`; delete `handle_mcp_errors`

**Files:**
- Delete: `src/moneybin/mcp/error_handler.py`
- Modify: `src/moneybin/mcp/decorator.py` (fold envelope-on-error into `mcp_tool`; switch return shape from `to_json()` string to `ResponseEnvelope` directly per spike notes "Structured outputs")
- Modify: every MCP tool that imports `handle_mcp_errors` from `error_handler` (just remove the import + decorator line)
- Modify: every MCP tool with hand-rolled `except duckdb.CatalogException` / `except DatabaseKeyError` blocks (delete redundant catches per Step 5)
- Test: `tests/mcp/test_error_handling.py` (new), `tests/mcp/test_decorator.py` (extend)

**Why this differs from earlier draft:** the spike (Task 1) confirmed fastmcp 3.x has **no `@handle_tool_errors` decorator**. Masking is a server-level constructor arg (`mask_error_details=True`, set in Task 3) and only affects *uncaught* exceptions — it does not convert exceptions to `ResponseEnvelope` shape. We still need our own catch-and-build-envelope logic to keep MCP, CLI `--output json`, and the future HTTP surface returning the same envelope shape on errors. Folding it into `mcp_tool` (always paired with the old decorator anyway) collapses two decorators into one and removes a layer of indirection.

- [ ] **Step 1: Audit current `handle_mcp_errors` usage and tool-level error catches.**

```bash
grep -rn "handle_mcp_errors\|from moneybin.mcp.error_handler" src tests --include="*.py"
grep -rn "except duckdb\|except DatabaseKeyError\|except FileNotFoundError\|except Exception" src/moneybin/mcp/tools --include="*.py"
```

Capture both lists. The first is for direct migration; the second is the audit-and-cleanup pass in Step 5.

- [ ] **Step 2: Write failing tests for the new `mcp_tool` error behavior.**

Create `tests/mcp/__init__.py` (empty) if missing, then `tests/mcp/test_error_handling.py`:

```python
"""Verify mcp_tool decorator converts domain exceptions to error envelopes."""
from __future__ import annotations

import pytest

from moneybin.errors import DatabaseKeyError, UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope


def test_mcp_tool_converts_user_error_to_envelope() -> None:
    """A UserError raised inside a tool becomes an error envelope."""

    @mcp_tool(sensitivity="low")
    def failing_tool() -> ResponseEnvelope:
        raise UserError("not found", code="NOT_FOUND")

    result = failing_tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "NOT_FOUND"
    assert result.data == []


def test_mcp_tool_converts_database_key_error_to_envelope() -> None:
    """DatabaseKeyError is a recognised classified exception."""

    @mcp_tool(sensitivity="low")
    def failing_tool() -> ResponseEnvelope:
        raise DatabaseKeyError("missing key")

    result = failing_tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "DATABASE_KEY_ERROR"


def test_mcp_tool_lets_unclassified_exceptions_propagate() -> None:
    """Non-domain exceptions propagate so fastmcp's mask_error_details can
    wrap them into masked ToolErrors. The decorator does NOT swallow them."""

    @mcp_tool(sensitivity="low")
    def failing_tool() -> ResponseEnvelope:
        raise RuntimeError("internal detail leak")

    with pytest.raises(RuntimeError):
        failing_tool()


def test_mcp_tool_returns_response_envelope_directly() -> None:
    """Per fastmcp 3.x structured outputs, return the Pydantic model — not
    its JSON string. fastmcp serializes it to both content and
    structured_content."""
    from moneybin.protocol.envelope import build_envelope

    @mcp_tool(sensitivity="low")
    def ok_tool() -> ResponseEnvelope:
        return build_envelope(data=[{"x": 1}], sensitivity="low")

    result = ok_tool()
    assert isinstance(result, ResponseEnvelope)  # NOT a str
    assert result.data == [{"x": 1}]
```

- [ ] **Step 3: Run the tests, see them fail.**

```bash
uv run pytest tests/mcp/test_error_handling.py -v
```

Expected: FAIL (decorator doesn't catch domain exceptions yet, and currently returns a JSON string from `to_json()` not a `ResponseEnvelope`).

- [ ] **Step 4: Update `mcp_tool` to (a) catch classified exceptions and build error envelopes, (b) return `ResponseEnvelope` directly.**

Open `src/moneybin/mcp/decorator.py`. The decorator's wrapper currently logs the call, runs the tool, and converts `ResponseEnvelope` to a JSON string via `to_json()`. Change the wrapper body to:

```python
def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
        log_tool_call(fn.__name__, tier)
        try:
            result = fn(*args, **kwargs)
        except UserError as exc:
            return build_error_envelope(
                code=exc.code, message=str(exc), sensitivity=sensitivity,
            )
        except DatabaseKeyError as exc:
            return build_error_envelope(
                code="DATABASE_KEY_ERROR",
                message=str(exc),
                sensitivity=sensitivity,
            )
        except FileNotFoundError as exc:
            return build_error_envelope(
                code="FILE_NOT_FOUND",
                message=str(exc),
                sensitivity=sensitivity,
            )
        # Unclassified exceptions propagate; fastmcp's mask_error_details
        # wraps them into masked ToolErrors at the server boundary.
        if not isinstance(result, ResponseEnvelope):
            raise TypeError(
                f"{fn.__name__} returned {type(result).__name__}, expected ResponseEnvelope"
            )
        return result

    wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
    return wrapper

return decorator
```

Imports needed at the top:
```python
from moneybin.errors import DatabaseKeyError, UserError
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_error_envelope,
)
```

Verify exact `UserError` shape (does it carry `.code`?) by reading `src/moneybin/errors.py`. If `UserError` doesn't expose a `.code`, fall back to a sensible default (e.g. `"USER_ERROR"`).

The change from `result.to_json()` → `return result` is what enables fastmcp 3.x to populate `structured_content` from the Pydantic model (per spike notes "Structured outputs").

- [ ] **Step 5: Sweep tools for redundant per-call catches.**

For each file from Step 1's second grep:

1. Catches of `DatabaseKeyError`, `FileNotFoundError`, `UserError` that build a custom error envelope: **delete the catch.** The decorator now handles them.
2. Catches of `duckdb.CatalogException` (missing table) or `duckdb.ConstraintException` (unique violation) that produce a tool-specific UX message: **convert to `raise UserError(message, code="...")`** so the decorator builds a standard error envelope.
3. Remove `# noqa: BLE001` suppressions that exist only to silence a bare `except Exception:` for envelope wrapping. If any genuine reason remains (e.g. swallowing a non-error path), document it.

Audit-and-edit pass — look at each tool individually.

- [ ] **Step 6: Delete the obsolete error handler module and its imports.**

```bash
rm src/moneybin/mcp/error_handler.py
grep -rn "moneybin.mcp.error_handler\|handle_mcp_errors" src tests --include="*.py"
```

If the second command returns hits, remove those imports/decorator usages. Expected post-cleanup: zero hits.

- [ ] **Step 7: Run the new tests plus the full suite.**

```bash
uv run pytest tests/mcp/test_error_handling.py -v
uv run pytest -x -q
```

Expected: new tests PASS; full suite PASS modulo any e2e tests xfailed in Task 3 Step 7. If those now pass, unmark them.

- [ ] **Step 8: Audit tests for `mask_error_details=True` impact.**

```bash
grep -rn "duckdb.*Catalog\|str(exc)\|in str(.*error\|.lower().*'duckdb\|assert.*Exception" tests --include="*.py"
```

Any test asserting on raw exception text reaching the client now sees `"Error calling tool 'X'"` (masked). Update assertions accordingly. Tests asserting on the **envelope** error shape (post-decorator) are unaffected — they go through the new classification path.

- [ ] **Step 9: Commit.**

```bash
git add -A
git commit -m "Fold envelope-on-error into mcp_tool decorator; delete handle_mcp_errors

fastmcp 3.x has no @handle_tool_errors decorator — masking is a
server-level constructor arg (set in the previous commit) that only
affects uncaught exceptions. To keep MCP, CLI --output json, and the
future HTTP surface returning the same envelope shape on errors, the
mcp_tool decorator now catches our domain exceptions (UserError,
DatabaseKeyError, FileNotFoundError) and builds an error envelope.
Other exceptions propagate so fastmcp masks them.

Also switches the decorator's return path from result.to_json() (a JSON
string) to the ResponseEnvelope itself; fastmcp 3.x serializes the
Pydantic model into both content and structured_content.

- Delete src/moneybin/mcp/error_handler.py.
- Sweep tool modules for redundant DatabaseKeyError / FileNotFoundError
  catches; convert tool-specific catches to raise UserError.
- Update tests asserting on raw exception strings to expect the masked
  form produced by mask_error_details=True."
```

---

## Phase 4: Adapter Extraction

### Task 5: Move `to_envelope()` off auto-rule service dataclasses

**Files:**
- Modify: `src/moneybin/services/auto_rule_service.py:60-119` (remove `to_envelope` from `AutoReviewResult`, `AutoConfirmResult`, `AutoStatsResult`)
- Create: `src/moneybin/mcp/adapters/categorize_adapters.py` (new module containing the moved adapters)
- Modify: `src/moneybin/mcp/tools/categorize.py` (use the adapters)
- Modify: `src/moneybin/cli/commands/categorize.py` (use the same adapters for `--output json`)
- Test: `tests/services/test_auto_rule_service.py` and `tests/mcp/test_categorize_adapters.py`

This closes the dependency-direction violation flagged in PR #60 review: services should not import from `moneybin.mcp.envelope` (now `moneybin.protocol.envelope`, but the principle stands — services should not need to know about envelope wrapping at all).

- [ ] **Step 1: Write a failing test asserting `AutoReviewResult` has no `to_envelope` method.**

Open `tests/services/test_auto_rule_service.py` (create if missing). Add:

```python
def test_auto_review_result_is_pure_data_carrier() -> None:
    """AutoReviewResult must not depend on transport-layer types.

    The result dataclass holds business data; envelope construction
    happens in the MCP/CLI adapter layer, not on the service.
    """
    from moneybin.services.auto_rule_service import AutoReviewResult

    assert not hasattr(AutoReviewResult, "to_envelope"), (
        "to_envelope must live in mcp/adapters/, not on the service dataclass"
    )
```

- [ ] **Step 2: Run the test, see it fail.**

```bash
uv run pytest tests/services/test_auto_rule_service.py::test_auto_review_result_is_pure_data_carrier -v
```

Expected: FAIL with `assert not True` because `to_envelope` is currently a method.

- [ ] **Step 3: Create the adapter module.**

```python
# src/moneybin/mcp/adapters/__init__.py
"""MCP-side adapters that convert service-layer dataclasses into envelopes."""
```

```python
# src/moneybin/mcp/adapters/categorize_adapters.py
"""Adapters for CategorizationService and AutoRuleService results.

Service-layer dataclasses (AutoReviewResult, etc.) are pure data carriers.
Wrapping them in a ResponseEnvelope happens here, at the transport boundary.
The CLI uses the same adapters so MCP and `--output json` produce the same
shape.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from moneybin.services.auto_rule_service import (
        AutoConfirmResult,
        AutoReviewResult,
        AutoStatsResult,
    )


def auto_review_envelope(result: "AutoReviewResult") -> ResponseEnvelope:
    """Build the envelope for the categorize.auto_review tool."""
    return build_envelope(
        # Move the body of AutoReviewResult.to_envelope here verbatim,
        # using `result.<field>` instead of `self.<field>`.
        ...
    )


def auto_confirm_envelope(result: "AutoConfirmResult") -> ResponseEnvelope:
    """Build the envelope for the categorize.auto_confirm tool."""
    return build_envelope(...)


def auto_stats_envelope(result: "AutoStatsResult") -> ResponseEnvelope:
    """Build the envelope for the categorize.auto_stats tool."""
    return build_envelope(...)
```

Replace the `...` markers with the actual body of each `to_envelope` method copied from `auto_rule_service.py:60-119`. Substitute `self` → `result`.

- [ ] **Step 4: Remove `to_envelope` and the envelope imports from `auto_rule_service.py`.**

In `src/moneybin/services/auto_rule_service.py`:
1. Delete the three `to_envelope` methods on `AutoReviewResult`, `AutoConfirmResult`, `AutoStatsResult` (lines 60–119 plus their corresponding methods further in the file — verify line numbers haven't shifted).
2. Delete the import: `from moneybin.protocol.envelope import ResponseEnvelope, build_envelope`.
3. Confirm the file no longer imports anything from `moneybin.mcp.*` or `moneybin.protocol.*`.

- [ ] **Step 5: Update `mcp/tools/categorize.py` to use the adapters.**

Find every `result.to_envelope()` call and replace with the corresponding adapter call:

```python
# OLD
result = service.auto_review(...)
return result.to_envelope()

# NEW
from moneybin.mcp.adapters.categorize_adapters import auto_review_envelope
result = service.auto_review(...)
return auto_review_envelope(result)
```

Three call sites: `categorize_auto_review`, `categorize_auto_confirm`, `categorize_auto_stats`. Confirm with `grep`.

- [ ] **Step 6: Update `cli/commands/categorize.py` to use the same adapters for `--output json`.**

Find every CLI command that handles auto-review/auto-confirm/auto-stats results. If they currently call `result.to_envelope()` (likely yes, since the methods were on the service dataclass), redirect to the adapter:

```python
from moneybin.mcp.adapters.categorize_adapters import auto_review_envelope
envelope = auto_review_envelope(result)
if output == "json":
    typer.echo(envelope.to_json())
```

- [ ] **Step 7: Run all changed-area tests.**

```bash
uv run pytest tests/services/test_auto_rule_service.py tests/mcp -v
uv run pytest tests/cli -v  # if CLI tests for these commands exist
```

Expected: the new test from Step 1 PASSES; existing service and MCP tests PASS.

- [ ] **Step 8: Run full suite.**

```bash
uv run pytest -x -q
```

Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
git add -A
git commit -m "Move to_envelope() off AutoRuleService dataclasses into MCP adapters

Closes the dependency-direction violation flagged in PR #60: services
must not import from the transport layer. AutoReviewResult,
AutoConfirmResult, and AutoStatsResult are now pure data carriers; the
new mcp/adapters/categorize_adapters module owns envelope construction.

The CLI uses the same adapters so MCP and --output json produce
identical envelopes."
```

---

## Phase 5: Visibility Migration

### Task 6: Add `domain` tag to extended-namespace tools

**Files:**
- Modify: `src/moneybin/mcp/decorator.py` (extend `mcp_tool` with optional `domain` field that emits a tag)
- Modify: every tool currently in `EXTENDED_NAMESPACES` (categorize, budget, tax, privacy, transactions.matches) to declare its `domain`

**Why this differs from earlier draft:** the spike (Task 1) confirmed fastmcp 3.x has **no per-tool `enabled=False` / `initially_hidden=True` decorator kwarg**. Visibility is tag-based: `mcp.tool(tags={...})` declares membership; a server-level `Visibility(False, tags={...})` transform hides the tagged set; `enable_components(ctx, tags={...})` re-enables for the calling session. This task introduces the metadata that drives tag emission. Task 7 wires the registration logic, transforms, and discover.

- [ ] **Step 1: Write failing tests for the new decorator field.**

Create or extend `tests/mcp/test_decorator.py`:

```python
def test_mcp_tool_supports_domain() -> None:
    """The mcp_tool decorator carries a domain string used to emit a tag at
    registration time. Tools in extended namespaces (categorize, budget,
    tax, privacy, transactions.matches) declare a domain; the registration
    layer translates it into mcp.tool(tags={domain})."""
    from moneybin.mcp.decorator import mcp_tool

    @mcp_tool(sensitivity="medium", domain="categorize")
    def example_tool() -> None:
        ...

    assert getattr(example_tool, "_mcp_domain", None) == "categorize"


def test_mcp_tool_default_domain_is_none() -> None:
    """Tools without an explicit domain are core tools (always visible)."""
    from moneybin.mcp.decorator import mcp_tool

    @mcp_tool(sensitivity="low")
    def example_tool() -> None:
        ...

    assert getattr(example_tool, "_mcp_domain", None) is None
```

- [ ] **Step 2: Run the tests, see them fail.**

```bash
uv run pytest tests/mcp/test_decorator.py -v
```

Expected: FAIL — `mcp_tool` doesn't accept `domain` yet.

- [ ] **Step 3: Extend `mcp_tool`.**

Edit `src/moneybin/mcp/decorator.py` — add the `domain` parameter and attribute. Building on the Task 4 wrapper (which catches domain exceptions and returns the envelope directly):

```python
def mcp_tool(
    *,
    sensitivity: str,
    domain: str | None = None,
) -> Callable[..., Any]:
    """Decorator that marks a function as an MCP tool with a sensitivity tier.

    Args:
        sensitivity: Data sensitivity tier (``"low"``, ``"medium"``, ``"high"``).
        domain: Extended-namespace name (e.g. ``"categorize"``, ``"budget"``).
            Tools with a ``domain`` start hidden and must be enabled per-session
            via ``moneybin.discover``. Tools without a domain are core tools,
            visible at connect. The registration layer translates this into
            ``mcp.tool(tags={domain})``; a server-level ``Visibility`` transform
            then hides the tagged set.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
            # Body is from Task 4 — log_tool_call + try/except classified
            # exceptions + return result directly. No JSON conversion.
            ...

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        return wrapper

    return decorator
```

(Keep the body from Task 4; only the signature and the new `_mcp_domain` attribute are introduced here.)

- [ ] **Step 4: Run the tests.**

```bash
uv run pytest tests/mcp/test_decorator.py -v
```

Expected: PASS.

- [ ] **Step 5: Tag every tool in extended namespaces with its domain.**

The extended namespaces from `src/moneybin/mcp/namespaces.py`'s `EXTENDED_NAMESPACES` set are: `categorize`, `budget`, `tax`, `privacy`, `transactions.matches`. For every tool function in those namespaces, add `domain=`:

```python
@mcp_tool(sensitivity="medium", domain="categorize")
def categorize_bulk(...) -> ResponseEnvelope:
    ...
```

To find them quickly:
```bash
grep -ln "@mcp_tool" src/moneybin/mcp/tools/categorize.py src/moneybin/mcp/tools/budget.py src/moneybin/mcp/tools/tax.py
```
(Plus any privacy or transactions.matches tools — check `EXTENDED_NAMESPACES` in `namespaces.py` for the canonical list.)

For tools in `transactions.matches`, only those whose registered name starts with `transactions.matches.` get `domain="transactions.matches"` — `transactions.search` and similar in the core `transactions` namespace stay un-domained (visible).

The domain string equals the namespace prefix exactly. `moneybin.discover` will accept it as the argument and call `enable_components(ctx, tags={domain})`.

- [ ] **Step 6: Run the full suite.**

```bash
uv run pytest -x -q
```

Expected: PASS. Adding metadata shouldn't change runtime behavior — registration still happens via the existing `NamespaceRegistry` path, which is replaced in Task 7.

- [ ] **Step 7: Commit.**

```bash
git add -A
git commit -m "Add domain field to mcp_tool decorator for tag-based visibility

Tools in extended namespaces (categorize, budget, tax, privacy,
transactions.matches) declare their domain. The registration layer
(Task 7) translates this into mcp.tool(tags={domain}); a server-level
Visibility transform then hides the tagged set, and moneybin.discover
re-enables them per-session via enable_components(ctx, tags={domain}).

This is metadata only — Task 7 wires it into the registration path."
```

---

### Task 7: Replace `NamespaceRegistry` with fastmcp visibility transforms

**Files:**
- Modify: `src/moneybin/mcp/server.py` (rewrite `register_core_tools`; install one `Visibility(False, tags={domain})` transform per extended namespace)
- Modify: `src/moneybin/mcp/tools/discover.py` (rewrite `moneybin_discover` to use `enable_components(ctx, tags={domain})`)
- Delete: `src/moneybin/mcp/namespaces.py`
- Modify: every `register_<namespace>_tools` function in `src/moneybin/mcp/tools/*.py` (no longer takes a `NamespaceRegistry`; uses `mcp.tool(name=..., tags={...})` directly)
- Modify: `src/moneybin/mcp/resources.py` if it consumes `tools_resource_data`
- Test: `tests/mcp/test_visibility.py` (new), `tests/e2e/test_e2e_mcp.py`

This is the largest task. Execute carefully. Spike notes Task 1 confirmed the API:
- `Visibility(enabled, *, names=None, tags=None, ...)` from `fastmcp.server.transforms` — installed via `mcp.add_transform(...)`. Hides matching tools globally.
- `enable_components(ctx, *, names=None, tags=None, ...)` from `fastmcp.server.transforms.visibility` — re-enables matching tools for the calling session. **`names` is `set[str]`, NOT varargs.**
- Hidden tools are uncallable by name (security check confirmed in spike).
- `Context` is auto-injected when a parameter is annotated `Context`. Import: `from fastmcp import Context`.

The `Tool` objects exposed by fastmcp's `_tool_manager` are accessible via `await mcp.get_tools()` (the public API for listing all registered tools — used by the spike's session test).

- [ ] **Step 1: Write failing tests for the new visibility behavior.**

Create `tests/mcp/test_visibility.py`. The fastmcp test client API per spike notes:

```python
"""Per-session visibility tests for MCP tools.

Replaces NamespaceRegistry-based progressive disclosure with fastmcp 3.x
tag-based visibility transforms.
"""
from __future__ import annotations

import pytest
from fastmcp import Client


@pytest.mark.asyncio
async def test_core_tools_visible_at_connect() -> None:
    """Tools without a domain are listed by default."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        names = {t.name for t in await client.list_tools()}
        assert "spending.summary" in names
        assert "accounts.list" in names


@pytest.mark.asyncio
async def test_extended_tools_hidden_at_connect() -> None:
    """Tools with a domain are not listed by default — Visibility transforms hide them."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        names = {t.name for t in await client.list_tools()}
        assert "categorize.bulk" not in names
        assert "budget.set" not in names


@pytest.mark.asyncio
async def test_discover_reveals_namespace_tools() -> None:
    """Calling moneybin.discover('categorize') enables every tool tagged
    'categorize' for the calling session."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        await client.call_tool("moneybin.discover", {"domain": "categorize"})
        names = {t.name for t in await client.list_tools()}
        assert "categorize.bulk" in names


@pytest.mark.asyncio
async def test_unknown_domain_returns_error_envelope() -> None:
    """Calling discover('not-a-real-namespace') returns an error envelope."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        result = await client.call_tool("moneybin.discover", {"domain": "nope"})
        # ResponseEnvelope as structured_content per Task 4 changes
        envelope = result.structured_content
        assert envelope.get("error") is not None or "Unknown domain" in str(envelope)


@pytest.mark.asyncio
async def test_hidden_tool_is_uncallable_via_tools_call() -> None:
    """Hidden tools must be uncallable. Verified safe by spike (3.2.4
    raises ToolError: Unknown tool: '<name>'). This test guards against
    regression if fastmcp's behavior ever changes."""
    from fastmcp.exceptions import ToolError

    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        with pytest.raises(ToolError):
            await client.call_tool("categorize.bulk", {})
```

- [ ] **Step 2: Run the tests, see them fail.**

```bash
uv run pytest tests/mcp/test_visibility.py -v
```

Expected: FAIL on missing transforms / discover not yet rewritten.

- [ ] **Step 3: Rewrite `register_<namespace>_tools` functions.**

Each tool module currently has a registration function like:

```python
def register_spending_tools(registry: NamespaceRegistry) -> None:
    registry.register(ToolDefinition(name="spending.summary", description="...", fn=spending_summary))
    ...
```

Replace each with direct registration on the `FastMCP` server, deriving the tag set from the tool's `_mcp_domain` attribute (set in Task 6):

```python
from fastmcp import FastMCP


def _tags_for(fn) -> set[str] | None:
    """Translate the mcp_tool decorator's _mcp_domain attribute into a tag set."""
    domain = getattr(fn, "_mcp_domain", None)
    return {domain} if domain else None


def register_spending_tools(mcp: FastMCP) -> None:
    """Register spending tools with the server."""
    mcp.tool(
        name="spending.summary",
        description="...",
        tags=_tags_for(spending_summary),
    )(spending_summary)
    ...
```

Helper `_tags_for` can live once in `src/moneybin/mcp/_registration.py` (new shared module) and be imported by every `register_<namespace>_tools` function. Apply this transformation to every registration function: `accounts`, `budget`, `categorize`, `import_tools`, `spending`, `sql`, `tax`, `transactions`. Skip `discover.py` — handled in Step 5.

`mcp.tool(...)` accepts `tags=None` cleanly (no tag added), so core tools pass through unchanged.

- [ ] **Step 4: Rewrite `register_core_tools` in `server.py` and install Visibility transforms.**

Replace the body of `register_core_tools` (`src/moneybin/mcp/server.py:150-181`) with a flat sequence of calls plus the visibility transforms:

```python
from fastmcp.server.transforms import Visibility

# The set of extended-namespace domain names. Derived from EXTENDED_NAMESPACES
# in the now-deleted namespaces.py — kept here as the single source of truth
# for which namespaces are hidden by default.
EXTENDED_DOMAINS: frozenset[str] = frozenset({
    "categorize",
    "budget",
    "tax",
    "privacy",
    "transactions.matches",
})


def register_core_tools() -> None:
    """Register all MCP tools and install per-domain Visibility transforms.

    Tools tagged with an extended-namespace domain (categorize, budget, tax,
    privacy, transactions.matches) are hidden globally by Visibility transforms
    installed below. moneybin.discover re-enables them per-session.
    """
    from moneybin.mcp.tools.accounts import register_accounts_tools
    from moneybin.mcp.tools.budget import register_budget_tools
    from moneybin.mcp.tools.categorize import register_categorize_tools
    from moneybin.mcp.tools.discover import register_discover_tool
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.spending import register_spending_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.tax import register_tax_tools
    from moneybin.mcp.tools.transactions import register_transactions_tools

    register_spending_tools(mcp)
    register_accounts_tools(mcp)
    register_transactions_tools(mcp)
    register_import_tools(mcp)
    register_categorize_tools(mcp)
    register_budget_tools(mcp)
    register_tax_tools(mcp)
    register_sql_tools(mcp)
    register_discover_tool(mcp)

    # Hide each extended namespace globally; sessions re-enable via discover.
    for domain in EXTENDED_DOMAINS:
        mcp.add_transform(Visibility(False, tags={domain}))

    logger.info(
        f"Registered tools; {len(EXTENDED_DOMAINS)} extended namespaces hidden by default"
    )
```

Delete the `_registry`, `get_registry`, and `_build_registry` functions from the same file — they're no longer needed.

- [ ] **Step 5: Rewrite `moneybin.discover`.**

Replace `src/moneybin/mcp/tools/discover.py` body with:

```python
"""moneybin.discover — per-session progressive disclosure via tag enablement."""
from __future__ import annotations

import logging

from fastmcp import Context, FastMCP
from fastmcp.server.transforms.visibility import enable_components

from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)


_NAMESPACE_DESCRIPTIONS: dict[str, str] = {
    "categorize": "Rules, merchant mappings, bulk categorization",
    "budget": "Budget targets, status, rollovers",
    "tax": "W-2 data, deductible expense search",
    "privacy": "Consent status, grants, revocations, audit log",
    "transactions.matches": "Match review workflow",
}


@mcp_tool(sensitivity="low")
async def moneybin_discover(domain: str, ctx: Context) -> ResponseEnvelope:
    """Reveal tools from an extended namespace for the calling session.

    Extended namespaces (categorize, budget, tax, privacy,
    transactions.matches) start hidden. Calling this tool with a domain
    name enables the tools tagged with that domain for the current session
    only — other connected clients are unaffected.

    Args:
        domain: The namespace to reveal (e.g. 'categorize', 'budget').
    """
    from moneybin.mcp.server import EXTENDED_DOMAINS

    if domain not in EXTENDED_DOMAINS:
        return build_envelope(
            data={"domain": domain, "error": f"Unknown domain: {domain}"},
            sensitivity="low",
        )

    await enable_components(ctx, tags={domain})
    return build_envelope(
        data={
            "domain": domain,
            "description": _NAMESPACE_DESCRIPTIONS.get(domain, ""),
        },
        sensitivity="low",
        actions=[
            f"Tools tagged '{domain}' enabled for this session.",
            "Call discover again with a different domain to reveal more tools.",
        ],
    )


def register_discover_tool(mcp: FastMCP) -> None:
    """Register moneybin.discover with the server (always visible — no domain tag)."""
    mcp.tool(
        name="moneybin.discover",
        description=(
            "Reveal tools from an extended namespace (categorize, budget, "
            "tax, privacy, transactions.matches) for the current session."
        ),
    )(moneybin_discover)
```

Note: discover does **not** derive a list of tool names — `enable_components` matches by tag directly, so no enumeration of `mcp.get_tools()` is needed. This is simpler than the original draft.

- [ ] **Step 6: Update `resources.py` if it depends on `tools_resource_data`.**

```bash
grep -n "tools_resource_data\|NamespaceRegistry\|get_registry\|EXTENDED_NAMESPACES\|CORE_NAMESPACES" src/moneybin/mcp/resources.py
```

If hits exist, rewrite the affected resource to derive the same data from `EXTENDED_DOMAINS` (now in `server.py`) and `await mcp.get_tools()` (filtering by tag membership). The shape stays the same (`{core: [...], extended: [...], discover_tool: "moneybin.discover"}`).

- [ ] **Step 7: Delete the obsolete registry module.**

```bash
rm src/moneybin/mcp/namespaces.py
```

Verify no stray imports remain:
```bash
grep -rn "moneybin.mcp.namespaces\|NamespaceRegistry\|CORE_NAMESPACES_DEFAULT\|EXTENDED_NAMESPACES" src tests --include="*.py"
```
Expected: only the `EXTENDED_DOMAINS` constant in `server.py` (different name) survives. Other hits indicate stragglers — fix them (likely in `resources.py` or tests).

- [ ] **Step 8: Run the visibility tests.**

```bash
uv run pytest tests/mcp/test_visibility.py -v
```

Expected: PASS. The `test_hidden_tool_is_uncallable_via_tools_call` test should pass without any extra gate — fastmcp 3.2.4 enforces this natively (verified in spike).

- [ ] **Step 9: Run the full suite.**

```bash
uv run pytest -x -q
```

Expected: PASS modulo any e2e tests that need updating in Task 8. If a test fails because it assumed all tools visible at connect, mark the failure for Task 8 — don't fix here.

- [ ] **Step 10: Commit.**

```bash
git add -A
git commit -m "Replace NamespaceRegistry with fastmcp 3.x visibility transforms

Each tool in an extended namespace is tagged with its domain at
registration time (mcp.tool(tags={domain}), driven by the _mcp_domain
attribute set by mcp_tool in Task 6). Server boot installs one
Visibility(False, tags={domain}) transform per extended namespace,
hiding the tagged set globally.

moneybin.discover becomes a thin wrapper around enable_components(ctx,
tags={domain}) — per-session, no enumeration. Different clients
connected to the same server have independent visibility.

Deletes src/moneybin/mcp/namespaces.py; the canonical extended-namespace
list is now EXTENDED_DOMAINS in server.py."
```

---

## Phase 6: Test Migration & Documentation

### Task 8: Update tests for per-session visibility model

**Files:**
- Modify: `tests/e2e/conftest.py` (add a fixture that pre-discovers all extended namespaces, for legacy tests)
- Modify: `tests/e2e/test_e2e_mcp.py`, `tests/e2e/test_e2e_mutating.py`, `tests/e2e/test_e2e_workflows.py` (use the new fixture or call `discover` explicitly)

- [ ] **Step 1: Identify tests that exercise extended-namespace tools.**

```bash
grep -ln "categorize\.\|budget\.\|tax\.\|privacy\." tests/e2e/*.py
```

Each hit is a test that touches an extended namespace and therefore needs `discover` called before the tool is reachable (or needs the all-discover fixture).

- [ ] **Step 2: Read the existing e2e conftest to learn the test client pattern.**

```bash
grep -n "fixture\|Client\|mcp_client" tests/e2e/conftest.py | head -30
```

The current e2e tests likely use a custom client wrapper or a pytest fixture that yields a pre-configured client. Match its style. The underlying fastmcp test client API is:

```python
from fastmcp import Client
async with Client(mcp_server) as client:
    tools = await client.list_tools()
    result = await client.call_tool("name", {"arg": "value"})
```

If the existing fixture wraps this with sync helpers (`client.call(...)`), keep that wrapper and add the fixture below; if tests use `await client.call_tool(...)` directly, write the fixture as `async`.

- [ ] **Step 3: Add an `all_namespaces_discovered` fixture in `tests/e2e/conftest.py`.**

Following the existing pattern (sync example shown; convert to `async` if the conftest uses async fixtures):

```python
@pytest.fixture
async def all_namespaces_discovered(mcp_client):
    """Pre-discover every extended namespace for tests written before
    progressive disclosure became per-session.

    New tests should call discover explicitly for the domains they touch
    (matches production flow); this fixture is a migration shim.

    TODO: convert all tests using this fixture to explicit discover calls
    over time. Track in followups.md.
    """
    for domain in ("categorize", "budget", "tax", "privacy", "transactions.matches"):
        await mcp_client.call_tool("moneybin.discover", {"domain": domain})
    return mcp_client
```

- [ ] **Step 4: Apply the fixture to legacy tests that fail without it.**

Run the failing e2e tests (from Task 7 Step 9) and add `all_namespaces_discovered` to their signatures:

```python
async def test_categorize_bulk_workflow(all_namespaces_discovered):
    ...
```

For tests that ALREADY exercise discovery as part of their workflow (e.g., they call `moneybin.discover` directly), don't add the fixture — they should remain explicit.

- [ ] **Step 5: Add at least one test that exercises explicit per-session discovery.**

In `tests/e2e/test_e2e_mcp.py`:

```python
import pytest
from fastmcp import Client

from moneybin.mcp.server import mcp


@pytest.mark.asyncio
async def test_per_session_discover_isolated() -> None:
    """Two clients connected to the same server have independent
    visibility — one client discovering categorize does not affect
    the other client's tool list. fastmcp Client sessions are isolated
    by construction; this test guards against regression."""
    async with Client(mcp) as client_a, Client(mcp) as client_b:
        before_a = {t.name for t in await client_a.list_tools()}
        assert "categorize.bulk" not in before_a

        await client_a.call_tool("moneybin.discover", {"domain": "categorize"})

        after_a = {t.name for t in await client_a.list_tools()}
        visible_b = {t.name for t in await client_b.list_tools()}

        assert "categorize.bulk" in after_a
        assert "categorize.bulk" not in visible_b, (
            "Client B's tool visibility leaked from Client A's discover call — "
            "session isolation is broken."
        )
```

If two simultaneous `Client(mcp)` contexts cannot share the same in-process server (some transports require subprocess), port this to a subprocess-based variant or mark it as a documented manual-verification step in `docs/specs/mcp-architecture.md` and skip with `pytest.skip("Multi-client session isolation requires manual verification")` plus a reason.

- [ ] **Step 6: Run the full e2e suite.**

```bash
uv run pytest tests/e2e -v
```

Expected: PASS. If failures remain that aren't covered by the fixture, either add the fixture to those tests or convert them to explicit-discover.

- [ ] **Step 7: Run the full test suite end-to-end.**

```bash
uv run pytest -q
```

Expected: PASS.

- [ ] **Step 8: Commit.**

```bash
git add -A
git commit -m "Migrate e2e tests to per-session visibility model

Adds tests/e2e/conftest.py::all_namespaces_discovered as a migration
shim for tests written before progressive disclosure became
per-session — they call this fixture to pre-discover all extended
namespaces.

Adds an explicit multi-client session-isolation test verifying that
one client's discover does not affect another client's tool list.

TODO (tracked in followups): convert shim users to explicit discover
calls over time."
```

---

### Task 9: Update architecture docs

**Files:**
- Modify: `docs/specs/mcp-architecture.md` (replace `NamespaceRegistry` references with visibility system; update §3 progressive disclosure)
- Modify: `.claude/rules/mcp-server.md` (update progressive-disclosure paragraph)
- Modify: `private/followups.md` (mark fastmcp migration item as resolved; add a note pointing to ADR-008; remove the `EnvelopeMixin` and "to_envelope dependency direction" items since this PR addresses them)

- [ ] **Step 1: Read current `mcp-architecture.md` §3.**

```bash
uv run sed -n '/^## 3/,/^## 4/p' docs/specs/mcp-architecture.md
```

Identify language about "registration at connection time" vs "loaded on demand via tools/list_changed" — these become "tagged at registration with `tags={domain}`, hidden globally by `Visibility(False, tags={domain})` transforms, and re-enabled per-session via `enable_components(ctx, tags={domain})`" in 3.x.

- [ ] **Step 2: Rewrite the progressive-disclosure section.**

Replace the relevant paragraph with text that describes per-session visibility, derived domain index, and explicit-discover semantics. Keep the discoverable-tool count estimates (~19 core) but reframe as "core (visible at connect)" + "extended (hidden, revealed per-session)."

- [ ] **Step 3: Update `.claude/rules/mcp-server.md`.**

The Progressive Disclosure paragraph currently says "Core namespaces (~19 tools) registered at connection time. Extended namespaces ... loaded on demand via `moneybin.discover` meta-tool + `tools/list_changed`." Update to describe visibility-based per-session disclosure.

- [ ] **Step 4: Resolve followups.**

Edit `private/followups.md`:
1. Delete the entire "Migrate from `mcp.server.fastmcp` to `fastmcp` 3.x" section (this PR ships it).
2. Delete the "Move `to_envelope()` out of service-layer dataclasses" section (this PR ships it).
3. Delete the "EnvelopeMixin" section (the precondition is gone — `to_envelope` no longer lives on services).
4. Delete the "Apply `handle_mcp_errors` across MCP tool modules" section (this PR ships the equivalent — domain-exception handling now lives in the `mcp_tool` decorator, with `mask_error_details=True` covering unclassified leaks).
5. Add a single line under a "Resolved" heading noting that ADR-008 captures the shipped scope.

- [ ] **Step 5: Update README roadmap if needed.**

Per `.claude/rules/shipping.md`, check whether any roadmap entry in `README.md` references the FastMCP layer or progressive disclosure. If so, leave the icon as ✅ if it was already shipped (we're not changing user-facing functionality, just internals) and add a one-line note indicating the SDK upgrade.

- [ ] **Step 6: Run `make format && make lint && uv run pyright src/moneybin tests`.**

```bash
make format && make lint && uv run pyright src/moneybin tests
```

Expected: clean.

- [ ] **Step 7: Commit.**

```bash
git add -A
git commit -m "Update docs and resolve followups for fastmcp 3.x migration

- Rewrite mcp-architecture.md §3 progressive disclosure for per-session
  visibility model.
- Update .claude/rules/mcp-server.md.
- Resolve followups: delete the migrate-fastmcp, move-to-envelope,
  EnvelopeMixin, and handle_mcp_errors-rollout entries; point to
  ADR-008 for shipped scope."
```

---

### Task 10: Pre-push quality pass

**Files:** entire branch.

- [ ] **Step 1: Delete spike notes (now superseded by ADR + plan).**

```bash
rm docs/superpowers/plans/2026-04-29-fastmcp-3x-spike-notes.md
```

The notes existed to inform implementation; once the plan is executed, the answers live in the code itself.

- [ ] **Step 2: Run `/simplify` per shipping.md.**

Invoke the `/simplify` slash command in this session and apply any simplifications it suggests within the migration's scope. Do NOT expand scope.

- [ ] **Step 3: Final pre-commit gate.**

```bash
make check test
```

Expected: clean format/lint/type-check/tests.

- [ ] **Step 4: Diff review against `main`.**

```bash
git --no-pager diff main...HEAD --stat
git --no-pager log main..HEAD --oneline
```

Sanity-check the file list against the ADR scope (six items). If any unrelated files appear in the diff, investigate before pushing.

- [ ] **Step 5: Final commit (if simplify produced changes).**

```bash
git add -A
git commit -m "Pre-push simplify pass for fastmcp 3.x migration"
```

(Skip if `git status` is clean.)

- [ ] **Step 6: Push and open PR.**

Use the `/commit-push-pr` skill (or invoke `gh pr create` directly per the project's conventions). PR title: `Upgrade to FastMCP 3.x with per-session visibility`. PR body should reference ADR-008 and summarize the six scope items.

---

## Verification Checklist (final)

Before merging:

- [ ] `make check test` passes locally.
- [ ] CI passes on the branch.
- [ ] `pyproject.toml` shows `fastmcp>=3.1,<4` and no longer shows `mcp[cli]` as a direct dependency.
- [ ] No file in `src/moneybin/` imports from `mcp.server.fastmcp` (`grep -rn "mcp.server.fastmcp" src` returns zero).
- [ ] `src/moneybin/mcp/namespaces.py` and `src/moneybin/mcp/error_handler.py` are deleted.
- [ ] `src/moneybin/protocol/envelope.py` exists; no production code imports `moneybin.mcp.envelope`.
- [ ] ADR-008 status flipped to `accepted` (separate commit or done as part of Task 9 Step 4).
- [ ] `private/followups.md` no longer contains entries for the four resolved items.
- [ ] At least one test verifies multi-client session isolation, OR a documented manual-verification step exists in `mcp-architecture.md`.
- [ ] No regression in MCP tool count exposed to a fully-discovered session vs pre-migration (every previously-available tool is still callable after explicit discovery).
