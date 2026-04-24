# MCP v1 Scaffolding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the MCP v1 service layer pattern, response envelope, sensitivity decorator, progressive disclosure via namespace registration, and migrate ~25 prototype MCP tools to v1 naming/patterns. This is Level 0 infrastructure that every future feature plugs into.

**Architecture:** Thin MCP tools and CLI commands wrap shared service classes that return typed dataclasses. Every tool response uses a consistent `{summary, data, actions}` envelope. Tools declare a sensitivity tier via `@mcp_tool(sensitivity=...)` decorator; privacy middleware is stubbed (logs tier, consent is no-op until privacy specs land). Progressive disclosure registers core namespaces (~19 tools) at connection time; extended namespaces load on demand via `moneybin.discover`. Clean break from prototype tool names.

**Tech Stack:** Python 3.12, FastMCP, DuckDB, Typer, Pydantic, pytest

**Specs:** `docs/specs/mcp-architecture.md` (especially sections 2-5, 7), `docs/specs/mcp-tool-surface.md` (sections 1-2 exemplars, 15-15b resources/discover, 16 migration, 17 dependencies)

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/moneybin/mcp/envelope.py` | `ResponseEnvelope`, `SummaryMeta` dataclasses, `DetailLevel` enum, `build_envelope()` helper |
| `src/moneybin/mcp/decorator.py` | `@mcp_tool(sensitivity=...)` decorator wrapping FastMCP's `@mcp.tool()` with privacy middleware stub |
| `src/moneybin/mcp/namespaces.py` | `NamespaceRegistry` class, core/extended namespace definitions, `moneybin.discover` meta-tool |
| `src/moneybin/mcp/tools/__init__.py` | Package init |
| `src/moneybin/mcp/tools/spending.py` | `spending.summary`, `spending.by_category` |
| `src/moneybin/mcp/tools/accounts.py` | `accounts.list`, `accounts.balances` |
| `src/moneybin/mcp/tools/transactions.py` | `transactions.search`, `transactions.recurring` |
| `src/moneybin/mcp/tools/import_tools.py` | `import.file`, `import.status`, `import.csv_preview`, `import.list_formats` |
| `src/moneybin/mcp/tools/categorize.py` | `categorize.uncategorized`, `categorize.bulk`, `categorize.rules`, `categorize.create_rules`, `categorize.delete_rule`, `categorize.merchants`, `categorize.create_merchants`, `categorize.categories`, `categorize.create_category`, `categorize.toggle_category`, `categorize.seed`, `categorize.stats` |
| `src/moneybin/mcp/tools/budget.py` | `budget.set`, `budget.status` |
| `src/moneybin/mcp/tools/tax.py` | `tax.w2` |
| `src/moneybin/mcp/tools/sql.py` | `sql.query` |
| `src/moneybin/services/spending_service.py` | `SpendingService` with `summary()`, `by_category()` |
| `src/moneybin/services/account_service.py` | `AccountService` with `list_accounts()`, `balances()` |
| `src/moneybin/services/transaction_service.py` | `TransactionService` with `search()`, `recurring()` |
| `src/moneybin/services/budget_service.py` | `BudgetService` with `set_budget()`, `status()` |
| `src/moneybin/services/tax_service.py` | `TaxService` with `w2()` |
| `src/moneybin/cli/output.py` | `OutputFormat` enum, `--output` option factory, `render_or_json()` helper |
| `tests/moneybin/test_mcp/test_envelope.py` | Envelope unit tests |
| `tests/moneybin/test_mcp/test_decorator.py` | Decorator unit tests |
| `tests/moneybin/test_mcp/test_namespaces.py` | Namespace registry and discover tests |
| `tests/moneybin/test_mcp/test_v1_tools.py` | V1 tool integration tests |
| `tests/moneybin/test_services/test_spending_service.py` | SpendingService tests |
| `tests/moneybin/test_services/test_account_service.py` | AccountService tests |
| `tests/moneybin/test_services/test_transaction_service.py` | TransactionService tests |
| `tests/moneybin/test_services/test_budget_service.py` | BudgetService tests |
| `tests/moneybin/test_services/test_tax_service.py` | TaxService tests |

### Modified files

| File | Changes |
|------|---------|
| `src/moneybin/mcp/server.py` | Update instructions for v1, use namespace-aware registration, keep `get_db()`/`get_db_path()`/`table_exists()` helpers |
| `src/moneybin/mcp/privacy.py` | Add `Sensitivity` enum, `log_tool_call()` stub; keep existing query validation |
| `src/moneybin/mcp/resources.py` | Rewrite with v1 resource URIs |
| `src/moneybin/mcp/prompts.py` | Rewrite with 4 v1 goal-oriented prompts |
| `src/moneybin/mcp/__init__.py` | Update docstring |
| `src/moneybin/config.py` | Add `core_namespaces` to `MCPConfig` |
| `src/moneybin/services/categorization_service.py` | Add typed return dataclasses for service methods |
| `tests/moneybin/test_mcp/conftest.py` | Keep as-is (shared fixture still valid) |

### Deleted files

| File | Reason |
|------|--------|
| `src/moneybin/mcp/tools.py` | Replaced by `src/moneybin/mcp/tools/` directory (v1 tools) |
| `src/moneybin/mcp/write_tools.py` | Replaced by `src/moneybin/mcp/tools/` directory (v1 tools) |

---

## Task 1: Response Envelope

The response envelope is the foundation — every tool returns `{summary, data, actions}`. Build and test it first.

**Files:**
- Create: `src/moneybin/mcp/envelope.py`
- Test: `tests/moneybin/test_mcp/test_envelope.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/moneybin/test_mcp/test_envelope.py
"""Tests for the MCP response envelope."""

import json
from decimal import Decimal

import pytest

from moneybin.mcp.envelope import (
    DetailLevel,
    ResponseEnvelope,
    SummaryMeta,
    build_envelope,
)


class TestDetailLevel:
    """Tests for the DetailLevel enum."""

    @pytest.mark.unit
    def test_values(self) -> None:
        assert DetailLevel.SUMMARY == "summary"
        assert DetailLevel.STANDARD == "standard"
        assert DetailLevel.FULL == "full"

    @pytest.mark.unit
    def test_from_string(self) -> None:
        assert DetailLevel("summary") == DetailLevel.SUMMARY
        assert DetailLevel("standard") == DetailLevel.STANDARD
        assert DetailLevel("full") == DetailLevel.FULL

    @pytest.mark.unit
    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            DetailLevel("verbose")


class TestSummaryMeta:
    """Tests for the SummaryMeta dataclass."""

    @pytest.mark.unit
    def test_defaults(self) -> None:
        meta = SummaryMeta(total_count=10, returned_count=10)
        assert meta.has_more is False
        assert meta.sensitivity == "low"
        assert meta.display_currency == "USD"
        assert meta.degraded is False
        assert meta.degraded_reason is None
        assert meta.period is None

    @pytest.mark.unit
    def test_has_more_when_truncated(self) -> None:
        meta = SummaryMeta(total_count=100, returned_count=50, has_more=True)
        assert meta.has_more is True


class TestResponseEnvelope:
    """Tests for the ResponseEnvelope dataclass."""

    @pytest.mark.unit
    def test_to_dict_structure(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=3, returned_count=3),
            data=[{"period": "2026-04", "income": 5200.00}],
            actions=["Use spending.by_category for breakdown"],
        )
        d = envelope.to_dict()
        assert set(d.keys()) == {"summary", "data", "actions"}
        assert d["summary"]["total_count"] == 3
        assert d["summary"]["returned_count"] == 3
        assert d["summary"]["has_more"] is False
        assert d["summary"]["sensitivity"] == "low"
        assert len(d["data"]) == 1
        assert len(d["actions"]) == 1

    @pytest.mark.unit
    def test_to_json_serializes(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=1, returned_count=1),
            data=[{"amount": Decimal("42.50")}],
        )
        text = envelope.to_json()
        parsed = json.loads(text)
        assert parsed["summary"]["total_count"] == 1
        assert parsed["data"][0]["amount"] == "42.50"

    @pytest.mark.unit
    def test_empty_actions_default(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0),
            data=[],
        )
        assert envelope.actions == []

    @pytest.mark.unit
    def test_degraded_envelope(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(
                total_count=247,
                returned_count=5,
                sensitivity="low",
                degraded=True,
                degraded_reason="Transaction-level data requires data-sharing consent",
            ),
            data=[{"category": "Groceries", "total": 1245.67}],
            actions=[
                "Run 'moneybin privacy grant mcp-data-sharing' to enable full details"
            ],
        )
        d = envelope.to_dict()
        assert d["summary"]["degraded"] is True
        assert "consent" in d["summary"]["degraded_reason"]


class TestBuildEnvelope:
    """Tests for the build_envelope helper."""

    @pytest.mark.unit
    def test_build_from_list(self) -> None:
        rows = [{"a": 1}, {"a": 2}, {"a": 3}]
        envelope = build_envelope(
            data=rows,
            sensitivity="low",
        )
        assert envelope.summary.total_count == 3
        assert envelope.summary.returned_count == 3
        assert envelope.summary.has_more is False

    @pytest.mark.unit
    def test_build_with_truncation(self) -> None:
        rows = [{"a": i} for i in range(50)]
        envelope = build_envelope(
            data=rows,
            sensitivity="medium",
            total_count=200,
        )
        assert envelope.summary.total_count == 200
        assert envelope.summary.returned_count == 50
        assert envelope.summary.has_more is True

    @pytest.mark.unit
    def test_build_with_period(self) -> None:
        envelope = build_envelope(
            data=[],
            sensitivity="low",
            period="2026-01 to 2026-04",
        )
        assert envelope.summary.period == "2026-01 to 2026-04"

    @pytest.mark.unit
    def test_build_with_actions(self) -> None:
        envelope = build_envelope(
            data=[],
            sensitivity="low",
            actions=["Try spending.by_category"],
        )
        assert envelope.actions == ["Try spending.by_category"]

    @pytest.mark.unit
    def test_build_write_result(self) -> None:
        """Write tools return a dict, not a list."""
        result = {"applied": 48, "skipped": 0, "errors": 2}
        envelope = build_envelope(
            data=result,
            sensitivity="medium",
            total_count=50,
        )
        assert envelope.summary.total_count == 50
        assert envelope.data == result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.mcp.envelope'`

- [ ] **Step 3: Implement the envelope module**

```python
# src/moneybin/mcp/envelope.py
"""Response envelope for MCP tools and CLI --output json.

Every MCP tool and every CLI command with ``--output json`` returns this
shape: ``{summary, data, actions}``. The envelope gives AI consumers
consistent metadata (counts, truncation, sensitivity, currency) and
contextual next-step hints.

See ``mcp-architecture.md`` section 4 for design rationale.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any


class DetailLevel(StrEnum):
    """Detail level for tool responses.

    Controls response verbosity:
    - ``summary``: aggregates only (always tier-1 safe)
    - ``standard``: default view
    - ``full``: every available field
    """

    SUMMARY = "summary"
    STANDARD = "standard"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class SummaryMeta:
    """Metadata section of the response envelope.

    Provides AI consumers with context about the response: counts,
    whether results are truncated, sensitivity tier, and currency.
    """

    total_count: int
    returned_count: int
    has_more: bool = False
    period: str | None = None
    sensitivity: str = "low"
    display_currency: str = "USD"
    degraded: bool = False
    degraded_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict, omitting None fields and False degraded."""
        d: dict[str, Any] = {
            "total_count": self.total_count,
            "returned_count": self.returned_count,
            "has_more": self.has_more,
            "sensitivity": self.sensitivity,
            "display_currency": self.display_currency,
        }
        if self.period is not None:
            d["period"] = self.period
        if self.degraded:
            d["degraded"] = True
            if self.degraded_reason:
                d["degraded_reason"] = self.degraded_reason
        return d


class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that serializes Decimal as string to avoid float imprecision."""

    def default(self, o: object) -> Any:
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


@dataclass(slots=True)
class ResponseEnvelope:
    """Standard response shape for all MCP tools.

    Three sections:
    - ``summary``: metadata for the AI (counts, truncation, sensitivity)
    - ``data``: the payload (list of objects or single result dict)
    - ``actions``: contextual next-step hints
    """

    summary: SummaryMeta
    data: list[dict[str, Any]] | dict[str, Any]
    actions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for JSON serialization."""
        return {
            "summary": self.summary.to_dict(),
            "data": self.data,
            "actions": self.actions,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), cls=_DecimalEncoder, default=str)


def build_envelope(
    *,
    data: list[dict[str, Any]] | dict[str, Any],
    sensitivity: str,
    total_count: int | None = None,
    period: str | None = None,
    display_currency: str = "USD",
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
) -> ResponseEnvelope:
    """Build a ResponseEnvelope with computed metadata.

    Args:
        data: The payload — list of records or a write-result dict.
        sensitivity: Sensitivity tier of the response.
        total_count: Total matching records (if known and different from
            returned count). When None, inferred from data length.
        period: Human-readable period string (e.g., ``"2026-01 to 2026-04"``).
        display_currency: Currency for all amounts in the response.
        actions: Contextual next-step hints.
        degraded: Whether this is a degraded (no-consent) response.
        degraded_reason: Why the response is degraded.

    Returns:
        A fully populated ResponseEnvelope.
    """
    if isinstance(data, list):
        returned = len(data)
    else:
        returned = 1

    actual_total = total_count if total_count is not None else returned
    has_more = actual_total > returned

    summary = SummaryMeta(
        total_count=actual_total,
        returned_count=returned,
        has_more=has_more,
        period=period,
        sensitivity=sensitivity,
        display_currency=display_currency,
        degraded=degraded,
        degraded_reason=degraded_reason,
    )

    return ResponseEnvelope(
        summary=summary,
        data=data,
        actions=actions or [],
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py -v`
Expected: All 12 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/envelope.py tests/moneybin/test_mcp/test_envelope.py
git commit -m "feat: add MCP response envelope dataclasses

ResponseEnvelope with {summary, data, actions} shape, SummaryMeta
with sensitivity/degraded/currency metadata, DetailLevel enum,
and build_envelope() helper for consistent tool responses."
```

---

## Task 2: Sensitivity Enum & Privacy Middleware Stub

Add `Sensitivity` enum to `privacy.py` and a `log_tool_call()` function that logs the sensitivity tier. Consent checking is a no-op until privacy specs land.

**Files:**
- Modify: `src/moneybin/mcp/privacy.py`
- Test: `tests/moneybin/test_mcp/test_decorator.py` (partial — decorator tests come in Task 3)

- [ ] **Step 1: Write the failing tests**

```python
# tests/moneybin/test_mcp/test_decorator.py
"""Tests for MCP tool decorator and sensitivity middleware."""

import pytest

from moneybin.mcp.privacy import Sensitivity, log_tool_call


class TestSensitivity:
    """Tests for the Sensitivity enum."""

    @pytest.mark.unit
    def test_values(self) -> None:
        assert Sensitivity.LOW == "low"
        assert Sensitivity.MEDIUM == "medium"
        assert Sensitivity.HIGH == "high"

    @pytest.mark.unit
    def test_ordering(self) -> None:
        # Sensitivity levels should be comparable for middleware checks
        assert Sensitivity.LOW.value < Sensitivity.MEDIUM.value
        assert Sensitivity.MEDIUM.value < Sensitivity.HIGH.value


class TestLogToolCall:
    """Tests for the tool call logging stub."""

    @pytest.mark.unit
    def test_log_tool_call_returns_none(self, caplog: pytest.LogCaptureFixture) -> None:
        """log_tool_call is a stub — it logs but doesn't block."""
        with caplog.at_level("DEBUG"):
            result = log_tool_call("spending.summary", Sensitivity.LOW)
        assert result is None

    @pytest.mark.unit
    def test_log_tool_call_logs_sensitivity(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("DEBUG"):
            log_tool_call("transactions.search", Sensitivity.MEDIUM)
        assert "transactions.search" in caplog.text
        assert "medium" in caplog.text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_decorator.py::TestSensitivity -v`
Expected: FAIL — `ImportError: cannot import name 'Sensitivity'`

- [ ] **Step 3: Add Sensitivity enum and log_tool_call to privacy.py**

Add at the top of `src/moneybin/mcp/privacy.py`, after the existing imports:

```python
from enum import StrEnum


class Sensitivity(StrEnum):
    """Data sensitivity tier for MCP tools.

    Every tool declares its maximum data sensitivity. The privacy
    middleware uses this to enforce consent gates and response filtering.

    See ``mcp-architecture.md`` section 5 for tier definitions.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


def log_tool_call(tool_name: str, sensitivity: Sensitivity) -> None:
    """Log an MCP tool invocation with its sensitivity tier.

    This is a privacy middleware stub. In v1, it only logs.
    When the consent management and audit log specs are implemented,
    this will check consent status, apply redaction, and write to
    the audit table.

    Args:
        tool_name: The v1 dot-separated tool name.
        sensitivity: The tool's declared sensitivity tier.
    """
    logger.debug(f"MCP tool call: {tool_name} (sensitivity={sensitivity.value})")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_decorator.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/privacy.py tests/moneybin/test_mcp/test_decorator.py
git commit -m "feat: add Sensitivity enum and privacy middleware stub

Sensitivity enum (low/medium/high) for tool declarations.
log_tool_call() stub that logs the tool name and tier.
Consent checking is a no-op until privacy specs land."
```

---

## Task 3: MCP Tool Decorator

Create `@mcp_tool(sensitivity=...)` decorator that wraps FastMCP's `@mcp.tool()` with sensitivity logging and consistent response formatting.

**Files:**
- Create: `src/moneybin/mcp/decorator.py`
- Modify: `tests/moneybin/test_mcp/test_decorator.py` (add decorator tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/moneybin/test_mcp/test_decorator.py`:

```python
from unittest.mock import MagicMock, patch

from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.envelope import ResponseEnvelope, SummaryMeta


class TestMCPToolDecorator:
    """Tests for the @mcp_tool decorator."""

    @pytest.mark.unit
    def test_decorator_sets_sensitivity_attribute(self) -> None:
        @mcp_tool(sensitivity="low")
        def my_tool() -> str:
            return "result"

        assert my_tool._mcp_sensitivity == "low"  # type: ignore[attr-defined]

    @pytest.mark.unit
    def test_decorator_preserves_function_name(self) -> None:
        @mcp_tool(sensitivity="medium")
        def spending_summary() -> str:
            return "data"

        assert spending_summary.__name__ == "spending_summary"

    @pytest.mark.unit
    def test_decorator_calls_log_tool_call(self) -> None:
        @mcp_tool(sensitivity="medium")
        def my_tool() -> str:
            return "result"

        with patch("moneybin.mcp.decorator.log_tool_call") as mock_log:
            my_tool()
            mock_log.assert_called_once()
            args = mock_log.call_args[0]
            assert args[0] == "my_tool"
            assert args[1] == Sensitivity.MEDIUM

    @pytest.mark.unit
    def test_decorator_returns_json_when_envelope(self) -> None:
        """When a tool returns a ResponseEnvelope, decorator serializes to JSON."""

        @mcp_tool(sensitivity="low")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=1, returned_count=1),
                data=[{"value": 42}],
            )

        result = my_tool()
        assert isinstance(result, str)
        import json

        parsed = json.loads(result)
        assert "summary" in parsed
        assert "data" in parsed

    @pytest.mark.unit
    def test_decorator_passes_through_string(self) -> None:
        """When a tool returns a plain string, decorator passes it through."""

        @mcp_tool(sensitivity="low")
        def my_tool() -> str:
            return "plain string result"

        result = my_tool()
        assert result == "plain string result"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_decorator.py::TestMCPToolDecorator -v`
Expected: FAIL — `ImportError: cannot import name 'mcp_tool'`

- [ ] **Step 3: Implement the decorator**

```python
# src/moneybin/mcp/decorator.py
"""MCP tool decorator with sensitivity tier and privacy middleware.

Wraps tool functions with:
1. Sensitivity logging via the privacy middleware stub
2. Automatic JSON serialization of ResponseEnvelope returns
3. Tool name tracking for audit/debugging

Usage::

    @mcp_tool(sensitivity="medium")
    def spending_summary(months: int = 3) -> ResponseEnvelope:
        service = SpendingService(get_database())
        return service.summary(months).to_envelope()

The decorator does NOT register the tool with FastMCP — that happens
in the namespace registry. This separation lets us control which tools
are registered at connection time vs on-demand.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable

from moneybin.mcp.envelope import ResponseEnvelope
from moneybin.mcp.privacy import Sensitivity, log_tool_call

logger = logging.getLogger(__name__)


def mcp_tool(
    *,
    sensitivity: str,
) -> Callable[..., Any]:
    """Decorator that marks a function as an MCP tool with a sensitivity tier.

    Args:
        sensitivity: Data sensitivity tier (``"low"``, ``"medium"``, ``"high"``).

    Returns:
        Decorator that wraps the function with privacy logging and
        envelope serialization.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            log_tool_call(fn.__name__, tier)
            result = fn(*args, **kwargs)
            if isinstance(result, ResponseEnvelope):
                return result.to_json()
            return result

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        return wrapper

    return decorator
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_decorator.py -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/decorator.py tests/moneybin/test_mcp/test_decorator.py
git commit -m "feat: add @mcp_tool decorator with sensitivity and envelope serialization

Decorator wraps tool functions with privacy middleware logging
and automatic JSON serialization of ResponseEnvelope returns.
Does not register with FastMCP — namespace registry handles that."
```

---

## Task 4: Namespace Registry & Progressive Disclosure

Build the `NamespaceRegistry` that controls which tools are registered at connection time (core) vs loaded on-demand (extended) via `moneybin.discover`.

**Files:**
- Create: `src/moneybin/mcp/namespaces.py`
- Test: `tests/moneybin/test_mcp/test_namespaces.py`
- Modify: `src/moneybin/config.py` (add `core_namespaces` to `MCPConfig`)

- [ ] **Step 1: Write the failing tests**

```python
# tests/moneybin/test_mcp/test_namespaces.py
"""Tests for namespace registry and progressive disclosure."""

import pytest

from moneybin.mcp.namespaces import (
    CORE_NAMESPACES_DEFAULT,
    EXTENDED_NAMESPACES,
    NamespaceRegistry,
    ToolDefinition,
)


def _make_tool(name: str, description: str = "A test tool") -> ToolDefinition:
    """Create a ToolDefinition for testing."""
    return ToolDefinition(
        name=name,
        description=description,
        fn=lambda: None,
    )


class TestToolDefinition:
    """Tests for ToolDefinition."""

    @pytest.mark.unit
    def test_namespace_extraction(self) -> None:
        tool = _make_tool("spending.summary")
        assert tool.namespace == "spending"

    @pytest.mark.unit
    def test_namespace_three_level(self) -> None:
        tool = _make_tool("transactions.matches.pending")
        assert tool.namespace == "transactions.matches"

    @pytest.mark.unit
    def test_no_namespace_raises(self) -> None:
        with pytest.raises(ValueError, match="must contain a dot"):
            _make_tool("badname")


class TestNamespaceRegistry:
    """Tests for the NamespaceRegistry."""

    @pytest.mark.unit
    def test_register_tool(self) -> None:
        registry = NamespaceRegistry()
        tool = _make_tool("spending.summary")
        registry.register(tool)
        assert "spending" in registry.all_namespaces()

    @pytest.mark.unit
    def test_get_namespace_tools(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("spending.by_category"))
        tools = registry.get_namespace_tools("spending")
        assert len(tools) == 2
        assert {t.name for t in tools} == {"spending.summary", "spending.by_category"}

    @pytest.mark.unit
    def test_core_tools(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("categorize.bulk"))

        core = registry.get_core_tools(core_namespaces={"spending"})
        names = {t.name for t in core}
        assert "spending.summary" in names
        assert "categorize.bulk" not in names

    @pytest.mark.unit
    def test_extended_tools(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("categorize.bulk"))

        extended = registry.get_extended_namespaces(core_namespaces={"spending"})
        assert "categorize" in extended
        assert "spending" not in extended

    @pytest.mark.unit
    def test_namespace_description(self) -> None:
        registry = NamespaceRegistry()
        registry.set_namespace_description("spending", "Expense analysis")
        assert registry.get_namespace_description("spending") == "Expense analysis"

    @pytest.mark.unit
    def test_loaded_tracking(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("categorize.bulk"))
        assert not registry.is_loaded("categorize")
        registry.mark_loaded("categorize")
        assert registry.is_loaded("categorize")

    @pytest.mark.unit
    def test_tools_resource_data(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("categorize.bulk"))
        registry.set_namespace_description("spending", "Expense analysis")
        registry.set_namespace_description("categorize", "Categorization pipeline")

        core_ns = {"spending"}
        registry.mark_loaded("spending")
        data = registry.tools_resource_data(core_ns)

        assert len(data["core"]) == 1
        assert data["core"][0]["namespace"] == "spending"
        assert data["core"][0]["loaded"] is True
        assert len(data["extended"]) == 1
        assert data["extended"][0]["namespace"] == "categorize"
        assert data["extended"][0]["loaded"] is False


class TestNamespaceConstants:
    """Tests for namespace constant definitions."""

    @pytest.mark.unit
    def test_core_namespaces_defined(self) -> None:
        assert "spending" in CORE_NAMESPACES_DEFAULT
        assert "accounts" in CORE_NAMESPACES_DEFAULT
        assert "transactions" in CORE_NAMESPACES_DEFAULT
        assert "overview" in CORE_NAMESPACES_DEFAULT
        assert "import" in CORE_NAMESPACES_DEFAULT

    @pytest.mark.unit
    def test_extended_namespaces_defined(self) -> None:
        assert "categorize" in EXTENDED_NAMESPACES
        assert "budget" in EXTENDED_NAMESPACES
        assert "tax" in EXTENDED_NAMESPACES
        assert "privacy" in EXTENDED_NAMESPACES

    @pytest.mark.unit
    def test_no_overlap(self) -> None:
        overlap = CORE_NAMESPACES_DEFAULT & EXTENDED_NAMESPACES
        assert overlap == set(), f"Overlapping namespaces: {overlap}"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_namespaces.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the namespace registry**

```python
# src/moneybin/mcp/namespaces.py
"""Namespace registry for MCP progressive disclosure.

Tools are organized into namespaces (``spending``, ``accounts``, etc.).
Core namespaces are registered at connection time (~19 tools). Extended
namespaces are loaded on demand via ``moneybin.discover``.

See ``mcp-architecture.md`` section 3 for design rationale.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Default core namespaces — registered at connection time.
# The user can override via ``mcp.core_namespaces`` in profile config.
CORE_NAMESPACES_DEFAULT: frozenset[str] = frozenset({
    "overview",
    "spending",
    "cashflow",
    "accounts",
    "transactions",
    "import",
    "sql",
})

# Extended namespaces — loaded on demand via moneybin.discover.
EXTENDED_NAMESPACES: frozenset[str] = frozenset({
    "categorize",
    "budget",
    "tax",
    "privacy",
    "transactions.matches",
})

# Descriptions for each namespace (used in moneybin://tools resource).
_NAMESPACE_DESCRIPTIONS: dict[str, str] = {
    "overview": "Data status and financial health snapshot",
    "spending": "Expense analysis, trends, category breakdowns",
    "cashflow": "Income vs outflows, net cash position",
    "accounts": "Account listing, balances, net worth",
    "transactions": "Search, corrections, annotations, recurring",
    "import": "File import, status, format management",
    "sql": "Direct read-only SQL queries",
    "categorize": "Rules, merchant mappings, bulk categorization",
    "budget": "Budget targets, status, rollovers",
    "tax": "W-2 data, deductible expense search",
    "privacy": "Consent status, grants, revocations, audit log",
    "transactions.matches": "Match review workflow",
}


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """A registered MCP tool with its metadata.

    Attributes:
        name: Dot-separated tool name (e.g., ``spending.summary``).
        description: Tool description for AI consumers.
        fn: The tool function (decorated with ``@mcp_tool``).
    """

    name: str
    description: str
    fn: Callable[..., Any]

    def __post_init__(self) -> None:
        if "." not in self.name:
            raise ValueError(
                f"Tool name '{self.name}' must contain a dot (namespace.action)"
            )

    @property
    def namespace(self) -> str:
        """Extract the namespace from the tool name.

        For two-level names like ``spending.summary``, returns ``spending``.
        For three-level names like ``transactions.matches.pending``,
        returns ``transactions.matches``.
        """
        parts = self.name.rsplit(".", 1)
        return parts[0]


class NamespaceRegistry:
    """Registry of all MCP tools organized by namespace.

    Tracks which namespaces are loaded (registered with FastMCP)
    vs available but unloaded.
    """

    def __init__(self) -> None:
        self._tools: dict[str, list[ToolDefinition]] = {}
        self._loaded: set[str] = set()
        self._descriptions: dict[str, str] = dict(_NAMESPACE_DESCRIPTIONS)

    def register(self, tool: ToolDefinition) -> None:
        """Register a tool definition (does not register with FastMCP).

        Args:
            tool: The tool definition to register.
        """
        ns = tool.namespace
        if ns not in self._tools:
            self._tools[ns] = []
        self._tools[ns].append(tool)

    def all_namespaces(self) -> set[str]:
        """Return all registered namespace names."""
        return set(self._tools.keys())

    def get_namespace_tools(self, namespace: str) -> list[ToolDefinition]:
        """Get all tools in a namespace.

        Args:
            namespace: The namespace to look up.

        Returns:
            List of ToolDefinitions, or empty list if namespace not found.
        """
        return self._tools.get(namespace, [])

    def get_core_tools(
        self, core_namespaces: set[str] | frozenset[str]
    ) -> list[ToolDefinition]:
        """Get all tools that belong to core namespaces.

        Args:
            core_namespaces: Set of namespace names considered core.

        Returns:
            Flat list of ToolDefinitions from core namespaces.
        """
        tools: list[ToolDefinition] = []
        for ns in core_namespaces:
            tools.extend(self._tools.get(ns, []))
        return tools

    def get_extended_namespaces(
        self, core_namespaces: set[str] | frozenset[str]
    ) -> set[str]:
        """Get namespace names that are not in the core set.

        Args:
            core_namespaces: Set of namespace names considered core.

        Returns:
            Set of extended namespace names.
        """
        return self.all_namespaces() - set(core_namespaces)

    def set_namespace_description(self, namespace: str, description: str) -> None:
        """Set the description for a namespace.

        Args:
            namespace: The namespace name.
            description: One-line description.
        """
        self._descriptions[namespace] = description

    def get_namespace_description(self, namespace: str) -> str:
        """Get the description for a namespace.

        Args:
            namespace: The namespace name.

        Returns:
            Description string, or empty string if not set.
        """
        return self._descriptions.get(namespace, "")

    def is_loaded(self, namespace: str) -> bool:
        """Check if a namespace has been loaded (registered with FastMCP).

        Args:
            namespace: The namespace name.

        Returns:
            True if the namespace is loaded.
        """
        return namespace in self._loaded

    def mark_loaded(self, namespace: str) -> None:
        """Mark a namespace as loaded.

        Args:
            namespace: The namespace name.
        """
        self._loaded.add(namespace)

    def tools_resource_data(
        self, core_namespaces: set[str] | frozenset[str]
    ) -> dict[str, list[dict[str, Any]]]:
        """Build the data payload for the ``moneybin://tools`` resource.

        Args:
            core_namespaces: Set of namespace names considered core.

        Returns:
            Dict with ``core`` and ``extended`` keys, each containing
            lists of namespace metadata dicts.
        """
        core_list: list[dict[str, Any]] = []
        extended_list: list[dict[str, Any]] = []

        for ns in sorted(self.all_namespaces()):
            entry = {
                "namespace": ns,
                "tools": len(self._tools.get(ns, [])),
                "loaded": self.is_loaded(ns),
                "description": self.get_namespace_description(ns),
            }
            if ns in core_namespaces:
                core_list.append(entry)
            else:
                extended_list.append(entry)

        return {
            "core": core_list,
            "extended": extended_list,
            "discover_tool": "moneybin.discover",
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_namespaces.py -v`
Expected: All 12 tests PASS

- [ ] **Step 5: Add `core_namespaces` to MCPConfig**

In `src/moneybin/config.py`, add to the `MCPConfig` class:

```python
    core_namespaces: list[str] | None = Field(
        default=None,
        description=(
            "MCP namespaces registered at connection time. "
            "None uses the default core set. Set to ['*'] to load all tools."
        ),
    )
```

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/mcp/namespaces.py tests/moneybin/test_mcp/test_namespaces.py src/moneybin/config.py
git commit -m "feat: add namespace registry for MCP progressive disclosure

NamespaceRegistry tracks tools by namespace, core vs extended sets,
and loaded status. Supports moneybin://tools resource data.
MCPConfig gains core_namespaces config for user customization."
```

---

## Task 5: Exemplar Service — SpendingService

Build `SpendingService` with `summary()` and `by_category()` to prove the service layer pattern end-to-end. This is the template all other services will follow.

**Files:**
- Create: `src/moneybin/services/spending_service.py`
- Test: `tests/moneybin/test_services/test_spending_service.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/moneybin/test_services/test_spending_service.py
"""Tests for SpendingService."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.spending_service import (
    CategoryBreakdown,
    MonthlySpending,
    SpendingService,
    SpendingSummary,
)
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def spending_db(tmp_path: Path) -> Generator[Database, None, None]:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert test transactions spanning 3 months
    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('T1', 'A1', '2026-04-10', -50.00, 50.00, 'expense', 'Coffee', 'DEBIT', false, 'USD', 'ofx', '2026-04-10', CURRENT_TIMESTAMP, 2026, 4, 10, 3, '2026-04', '2026-Q2'),
        ('T2', 'A1', '2026-04-15', 5000.00, 5000.00, 'income', 'Payroll', 'CREDIT', false, 'USD', 'ofx', '2026-04-15', CURRENT_TIMESTAMP, 2026, 4, 15, 1, '2026-04', '2026-Q2'),
        ('T3', 'A1', '2026-03-10', -200.00, 200.00, 'expense', 'Groceries', 'DEBIT', false, 'USD', 'ofx', '2026-03-10', CURRENT_TIMESTAMP, 2026, 3, 10, 1, '2026-03', '2026-Q1'),
        ('T4', 'A1', '2026-03-20', 5000.00, 5000.00, 'income', 'Payroll', 'CREDIT', false, 'USD', 'ofx', '2026-03-20', CURRENT_TIMESTAMP, 2026, 3, 20, 4, '2026-03', '2026-Q1')
    """)

    # Insert transaction_categories for by_category tests
    conn.execute("""
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at, categorized_by)
        VALUES
        ('T1', 'Food & Drink', 'Coffee Shops', CURRENT_TIMESTAMP, 'user'),
        ('T3', 'Food & Drink', 'Groceries', CURRENT_TIMESTAMP, 'user')
    """)

    db_module._database_instance = database  # type: ignore[reportPrivateUsage]
    yield database
    db_module._database_instance = None  # type: ignore[reportPrivateUsage]
    database.close()


class TestSpendingSummary:
    """Tests for SpendingService.summary()."""

    @pytest.mark.unit
    def test_returns_monthly_data(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=3)
        assert isinstance(result, SpendingSummary)
        assert len(result.months) >= 2

    @pytest.mark.unit
    def test_monthly_spending_fields(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=3)
        month = result.months[0]
        assert isinstance(month, MonthlySpending)
        assert hasattr(month, "period")
        assert hasattr(month, "income")
        assert hasattr(month, "expenses")
        assert hasattr(month, "net")
        assert hasattr(month, "transaction_count")

    @pytest.mark.unit
    def test_to_envelope_structure(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
        assert isinstance(d["data"], list)
        assert len(d["actions"]) > 0


class TestSpendingByCategory:
    """Tests for SpendingService.by_category()."""

    @pytest.mark.unit
    def test_returns_category_breakdown(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.by_category(months=3)
        assert isinstance(result, CategoryBreakdown)
        assert len(result.categories) > 0

    @pytest.mark.unit
    def test_to_envelope_structure(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.by_category(months=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement SpendingService**

```python
# src/moneybin/services/spending_service.py
"""Spending analysis service.

Business logic for income vs expense summaries, category breakdowns,
merchant analysis, and period comparisons. Consumed by both MCP tools
and CLI commands.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from moneybin.database import Database
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MonthlySpending:
    """Income vs expense totals for a single month."""

    period: str
    income: float
    expenses: float
    net: float
    transaction_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "period": self.period,
            "income": self.income,
            "expenses": self.expenses,
            "net": self.net,
            "transaction_count": self.transaction_count,
        }


@dataclass(slots=True)
class SpendingSummary:
    """Result of spending summary query."""

    months: list[MonthlySpending]
    period_label: str = ""

    def to_envelope(self) -> ResponseEnvelope:
        return build_envelope(
            data=[m.to_dict() for m in self.months],
            sensitivity="low",
            period=self.period_label,
            actions=[
                "Use spending.by_category for category breakdown",
                "Use spending.compare to compare periods",
            ],
        )


@dataclass(frozen=True, slots=True)
class CategorySpending:
    """Spending total for a single category."""

    category: str
    subcategory: str | None
    total: float
    transaction_count: int
    percent_of_total: float

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "category": self.category,
            "total": self.total,
            "transaction_count": self.transaction_count,
            "percent_of_total": self.percent_of_total,
        }
        if self.subcategory:
            d["subcategory"] = self.subcategory
        return d


@dataclass(slots=True)
class CategoryBreakdown:
    """Result of spending-by-category query."""

    categories: list[CategorySpending]
    period_label: str = ""

    def to_envelope(self) -> ResponseEnvelope:
        return build_envelope(
            data=[c.to_dict() for c in self.categories],
            sensitivity="low",
            period=self.period_label,
            actions=[
                "Use spending.merchants for merchant-level breakdown",
                "Use transactions.search to see individual transactions in a category",
            ],
        )


class SpendingService:
    """Spending analysis operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    MCP tools call ``to_envelope().to_json()``. CLI commands render the
    dataclass directly as a table or call ``to_envelope().to_json()``
    for ``--output json``.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def summary(
        self,
        months: int = 3,
        start_date: str | None = None,
        end_date: str | None = None,
        account_id: list[str] | None = None,
    ) -> SpendingSummary:
        """Get income vs expense totals by month.

        Args:
            months: Number of recent months to include.
            start_date: ISO 8601 start date (overrides months).
            end_date: ISO 8601 end date.
            account_id: Filter to specific accounts.

        Returns:
            SpendingSummary with monthly breakdown.
        """
        conditions: list[str] = []
        params: list[object] = []

        if start_date:
            conditions.append("transaction_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("transaction_date <= ?")
            params.append(end_date)
        if account_id:
            placeholders = ", ".join("?" for _ in account_id)
            conditions.append(f"account_id IN ({placeholders})")
            params.extend(account_id)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        if not start_date:
            # Use months-based lookback
            params.append(months)
            limit_clause = "LIMIT ?"
        else:
            limit_clause = ""

        sql = f"""
            SELECT
                transaction_year_month AS period,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS expenses,
                SUM(amount) AS net,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name}
            {where}
            GROUP BY transaction_year_month
            ORDER BY transaction_year_month DESC
            {limit_clause}
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        monthly = [
            MonthlySpending(
                period=str(row[0]),
                income=float(row[1]),
                expenses=float(row[2]),
                net=float(row[3]),
                transaction_count=int(row[4]),
            )
            for row in rows
        ]

        period_label = ""
        if monthly:
            first = monthly[-1].period
            last = monthly[0].period
            period_label = f"{first} to {last}" if first != last else first

        return SpendingSummary(months=monthly, period_label=period_label)

    def by_category(
        self,
        months: int = 3,
        start_date: str | None = None,
        end_date: str | None = None,
        account_id: list[str] | None = None,
        top_n: int = 10,
        include_uncategorized: bool = True,
    ) -> CategoryBreakdown:
        """Get spending broken down by category.

        Args:
            months: Number of recent months to include.
            start_date: ISO 8601 start date (overrides months).
            end_date: ISO 8601 end date.
            account_id: Filter to specific accounts.
            top_n: Limit to top N categories.
            include_uncategorized: Include uncategorized rollup row.

        Returns:
            CategoryBreakdown with per-category totals.
        """
        conditions: list[str] = ["t.amount < 0"]
        params: list[object] = []

        if start_date:
            conditions.append("t.transaction_date >= ?")
            params.append(start_date)
        else:
            conditions.append(
                f"t.transaction_year_month >= strftime(CURRENT_DATE - INTERVAL '{months} months', '%Y-%m')"
            )
        if end_date:
            conditions.append("t.transaction_date <= ?")
            params.append(end_date)
        if account_id:
            placeholders = ", ".join("?" for _ in account_id)
            conditions.append(f"t.account_id IN ({placeholders})")
            params.extend(account_id)

        where = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                COALESCE(c.category, 'Uncategorized') AS category,
                c.subcategory,
                SUM(ABS(t.amount)) AS total,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
            GROUP BY COALESCE(c.category, 'Uncategorized'), c.subcategory
            ORDER BY total DESC
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        grand_total = sum(float(row[2]) for row in rows) or 1.0
        categories = []
        for row in rows:
            cat_name = str(row[0])
            if not include_uncategorized and cat_name == "Uncategorized":
                continue
            categories.append(
                CategorySpending(
                    category=cat_name,
                    subcategory=row[1],
                    total=float(row[2]),
                    transaction_count=int(row[3]),
                    percent_of_total=round(float(row[2]) / grand_total * 100, 1),
                )
            )

        if top_n and len(categories) > top_n:
            categories = categories[:top_n]

        return CategoryBreakdown(categories=categories)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/spending_service.py tests/moneybin/test_services/test_spending_service.py
git commit -m "feat: add SpendingService as exemplar service layer class

SpendingService.summary() and by_category() return typed dataclasses
with to_envelope() for consistent response shape. Proves the service
layer pattern end-to-end: SQL in service, typed returns, envelope wrapping."
```

---

## Task 6: Exemplar MCP Tools — `spending.summary` and `spending.by_category`

Wire the SpendingService into v1 MCP tools using the decorator and namespace registry.

**Files:**
- Create: `src/moneybin/mcp/tools/__init__.py`
- Create: `src/moneybin/mcp/tools/spending.py`
- Test: `tests/moneybin/test_mcp/test_v1_tools.py`

- [ ] **Step 1: Create the tools package init**

```python
# src/moneybin/mcp/tools/__init__.py
"""V1 MCP tool implementations organized by namespace."""
```

- [ ] **Step 2: Write the failing tests**

```python
# tests/moneybin/test_mcp/test_v1_tools.py
"""Tests for v1 MCP tools."""

import json

import pytest

from moneybin.mcp.tools.spending import register_spending_tools
from moneybin.mcp.namespaces import NamespaceRegistry

pytestmark = pytest.mark.usefixtures("mcp_db")

_INSERT_TRANSACTIONS = """
    INSERT INTO core.fct_transactions (
        transaction_id, account_id, transaction_date, amount,
        amount_absolute, transaction_direction, description,
        transaction_type, is_pending, currency_code, source_type,
        source_extracted_at, loaded_at,
        transaction_year, transaction_month, transaction_day,
        transaction_day_of_week, transaction_year_month, transaction_year_quarter
    ) VALUES
    ('T1', 'ACC001', '2026-04-10', -50.00, 50.00, 'expense', 'Coffee Shop', 'DEBIT', false, 'USD', 'ofx', '2026-04-10', CURRENT_TIMESTAMP, 2026, 4, 10, 3, '2026-04', '2026-Q2'),
    ('T2', 'ACC001', '2026-04-15', 5000.00, 5000.00, 'income', 'Employer Inc', 'CREDIT', false, 'USD', 'ofx', '2026-04-15', CURRENT_TIMESTAMP, 2026, 4, 15, 1, '2026-04', '2026-Q2')
"""


class TestSpendingSummaryTool:
    """Tests for spending.summary v1 tool."""

    def _insert_data(self, mcp_db: object) -> None:
        from moneybin.mcp.server import get_db

        get_db().execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_returns_envelope(self, mcp_db: object) -> None:
        self._insert_data(mcp_db)
        registry = NamespaceRegistry()
        tools = register_spending_tools(registry)
        # Find spending.summary
        summary_tool = next(t for t in tools if t.name == "spending.summary")
        result = summary_tool.fn(months=3)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_data_shape(self, mcp_db: object) -> None:
        self._insert_data(mcp_db)
        registry = NamespaceRegistry()
        tools = register_spending_tools(registry)
        summary_tool = next(t for t in tools if t.name == "spending.summary")
        parsed = json.loads(summary_tool.fn(months=3))
        data = parsed["data"]
        assert len(data) >= 1
        assert "period" in data[0]
        assert "income" in data[0]
        assert "expenses" in data[0]
        assert "net" in data[0]
        assert "transaction_count" in data[0]
```

- [ ] **Step 3: Implement the spending tools module**

```python
# src/moneybin/mcp/tools/spending.py
"""Spending namespace tools — expense analysis, trends, category breakdowns.

Tools:
    - spending.summary — Income vs expense totals by month (low sensitivity)
    - spending.by_category — Spending by category for a period (low sensitivity)
"""

from __future__ import annotations

import logging

from moneybin.database import get_database
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.envelope import ResponseEnvelope
from moneybin.mcp.namespaces import NamespaceRegistry, ToolDefinition
from moneybin.services.spending_service import SpendingService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low")
def spending_summary(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
) -> ResponseEnvelope:
    """Get income vs expense totals by month.

    Returns time-series data suitable for charting. Use ``months`` for
    recent history or ``start_date``/``end_date`` for a specific range.
    """
    service = SpendingService(get_database())
    result = service.summary(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )
    return result.to_envelope()


@mcp_tool(sensitivity="low")
def spending_by_category(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 10,
    include_uncategorized: bool = True,
) -> ResponseEnvelope:
    """Get spending breakdown by category for a period.

    Requires transactions to be categorized. Use ``categorize.uncategorized``
    and ``categorize.bulk`` to categorize transactions first.
    """
    service = SpendingService(get_database())
    result = service.by_category(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        top_n=top_n,
        include_uncategorized=include_uncategorized,
    )
    return result.to_envelope()


def register_spending_tools(registry: NamespaceRegistry) -> list[ToolDefinition]:
    """Register all spending namespace tools with the registry.

    Args:
        registry: The namespace registry to register tools into.

    Returns:
        List of registered ToolDefinitions.
    """
    tools = [
        ToolDefinition(
            name="spending.summary",
            description=(
                "Get income vs expense totals by month. Returns time-series "
                "data suitable for charting."
            ),
            fn=spending_summary,
        ),
        ToolDefinition(
            name="spending.by_category",
            description=(
                "Get spending breakdown by category for a period. "
                "Requires transactions to be categorized."
            ),
            fn=spending_by_category,
        ),
    ]
    for tool in tools:
        registry.register(tool)
    return tools
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_v1_tools.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/tools/__init__.py src/moneybin/mcp/tools/spending.py tests/moneybin/test_mcp/test_v1_tools.py
git commit -m "feat: add spending.summary and spending.by_category v1 MCP tools

Exemplar v1 tools proving the full pattern: service layer -> typed
dataclass -> ResponseEnvelope -> JSON. Tools use @mcp_tool decorator
and register via NamespaceRegistry."
```

---

## Task 7: CLI `--output json` Infrastructure

Build the `--output json` option factory so CLI commands can return the same response envelope as MCP tools.

**Files:**
- Create: `src/moneybin/cli/output.py`
- Test: integration tested via existing CLI test patterns

- [ ] **Step 1: Implement the output module**

```python
# src/moneybin/cli/output.py
"""CLI output format support.

Provides ``--output json`` on all CLI commands that have a corresponding
MCP tool. When ``json`` is selected, the command returns the same
``{summary, data, actions}`` response envelope as the MCP tool.

Usage in a CLI command::

    from moneybin.cli.output import OutputFormat, output_option, render_or_json

    @app.command("summary")
    def summary_cmd(
        months: int = typer.Option(3),
        output: OutputFormat = output_option,
    ) -> None:
        service = SpendingService(get_database())
        result = service.summary(months=months)
        render_or_json(result.to_envelope(), output, render_fn=_render_table)
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import Any, Callable

import typer

from moneybin.mcp.envelope import ResponseEnvelope

logger = logging.getLogger(__name__)


class OutputFormat(StrEnum):
    """CLI output format."""

    TABLE = "table"
    JSON = "json"


output_option: OutputFormat = typer.Option(
    OutputFormat.TABLE,
    "--output",
    "-o",
    help="Output format: 'table' (human-readable) or 'json' (response envelope).",
)


def render_or_json(
    envelope: ResponseEnvelope,
    output: OutputFormat,
    render_fn: Callable[[ResponseEnvelope], None] | None = None,
) -> None:
    """Render a response envelope as a table or JSON.

    Args:
        envelope: The response envelope to render.
        output: The output format.
        render_fn: Function to render the envelope as a human-readable table.
            If None, falls back to printing the JSON.
    """
    if output == OutputFormat.JSON:
        typer.echo(envelope.to_json())
    elif render_fn is not None:
        render_fn(envelope)
    else:
        typer.echo(envelope.to_json())
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/cli/output.py
git commit -m "feat: add CLI --output json infrastructure

OutputFormat enum, output_option factory, and render_or_json() helper.
CLI commands can return the same response envelope as MCP tools."
```

---

## Task 8: Account, Transaction, Budget, Tax Services

Build the remaining services needed for prototype migration. Each follows the SpendingService pattern: typed dataclasses with `to_envelope()`.

**Files:**
- Create: `src/moneybin/services/account_service.py`
- Create: `src/moneybin/services/transaction_service.py`
- Create: `src/moneybin/services/budget_service.py`
- Create: `src/moneybin/services/tax_service.py`
- Test: `tests/moneybin/test_services/test_account_service.py`
- Test: `tests/moneybin/test_services/test_transaction_service.py`
- Test: `tests/moneybin/test_services/test_budget_service.py`
- Test: `tests/moneybin/test_services/test_tax_service.py`

This is a large task. The services follow the same mechanical pattern as SpendingService. Each service:
1. Takes a `Database` instance
2. Has methods that build parameterized SQL, execute, and return typed dataclasses
3. Dataclasses have `to_envelope()` → `ResponseEnvelope`

- [ ] **Step 1: Write AccountService tests**

```python
# tests/moneybin/test_services/test_account_service.py
"""Tests for AccountService."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.account_service import AccountService
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def account_db(tmp_path: Path) -> Generator[Database, None, None]:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    conn = database.conn
    create_core_tables_raw(conn)
    conn.execute("""
        INSERT INTO core.dim_accounts VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx', 'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx', 'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)
    conn.execute("""
        INSERT INTO raw.ofx_balances VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00, '2025-06-30', 4800.00, 'test.qfx', '2025-01-24', CURRENT_TIMESTAMP),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00, '2025-06-30', 15000.00, 'other.qfx', '2025-01-24', CURRENT_TIMESTAMP)
    """)
    db_module._database_instance = database  # type: ignore[reportPrivateUsage]
    yield database
    db_module._database_instance = None  # type: ignore[reportPrivateUsage]
    database.close()


class TestAccountList:
    @pytest.mark.unit
    def test_returns_accounts(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert len(d["data"]) == 2
        assert d["summary"]["sensitivity"] == "low"


class TestAccountBalances:
    @pytest.mark.unit
    def test_returns_balances(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.balances()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert len(d["data"]) == 2
        assert d["summary"]["sensitivity"] == "medium"
```

- [ ] **Step 2: Implement AccountService**

```python
# src/moneybin/services/account_service.py
"""Account management service."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import DIM_ACCOUNTS, OFX_BALANCES

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Account:
    account_id: str
    account_type: str | None
    institution_name: str | None
    source_type: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "account_type": self.account_type,
            "institution_name": self.institution_name,
            "source_type": self.source_type,
        }


@dataclass(slots=True)
class AccountListResult:
    accounts: list[Account]

    def to_envelope(self) -> ResponseEnvelope:
        return build_envelope(
            data=[a.to_dict() for a in self.accounts],
            sensitivity="low",
            actions=["Use accounts.balances for current balances"],
        )


@dataclass(frozen=True, slots=True)
class AccountBalance:
    account_id: str
    institution_name: str | None
    account_type: str | None
    ledger_balance: float | None
    available_balance: float | None
    as_of_date: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "institution_name": self.institution_name,
            "account_type": self.account_type,
            "ledger_balance": self.ledger_balance,
            "available_balance": self.available_balance,
            "as_of_date": self.as_of_date,
        }


@dataclass(slots=True)
class BalanceListResult:
    balances: list[AccountBalance]

    def to_envelope(self) -> ResponseEnvelope:
        return build_envelope(
            data=[b.to_dict() for b in self.balances],
            sensitivity="medium",
            actions=["Use accounts.details for full account info"],
        )


class AccountService:
    def __init__(self, db: Database) -> None:
        self._db = db

    def list_accounts(self) -> AccountListResult:
        rows = self._db.execute(f"""
            SELECT account_id, account_type, institution_name, source_type
            FROM {DIM_ACCOUNTS.full_name}
            ORDER BY institution_name, account_type
        """).fetchall()
        return AccountListResult(
            accounts=[
                Account(
                    account_id=str(r[0]),
                    account_type=r[1],
                    institution_name=r[2],
                    source_type=r[3],
                )
                for r in rows
            ]
        )

    def balances(self, account_id: str | None = None) -> BalanceListResult:
        conditions: list[str] = []
        params: list[object] = []
        if account_id:
            conditions.append("b.account_id = ?")
            params.append(account_id)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        rows = self._db.execute(
            f"""
            WITH latest AS (
                SELECT account_id, ledger_balance, available_balance,
                       ledger_balance_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY account_id
                           ORDER BY ledger_balance_date DESC
                       ) AS rn
                FROM {OFX_BALANCES.full_name}
                {where}
            )
            SELECT b.account_id, a.institution_name, a.account_type,
                   b.ledger_balance, b.available_balance, b.ledger_balance_date
            FROM latest b
            LEFT JOIN {DIM_ACCOUNTS.full_name} a ON b.account_id = a.account_id
            WHERE b.rn = 1
            ORDER BY b.account_id
        """,
            params or None,
        ).fetchall()
        return BalanceListResult(
            balances=[
                AccountBalance(
                    account_id=str(r[0]),
                    institution_name=r[1],
                    account_type=r[2],
                    ledger_balance=float(r[3]) if r[3] is not None else None,
                    available_balance=float(r[4]) if r[4] is not None else None,
                    as_of_date=str(r[5]) if r[5] is not None else None,
                )
                for r in rows
            ]
        )
```

- [ ] **Step 3: Write and implement TransactionService, BudgetService, TaxService**

Follow the same pattern for each. `TransactionService` wraps `query_transactions` and `find_recurring_transactions` SQL. `BudgetService` wraps `set_budget` and `get_budget_status`. `TaxService` wraps `get_w2_summary`.

Each service file follows this template:
1. Typed dataclass results with `to_dict()` and container with `to_envelope()`
2. Service class with `__init__(self, db: Database)` and query methods
3. Parameterized SQL only, returns typed objects

Create `src/moneybin/services/transaction_service.py`, `src/moneybin/services/budget_service.py`, `src/moneybin/services/tax_service.py` and corresponding test files following the exact same pattern as `AccountService` above.

For **TransactionService**, the key methods are:
- `search()` — migrates `query_transactions` logic with v1 parameter names (`description` instead of `payee_pattern`, `category` filter, `uncategorized_only`, `offset`)
- `recurring()` — migrates `find_recurring_transactions` logic

For **BudgetService**, the key methods are:
- `set_budget()` — migrates `set_budget` logic (upsert)
- `status()` — migrates `get_budget_status` logic

For **TaxService**, the key method is:
- `w2()` — migrates `get_w2_summary` logic

- [ ] **Step 4: Run all service tests**

Run: `uv run pytest tests/moneybin/test_services/ -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/account_service.py src/moneybin/services/transaction_service.py src/moneybin/services/budget_service.py src/moneybin/services/tax_service.py tests/moneybin/test_services/test_account_service.py tests/moneybin/test_services/test_transaction_service.py tests/moneybin/test_services/test_budget_service.py tests/moneybin/test_services/test_tax_service.py
git commit -m "feat: add account, transaction, budget, and tax services

Each service follows the SpendingService pattern: typed dataclass
results with to_envelope(), parameterized SQL, Database dependency."
```

---

## Task 9: Evolve CategorizationService with Typed Returns

The existing `CategorizationService` in `src/moneybin/services/categorization_service.py` already has business logic but returns raw dicts and ints. Add typed return dataclasses with `to_envelope()` methods without breaking existing callers.

**Files:**
- Modify: `src/moneybin/services/categorization_service.py`
- Test: `tests/moneybin/test_services/test_categorization_service.py` (extend)

- [ ] **Step 1: Add typed result dataclasses to categorization_service.py**

Add after the existing imports at the top of `src/moneybin/services/categorization_service.py`:

```python
from dataclasses import dataclass
from typing import Any

from moneybin.mcp.envelope import ResponseEnvelope, build_envelope


@dataclass(slots=True)
class CategorizationStats:
    total: int
    categorized: int
    uncategorized: int
    percent_categorized: float
    by_source: dict[str, int]

    def to_envelope(self) -> ResponseEnvelope:
        data = {
            "total_transactions": self.total,
            "categorized": self.categorized,
            "uncategorized": self.uncategorized,
            "percent_categorized": self.percent_categorized,
            "by_source": self.by_source,
        }
        return build_envelope(
            data=data,
            sensitivity="low",
            actions=["Use categorize.uncategorized to see uncategorized transactions"],
        )


@dataclass(slots=True)
class BulkCategorizationResult:
    applied: int
    skipped: int
    errors: int
    error_details: list[dict[str, str]]
    merchants_created: int = 0

    def to_envelope(self, input_count: int) -> ResponseEnvelope:
        return build_envelope(
            data={
                "applied": self.applied,
                "skipped": self.skipped,
                "errors": self.errors,
                "error_details": self.error_details,
                "merchants_created": self.merchants_created,
            },
            sensitivity="medium",
            total_count=input_count,
            actions=[
                "Use categorize.rules to review auto-created rules",
                "Use categorize.uncategorized to fetch the next batch",
            ],
        )


@dataclass(slots=True)
class SeedResult:
    seeded_count: int

    def to_envelope(self) -> ResponseEnvelope:
        return build_envelope(
            data={"seeded_count": self.seeded_count},
            sensitivity="low",
        )
```

- [ ] **Step 2: Add a `get_stats()` method that returns typed `CategorizationStats`**

Add a new method to the module (don't modify `get_categorization_stats` to avoid breaking existing callers):

```python
def get_stats(db: Database) -> CategorizationStats:
    """Get categorization stats as a typed result.

    Wrapper around get_categorization_stats() that returns a typed object.
    """
    raw = get_categorization_stats(db)
    by_source = {
        k.removeprefix("by_"): v
        for k, v in raw.items()
        if k.startswith("by_") and isinstance(v, int)
    }
    return CategorizationStats(
        total=int(raw["total"]),
        categorized=int(raw["categorized"]),
        uncategorized=int(raw["uncategorized"]),
        percent_categorized=float(raw["pct_categorized"]),
        by_source=by_source,
    )
```

- [ ] **Step 3: Run existing categorization service tests to ensure nothing breaks**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v`
Expected: All existing tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/services/categorization_service.py
git commit -m "feat: add typed return dataclasses to CategorizationService

CategorizationStats, BulkCategorizationResult, SeedResult with
to_envelope() methods. Existing functions preserved for backwards
compatibility; new get_stats() returns typed CategorizationStats."
```

---

## Task 10: V1 Tool Modules — All Namespaces

Create the v1 tool modules for each namespace. Each module defines `@mcp_tool`-decorated functions and a `register_*_tools()` function.

**Files:**
- Create: `src/moneybin/mcp/tools/accounts.py`
- Create: `src/moneybin/mcp/tools/transactions.py`
- Create: `src/moneybin/mcp/tools/import_tools.py`
- Create: `src/moneybin/mcp/tools/categorize.py`
- Create: `src/moneybin/mcp/tools/budget.py`
- Create: `src/moneybin/mcp/tools/tax.py`
- Create: `src/moneybin/mcp/tools/sql.py`
- Create: `src/moneybin/mcp/tools/discover.py`

Each tool module follows the `spending.py` pattern:
1. Import the service
2. Define `@mcp_tool(sensitivity=...)` decorated functions
3. Each function calls the service and returns `result.to_envelope()`
4. `register_*_tools()` creates `ToolDefinition` objects and registers them

- [ ] **Step 1: Create accounts.py**

Pattern: `accounts_list()` calls `AccountService.list_accounts()`, `accounts_balances()` calls `AccountService.balances()`. Register as `accounts.list` and `accounts.balances`.

- [ ] **Step 2: Create transactions.py**

Pattern: `transactions_search()` calls `TransactionService.search()`, `transactions_recurring()` calls `TransactionService.recurring()`. Register as `transactions.search` and `transactions.recurring`.

- [ ] **Step 3: Create import_tools.py**

Migrate `import_file`, `import_preview`→`import.csv_preview`, `import_history`→`import.status`, `list_formats`→`import.list_formats`. These are thinner wrappers since `ImportService` already exists — wrap the existing `import_file()` function call and return results as envelopes.

- [ ] **Step 4: Create categorize.py**

Migrate all 12 categorization tools. The bulk operations (`categorize.bulk`, `categorize.create_rules`, `categorize.create_merchants`) absorb their single-item equivalents. The existing service functions are called and results wrapped in envelopes.

This is the largest tool module (12 tools). Follow the same pattern for each:
```python
@mcp_tool(sensitivity="low")
def categorize_categories(include_inactive: bool = False) -> ResponseEnvelope:
    """List the category taxonomy."""
    db = get_database()
    cats = get_active_categories(db)  # existing function
    return build_envelope(data=cats, sensitivity="low", ...)
```

- [ ] **Step 5: Create budget.py, tax.py, sql.py**

Each follows the same pattern. `sql.py` is the simplest — wraps `validate_read_only_query()` and `_query_to_json()` into an envelope.

- [ ] **Step 6: Create discover.py**

The `moneybin.discover` meta-tool. Registers tools from a namespace on demand:

```python
@mcp_tool(sensitivity="low")
def moneybin_discover(namespace: str) -> ResponseEnvelope:
    """Load tools from a namespace."""
    from moneybin.mcp.server import mcp

    registry = get_registry()
    tools = registry.get_namespace_tools(namespace)
    if not tools:
        return build_envelope(
            data={"namespace": namespace, "error": f"Unknown namespace: {namespace}"},
            sensitivity="low",
        )
    if not registry.is_loaded(namespace):
        for tool in tools:
            mcp.tool(name=tool.name, description=tool.description)(tool.fn)
        registry.mark_loaded(namespace)
        # Note: tools/list_changed notification requires FastMCP support
    return build_envelope(
        data={
            "namespace": namespace,
            "tools_loaded": [
                {"name": t.name, "description": t.description} for t in tools
            ],
            "already_loaded": registry.is_loaded(namespace),
        },
        sensitivity="low",
    )
```

- [ ] **Step 7: Run all tool tests**

Run: `uv run pytest tests/moneybin/test_mcp/test_v1_tools.py -v`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/mcp/tools/
git commit -m "feat: add v1 MCP tool modules for all namespaces

accounts, transactions, import, categorize, budget, tax, sql, and
discover tool modules. Each uses @mcp_tool decorator, calls service
layer, returns ResponseEnvelope. moneybin.discover enables progressive
disclosure via on-demand namespace loading."
```

---

## Task 11: V1 Resources

Rewrite `resources.py` with the 5 v1 resource URIs from `mcp-tool-surface.md` section 15.

**Files:**
- Modify: `src/moneybin/mcp/resources.py`

- [ ] **Step 1: Rewrite resources.py**

Replace the entire file content with v1 resources:

```python
# src/moneybin/mcp/resources.py
"""MCP v1 resource definitions.

Resources provide ambient context loaded when the AI connects — schema
information, account list, privacy status, data freshness. They are
read-only, compact, and change infrequently.

See ``mcp-tool-surface.md`` section 15.
"""

import json
import logging
from typing import Any

from .server import get_db, mcp, table_exists
from moneybin.tables import DIM_ACCOUNTS, FCT_TRANSACTIONS, OFX_BALANCES

logger = logging.getLogger(__name__)


@mcp.resource("moneybin://status")
def resource_status() -> str:
    """Data freshness: row counts, date ranges, last import, categorization coverage."""
    logger.info("Resource read: moneybin://status")
    db = get_db()
    status: dict[str, Any] = {}

    # Transaction stats
    if table_exists(FCT_TRANSACTIONS):
        row = db.execute(f"""
            SELECT COUNT(*), MIN(transaction_date), MAX(transaction_date)
            FROM {FCT_TRANSACTIONS.full_name}
        """).fetchone()
        if row:
            status["transactions"] = {
                "total": row[0],
                "date_range_start": str(row[1]) if row[1] else None,
                "date_range_end": str(row[2]) if row[2] else None,
            }

    # Account count
    if table_exists(DIM_ACCOUNTS):
        row = db.execute(f"SELECT COUNT(*) FROM {DIM_ACCOUNTS.full_name}").fetchone()
        status["accounts"] = {"total": row[0] if row else 0}

    return json.dumps(status, indent=2, default=str)


@mcp.resource("moneybin://accounts")
def resource_accounts() -> str:
    """Account list with types, institutions, currencies. No balances."""
    logger.info("Resource read: moneybin://accounts")
    db = get_db()

    if not table_exists(DIM_ACCOUNTS):
        return json.dumps({"accounts": []})

    result = db.execute(f"""
        SELECT account_id, account_type, institution_name, source_type
        FROM {DIM_ACCOUNTS.full_name}
        ORDER BY institution_name, account_type
    """)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return json.dumps({"accounts": records}, indent=2, default=str)


@mcp.resource("moneybin://privacy")
def resource_privacy() -> str:
    """Active consent grants and configured AI backend. Stub until privacy specs land."""
    logger.info("Resource read: moneybin://privacy")
    # Stub — returns static defaults until consent infrastructure is built
    return json.dumps(
        {
            "consent_grants": [],
            "configured_backend": None,
            "consent_mode": "opt-in",
            "unmask_critical": False,
        },
        indent=2,
    )


@mcp.resource("moneybin://schema")
def resource_schema() -> str:
    """Core and app table schemas with column names, types, and descriptions."""
    logger.info("Resource read: moneybin://schema")
    db = get_db()

    result = db.execute("""
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type,
            comment
        FROM duckdb_columns()
        WHERE table_schema IN ('core', 'app', 'raw')
        ORDER BY table_schema, table_name, column_index
    """)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()

    # Group by table
    tables: dict[str, Any] = {}
    for row in rows:
        key = f"{row[0]}.{row[1]}"
        if key not in tables:
            tables[key] = {"schema": row[0], "table": row[1], "columns": []}
        tables[key]["columns"].append({
            "name": row[2],
            "type": row[3],
            "description": row[4],
        })

    return json.dumps({"tables": list(tables.values())}, indent=2, default=str)


@mcp.resource("moneybin://tools")
def resource_tools() -> str:
    """Available tool namespaces with descriptions and loaded status."""
    logger.info("Resource read: moneybin://tools")
    # This will be wired to the NamespaceRegistry in the server setup
    # For now, return static namespace list
    from moneybin.mcp.namespaces import CORE_NAMESPACES_DEFAULT, _NAMESPACE_DESCRIPTIONS

    core = [
        {
            "namespace": ns,
            "loaded": True,
            "description": _NAMESPACE_DESCRIPTIONS.get(ns, ""),
        }
        for ns in sorted(CORE_NAMESPACES_DEFAULT)
    ]
    return json.dumps({"core": core, "discover_tool": "moneybin.discover"}, indent=2)
```

- [ ] **Step 2: Run existing resource tests (may need updates for new URIs)**

Run: `uv run pytest tests/moneybin/test_mcp/test_resources.py -v`
Expected: Tests may need updating for new resource URIs. Update test imports and assertions.

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/mcp/resources.py
git commit -m "feat: rewrite MCP resources for v1 URIs

Five v1 resources: moneybin://status, accounts, privacy, schema, tools.
Replaces prototype resources (schema/tables, schema/{name}, accounts/summary,
transactions/recent, w2/{year})."
```

---

## Task 12: V1 Prompts

Replace the 8 prototype step-by-step prompts with the 4 v1 goal-oriented prompts from `mcp-tool-surface.md` section 14.

**Files:**
- Modify: `src/moneybin/mcp/prompts.py`

- [ ] **Step 1: Rewrite prompts.py**

Replace the entire file with the 4 v1 prompts: `monthly-review`, `categorization-organize`, `onboarding`, `tax-prep`. Each prompt uses the v1 tool names and follows the goal-oriented template from the spec (guardrails + decision points, not step-by-step scripts).

- [ ] **Step 2: Run any prompt-related tests**

Run: `uv run pytest tests/moneybin/test_mcp/ -v -k prompt`
Expected: PASS (may need test updates)

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/mcp/prompts.py
git commit -m "feat: rewrite MCP prompts as goal-oriented v1 templates

Four prompts: monthly-review, categorization-organize, onboarding,
tax-prep. Replaces 8 prototype step-by-step prompts. Each defines
goals, relevant tools, guardrails, and decision points."
```

---

## Task 13: Server Setup & Namespace Registration

Update `server.py` to use namespace-aware registration: register core namespace tools at startup, keep `moneybin.discover` always available, and update the server instructions.

**Files:**
- Modify: `src/moneybin/mcp/server.py`
- Modify: `src/moneybin/mcp/__init__.py`

- [ ] **Step 1: Update server.py**

Key changes:
1. Update `mcp = FastMCP(...)` instructions for v1 tool names and progressive disclosure
2. Add `register_core_tools()` function that:
   - Creates a `NamespaceRegistry` singleton
   - Calls all `register_*_tools()` functions to populate the registry
   - Registers core namespace tools with FastMCP via `mcp.tool()`
   - Always registers `moneybin.discover`
3. Add `get_registry()` function for the singleton
4. Call `register_core_tools()` at module level (after the registry call imports)

```python
# Key additions to server.py:

_registry: NamespaceRegistry | None = None


def get_registry() -> NamespaceRegistry:
    """Get the namespace registry singleton."""
    global _registry
    if _registry is None:
        _registry = _build_registry()
    return _registry


def _build_registry() -> NamespaceRegistry:
    """Build and populate the namespace registry with all tool modules."""
    from moneybin.mcp.namespaces import NamespaceRegistry
    from moneybin.mcp.tools.spending import register_spending_tools
    from moneybin.mcp.tools.accounts import register_accounts_tools
    from moneybin.mcp.tools.transactions import register_transactions_tools
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.categorize import register_categorize_tools
    from moneybin.mcp.tools.budget import register_budget_tools
    from moneybin.mcp.tools.tax import register_tax_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.discover import register_discover_tool

    registry = NamespaceRegistry()
    register_spending_tools(registry)
    register_accounts_tools(registry)
    register_transactions_tools(registry)
    register_import_tools(registry)
    register_categorize_tools(registry)
    register_budget_tools(registry)
    register_tax_tools(registry)
    register_sql_tools(registry)
    register_discover_tool(registry)
    return registry


def register_core_tools() -> None:
    """Register core namespace tools with FastMCP at startup."""
    from moneybin.config import get_settings
    from moneybin.mcp.namespaces import CORE_NAMESPACES_DEFAULT

    registry = get_registry()
    cfg = get_settings().mcp

    # Determine core namespaces from config or defaults
    if cfg.core_namespaces and cfg.core_namespaces == ["*"]:
        core_ns = registry.all_namespaces()
    elif cfg.core_namespaces:
        core_ns = set(cfg.core_namespaces)
    else:
        core_ns = set(CORE_NAMESPACES_DEFAULT)

    # Register core tools with FastMCP
    for tool in registry.get_core_tools(core_ns):
        mcp.tool(name=tool.name, description=tool.description)(tool.fn)
        registry.mark_loaded(tool.namespace)

    # moneybin.discover is always registered
    discover_tools = registry.get_namespace_tools("moneybin")
    for tool in discover_tools:
        if not registry.is_loaded("moneybin"):
            mcp.tool(name=tool.name, description=tool.description)(tool.fn)
    registry.mark_loaded("moneybin")

    logger.info(
        f"Registered {sum(len(registry.get_namespace_tools(ns)) for ns in core_ns)} "
        f"core tools from {len(core_ns)} namespaces"
    )
```

- [ ] **Step 2: Update __init__.py**

Update docstring to mention v1:

```python
"""MCP server for MoneyBin — AI-powered personal finance.

V1 tool surface with namespace-based progressive disclosure, response
envelopes, and sensitivity-tiered privacy middleware.
"""
```

- [ ] **Step 3: Run server tests**

Run: `uv run pytest tests/moneybin/test_mcp/test_server.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/mcp/server.py src/moneybin/mcp/__init__.py
git commit -m "feat: wire namespace registry into MCP server startup

register_core_tools() populates the namespace registry with all tool
modules, registers core namespace tools with FastMCP at startup, and
always registers moneybin.discover. Progressive disclosure is active."
```

---

## Task 14: Remove Prototype Tools (Clean Break)

Delete the old `tools.py` and `write_tools.py` files. Update test imports.

**Files:**
- Delete: `src/moneybin/mcp/tools.py`
- Delete: `src/moneybin/mcp/write_tools.py`
- Modify: `tests/moneybin/test_mcp/test_tools.py` (rewrite for v1)
- Modify: `tests/moneybin/test_mcp/test_categorization_tools.py` (rewrite for v1)

- [ ] **Step 1: Delete prototype tool files**

```bash
rm src/moneybin/mcp/tools.py src/moneybin/mcp/write_tools.py
```

- [ ] **Step 2: Rewrite test_tools.py for v1 tool names**

Update all test imports to use v1 tool functions from `src/moneybin/mcp/tools/`. Update assertions to check for the response envelope structure instead of raw JSON arrays. Tests should call tool functions directly and verify the `{summary, data, actions}` shape.

- [ ] **Step 3: Rewrite test_categorization_tools.py for v1 tool names**

Same pattern — update imports and assertions for the v1 envelope structure.

- [ ] **Step 4: Run all MCP tests**

Run: `uv run pytest tests/moneybin/test_mcp/ -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: remove prototype MCP tools, clean break to v1

Delete tools.py and write_tools.py. All prototype tool names
(list_accounts, query_transactions, etc.) are gone. V1 tools
(accounts.list, transactions.search, etc.) are the only surface."
```

---

## Task 15: Full Integration Test & `make check`

Run the full test suite and fix any breakage from the migration.

**Files:**
- Various test files may need import fixes

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: Fix any failures from the migration

- [ ] **Step 2: Run format and lint**

Run: `make format && make lint`
Expected: Clean

- [ ] **Step 3: Run type check on modified files**

Run: `uv run pyright src/moneybin/mcp/ src/moneybin/services/ src/moneybin/cli/output.py`
Expected: No errors

- [ ] **Step 4: Run full pre-commit check**

Run: `make check test`
Expected: All pass

- [ ] **Step 5: Commit any fixes**

```bash
git add -A
git commit -m "fix: resolve lint, type, and test issues from MCP v1 migration"
```

---

## Task 16: Update Spec Status & README

Mark the MCP architecture and tool surface specs as `in-progress`, update the README roadmap.

**Files:**
- Modify: `docs/specs/INDEX.md`
- Modify: `docs/specs/mcp-architecture.md` (status line)
- Modify: `docs/specs/mcp-tool-surface.md` (status line)

- [ ] **Step 1: Update spec statuses to `in-progress`**

In `docs/specs/mcp-architecture.md` line 16, change `ready` to `in-progress`.
In `docs/specs/mcp-tool-surface.md` line 26, change `ready` to `in-progress`.
Update `docs/specs/INDEX.md` to reflect the status changes.

- [ ] **Step 2: Commit**

```bash
git add docs/specs/INDEX.md docs/specs/mcp-architecture.md docs/specs/mcp-tool-surface.md
git commit -m "docs: mark MCP architecture and tool surface specs as in-progress"
```

---

## Summary

| Task | Description | New files | Key deliverable |
|------|-------------|-----------|----------------|
| 1 | Response Envelope | `envelope.py` | `ResponseEnvelope`, `SummaryMeta`, `build_envelope()` |
| 2 | Sensitivity & Privacy Stub | modify `privacy.py` | `Sensitivity` enum, `log_tool_call()` |
| 3 | MCP Tool Decorator | `decorator.py` | `@mcp_tool(sensitivity=...)` |
| 4 | Namespace Registry | `namespaces.py` | `NamespaceRegistry`, progressive disclosure |
| 5 | Exemplar Service | `spending_service.py` | `SpendingService` pattern proof |
| 6 | Exemplar MCP Tools | `tools/spending.py` | `spending.summary`, `spending.by_category` |
| 7 | CLI --output json | `cli/output.py` | `OutputFormat`, `render_or_json()` |
| 8 | Remaining Services | 4 service files | AccountService, TransactionService, BudgetService, TaxService |
| 9 | CategorizationService Types | modify existing | Typed dataclasses for categorization |
| 10 | V1 Tool Modules | 8 tool files | All ~25 v1 tools in namespace modules |
| 11 | V1 Resources | rewrite `resources.py` | 5 v1 resource URIs |
| 12 | V1 Prompts | rewrite `prompts.py` | 4 goal-oriented prompts |
| 13 | Server Setup | modify `server.py` | Namespace-aware registration |
| 14 | Prototype Cleanup | delete 2 files | Clean break from old names |
| 15 | Integration Test | various | `make check test` passes |
| 16 | Spec Status | modify specs | `in-progress` status |

### Prototype → V1 Migration Mapping (from `mcp-tool-surface.md` §16)

| Prototype | V1 | Change type |
|-----------|-----|-------------|
| `list_tables` | `moneybin://schema` | Tool → resource |
| `describe_table` | `moneybin://schema` | Tool → resource |
| `list_accounts` | `accounts.list` | Rename |
| `get_account_balances` | `accounts.balances` | Rename |
| `list_institutions` | `accounts.list` | Merged |
| `query_transactions` | `transactions.search` | Rename + richer filters |
| `find_recurring_transactions` | `transactions.recurring` | Rename |
| `get_w2_summary` | `tax.w2` | Rename |
| `list_categories` | `categorize.categories` | Rename |
| `list_categorization_rules` | `categorize.rules` | Rename |
| `list_merchants` | `categorize.merchants` | Rename |
| `get_categorization_stats` | `categorize.stats` | Rename |
| `run_read_query` | `sql.query` | Rename |
| `import_file` | `import.file` | Rename |
| `import_preview` | `import.csv_preview` | Rename |
| `import_history` | `import.status` | Rename |
| `list_formats` | `import.list_formats` | Rename |
| `categorize_transaction` | `categorize.bulk` | Single → use list of one |
| `get_uncategorized_transactions` | `categorize.uncategorized` | Rename |
| `seed_categories` | `categorize.seed` | Rename |
| `toggle_category` | `categorize.toggle_category` | Rename |
| `create_category` | `categorize.create_category` | Rename |
| `create_merchant_mapping` | `categorize.create_merchants` | Single → bulk |
| `create_categorization_rule` | `categorize.create_rules` | Single → bulk |
| `delete_categorization_rule` | `categorize.delete_rule` | Rename |
| `bulk_categorize` | `categorize.bulk` | Rename |
| `bulk_create_categorization_rules` | `categorize.create_rules` | Rename |
| `bulk_create_merchant_mappings` | `categorize.create_merchants` | Rename |
| `set_budget` | `budget.set` | Rename |
| `get_budget_status` | `budget.status` | Rename |
| `get_monthly_summary` | `spending.summary` | Rename |
| `get_spending_by_category` | `spending.by_category` | Rename |
