# MCP Tool Surface — Foundation & Exemplar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared infrastructure for MoneyBin's v1 MCP tool surface and implement one namespace (`spending.*`) end-to-end as the pattern exemplar.

**Architecture:** Three-layer stack: service layer (business logic, typed returns) → privacy middleware (sensitivity gates, audit stubs) → thin MCP/CLI wrappers. The response envelope (`{summary, data, actions}`) is shared infrastructure. Each namespace maps to one service class consumed by both MCP and CLI. This plan builds the layers bottom-up and proves them with `spending.*`.

**Tech Stack:** Python 3.12+, FastMCP (MCP SDK), Typer (CLI), DuckDB, Pydantic/dataclasses for response types, pytest.

**Specs:** [`docs/specs/mcp-architecture.md`](../../specs/mcp-architecture.md), [`docs/specs/mcp-tool-surface.md`](../../specs/mcp-tool-surface.md)

---

## Prerequisites: What must be designed or built first

Before implementing the tool surface, these items need attention. Some are design decisions that this plan resolves inline; others are external dependencies.

### Resolved by this plan

| Prerequisite | Resolution |
|---|---|
| **Response envelope** | Task 1 defines the `ResponseEnvelope` dataclass and builder |
| **Service layer base pattern** | Task 2 establishes the convention with `SpendingService` as exemplar |
| **MCP namespace registration** | Task 4 — FastMCP supports `name='spending.summary'` natively (verified) |
| **Privacy middleware stubs** | Task 3 — decorator-based `@tool_meta(sensitivity="low")` that logs but doesn't enforce consent (enforcement arrives with consent management spec) |
| **CLI `--output json` support** | Task 5 — shared callback that switches output format |
| **CLI command group restructure** | Task 5 — new `spending` command group as exemplar |

### External dependencies (not blocked, but noted)

| Dependency | Impact | Workaround in this plan |
|---|---|---|
| **Consent management spec** | Degraded responses are the spec's key feature but consent checking doesn't exist yet | Privacy middleware stubs record sensitivity metadata but always allow full responses. `summary.degraded` is always `false`. When consent spec ships, the middleware gains real enforcement. |
| **Audit log spec** | Middleware should log every tool call to `app.ai_audit_log` | Middleware logs to Python `logging` for now. Schema and DB logging added when audit spec ships. |
| **Corrections/annotations table schemas** | `transactions.correct` and `transactions.annotate` need new tables | Not in this plan's scope (those tools are in later namespace plans). Schema DDL will be added when those namespaces are implemented. |

### Scope boundary

This plan covers:
- Foundation infrastructure (tasks 1-5)
- `spending.*` namespace: service, MCP tools, CLI commands, tests (tasks 6-9)
- Migration prep: server instructions update, prototype tool deprecation notes (task 10)

This plan does NOT cover:
- Other namespace implementations (`cashflow.*`, `accounts.*`, etc.) — separate plans
- Prompts and resources — separate plan after core namespaces
- Privacy middleware enforcement — when consent management spec is implemented
- CLI command restructure for non-spending namespaces — each namespace plan adds its CLI group

---

## File structure

### New files

```
src/moneybin/
  mcp/
    envelope.py              # ResponseEnvelope, SummaryMeta, envelope builder
    middleware.py             # @tool_meta decorator, sensitivity registry, audit stubs
    tools/                   # New directory — one file per namespace
      __init__.py
      spending.py            # spending.* MCP tool wrappers
  services/
    spending_service.py      # SpendingService (business logic, SQL, typed returns)
  cli/
    commands/
      spending.py            # CLI spending command group
    output.py                # --output json support, table rendering helpers

tests/moneybin/
  test_services/
    test_spending_service.py # SpendingService unit tests
  test_mcp/
    test_envelope.py         # ResponseEnvelope tests
    test_middleware.py        # Privacy middleware stub tests
    test_spending_tools.py   # spending.* MCP tool integration tests
  test_cli/
    test_spending_cli.py     # CLI spending command tests
```

### Modified files

```
src/moneybin/mcp/server.py      # Update instructions, keep connection management
src/moneybin/mcp/__init__.py     # Update docstring, import new tool modules
src/moneybin/cli/main.py         # Add spending command group
```

### Preserved (not modified in this plan)

```
src/moneybin/mcp/tools.py        # Prototype tools — removed in migration plan
src/moneybin/mcp/write_tools.py  # Prototype tools — removed in migration plan
src/moneybin/mcp/prompts.py      # Replaced in prompts plan
src/moneybin/mcp/resources.py    # Replaced in resources plan
```

---

## Task 1: Response envelope

The shared response shape for all MCP tools and CLI `--output json`.

**Files:**
- Create: `src/moneybin/mcp/envelope.py`
- Test: `tests/moneybin/test_mcp/test_envelope.py`

- [ ] **Step 1: Write failing tests for ResponseEnvelope**

```python
# tests/moneybin/test_mcp/test_envelope.py
"""Tests for the MCP response envelope."""

from moneybin.mcp.envelope import ResponseEnvelope


class TestResponseEnvelope:
    """ResponseEnvelope construction and serialization."""

    def test_basic_envelope(self) -> None:
        env = ResponseEnvelope(
            data=[{"period": "2026-04", "income": 5200.00}],
            total_count=1,
            sensitivity="low",
        )
        result = env.to_dict()

        assert result["summary"]["total_count"] == 1
        assert result["summary"]["returned_count"] == 1
        assert result["summary"]["has_more"] is False
        assert result["summary"]["sensitivity"] == "low"
        assert result["data"] == [{"period": "2026-04", "income": 5200.00}]
        assert result["actions"] == []

    def test_envelope_with_pagination(self) -> None:
        data = [{"id": i} for i in range(50)]
        env = ResponseEnvelope(
            data=data,
            total_count=247,
            sensitivity="medium",
            period="2026-01 to 2026-04",
        )
        result = env.to_dict()

        assert result["summary"]["total_count"] == 247
        assert result["summary"]["returned_count"] == 50
        assert result["summary"]["has_more"] is True
        assert result["summary"]["period"] == "2026-01 to 2026-04"

    def test_envelope_with_actions(self) -> None:
        env = ResponseEnvelope(
            data=[],
            total_count=0,
            sensitivity="low",
            actions=["Use spending.by_category for breakdown"],
        )
        result = env.to_dict()

        assert result["actions"] == ["Use spending.by_category for breakdown"]

    def test_envelope_with_currency(self) -> None:
        env = ResponseEnvelope(
            data=[{"amount": 100}],
            total_count=1,
            sensitivity="low",
            display_currency="USD",
        )
        result = env.to_dict()

        assert result["summary"]["display_currency"] == "USD"

    def test_envelope_degraded(self) -> None:
        env = ResponseEnvelope(
            data=[{"category": "Food", "total": 500}],
            total_count=5,
            sensitivity="low",
            degraded=True,
            degraded_reason="Transaction-level data requires data-sharing consent",
        )
        result = env.to_dict()

        assert result["summary"]["degraded"] is True
        assert "consent" in result["summary"]["degraded_reason"]

    def test_envelope_write_result(self) -> None:
        """Write tools return a result object, not an array."""
        env = ResponseEnvelope(
            data={"applied": 48, "skipped": 0, "errors": 2},
            total_count=50,
            sensitivity="medium",
        )
        result = env.to_dict()

        assert result["data"]["applied"] == 48
        assert result["summary"]["total_count"] == 50
        # returned_count is len(data) for lists, 1 for dicts
        assert result["summary"]["returned_count"] == 1

    def test_envelope_to_json(self) -> None:
        env = ResponseEnvelope(
            data=[{"amount": 42.50}],
            total_count=1,
            sensitivity="low",
        )
        json_str = env.to_json()

        assert isinstance(json_str, str)
        assert '"amount": 42.5' in json_str
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.mcp.envelope'`

- [ ] **Step 3: Implement ResponseEnvelope**

```python
# src/moneybin/mcp/envelope.py
"""Response envelope for MCP tools and CLI JSON output.

Every MCP tool returns this shape. The CLI returns it when --output json
is used. See docs/specs/mcp-architecture.md section 4 for the design.
"""

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


def _default_serializer(obj: object) -> str:
    """JSON serializer for types not handled by default."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


@dataclass
class ResponseEnvelope:
    """Standard response shape for all MCP tools.

    Args:
        data: The payload — list of objects for read tools, result object
            for write tools.
        total_count: Total matching records (may exceed returned_count).
        sensitivity: The tool's declared sensitivity tier.
        period: Human-readable date range covered (e.g., "2026-01 to 2026-04").
        display_currency: Currency all amounts are denominated in.
        degraded: Whether this is a degraded (consent-limited) response.
        degraded_reason: Why the response is degraded.
        actions: Contextual next-step hints for composability.
    """

    data: list[dict] | dict
    total_count: int
    sensitivity: str
    period: str | None = None
    display_currency: str | None = None
    degraded: bool = False
    degraded_reason: str | None = None
    actions: list[str] = field(default_factory=list)

    @property
    def returned_count(self) -> int:
        """Number of records in the response data."""
        if isinstance(self.data, list):
            return len(self.data)
        return 1

    @property
    def has_more(self) -> bool:
        """Whether more records exist beyond what was returned."""
        return self.total_count > self.returned_count

    def to_dict(self) -> dict:
        """Convert to the response envelope dict shape."""
        summary: dict = {
            "total_count": self.total_count,
            "returned_count": self.returned_count,
            "has_more": self.has_more,
            "sensitivity": self.sensitivity,
        }
        if self.period is not None:
            summary["period"] = self.period
        if self.display_currency is not None:
            summary["display_currency"] = self.display_currency
        if self.degraded:
            summary["degraded"] = True
            if self.degraded_reason:
                summary["degraded_reason"] = self.degraded_reason

        return {
            "summary": summary,
            "data": self.data,
            "actions": self.actions,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), default=_default_serializer, indent=2)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/envelope.py tests/moneybin/test_mcp/test_envelope.py
git commit -m "feat: add ResponseEnvelope for MCP tool responses"
```

---

## Task 2: Privacy middleware stubs

Decorator that registers tool metadata (sensitivity tier) and stubs consent/audit behavior. Real enforcement ships with the consent management spec.

**Files:**
- Create: `src/moneybin/mcp/middleware.py`
- Test: `tests/moneybin/test_mcp/test_middleware.py`

- [ ] **Step 1: Write failing tests for tool_meta and sensitivity registry**

```python
# tests/moneybin/test_mcp/test_middleware.py
"""Tests for the privacy middleware stubs."""

from moneybin.mcp.middleware import get_tool_sensitivity, tool_meta


class TestToolMeta:
    """The @tool_meta decorator registers sensitivity metadata."""

    def test_registers_sensitivity(self) -> None:
        @tool_meta(sensitivity="medium")
        def my_tool() -> str:
            return "result"

        assert get_tool_sensitivity("my_tool") == "medium"

    def test_preserves_function_behavior(self) -> None:
        @tool_meta(sensitivity="low")
        def my_tool(x: int) -> int:
            return x * 2

        assert my_tool(5) == 10

    def test_preserves_function_name(self) -> None:
        @tool_meta(sensitivity="high")
        def my_tool() -> str:
            return "ok"

        assert my_tool.__name__ == "my_tool"

    def test_default_sensitivity_is_none(self) -> None:
        assert get_tool_sensitivity("nonexistent_tool") is None

    def test_registers_dotted_name(self) -> None:
        @tool_meta(sensitivity="low", tool_name="spending.summary")
        def spending_summary() -> str:
            return "ok"

        assert get_tool_sensitivity("spending.summary") == "low"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_middleware.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement tool_meta decorator and sensitivity registry**

```python
# src/moneybin/mcp/middleware.py
"""Privacy middleware stubs for MCP tools.

Registers tool sensitivity metadata. In v1, this records sensitivity
but does not enforce consent gates — enforcement arrives with the consent
management spec. The decorator captures metadata so that when the middleware
gains real enforcement, no tool code changes are needed.

See docs/specs/mcp-architecture.md sections 2 and 5.
"""

import functools
import logging
from collections.abc import Callable
from typing import ParamSpec, TypeVar

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# Registry: tool_name -> sensitivity tier
_sensitivity_registry: dict[str, str] = {}


def get_tool_sensitivity(tool_name: str) -> str | None:
    """Look up the declared sensitivity tier for a tool.

    Args:
        tool_name: The tool name (may be dot-separated).

    Returns:
        The sensitivity tier string, or None if not registered.
    """
    return _sensitivity_registry.get(tool_name)


def tool_meta(
    sensitivity: str,
    tool_name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator that registers sensitivity metadata for an MCP tool.

    Args:
        sensitivity: The tool's maximum data sensitivity tier
            ("low", "medium", "high").
        tool_name: Override the registry key. Defaults to the function name.
            Use this for dot-separated MCP tool names.

    Returns:
        Decorator that preserves the wrapped function's behavior.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        name = tool_name or func.__name__
        _sensitivity_registry[name] = sensitivity
        logger.debug(f"Registered tool sensitivity: {name} = {sensitivity}")

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # Stub: log the call with sensitivity metadata.
            # Real enforcement (consent check, degraded response swap,
            # audit logging) will be added when the consent spec ships.
            logger.info(f"Tool call: {name} (sensitivity={sensitivity})")
            return func(*args, **kwargs)

        return wrapper

    return decorator
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_middleware.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/mcp/middleware.py tests/moneybin/test_mcp/test_middleware.py
git commit -m "feat: add privacy middleware stubs with sensitivity registry"
```

---

## Task 3: SpendingService

The service layer for `spending.*` — business logic, parameterized SQL, typed returns. This is the exemplar service class that establishes the pattern for all namespaces.

**Files:**
- Create: `src/moneybin/services/spending_service.py`
- Test: `tests/moneybin/test_services/test_spending_service.py`

- [ ] **Step 1: Write failing tests for SpendingService.summary()**

```python
# tests/moneybin/test_services/test_spending_service.py
"""Tests for SpendingService."""

from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

from moneybin.schema import init_schemas
from moneybin.services.spending_service import SpendingService
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture()
def db(tmp_path: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """DuckDB with schemas, core tables, and sample transactions."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    init_schemas(conn)
    create_core_tables(conn)

    # Insert sample transactions across 3 months
    conn.execute("""
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_type, transaction_date,
         amount, description, source_system, source_file,
         transaction_direction, transaction_year_month, amount_absolute,
         extracted_at, loaded_at)
        VALUES
        -- April 2026: income + expenses
        ('tx1', 'ACC001', 'CREDIT', '2026-04-15', 5200.00, 'PAYROLL',
         'ofx', 'test.qfx', 'credit', '2026-04', 5200.00,
         '2026-04-15', CURRENT_TIMESTAMP),
        ('tx2', 'ACC001', 'DEBIT', '2026-04-10', -42.50, 'WHOLEFDS MKT',
         'ofx', 'test.qfx', 'debit', '2026-04', 42.50,
         '2026-04-10', CURRENT_TIMESTAMP),
        ('tx3', 'ACC001', 'DEBIT', '2026-04-12', -85.00, 'SHELL OIL',
         'ofx', 'test.qfx', 'debit', '2026-04', 85.00,
         '2026-04-12', CURRENT_TIMESTAMP),
        -- March 2026
        ('tx4', 'ACC001', 'CREDIT', '2026-03-15', 5200.00, 'PAYROLL',
         'ofx', 'test.qfx', 'credit', '2026-03', 5200.00,
         '2026-03-15', CURRENT_TIMESTAMP),
        ('tx5', 'ACC001', 'DEBIT', '2026-03-05', -120.00, 'AMAZON',
         'ofx', 'test.qfx', 'debit', '2026-03', 120.00,
         '2026-03-05', CURRENT_TIMESTAMP),
        -- February 2026
        ('tx6', 'ACC001', 'CREDIT', '2026-02-15', 5200.00, 'PAYROLL',
         'ofx', 'test.qfx', 'credit', '2026-02', 5200.00,
         '2026-02-15', CURRENT_TIMESTAMP),
        ('tx7', 'ACC001', 'DEBIT', '2026-02-20', -200.00, 'RENT',
         'ofx', 'test.qfx', 'debit', '2026-02', 200.00,
         '2026-02-20', CURRENT_TIMESTAMP)
    """)

    yield conn
    conn.close()


class TestSpendingSummary:
    """SpendingService.summary() returns monthly income/expense aggregates."""

    def test_default_three_months(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(months=3)

        assert len(result.months) == 3
        # Most recent month first
        assert result.months[0].period == "2026-04"
        assert result.months[0].income == 5200.00
        assert result.months[0].expenses == 127.50
        assert result.months[0].net == 5072.50

    def test_single_month(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(months=1)

        assert len(result.months) == 1
        assert result.months[0].period == "2026-04"

    def test_date_range_override(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(start_date="2026-02-01", end_date="2026-03-31")

        assert len(result.months) == 2
        assert result.months[0].period == "2026-03"
        assert result.months[1].period == "2026-02"

    def test_account_filter(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(months=3, account_id=["ACC001"])

        # All data is ACC001, so same result
        assert len(result.months) == 3

    def test_transaction_counts(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(months=3)

        assert result.months[0].transaction_count == 3  # April: 1 income + 2 expense
        assert result.months[1].transaction_count == 2  # March: 1 income + 1 expense
        assert result.months[2].transaction_count == 2  # Feb: 1 income + 1 expense

    def test_date_range_metadata(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(months=3)

        assert result.date_range_start == "2026-02"
        assert result.date_range_end == "2026-04"

    def test_empty_result(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.summary(start_date="2020-01-01", end_date="2020-12-31")

        assert len(result.months) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement SpendingService.summary()**

```python
# src/moneybin/services/spending_service.py
"""Spending analysis service.

Business logic for spending summaries, category breakdowns, merchant
analysis, and period comparisons. Consumed by MCP tools and CLI commands.
"""

import logging
from dataclasses import dataclass, field

import duckdb

from moneybin.tables import FCT_TRANSACTIONS

logger = logging.getLogger(__name__)


@dataclass
class MonthlySpending:
    """Spending data for a single month."""

    period: str
    income: float
    expenses: float
    net: float
    transaction_count: int


@dataclass
class SpendingSummary:
    """Result of SpendingService.summary()."""

    months: list[MonthlySpending]
    date_range_start: str = ""
    date_range_end: str = ""


@dataclass
class CategoryRow:
    """A single category's spending data."""

    category: str
    subcategory: str | None
    total: float
    transaction_count: int
    percent_of_total: float


@dataclass
class CategoryBreakdown:
    """Result of SpendingService.by_category()."""

    categories: list[CategoryRow]
    total_spending: float
    period: str


@dataclass
class MerchantRow:
    """A single merchant's spending data."""

    merchant_name: str
    total: float
    transaction_count: int
    category: str | None
    last_seen: str


@dataclass
class MerchantBreakdown:
    """Result of SpendingService.merchants()."""

    merchants: list[MerchantRow]
    period: str


@dataclass
class PeriodComparisonRow:
    """Category-level comparison between two periods."""

    category: str
    period_a_total: float
    period_b_total: float
    change_amount: float
    change_percent: float | None


@dataclass
class PeriodComparison:
    """Result of SpendingService.compare()."""

    period_a: str
    period_b: str
    categories: list[PeriodComparisonRow]
    period_a_total: float
    period_b_total: float


class SpendingService:
    """Spending analysis operations.

    Args:
        db: DuckDB connection (read-only for queries).
    """

    def __init__(self, db: duckdb.DuckDBPyConnection) -> None:
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

        if start_date and end_date:
            conditions.append("transaction_year_month >= ?")
            params.append(start_date[:7])
            conditions.append("transaction_year_month <= ?")
            params.append(end_date[:7])
        elif start_date:
            conditions.append("transaction_year_month >= ?")
            params.append(start_date[:7])

        if account_id:
            placeholders = ", ".join(["?"] * len(account_id))
            conditions.append(f"account_id IN ({placeholders})")
            params.extend(account_id)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Build the limit clause: only apply LIMIT when using months lookback
        # (no explicit date range)
        limit_clause = ""
        if not start_date:
            limit_clause = "LIMIT ?"
            params.append(months)

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
                period=row[0],
                income=float(row[1]),
                expenses=float(row[2]),
                net=float(row[3]),
                transaction_count=int(row[4]),
            )
            for row in rows
        ]

        date_range_start = monthly[-1].period if monthly else ""
        date_range_end = monthly[0].period if monthly else ""

        return SpendingSummary(
            months=monthly,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/spending_service.py tests/moneybin/test_services/test_spending_service.py
git commit -m "feat: add SpendingService with summary method"
```

---

## Task 4: SpendingService — by_category, merchants, compare

Remaining `SpendingService` methods. Same TDD pattern as Task 3.

**Files:**
- Modify: `src/moneybin/services/spending_service.py`
- Modify: `tests/moneybin/test_services/test_spending_service.py`

- [ ] **Step 1: Write failing tests for by_category()**

Add to `tests/moneybin/test_services/test_spending_service.py`:

```python
from moneybin.tables import TRANSACTION_CATEGORIES


@pytest.fixture()
def db_with_categories(db: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Add categorizations to the base fixture."""
    db.execute(f"""
        INSERT INTO {TRANSACTION_CATEGORIES.full_name}
        (transaction_id, category, subcategory, categorized_at, categorized_by)
        VALUES
        ('tx2', 'Food & Drink', 'Groceries', CURRENT_TIMESTAMP, 'user'),
        ('tx3', 'Transportation', 'Gas', CURRENT_TIMESTAMP, 'user'),
        ('tx5', 'Shopping', 'Online', CURRENT_TIMESTAMP, 'user'),
        ('tx7', 'Housing', 'Rent', CURRENT_TIMESTAMP, 'user')
    """)
    return db


class TestSpendingByCategory:
    """SpendingService.by_category() returns category breakdown."""

    def test_returns_categories_sorted_by_total(
        self, db_with_categories: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db_with_categories)
        result = svc.by_category(months=3)

        assert len(result.categories) > 0
        # Sorted by total descending
        totals = [c.total for c in result.categories]
        assert totals == sorted(totals, reverse=True)

    def test_includes_percent_of_total(
        self, db_with_categories: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db_with_categories)
        result = svc.by_category(months=3)

        pct_sum = sum(c.percent_of_total for c in result.categories)
        # Should sum to ~100% (allow floating point tolerance)
        assert 99.0 <= pct_sum <= 101.0

    def test_top_n_limits_results(
        self, db_with_categories: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db_with_categories)
        result = svc.by_category(months=3, top_n=2)

        assert len(result.categories) <= 2

    def test_includes_uncategorized_by_default(
        self, db: duckdb.DuckDBPyConnection
    ) -> None:
        """When include_uncategorized=True, uncategorized spending appears."""
        svc = SpendingService(db)
        result = svc.by_category(months=3, include_uncategorized=True)

        category_names = [c.category for c in result.categories]
        assert "Uncategorized" in category_names
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py::TestSpendingByCategory -v`
Expected: FAIL — `AttributeError: 'SpendingService' object has no attribute 'by_category'`

- [ ] **Step 3: Implement by_category()**

Add to `SpendingService` in `src/moneybin/services/spending_service.py`:

```python
def by_category(
    self,
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 10,
    include_uncategorized: bool = True,
) -> CategoryBreakdown:
    """Get spending breakdown by category.

    Args:
        months: Number of recent months to include.
        start_date: ISO 8601 start date (overrides months).
        end_date: ISO 8601 end date.
        account_id: Filter to specific accounts.
        top_n: Limit to top N categories by total.
        include_uncategorized: Include uncategorized spending as a rollup row.

    Returns:
        CategoryBreakdown with per-category totals.
    """
    from moneybin.tables import TRANSACTION_CATEGORIES

    conditions: list[str] = ["t.amount < 0"]
    params: list[object] = []

    if start_date and end_date:
        conditions.append("t.transaction_year_month >= ?")
        params.append(start_date[:7])
        conditions.append("t.transaction_year_month <= ?")
        params.append(end_date[:7])

    if account_id:
        placeholders = ", ".join(["?"] * len(account_id))
        conditions.append(f"t.account_id IN ({placeholders})")
        params.extend(account_id)

    where = "WHERE " + " AND ".join(conditions)

    # Date filter for month-based lookback
    month_limit = ""
    if not start_date:
        month_limit = f"""
            AND t.transaction_year_month IN (
                SELECT DISTINCT transaction_year_month
                FROM {FCT_TRANSACTIONS.full_name}
                ORDER BY transaction_year_month DESC
                LIMIT ?
            )
        """
        params.append(months)

    sql = f"""
        WITH categorized AS (
            SELECT
                COALESCE(c.category, 'Uncategorized') AS category,
                c.subcategory,
                SUM(ABS(t.amount)) AS total,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
            {month_limit}
            GROUP BY COALESCE(c.category, 'Uncategorized'), c.subcategory
        ),
        grand_total AS (
            SELECT SUM(total) AS grand_total FROM categorized
        )
        SELECT
            category,
            subcategory,
            total,
            transaction_count,
            ROUND(total * 100.0 / NULLIF(gt.grand_total, 0), 1) AS percent_of_total,
            gt.grand_total
        FROM categorized, grand_total gt
        ORDER BY total DESC
    """

    result = self._db.execute(sql, params)
    rows = result.fetchall()

    total_spending = float(rows[0][5]) if rows else 0.0

    categories = []
    for row in rows:
        cat_name = row[0]
        if cat_name == "Uncategorized" and not include_uncategorized:
            continue
        categories.append(
            CategoryRow(
                category=cat_name,
                subcategory=row[1],
                total=float(row[2]),
                transaction_count=int(row[3]),
                percent_of_total=float(row[4] or 0),
            )
        )

    if top_n and len(categories) > top_n:
        categories = categories[:top_n]

    # Build period string
    period = self._resolve_period(months, start_date, end_date)

    return CategoryBreakdown(
        categories=categories,
        total_spending=total_spending,
        period=period,
    )


def _resolve_period(
    self,
    months: int,
    start_date: str | None,
    end_date: str | None,
) -> str:
    """Build a human-readable period string."""
    if start_date and end_date:
        return f"{start_date[:7]} to {end_date[:7]}"

    result = self._db.execute(
        f"""
        SELECT
            MIN(transaction_year_month),
            MAX(transaction_year_month)
        FROM (
            SELECT DISTINCT transaction_year_month
            FROM {FCT_TRANSACTIONS.full_name}
            ORDER BY transaction_year_month DESC
            LIMIT ?
        )
    """,
        [months],
    )
    row = result.fetchone()
    if row and row[0]:
        return f"{row[0]} to {row[1]}"
    return ""
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py -v`
Expected: All tests PASS (both TestSpendingSummary and TestSpendingByCategory)

- [ ] **Step 5: Write failing tests for merchants() and compare()**

Add to `tests/moneybin/test_services/test_spending_service.py`:

```python
class TestSpendingMerchants:
    """SpendingService.merchants() returns top merchants by spend."""

    def test_returns_merchants_sorted_by_total(
        self, db: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db)
        result = svc.merchants(months=3)

        assert len(result.merchants) > 0
        totals = [m.total for m in result.merchants]
        assert totals == sorted(totals, reverse=True)

    def test_top_n_limits_results(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.merchants(months=3, top_n=1)

        assert len(result.merchants) == 1

    def test_merchant_has_last_seen(self, db: duckdb.DuckDBPyConnection) -> None:
        svc = SpendingService(db)
        result = svc.merchants(months=3)

        for m in result.merchants:
            assert m.last_seen  # non-empty date string


class TestSpendingCompare:
    """SpendingService.compare() compares two periods."""

    def test_compare_two_months(
        self, db_with_categories: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db_with_categories)
        result = svc.compare(period_a="2026-03", period_b="2026-04")

        assert result.period_a == "2026-03"
        assert result.period_b == "2026-04"
        assert len(result.categories) > 0

    def test_compare_includes_change(
        self, db_with_categories: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db_with_categories)
        result = svc.compare(period_a="2026-03", period_b="2026-04")

        for row in result.categories:
            expected_change = row.period_b_total - row.period_a_total
            assert abs(row.change_amount - expected_change) < 0.01

    def test_compare_totals(
        self, db_with_categories: duckdb.DuckDBPyConnection
    ) -> None:
        svc = SpendingService(db_with_categories)
        result = svc.compare(period_a="2026-03", period_b="2026-04")

        assert result.period_a_total >= 0
        assert result.period_b_total >= 0
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py::TestSpendingMerchants -v`
Expected: FAIL — `AttributeError`

- [ ] **Step 7: Implement merchants() and compare()**

Add to `SpendingService` in `src/moneybin/services/spending_service.py`:

```python
def merchants(
    self,
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 20,
) -> MerchantBreakdown:
    """Get top merchants by spending.

    Args:
        months: Number of recent months.
        start_date: ISO 8601 start date (overrides months).
        end_date: ISO 8601 end date.
        account_id: Filter to specific accounts.
        top_n: Number of merchants to return.

    Returns:
        MerchantBreakdown with per-merchant totals.
    """
    from moneybin.tables import MERCHANTS, TRANSACTION_CATEGORIES

    conditions: list[str] = ["t.amount < 0"]
    params: list[object] = []

    if start_date and end_date:
        conditions.append("t.transaction_year_month >= ?")
        params.append(start_date[:7])
        conditions.append("t.transaction_year_month <= ?")
        params.append(end_date[:7])

    if account_id:
        placeholders = ", ".join(["?"] * len(account_id))
        conditions.append(f"t.account_id IN ({placeholders})")
        params.extend(account_id)

    where = "WHERE " + " AND ".join(conditions)

    month_limit = ""
    if not start_date:
        month_limit = f"""
            AND t.transaction_year_month IN (
                SELECT DISTINCT transaction_year_month
                FROM {FCT_TRANSACTIONS.full_name}
                ORDER BY transaction_year_month DESC
                LIMIT ?
            )
        """
        params.append(months)

    params.append(top_n)

    sql = f"""
        SELECT
            COALESCE(m.canonical_name, t.description) AS merchant_name,
            SUM(ABS(t.amount)) AS total,
            COUNT(*) AS transaction_count,
            MAX(c.category) AS category,
            MAX(t.transaction_date)::VARCHAR AS last_seen
        FROM {FCT_TRANSACTIONS.full_name} t
        LEFT JOIN {MERCHANTS.full_name} m
            ON t.description ILIKE '%' || m.raw_pattern || '%'
            AND m.match_type = 'contains'
        LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
            ON t.transaction_id = c.transaction_id
        {where}
        {month_limit}
        GROUP BY COALESCE(m.canonical_name, t.description)
        ORDER BY total DESC
        LIMIT ?
    """

    result = self._db.execute(sql, params)
    rows = result.fetchall()

    merchants_list = [
        MerchantRow(
            merchant_name=row[0],
            total=float(row[1]),
            transaction_count=int(row[2]),
            category=row[3],
            last_seen=str(row[4]),
        )
        for row in rows
    ]

    period = self._resolve_period(months, start_date, end_date)

    return MerchantBreakdown(merchants=merchants_list, period=period)


def compare(
    self,
    period_a: str,
    period_b: str,
    account_id: list[str] | None = None,
) -> PeriodComparison:
    """Compare spending between two periods.

    Args:
        period_a: First period (YYYY-MM).
        period_b: Second period (YYYY-MM).
        account_id: Filter to specific accounts.

    Returns:
        PeriodComparison with per-category change amounts.
    """
    from moneybin.tables import TRANSACTION_CATEGORIES

    account_filter = ""
    params: list[object] = [period_a, period_b]

    if account_id:
        placeholders = ", ".join(["?"] * len(account_id))
        account_filter = f"AND t.account_id IN ({placeholders})"
        params.extend(account_id)

    sql = f"""
        WITH spending AS (
            SELECT
                COALESCE(c.category, 'Uncategorized') AS category,
                t.transaction_year_month AS period,
                SUM(ABS(t.amount)) AS total
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            WHERE t.amount < 0
                AND t.transaction_year_month IN (?, ?)
                {account_filter}
            GROUP BY COALESCE(c.category, 'Uncategorized'),
                     t.transaction_year_month
        )
        SELECT
            category,
            SUM(CASE WHEN period = ? THEN total ELSE 0 END) AS period_a_total,
            SUM(CASE WHEN period = ? THEN total ELSE 0 END) AS period_b_total
        FROM spending
        GROUP BY category
        ORDER BY GREATEST(
            SUM(CASE WHEN period = ? THEN total ELSE 0 END),
            SUM(CASE WHEN period = ? THEN total ELSE 0 END)
        ) DESC
    """
    # Add period_a/period_b for the CASE expressions and ORDER BY
    params.extend([period_a, period_b, period_a, period_b])

    result = self._db.execute(sql, params)
    rows = result.fetchall()

    categories = []
    total_a = 0.0
    total_b = 0.0
    for row in rows:
        a = float(row[1])
        b = float(row[2])
        change = b - a
        pct = (change / a * 100) if a > 0 else None
        categories.append(
            PeriodComparisonRow(
                category=row[0],
                period_a_total=a,
                period_b_total=b,
                change_amount=change,
                change_percent=pct,
            )
        )
        total_a += a
        total_b += b

    return PeriodComparison(
        period_a=period_a,
        period_b=period_b,
        categories=categories,
        period_a_total=total_a,
        period_b_total=total_b,
    )
```

- [ ] **Step 8: Run all SpendingService tests**

Run: `uv run pytest tests/moneybin/test_services/test_spending_service.py -v`
Expected: All tests PASS

- [ ] **Step 9: Commit**

```bash
git add src/moneybin/services/spending_service.py tests/moneybin/test_services/test_spending_service.py
git commit -m "feat: add by_category, merchants, compare to SpendingService"
```

---

## Task 5: MCP spending tools

Thin MCP wrappers over `SpendingService`, using the response envelope and middleware.

**Files:**
- Create: `src/moneybin/mcp/tools/__init__.py`
- Create: `src/moneybin/mcp/tools/spending.py`
- Test: `tests/moneybin/test_mcp/test_spending_tools.py`

- [ ] **Step 1: Create the tools package**

```python
# src/moneybin/mcp/tools/__init__.py
"""MCP tool modules — one file per namespace.

Each module registers tools with the FastMCP server instance via
@mcp.tool(name='namespace.action') decorators.
"""
```

- [ ] **Step 2: Write failing tests for spending MCP tools**

```python
# tests/moneybin/test_mcp/test_spending_tools.py
"""Tests for spending.* MCP tools."""

import json

import pytest

from moneybin.tables import TRANSACTION_CATEGORIES

# Import the tool module to register tools with the server
import moneybin.mcp.tools.spending  # noqa: F401 — registers tools on import
from moneybin.mcp.tools.spending import (
    spending_by_category,
    spending_compare,
    spending_merchants,
    spending_summary,
)


class TestSpendingSummaryTool:
    """spending.summary MCP tool."""

    @pytest.fixture(autouse=True)
    def _setup(self, mcp_db):  # noqa: ANN001 — fixture from conftest
        """Insert transactions for spending tests."""
        from moneybin.mcp.server import get_write_db

        with get_write_db() as db:
            db.execute("""
                INSERT INTO core.fct_transactions
                (transaction_id, account_id, transaction_type, transaction_date,
                 amount, description, source_system, source_file,
                 transaction_direction, transaction_year_month, amount_absolute,
                 extracted_at, loaded_at)
                VALUES
                ('tx1', 'ACC001', 'CREDIT', '2026-04-15', 5200.00, 'PAYROLL',
                 'ofx', 'test.qfx', 'credit', '2026-04', 5200.00,
                 '2026-04-15', CURRENT_TIMESTAMP),
                ('tx2', 'ACC001', 'DEBIT', '2026-04-10', -42.50, 'WHOLEFDS',
                 'ofx', 'test.qfx', 'debit', '2026-04', 42.50,
                 '2026-04-10', CURRENT_TIMESTAMP)
            """)

    def test_returns_envelope_shape(self) -> None:
        result = spending_summary(months=1)
        parsed = json.loads(result)

        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    def test_returns_monthly_data(self) -> None:
        result = spending_summary(months=1)
        parsed = json.loads(result)

        assert parsed["summary"]["total_count"] == 1
        assert len(parsed["data"]) == 1
        assert parsed["data"][0]["period"] == "2026-04"
        assert parsed["data"][0]["income"] == 5200.00

    def test_includes_actions(self) -> None:
        result = spending_summary(months=1)
        parsed = json.loads(result)

        assert len(parsed["actions"]) > 0


class TestSpendingByCategoryTool:
    """spending.by_category MCP tool."""

    @pytest.fixture(autouse=True)
    def _setup(self, mcp_db):  # noqa: ANN001 — fixture from conftest
        from moneybin.mcp.server import get_write_db

        with get_write_db() as db:
            db.execute("""
                INSERT INTO core.fct_transactions
                (transaction_id, account_id, transaction_type, transaction_date,
                 amount, description, source_system, source_file,
                 transaction_direction, transaction_year_month, amount_absolute,
                 extracted_at, loaded_at)
                VALUES
                ('tx1', 'ACC001', 'DEBIT', '2026-04-10', -42.50, 'WHOLEFDS',
                 'ofx', 'test.qfx', 'debit', '2026-04', 42.50,
                 '2026-04-10', CURRENT_TIMESTAMP)
            """)
            db.execute(f"""
                INSERT INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory, categorized_at, categorized_by)
                VALUES ('tx1', 'Food & Drink', 'Groceries', CURRENT_TIMESTAMP, 'user')
            """)

    def test_returns_envelope_with_categories(self) -> None:
        result = spending_by_category(months=1)
        parsed = json.loads(result)

        assert parsed["summary"]["sensitivity"] == "low"
        assert len(parsed["data"]) > 0
        assert parsed["data"][0]["category"] == "Food & Drink"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_spending_tools.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 4: Implement spending MCP tools**

```python
# src/moneybin/mcp/tools/spending.py
"""spending.* MCP tools — expense analysis.

Thin wrappers over SpendingService. No business logic, no SQL.
See docs/specs/mcp-tool-surface.md section 3.
"""

import logging

from moneybin.mcp.envelope import ResponseEnvelope
from moneybin.mcp.middleware import tool_meta
from moneybin.mcp.server import get_db, mcp
from moneybin.services.spending_service import SpendingService

logger = logging.getLogger(__name__)


@mcp.tool(name="spending.summary")
@tool_meta(sensitivity="low", tool_name="spending.summary")
def spending_summary(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    detail: str = "standard",
) -> str:
    """Get income vs expense totals by month.

    Returns time-series data suitable for charting. Use `months` for recent
    history or `start_date`/`end_date` for a specific range.

    Args:
        months: Number of recent months to include (default 3).
        start_date: ISO 8601 start date (overrides months).
        end_date: ISO 8601 end date.
        account_id: Filter to specific accounts.
        detail: 'summary' (totals only), 'standard' (monthly breakdown),
            'full' (adds per-account splits).
    """
    svc = SpendingService(get_db())
    result = svc.summary(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    data = [
        {
            "period": m.period,
            "income": m.income,
            "expenses": m.expenses,
            "net": m.net,
            "transaction_count": m.transaction_count,
        }
        for m in result.months
    ]

    period = (
        f"{result.date_range_start} to {result.date_range_end}"
        if result.date_range_start
        else None
    )

    env = ResponseEnvelope(
        data=data,
        total_count=len(data),
        sensitivity="low",
        period=period,
        display_currency="USD",
        actions=[
            "Use spending.by_category for category breakdown",
            "Use spending.compare to compare periods",
        ],
    )
    return env.to_json()


@mcp.tool(name="spending.by_category")
@tool_meta(sensitivity="low", tool_name="spending.by_category")
def spending_by_category(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 10,
    include_uncategorized: bool = True,
    detail: str = "standard",
) -> str:
    """Get spending breakdown by category for a period.

    Requires transactions to be categorized. Returns categories sorted by
    total spending descending.

    Args:
        months: Number of recent months (default 3).
        start_date: ISO 8601 start date (overrides months).
        end_date: ISO 8601 end date.
        account_id: Filter to specific accounts.
        top_n: Limit to top N categories (default 10).
        include_uncategorized: Include uncategorized spending rollup (default true).
        detail: 'summary' (total only), 'standard' (category list),
            'full' (per-month within each category).
    """
    svc = SpendingService(get_db())
    result = svc.by_category(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        top_n=top_n,
        include_uncategorized=include_uncategorized,
    )

    data = [
        {
            "category": c.category,
            "subcategory": c.subcategory,
            "total": c.total,
            "transaction_count": c.transaction_count,
            "percent_of_total": c.percent_of_total,
        }
        for c in result.categories
    ]

    env = ResponseEnvelope(
        data=data,
        total_count=len(data),
        sensitivity="low",
        period=result.period,
        display_currency="USD",
        actions=[
            "Use spending.merchants for merchant-level breakdown",
            "Use spending.compare to compare to another period",
        ],
    )
    return env.to_json()


@mcp.tool(name="spending.merchants")
@tool_meta(sensitivity="medium", tool_name="spending.merchants")
def spending_merchants(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 20,
) -> str:
    """Get top merchants by spending for a period.

    Returns merchant names with totals, transaction counts, and categories.
    Merchants without mappings appear by raw description.

    Args:
        months: Number of recent months (default 3).
        start_date: ISO 8601 start date (overrides months).
        end_date: ISO 8601 end date.
        account_id: Filter to specific accounts.
        top_n: Number of merchants to return (default 20).
    """
    svc = SpendingService(get_db())
    result = svc.merchants(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        top_n=top_n,
    )

    data = [
        {
            "merchant_name": m.merchant_name,
            "total": m.total,
            "transaction_count": m.transaction_count,
            "category": m.category,
            "last_seen": m.last_seen,
        }
        for m in result.merchants
    ]

    env = ResponseEnvelope(
        data=data,
        total_count=len(data),
        sensitivity="medium",
        period=result.period,
        display_currency="USD",
        actions=[
            "Use categorize.create_merchants to normalize merchant names",
        ],
    )
    return env.to_json()


@mcp.tool(name="spending.compare")
@tool_meta(sensitivity="low", tool_name="spending.compare")
def spending_compare(
    period_a: str,
    period_b: str,
    account_id: list[str] | None = None,
) -> str:
    """Compare spending between two periods (month-over-month, year-over-year).

    Returns per-category comparison with change amounts and percentages.

    Args:
        period_a: First period (YYYY-MM).
        period_b: Second period (YYYY-MM).
        account_id: Filter to specific accounts.
    """
    svc = SpendingService(get_db())
    result = svc.compare(
        period_a=period_a,
        period_b=period_b,
        account_id=account_id,
    )

    data = [
        {
            "category": c.category,
            "period_a_total": c.period_a_total,
            "period_b_total": c.period_b_total,
            "change_amount": c.change_amount,
            "change_percent": c.change_percent,
        }
        for c in result.categories
    ]

    env = ResponseEnvelope(
        data=data,
        total_count=len(data),
        sensitivity="low",
        period=f"{result.period_a} vs {result.period_b}",
        display_currency="USD",
        actions=[
            "Use spending.by_category for detailed breakdown of either period",
        ],
    )
    return env.to_json()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_spending_tools.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/mcp/tools/__init__.py src/moneybin/mcp/tools/spending.py tests/moneybin/test_mcp/test_spending_tools.py
git commit -m "feat: add spending.* MCP tools with response envelope"
```

---

## Task 6: CLI output helpers and spending command group

Shared `--output json` support and the `spending` CLI command group.

**Files:**
- Create: `src/moneybin/cli/output.py`
- Create: `src/moneybin/cli/commands/spending.py`
- Modify: `src/moneybin/cli/main.py`
- Test: `tests/moneybin/test_cli/test_spending_cli.py`

- [ ] **Step 1: Create CLI output helpers**

```python
# src/moneybin/cli/output.py
"""CLI output formatting helpers.

Provides --output json support and table rendering for CLI commands.
When --output json is used, the command returns the same ResponseEnvelope
as the MCP tool. Otherwise, output is a human-readable table.
"""

import json
import logging
import sys
from enum import Enum

logger = logging.getLogger(__name__)


class OutputFormat(str, Enum):
    """Output format for CLI commands."""

    TABLE = "table"
    JSON = "json"


def print_json(envelope_dict: dict) -> None:
    """Print a response envelope as formatted JSON to stdout.

    Args:
        envelope_dict: The ResponseEnvelope.to_dict() result.
    """
    json.dump(envelope_dict, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def print_table(headers: list[str], rows: list[list[str]], title: str = "") -> None:
    """Print a simple aligned table to stdout.

    Args:
        headers: Column header names.
        rows: List of rows, each a list of string values.
        title: Optional title line above the table.
    """
    if title:
        logger.info(title)

    if not rows:
        logger.info("No data to display.")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))

    # Format header
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    separator = "  ".join("-" * w for w in widths)

    logger.info(header_line)
    logger.info(separator)
    for row in rows:
        line = "  ".join(
            (row[i] if i < len(row) else "").ljust(widths[i])
            for i in range(len(headers))
        )
        logger.info(line)
```

- [ ] **Step 2: Write failing tests for spending CLI**

```python
# tests/moneybin/test_cli/test_spending_cli.py
"""Tests for the spending CLI command group."""

import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.spending_service import MonthlySpending, SpendingSummary

runner = CliRunner()


@pytest.fixture(autouse=True)
def mock_spending_service(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock SpendingService to avoid database dependency in CLI tests."""
    mock_svc = MagicMock()
    mock_svc.summary.return_value = SpendingSummary(
        months=[
            MonthlySpending(
                period="2026-04",
                income=5200.00,
                expenses=3847.32,
                net=1352.68,
                transaction_count=87,
            ),
        ],
        date_range_start="2026-04",
        date_range_end="2026-04",
    )

    mock_cls = MagicMock(return_value=mock_svc)
    monkeypatch.setattr("moneybin.cli.commands.spending.SpendingService", mock_cls)
    return mock_svc


class TestSpendingSummaryCLI:
    """moneybin spending summary command."""

    def test_json_output(self, mock_spending_service: MagicMock) -> None:
        result = runner.invoke(app, ["spending", "summary", "--output", "json"])
        assert result.exit_code == 0

        parsed = json.loads(result.stdout)
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["data"][0]["period"] == "2026-04"

    def test_table_output_default(self, mock_spending_service: MagicMock) -> None:
        result = runner.invoke(app, ["spending", "summary"])
        assert result.exit_code == 0

    def test_months_parameter(self, mock_spending_service: MagicMock) -> None:
        runner.invoke(app, ["spending", "summary", "--months", "6"])
        mock_spending_service.summary.assert_called_once()
        call_kwargs = mock_spending_service.summary.call_args
        assert (
            call_kwargs.kwargs.get("months") == 6 or call_kwargs[1].get("months") == 6
        )
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_spending_cli.py -v`
Expected: FAIL — `ModuleNotFoundError` or `No such command 'spending'`

- [ ] **Step 4: Implement spending CLI command group**

```python
# src/moneybin/cli/commands/spending.py
"""CLI commands for spending analysis.

Thin wrappers over SpendingService. Mirrors the spending.* MCP tools.
"""

import logging
from typing import Annotated

import duckdb
import typer

from moneybin.cli.output import OutputFormat, print_json, print_table
from moneybin.config import get_database_path
from moneybin.mcp.envelope import ResponseEnvelope
from moneybin.services.spending_service import SpendingService

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="spending",
    help="Analyze spending patterns — summaries, categories, merchants, comparisons",
    no_args_is_help=True,
)


def _get_service() -> SpendingService:
    """Create a SpendingService with a read-only DB connection."""
    db_path = get_database_path()
    conn = duckdb.connect(str(db_path), read_only=True)
    return SpendingService(conn)


@app.command("summary")
def summary(
    months: Annotated[int, typer.Option(help="Number of recent months to include")] = 3,
    start_date: Annotated[
        str | None, typer.Option("--start-date", help="ISO 8601 start date")
    ] = None,
    end_date: Annotated[
        str | None, typer.Option("--end-date", help="ISO 8601 end date")
    ] = None,
    account_id: Annotated[
        list[str] | None, typer.Option("--account-id", help="Filter to accounts")
    ] = None,
    output: Annotated[
        OutputFormat, typer.Option("--output", help="Output format")
    ] = OutputFormat.TABLE,
) -> None:
    """Get income vs expense totals by month."""
    svc = _get_service()
    result = svc.summary(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    if output == OutputFormat.JSON:
        data = [
            {
                "period": m.period,
                "income": m.income,
                "expenses": m.expenses,
                "net": m.net,
                "transaction_count": m.transaction_count,
            }
            for m in result.months
        ]
        period = (
            f"{result.date_range_start} to {result.date_range_end}"
            if result.date_range_start
            else None
        )
        env = ResponseEnvelope(
            data=data,
            total_count=len(data),
            sensitivity="low",
            period=period,
            display_currency="USD",
            actions=[
                "Use spending.by_category for category breakdown",
                "Use spending.compare to compare periods",
            ],
        )
        print_json(env.to_dict())
    else:
        headers = ["Period", "Income", "Expenses", "Net", "Count"]
        rows = [
            [
                m.period,
                f"${m.income:,.2f}",
                f"${m.expenses:,.2f}",
                f"${m.net:,.2f}",
                str(m.transaction_count),
            ]
            for m in result.months
        ]
        print_table(headers, rows)
```

- [ ] **Step 5: Register spending command group in main.py**

Add to `src/moneybin/cli/main.py`, after the existing imports:

```python
from .commands import config, data, db, import_cmd, mcp, spending, sync
```

And add the command group registration after the existing `app.add_typer` calls:

```python
app.add_typer(
    spending.app,
    name="spending",
    help="Analyze spending patterns — summaries, categories, merchants, comparisons",
)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_cli/test_spending_cli.py -v`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/output.py src/moneybin/cli/commands/spending.py src/moneybin/cli/main.py tests/moneybin/test_cli/test_spending_cli.py
git commit -m "feat: add spending CLI command group with --output json support"
```

---

## Task 7: Register new tools with MCP server

Update the MCP server to import the new tool modules so they register alongside (not replacing) prototype tools during the transition.

**Files:**
- Modify: `src/moneybin/mcp/__init__.py`

- [ ] **Step 1: Update MCP __init__.py to import new tool modules**

Replace the contents of `src/moneybin/mcp/__init__.py`:

```python
"""MCP server for MoneyBin — AI-powered personal finance.

This package provides a Model Context Protocol (MCP) server that gives
AI assistants full access to financial data management: importing bank
statements, querying transactions, categorizing spending, and budgeting.
All data stays local in a DuckDB database — nothing is sent externally.

Tool modules in mcp/tools/ register themselves with the FastMCP server
on import. Each file corresponds to one namespace (e.g., spending.py
registers spending.summary, spending.by_category, etc.).
"""

# Import tool modules to trigger @mcp.tool() registration.
# Prototype tools (tools.py, write_tools.py) remain active during migration.
import moneybin.mcp.tools.spending  # noqa: F401 — registers spending.* tools
```

- [ ] **Step 2: Run the full test suite to verify nothing broke**

Run: `uv run pytest tests/ -v`
Expected: All existing tests PASS, new tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/mcp/__init__.py
git commit -m "feat: register spending.* tools with MCP server"
```

---

## Task 8: Lint, type-check, full test suite

Final verification that everything works together.

**Files:** None — verification only.

- [ ] **Step 1: Run ruff format and check**

Run: `uv run ruff format . && uv run ruff check .`
Expected: No errors. Fix any issues.

- [ ] **Step 2: Run pyright on new files**

Run: `uv run pyright src/moneybin/mcp/envelope.py src/moneybin/mcp/middleware.py src/moneybin/services/spending_service.py src/moneybin/mcp/tools/spending.py src/moneybin/cli/output.py src/moneybin/cli/commands/spending.py`
Expected: No errors. Fix any type issues.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A
git commit -m "chore: lint and type-check fixes for spending namespace"
```

---

## Subsequent plans

This plan establishes the foundation. The following plans each add one namespace (or related group) following the same pattern: service class → tests → MCP wrappers → CLI commands → commit.

| Plan | Namespaces | Estimated tasks |
|---|---|---|
| **Plan 2** | `cashflow.*`, `accounts.*` | 6 tasks |
| **Plan 3** | `transactions.*` (search, correct, annotate, recurring) | 5 tasks |
| **Plan 4** | `import.*` (file, status, csv_preview, csv_profiles, csv_save_profile) | 4 tasks (mostly migrating existing service) |
| **Plan 5** | `categorize.*` (shippable tools only) | 5 tasks (mostly migrating existing service) |
| **Plan 6** | `budget.*`, `tax.*` | 4 tasks |
| **Plan 7** | `overview.*`, `sql.*` | 3 tasks |
| **Plan 8** | `privacy.*` (stubs) | 2 tasks |
| **Plan 9** | Prompts, resources, migration cleanup | 4 tasks |
| **Plan 10** | `transactions.matches.*` | Blocked on transaction matching spec |
| **Plan 11** | `categorize.*` ML/auto-rule tools | Blocked on categorization umbrella spec |
| **Plan 12** | `import.*` AI/folder tools | Blocked on Smart Import specs |

Plans 2-9 can execute sequentially or in parallel (in separate worktrees) since they share only the foundation infrastructure built in this plan.
