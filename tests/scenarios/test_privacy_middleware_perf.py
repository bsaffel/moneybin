"""Privacy-protected egress overhead gate on a populated family persona.

The baseline was captured in Phase 0 of PR 2 against the persona documented
in ``docs/specs/privacy-data-classification.md`` "Performance validation"
(family × 3 years, seed 8229). This test re-runs the same five flows
in paired raw and protected calls. The protected callback uses the production
``@mcp_tool`` decorator, so it measures egress overhead: typed redaction where
applicable, envelope handling, and the privacy audit write.

**Budgets:**

- p50 latency increase: ≤ 50 ms per flow
- p99 latency increase: ≤ 200 ms per flow
- Aggregate regression across the sums of five per-flow p50 values: ≤ 20 %

The tests carry ``scenarios`` and ``perf`` markers. A dedicated CI job runs
them serially against the family persona built by this module's fixture;
`make test` does not select them. Setup must produce a populated database
before measurements begin, so missing data cannot silently skip the gate.

**Diagnosing a regression.** If the budget is exceeded:

- Profile the slowest flow (``uv run python -m cProfile -s cumtime ...``).
- Common culprits: ``derive_tier`` cache miss (verify cache hit-rate
  >99 %), ``write_privacy_event`` blocking on disk I/O, ``redact_typed``
  rebuilding type hints on a hot loop (cache ``get_type_hints``
  results per-type).
- Diagnose and fix in a new commit; do not relax the budget without
  explicit user override.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections.abc import Callable, Generator
from datetime import date, timedelta
from typing import Any
from unittest.mock import patch

import duckdb
import pytest

from moneybin.database import DatabaseNotInitializedError, get_database
from moneybin.mcp import decorator as mcp_decorator
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.tools.accounts import accounts
from moneybin.mcp.tools.reports import reports
from moneybin.mcp.tools.transactions import transactions_get
from moneybin.privacy.payloads.accounts import AccountListPayload
from moneybin.privacy.payloads.budget import BudgetStatusPayload
from moneybin.privacy.payloads.networth import NetWorthHistoryPayload
from moneybin.privacy.payloads.transactions import TransactionGetPayload
from moneybin.privacy.redaction import redact_records, redact_typed
from moneybin.privacy.sql_lineage import (
    expand_star,
    get_current_schema_snapshot,
    parse_cached,
    resolve_output_classes,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.reports._framework import execute as report_execute
from moneybin.services.budget_service import BudgetService
from moneybin.services.networth_service import NetworthService
from tests.scenarios._perf_runner import FlowResult, measure_flow
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

pytestmark = [
    pytest.mark.scenarios,
    pytest.mark.slow,
    pytest.mark.usefixtures("build_perf_persona"),
]

logger = logging.getLogger(__name__)

ITERATIONS = 30
P50_BUDGET_MS = 50.0
P99_BUDGET_MS = 200.0
TOTAL_REGRESSION_PCT = 20.0

# 12-month window for networth_history matches the default-window used by
# reports_networth_history when called without dates.
_HISTORY_FROM = date.today() - timedelta(days=365)
_HISTORY_TO = date.today()
_PERSONA_SETUP_HINT = (
    "set MONEYBIN_HOME + MONEYBIN_PROFILE and run "
    "`moneybin synthetic generate family --seed 8229 --years 3 "
    "&& moneybin transform apply` first"
)


@pytest.fixture(scope="module")
def build_perf_persona() -> Generator[None, None, None]:
    """Build the documented family fixture before measuring its privacy budget."""
    scenario = Scenario(
        scenario="privacy-middleware-perf",
        setup=SetupSpec(persona="family", seed=8229, years=3),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmpdir, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)
        db.close()
        assert _persona_db_skip_reason() is None
        yield


def _persona_db_skip_reason() -> str | None:
    """Return why the perf persona DB is absent, or ``None`` when ready.

    The runner opens the configured DB read-only and runs a cheap count
    against ``core.fct_transactions``. Missing DBs, empty DBs, and DBs
    without transformed core tables report a setup problem. Key/open failures
    and malformed profile config are not caught; those are infrastructure
    problems and must fail visibly.
    """
    try:
        with get_database(read_only=True) as db:
            (count,) = db.execute(
                "SELECT COUNT(*) FROM core.fct_transactions"
            ).fetchone() or (0,)
    except DatabaseNotInitializedError:
        return (
            f"perf baseline test requires a populated persona DB; {_PERSONA_SETUP_HINT}"
        )
    except RuntimeError as e:
        # Keep in sync with config.py _get_settings() when no profile is active.
        if "No profile set" not in str(e):
            raise
        return f"perf baseline test requires an active MoneyBin profile; {_PERSONA_SETUP_HINT}"
    except duckdb.CatalogException as e:
        message = str(e)
        if "fct_transactions" not in message and "Schema with name core" not in message:
            raise
        return (
            "perf baseline test requires transformed core.fct_transactions; "
            f"{_PERSONA_SETUP_HINT}"
        )

    if count <= 0:
        return (
            f"perf baseline test requires a populated persona DB; {_PERSONA_SETUP_HINT}"
        )
    return None


@pytest.mark.perf
def test_privacy_middleware_within_budget() -> None:
    """Keep production privacy egress overhead inside the approved budgets."""

    def _transactions_raw() -> ResponseEnvelope[TransactionGetPayload]:
        return transactions_get(limit=100)

    @mcp_tool()
    def _transactions_protected() -> ResponseEnvelope[TransactionGetPayload]:
        return _transactions_raw()

    # The report route performs substantial non-privacy work after raw catalog
    # execution: limit selection, profile currency lookup, conversion,
    # truncation, payload construction, and envelope assembly. Both paths call
    # that exact route. The raw path bypasses only terminal record redaction and
    # the decorator's audit write; protected restores the production callables.
    # Matching patches on both paths control their context-manager overhead.
    report_redact_records = report_execute.redact_records
    write_privacy_event = mcp_decorator.write_privacy_event

    def _identity_report_redaction(
        records: list[dict[str, Any]], *_: object, **__: object
    ) -> list[dict[str, Any]]:
        return records

    def _skip_privacy_audit(*_: object, **__: object) -> None:
        return None

    def _spending_call(
        redactor: Callable[..., list[dict[str, Any]]],
        audit_writer: Callable[..., None],
    ) -> ResponseEnvelope[object]:
        with (
            patch.object(report_execute, "redact_records", redactor),
            patch.object(mcp_decorator, "write_privacy_event", audit_writer),
        ):
            return asyncio.run(reports(report_id="core:spending", limit=1000))

    def _spending_raw() -> ResponseEnvelope[object]:
        return _spending_call(_identity_report_redaction, _skip_privacy_audit)

    def _spending_protected() -> ResponseEnvelope[object]:
        return _spending_call(report_redact_records, write_privacy_event)

    def _accounts_raw() -> ResponseEnvelope[AccountListPayload]:
        return accounts()

    @mcp_tool()
    def _accounts_protected() -> ResponseEnvelope[AccountListPayload]:
        return accounts()

    # Budget status has no registered production egress. This test-only wrapper
    # measures the real decorator around the same service callback without
    # adding a public tool or a production switch.
    def _budget_raw() -> ResponseEnvelope[BudgetStatusPayload]:
        with get_database(read_only=True) as db:
            return build_envelope(data=BudgetService(db).status())

    @mcp_tool()
    def _budget_protected() -> ResponseEnvelope[BudgetStatusPayload]:
        return _budget_raw()

    def _networth_raw() -> ResponseEnvelope[NetWorthHistoryPayload]:
        with get_database(read_only=True) as db:
            return build_envelope(
                data=NetworthService(db).history(
                    from_date=_HISTORY_FROM, to_date=_HISTORY_TO
                )
            )

    @mcp_tool()
    def _networth_protected() -> ResponseEnvelope[NetWorthHistoryPayload]:
        return _networth_raw()

    def _assert_static_protection(
        raw: ResponseEnvelope[Any], protected: ResponseEnvelope[Any]
    ) -> None:
        assert protected.error is None
        assert protected.data == redact_typed(raw.data, consent=None)

    def _require_protected_success(
        protected: ResponseEnvelope[Any],
    ) -> ResponseEnvelope[Any]:
        """Keep timing an egress response, never a cheap error envelope."""
        assert protected.error is None
        return protected

    def _measure_counterbalanced(
        name: str, raw: Callable[[], object], protected: Callable[[], object]
    ) -> tuple[FlowResult, FlowResult]:
        """Pool alternating same-run samples so neither path owns warm cache."""
        raw()
        protected()
        raw_samples: list[float] = []
        protected_samples: list[float] = []

        def sample(fn: Callable[[], object], samples: list[float]) -> None:
            started = time.perf_counter_ns()
            fn()
            samples.append((time.perf_counter_ns() - started) / 1_000_000)

        for iteration in range(ITERATIONS):
            if iteration % 2 == 0:
                sample(raw, raw_samples)
                sample(protected, protected_samples)
            else:
                sample(protected, protected_samples)
                sample(raw, raw_samples)

        def result(path: str, samples: list[float]) -> FlowResult:
            ordered = sorted(samples)

            def percentile(pct: float) -> float:
                return ordered[int(round(pct * (len(ordered) - 1)))]

            return FlowResult(
                name=f"{name}_{path}",
                iterations=ITERATIONS,
                p50_ms=statistics.median(ordered),
                p95_ms=percentile(0.95),
                p99_ms=percentile(0.99),
            )

        return result("raw", raw_samples), result("protected", protected_samples)

    audit_before = _audit_event_count()

    transaction_raw = _transactions_raw()
    transaction_protected = asyncio.run(_transactions_protected())
    _assert_static_protection(transaction_raw, transaction_protected)

    accounts_raw = _accounts_raw()
    accounts_protected = asyncio.run(_accounts_protected())
    _assert_static_protection(accounts_raw, accounts_protected)
    assert accounts_protected.data != accounts_raw.data

    budget_raw = _budget_raw()
    budget_protected = asyncio.run(_budget_protected())
    _assert_static_protection(budget_raw, budget_protected)

    networth_raw = _networth_raw()
    networth_protected = asyncio.run(_networth_protected())
    _assert_static_protection(networth_raw, networth_protected)

    spending_audit_before = _audit_event_count()
    spending_raw = _spending_raw()
    spending_protected = _spending_protected()
    assert spending_raw.error is None
    assert spending_protected.error is None
    # core:spending contains no currently transformed classes, so its data is
    # equal after terminal redaction. The separate raw/protected callbacks still
    # retain the comparison when future report transforms begin changing rows.
    assert spending_protected.data == spending_raw.data
    assert _audit_event_count() == spending_audit_before + 1
    assert _audit_event_count() >= audit_before + 5

    flows = {
        "transactions_get": (
            _transactions_raw,
            lambda: _require_protected_success(asyncio.run(_transactions_protected())),
        ),
        "reports_spending": (
            _spending_raw,
            lambda: _require_protected_success(_spending_protected()),
        ),
        "accounts": (
            _accounts_raw,
            lambda: _require_protected_success(asyncio.run(_accounts_protected())),
        ),
        "budget_status_service": (
            _budget_raw,
            lambda: _require_protected_success(asyncio.run(_budget_protected())),
        ),
        "reports_networth_history": (
            _networth_raw,
            lambda: _require_protected_success(asyncio.run(_networth_protected())),
        ),
    }

    measurements: list[tuple[str, FlowResult, FlowResult, float, float]] = []
    total_raw_p50 = 0.0
    total_protected_p50 = 0.0
    for name, (raw, protected) in flows.items():
        raw_result, protected_result = _measure_counterbalanced(name, raw, protected)
        d_p50 = protected_result.p50_ms - raw_result.p50_ms
        d_p99 = protected_result.p99_ms - raw_result.p99_ms
        measurements.append((name, raw_result, protected_result, d_p50, d_p99))
        total_raw_p50 += raw_result.p50_ms
        total_protected_p50 += protected_result.p50_ms

    total_pct = (
        ((total_protected_p50 - total_raw_p50) / total_raw_p50) * 100.0
        if total_raw_p50 > 0
        else 0.0
    )
    timing_summary = [
        (
            name,
            raw.p50_ms,
            raw.p99_ms,
            protected.p50_ms,
            protected.p99_ms,
            p50_delta,
            p99_delta,
        )
        for name, raw, protected, p50_delta, p99_delta in measurements
    ]
    logger.info(
        "privacy protected-egress timings: "
        f"{timing_summary}; sum-of-p50 raw {total_raw_p50:.2f}ms, "
        f"protected {total_protected_p50:.2f}ms, overhead {total_pct:+.1f}%"
    )
    for name, raw_result, protected_result, d_p50, d_p99 in measurements:
        assert d_p50 <= P50_BUDGET_MS, (
            f"{name}: protected egress p50 overhead {d_p50:+.2f}ms exceeds "
            f"cap {P50_BUDGET_MS}ms (raw {raw_result.p50_ms:.2f}ms, "
            f"protected {protected_result.p50_ms:.2f}ms)"
        )
        assert d_p99 <= P99_BUDGET_MS, (
            f"{name}: protected egress p99 overhead {d_p99:+.2f}ms exceeds "
            f"cap {P99_BUDGET_MS}ms (raw {raw_result.p99_ms:.2f}ms, "
            f"protected {protected_result.p99_ms:.2f}ms)"
        )
    delta_summary = [
        (name, p50_delta, p99_delta)
        for name, _, _, p50_delta, p99_delta in measurements
    ]
    assert total_pct <= TOTAL_REGRESSION_PCT, (
        f"Sum of per-flow p50 protected-egress overhead {total_pct:+.1f}% "
        f"exceeds cap {TOTAL_REGRESSION_PCT}%; per-flow deltas: {delta_summary}"
    )


def _audit_event_count() -> int:
    """Count audit events without exposing their privacy-safe metadata."""
    from moneybin.config import get_base_dir, get_current_profile

    path = get_base_dir() / "profiles" / get_current_profile() / "privacy.log.jsonl"
    if not path.exists():
        return 0
    return sum(1 for _ in path.open())


# Representative agent query: mixes a CRITICAL column (masked) with HIGH/LOW/
# MEDIUM columns (passthrough), so the lineage flow exercises resolution +
# redaction, not just parsing.
_LINEAGE_PERF_SQL = (
    "SELECT account_id, amount, category, transaction_date "
    "FROM core.fct_transactions LIMIT 100"
)


@pytest.mark.perf
def test_sql_query_lineage_overhead_within_budget() -> None:
    """The sqlglot lineage + redaction PR 4 adds to sql_query stays under budget.

    sql_query has no pre-privacy baseline entry — before PR 4 it did no
    classification at all. So instead of a delta-vs-baseline, this measures the
    overhead PR 4 introduced *directly*: the same query run two ways — raw fetch
    (what sql_query did before) vs. the full lineage path (snapshot + parse +
    expand + resolve + redact_records, what it does now). The overhead is the
    difference, judged against the project's standard per-flow caps. The budget
    is independently derived (the PR's own contract: lineage adds ≤ P50_BUDGET_MS
    p50), not a paste of observed numbers.
    """

    def _raw() -> object:
        with get_database(read_only=True) as db:
            result = db.execute(_LINEAGE_PERF_SQL)
            columns = [d[0] for d in result.description]
            rows = result.fetchall()
        return [dict(zip(columns, r, strict=False)) for r in rows]

    def _with_lineage() -> object:
        with get_database(read_only=True) as db:
            snapshot = get_current_schema_snapshot(db)
            tree = expand_star(parse_cached(_LINEAGE_PERF_SQL), snapshot)
            output_classes = resolve_output_classes(tree, snapshot, _LINEAGE_PERF_SQL)
            result = db.execute(_LINEAGE_PERF_SQL)
            columns = [d[0] for d in result.description]
            rows = result.fetchall()
        records = [dict(zip(columns, r, strict=False)) for r in rows]
        return redact_records(records, output_classes)

    raw = measure_flow("sql_query_raw", _raw, iterations=ITERATIONS)
    lineage = measure_flow("sql_query_lineage", _with_lineage, iterations=ITERATIONS)
    overhead_p50 = lineage.p50_ms - raw.p50_ms
    overhead_p99 = lineage.p99_ms - raw.p99_ms

    assert overhead_p50 <= P50_BUDGET_MS, (
        f"sql_query lineage overhead p50 {overhead_p50:+.2f}ms exceeds cap "
        f"{P50_BUDGET_MS}ms (raw {raw.p50_ms:.2f}ms, lineage {lineage.p50_ms:.2f}ms)"
    )
    assert overhead_p99 <= P99_BUDGET_MS, (
        f"sql_query lineage overhead p99 {overhead_p99:+.2f}ms exceeds cap "
        f"{P99_BUDGET_MS}ms (raw {raw.p99_ms:.2f}ms, lineage {lineage.p99_ms:.2f}ms)"
    )
