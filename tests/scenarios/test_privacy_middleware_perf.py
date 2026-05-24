"""Post-middleware perf regression: assert deltas vs pre-PR-2 baseline.

The baseline was captured in Phase 0 of PR 2 against the persona documented
in ``docs/specs/privacy-data-classification.md`` "Performance validation"
(family × 3 years, seed 8229). This test re-runs the same five flows
post-middleware and fails if any flow exceeds the per-flow or aggregate
budget.

**Budgets:**

- p50 latency increase: ≤ 50 ms per flow
- p99 latency increase: ≤ 200 ms per flow
- Total wall-clock regression on the full flow set: ≤ 20 %

The test is marked ``@pytest.mark.perf`` (declared in pyproject.toml). The
``perf`` marker is opt-in — `make test` does not run it; the runner needs
a populated persona DB and a profile selected via ``MONEYBIN_HOME`` +
``MONEYBIN_PROFILE`` (per the Phase 9 instructions in
``private/plans/privacy-middleware-pr2.md`` §Task 9.1 Step 2).

If the test runs in an environment without a populated DB, it skips with
a clear message rather than failing — the budget assertion is meaningful
only against a representative dataset.

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

from datetime import date, timedelta
from pathlib import Path

import pytest

from moneybin.database import get_database
from moneybin.errors import DatabaseKeyError
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.sql_lineage import (
    expand_star,
    get_current_schema_snapshot,
    parse_cached,
    resolve_output_classes,
)
from moneybin.services.account_service import AccountService
from moneybin.services.budget_service import BudgetService
from moneybin.services.networth_service import NetworthService
from moneybin.services.reports_service import ReportsService
from moneybin.services.transaction_service import TransactionService
from tests.scenarios._perf_runner import measure_flow, read_baseline

BASELINE_PATH = Path("tests/scenarios/fixtures/perf_baseline_pre_privacy.json")
ITERATIONS = 30
P50_BUDGET_MS = 50.0
P99_BUDGET_MS = 200.0
TOTAL_REGRESSION_PCT = 20.0

# 12-month window for networth_history matches the default-window used by
# reports_networth_history when called without dates.
_HISTORY_FROM = date.today() - timedelta(days=365)
_HISTORY_TO = date.today()


def _persona_db_available() -> bool:
    """Probe whether a populated persona DB is reachable.

    The runner opens the configured DB read-only and runs a cheap count
    against ``core.fct_transactions``. Anything that prevents that — no
    profile, sealed DB, empty DB, missing core tables — counts as
    unavailable and the test skips.
    """
    try:
        with get_database(read_only=True) as db:
            (count,) = db.execute(
                "SELECT COUNT(*) FROM core.fct_transactions"
            ).fetchone() or (0,)
            return count > 0
    except (DatabaseKeyError, Exception):  # noqa: BLE001 — any open/query error → skip
        return False


@pytest.mark.perf
def test_privacy_middleware_within_budget() -> None:
    """Re-run baseline flows post-middleware; assert deltas within budget."""
    if not _persona_db_available():
        pytest.skip(
            "perf baseline test requires a populated persona DB; set "
            "MONEYBIN_HOME + MONEYBIN_PROFILE and run "
            "`moneybin synthetic generate family --seed 8229 --years 3 "
            "&& moneybin transform apply` first"
        )

    baseline = read_baseline(BASELINE_PATH)

    def _transactions_get() -> object:
        with get_database(read_only=True) as db:
            return TransactionService(db).get(limit=100)

    def _reports_spending() -> object:
        with get_database(read_only=True) as db:
            return ReportsService(db).spending_trend()

    def _accounts() -> object:
        with get_database(read_only=True) as db:
            return AccountService(db).list_accounts()

    def _reports_budget() -> object:
        with get_database(read_only=True) as db:
            return BudgetService(db).status()

    def _reports_networth_history() -> object:
        with get_database(read_only=True) as db:
            return NetworthService(db).history(
                from_date=_HISTORY_FROM, to_date=_HISTORY_TO
            )

    flows = {
        "transactions_get": _transactions_get,
        "reports_spending": _reports_spending,
        "accounts": _accounts,
        "reports_budget": _reports_budget,
        "reports_networth_history": _reports_networth_history,
    }

    deltas: list[tuple[str, float, float]] = []
    total_baseline_p50 = 0.0
    total_current_p50 = 0.0
    for name, fn in flows.items():
        base = baseline.get(name)
        assert base is not None, f"missing baseline for {name}"
        current = measure_flow(name, fn, iterations=ITERATIONS)
        d_p50 = current.p50_ms - base.p50_ms
        d_p99 = current.p99_ms - base.p99_ms
        deltas.append((name, d_p50, d_p99))
        total_baseline_p50 += base.p50_ms
        total_current_p50 += current.p50_ms
        assert d_p50 <= P50_BUDGET_MS, (
            f"{name}: p50 regressed by {d_p50:+.2f}ms (cap {P50_BUDGET_MS}ms); "
            f"baseline {base.p50_ms:.2f}ms, current {current.p50_ms:.2f}ms"
        )
        assert d_p99 <= P99_BUDGET_MS, (
            f"{name}: p99 regressed by {d_p99:+.2f}ms (cap {P99_BUDGET_MS}ms); "
            f"baseline {base.p99_ms:.2f}ms, current {current.p99_ms:.2f}ms"
        )

    total_pct = (
        ((total_current_p50 - total_baseline_p50) / total_baseline_p50) * 100.0
        if total_baseline_p50 > 0
        else 0.0
    )
    assert total_pct <= TOTAL_REGRESSION_PCT, (
        f"Total p50 wall-clock regressed by {total_pct:+.1f}% "
        f"(cap {TOTAL_REGRESSION_PCT}%); per-flow deltas: {deltas}"
    )


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
    if not _persona_db_available():
        pytest.skip(
            "perf lineage test requires a populated persona DB; set "
            "MONEYBIN_HOME + MONEYBIN_PROFILE and run "
            "`moneybin synthetic generate family --seed 8229 --years 3 "
            "&& moneybin transform apply` first"
        )

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
