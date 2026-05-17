"""Tests for reports_spending_get / reports_cashflow_get default-window behavior.

`_default_window` is pure math (no DB). The defaulted-tool behavior tests
need empty stubs for `reports.spending_trend` and `reports.cash_flow` so
the tool path executes its actions/period branch without hitting the
SQLMesh-built materializations (which `mcp_db_template` doesn't install).
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from moneybin.database import get_database
from moneybin.mcp.tools import reports as reports_tools


@pytest.fixture(autouse=True)
def _stub_reports_views(  # pyright: ignore[reportUnusedFunction]
    mcp_db: Path,  # noqa: ARG001 — mcp_db must run first to copy the template DB
) -> None:
    """Install empty `reports.cash_flow` and `reports.spending_trend` views.

    The SQLMesh-built materializations aren't part of the `mcp_db` template;
    these stubs let the tool execute and produce its envelope without the
    real views needing to exist.
    """
    with get_database() as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS reports")
        db.execute(
            "CREATE OR REPLACE VIEW reports.spending_trend AS "
            "SELECT CAST(NULL AS VARCHAR) AS year_month, "
            "CAST(NULL AS VARCHAR) AS category, "
            "CAST(NULL AS DECIMAL(18,2)) AS total_spend, "
            "CAST(NULL AS BIGINT) AS txn_count, "
            "CAST(NULL AS DECIMAL(18,2)) AS prev_month_spend, "
            "CAST(NULL AS DECIMAL(18,2)) AS mom_delta, "
            "CAST(NULL AS DOUBLE) AS mom_pct, "
            "CAST(NULL AS DECIMAL(18,2)) AS prev_year_spend, "
            "CAST(NULL AS DECIMAL(18,2)) AS yoy_delta, "
            "CAST(NULL AS DOUBLE) AS yoy_pct, "
            "CAST(NULL AS DECIMAL(18,2)) AS trailing_3mo_avg "
            "WHERE FALSE"
        )
        db.execute(
            "CREATE OR REPLACE VIEW reports.cash_flow AS "
            "SELECT CAST(NULL AS VARCHAR) AS year_month, "
            "CAST(NULL AS VARCHAR) AS account_id, "
            "CAST(NULL AS VARCHAR) AS account_name, "
            "CAST(NULL AS VARCHAR) AS category, "
            "CAST(NULL AS DECIMAL(18,2)) AS inflow, "
            "CAST(NULL AS DECIMAL(18,2)) AS outflow, "
            "CAST(NULL AS DECIMAL(18,2)) AS net, "
            "CAST(NULL AS BIGINT) AS txn_count "
            "WHERE FALSE"
        )
        # Encrypted-DB writes don't reach a separate read-only connection
        # until the WAL is flushed.
        db.execute("CHECKPOINT")


class TestDefaultWindow:
    """`_default_window` is pure date arithmetic."""

    @pytest.mark.parametrize(
        ("now", "months", "expected"),
        [
            # Middle of year — straight subtraction.
            (
                _dt.datetime(2026, 5, 17, tzinfo=_dt.UTC),
                12,
                ("2025-06", "2026-05"),
            ),
            # January — wraps to previous year.
            (
                _dt.datetime(2026, 1, 5, tzinfo=_dt.UTC),
                12,
                ("2025-02", "2026-01"),
            ),
            # December — end-of-year boundary, 3-month window.
            (
                _dt.datetime(2026, 12, 31, tzinfo=_dt.UTC),
                3,
                ("2026-10", "2026-12"),
            ),
            # Single-month window — start equals end.
            (
                _dt.datetime(2026, 7, 15, tzinfo=_dt.UTC),
                1,
                ("2026-07", "2026-07"),
            ),
            # 24-month window — wraps two years back.
            (
                _dt.datetime(2026, 3, 1, tzinfo=_dt.UTC),
                24,
                ("2024-04", "2026-03"),
            ),
        ],
    )
    def test_window_math(
        self,
        monkeypatch: pytest.MonkeyPatch,
        now: _dt.datetime,
        months: int,
        expected: tuple[str, str],
    ) -> None:
        """Window starts (N-1) calendar months before `now` and ends at `now`'s month."""

        class _FrozenDateTime(_dt.datetime):
            @classmethod
            def now(cls, tz: _dt.tzinfo | None = None) -> _dt.datetime:  # type: ignore[override]
                return now if tz is not None else now.replace(tzinfo=None)

        monkeypatch.setattr(reports_tools, "_datetime", _FrozenDateTime)
        assert reports_tools._default_window(months=months) == expected  # pyright: ignore[reportPrivateUsage]

    def test_default_is_twelve_months(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The default `months` arg is 12."""

        class _FrozenDateTime(_dt.datetime):
            @classmethod
            def now(cls, tz: _dt.tzinfo | None = None) -> _dt.datetime:  # type: ignore[override]
                return _dt.datetime(2026, 5, 17, tzinfo=_dt.UTC)

        monkeypatch.setattr(reports_tools, "_datetime", _FrozenDateTime)
        assert reports_tools._default_window() == ("2025-06", "2026-05")  # pyright: ignore[reportPrivateUsage]


class TestDefaultedSpendingTool:
    """`reports_spending_get()` with no args surfaces a widen hint + period."""

    async def test_no_arg_call_prepends_widen_hint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _FrozenDateTime(_dt.datetime):
            @classmethod
            def now(cls, tz: _dt.tzinfo | None = None) -> _dt.datetime:  # type: ignore[override]
                return _dt.datetime(2026, 5, 17, tzinfo=_dt.UTC)

        monkeypatch.setattr(reports_tools, "_datetime", _FrozenDateTime)
        envelope = await reports_tools.reports_spending_get()
        body = envelope.to_dict()
        assert body["actions"][0].startswith("Showing the last 12 months")
        assert body["summary"]["period"] == "2025-06 to 2026-05"

    async def test_explicit_bounds_skip_widen_hint(self) -> None:
        envelope = await reports_tools.reports_spending_get(
            from_month="2025-01", to_month="2025-03"
        )
        body = envelope.to_dict()
        # No widen-hint prepend when caller supplied bounds.
        assert not body["actions"][0].startswith("Showing the last 12 months")
        # Period reflects the caller-supplied bounds.
        assert body["summary"]["period"] == "2025-01 to 2025-03"


class TestDefaultedCashflowTool:
    """`reports_cashflow_get()` with no args mirrors the spending behavior."""

    async def test_no_arg_call_prepends_widen_hint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _FrozenDateTime(_dt.datetime):
            @classmethod
            def now(cls, tz: _dt.tzinfo | None = None) -> _dt.datetime:  # type: ignore[override]
                return _dt.datetime(2026, 5, 17, tzinfo=_dt.UTC)

        monkeypatch.setattr(reports_tools, "_datetime", _FrozenDateTime)
        envelope = await reports_tools.reports_cashflow_get()
        body = envelope.to_dict()
        assert body["actions"][0].startswith("Showing the last 12 months")
        assert body["summary"]["period"] == "2025-06 to 2026-05"
