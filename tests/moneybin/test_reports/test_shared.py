"""Tests for shared report-runner helpers (window defaulting)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from moneybin.reports.definitions._shared import default_window, resolve_window


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        # 12 calendar months ending at the current month, inclusive. Expected
        # values are derived from that definition by hand, not from the code.
        (datetime(2026, 1, 15, tzinfo=UTC), ("2025-02", "2026-01")),  # year rollback
        (datetime(2026, 12, 15, tzinfo=UTC), ("2026-01", "2026-12")),  # no rollback
        (datetime(2026, 2, 15, tzinfo=UTC), ("2025-03", "2026-02")),  # rollback
    ],
)
def test_default_window_year_boundary(now: datetime, expected: tuple[str, str]) -> None:
    # Guards the year-rollback arithmetic in default_window — a classic
    # off-by-one site. Only datetime.now is patched; .replace runs on the real
    # datetime the mock returns.
    with patch("moneybin.reports.definitions._shared.datetime") as mock_dt:
        mock_dt.now.return_value = now
        assert default_window(12) == expected


def test_resolve_window_defaults_both_bounds() -> None:
    with patch("moneybin.reports.definitions._shared.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 6, 15, tzinfo=UTC)
        from_month, to_month, period, hint = resolve_window(None, None)
    assert (from_month, to_month) == ("2025-07", "2026-06")
    assert period == "2025-07 to 2026-06"
    assert hint is not None  # widen-the-window hint present when defaulted


def test_resolve_window_passes_through_explicit_bounds() -> None:
    from_month, to_month, period, hint = resolve_window("2024-01", "2024-12")
    assert (from_month, to_month) == ("2024-01", "2024-12")
    assert period == "2024-01 to 2024-12"
    assert hint is None  # no hint when the caller set the window


def test_resolve_window_one_sided_from_only_reports_period() -> None:
    # A single bound still filters; the envelope's period must signal that a
    # temporal filter was applied (was silently None for one-sided windows).
    from_month, to_month, period, hint = resolve_window("2024-03", None)
    assert (from_month, to_month) == ("2024-03", None)
    assert period == "from 2024-03"
    assert hint is None


def test_resolve_window_one_sided_to_only_reports_period() -> None:
    from_month, to_month, period, hint = resolve_window(None, "2024-09")
    assert (from_month, to_month) == (None, "2024-09")
    assert period == "through 2024-09"
    assert hint is None
