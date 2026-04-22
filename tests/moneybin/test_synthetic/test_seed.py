# ruff: noqa: S101
"""Tests for the SeededRandom wrapper."""

import statistics
from collections import Counter

from moneybin.testing.synthetic.seed import SeededRandom


class TestSeededRandomDeterminism:
    """Same seed produces identical output; different seeds diverge."""

    def test_same_seed_same_output(self) -> None:
        rng1 = SeededRandom(42)
        rng2 = SeededRandom(42)
        values1 = [rng1.uniform(0, 100) for _ in range(50)]
        values2 = [rng2.uniform(0, 100) for _ in range(50)]
        assert values1 == values2

    def test_different_seed_different_output(self) -> None:
        rng1 = SeededRandom(42)
        rng2 = SeededRandom(99)
        values1 = [rng1.uniform(0, 100) for _ in range(20)]
        values2 = [rng2.uniform(0, 100) for _ in range(20)]
        assert values1 != values2


class TestSeededRandomDistributions:
    """Statistical properties of generated distributions."""

    def test_log_normal_mean_within_bounds(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.log_normal(100.0, 30.0) for _ in range(5000)]
        sample_mean = statistics.mean(samples)
        # Mean should be within 10% of target
        assert 90.0 < sample_mean < 110.0

    def test_log_normal_all_positive(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.log_normal(50.0, 20.0) for _ in range(1000)]
        assert all(s > 0 for s in samples)

    def test_poisson_mean_within_bounds(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.poisson(10.0) for _ in range(5000)]
        sample_mean = statistics.mean(samples)
        assert 9.0 < sample_mean < 11.0

    def test_poisson_non_negative(self) -> None:
        rng = SeededRandom(42)
        samples = [rng.poisson(3.0) for _ in range(1000)]
        assert all(s >= 0 for s in samples)

    def test_weighted_choice_respects_weights(self) -> None:
        rng = SeededRandom(42)
        items = ["a", "b", "c"]
        weights = [10.0, 1.0, 1.0]
        results = [rng.weighted_choice(items, weights) for _ in range(1000)]
        counts = Counter(results)
        # "a" should appear much more often than "b" or "c"
        assert counts["a"] > counts["b"] * 3


class TestSeededRandomDayInMonth:
    """Day-in-month generation with optional day-of-week bias."""

    def test_day_within_month_bounds(self) -> None:
        rng = SeededRandom(42)
        for month in range(1, 13):
            day = rng.day_in_month(2024, month)
            assert 1 <= day <= 31

    def test_february_respects_month_length(self) -> None:
        rng = SeededRandom(42)
        days = [rng.day_in_month(2024, 2) for _ in range(100)]
        assert all(1 <= d <= 29 for d in days)  # 2024 is a leap year

    def test_day_of_week_weights_bias_output(self) -> None:
        rng = SeededRandom(42)
        weights = {"friday": 5.0, "saturday": 5.0}
        days = [rng.day_in_month(2024, 3, day_weights=weights) for _ in range(1000)]
        # Count weekend vs weekday days
        import calendar

        weekend_count = sum(
            1
            for d in days
            if calendar.weekday(2024, 3, d) in (4, 5)  # Fri, Sat
        )
        weekday_count = len(days) - weekend_count
        # Weekend days have weight 5.0 vs 1.0 default — should dominate
        assert weekend_count > weekday_count

    def test_gauss_returns_float(self) -> None:
        rng = SeededRandom(42)
        result = rng.gauss(100.0, 15.0)
        assert isinstance(result, float)
