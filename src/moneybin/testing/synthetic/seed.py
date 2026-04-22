"""Seeded random number generator wrapper.

All randomness in the synthetic data generator flows through a single
SeededRandom instance initialized with a user-provided seed. Generator
modules receive this wrapper — never import ``random`` directly or use
ambient state. This makes determinism structural, not by convention.
"""

import calendar
import math
import random
from typing import Any


class SeededRandom:
    """Wrapper around ``random.Random`` providing all stochastic operations.

    Args:
        seed: Integer seed for reproducible output.
    """

    def __init__(self, seed: int) -> None:  # noqa: D107 — args documented in class docstring
        self._rng = random.Random(seed)  # noqa: S311  # deterministic test data, not cryptography

    def uniform(self, a: float, b: float) -> float:
        """Uniform random float in [a, b]."""
        return self._rng.uniform(a, b)

    def randint(self, a: int, b: int) -> int:
        """Random integer in [a, b] inclusive."""
        return self._rng.randint(a, b)

    def choice(self, items: list[Any]) -> Any:
        """Random choice from a non-empty list."""
        return self._rng.choice(items)

    def weighted_choice(self, items: list[Any], weights: list[float]) -> Any:
        """Weighted random choice from items.

        Args:
            items: List of items to choose from.
            weights: Corresponding weights (higher = more likely).

        Returns:
            A single selected item.
        """
        return self._rng.choices(items, weights=weights, k=1)[0]

    def log_normal(self, mean: float, stddev: float) -> float:
        """Log-normal sample parameterized by desired output mean and stddev.

        Converts the desired output distribution parameters to the underlying
        normal distribution parameters for ``lognormvariate``.

        Args:
            mean: Desired mean of the output distribution.
            stddev: Desired standard deviation of the output distribution.

        Returns:
            A positive float drawn from the log-normal distribution.
        """
        variance = stddev**2
        mu = math.log(mean**2 / math.sqrt(variance + mean**2))
        sigma = math.sqrt(math.log(1 + variance / mean**2))
        return self._rng.lognormvariate(mu, sigma)

    def poisson(self, lam: float) -> int:
        """Poisson-distributed random non-negative integer.

        Uses Knuth's algorithm for small lambda values.

        Args:
            lam: Expected value (lambda) of the distribution.

        Returns:
            A non-negative integer.
        """
        if lam <= 0:
            return 0
        big_l = math.exp(-lam)
        k = 0
        p = 1.0
        while p > big_l:
            k += 1
            p *= self._rng.random()
        return k - 1

    def gauss(self, mu: float, sigma: float) -> float:
        """Gaussian (normal) random float.

        Args:
            mu: Mean of the distribution.
            sigma: Standard deviation.

        Returns:
            A float from the normal distribution.
        """
        return self._rng.gauss(mu, sigma)

    def day_in_month(
        self,
        year: int,
        month: int,
        day_weights: dict[str, float] | None = None,
    ) -> int:
        """Random day within a month, optionally biased by day-of-week.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).
            day_weights: Optional mapping of lowercase day names to weights.
                Unspecified days default to weight 1.0.

        Returns:
            A day number (1 to last day of month).
        """
        days_in_month = calendar.monthrange(year, month)[1]

        if day_weights is None:
            return self._rng.randint(1, days_in_month)

        day_names = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        weights: list[float] = []
        for day in range(1, days_in_month + 1):
            dow = calendar.weekday(year, month, day)
            weight = day_weights.get(day_names[dow], 1.0)
            weights.append(weight)

        days = list(range(1, days_in_month + 1))
        return self.weighted_choice(days, weights)

    def sample(self, items: list[Any], k: int) -> list[Any]:
        """Random sample of k items without replacement."""
        return self._rng.sample(items, k)

    def shuffle(self, items: list[Any]) -> None:
        """In-place shuffle of a list."""
        self._rng.shuffle(items)
