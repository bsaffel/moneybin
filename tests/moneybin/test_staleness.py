"""The shared staleness vocabulary both valuation domains resolve through.

``investments-price-feeds.md`` is the first implementer; ``asset-tracking.md``
inherits the same two-tier resolution for physical assets, so these tests pin
the shape rather than one domain's use of it.

The boundary case is the point of the whole module: markets close ~114 days a
year, so a threshold that fires on an ordinary weekend would train the reader to
ignore every staleness warning. ``asset-tracking.md`` says a warning surfaces
when an observation *exceeds* its threshold, which makes the comparison strictly
``>`` — a Monday reading 3 days stale on a 4-day equity is a normal weekend, not
a fault.
"""

from __future__ import annotations

import pytest

from moneybin.staleness import (
    SECURITY_TYPE_STALENESS_DAYS,
    is_stale,
    resolve_threshold_days,
)


class TestResolveThresholdDays:
    """Two tiers: per-entity-type default, then the caller's global default."""

    def test_known_type_uses_its_own_default(self) -> None:
        assert (
            resolve_threshold_days(
                "crypto", type_defaults=SECURITY_TYPE_STALENESS_DAYS, global_default=4
            )
            == 1
        )

    def test_unknown_type_falls_through_to_global_default(self) -> None:
        """`cash` and `other` are valid security types the spec's table omits.

        Falling through is deliberate: inventing a bespoke number for a type
        nobody specified would be a guess wearing the authority of a constant.
        """
        assert (
            resolve_threshold_days(
                "other", type_defaults=SECURITY_TYPE_STALENESS_DAYS, global_default=9
            )
            == 9
        )

    def test_resolution_is_domain_neutral(self) -> None:
        """Physical assets resolve through the same helper with their own table.

        This is the one-way door the module exists to close: if assets could not
        use this function, C.2 would have shipped a second implementation of one
        rule.
        """
        asset_defaults = {"real_estate": 180, "vehicle": 90, "valuable": 365}
        assert (
            resolve_threshold_days(
                "vehicle", type_defaults=asset_defaults, global_default=180
            )
            == 90
        )


class TestIsStale:
    """Staleness is informational — it never removes a value from a total."""

    @pytest.mark.parametrize(
        ("days", "threshold", "expected"),
        [
            (0, 4, False),
            (3, 4, False),
            (4, 4, False),
            (5, 4, True),
            (1, 1, False),
            (2, 1, True),
        ],
    )
    def test_stale_only_when_days_exceed_threshold(
        self, days: int, threshold: int, expected: bool
    ) -> None:
        """Exactly-at-threshold is NOT stale; asset-tracking.md says 'exceeds'."""
        assert is_stale(days, threshold) is expected

    def test_never_observed_is_not_stale(self) -> None:
        """A NULL observation age is `unpriced`, a different status with its own remedy.

        Reporting it as stale would tell the user to refresh a feed when what
        they actually need is a price source for a security nothing covers.
        """
        assert is_stale(None, 4) is False


class TestSecurityTypeDefaults:
    """The per-type table absorbs ordinary market closure, nothing more."""

    def test_crypto_is_one_day(self) -> None:
        """Crypto trades continuously, so yesterday's close is already old."""
        assert SECURITY_TYPE_STALENESS_DAYS["crypto"] == 1

    @pytest.mark.parametrize("security_type", ["equity", "etf", "mutual_fund", "bond"])
    def test_exchange_traded_types_absorb_a_weekend(self, security_type: str) -> None:
        """4 days covers Friday close read on Tuesday after a Monday holiday."""
        assert SECURITY_TYPE_STALENESS_DAYS[security_type] == 4

    def test_table_covers_only_the_types_the_spec_names(self) -> None:
        """`cash` and `other` are intentionally absent — they fall to the global default."""
        assert set(SECURITY_TYPE_STALENESS_DAYS) == {
            "equity",
            "etf",
            "mutual_fund",
            "bond",
            "crypto",
        }

    def test_table_is_immutable(self) -> None:
        """A shared default table a caller could mutate is a cross-domain footgun."""
        with pytest.raises(TypeError):
            SECURITY_TYPE_STALENESS_DAYS["equity"] = 99  # type: ignore[index]
