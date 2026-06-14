"""Tests for account-link metrics registration (M1S).

``ACCOUNT_LINK_OUTCOMES_TOTAL`` is defined here in M1S.1; emission wires into the
``AccountResolver`` in M1S.2 (superseding ``ACCOUNT_MATCH_OUTCOMES_TOTAL``).
"""

from moneybin.metrics.registry import ACCOUNT_LINK_OUTCOMES_TOTAL

_EXPECTED_RESULTS = {
    "adopted_strong",
    "minted_new",
    "pending_review",
    "merged",
    "rejected",
}


class TestAccountLinkMetrics:
    """Tests for the account-link outcome counter definition in the registry."""

    def test_account_link_outcomes_total_name(self) -> None:
        # Counter._name stores the base name without the auto-appended _total suffix.
        assert ACCOUNT_LINK_OUTCOMES_TOTAL._name == "moneybin_account_link_outcomes"  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals

    def test_account_link_outcomes_total_has_result_label(self) -> None:
        assert "result" in ACCOUNT_LINK_OUTCOMES_TOTAL._labelnames  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals

    def test_account_link_outcomes_total_accepts_documented_results(self) -> None:
        # Each documented outcome is a usable label value (no validation error).
        for result in _EXPECTED_RESULTS:
            ACCOUNT_LINK_OUTCOMES_TOTAL.labels(result=result)
