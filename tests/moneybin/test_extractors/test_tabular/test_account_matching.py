"""Tests for account matching across source types."""

from moneybin.extractors.tabular.account_matching import match_account


class TestMatchAccount:
    """Tests for match_account function."""

    def test_exact_slug_match(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking"},
        ]
        result = match_account("Chase Checking", existing_accounts=existing)
        assert result.matched is True
        assert result.account_id == "chase-checking"

    def test_fuzzy_match_candidates(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking"},
            {"account_id": "chase-credit", "account_name": "Chase Credit Card"},
        ]
        result = match_account("Chase Check", existing_accounts=existing)
        assert result.matched is False
        assert len(result.candidates) > 0
        assert result.candidates[0]["account_name"] == "Chase Checking"

    def test_account_number_match(self) -> None:
        existing = [
            {
                "account_id": "chase-checking",
                "account_name": "Chase Checking",
                "account_number": "1234567890",
            },
        ]
        result = match_account(
            "Chase Checking",
            account_number="1234567890",
            existing_accounts=existing,
        )
        assert result.matched is True
        assert result.account_id == "chase-checking"

    def test_no_match_returns_new(self) -> None:
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking"},
        ]
        result = match_account("Ally Savings", existing_accounts=existing)
        assert result.matched is False
        assert len(result.candidates) == 0

    def test_explicit_account_id_bypasses_matching(self) -> None:
        result = match_account(
            "Anything",
            explicit_account_id="my-custom-id",
            existing_accounts=[],
        )
        assert result.matched is True
        assert result.account_id == "my-custom-id"

    def test_slug_match_via_name(self) -> None:
        """account_name slugifies to match an existing account_id."""
        existing = [
            {"account_id": "chase-checking", "account_name": "Chase Checking Account"},
        ]
        result = match_account("Chase Checking", existing_accounts=existing)
        assert result.matched is True
        assert result.account_id == "chase-checking"


def test_match_account_respects_custom_threshold() -> None:
    """A high threshold rejects fuzzy candidates that the default would accept."""
    existing = [{"account_id": "acct1", "account_name": "Chase Checking"}]
    result = match_account(
        "Chase Chk",
        existing_accounts=existing,
        threshold=0.95,
    )
    assert result.matched is False
    assert result.candidates == []


def test_match_account_default_threshold_returns_candidates() -> None:
    """The default 0.6 threshold surfaces near-misses as candidates."""
    existing = [{"account_id": "acct1", "account_name": "Chase Checking"}]
    result = match_account("Chase Chk", existing_accounts=existing)
    assert result.matched is False
    assert any(c["account_id"] == "acct1" for c in result.candidates)
