"""CLI tests for moneybin accounts commands."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner with split streams."""
    return CliRunner()


def _make_account(
    account_id: str = "acct_a",
    display_name: str = "Chase Checking",
    institution_name: str = "Chase",
    account_type: str = "CHECKING",
    account_subtype: str | None = "checking",
    holder_category: str | None = "personal",
    iso_currency_code: str = "USD",
    last_four: str | None = None,
    credit_limit: object = None,
    archived: bool = False,
    include_in_net_worth: bool = True,
) -> dict[str, object]:
    return {
        "account_id": account_id,
        "display_name": display_name,
        "institution_name": institution_name,
        "account_type": account_type,
        "account_subtype": account_subtype,
        "holder_category": holder_category,
        "iso_currency_code": iso_currency_code,
        "last_four": last_four,
        "credit_limit": credit_limit,
        "archived": archived,
        "include_in_net_worth": include_in_net_worth,
    }


_ACCOUNT_A = _make_account("acct_a", "Chase Checking")
_ACCOUNT_B = _make_account(
    "acct_b", "Chase Savings", account_type="SAVINGS", account_subtype="savings"
)
_ACCOUNT_ARCHIVED = _make_account(
    "acct_archived", "Old Account", institution_name="Old Bank", archived=True
)


class TestAccountsHelp:
    """Tests that accounts --help surfaces the expected subcommands."""

    @pytest.mark.unit
    def test_accounts_help_lists_subcommands(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["accounts", "--help"])
        assert result.exit_code == 0
        assert "list" in result.stdout
        assert "show" in result.stdout


class TestAccountsList:
    """Tests for the accounts list command."""

    @pytest.mark.unit
    @patch("moneybin.cli.utils.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_json_returns_accounts(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        from moneybin.services.account_service import AccountListResult

        svc.list_accounts.return_value = AccountListResult(
            accounts=[_ACCOUNT_A, _ACCOUNT_B]
        )

        result = runner.invoke(app, ["accounts", "list", "--output", "json"])
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        assert "data" in data
        assert isinstance(data["data"], list)
        assert len(data["data"]) == 2

    @pytest.mark.unit
    @patch("moneybin.cli.utils.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_hides_archived_by_default(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        from moneybin.services.account_service import AccountListResult

        svc.list_accounts.return_value = AccountListResult(accounts=[_ACCOUNT_A])

        result = runner.invoke(app, ["accounts", "list", "--output", "json"])
        assert result.exit_code == 0
        # Verify include_archived=False was passed
        svc.list_accounts.assert_called_once_with(
            include_archived=False, type_filter=None
        )

    @pytest.mark.unit
    @patch("moneybin.cli.utils.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_include_archived_passes_flag(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        from moneybin.services.account_service import AccountListResult

        svc.list_accounts.return_value = AccountListResult(
            accounts=[_ACCOUNT_A, _ACCOUNT_B, _ACCOUNT_ARCHIVED]
        )

        result = runner.invoke(
            app, ["accounts", "list", "--include-archived", "--output", "json"]
        )
        assert result.exit_code == 0
        svc.list_accounts.assert_called_once_with(
            include_archived=True, type_filter=None
        )
        ids = [a["account_id"] for a in json.loads(result.stdout)["data"]]
        assert "acct_archived" in ids

    @pytest.mark.unit
    @patch("moneybin.cli.utils.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_type_filter_passed_through(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        from moneybin.services.account_service import AccountListResult

        svc.list_accounts.return_value = AccountListResult(accounts=[_ACCOUNT_A])

        result = runner.invoke(
            app, ["accounts", "list", "--type", "CHECKING", "--output", "json"]
        )
        assert result.exit_code == 0
        svc.list_accounts.assert_called_once_with(
            include_archived=False, type_filter="CHECKING"
        )


class TestAccountsShow:
    """Tests for the accounts show command."""

    @pytest.mark.unit
    @patch("moneybin.cli.utils.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_show_returns_full_record(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.get_account.return_value = {
            **_ACCOUNT_A,
            "source_type": "ofx",
            "routing_number": "021000021",
            "official_name": None,
        }

        result = runner.invoke(app, ["accounts", "show", "acct_a", "--output", "json"])
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        assert data["account_id"] == "acct_a"

    @pytest.mark.unit
    @patch("moneybin.cli.utils.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_show_unknown_exits_1(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.get_account.return_value = None

        result = runner.invoke(app, ["accounts", "show", "missing"])
        assert result.exit_code == 1
        assert (
            "missing" in result.stderr.lower() or "not found" in result.stderr.lower()
        )
