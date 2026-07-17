"""CLI tests for moneybin accounts commands."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.privacy.payloads.accounts import (
    AccountListPayload,
    AccountResolutionItem,
    AccountResolvePayload,
    AccountSummary,
)
from moneybin.services.account_service import CLEAR


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
    currency_code: str = "USD",
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
        "currency_code": currency_code,
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


def _as_account_summary(d: dict[str, object]) -> AccountSummary:
    """Build an AccountSummary from the _make_account dict shape."""
    from decimal import Decimal

    credit = d.get("credit_limit")
    return AccountSummary(
        account_id=str(d["account_id"]),
        display_name=d.get("display_name"),  # type: ignore[arg-type]
        institution_name=d.get("institution_name"),  # type: ignore[arg-type]
        account_type=str(d["account_type"]),
        account_subtype=d.get("account_subtype"),  # type: ignore[arg-type]
        holder_category=d.get("holder_category"),  # type: ignore[arg-type]
        currency_code=str(d.get("currency_code", "USD")),
        archived=bool(d.get("archived", False)),
        include_in_net_worth=bool(d.get("include_in_net_worth", True)),
        last_four=d.get("last_four"),  # type: ignore[arg-type]
        credit_limit=Decimal(str(credit)) if credit is not None else None,
    )


class TestAccountsHelp:
    """Tests that accounts --help surfaces the expected subcommands."""

    @pytest.mark.unit
    def test_accounts_help_lists_subcommands(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["accounts", "--help"])
        assert result.exit_code == 0
        assert "list" in result.stdout
        assert "get" in result.stdout


class TestAccountsList:
    """Tests for the accounts list command."""

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_json_returns_accounts(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.list_accounts.return_value = AccountListPayload(
            rows=[_as_account_summary(_ACCOUNT_A), _as_account_summary(_ACCOUNT_B)]
        )

        result = runner.invoke(app, ["accounts", "list", "--output", "json"])
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        assert "data" in data
        # data is now {"rows": [...]} from AccountListPayload serialization
        assert isinstance(data["data"]["rows"], list)
        assert len(data["data"]["rows"]) == 2

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_hides_archived_by_default(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.list_accounts.return_value = AccountListPayload(
            rows=[_as_account_summary(_ACCOUNT_A)]
        )

        result = runner.invoke(app, ["accounts", "list", "--output", "json"])
        assert result.exit_code == 0
        # Verify include_archived=False was passed
        svc.list_accounts.assert_called_once_with(
            include_archived=False, type_filter=None
        )

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_include_archived_passes_flag(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.list_accounts.return_value = AccountListPayload(
            rows=[
                _as_account_summary(_ACCOUNT_A),
                _as_account_summary(_ACCOUNT_B),
                _as_account_summary(_ACCOUNT_ARCHIVED),
            ]
        )

        result = runner.invoke(
            app, ["accounts", "list", "--include-archived", "--output", "json"]
        )
        assert result.exit_code == 0
        svc.list_accounts.assert_called_once_with(
            include_archived=True, type_filter=None
        )
        ids = [a["account_id"] for a in json.loads(result.stdout)["data"]["rows"]]
        # account_id is RECORD_ID (spec D6) — passes through unmasked.
        assert "acct_archived" in ids

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_list_type_filter_passed_through(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.list_accounts.return_value = AccountListPayload(
            rows=[_as_account_summary(_ACCOUNT_A)]
        )

        result = runner.invoke(
            app, ["accounts", "list", "--type", "CHECKING", "--output", "json"]
        )
        assert result.exit_code == 0
        svc.list_accounts.assert_called_once_with(
            include_archived=False, type_filter="CHECKING"
        )


class TestAccountsGet:
    """Tests for the accounts get command."""

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_show_returns_full_record(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:

        from moneybin.privacy.payloads.accounts import AccountDetail

        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.get_account.return_value = AccountDetail(
            account_id="acct_a",
            display_name="Chase Checking",
            official_name=None,
            institution_name="Chase",
            account_type="CHECKING",
            account_subtype="checking",
            holder_category="personal",
            currency_code="USD",
            last_four=None,
            routing_number="021000021",
            credit_limit=None,
            archived=False,
            include_in_net_worth=True,
            source_type="ofx",
        )

        result = runner.invoke(app, ["accounts", "get", "acct_a", "--output", "json"])
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        assert data["status"] == "ok"
        # account_id is RECORD_ID (spec D6) — passes through unmasked.
        assert data["data"]["account_id"] == "acct_a"
        # routing_number is ROUTING_NUMBER (CRITICAL) — still masked.
        assert data["data"]["routing_number"] == "*****"

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
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

        result = runner.invoke(app, ["accounts", "get", "missing"])
        assert result.exit_code == 1
        assert (
            "missing" in result.stderr.lower() or "not found" in result.stderr.lower()
        )


class TestAccountsSetBehavioralFlags:
    """Tests for the behavioral flags folded into `accounts set`.

    Replaces the formerly-separate rename/include/archive/unarchive commands.
    Each test asserts routing through `AccountService.settings_update` with
    the expected kwargs — service-layer behavior is verified separately in
    `tests/moneybin/test_services/test_account_service.py`.
    """

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_display_name_writes(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(display_name="Checking"),
            [],
        )
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--display-name", "Checking"]
        )
        assert result.exit_code == 0, result.stderr
        kwargs = mock_service.settings_update.call_args.kwargs
        assert kwargs["display_name"] == "Checking"

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_clear_display_name(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(display_name=None),
            [],
        )
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--clear-display-name"]
        )
        assert result.exit_code == 0
        kwargs = mock_service.settings_update.call_args.kwargs
        assert kwargs["display_name"] is CLEAR

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_include_writes_true(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(include_in_net_worth=True),
            [],
        )
        result = runner.invoke(app, ["accounts", "set", "acct_a", "--include"])
        assert result.exit_code == 0
        kwargs = mock_service.settings_update.call_args.kwargs
        assert kwargs["include_in_net_worth"] is True

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_exclude_writes_false(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(include_in_net_worth=False),
            [],
        )
        result = runner.invoke(app, ["accounts", "set", "acct_a", "--exclude"])
        assert result.exit_code == 0
        kwargs = mock_service.settings_update.call_args.kwargs
        assert kwargs["include_in_net_worth"] is False

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_archive_announces_cascade(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(archived=True, include_in_net_worth=False),
            [],
        )
        result = runner.invoke(app, ["accounts", "set", "acct_a", "--archive"])
        assert result.exit_code == 0
        kwargs = mock_service.settings_update.call_args.kwargs
        assert kwargs["archived"] is True
        assert "net worth" in result.stderr.lower()

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_unarchive_no_cascade_note(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(archived=False, include_in_net_worth=False),
            [],
        )
        result = runner.invoke(app, ["accounts", "set", "acct_a", "--unarchive"])
        assert result.exit_code == 0
        kwargs = mock_service.settings_update.call_args.kwargs
        assert kwargs["archived"] is False
        # Cascade note appears only on --archive, never on --unarchive
        assert "also excluded from net worth" not in result.stderr.lower()

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_set_archive_and_unarchive_mutex(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Passing both --archive and --unarchive collapses to whichever Typer parses last.

        Typer's flag-pair `--archive/--unarchive` is a single boolean — later
        flag wins. Documenting the behavior so a future change can lock in
        an explicit mutex error if desired.
        """
        mock_get_db.return_value = MagicMock()
        mock_service = mock_svc_cls.return_value
        mock_service.settings_update.return_value = (
            MagicMock(archived=False, include_in_net_worth=False),
            [],
        )
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--archive", "--unarchive"]
        )
        assert result.exit_code == 0
        kwargs = mock_service.settings_update.call_args.kwargs
        # --unarchive came last → archived=False wins
        assert kwargs["archived"] is False


class TestAccountsSet:
    """Tests for the accounts set command."""

    @pytest.mark.unit
    def test_set_requires_at_least_one_flag(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["accounts", "set", "acct_a"])
        assert result.exit_code == 2
        # Usage error should mention that a flag is needed
        assert "flag" in result.stderr.lower() or "required" in result.stderr.lower()

    @pytest.mark.unit
    def test_set_writes_canonical_subtype(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(account_subtype="checking"),
                [],  # no warnings — canonical value
            )
            result = runner.invoke(
                app, ["accounts", "set", "acct_a", "--subtype", "checking", "--yes"]
            )
        assert result.exit_code == 0, result.stderr
        # Verify settings_update was called with the canonical subtype
        call_kwargs = mock_service.settings_update.call_args.kwargs
        assert call_kwargs.get("account_subtype") == "checking"

    @pytest.mark.unit
    def test_set_clear_credit_limit(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        from moneybin.services.account_service import CLEAR

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(credit_limit=None),
                [],
            )
            result = runner.invoke(
                app, ["accounts", "set", "acct_a", "--clear-credit-limit", "--yes"]
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service.settings_update.call_args.kwargs
        assert call_kwargs.get("credit_limit") is CLEAR

    @pytest.mark.unit
    def test_set_unknown_subtype_non_tty_no_yes_exits_2(
        self, runner: CliRunner
    ) -> None:
        from unittest.mock import patch

        # CliRunner is non-TTY by default. Without --yes, non-canonical subtype
        # should exit 2 with a warning and NOT call the service.
        with patch(
            "moneybin.cli.commands.accounts.AccountService"
        ) as mock_service_class:
            result = runner.invoke(
                app, ["accounts", "set", "acct_a", "--subtype", "chequing"]
            )
        assert result.exit_code == 2
        assert "chequing" in result.stderr.lower()
        # Service must NOT have been called
        mock_service_class.return_value.settings_update.assert_not_called()

    @pytest.mark.unit
    def test_set_unknown_subtype_with_yes_writes(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(account_subtype="chequing"),
                [
                    {
                        "field": "account_subtype",
                        "message": "...",
                        "suggestion": "checking",
                    }
                ],
            )
            result = runner.invoke(
                app, ["accounts", "set", "acct_a", "--subtype", "chequing", "--yes"]
            )
        assert result.exit_code == 0
        mock_service.settings_update.assert_called_once()

    @pytest.mark.unit
    def test_set_default_cost_basis_method_persists(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(default_cost_basis_method="fifo"),
                [],
            )
            result = runner.invoke(
                app,
                ["accounts", "set", "acct_a", "--default-cost-basis-method", "fifo"],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service.settings_update.call_args.kwargs
        assert call_kwargs.get("default_cost_basis_method") == "fifo"

    @pytest.mark.unit
    def test_set_clear_default_cost_basis_method(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        from moneybin.services.account_service import CLEAR

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(default_cost_basis_method=None),
                [],
            )
            result = runner.invoke(
                app,
                ["accounts", "set", "acct_a", "--clear-default-cost-basis-method"],
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service.settings_update.call_args.kwargs
        assert call_kwargs.get("default_cost_basis_method") is CLEAR

    @pytest.mark.unit
    def test_set_unknown_holder_category_non_tty_exits_2(
        self, runner: CliRunner
    ) -> None:
        from unittest.mock import patch

        with patch(
            "moneybin.cli.commands.accounts.AccountService"
        ) as mock_service_class:
            result = runner.invoke(
                app, ["accounts", "set", "acct_a", "--holder-category", "corporate"]
            )
        assert result.exit_code == 2
        assert "corporate" in result.stderr.lower()
        mock_service_class.return_value.settings_update.assert_not_called()

    @pytest.mark.unit
    def test_set_credit_limit_parses_decimal(self, runner: CliRunner) -> None:
        from decimal import Decimal
        from unittest.mock import MagicMock, patch

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(credit_limit=Decimal("5000.00")),
                [],
            )
            result = runner.invoke(
                app, ["accounts", "set", "acct_a", "--credit-limit", "5000.00", "--yes"]
            )
        assert result.exit_code == 0, result.stderr
        call_kwargs = mock_service.settings_update.call_args.kwargs
        assert call_kwargs.get("credit_limit") == Decimal("5000.00")

    @pytest.mark.unit
    def test_set_canonical_holder_category_no_prompt(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        with (
            patch("moneybin.cli.commands.accounts.get_database"),
            patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class,
        ):
            mock_service = mock_service_class.return_value
            mock_service.settings_update.return_value = (
                MagicMock(holder_category="business"),
                [],
            )
            # No --yes needed; canonical value doesn't trigger the prompt
            result = runner.invoke(
                app,
                ["accounts", "set", "acct_a", "--holder-category", "business"],
            )
        assert result.exit_code == 0, result.stderr
        mock_service.settings_update.assert_called_once()

    @pytest.mark.unit
    def test_set_unknown_subtype_tty_confirm_yes(self, runner: CliRunner) -> None:
        from unittest.mock import MagicMock, patch

        # Patch at the module level where sys is imported
        with patch("moneybin.cli.commands.accounts.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = True
            with (
                patch("moneybin.cli.commands.accounts.get_database"),
                patch(
                    "moneybin.cli.commands.accounts.AccountService"
                ) as mock_service_class,
            ):
                mock_service = mock_service_class.return_value
                mock_service.settings_update.return_value = (
                    MagicMock(account_subtype="chequing"),
                    [
                        {
                            "field": "account_subtype",
                            "message": "...",
                            "suggestion": "checking",
                        }
                    ],
                )
                result = runner.invoke(
                    app,
                    ["accounts", "set", "acct_a", "--subtype", "chequing"],
                    input="y\n",
                )
        assert result.exit_code == 0, result.stderr
        mock_service.settings_update.assert_called_once()
        assert "chequing" in result.stderr.lower()

    @pytest.mark.unit
    def test_set_unknown_subtype_tty_confirm_no(self, runner: CliRunner) -> None:
        from unittest.mock import patch

        # Patch at the module level where sys is imported
        with patch("moneybin.cli.commands.accounts.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = True
            with patch(
                "moneybin.cli.commands.accounts.AccountService"
            ) as mock_service_class:
                result = runner.invoke(
                    app,
                    ["accounts", "set", "acct_a", "--subtype", "chequing"],
                    input="n\n",
                )
        assert result.exit_code == 2
        mock_service_class.return_value.settings_update.assert_not_called()


class TestAccountsResolve:
    """Tests for the `moneybin accounts resolve` CLI command."""

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_text_output_prints_match(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        """Text mode prints account_id and display_name for each match."""
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.resolve.return_value = AccountResolvePayload(
            matches=[
                AccountResolutionItem(
                    account_id="a1",
                    display_name="Chase Checking",
                    account_subtype="checking",
                    institution_name="Chase",
                    confidence=1.0,
                )
            ]
        )
        result = runner.invoke(app, ["accounts", "resolve", "chase"])
        assert result.exit_code == 0, result.stderr
        assert "a1" in result.stdout
        assert "Chase Checking" in result.stdout

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_json_output_returns_envelope(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        """`--output json` returns the same envelope shape MCP returns."""
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.resolve.return_value = AccountResolvePayload(
            matches=[
                AccountResolutionItem(
                    account_id="a1",
                    display_name="Chase Checking",
                    account_subtype="checking",
                    institution_name="Chase",
                    confidence=1.0,
                )
            ]
        )
        result = runner.invoke(
            app, ["accounts", "resolve", "chase", "--output", "json"]
        )
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        # AccountResolvePayload's highest class is USER_NOTE (display_name) → MEDIUM;
        # account_id is RECORD_ID (spec D6). render_or_json stamps the derived tier.
        assert payload["summary"]["sensitivity"] == "medium"
        # data is {"matches": [...]} from AccountResolvePayload serialization
        # account_id is RECORD_ID (spec D6) — passes through unmasked.
        assert payload["data"]["matches"][0]["account_id"] == "a1"
        assert payload["data"]["matches"][0]["confidence"] == 1.0

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_limit_flag_passed_to_service(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        """--limit caps the number of results requested from the service."""
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.resolve.return_value = AccountResolvePayload(matches=[])
        result = runner.invoke(app, ["accounts", "resolve", "account", "--limit", "1"])
        assert result.exit_code == 0
        svc.resolve.assert_called_once_with(query="account", limit=1)

    @pytest.mark.unit
    @patch("moneybin.cli.commands.accounts.get_database")
    @patch("moneybin.cli.commands.accounts.AccountService")
    def test_no_matches_text_mode_writes_to_stderr(
        self,
        mock_svc_cls: MagicMock,
        mock_get_db: MagicMock,
        runner: CliRunner,
    ) -> None:
        """No matches in text mode emits a stderr message and exits 0."""
        mock_get_db.return_value = MagicMock()
        svc = mock_svc_cls.return_value
        svc.resolve.return_value = AccountResolvePayload(matches=[])
        result = runner.invoke(app, ["accounts", "resolve", "zzz"])
        assert result.exit_code == 0
        assert "no accounts" in result.stderr.lower() or "zzz" in result.stderr
