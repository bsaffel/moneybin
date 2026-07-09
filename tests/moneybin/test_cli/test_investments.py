"""CLI tests for the top-level ``moneybin investments`` group."""

from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables


@pytest.fixture()
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner with split streams."""
    return CliRunner()


def _make_investments_db(tmp_path: Path) -> Database:
    """Build a Database with real raw/app schema + stubbed core.* investment tables.

    ``Database(...)`` runs the real ``init_schemas()`` (raw.manual_investment_transactions,
    app.securities, app.lot_selections, raw.import_log, app.audit_log all come
    from that for free); ``core.*`` is SQLMesh-managed in production, so
    ``create_core_tables``/``create_core_dim_stub_views`` stub it for the
    read-path commands. Mirrors ``make_curation_db`` in
    ``_curation_helpers.py`` and ``test_investment_service.py``'s ``db`` fixture.
    """
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "investments.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    create_core_tables(database)
    # create_core_dim_stub_views builds core.dim_securities as a faithful
    # passthrough of app.securities (see db_helpers), so `securities list`
    # reflects real `securities add`/`set` writes with no inline patch here.
    create_core_dim_stub_views(database)
    database.conn.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, account_type, institution_name, source_type)
        VALUES ('acct_brokerage', 'investment', 'Fidelity', 'manual')
        """  # noqa: S608  # test fixture insert, static SQL
    )
    return database


def _patch_db(monkeypatch: pytest.MonkeyPatch, database: Database) -> None:
    """Redirect ``get_database`` in every investments CLI module to ``database``.

    The group is a package (``__init__`` + ``lots`` + ``securities``), and each
    module imports ``get_database`` into its own namespace — so patch all three,
    mirroring ``_curation_helpers.patch_db``'s per-module approach.
    """

    @contextmanager
    def _noop_cm(*_a: object, **_kw: object) -> Generator[Database, None, None]:
        yield database

    for module in (
        "moneybin.cli.commands.investments",
        "moneybin.cli.commands.investments.lots",
        "moneybin.cli.commands.investments.securities",
    ):
        monkeypatch.setattr(f"{module}.get_database", _noop_cm)


@pytest.fixture()
def db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Database, None, None]:
    database = _make_investments_db(tmp_path)
    _patch_db(monkeypatch, database)
    yield database
    database.close()


def _add_security(
    runner: CliRunner,
    *,
    name: str,
    type_: str,
    ticker: str | None = None,
) -> str:
    """Add one security via the real CLI path; return its minted security_id."""
    args = ["investments", "securities", "add", "--name", name, "--type", type_]
    if ticker is not None:
        args += ["--ticker", ticker]
    result = runner.invoke(app, [*args, "--output", "json"])
    assert result.exit_code == 0, result.output
    security_id: str = json.loads(result.stdout)["data"]["security_id"]
    return security_id


# ---------------------------------------------------------------------------
# Help smoke tests
# ---------------------------------------------------------------------------


class TestInvestmentsHelp:
    """--help smoke tests for the group and every subcommand."""

    @pytest.mark.unit
    def test_group_help_lists_subcommands(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["investments", "--help"])
        assert result.exit_code == 0
        for name in ("add", "list", "holdings", "gains", "lots", "securities"):
            assert name in result.stdout

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "cmd",
        [
            ["investments", "add", "--help"],
            ["investments", "list", "--help"],
            ["investments", "holdings", "--help"],
            ["investments", "gains", "--help"],
            ["investments", "lots", "--help"],
            ["investments", "lots", "list", "--help"],
            ["investments", "lots", "select", "--help"],
            ["investments", "securities", "--help"],
            ["investments", "securities", "list", "--help"],
            ["investments", "securities", "add", "--help"],
            ["investments", "securities", "set", "--help"],
        ],
        ids=lambda c: " ".join(c),
    )
    def test_subcommand_help_exits_cleanly(
        self, runner: CliRunner, cmd: list[str]
    ) -> None:
        result = runner.invoke(app, cmd)
        assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# securities add + add --type buy (happy path, real DB write)
# ---------------------------------------------------------------------------


class TestSecuritiesAddAndBuy:
    """`securities add` then `add --type buy`/`reinvest`, and Req 6 sign errors."""

    @pytest.mark.unit
    def test_securities_add_then_buy_by_ticker_writes_raw_row(
        self, runner: CliRunner, db: Database
    ) -> None:
        security_id = _add_security(
            runner, name="Apple Inc.", type_="equity", ticker="AAPL"
        )

        buy_result = runner.invoke(
            app,
            [
                "investments",
                "add",
                "--account",
                "acct_brokerage",
                "--security",
                "AAPL",
                "--type",
                "buy",
                "--date",
                "2024-01-15",
                "--quantity",
                "10",
                "--price",
                "150.00",
                "--amount",
                "-1500.00",
                "--fees",
                "4.95",
            ],
        )
        assert buy_result.exit_code == 0, buy_result.output
        assert "Traceback" not in buy_result.output

        rows = db.conn.execute(
            """
            SELECT type, security_id, quantity, amount, fees
              FROM raw.manual_investment_transactions
             WHERE account_id = ?
            """,  # noqa: S608  # test read, static SQL
            ["acct_brokerage"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "buy"
        assert rows[0][1] == security_id
        assert rows[0][2] == Decimal("10")
        assert rows[0][3] == Decimal("-1500.00")
        assert rows[0][4] == Decimal("4.95")

    @pytest.mark.unit
    def test_securities_add_json_output_reports_record_id_class(
        self, runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Same typed-payload requirement as investments_add: the success
        # payload must route through InvestmentSecuritySetPayload, not a bare
        # dict, so the privacy audit trail records the real data class.
        captured: dict[str, object] = {}
        monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
        result = runner.invoke(
            app,
            [
                "investments",
                "securities",
                "add",
                "--name",
                "Apple Inc.",
                "--type",
                "equity",
                "--ticker",
                "AAPL",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        assert captured["classes_returned"] == ["record_id"]

    @pytest.mark.unit
    def test_add_json_output_reports_txn_id(
        self, runner: CliRunner, db: Database
    ) -> None:
        _add_security(runner, name="Apple Inc.", type_="equity", ticker="AAPL")
        result = runner.invoke(
            app,
            [
                "investments",
                "add",
                "--account",
                "acct_brokerage",
                "--security",
                "AAPL",
                "--type",
                "buy",
                "--date",
                "2024-01-15",
                "--quantity",
                "10",
                "--price",
                "150.00",
                "--amount",
                "-1500.00",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        ids = data["data"]["investment_transaction_ids"]
        assert len(ids) == 1
        assert ids[0]

    @pytest.mark.unit
    def test_add_json_output_reports_record_id_class(
        self, runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The success payload must route through the typed
        # InvestmentRecordPayload dataclass (not a bare dict), so the privacy
        # audit trail records the real data class instead of
        # classes_returned=[] — same bug class already fixed once in this PR
        # for investments_lots_select.
        _add_security(runner, name="Apple Inc.", type_="equity", ticker="AAPL")
        captured: dict[str, object] = {}
        monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
        result = runner.invoke(
            app,
            [
                "investments",
                "add",
                "--account",
                "acct_brokerage",
                "--security",
                "AAPL",
                "--type",
                "buy",
                "--date",
                "2024-01-15",
                "--quantity",
                "10",
                "--price",
                "150.00",
                "--amount",
                "-1500.00",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        # InvestmentRecordPayload also carries an `error_details` field
        # (AGGREGATE tier) alongside `investment_transaction_ids` (RECORD_ID).
        assert captured["classes_returned"] == ["aggregate", "record_id"]

    @pytest.mark.unit
    def test_add_reinvest_reports_both_rows(
        self, runner: CliRunner, db: Database
    ) -> None:
        _add_security(
            runner, name="Vanguard Total", type_="mutual_fund", ticker="VTSAX"
        )
        result = runner.invoke(
            app,
            [
                "investments",
                "add",
                "--account",
                "acct_brokerage",
                "--security",
                "VTSAX",
                "--type",
                "reinvest",
                "--date",
                "2024-03-01",
                "--quantity",
                "5",
                "--price",
                "100.00",
                "--amount",
                "-500.00",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        ids = json.loads(result.stdout)["data"]["investment_transaction_ids"]
        assert len(ids) == 2

        rows = db.conn.execute(
            """
            SELECT type FROM raw.manual_investment_transactions
             WHERE investment_transaction_id IN (?, ?)
            """,  # noqa: S608  # test read, static SQL
            ids,
        ).fetchall()
        assert {r[0] for r in rows} == {"reinvest", "dividend"}

    @pytest.mark.unit
    def test_add_buy_with_positive_amount_surfaces_clean_error(
        self, runner: CliRunner, db: Database
    ) -> None:
        """A sign-rule violation (Req 6) surfaces via UserError, not a traceback."""
        _add_security(runner, name="Apple Inc.", type_="equity", ticker="AAPL")
        result = runner.invoke(
            app,
            [
                "investments",
                "add",
                "--account",
                "acct_brokerage",
                "--security",
                "AAPL",
                "--type",
                "buy",
                "--date",
                "2024-01-15",
                "--quantity",
                "10",
                "--price",
                "150.00",
                "--amount",
                "1500.00",
            ],
        )
        assert result.exit_code == 1
        assert "Traceback" not in result.output
        assert "Traceback" not in result.stderr
        assert "negative" in result.stderr.lower()


# ---------------------------------------------------------------------------
# investments list
# ---------------------------------------------------------------------------


class TestInvestmentsList:
    """Tests for `investments list`."""

    @pytest.mark.unit
    def test_list_json_returns_rows(self, runner: CliRunner, db: Database) -> None:
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity, amount, currency_code)
            VALUES ('evt_1', 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    10, -1500.00, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )
        result = runner.invoke(app, ["investments", "list", "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert len(data["data"]["rows"]) == 1
        assert data["data"]["rows"][0]["investment_transaction_id"] == "evt_1"

    @pytest.mark.unit
    def test_list_text_renders_without_error(
        self, runner: CliRunner, db: Database
    ) -> None:
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity, amount, currency_code)
            VALUES ('evt_1', 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    10, -1500.00, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )
        result = runner.invoke(app, ["investments", "list"])
        assert result.exit_code == 0, result.output
        assert "buy" in result.output

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "args",
        [
            ["investments", "list"],
            ["investments", "holdings"],
            ["investments", "gains"],
            ["investments", "lots", "list"],
        ],
    )
    def test_read_surfaces_report_high_sensitivity(
        self, runner: CliRunner, db: Database, args: list[str]
    ) -> None:
        # CLI must match the MCP-derived tier: cost-basis/proceeds/quantity rows
        # are Tier.HIGH (payloads/investments.py), so the CLI JSON envelope must
        # report "high" — not "medium" — to keep the redaction contract
        # identical across surfaces (cli.md).
        result = runner.invoke(app, [*args, "--output", "json"])
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["summary"]["sensitivity"] == "high"


# ---------------------------------------------------------------------------
# investments holdings, investments gains
# ---------------------------------------------------------------------------


class TestHoldingsAndGains:
    """Tests for `investments holdings` and `investments gains`."""

    @pytest.mark.unit
    def test_holdings_json_carries_pillar_c_warning(
        self, runner: CliRunner, db: Database
    ) -> None:
        result = runner.invoke(app, ["investments", "holdings", "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["data"]["warnings"]
        assert "market value" in data["data"]["warnings"][0].lower()

    @pytest.mark.unit
    def test_gains_json_reports_basis_incomplete_warning(
        self, runner: CliRunner, db: Database
    ) -> None:
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('gain_1', 'acct_brokerage', 'sec_1', 'sell_1', 'lot_a', 5,
                    '2024-01-01', '2024-06-12', 950.00, 750.00, 200.00, 'long',
                    'fifo', true, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )
        result = runner.invoke(app, ["investments", "gains", "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert len(data["data"]["rows"]) == 1
        assert data["data"]["warnings"]


# ---------------------------------------------------------------------------
# investments lots list
# ---------------------------------------------------------------------------


class TestLotsList:
    """Tests for `investments lots list`."""

    @pytest.mark.unit
    def test_lots_list_open_only_by_default(
        self, runner: CliRunner, db: Database
    ) -> None:
        db.conn.executemany(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open)
            VALUES (?, 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    ?, ?, ?, ?, 'fifo', 'USD', ?)
            """,  # noqa: S608  # test fixture insert, static SQL
            [
                [
                    "lot_open",
                    Decimal("10"),
                    Decimal("10"),
                    Decimal("1500.00"),
                    Decimal("1500.00"),
                    True,
                ],
                [
                    "lot_closed",
                    Decimal("5"),
                    Decimal("0"),
                    Decimal("750.00"),
                    Decimal("0.00"),
                    False,
                ],
            ],
        )
        open_result = runner.invoke(
            app, ["investments", "lots", "list", "--output", "json"]
        )
        assert open_result.exit_code == 0, open_result.output
        open_data = json.loads(open_result.stdout)
        assert [r["lot_id"] for r in open_data["data"]["rows"]] == ["lot_open"]

        all_result = runner.invoke(
            app, ["investments", "lots", "list", "--all", "--output", "json"]
        )
        assert all_result.exit_code == 0, all_result.output
        all_data = json.loads(all_result.stdout)
        assert {r["lot_id"] for r in all_data["data"]["rows"]} == {
            "lot_open",
            "lot_closed",
        }

    @pytest.mark.unit
    def test_lots_json_reports_basis_incomplete_warning(
        self, runner: CliRunner, db: Database
    ) -> None:
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES ('lot_incomplete', 'acct_brokerage', 'sec_1', '2024-01-15',
                    'transfer_in', 10, 10, 0.00, 0.00, 'fifo', 'USD', true, true)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        result = runner.invoke(app, ["investments", "lots", "list", "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["data"]["rows"][0]["basis_incomplete"] is True
        assert data["data"]["warnings"]

    @pytest.mark.unit
    def test_lots_list_text_flags_basis_incomplete_row(
        self, runner: CliRunner, db: Database
    ) -> None:
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES ('lot_incomplete', 'acct_brokerage', 'sec_1', '2024-01-15',
                    'transfer_in', 10, 10, 0.00, 0.00, 'fifo', 'USD', true, true)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        result = runner.invoke(app, ["investments", "lots", "list"])
        assert result.exit_code == 0, result.output
        assert "basis_incomplete" in result.stdout
        assert "incomplete" in result.stderr


# ---------------------------------------------------------------------------
# investments lots select / --clear
# ---------------------------------------------------------------------------


class TestLotsSelect:
    """Tests for `investments lots select` (set + --clear)."""

    @pytest.mark.unit
    def test_select_sets_and_clear_removes(
        self, runner: CliRunner, db: Database
    ) -> None:
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity)
            VALUES ('sell_1', 'acct_brokerage', 'sec_1', '2024-06-15', 'sell', -10)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.executemany(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 original_quantity, remaining_quantity)
            VALUES (?, 'acct_brokerage', 'sec_1', '2024-01-10', ?, ?)
            """,  # noqa: S608  # test fixture insert, static SQL
            [
                ["lot_a", Decimal("6"), Decimal("6")],
                ["lot_b", Decimal("6"), Decimal("6")],
            ],
        )

        select_result = runner.invoke(
            app,
            [
                "investments",
                "lots",
                "select",
                "sell_1",
                "--lot",
                "lot_a:5",
                "--lot",
                "lot_b:5",
            ],
        )
        assert select_result.exit_code == 0, select_result.output

        rows = db.conn.execute(
            """
            SELECT lot_id, quantity FROM app.lot_selections
             WHERE investment_transaction_id = ?
             ORDER BY lot_id
            """,  # noqa: S608  # test read, static SQL
            ["sell_1"],
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [
            ("lot_a", Decimal("5")),
            ("lot_b", Decimal("5")),
        ]

        clear_result = runner.invoke(
            app, ["investments", "lots", "select", "sell_1", "--clear"]
        )
        assert clear_result.exit_code == 0, clear_result.output

        remaining = db.conn.execute(
            """
            SELECT COUNT(*) FROM app.lot_selections
             WHERE investment_transaction_id = ?
            """,  # noqa: S608  # test read, static SQL
            ["sell_1"],
        ).fetchone()
        assert remaining is not None
        assert remaining[0] == 0

    @pytest.mark.unit
    def test_select_json_reports_high_sensitivity_and_selections(
        self, runner: CliRunner, db: Database
    ) -> None:
        # Must match the investments_lots_select MCP tool's tier: selected
        # quantities carry TXN_AMOUNT (HIGH) — a hardcoded "low" here would
        # break the redaction-contract parity cli.md requires.
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity)
            VALUES ('sell_1', 'acct_brokerage', 'sec_1', '2024-06-15', 'sell', -5)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 original_quantity, remaining_quantity)
            VALUES ('lot_a', 'acct_brokerage', 'sec_1', '2024-01-10', 5, 5)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        result = runner.invoke(
            app,
            [
                "investments",
                "lots",
                "select",
                "sell_1",
                "--lot",
                "lot_a:5",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["summary"]["sensitivity"] == "high"
        assert data["data"]["disposal_txn_id"] == "sell_1"
        assert data["data"]["selections"] == [{"lot_id": "lot_a", "quantity": 5.0}]

    @pytest.mark.unit
    def test_select_and_clear_mutually_exclusive_exits_2(
        self, runner: CliRunner, db: Database
    ) -> None:
        result = runner.invoke(
            app,
            [
                "investments",
                "lots",
                "select",
                "sell_1",
                "--lot",
                "lot_a:5",
                "--clear",
            ],
        )
        assert result.exit_code == 2

    @pytest.mark.unit
    def test_select_requires_lot_or_clear_exits_2(
        self, runner: CliRunner, db: Database
    ) -> None:
        result = runner.invoke(app, ["investments", "lots", "select", "sell_1"])
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# investments securities list / set
# ---------------------------------------------------------------------------


class TestSecuritiesListAndSet:
    """Tests for `investments securities list` and `securities set`."""

    @pytest.mark.unit
    def test_list_json_returns_added_security(
        self, runner: CliRunner, db: Database
    ) -> None:
        _add_security(runner, name="Apple Inc.", type_="equity", ticker="AAPL")
        result = runner.invoke(
            app, ["investments", "securities", "list", "--output", "json"]
        )
        assert result.exit_code == 0, result.output
        rows = json.loads(result.stdout)["data"]["rows"]
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"

    @pytest.mark.unit
    def test_set_method_preserves_other_fields(
        self, runner: CliRunner, db: Database
    ) -> None:
        security_id = _add_security(
            runner,
            name="Vanguard Total Stock Market",
            type_="mutual_fund",
            ticker="VTSAX",
        )

        set_result = runner.invoke(
            app,
            ["investments", "securities", "set", security_id, "--method", "average"],
        )
        assert set_result.exit_code == 0, set_result.output

        row = db.conn.execute(
            """
            SELECT name, ticker, cost_basis_method FROM app.securities
             WHERE security_id = ?
            """,  # noqa: S608  # test read, static SQL
            [security_id],
        ).fetchone()
        assert row == ("Vanguard Total Stock Market", "VTSAX", "average")

    @pytest.mark.unit
    def test_set_json_output_reports_record_id_class(
        self, runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Same typed-payload requirement as investments_add: the success
        # payload must route through InvestmentSecuritySetPayload, not a bare
        # dict, so the privacy audit trail records the real data class.
        security_id = _add_security(
            runner, name="Apple Inc.", type_="equity", ticker="AAPL"
        )
        captured: dict[str, object] = {}
        monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
        result = runner.invoke(
            app,
            [
                "investments",
                "securities",
                "set",
                security_id,
                "--method",
                "fifo",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        assert captured["classes_returned"] == ["record_id"]

    @pytest.mark.unit
    def test_set_no_fields_exits_2(self, runner: CliRunner, db: Database) -> None:
        result = runner.invoke(app, ["investments", "securities", "set", "sec_x"])
        assert result.exit_code == 2

    @pytest.mark.unit
    def test_set_unknown_security_exits_1(
        self, runner: CliRunner, db: Database
    ) -> None:
        result = runner.invoke(
            app,
            [
                "investments",
                "securities",
                "set",
                "does-not-exist",
                "--method",
                "fifo",
            ],
        )
        assert result.exit_code == 1
        assert "Traceback" not in result.stderr

    @pytest.mark.unit
    def test_add_invalid_cost_basis_method_exits_1_cleanly(
        self, runner: CliRunner, db: Database
    ) -> None:
        # Must surface as a clean UserError, not a raw duckdb.ConstraintException
        # traceback — the whole point of the upsert_security hard-validation fix.
        result = runner.invoke(
            app,
            [
                "investments",
                "securities",
                "add",
                "--name",
                "Apple Inc.",
                "--type",
                "equity",
                "--method",
                "lifo",
            ],
        )
        assert result.exit_code == 1
        assert "Traceback" not in result.stderr
        assert "lifo" in result.stderr

    @pytest.mark.unit
    def test_add_invalid_security_type_exits_1_cleanly(
        self, runner: CliRunner, db: Database
    ) -> None:
        result = runner.invoke(
            app,
            [
                "investments",
                "securities",
                "add",
                "--name",
                "Apple Inc.",
                "--type",
                "stock",
            ],
        )
        assert result.exit_code == 1
        assert "Traceback" not in result.stderr
        assert "stock" in result.stderr

    @pytest.mark.unit
    def test_set_invalid_cost_basis_method_exits_1_cleanly(
        self, runner: CliRunner, db: Database
    ) -> None:
        security_id = _add_security(runner, name="Apple Inc.", type_="equity")
        result = runner.invoke(
            app,
            ["investments", "securities", "set", security_id, "--method", "lifo"],
        )
        assert result.exit_code == 1
        assert "Traceback" not in result.stderr
        assert "lifo" in result.stderr
