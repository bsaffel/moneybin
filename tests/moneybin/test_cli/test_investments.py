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
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables


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

    @pytest.mark.unit
    def test_add_without_currency_stores_no_currency(
        self, runner: CliRunner, db: Database
    ) -> None:
        """An omitted ``--currency`` writes NULL, for core to inherit onto.

        A default of ``"USD"`` here labels a EUR account's lot in dollars before
        ``core.fct_investment_transactions`` gets the chance to inherit the
        account's own currency (multi-currency.md Requirement 3).
        """
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
                "--amount",
                "-1500.00",
            ],
        )
        assert result.exit_code == 0, result.output
        rows = db.conn.execute(
            "SELECT currency_code FROM raw.manual_investment_transactions"
        ).fetchall()
        assert rows == [(None,)]

    @pytest.mark.unit
    def test_add_with_currency_stores_it(self, runner: CliRunner, db: Database) -> None:
        """A supplied ``--currency`` is still stored verbatim."""
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
                "--amount",
                "-1500.00",
                "--currency",
                "EUR",
            ],
        )
        assert result.exit_code == 0, result.output
        rows = db.conn.execute(
            "SELECT currency_code FROM raw.manual_investment_transactions"
        ).fetchall()
        assert rows == [("EUR",)]


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
        # No `--wide`: all six columns are the default view, so the flag would
        # promise columns nothing is holding back and the command does not
        # offer one.
        result = runner.invoke(app, ["investments", "list"])
        assert result.exit_code == 0, result.output
        assert "buy" in result.output
        # Requirement 1: the event row was `qty=… amt=…`, which repeats a field
        # name per line and leaves nothing to align on.
        assert "┃" in result.stdout
        for header in ("date", "type", "security", "quantity", "amount", "currency"):
            assert header in result.stdout
        assert "qty=" not in result.stdout
        assert "amt=" not in result.stdout
        # Nothing was narrowed, so nothing claims it was.
        assert "columns shown" not in result.stdout

    @pytest.mark.unit
    def test_list_default_view_keeps_the_currency_beside_the_amount(
        self, runner: CliRunner, db: Database
    ) -> None:
        """An amount without its denomination is not an amount.

        This command has no currency filter, so one unfiltered call can render
        events from accounts in different currencies; `multi-currency.md` makes
        the row's own `currency_code` the canonical unit of its `amount`.
        Holding the column back behind `--wide` leaves two rows reading
        `1,500.00` that are not the same quantity, and nothing on screen says
        so. `investments holdings` already keeps it, so dropping it here also
        made two commands in one group disagree.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity, amount, currency_code)
            VALUES ('evt_ccy', 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    10, -1500.00, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )

        result = runner.invoke(app, ["investments", "list"])

        assert result.exit_code == 0, result.output
        assert "currency" in result.stdout
        assert "USD" in result.stdout

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
    def test_holdings_json_carries_no_stale_price_feed_caveat(
        self, runner: CliRunner, db: Database
    ) -> None:
        """Market value ships, so no row may claim it is unavailable."""
        result = runner.invoke(app, ["investments", "holdings", "--output", "json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["data"]["warnings"] == []

    @pytest.mark.unit
    def test_holdings_text_renders_value_and_an_unpriced_dash(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """An unpriced position renders "-", never a blank that reads as zero.

        Asks for `--wide` because it asserts on every figure, including the
        valuation status and observation date the default view leaves out.
        """
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   1200.00::DECIMAL(18,2) AS market_value,
                   200.00::DECIMAL(18,2) AS unrealized_gain,
                   DATE '2026-07-15' AS price_date, 'plaid' AS price_source,
                   0::INT AS days_since_observed, 'valued' AS valuation_status
            UNION ALL
            SELECT 'acct_brokerage', 'sec_2', 5::DECIMAL(28,10),
                   500.00::DECIMAL(18,2), 100.00::DECIMAL(28,10), 'USD',
                   CAST(NULL AS DECIMAL(18,2)), CAST(NULL AS DECIMAL(18,2)),
                   CAST(NULL AS DATE), CAST(NULL AS VARCHAR), CAST(NULL AS INT),
                   'unpriced'
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings", "--wide"])
        assert result.exit_code == 0, result.output
        priced = next(li for li in result.output.splitlines() if "sec_1" in li)
        unpriced = next(li for li in result.output.splitlines() if "sec_2" in li)
        # The priced row states every figure; the unpriced one dashes the three
        # it cannot know, rather than blanking them into an apparent zero.
        assert "1,200.00" in priced
        assert "+200.00" in priced
        assert "valued" in priced
        assert "2026-07-15 (0d)" in priced
        assert "unpriced" in unpriced
        # Market value and unrealized gain are the two figures a missing close
        # withholds; the cost basis and average cost are known either way.
        assert unpriced.count("-") == 2
        assert "1,200.00" not in unpriced
        assert "USD" in unpriced

    @pytest.mark.unit
    def test_holdings_default_view_keeps_money_whole_at_eighty_columns(
        self, runner: CliRunner, db: Database
    ) -> None:
        """The curated default fits 80 columns, so no amount folds mid-number.

        Deliberately takes no `wide_terminal`: 80 columns is the contract, and
        the fixture that widens the terminal is what let all nine columns look
        fine in every other test while `1,200.00` rendered as `1,200.` above
        `00` for anyone running a default-sized one.

        A width-measuring fit cannot produce this view — it keeps the first and
        last columns, so it drops `market value`, which is the figure the
        command exists to report.
        """
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   1200.00::DECIMAL(18,2) AS market_value,
                   200.00::DECIMAL(18,2) AS unrealized_gain,
                   DATE '2026-07-15' AS price_date, 'plaid' AS price_source,
                   0::INT AS days_since_observed, 'valued' AS valuation_status
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings"])

        assert result.exit_code == 0, result.output
        # Contiguous, on one line — a folded amount would split these.
        assert "1,200.00" in result.output
        assert "+200.00" in result.output
        assert "market value" in result.output
        # Every rendered line fits, borders included.
        assert max(len(li) for li in result.output.splitlines()) <= 80
        # The narrowing discloses itself, and names a flag this command has.
        assert "6 of 9 columns shown" in result.output
        assert "--wide" in result.output
        assert "valuation_status" not in result.output

    @pytest.mark.unit
    def test_holdings_default_view_says_why_a_position_has_no_value(
        self, runner: CliRunner, db: Database
    ) -> None:
        """`-` is two different facts, and the column that separates them stays.

        A position reads `-` either because no close resolved (`unpriced`) or
        because its share count is known wrong (`withheld`) — and the remedies
        differ: one wants a price refresh, the other wants the position fixed.
        The service's own warning tells the reader to "see each row's
        `valuation_status`", so holding that column back behind `--wide` made
        the instruction name something not on screen; `-q` then drops the
        warning too, leaving an unexplained dash and no way to ask why.
        """
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_2' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   NULL::DECIMAL(18,2) AS market_value,
                   NULL::DECIMAL(18,2) AS unrealized_gain,
                   NULL::DATE AS price_date, NULL AS price_source,
                   NULL::INT AS days_since_observed,
                   'unpriced' AS valuation_status
            """  # noqa: S608  # test fixture view, literal test data only
        )

        result = runner.invoke(app, ["investments", "holdings", "-q"])

        assert result.exit_code == 0, result.output
        assert "unpriced" in result.stdout
        # The warning is the suppressed half; the row's own status is not.
        assert "valuation_status" not in result.stderr

    @pytest.mark.unit
    def test_holdings_json_degrades_on_a_source_overlap(
        self, runner: CliRunner, db: Database
    ) -> None:
        """CLI JSON carries the same machine-readable state the MCP tool does.

        Both surfaces read one service, so the code that says "these numbers
        would double-count" must reach both envelopes — a caller scripting the
        CLI is as entitled to branch on it as an agent driving MCP.
        """
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_2' AS security_id,
                   20::DECIMAL(28,10) AS quantity,
                   2000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   NULL::DECIMAL(18,2) AS market_value,
                   NULL::DECIMAL(18,2) AS unrealized_gain,
                   NULL::DATE AS price_date, NULL AS price_source,
                   NULL::INT AS days_since_observed,
                   'source_overlap' AS valuation_status
            """  # noqa: S608  # test fixture view, literal test data only
        )

        result = runner.invoke(app, ["investments", "holdings", "--output", "json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["summary"]["degraded"] is True
        assert payload["summary"]["degraded_reason"].startswith(
            "investment_source_overlap:"
        )

    @pytest.mark.unit
    def test_holdings_text_reports_the_stalest_close_as_a_number(
        self, runner: CliRunner, db: Database
    ) -> None:
        """A four-month-old close discloses its age on the portfolio line."""
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   1200.00::DECIMAL(18,2) AS market_value,
                   200.00::DECIMAL(18,2) AS unrealized_gain,
                   DATE '2026-03-02' AS price_date, 'plaid' AS price_source,
                   135::INT AS days_since_observed,
                   'carried_forward' AS valuation_status
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings"])
        assert result.exit_code == 0, result.output
        assert "max_days_since_observed=135" in result.output

    @pytest.mark.unit
    def test_holdings_text_dashes_the_stalest_close_when_nothing_is_priced(
        self, runner: CliRunner, db: Database
    ) -> None:
        """An undefined max renders "-", never a 0 that reads as fresh."""
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   CAST(NULL AS DECIMAL(18,2)) AS market_value,
                   CAST(NULL AS DECIMAL(18,2)) AS unrealized_gain,
                   CAST(NULL AS DATE) AS price_date,
                   CAST(NULL AS VARCHAR) AS price_source,
                   CAST(NULL AS INT) AS days_since_observed,
                   'unpriced' AS valuation_status
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings"])
        assert result.exit_code == 0, result.output
        assert "max_days_since_observed=-" in result.output

    @pytest.mark.unit
    def test_holdings_text_totals_a_single_currency_portfolio(
        self, runner: CliRunner, db: Database
    ) -> None:
        """One currency across the priced rows — the total prints."""
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   1200.00::DECIMAL(18,2) AS market_value,
                   200.00::DECIMAL(18,2) AS unrealized_gain,
                   DATE '2026-07-15' AS price_date, 'plaid' AS price_source,
                   0::INT AS days_since_observed, 'valued' AS valuation_status
            UNION ALL
            SELECT 'acct_brokerage', 'sec_2', 5::DECIMAL(28,10),
                   500.00::DECIMAL(18,2), 100.00::DECIMAL(28,10), 'USD',
                   800.00::DECIMAL(18,2), 300.00::DECIMAL(18,2),
                   DATE '2026-07-15', 'plaid', 0::INT, 'valued'
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings"])
        assert result.exit_code == 0, result.output
        assert "market_value=2,000.00 USD" in result.output
        assert "mixed currencies" not in result.output

    @pytest.mark.unit
    def test_holdings_text_refuses_a_mixed_currency_total_with_no_rate(
        self, runner: CliRunner, db: Database
    ) -> None:
        """EUR beside USD and no stored rate — print the split, never a sum."""
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   1200.00::DECIMAL(18,2) AS market_value,
                   200.00::DECIMAL(18,2) AS unrealized_gain,
                   DATE '2026-07-15' AS price_date, 'plaid' AS price_source,
                   0::INT AS days_since_observed, 'valued' AS valuation_status
            UNION ALL
            SELECT 'acct_brokerage', 'sec_2', 5::DECIMAL(28,10),
                   500.00::DECIMAL(18,2), 100.00::DECIMAL(28,10), 'EUR',
                   900.00::DECIMAL(18,2), 400.00::DECIMAL(18,2),
                   DATE '2026-07-15', 'plaid', 0::INT, 'valued'
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings"])
        assert result.exit_code == 0, result.output
        assert (
            "market_value=- (mixed currencies, no home currency or no rate)"
            in result.output
        )
        assert "USD=1,200.00" in result.output
        assert "EUR=900.00" in result.output
        # The wrong sum must appear nowhere in the output, in either spelling:
        # the footer formats through `format_money` now, so checking only the
        # bare form would let a formatted wrong sum through.
        assert "2100.00" not in result.output
        assert "2,100.00" not in result.output

    @pytest.mark.unit
    def test_holdings_text_shows_the_originals_behind_a_converted_total(
        self, runner: CliRunner, db: Database
    ) -> None:
        """A converted figure never stands alone — the originals print beside it.

        Same two positions as the refusal above, plus the rate that makes the
        pair priceable: 900.00 EUR x 1.10 = 990.00 USD, added to 1200.00 USD.
        """
        ProfileSettingsRepo(db).set_home_currency("USD", actor="test")
        db.execute(
            """
            INSERT INTO raw.exchange_rates
                (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
            VALUES ('EUR', 'USD', DATE '2026-07-15', 1.10, 'frankfurter', NOW())
            """
        )
        db.conn.execute(
            """
            CREATE OR REPLACE VIEW core.dim_holdings AS
            SELECT 'acct_brokerage' AS account_id, 'sec_1' AS security_id,
                   10::DECIMAL(28,10) AS quantity,
                   1000.00::DECIMAL(18,2) AS cost_basis,
                   100.00::DECIMAL(28,10) AS average_cost,
                   'USD' AS currency_code,
                   1200.00::DECIMAL(18,2) AS market_value,
                   200.00::DECIMAL(18,2) AS unrealized_gain,
                   DATE '2026-07-15' AS price_date, 'plaid' AS price_source,
                   0::INT AS days_since_observed, 'valued' AS valuation_status
            UNION ALL
            SELECT 'acct_brokerage', 'sec_2', 5::DECIMAL(28,10),
                   500.00::DECIMAL(18,2), 100.00::DECIMAL(28,10), 'EUR',
                   900.00::DECIMAL(18,2), 400.00::DECIMAL(18,2),
                   DATE '2026-07-15', 'plaid', 0::INT, 'valued'
            """  # noqa: S608  # test fixture view, literal test data only
        )
        result = runner.invoke(app, ["investments", "holdings"])
        assert result.exit_code == 0, result.output
        assert "market_value=2,190.00 USD" in result.output
        assert "(converted from USD=1,200.00 EUR=900.00)" in result.output
        # Requirement 10: the originals say what was converted, the rate says
        # what converted it. Asserted on the split streams because Click 8.2+
        # interleaves both into `result.output`, so `in result.output` cannot
        # tell a diagnostic on stderr from one polluting the data on stdout.
        assert "💱 Converted from EUR at 1.10" in result.stderr
        assert "💱" not in result.stdout

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

    @pytest.mark.unit
    def test_gains_discloses_an_incomplete_basis_even_under_quiet(
        self, runner: CliRunner, db: Database
    ) -> None:
        """A conservative gain may not print as though it were the whole figure.

        `gain` is computed against a basis the row itself knows is a floor, so
        saying so qualifies the answer rather than commenting on the run. This
        command raises no other warning, so the line is not gated on `-q` — the
        1099-B figures would otherwise read as authoritative with nothing on
        screen to say they are not.

        `investments lots list` meets the same requirement with a per-row
        marker in its default view. This table cannot: measured at 80 columns,
        a seventh column folds the disposal date and the security id and breaks
        the marker across three lines. So the marker is declared and reachable
        with `--wide`, and the disclosure itself always prints. Both halves are
        pinned here, because either alone leaves `-q` output silent about it.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('gain_quiet', 'acct_brokerage', 'sec_1', 'sell_1', 'lot_a',
                    5, '2024-01-01', '2024-06-12', 950.00, 0.00, 950.00,
                    'long', 'fifo', true, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )

        quiet = runner.invoke(app, ["investments", "gains", "-q"])

        assert quiet.exit_code == 0, quiet.output
        assert "incomplete cost basis" in quiet.stderr
        # The disclosure is a diagnostic, so it stays off the data stream.
        assert "incomplete cost basis" not in quiet.stdout

    @pytest.mark.unit
    def test_gains_wide_names_which_rows_have_an_incomplete_basis(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """The warning counts the rows; only the column says which ones."""
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('gain_floor', 'acct_brokerage', 'sec_1', 'sell_1', 'lot_a',
                    5, '2024-01-01', '2024-06-12', 950.00, 0.00, 950.00,
                    'long', 'fifo', true, 'USD'),
                   ('gain_whole', 'acct_brokerage', 'sec_2', 'sell_2', 'lot_b',
                    5, '2024-01-01', '2024-06-13', 950.00, 750.00, 200.00,
                    'long', 'fifo', false, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )

        wide = runner.invoke(app, ["investments", "gains", "--wide"])

        assert wide.exit_code == 0, wide.output
        assert wide.stdout.count("basis_incomplete") == 1

        narrow = runner.invoke(app, ["investments", "gains"])

        assert narrow.exit_code == 0, narrow.output
        assert "basis_incomplete" not in narrow.stdout

    @pytest.mark.unit
    def test_gains_text_names_its_columns_and_signs_the_gain(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """Requirements 1, 12, and 14: named columns, and one signed answer.

        Proceeds and basis are positive by construction, so they render
        unsigned; the gain is the figure whose direction is the point, so it
        carries its sign.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('gain_signed', 'acct_brokerage', 'sec_1', 'sell_1', 'lot_a',
                    5, '2024-01-01', '2024-06-12', 1950.00, 1750.00, 200.00,
                    'long', 'fifo', false, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )

        result = runner.invoke(app, ["investments", "gains", "--wide"])

        assert result.exit_code == 0, result.output
        assert "┃" in result.stdout
        for header in ("disposed", "security", "proceeds", "basis", "gain", "term"):
            assert header in result.stdout
        assert "1,950.00" in result.stdout
        assert "1,750.00" in result.stdout
        assert "+200.00" in result.stdout
        assert "proceeds=" not in result.stdout

    @pytest.mark.unit
    def test_holdings_help_does_not_promise_status_behind_wide(
        self, runner: CliRunner
    ) -> None:
        """Help naming the wrong home for a column is worse than silence.

        `status` moved into the default view when it stopped hiding behind
        `--wide`, but the help still promised the flag would add it — so it
        misdescribed both what prints by default and what the flag buys. This
        is the third help string in this branch invalidated by curating a
        column, so the false claim is pinned rather than just corrected.
        """
        result = runner.invoke(app, ["investments", "holdings", "--help"])

        assert result.exit_code == 0, result.output
        help_text = " ".join(result.stdout.split())
        assert "adds the ``status``" not in help_text
        assert "``status``" in help_text
        for wide_column in ("cost basis", "average cost"):
            assert wide_column in help_text

    @pytest.mark.unit
    def test_gains_default_view_keeps_the_currency_beside_the_gain(
        self, runner: CliRunner, db: Database
    ) -> None:
        """A gain without its denomination is not a gain.

        Takes no ``wide_terminal`` on purpose: the column has to reach the
        curated default view, not merely the full projection. This command has
        no currency filter, so one unfiltered call can span accounts
        denominated differently, and `multi-currency.md` makes the row's own
        `currency_code` the canonical unit of its `proceeds`, `basis` and
        `gain`. Two rows reading `+200.00` are then not the same quantity.
        `investments list` and `investments holdings` already keep it.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('gain_ccy', 'acct_brokerage', 'sec_1', 'sell_1', 'lot_a',
                    5, '2024-01-01', '2024-06-12', 1950.00, 1750.00, 200.00,
                    'long', 'fifo', false, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )

        result = runner.invoke(app, ["investments", "gains"])

        assert result.exit_code == 0, result.output
        assert "currency" in result.stdout
        assert "USD" in result.stdout


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
        self, runner: CliRunner, db: Database, wide_terminal: None
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
        result = runner.invoke(app, ["investments", "lots", "list", "--wide"])
        assert result.exit_code == 0, result.output
        assert "basis_incomplete" in result.stdout
        assert "incomplete" in result.stderr

    @pytest.mark.unit
    def test_lots_list_default_view_marks_an_incomplete_basis_under_quiet(
        self, runner: CliRunner, db: Database
    ) -> None:
        """A basis known to be wrong may not print as though it were right.

        The marker qualifies the `basis` cell rather than commenting on the
        run, so it belongs to the answer. Behind `--wide` it had one substitute
        — a warning line — and `render_note` puts that on stderr where `-q`
        drops it, so `-q` output showed a conservative basis with nothing
        saying it is a floor rather than a figure.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES ('lot_quiet', 'acct_brokerage', 'sec_1', '2024-01-15',
                    'transfer_in', 10, 10, 0.00, 0.00, 'fifo', 'USD', true, true)
            """  # noqa: S608  # test fixture insert, static SQL
        )

        result = runner.invoke(app, ["investments", "lots", "list", "-q"])

        assert result.exit_code == 0, result.output
        assert "basis_incomplete" in result.stdout
        # The warning is the suppressed half — that is the point of `-q`.
        assert "incomplete" not in result.stderr

    @pytest.mark.unit
    def test_lots_list_reaches_the_currency_through_wide(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """The one curated table whose default view cannot afford `currency`.

        Its three siblings keep the column, since none of the four takes a
        currency filter and `multi-currency.md` makes the row's own
        `currency_code` the canonical unit of its amount. Six columns already
        spend this table's 80-column budget, though: measured at 80 with
        production-width ids, the default already folds the lot id by a
        character and breaks `⚠️ basis_incomplete` across lines, and a seventh
        column folds `security` too and takes the marker to three lines. So it
        is declared rather than omitted, and `--wide` is the escape; both
        halves are pinned here, because a column left undeclared has no escape
        at all.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES ('lot_ccy', 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    10, 10, 1500.00, 1500.00, 'fifo', 'USD', true, false)
            """  # noqa: S608  # test fixture insert, static SQL
        )

        wide = runner.invoke(app, ["investments", "lots", "list", "--wide"])

        assert wide.exit_code == 0, wide.output
        assert "currency" in wide.stdout
        assert "USD" in wide.stdout

        narrow = runner.invoke(app, ["investments", "lots", "list"])

        assert narrow.exit_code == 0, narrow.output
        assert "currency" not in narrow.stdout

    @pytest.mark.unit
    def test_lots_list_wide_shows_state_and_the_help_text_says_so(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """`--wide` is every declared column, not a second curated set.

        `column_view` projects the whole declaration whenever `wide` is set and
        ignores `default` entirely, so `--wide` without `--all` restores
        `state` as well as the currency and the method — reading `open` on
        every row, since `--open` is still in force. That is the honest
        meaning of the flag and the disclosure line already implies it by
        counting three hidden columns rather than two; what was wrong was the
        `--help` text promising only two of them. The behaviour and the
        sentence describing it are pinned together, because this branch has
        now invalidated four help strings by curating a column and the
        sentence is the half nothing else checks.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES ('lot_wide', 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    10, 10, 1500.00, 1500.00, 'fifo', 'USD', true, false)
            """  # noqa: S608  # test fixture insert, static SQL
        )

        wide = runner.invoke(app, ["investments", "lots", "list", "--wide"])

        assert wide.exit_code == 0, wide.output
        assert "state" in wide.stdout
        assert "open" in wide.stdout

        help_text = runner.invoke(app, ["investments", "lots", "list", "--help"]).stdout
        normalized = " ".join(help_text.split())
        assert "adds the currency and the cost-basis method" not in normalized, (
            "the help text promises two columns behind --wide while the flag "
            "restores three; a curated view must describe itself correctly"
        )

    @pytest.mark.unit
    def test_lots_list_all_says_which_lots_are_closed(
        self, runner: CliRunner, db: Database
    ) -> None:
        """`--all` asks for mixed history, so the row has to say which it is.

        The state is strictly derivable — `core.fct_investment_lots` defines
        `is_open` as `remaining_quantity > 0` — but that rule appears neither
        on screen nor in `--help`, so without the column a reader infers a
        lifecycle state from a numeric cell via a rule nothing shows them. The
        column earns its slot exactly when the result can contain both kinds,
        so `--open` (the default) does not pay for it.
        """
        db.conn.executemany(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES (?, 'acct_brokerage', 'sec_1', '2024-01-15', 'buy',
                    10, ?, 1500.00, ?, 'fifo', 'USD', ?, false)
            """,  # noqa: S608  # test fixture insert, static SQL
            [
                ["lot_still_open", Decimal("10"), Decimal("1500.00"), True],
                ["lot_sold_off", Decimal("0"), Decimal("0.00"), False],
            ],
        )

        every = runner.invoke(app, ["investments", "lots", "list", "--all"])

        assert every.exit_code == 0, every.output
        assert "state" in every.stdout
        assert "closed" in every.stdout

        open_only = runner.invoke(app, ["investments", "lots", "list"])

        assert open_only.exit_code == 0, open_only.output
        assert "state" not in open_only.stdout

    @pytest.mark.unit
    def test_lots_list_names_its_columns_instead_of_its_fields(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """Requirement 1: `key=value` at a reader is a table spelled badly.

        The row read `acq=2024-01-15 remaining=10 basis_remaining=0.00
        method=fifo`, which repeats a field name on every line and leaves the
        values unalignable. The header carries each name once instead.
        """
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 acquisition_type, original_quantity, remaining_quantity,
                 cost_basis_total, cost_basis_remaining, cost_basis_method,
                 currency_code, is_open, basis_incomplete)
            VALUES ('lot_named', 'acct_brokerage', 'sec_1', '2024-01-15',
                    'buy', 10, 10, 1234.50, 1234.50, 'fifo', 'USD', true, false)
            """  # noqa: S608  # test fixture insert, static SQL
        )

        result = runner.invoke(app, ["investments", "lots", "list", "--wide"])

        assert result.exit_code == 0, result.output
        assert "┃" in result.stdout
        for header in ("lot", "security", "acquired", "remaining", "method", "state"):
            assert header in result.stdout
        assert "remaining=" not in result.stdout
        # Requirement 11: the basis is an amount, so it is separated.
        assert "1,234.50" in result.stdout


# ---------------------------------------------------------------------------
# investments lots select / --clear
# ---------------------------------------------------------------------------


def _elect_cost_basis_method(db: Database, method: str) -> None:
    """Elect ``method`` on ``sec_1``, the security the lot fixtures reference.

    Bypasses the CLI ``_add_security`` helper above deliberately: that path mints
    a random id, and the lot/disposal fixtures here reference ``sec_1`` by name.
    """
    SecuritiesRepo(db).upsert(
        security_id="sec_1",
        name="Apple Inc.",
        security_type="equity",
        ticker="AAPL",
        cost_basis_method=method,
        actor="cli",
    )


def _seed_single_lot_disposal(db: Database, method: str) -> None:
    """Elect ``method`` on ``sec_1``, then seed a −5 sell against one 5-unit lot.

    ``investments lots select`` refuses a selection the resolved method would
    discard, so a fixture that only writes ``core.*`` rows has to state the
    election the write depends on.
    """
    _elect_cost_basis_method(db, method)
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


class TestLotsSelect:
    """Tests for `investments lots select` (set + --clear)."""

    @pytest.mark.unit
    def test_select_sets_and_clear_removes(
        self, runner: CliRunner, db: Database
    ) -> None:
        _elect_cost_basis_method(db, "specific")
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
        _seed_single_lot_disposal(db, "specific")
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
    def test_select_under_a_non_specific_method_exits_1_and_writes_nothing(
        self, runner: CliRunner, db: Database
    ) -> None:
        _seed_single_lot_disposal(db, "fifo")
        result = runner.invoke(
            app, ["investments", "lots", "select", "sell_1", "--lot", "lot_a:5"]
        )
        assert result.exit_code == 1, result.output
        assert "specific" in result.stderr
        count = db.conn.execute(
            "SELECT COUNT(*) FROM app.lot_selections "
            "WHERE investment_transaction_id = 'sell_1'"  # noqa: S608  # test read, static SQL
        ).fetchone()
        assert count is not None
        assert count[0] == 0

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
    def test_list_text_names_its_columns(
        self, runner: CliRunner, db: Database, wide_terminal: None
    ) -> None:
        """Requirement 1: four padded values are a table, so render one."""
        _add_security(runner, name="Apple Inc.", type_="equity", ticker="AAPL")

        result = runner.invoke(app, ["investments", "securities", "list"])

        assert result.exit_code == 0, result.output
        assert "┃" in result.stdout
        for header in ("security", "ticker", "name", "type"):
            assert header in result.stdout
        assert "AAPL" in result.stdout
        assert "Apple Inc." in result.stdout

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
