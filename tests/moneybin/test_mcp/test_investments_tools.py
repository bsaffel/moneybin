# tests/moneybin/test_mcp/test_investments_tools.py
"""Tests for investments.* MCP tools (Task 16).

Read tools query core.* investment tables/views, which are SQLMesh-managed
in production; ``create_core_dim_stub_views`` materializes them with the
real column shapes for unit tests — the same helper
``tests/moneybin/test_services/test_investment_service.py`` uses (the
service these tools wrap). Seeding goes directly into those core tables
(per the Task-16 brief) rather than through the SQLMesh transform, since the
transform's correctness is covered by ``test_investment_models_transform.py``
and the engine unit tests — this module is testing the MCP tool boundary
(envelope shape, derived sensitivity, warnings, error classification), not
the transform.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import NamedTuple

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.investments import (
    investments,
    investments_gains,
    investments_holdings,
    investments_lots,
    investments_lots_select,
    investments_record,
    investments_securities,
    investments_securities_set,
    register_investments_tools,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from tests.moneybin.db_helpers import create_core_dim_stub_views

pytestmark = pytest.mark.usefixtures("mcp_db")

_ACCOUNT = "ACC001"


def _seed_investment_core() -> None:
    """Materialize the SQLMesh-managed investment core.* stubs for this test's DB."""
    with get_database(read_only=False) as db:
        create_core_dim_stub_views(db)


def _add_security(**kwargs: object) -> str:
    """Insert one security via the real repo; return its (possibly minted) id."""
    defaults: dict[str, object] = {
        "security_id": None,
        "name": "Apple Inc.",
        "security_type": "equity",
        "ticker": "AAPL",
        "actor": "test",
    }
    defaults.update(kwargs)
    with get_database(read_only=False) as db:
        event = SecuritiesRepo(db).upsert(**defaults)  # type: ignore[arg-type]
    assert event.target_id is not None
    return event.target_id


def _insert_event(
    *,
    investment_transaction_id: str,
    account_id: str = _ACCOUNT,
    security_id: str | None,
    trade_date: date = date(2024, 1, 15),
    type_: str = "buy",
    quantity: Decimal | None = Decimal("10"),
    amount: Decimal | None = Decimal("-1500.00"),
) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity, amount, currency_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'USD')
            """,  # noqa: S608  # test fixture insert, static SQL
            [
                investment_transaction_id,
                account_id,
                security_id,
                trade_date,
                type_,
                quantity,
                amount,
            ],
        )


def _insert_lot(
    *,
    lot_id: str,
    account_id: str = _ACCOUNT,
    security_id: str,
    acquisition_date: date = date(2024, 1, 15),
    remaining_quantity: Decimal = Decimal("10"),
    cost_basis_remaining: Decimal = Decimal("1500.00"),
    is_open: bool = True,
    basis_incomplete: bool = False,
) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date, acquisition_type,
                 original_quantity, remaining_quantity, cost_basis_total,
                 cost_basis_remaining, cost_basis_method, currency_code, is_open,
                 basis_incomplete)
            VALUES (?, ?, ?, ?, 'buy', ?, ?, ?, ?, 'fifo', 'USD', ?, ?)
            """,  # noqa: S608  # test fixture insert, static SQL
            [
                lot_id,
                account_id,
                security_id,
                acquisition_date,
                remaining_quantity,
                remaining_quantity,
                cost_basis_remaining,
                cost_basis_remaining,
                is_open,
                basis_incomplete,
            ],
        )


def _insert_gain(
    *,
    realized_gain_id: str,
    account_id: str = _ACCOUNT,
    security_id: str,
    disposal_txn_id: str = "sell_1",
    lot_id: str = "lot_a",
    disposal_date: date = date(2024, 6, 12),
    proceeds: Decimal = Decimal("950.00"),
    cost_basis: Decimal = Decimal("750.00"),
    gain_loss: Decimal = Decimal("200.00"),
    term: str = "long",
    basis_incomplete: bool = False,
) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id, lot_id,
                 quantity, acquisition_date, disposal_date, proceeds, cost_basis,
                 gain_loss, term, cost_basis_method, basis_incomplete, currency_code)
            VALUES (?, ?, ?, ?, ?, 5, '2024-01-01'::DATE, ?, ?, ?, ?, ?, 'fifo', ?, 'USD')
            """,  # noqa: S608  # test fixture insert, static SQL
            [
                realized_gain_id,
                account_id,
                security_id,
                disposal_txn_id,
                lot_id,
                disposal_date,
                proceeds,
                cost_basis,
                gain_loss,
                term,
                basis_incomplete,
            ],
        )


class _Holding(NamedTuple):
    """One ``core.dim_holdings`` fixture row (valuation defaults to unpriced)."""

    account_id: str
    security_id: str
    quantity: str = "10"
    cost_basis: str = "1000.00"
    average_cost: str | None = "100.00"
    currency_code: str = "USD"
    market_value: str | None = None
    unrealized_gain: str | None = None
    price_date: str | None = None
    price_source: str | None = None
    days_since_observed: str | None = None
    valuation_status: str = "unpriced"


def _replace_holdings_view(rows: list[_Holding]) -> None:
    """Override the empty core.dim_holdings stub with literal test rows.

    Mirrors ``test_investment_service.py``'s helper of the same name — values
    are literal test data, not user input (security.md's test-fixture
    exception).
    """
    parts: list[str] = []
    for h in rows:
        avg_sql = (
            "CAST(NULL AS DECIMAL(28,10))"
            if h.average_cost is None
            else f"{h.average_cost}::DECIMAL(28,10)"
        )
        mv_sql = (
            "CAST(NULL AS DECIMAL(18,2))"
            if h.market_value is None
            else f"{h.market_value}::DECIMAL(18,2)"
        )
        ug_sql = (
            "CAST(NULL AS DECIMAL(18,2))"
            if h.unrealized_gain is None
            else f"{h.unrealized_gain}::DECIMAL(18,2)"
        )
        pd_sql = (
            "CAST(NULL AS DATE)" if h.price_date is None else f"DATE '{h.price_date}'"
        )
        ps_sql = (
            "CAST(NULL AS VARCHAR)" if h.price_source is None else f"'{h.price_source}'"
        )
        dso_sql = (
            "CAST(NULL AS INT)"
            if h.days_since_observed is None
            else f"{h.days_since_observed}::INT"
        )
        parts.append(
            f"SELECT '{h.account_id}' AS account_id, "
            f"'{h.security_id}' AS security_id, "
            f"{h.quantity}::DECIMAL(28,10) AS quantity, "
            f"{h.cost_basis}::DECIMAL(18,2) AS cost_basis, "
            f"{avg_sql} AS average_cost, '{h.currency_code}' AS currency_code, "
            f"{mv_sql} AS market_value, {ug_sql} AS unrealized_gain, "
            f"{pd_sql} AS price_date, {ps_sql} AS price_source, "
            f"{dso_sql} AS days_since_observed, "
            f"'{h.valuation_status}' AS valuation_status"
        )
    select_sql = " UNION ALL ".join(parts)
    with get_database(read_only=False) as db:
        db.execute(  # noqa: S608  # test fixture view, literal test data only
            f"CREATE OR REPLACE VIEW core.dim_holdings AS {select_sql}"
        )


def _count_raw_investment_rows() -> int:
    """Count rows in raw.manual_investment_transactions (the record write target)."""
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT COUNT(*) FROM raw.manual_investment_transactions"
        ).fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    """Verify all investments tools are registered with the FastMCP server."""

    @pytest.mark.unit
    async def test_all_investments_tools_registered(self) -> None:
        srv = FastMCP("test")
        register_investments_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert names == {
            "investments",
            "investments_holdings",
            "investments_lots",
            "investments_gains",
            "investments_securities",
            "investments_record",
            "investments_securities_set",
            "investments_lots_select",
            "investments_securities_links_pending",
            "investments_securities_links_set",
            "investments_securities_links_history",
        }

    @pytest.mark.unit
    async def test_holdings_description_explains_the_staleness_number(self) -> None:
        """The description is the agent's only contract for the new field."""
        srv = FastMCP("test")
        register_investments_tools(srv)
        tools = await srv._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        tool = next(t for t in tools if t.name == "investments_holdings")
        assert tool.description is not None
        assert "max_days_since_observed" in tool.description

    @pytest.mark.unit
    async def test_holdings_description_does_not_claim_display_currency(self) -> None:
        """market_value is per-row, so the repo-wide currency line must not apply."""
        srv = FastMCP("test")
        register_investments_tools(srv)
        tools = await srv._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        holdings = next(t for t in tools if t.name == "investments_holdings")
        assert holdings.description is not None
        assert (
            "Amounts are in the currency named by `summary.display_currency`"
            not in holdings.description
        )
        assert "each row's own currency_code" in holdings.description
        # Paired positive: the sibling tools still carry the repo-wide line, so
        # this asserts a holdings-specific correction, not a global removal.
        gains = next(t for t in tools if t.name == "investments_gains")
        assert gains.description is not None
        assert (
            "Amounts are in the currency named by `summary.display_currency`"
            in gains.description
        )


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------


class TestInvestmentsLedger:
    """Tests for the investments (ledger) MCP tool."""

    @pytest.mark.unit
    async def test_returns_rows_with_high_sensitivity(self, mcp_db: Path) -> None:
        """quantity/price/amount are TXN_AMOUNT (Tier.HIGH) -> derived 'high'."""
        _seed_investment_core()
        sec = _add_security()
        _insert_event(investment_transaction_id="evt_1", security_id=sec)
        result = await investments()
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "high"
        rows = parsed["data"]["rows"]
        assert len(rows) == 1
        assert rows[0]["investment_transaction_id"] == "evt_1"
        assert rows[0]["account_id"] == _ACCOUNT
        assert parsed["data"]["warnings"] == []

    @pytest.mark.unit
    async def test_filters_by_type(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_event(investment_transaction_id="evt_buy", security_id=sec, type_="buy")
        _insert_event(
            investment_transaction_id="evt_sell",
            security_id=sec,
            type_="sell",
            quantity=Decimal("-5"),
            amount=Decimal("750.00"),
        )
        result = await investments(type_filter="sell")
        rows = result.to_dict()["data"]["rows"]
        assert [r["investment_transaction_id"] for r in rows] == ["evt_sell"]

    @pytest.mark.unit
    async def test_unknown_account_ref_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        _seed_investment_core()
        result = await investments(account="does-not-exist")
        parsed = result.to_dict()
        assert parsed["status"] == "error"


class TestInvestmentsHoldings:
    """Tests for the investments_holdings MCP tool."""

    @pytest.mark.unit
    async def test_empty_result_is_high_sensitivity_and_unwarned(
        self, mcp_db: Path
    ) -> None:
        _seed_investment_core()
        result = await investments_holdings()
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "high"
        assert parsed["data"]["warnings"] == []

    @pytest.mark.unit
    async def test_returns_seeded_rows_with_valuation(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _replace_holdings_view([
            _Holding(
                _ACCOUNT,
                sec,
                quantity="15",
                cost_basis="2475.00",
                average_cost="165.00",
                market_value="2700.00",
                unrealized_gain="225.00",
                price_date="2026-07-15",
                price_source="plaid",
                days_since_observed="0",
                valuation_status="valued",
            ),
        ])
        result = await investments_holdings()
        parsed = result.to_dict()
        rows = parsed["data"]["rows"]
        assert len(rows) == 1
        assert rows[0]["quantity"] == 15.0
        assert rows[0]["cost_basis"] == 2475.0
        # Pillar C: the agent sees the value, not a "no price feed" caveat.
        assert rows[0]["market_value"] == 2700.0
        assert rows[0]["unrealized_gain"] == 225.0
        assert rows[0]["valuation_status"] == "valued"
        assert parsed["data"]["warnings"] == []

    @pytest.mark.unit
    async def test_withheld_row_reports_null_value_and_a_counted_warning(
        self, mcp_db: Path
    ) -> None:
        """A withheld position must not surface as zero — null plus a count."""
        _seed_investment_core()
        sec = _add_security()
        _replace_holdings_view([
            _Holding(_ACCOUNT, sec, valuation_status="withheld"),
        ])
        result = await investments_holdings()
        parsed = result.to_dict()
        assert parsed["data"]["rows"][0]["market_value"] is None
        assert parsed["data"]["rows"][0]["unrealized_gain"] is None
        assert "1" in parsed["data"]["warnings"][0]

    @pytest.mark.unit
    async def test_stale_portfolio_discloses_its_age_not_a_warning(
        self, mcp_db: Path
    ) -> None:
        """A four-month-old close publishes its age; no warning is raised."""
        _seed_investment_core()
        sec = _add_security()
        _replace_holdings_view([
            _Holding(
                _ACCOUNT,
                sec,
                market_value="2700.00",
                unrealized_gain="225.00",
                price_date="2026-03-02",
                price_source="plaid",
                days_since_observed="135",
                valuation_status="carried_forward",
            ),
        ])
        result = await investments_holdings()
        parsed = result.to_dict()
        assert parsed["data"]["max_days_since_observed"] == 135
        assert parsed["data"]["warnings"] == []

    @pytest.mark.unit
    async def test_max_days_since_observed_is_null_when_nothing_is_priced(
        self, mcp_db: Path
    ) -> None:
        """Null, not 0 — a 0 would read as "every close is today's"."""
        _seed_investment_core()
        sec = _add_security()
        _replace_holdings_view([
            _Holding(_ACCOUNT, sec, valuation_status="unpriced"),
        ])
        result = await investments_holdings()
        assert result.to_dict()["data"]["max_days_since_observed"] is None

    @pytest.mark.unit
    async def test_single_currency_portfolio_publishes_a_total(
        self, mcp_db: Path
    ) -> None:
        """The common case: one currency, one summable total."""
        _seed_investment_core()
        sec_a = _add_security(security_id="sec_usd_a", ticker="AAA")
        sec_b = _add_security(security_id="sec_usd_b", ticker="BBB")
        _replace_holdings_view([
            _Holding(
                _ACCOUNT,
                sec_a,
                currency_code="USD",
                market_value="1200.00",
                unrealized_gain="200.00",
                price_date="2026-07-15",
                price_source="plaid",
                days_since_observed="0",
                valuation_status="valued",
            ),
            _Holding(
                _ACCOUNT,
                sec_b,
                currency_code="USD",
                market_value="800.00",
                unrealized_gain="-200.00",
                price_date="2026-07-15",
                price_source="plaid",
                days_since_observed="0",
                valuation_status="valued",
            ),
        ])
        result = await investments_holdings()
        data = result.to_dict()["data"]
        assert data["total_market_value"] == 2000.0
        assert data["market_value_by_currency"] == {"USD": 2000.0}

    @pytest.mark.unit
    async def test_mixed_currency_portfolio_publishes_no_total(
        self, mcp_db: Path
    ) -> None:
        """No single figure an agent could report as "the portfolio value"."""
        _seed_investment_core()
        sec_a = _add_security(security_id="sec_usd", ticker="AAA")
        sec_b = _add_security(security_id="sec_eur", ticker="BBB")
        _replace_holdings_view([
            _Holding(
                _ACCOUNT,
                sec_a,
                currency_code="USD",
                market_value="1200.00",
                unrealized_gain="200.00",
                price_date="2026-07-15",
                price_source="plaid",
                days_since_observed="0",
                valuation_status="valued",
            ),
            _Holding(
                _ACCOUNT,
                sec_b,
                currency_code="EUR",
                market_value="900.00",
                unrealized_gain="100.00",
                price_date="2026-07-15",
                price_source="plaid",
                days_since_observed="0",
                valuation_status="valued",
            ),
        ])
        result = await investments_holdings()
        data = result.to_dict()["data"]
        assert data["total_market_value"] is None
        assert data["market_value_by_currency"] == {"USD": 1200.0, "EUR": 900.0}


class TestInvestmentsLots:
    """Tests for the investments_lots MCP tool."""

    @pytest.mark.unit
    async def test_open_only_default_and_high_sensitivity(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_lot(lot_id="lot_open", security_id=sec, is_open=True)
        _insert_lot(
            lot_id="lot_closed",
            security_id=sec,
            is_open=False,
            remaining_quantity=Decimal("0"),
            cost_basis_remaining=Decimal("0"),
        )
        result = await investments_lots()
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "high"
        assert [r["lot_id"] for r in parsed["data"]["rows"]] == ["lot_open"]

    @pytest.mark.unit
    async def test_open_only_false_returns_all(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_lot(lot_id="lot_open", security_id=sec, is_open=True)
        _insert_lot(
            lot_id="lot_closed",
            security_id=sec,
            is_open=False,
            remaining_quantity=Decimal("0"),
            cost_basis_remaining=Decimal("0"),
        )
        result = await investments_lots(open_only=False)
        rows = result.to_dict()["data"]["rows"]
        assert {r["lot_id"] for r in rows} == {"lot_open", "lot_closed"}

    @pytest.mark.unit
    async def test_no_warning_when_all_lots_complete(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_lot(lot_id="lot_1", security_id=sec, basis_incomplete=False)
        result = await investments_lots()
        parsed = result.to_dict()
        assert parsed["data"]["warnings"] == []

    @pytest.mark.unit
    async def test_basis_incomplete_field_and_warning_present(
        self, mcp_db: Path
    ) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_lot(lot_id="lot_complete", security_id=sec, basis_incomplete=False)
        _insert_lot(lot_id="lot_incomplete", security_id=sec, basis_incomplete=True)
        result = await investments_lots()
        parsed = result.to_dict()
        by_id = {r["lot_id"]: r for r in parsed["data"]["rows"]}
        assert by_id["lot_complete"]["basis_incomplete"] is False
        assert by_id["lot_incomplete"]["basis_incomplete"] is True
        assert parsed["data"]["warnings"]


class TestInvestmentsGains:
    """Tests for the investments_gains MCP tool."""

    @pytest.mark.unit
    async def test_no_warning_when_all_rows_complete(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_gain(realized_gain_id="gain_1", security_id=sec, basis_incomplete=False)
        result = await investments_gains()
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "high"
        assert parsed["data"]["warnings"] == []

    @pytest.mark.unit
    async def test_basis_incomplete_warning_present(self, mcp_db: Path) -> None:
        _seed_investment_core()
        sec = _add_security()
        _insert_gain(
            realized_gain_id="gain_complete", security_id=sec, basis_incomplete=False
        )
        _insert_gain(
            realized_gain_id="gain_incomplete", security_id=sec, basis_incomplete=True
        )
        result = await investments_gains()
        warnings = result.to_dict()["data"]["warnings"]
        assert len(warnings) == 1
        assert "1" in warnings[0]
        assert "incomplete" in warnings[0]

    @pytest.mark.unit
    async def test_invalid_term_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        _seed_investment_core()
        result = await investments_gains(term="medium")
        assert result.to_dict()["status"] == "error"


class TestInvestmentsSecurities:
    """Tests for the investments_securities MCP tool."""

    @pytest.mark.unit
    async def test_low_sensitivity_reference_data_only(self, mcp_db: Path) -> None:
        """No BALANCE/TXN_AMOUNT fields on the catalog -> derived 'low'."""
        _seed_investment_core()
        _add_security(name="Apple Inc.", ticker="AAPL", security_type="equity")
        result = await investments_securities()
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "low"
        assert len(parsed["data"]["rows"]) == 1

    @pytest.mark.unit
    async def test_type_filter_narrows_results(self, mcp_db: Path) -> None:
        _seed_investment_core()
        _add_security(
            security_id="sec_eq",
            name="Apple Inc.",
            ticker="AAPL",
            security_type="equity",
        )
        _add_security(
            security_id="sec_fund",
            name="Vanguard Total",
            ticker="VTSAX",
            security_type="mutual_fund",
        )
        result = await investments_securities(security_type="mutual_fund")
        rows = result.to_dict()["data"]["rows"]
        assert [r["ticker"] for r in rows] == ["VTSAX"]


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------


class TestInvestmentsRecord:
    """Tests for the investments_record MCP tool (batch event recording)."""

    @pytest.mark.unit
    async def test_records_single_buy_event(self, mcp_db: Path) -> None:
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-15",
                    "quantity": "10",
                    "price": "150.00",
                    "amount": "-1504.95",
                    "fees": "4.95",
                }
            ]
        )
        parsed = result.to_dict()
        assert parsed["status"] == "ok"
        # InvestmentRecordPayload carries only RECORD_ID + AGGREGATE fields -> LOW.
        assert parsed["summary"]["sensitivity"] == "low"
        ids = parsed["data"]["investment_transaction_ids"]
        assert len(ids) == 1
        assert parsed["data"]["error_details"] == []

    @pytest.mark.unit
    async def test_reinvest_expands_to_two_rows(self, mcp_db: Path) -> None:
        _seed_investment_core()
        _add_security(security_id="sec_1", name="Vanguard", ticker="VTSAX")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "VTSAX",
                    "type": "reinvest",
                    "date": "2024-03-20",
                    "quantity": "1.5",
                    "price": "100.00",
                    "amount": "-150.00",
                }
            ]
        )
        parsed = result.to_dict()
        assert len(parsed["data"]["investment_transaction_ids"]) == 2

    @pytest.mark.unit
    async def test_unresolved_security_is_soft_others_still_written(
        self, mcp_db: Path
    ) -> None:
        """Event 2's unknown security is a SOFT skip; events 1 & 3 are still written."""
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-15",
                    "quantity": "10",
                    "price": "150.00",
                    "amount": "-1500.00",
                },
                {
                    "account": _ACCOUNT,
                    "security": "NOPE-DOES-NOT-EXIST",
                    "type": "buy",
                    "date": "2024-01-16",
                    "quantity": "1",
                    "price": "1.00",
                    "amount": "-1.00",
                },
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-17",
                    "quantity": "2",
                    "price": "150.00",
                    "amount": "-300.00",
                },
            ]
        )
        parsed = result.to_dict()
        assert parsed["status"] == "ok"
        # Events 1 & 3 written (both single-row buys); event 2 skipped.
        assert len(parsed["data"]["investment_transaction_ids"]) == 2
        assert _count_raw_investment_rows() == 2
        assert len(parsed["data"]["error_details"]) == 1
        assert parsed["data"]["error_details"][0]["index"] == "1"

    @pytest.mark.unit
    async def test_hard_error_mid_batch_writes_nothing(self, mcp_db: Path) -> None:
        """A sign violation on event 2 aborts the whole call with NOTHING written."""
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-15",
                    "quantity": "10",
                    "price": "150.00",
                    "amount": "-1500.00",
                },
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-16",
                    "quantity": "5",
                    "price": "150.00",
                    "amount": "750.00",  # wrong sign for a buy — HARD failure
                },
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-17",
                    "quantity": "2",
                    "price": "150.00",
                    "amount": "-300.00",
                },
            ]
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        # Pre-pass caught event 2 before any write: event 1 must NOT have committed.
        assert _count_raw_investment_rows() == 0

    @pytest.mark.unit
    async def test_unknown_account_mid_batch_writes_nothing(self, mcp_db: Path) -> None:
        """An unresolved ACCOUNT on event 2 is HARD: whole call aborts, nothing written."""
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-15",
                    "quantity": "10",
                    "price": "150.00",
                    "amount": "-1500.00",
                },
                {
                    "account": "no-such-account",  # HARD: AccountNotFoundError
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-16",
                    "quantity": "5",
                    "price": "150.00",
                    "amount": "-750.00",
                },
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-17",
                    "quantity": "2",
                    "price": "150.00",
                    "amount": "-300.00",
                },
            ]
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert _count_raw_investment_rows() == 0

    @pytest.mark.unit
    async def test_sign_violation_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-15",
                    "quantity": "10",
                    "price": "150.00",
                    "amount": "1504.95",  # wrong sign for a buy
                }
            ]
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"

    @pytest.mark.unit
    async def test_missing_required_field_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        _seed_investment_core()
        result = await investments_record(events=[{"account": _ACCOUNT}])
        parsed = result.to_dict()
        assert parsed["status"] == "error"

    @pytest.mark.unit
    async def test_empty_events_returns_empty_payload(self, mcp_db: Path) -> None:
        result = await investments_record(events=[])
        parsed = result.to_dict()
        assert parsed["status"] == "ok"
        assert parsed["data"]["investment_transaction_ids"] == []

    @pytest.mark.unit
    async def test_infra_failure_mid_write_rolls_back_whole_batch(
        self, mcp_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Atomicity: an infra error AFTER the validation pre-pass, part-way
        # through the write, must roll the WHOLE batch back — otherwise the
        # tool's "nothing written / safe to retry" contract is violated and a
        # retry double-inserts the events that committed before the failure.
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        import moneybin.services.investment_service as svc_mod

        # Patch the per-row gold-key fn to raise mid-batch — the injection point
        # for a simulated infra failure inside the write transaction.
        real = svc_mod._predict_investment_gold_key  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        calls = {"n": 0}

        def _boom(source_transaction_id: str, account_id: str) -> str:
            calls["n"] += 1
            if calls["n"] == 2:  # fail on the second row's insert
                raise RuntimeError("simulated infra failure mid-batch")
            return real(source_transaction_id, account_id)

        monkeypatch.setattr(svc_mod, "_predict_investment_gold_key", _boom)

        def _buy(day: str, amount: str) -> dict[str, object]:
            return {
                "account": _ACCOUNT,
                "security": "AAPL",
                "type": "buy",
                "date": day,
                "quantity": "10",
                "price": "150.00",
                "amount": amount,
            }

        try:
            result = await investments_record(
                events=[_buy("2024-01-15", "-1500.00"), _buy("2024-01-16", "-1600.00")]
            )
            assert result.to_dict()["status"] == "error"
        except RuntimeError:
            pass
        # The first event must NOT have committed on its own.
        assert _count_raw_investment_rows() == 0

    @pytest.mark.unit
    async def test_record_actions_hint_at_refresh_run(self, mcp_db: Path) -> None:
        # record_event writes only raw.*; the returned read-tool hints go stale
        # until refresh_run materializes core.* — the actions[] must say so,
        # matching the sibling transactions_create tool.
        _seed_investment_core()
        _add_security(security_id="sec_1", ticker="AAPL")
        result = await investments_record(
            events=[
                {
                    "account": _ACCOUNT,
                    "security": "AAPL",
                    "type": "buy",
                    "date": "2024-01-15",
                    "quantity": "10",
                    "price": "150.00",
                    "amount": "-1500.00",
                }
            ]
        )
        actions = result.to_dict()["actions"]
        assert any("refresh_run" in a for a in actions)


class TestInvestmentsSecuritiesSet:
    """Tests for the investments_securities_set MCP tool (Shape 1b upsert)."""

    @pytest.mark.unit
    async def test_create_new_security(self, mcp_db: Path) -> None:
        result = await investments_securities_set(
            name="Apple Inc.", security_type="equity", ticker="AAPL"
        )
        parsed = result.to_dict()
        assert parsed["status"] == "ok"
        assert parsed["summary"]["sensitivity"] == "low"
        sid = parsed["data"]["security_id"]
        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT name, ticker FROM app.securities WHERE security_id = ?",
                [sid],
            ).fetchone()
        assert row == ("Apple Inc.", "AAPL")

    @pytest.mark.unit
    async def test_create_missing_required_fields_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        result = await investments_securities_set(name="Apple Inc.")
        assert result.to_dict()["status"] == "error"

    @pytest.mark.unit
    async def test_invalid_cost_basis_method_returns_clean_error_envelope(
        self, mcp_db: Path
    ) -> None:
        # Must surface as a clean UserError envelope, not a raw
        # duckdb.ConstraintException — the point of the hard-validation fix.
        result = await investments_securities_set(
            name="Apple Inc.", security_type="equity", cost_basis_method="lifo"
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert "lifo" in parsed["error"]["message"]

    @pytest.mark.unit
    async def test_invalid_security_type_returns_clean_error_envelope(
        self, mcp_db: Path
    ) -> None:
        result = await investments_securities_set(
            name="Apple Inc.", security_type="stock"
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert "stock" in parsed["error"]["message"]

    @pytest.mark.unit
    async def test_update_existing_security_preserves_unset_fields(
        self, mcp_db: Path
    ) -> None:
        sid = _add_security(
            security_id="sec_vt",
            name="Vanguard Total Stock Market",
            security_type="mutual_fund",
            ticker="VTSAX",
            cost_basis_method="fifo",
        )
        result = await investments_securities_set(
            security_id=sid, cost_basis_method="average"
        )
        parsed = result.to_dict()
        assert parsed["data"]["security_id"] == sid
        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT name, ticker, cost_basis_method FROM app.securities "
                "WHERE security_id = ?",
                [sid],
            ).fetchone()
        assert row == ("Vanguard Total Stock Market", "VTSAX", "average")

    @pytest.mark.unit
    async def test_average_on_equity_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        sid = _add_security(security_id="sec_eq", security_type="equity")
        result = await investments_securities_set(
            security_id=sid, cost_basis_method="average"
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"

    @pytest.mark.unit
    async def test_update_unknown_security_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        result = await investments_securities_set(
            security_id="does-not-exist", name="X"
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_not_found"

    @pytest.mark.unit
    async def test_update_rejects_security_type_change(self, mcp_db: Path) -> None:
        # security_type is immutable post-creation (docstring says so); the
        # update path must reject an attempt to change it, not silently drop
        # it while returning an "ok" envelope.
        sid = _add_security(
            security_id="sec_eq", name="Apple Inc.", security_type="equity"
        )
        result = await investments_securities_set(security_id=sid, security_type="bond")
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT security_type FROM app.securities WHERE security_id = ?",
                [sid],
            ).fetchone()
        assert row == ("equity",)


class TestInvestmentsLotsSelect:
    """Tests for the investments_lots_select MCP tool (Shape 1a state-set)."""

    def _seed_disposal_and_lots(self) -> None:
        _seed_investment_core()
        sec = _add_security(security_id="sec_1", ticker="AAPL")
        with get_database(read_only=False) as db:
            db.execute(
                """
                INSERT INTO core.fct_investment_transactions
                    (investment_transaction_id, account_id, security_id, trade_date,
                     type, quantity)
                VALUES ('sell_1', ?, ?, '2024-06-15', 'sell', -10)
                """,  # noqa: S608  # test fixture insert, static SQL
                [_ACCOUNT, sec],
            )
            db.executemany(
                """
                INSERT INTO core.fct_investment_lots
                    (lot_id, account_id, security_id, acquisition_date,
                     original_quantity, remaining_quantity)
                VALUES (?, ?, ?, '2024-01-10', ?, ?)
                """,  # noqa: S608  # test fixture insert, static SQL
                [
                    ["lot_a", _ACCOUNT, sec, Decimal("6"), Decimal("6")],
                    ["lot_b", _ACCOUNT, sec, Decimal("6"), Decimal("6")],
                ],
            )

    @pytest.mark.unit
    async def test_sets_selection(self, mcp_db: Path) -> None:
        self._seed_disposal_and_lots()
        result = await investments_lots_select(
            disposal_txn_id="sell_1",
            selections=[
                {"lot_id": "lot_a", "quantity": "6"},
                {"lot_id": "lot_b", "quantity": "4"},
            ],
        )
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "high"
        assert len(parsed["data"]["selections"]) == 2
        with get_database(read_only=True) as db:
            rows = db.execute(
                "SELECT lot_id, quantity FROM app.lot_selections "
                "WHERE investment_transaction_id = 'sell_1' ORDER BY lot_id"
            ).fetchall()
        assert rows == [
            ("lot_a", Decimal("6.0000000000")),
            ("lot_b", Decimal("4.0000000000")),
        ]

    @pytest.mark.unit
    async def test_empty_selections_clears(self, mcp_db: Path) -> None:
        self._seed_disposal_and_lots()
        await investments_lots_select(
            disposal_txn_id="sell_1",
            selections=[{"lot_id": "lot_a", "quantity": "5"}],
        )
        result = await investments_lots_select(disposal_txn_id="sell_1", selections=[])
        parsed = result.to_dict()
        assert parsed["data"]["selections"] == []
        with get_database(read_only=True) as db:
            rows = db.execute(
                "SELECT 1 FROM app.lot_selections "
                "WHERE investment_transaction_id = 'sell_1'"
            ).fetchall()
        assert rows == []

    @pytest.mark.unit
    async def test_unknown_lot_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        self._seed_disposal_and_lots()
        result = await investments_lots_select(
            disposal_txn_id="sell_1",
            selections=[{"lot_id": "lot_ghost", "quantity": "1"}],
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_not_found"

    @pytest.mark.unit
    async def test_malformed_selection_returns_standard_error_envelope(
        self, mcp_db: Path
    ) -> None:
        self._seed_disposal_and_lots()
        result = await investments_lots_select(
            disposal_txn_id="sell_1", selections=[{"lot_id": "lot_a"}]
        )
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "mutation_invalid_input"
