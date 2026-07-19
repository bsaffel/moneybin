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

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.investments import (
    investments_coarse,
    investments_lots_select,
    investments_record,
    investments_securities,
    investments_securities_set,
    register_investment_coarse_reads,
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
            "investments_record",
            "investments_securities_set",
            "investments_lots_select",
        }

    @pytest.mark.unit
    async def test_coarse_registrar_registers_only_replacement(self) -> None:
        srv = FastMCP("test")
        register_investment_coarse_reads(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert names == {"investments"}


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "view",
    ["events", "holdings", "lots", "gains", "securities"],
)
async def test_investment_coarse_views_are_typed(
    view: str,
    mcp_db: Path,
) -> None:
    _seed_investment_core()

    response = await investments_coarse(view=view)  # pyright: ignore[reportArgumentType]

    assert response.data.kind == view


@pytest.mark.parametrize(
    ("view", "arguments", "code"),
    [
        (
            "holdings",
            {"start": date(2024, 1, 1)},
            "INVESTMENT_DATES_NOT_ALLOWED",
        ),
        ("lots", {"end": date(2024, 12, 31)}, "INVESTMENT_DATES_NOT_ALLOWED"),
        ("securities", {"account": _ACCOUNT}, "INVESTMENT_ACCOUNT_NOT_ALLOWED"),
    ],
)
async def test_investment_coarse_rejects_unused_arguments(
    view: str,
    arguments: dict[str, object],
    code: str,
    mcp_db: Path,
) -> None:
    _seed_investment_core()

    response = await investments_coarse(  # pyright: ignore[reportArgumentType]
        view=view,
        **arguments,
    )

    assert response.error is not None
    assert response.error.code == code


async def test_investment_coarse_paginates_with_exact_counts(
    mcp_db: Path,
) -> None:
    _seed_investment_core()
    sec = _add_security()
    _insert_event(investment_transaction_id="evt_1", security_id=sec)
    _insert_event(
        investment_transaction_id="evt_2",
        security_id=sec,
        trade_date=date(2024, 1, 15),
    )

    first = await investments_coarse(
        view="events",
        account=_ACCOUNT,
        security="AAPL",
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
        limit=1,
    )
    second = await investments_coarse(
        view="events",
        account=_ACCOUNT,
        security="AAPL",
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
        limit=1,
        cursor=first.next_cursor,
    )

    assert first.summary.total_count == 2
    assert first.summary.returned_count == 1
    assert first.summary.has_more is True
    assert first.next_cursor is not None
    assert second.summary.total_count == 2
    assert second.summary.returned_count == 1
    assert second.summary.has_more is False
    assert second.next_cursor is None
    assert [
        first.data.rows[0].investment_transaction_id,
        second.data.rows[0].investment_transaction_id,
    ] == ["evt_1", "evt_2"]
    continuation = next(
        action for action in first.actions if action.startswith("Continue with ")
    )
    for argument in (
        "view='events'",
        f"account={_ACCOUNT!r}",
        "security='AAPL'",
        "start='2024-01-01'",
        "end='2024-12-31'",
        "limit=1",
        f"cursor={first.next_cursor!r}",
    ):
        assert argument in continuation


async def test_investment_coarse_cursor_is_bound_to_filters(
    mcp_db: Path,
) -> None:
    _seed_investment_core()
    sec = _add_security()
    _insert_event(investment_transaction_id="evt_1", security_id=sec)
    _insert_event(
        investment_transaction_id="evt_2",
        security_id=sec,
        trade_date=date(2024, 1, 16),
    )
    first = await investments_coarse(view="events", limit=1)

    response = await investments_coarse(
        view="events",
        start=date(2024, 1, 16),
        limit=1,
        cursor=first.next_cursor,
    )

    assert response.error is not None
    assert response.error.code == "INVESTMENT_CURSOR_INVALID"


async def test_investment_coarse_returns_sanitized_ambiguity(
    mcp_db: Path,
) -> None:
    _seed_investment_core()
    _add_security(security_id="sec_a", name="Shared Fund", ticker=None)
    _add_security(security_id="sec_b", name="Shared Fund", ticker=None)

    response = await investments_coarse(
        view="events",
        security="Shared Fund",
    )

    assert response.error is not None
    assert response.error.code == "ENTITY_REFERENCE_AMBIGUOUS"
    assert response.error.details == {"candidate_ids": ["sec_a", "sec_b"]}
    assert "Shared Fund" not in response.error.message


async def test_investment_coarse_binds_resolved_security_id(
    mcp_db: Path,
) -> None:
    _seed_investment_core()
    sec = _add_security(security_id="sec_1", name="Indexed Fund", ticker="IDX")
    _insert_event(investment_transaction_id="evt_1", security_id=sec)

    response = await investments_coarse(view="events", security="Indexed   Fund")

    assert response.error is None
    assert [row.investment_transaction_id for row in response.data.rows] == ["evt_1"]


class TestInvestmentsSecurities:
    """Tests for the investments_securities MCP tool."""

    @pytest.mark.unit
    async def test_low_sensitivity_reference_data_only(self, mcp_db: Path) -> None:
        """No BALANCE/TXN_AMOUNT fields on the catalog -> derived 'low'."""
        _seed_investment_core()
        _add_security(name="Apple Inc.", ticker="AAPL", security_type="equity")
        result = investments_securities()
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
        result = investments_securities(security_type="mutual_fund")
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
