"""Tests for ``InvestmentService`` — the investment write path (Task 14a).

Covers security resolution (Req 3), sign validation (Req 6), reinvest pairing,
split encoding (D6), transfer_in mapping, cost-basis-method election validation
(Req 12), and declarative lot selection with pre-delegation validation (Req 13).

The resolution-chain contract for the single-string ``resolve_security(ref)``
interface (v1 manual-only) is exercised here: CUSIP/ISIN → ticker (exchange
suffix stripped) → name, identifier collisions raise naming the attribute, and
a name match to a candidate that carries a strong identifier is rejected (the
name-contradiction guard adapted to the single-string interface — a
strongly-identified security must be referenced by its identifier).
"""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, NamedTuple

import pytest
from prometheus_client import REGISTRY

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import PRICE_STALENESS_DAYS
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.investment_service import (
    _PIPELINE_EMITTED_SUBTYPES,  # pyright: ignore[reportPrivateUsage]  # tested directly
    _SUBTYPE_VOCAB,  # pyright: ignore[reportPrivateUsage]  # tested directly
    InvestmentService,
    SecurityResolutionError,
)
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _add_account(db: Database, account_id: str = "acct_brokerage") -> str:
    create_core_tables(db)  # idempotent: CREATE IF NOT EXISTS for core.dim_accounts
    db.conn.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, account_type, institution_name, source_type)
        VALUES (?, 'investment', 'Fidelity', 'manual')
        """,  # noqa: S608  # test fixture insert, static SQL
        [account_id],
    )
    return account_id


def _add_security(db: Database, **kwargs: Any) -> str:
    """Insert one security via the real repo; return its (possibly minted) id."""
    defaults: dict[str, Any] = {
        "security_id": None,
        "name": "Test Security",
        "security_type": "equity",
        "actor": "cli",
    }
    defaults.update(kwargs)
    event = SecuritiesRepo(db).upsert(**defaults)
    assert event.target_id is not None
    return event.target_id


def _set_account_default_method(db: Database, method: str) -> None:
    """Elect ``method`` as the account-level cost-basis default via the real repo."""
    AccountSettingsRepo(db).set(
        account_id="acct_brokerage",
        display_name=None,
        official_name=None,
        last_four=None,
        account_subtype=None,
        holder_category=None,
        currency_code=None,
        credit_limit=None,
        archived=False,
        include_in_net_worth=True,
        default_cost_basis_method=method,
        actor="cli",
    )


def _seed_disposal_and_lots(
    db: Database, *, cost_basis_method: str | None = "specific"
) -> None:
    """Materialize + seed the two derived core tables select_lots validates against.

    These are SQLMesh-managed in production; ``create_core_dim_stub_views``
    builds them with the real column shapes for unit tests. ``sell_1`` trades
    on 2024-06-15; both lots are acquired well before that so date-validity
    tests can add a lot acquired *after* it without disturbing these dates.

    ``sec_1`` elects ``specific`` by default: ``select_lots`` refuses a selection
    under any other resolved method. Override to exercise that refusal.
    """
    create_core_dim_stub_views(db)
    _add_security(
        db,
        security_id="sec_1",
        name="Apple Inc.",
        ticker="AAPL",
        cost_basis_method=cost_basis_method,
    )
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
            (lot_id, account_id, security_id, acquisition_date, original_quantity,
             remaining_quantity)
        VALUES (?, 'acct_brokerage', 'sec_1', ?, ?, ?)
        """,  # noqa: S608  # test fixture insert, static SQL
        [
            ["lot_a", date(2024, 1, 10), Decimal("6"), Decimal("6")],
            ["lot_b", date(2024, 3, 20), Decimal("6"), Decimal("6")],
        ],
    )


def _metric(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _events_metric(type_: str) -> float:
    return _metric("moneybin_investment_events_recorded_total", {"type": type_})


def _resolution_metric(rung: str) -> float:
    return _metric("moneybin_security_resolution_outcomes_total", {"rung": rung})


def _raw_rows(db: Database, account_id: str = "acct_brokerage") -> list[Any]:
    return db.conn.execute(
        """
        SELECT investment_transaction_id, security_id, type, subtype,
               event_group_id, quantity, price, amount, fees,
               original_acquisition_date, created_by
          FROM raw.manual_investment_transactions
         WHERE account_id = ?
         ORDER BY created_at, source_transaction_id
        """,  # noqa: S608  # test read, static SQL
        [account_id],
    ).fetchall()


# ---------------------------------------------------------------------------
# Closed ledger vocabulary (user-authorable ∪ pipeline-emitted)
# ---------------------------------------------------------------------------


class TestSubtypeVocabulary:
    """Pins the full ledger-wide subtype vocabulary at the core boundary.

    _SUBTYPE_VOCAB gates USER-AUTHORABLE subtypes (record_event's
    _validate_event); the sync pipeline (e.g. prep.stg_plaid__opening_lots)
    writes subtype='opening_bootstrap' straight into core, bypassing that
    validator entirely -- a strict superset tracked separately in
    _PIPELINE_EMITTED_SUBTYPES. A new value on either side must edit this test
    deliberately, so a subtype can never slip into
    core.fct_investment_transactions unnoticed by both surfaces' closed-vocabulary
    contracts.
    """

    def test_ledger_subtype_vocabulary_is_closed(self) -> None:
        combined = {
            type_: _SUBTYPE_VOCAB.get(type_, frozenset())
            | _PIPELINE_EMITTED_SUBTYPES.get(type_, frozenset())
            for type_ in set(_SUBTYPE_VOCAB) | set(_PIPELINE_EMITTED_SUBTYPES)
        }
        assert combined == {
            "dividend": frozenset({"qualified", "non_qualified"}),
            "capital_gain_distribution": frozenset({"short_term", "long_term"}),
            "fee": frozenset({"tax_withheld"}),
            "reinvest": frozenset({"dividend", "interest", "capital_gain"}),
            "transfer_in": frozenset({"opening_bootstrap"}),
        }

    def test_pipeline_emitted_subtypes_are_not_user_authorable(self) -> None:
        """opening_bootstrap must stay impossible to hand-author (Fix 5)."""
        for type_, subtypes in _PIPELINE_EMITTED_SUBTYPES.items():
            assert not subtypes & _SUBTYPE_VOCAB.get(type_, frozenset())


# ---------------------------------------------------------------------------
# resolve_security (Req 3)
# ---------------------------------------------------------------------------


class TestResolveSecurity:
    """Tests for InvestmentService.resolve_security() — the Req 3 chain."""

    def test_cusip_exact_beats_ticker_and_name(self, db: Database) -> None:
        # A distractor sharing the ticker but not the cusip must not win.
        _add_security(db, name="Distractor", ticker="AAPL", exchange="NYSE")
        target = _add_security(
            db, name="Apple Inc.", ticker="AAPL", exchange="NASDAQ", cusip="037833100"
        )
        before = _resolution_metric("cusip")
        assert db_service(db).resolve_security("037833100") == target
        assert _resolution_metric("cusip") - before == 1.0

    def test_isin_exact_resolves(self, db: Database) -> None:
        target = _add_security(db, name="Apple Inc.", isin="US0378331005")
        before = _resolution_metric("isin")
        assert db_service(db).resolve_security("US0378331005") == target
        assert _resolution_metric("isin") - before == 1.0

    def test_ticker_strips_exchange_suffix(self, db: Database) -> None:
        target = _add_security(db, name="Betashares UMAX", ticker="UMAX", exchange="AX")
        before = _resolution_metric("ticker")
        assert db_service(db).resolve_security("UMAX.AX") == target
        assert _resolution_metric("ticker") - before == 1.0

    def test_ticker_suffix_disambiguates_duplicate_tickers(self, db: Database) -> None:
        ax = _add_security(db, name="UMAX AU", ticker="UMAX", exchange="AX")
        _add_security(db, name="UMAX NZ", ticker="UMAX", exchange="NZ")
        assert db_service(db).resolve_security("UMAX.AX") == ax

    def test_bare_ticker_collision_raises_naming_ticker(self, db: Database) -> None:
        _add_security(db, name="UMAX AU", ticker="UMAX", exchange="AX")
        _add_security(db, name="UMAX NZ", ticker="UMAX", exchange="NZ")
        before = _resolution_metric("ambiguous")
        with pytest.raises(SecurityResolutionError, match="ticker"):
            db_service(db).resolve_security("UMAX")
        assert _resolution_metric("ambiguous") - before == 1.0

    def test_cusip_collision_raises_naming_cusip(self, db: Database) -> None:
        _add_security(db, name="Dup One", cusip="037833100")
        _add_security(db, name="Dup Two", cusip="037833100")
        with pytest.raises(SecurityResolutionError, match="cusip"):
            db_service(db).resolve_security("037833100")

    def test_name_match_when_no_strong_identifier(self, db: Database) -> None:
        target = _add_security(db, name="My Private Fund", security_type="other")
        before = _resolution_metric("name")
        assert db_service(db).resolve_security("my private fund") == target
        assert _resolution_metric("name") - before == 1.0

    def test_name_contradiction_guard_rejects_strongly_identified(
        self, db: Database
    ) -> None:
        # "Apple Inc." carries a ticker; a loose name match must be rejected —
        # the user must reference it by its identifier.
        _add_security(db, name="Apple Inc.", ticker="AAPL")
        before = _resolution_metric("unresolved")
        with pytest.raises(SecurityResolutionError, match="ticker|identifier"):
            db_service(db).resolve_security("Apple Inc.")
        assert _resolution_metric("unresolved") - before == 1.0

    def test_unknown_ref_raises(self, db: Database) -> None:
        before = _resolution_metric("unresolved")
        with pytest.raises(SecurityResolutionError):
            db_service(db).resolve_security("nothing-matches-this")
        assert _resolution_metric("unresolved") - before == 1.0

    def test_dotted_ticker_resolves_by_full_ticker(self, db: Database) -> None:
        # A ticker that legitimately contains a dot (BRK.B, BF.B, RDS.A) must
        # resolve by its own stored ticker, not be mis-split into base='BRK' +
        # exchange='B' (which never matches).
        target = _add_security(db, name="Berkshire Hathaway B", ticker="BRK.B")
        before = _resolution_metric("ticker")
        assert db_service(db).resolve_security("BRK.B") == target
        assert _resolution_metric("ticker") - before == 1.0

    def test_full_ticker_match_precedes_exchange_suffix_split(
        self, db: Database
    ) -> None:
        # When both a full dotted ticker AND a base+exchange interpretation could
        # match, the exact full-ticker match wins.
        full = _add_security(db, name="Dotted", ticker="ABC.D")
        _add_security(db, name="Base On Exchange", ticker="ABC", exchange="D")
        assert db_service(db).resolve_security("ABC.D") == full


# ---------------------------------------------------------------------------
# upsert_security (Req 12)
# ---------------------------------------------------------------------------


class TestUpsertSecurity:
    """Tests for InvestmentService.upsert_security() — Req 12 method election."""

    def test_average_rejected_on_equity(self, db: Database) -> None:
        with pytest.raises(UserError, match="average"):
            db_service(db).upsert_security(
                security_id=None,
                name="Apple Inc.",
                security_type="equity",
                cost_basis_method="average",
                actor="cli",
            )

    @pytest.mark.parametrize("sec_type", ["etf", "mutual_fund"])
    def test_average_accepted_on_fund_types(self, db: Database, sec_type: str) -> None:
        sid = db_service(db).upsert_security(
            security_id=None,
            name="Vanguard Total",
            security_type=sec_type,
            cost_basis_method="average",
            actor="cli",
        )
        assert len(sid) == 12  # minted id recovered from AuditEvent.target_id

    def test_fifo_unrestricted_on_equity(self, db: Database) -> None:
        sid = db_service(db).upsert_security(
            security_id=None,
            name="Apple Inc.",
            security_type="equity",
            cost_basis_method="fifo",
            actor="cli",
        )
        row = db.conn.execute(
            "SELECT cost_basis_method FROM app.securities WHERE security_id = ?",
            [sid],
        ).fetchone()
        assert row == ("fifo",)

    def test_update_by_supplied_id_round_trips(self, db: Database) -> None:
        svc = db_service(db)
        sid = svc.upsert_security(
            security_id="sec_fixed",
            name="Old",
            security_type="equity",
            actor="cli",
        )
        assert sid == "sec_fixed"
        svc.upsert_security(
            security_id="sec_fixed",
            name="New",
            security_type="equity",
            actor="cli",
        )
        row = db.conn.execute(
            "SELECT name FROM app.securities WHERE security_id = ?", ["sec_fixed"]
        ).fetchone()
        assert row == ("New",)

    def test_invalid_cost_basis_method_raises_user_error(self, db: Database) -> None:
        # Mirrors AccountService.settings_update's hard-validation of the same
        # closed vocabulary: the DB CHECK constraint is the backstop, not the
        # primary contract — an invalid value must raise UserError, not a raw
        # duckdb.ConstraintException.
        with pytest.raises(UserError, match="[Ll]ifo"):
            db_service(db).upsert_security(
                security_id=None,
                name="Apple Inc.",
                security_type="equity",
                cost_basis_method="lifo",
                actor="cli",
            )

    def test_invalid_security_type_raises_user_error(self, db: Database) -> None:
        with pytest.raises(UserError, match="stock"):
            db_service(db).upsert_security(
                security_id=None,
                name="Apple Inc.",
                security_type="stock",
                actor="cli",
            )


# ---------------------------------------------------------------------------
# set_security — partial-update merge (read-modify-write)
# ---------------------------------------------------------------------------


class TestSetSecurity:
    """Tests for InvestmentService.set_security() — partial-update merge.

    The Task-16 MCP seam: ``SecuritiesRepo.upsert`` always writes the full
    row, so ``set_security`` must fetch → merge non-None overrides → delegate,
    without nulling untouched columns (esp. ``cost_basis_method``, which the
    ``core.dim_securities`` read-projection omits).
    """

    def _seed(self, db: Database, **overrides: Any) -> str:
        """Create one fully-populated security; return its id."""
        defaults: dict[str, Any] = {
            "security_id": "sec_vt",
            "name": "Vanguard Total Stock Market",
            "security_type": "mutual_fund",
            "ticker": "VTSAX",
            "exchange": "NASDAQ",
            "cusip": "922908728",
            "cost_basis_method": "fifo",
            "actor": "cli",
        }
        defaults.update(overrides)
        return db_service(db).upsert_security(**defaults)

    def test_set_method_only_preserves_all_other_fields(self, db: Database) -> None:
        sid = self._seed(db)
        db_service(db).set_security(sid, cost_basis_method="average", actor="cli")
        row = db.conn.execute(
            """
            SELECT name, ticker, exchange, cusip, cost_basis_method
              FROM app.securities WHERE security_id = ?
            """,  # noqa: S608  # test read, static SQL
            [sid],
        ).fetchone()
        assert row == (
            "Vanguard Total Stock Market",
            "VTSAX",
            "NASDAQ",
            "922908728",
            "average",
        )

    def test_set_name_only_preserves_cost_basis_method(self, db: Database) -> None:
        # The core.dim_securities projection omits cost_basis_method; a merge
        # sourced from the view (not app.securities) would null it here.
        sid = self._seed(db, cost_basis_method="hifo")
        db_service(db).set_security(sid, name="Renamed Fund", actor="cli")
        row = db.conn.execute(
            "SELECT name, cost_basis_method FROM app.securities WHERE security_id = ?",
            [sid],
        ).fetchone()
        assert row == ("Renamed Fund", "hifo")

    def test_set_ticker_preserves_name_and_type(self, db: Database) -> None:
        sid = self._seed(db)
        db_service(db).set_security(sid, ticker="VTI", actor="cli")
        row = db.conn.execute(
            "SELECT name, security_type, ticker FROM app.securities "
            "WHERE security_id = ?",
            [sid],
        ).fetchone()
        assert row == ("Vanguard Total Stock Market", "mutual_fund", "VTI")

    def test_set_unknown_security_raises_not_found(self, db: Database) -> None:
        with pytest.raises(UserError, match="not found"):
            db_service(db).set_security("does-not-exist", name="X", actor="cli")

    def test_set_average_on_non_fund_type_still_validated(self, db: Database) -> None:
        # security_type carries through unchanged; the average/fund guard in
        # upsert_security still fires on the merged row.
        sid = self._seed(db, security_type="equity", cost_basis_method="fifo")
        with pytest.raises(UserError, match="average"):
            db_service(db).set_security(sid, cost_basis_method="average", actor="cli")


# ---------------------------------------------------------------------------
# list_securities — catalog read projection
# ---------------------------------------------------------------------------


class TestListSecurities:
    """Tests for InvestmentService.list_securities() — the catalog read."""

    def test_returns_all_catalog_rows_ordered_by_name(self, db: Database) -> None:
        create_core_dim_stub_views(db)  # dim_securities passthrough of app.securities
        _add_security(db, name="Zebra Corp", ticker="ZBRA", security_type="equity")
        _add_security(db, name="Apple Inc.", ticker="AAPL", security_type="equity")
        result = db_service(db).list_securities()
        assert [r.name for r in result.rows] == ["Apple Inc.", "Zebra Corp"]
        assert result.warnings == []

    def test_type_filter_narrows_results(self, db: Database) -> None:
        create_core_dim_stub_views(db)
        _add_security(db, name="Apple Inc.", ticker="AAPL", security_type="equity")
        _add_security(
            db, name="Vanguard Total", ticker="VTSAX", security_type="mutual_fund"
        )
        result = db_service(db).list_securities(security_type="mutual_fund")
        assert [r.ticker for r in result.rows] == ["VTSAX"]

    def test_empty_catalog_returns_no_rows(self, db: Database) -> None:
        create_core_dim_stub_views(db)
        result = db_service(db).list_securities()
        assert result.rows == []

    def test_invalid_security_type_filter_raises(self, db: Database) -> None:
        # Matches the sibling type_filter/term validation pattern in
        # list_events/gains — a typo'd filter must raise, not silently
        # return zero rows.
        create_core_dim_stub_views(db)
        with pytest.raises(ValueError, match="security_type"):
            db_service(db).list_securities(security_type="stock")


# ---------------------------------------------------------------------------
# record_event — sign validation (Req 6)
# ---------------------------------------------------------------------------


class TestRecordEventSigns:
    """Tests for record_event sign/taxonomy/presence validation (Req 5/6)."""

    def _svc(self, db: Database) -> InvestmentService:
        _add_account(db)
        _add_security(db, security_id="sec_1", name="Apple Inc.", ticker="AAPL")
        return db_service(db)

    def test_buy_writes_positive_qty_negative_amount(self, db: Database) -> None:
        svc = self._svc(db)
        before = _events_metric("buy")
        ids = svc.record_event(
            account_ref="acct_brokerage",
            security_ref="AAPL",
            type_="buy",
            subtype=None,
            trade_date=date(2024, 1, 15),
            quantity=Decimal("10"),
            price=Decimal("150.00"),
            amount=Decimal("-1504.95"),
            fees=Decimal("4.95"),
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description="buy aapl",
            actor="cli",
            created_by="cli",
        )
        assert len(ids) == 1
        rows = _raw_rows(db)
        assert len(rows) == 1
        (txn_id, sec_id, type_, _sub, _grp, qty, _price, amount, _fees, _oad, cb) = (
            rows[0]
        )
        assert txn_id == ids[0]
        assert len(txn_id) == 16  # content-hash gold key
        assert sec_id == "sec_1"
        assert type_ == "buy"
        assert qty == Decimal("10.0000000000")
        assert amount == Decimal("-1504.95")
        assert cb == "cli"
        assert _events_metric("buy") - before == 1.0

    def test_buy_positive_amount_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="buy",
                subtype=None,
                trade_date=date(2024, 1, 15),
                quantity=Decimal("10"),
                price=Decimal("150.00"),
                amount=Decimal("1504.95"),  # wrong sign for a buy
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )
        assert _raw_rows(db) == []

    def test_buy_null_amount_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="amount"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="buy",
                subtype=None,
                trade_date=date(2024, 1, 15),
                quantity=Decimal("10"),
                price=Decimal("150.00"),
                amount=None,  # null amount degrades the engine — reject
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    def test_buy_negative_quantity_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="quantity"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="buy",
                subtype=None,
                trade_date=date(2024, 1, 15),
                quantity=Decimal("-10"),
                price=Decimal("150.00"),
                amount=Decimal("-1500"),
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    def test_sell_writes_negative_qty_positive_amount(self, db: Database) -> None:
        svc = self._svc(db)
        ids = svc.record_event(
            account_ref="acct_brokerage",
            security_ref="AAPL",
            type_="sell",
            subtype=None,
            trade_date=date(2024, 6, 12),
            quantity=Decimal("-5"),
            price=Decimal("190.00"),
            amount=Decimal("945.05"),
            fees=Decimal("4.95"),
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description=None,
            actor="cli",
            created_by="cli",
        )
        assert len(ids) == 1
        row = _raw_rows(db)[0]
        assert row[2] == "sell"
        assert row[5] == Decimal("-5.0000000000")
        assert row[7] == Decimal("945.05")

    def test_sell_null_amount_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="amount"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="sell",
                subtype=None,
                trade_date=date(2024, 6, 12),
                quantity=Decimal("-5"),
                price=Decimal("190.00"),
                amount=None,
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    def test_deposit_with_security_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="security"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",  # deposit is external cash — no security
                type_="deposit",
                subtype=None,
                trade_date=date(2024, 1, 1),
                quantity=None,
                price=None,
                amount=Decimal("1000"),
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    def test_deposit_cash_only_writes_null_security_and_quantity(
        self, db: Database
    ) -> None:
        _add_account(db)
        ids = db_service(db).record_event(
            account_ref="acct_brokerage",
            security_ref=None,
            type_="deposit",
            subtype=None,
            trade_date=date(2024, 1, 1),
            quantity=None,
            price=None,
            amount=Decimal("1000"),
            fees=None,
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description="fund the account",
            actor="cli",
            created_by="cli",
        )
        assert len(ids) == 1
        row = _raw_rows(db)[0]
        assert row[1] is None  # security_id
        assert row[5] is None  # quantity

    def test_unknown_type_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="type"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="frobnicate",
                subtype=None,
                trade_date=date(2024, 1, 1),
                quantity=Decimal("1"),
                price=None,
                amount=Decimal("-1"),
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    def test_invalid_subtype_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="subtype"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="dividend",
                subtype="not_a_dividend_subtype",
                trade_date=date(2024, 1, 1),
                quantity=None,
                price=None,
                amount=Decimal("50"),
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    @pytest.mark.parametrize(
        ("type_", "security_ref", "quantity", "amount", "should_raise"),
        [
            # transfer_out: security required, quantity negative, amount unchecked.
            ("transfer_out", "AAPL", Decimal("-5"), None, False),
            ("transfer_out", "AAPL", Decimal("5"), None, True),  # wrong qty sign
            # withdrawal: external cash out — no security, null qty, negative amount.
            ("withdrawal", None, None, Decimal("-100"), False),
            ("withdrawal", None, None, Decimal("100"), True),  # wrong amount sign
            # interest: cash in — security optional, null qty, positive amount.
            ("interest", None, None, Decimal("10"), False),
            ("interest", None, None, Decimal("-10"), True),  # wrong amount sign
            # return_of_capital: basis-reduction cash in — security required.
            ("return_of_capital", "AAPL", None, Decimal("50"), False),
            ("return_of_capital", "AAPL", None, Decimal("-50"), True),  # wrong sign
        ],
    )
    def test_sign_rules_sweep(
        self,
        db: Database,
        type_: str,
        security_ref: str | None,
        quantity: Decimal | None,
        amount: Decimal | None,
        should_raise: bool,
    ) -> None:
        """Quantity/amount sign rules for types the buy/sell paths don't cover."""
        svc = self._svc(db)

        def _call() -> list[str]:
            return svc.record_event(
                account_ref="acct_brokerage",
                security_ref=security_ref,
                type_=type_,
                subtype=None,
                trade_date=date(2024, 1, 1),
                quantity=quantity,
                price=None,
                amount=amount,
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

        if should_raise:
            with pytest.raises(UserError):
                _call()
            assert _raw_rows(db) == []
        else:
            ids = _call()
            assert len(ids) == 1
            assert _raw_rows(db)[0][2] == type_

    def test_invalid_created_by_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="created_by"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="buy",
                subtype=None,
                trade_date=date(2024, 1, 1),
                quantity=Decimal("1"),
                price=None,
                amount=Decimal("-1"),
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="api",  # only cli / mcp
            )


# ---------------------------------------------------------------------------
# record_event / record_events — import-log finalization
# ---------------------------------------------------------------------------


def _manual_investment_import_rows(db: Database) -> list[Any]:
    return db.conn.execute(
        """
        SELECT status, rows_total, rows_imported, completed_at
          FROM raw.import_log
         WHERE format_name = 'manual_investment_entry'
        """  # noqa: S608  # test read, static SQL
    ).fetchall()


class TestManualInvestmentImportFinalization:
    """The success path must close the batch it opened.

    Both write paths allocate the batch as ``importing`` and call
    ``finalize_import`` only from their ``except`` branch, so a *successful*
    record leaves a permanently non-terminal batch — ``import_status`` reports
    ``rows_imported: null`` and ``completed_at`` never gets a value.
    """

    def _svc(self, db: Database) -> InvestmentService:
        _add_account(db)
        _add_security(db, security_id="sec_1", name="Apple Inc.", ticker="AAPL")
        return db_service(db)

    def _buy(self, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "account_ref": "acct_brokerage",
            "security_ref": "AAPL",
            "type_": "buy",
            "subtype": None,
            "trade_date": date(2024, 1, 15),
            "quantity": Decimal("10"),
            "price": Decimal("150.00"),
            "amount": Decimal("-1500.00"),
            "fees": None,
            "acquired": None,
            "basis": None,
            "event_group_id": None,
            "currency_code": "USD",
            "description": "buy aapl",
        }
        base.update(overrides)
        return base

    def test_record_event_finalizes_the_batch_on_the_success_path(
        self, db: Database
    ) -> None:
        svc = self._svc(db)
        ids = svc.record_event(**self._buy(), actor="cli", created_by="cli")
        assert len(ids) == 1

        rows = _manual_investment_import_rows(db)
        assert len(rows) == 1
        status, rows_total, rows_imported, completed_at = rows[0]
        assert status == "complete"
        assert rows_total == 1
        assert rows_imported == 1
        assert completed_at is not None

    def test_record_events_finalizes_the_batch_on_the_success_path(
        self, db: Database
    ) -> None:
        svc = self._svc(db)
        result = svc.record_events(
            [self._buy(), self._buy(quantity=Decimal("5"), amount=Decimal("-750.00"))],
            actor="cli",
            created_by="cli",
        )
        assert len(result.investment_transaction_ids) == 2

        rows = _manual_investment_import_rows(db)
        assert len(rows) == 1
        status, rows_total, rows_imported, completed_at = rows[0]
        assert status == "complete"
        assert rows_total == 2
        assert rows_imported == 2
        assert completed_at is not None


# ---------------------------------------------------------------------------
# record_event — reinvest pairing (Req 6)
# ---------------------------------------------------------------------------


class TestReinvestPairing:
    """Tests for the reinvest acquisition + income row pairing (Req 6)."""

    def _svc(self, db: Database) -> InvestmentService:
        _add_account(db)
        _add_security(db, security_id="sec_1", name="Vanguard", ticker="VTSAX")
        return db_service(db)

    def test_reinvest_writes_two_rows_sharing_group_id(self, db: Database) -> None:
        svc = self._svc(db)
        before_reinvest = _events_metric("reinvest")
        before_dividend = _events_metric("dividend")
        ids = svc.record_event(
            account_ref="acct_brokerage",
            security_ref="VTSAX",
            type_="reinvest",
            subtype=None,  # dividend is the default funding source
            trade_date=date(2024, 3, 20),
            quantity=Decimal("1.5"),
            price=Decimal("100.00"),
            amount=Decimal("-150.00"),  # cash redeployed
            fees=None,
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description="reinvest dividend",
            actor="cli",
            created_by="cli",
        )
        assert len(ids) == 2
        rows = _raw_rows(db)
        assert len(rows) == 2
        acq = next(r for r in rows if r[2] == "reinvest")
        income = next(r for r in rows if r[2] == "dividend")
        # Shared, minted event_group_id
        assert acq[4] is not None
        assert acq[4] == income[4]
        # Acquisition leg: positive qty, negative amount
        assert acq[5] == Decimal("1.5000000000")
        assert acq[7] == Decimal("-150.00")
        # Income leg: null qty, positive amount, security carried
        assert income[5] is None
        assert income[7] == Decimal("150.00")
        assert income[1] == "sec_1"
        assert _events_metric("reinvest") - before_reinvest == 1.0
        assert _events_metric("dividend") - before_dividend == 1.0

    @pytest.mark.parametrize(
        ("subtype", "expected_income_type"),
        [
            ("interest", "interest"),
            ("capital_gain", "capital_gain_distribution"),
            ("dividend", "dividend"),
        ],
    )
    def test_reinvest_income_type_from_subtype(
        self, db: Database, subtype: str, expected_income_type: str
    ) -> None:
        svc = self._svc(db)
        svc.record_event(
            account_ref="acct_brokerage",
            security_ref="VTSAX",
            type_="reinvest",
            subtype=subtype,
            trade_date=date(2024, 3, 20),
            quantity=Decimal("1"),
            price=Decimal("100.00"),
            amount=Decimal("-100.00"),
            fees=None,
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description=None,
            actor="cli",
            created_by="cli",
        )
        types = {r[2] for r in _raw_rows(db)}
        assert types == {"reinvest", expected_income_type}

    def test_reinvest_income_excludes_fees(self, db: Database) -> None:
        # `amount` is fee-inclusive per the module's sign-convention docstring
        # ("--amount help: Signed cash amount, including fees") — a $150.00
        # dividend reinvested with a $4.95 fee redeploys $154.95 total. The
        # income leg must report the $150.00 gross dividend, not the
        # fee-inclusive $154.95 acquisition amount.
        svc = self._svc(db)
        svc.record_event(
            account_ref="acct_brokerage",
            security_ref="VTSAX",
            type_="reinvest",
            subtype=None,
            trade_date=date(2024, 3, 20),
            quantity=Decimal("1.5"),
            price=Decimal("100.00"),
            amount=Decimal("-154.95"),
            fees=Decimal("4.95"),
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description="reinvest dividend with fee",
            actor="cli",
            created_by="cli",
        )
        rows = _raw_rows(db)
        acq = next(r for r in rows if r[2] == "reinvest")
        income = next(r for r in rows if r[2] == "dividend")
        assert acq[7] == Decimal("-154.95")  # acquisition stays fee-inclusive
        assert income[7] == Decimal("150.00")  # income excludes the fee


# ---------------------------------------------------------------------------
# record_event — split (D6) + transfer_in (Req 5 corporate actions)
# ---------------------------------------------------------------------------


class TestSplitAndTransfer:
    """Tests for split multiplier encoding (D6) and transfer_in mapping."""

    def _svc(self, db: Database) -> InvestmentService:
        _add_account(db)
        _add_security(db, security_id="sec_1", name="Apple Inc.", ticker="AAPL")
        return db_service(db)

    def test_split_multiplier_accepted_and_amount_null(self, db: Database) -> None:
        svc = self._svc(db)
        ids = svc.record_event(
            account_ref="acct_brokerage",
            security_ref="AAPL",
            type_="split",
            subtype=None,
            trade_date=date(2024, 8, 31),
            quantity=Decimal("2"),  # 2:1 multiplier
            price=None,
            amount=None,
            fees=None,
            acquired=None,
            basis=None,
            event_group_id=None,
            currency_code="USD",
            description="2:1 split",
            actor="cli",
            created_by="cli",
        )
        assert len(ids) == 1
        row = _raw_rows(db)[0]
        assert row[2] == "split"
        assert row[5] == Decimal("2.0000000000")  # multiplier in quantity
        assert row[6] is None  # price
        assert row[7] is None  # amount
        assert row[8] is None  # fees

    def test_split_non_positive_multiplier_rejected(self, db: Database) -> None:
        svc = self._svc(db)
        with pytest.raises(UserError, match="quantity|multiplier"):
            svc.record_event(
                account_ref="acct_brokerage",
                security_ref="AAPL",
                type_="split",
                subtype=None,
                trade_date=date(2024, 8, 31),
                quantity=None,  # missing multiplier
                price=None,
                amount=None,
                fees=None,
                acquired=None,
                basis=None,
                event_group_id=None,
                currency_code="USD",
                description=None,
                actor="cli",
                created_by="cli",
            )

    def test_transfer_in_maps_acquired_and_basis(self, db: Database) -> None:
        svc = self._svc(db)
        ids = svc.record_event(
            account_ref="acct_brokerage",
            security_ref="AAPL",
            type_="transfer_in",
            subtype=None,
            trade_date=date(2024, 5, 1),
            quantity=Decimal("10"),
            price=None,
            amount=None,
            fees=None,
            acquired=date(2020, 2, 2),  # original acquisition date
            basis=Decimal("1200.00"),  # supplied basis
            event_group_id=None,
            currency_code="USD",
            description="acats in",
            actor="cli",
            created_by="cli",
        )
        assert len(ids) == 1
        row = _raw_rows(db)[0]
        assert row[2] == "transfer_in"
        assert row[9] == date(2020, 2, 2)  # original_acquisition_date
        assert row[7] == Decimal("-1200.00")  # basis persisted as negative amount


# ---------------------------------------------------------------------------
# select_lots (Req 13)
# ---------------------------------------------------------------------------


def _selected_lots(db: Database) -> list[Any]:
    return db.conn.execute(
        "SELECT lot_id, quantity FROM app.lot_selections "
        "WHERE investment_transaction_id = 'sell_1' ORDER BY lot_id"  # noqa: S608  # test read, static SQL
    ).fetchall()


class TestSelectLots:
    """Tests for select_lots validation + declarative delegation (Req 13)."""

    def test_valid_selection_delegates(self, db: Database) -> None:
        _seed_disposal_and_lots(db)
        db_service(db).select_lots(
            "sell_1", [("lot_a", Decimal("6")), ("lot_b", Decimal("4"))], actor="cli"
        )
        assert _selected_lots(db) == [
            ("lot_a", Decimal("6.0000000000")),
            ("lot_b", Decimal("4.0000000000")),
        ]

    def test_empty_selection_clears(self, db: Database) -> None:
        _seed_disposal_and_lots(db)
        svc = db_service(db)
        svc.select_lots("sell_1", [("lot_a", Decimal("5"))], actor="cli")
        assert _selected_lots(db) != []
        svc.select_lots("sell_1", [], actor="cli")
        assert _selected_lots(db) == []

    def test_unknown_disposal_raises(self, db: Database) -> None:
        _seed_disposal_and_lots(db)
        with pytest.raises(UserError, match="disposal|not found"):
            db_service(db).select_lots(
                "does_not_exist", [("lot_a", Decimal("1"))], actor="cli"
            )

    def test_non_disposal_txn_raises(self, db: Database) -> None:
        _seed_disposal_and_lots(db)
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, type, quantity)
            VALUES ('buy_1', 'acct_brokerage', 'sec_1', 'buy', 10)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        with pytest.raises(UserError, match="disposal|sell"):
            db_service(db).select_lots("buy_1", [("lot_a", Decimal("1"))], actor="cli")

    def test_unknown_lot_raises(self, db: Database) -> None:
        _seed_disposal_and_lots(db)
        with pytest.raises(UserError, match="lot"):
            db_service(db).select_lots(
                "sell_1", [("lot_ghost", Decimal("1"))], actor="cli"
            )

    def test_oversubscribed_selection_raises(self, db: Database) -> None:
        _seed_disposal_and_lots(db)
        with pytest.raises(UserError, match="quantit|exceed"):
            db_service(db).select_lots(
                "sell_1",
                [("lot_a", Decimal("6")), ("lot_b", Decimal("6"))],  # 12 > |−10|
                actor="cli",
            )

    def test_lot_from_other_position_rejected(self, db: Database) -> None:
        # A lot that exists globally but belongs to a different (account,
        # security) than the disposal must be rejected — not silently accepted
        # and then dropped to a FIFO fallback by the engine (silent wrong 1099-B).
        _seed_disposal_and_lots(db)  # sell_1 on (acct_brokerage, sec_1)
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, remaining_quantity)
            VALUES ('lot_other', 'acct_brokerage', 'sec_2', 10)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        with pytest.raises(UserError, match="position|lot"):
            db_service(db).select_lots(
                "sell_1", [("lot_other", Decimal("5"))], actor="cli"
            )

    def test_lot_acquired_after_disposal_date_rejected(self, db: Database) -> None:
        # A lot acquired after the disposal's trade date isn't open yet at
        # replay time — the engine's chronological loop hasn't reached the
        # acquisition event when it processes this disposal, so the lot is
        # absent from `_consumption_plan`'s `by_lot_id` and the selection is
        # silently dropped to FIFO (same silent-wrong-1099-B failure mode as
        # the cross-position case above, just triggered by date instead of
        # account/security). Reject it up front instead.
        _seed_disposal_and_lots(db)  # sell_1 trades 2024-06-15
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 remaining_quantity)
            VALUES ('lot_future', 'acct_brokerage', 'sec_1', '2024-07-01', 10)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        with pytest.raises(UserError, match="position|lot"):
            db_service(db).select_lots(
                "sell_1", [("lot_future", Decimal("5"))], actor="cli"
            )

    def test_lot_acquired_same_day_as_disposal_accepted(self, db: Database) -> None:
        # The engine orders same-day acquisitions before disposals
        # (_SAME_DAY_TYPE_ORDER), so a lot acquired on the disposal's own
        # trade date IS open by replay time and must be a valid selection.
        _seed_disposal_and_lots(db)  # sell_1 trades 2024-06-15
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_lots
                (lot_id, account_id, security_id, acquisition_date,
                 original_quantity, remaining_quantity)
            VALUES ('lot_sameday', 'acct_brokerage', 'sec_1', '2024-06-15', 10, 10)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db_service(db).select_lots(
            "sell_1", [("lot_sameday", Decimal("5"))], actor="cli"
        )
        assert _selected_lots(db) == [("lot_sameday", Decimal("5.0000000000"))]

    def test_lot_already_closed_by_earlier_disposal_rejected(
        self, db: Database
    ) -> None:
        # lot_a (original_quantity=6) was already fully consumed by an
        # earlier disposal (sell_earlier, 2024-04-01, before sell_1's
        # 2024-06-15). At replay time sell_1 sees lot_a with
        # remaining_quantity=0 in `_consumption_plan`'s `by_lot_id` and
        # silently skips it, falling to FIFO for the full amount — the same
        # silent-wrong-1099-B failure mode as an unavailable-by-date lot.
        _seed_disposal_and_lots(db)  # sell_1 trades 2024-06-15, lot_a qty=6
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity)
            VALUES ('sell_earlier', 'acct_brokerage', 'sec_1', '2024-04-01',
                    'sell', -6)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('rg_1', 'acct_brokerage', 'sec_1', 'sell_earlier', 'lot_a',
                    6, '2024-01-10', '2024-04-01', 600.00, 500.00, 100.00,
                    'short', 'fifo', false, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.execute(
            "UPDATE core.fct_investment_lots SET remaining_quantity = 0 "
            "WHERE lot_id = 'lot_a'"  # noqa: S608  # test fixture update, static SQL
        )
        with pytest.raises(UserError, match="position|lot"):
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")

    def test_lot_partially_consumed_by_earlier_disposal_caps_selection(
        self, db: Database
    ) -> None:
        # lot_a (original_quantity=6) had 4 units drawn by an earlier
        # disposal, leaving 2 available as of sell_1's trade date. Selecting
        # more than that available remainder must be rejected — the
        # requested-but-unavailable units would otherwise silently fall to
        # FIFO instead of raising.
        _seed_disposal_and_lots(db)
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity)
            VALUES ('sell_earlier', 'acct_brokerage', 'sec_1', '2024-04-01',
                    'sell', -4)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.execute(
            """
            INSERT INTO core.fct_realized_gains
                (realized_gain_id, account_id, security_id, disposal_txn_id,
                 lot_id, quantity, acquisition_date, disposal_date, proceeds,
                 cost_basis, gain_loss, term, cost_basis_method,
                 basis_incomplete, currency_code)
            VALUES ('rg_1', 'acct_brokerage', 'sec_1', 'sell_earlier', 'lot_a',
                    4, '2024-01-10', '2024-04-01', 400.00, 333.33, 66.67,
                    'short', 'fifo', false, 'USD')
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.execute(
            "UPDATE core.fct_investment_lots SET remaining_quantity = 2 "
            "WHERE lot_id = 'lot_a'"  # noqa: S608  # test fixture update, static SQL
        )
        with pytest.raises(UserError, match="position|lot"):
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("3"))], actor="cli")
        # The 2 units still available are a valid selection.
        db_service(db).select_lots("sell_1", [("lot_a", Decimal("2"))], actor="cli")

    def test_lot_consumed_by_earlier_transfer_out_rejected(self, db: Database) -> None:
        # `transfer_out` consumes lots but realizes no gain, so it never writes
        # a `core.fct_realized_gains` row (cost_basis.py:449-450). A check that
        # only sums `fct_realized_gains` to find earlier consumption is blind
        # to an earlier transfer_out's draw-down, so a selection could exceed
        # what's actually left and silently fall back to FIFO at replay time —
        # same failure mode as an earlier sell, just invisible to that table.
        # lot_a (original_quantity=6) was reduced to remaining_quantity=2 by an
        # earlier transfer_out (2024-04-01, before sell_1's 2024-06-15).
        _seed_disposal_and_lots(db)  # sell_1 trades 2024-06-15, lot_a qty=6
        db.conn.execute(
            """
            INSERT INTO core.fct_investment_transactions
                (investment_transaction_id, account_id, security_id, trade_date,
                 type, quantity)
            VALUES ('transfer_out_earlier', 'acct_brokerage', 'sec_1',
                    '2024-04-01', 'transfer_out', -4)
            """  # noqa: S608  # test fixture insert, static SQL
        )
        db.conn.execute(
            "UPDATE core.fct_investment_lots SET remaining_quantity = 2 "
            "WHERE lot_id = 'lot_a'"  # noqa: S608  # test fixture update, static SQL
        )
        with pytest.raises(UserError, match="position|lot"):
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("3"))], actor="cli")
        # The 2 units still available are a valid selection.
        db_service(db).select_lots("sell_1", [("lot_a", Decimal("2"))], actor="cli")

    def test_refuses_selection_when_the_resolved_method_ignores_it(
        self, db: Database
    ) -> None:
        # With nothing elected the disposal replays under FIFO, which never
        # reads app.lot_selections. Persisting the rows and reporting success
        # would leave a write that is silently discarded at the next refresh.
        _seed_disposal_and_lots(db, cost_basis_method=None)
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert exc.value.code == error_codes.INVESTMENT_METHOD_NOT_SPECIFIC
        assert _selected_lots(db) == []

    def test_refusal_hands_back_the_election_that_would_fix_it(
        self, db: Database
    ) -> None:
        _seed_disposal_and_lots(db, cost_basis_method=None)
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        actions = exc.value.recovery_actions or []
        assert [(a.tool, a.arguments) for a in actions] == [
            (
                "investments_securities_set",
                {"security_id": "sec_1", "cost_basis_method": "specific"},
            )
        ]

    def test_refuses_selection_when_the_security_row_is_gone(
        self, db: Database
    ) -> None:
        # An accepted security-link merge deletes the losing security while the
        # materialized ledger still points at it until the next refresh. No
        # RecoveryAction is offered — investments_securities_set on that id
        # would raise mutation_not_found — so the hint carries the fix alone.
        _seed_disposal_and_lots(db, cost_basis_method=None)
        db.conn.execute("DELETE FROM app.securities WHERE security_id = 'sec_1'")
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert exc.value.code == error_codes.INVESTMENT_SECURITY_NOT_IN_CATALOG
        assert exc.value.recovery_actions is None
        assert "refresh" in (exc.value.hint or "").lower()
        assert _selected_lots(db) == []

    def test_a_specific_account_default_cannot_rescue_a_deleted_security(
        self, db: Database
    ) -> None:
        # The account default must not be consulted once the security row is
        # gone. Resolving it returned 'specific' and let the write through
        # against lot ids the next refresh re-keys under the merge survivor;
        # the selection then stops matching and _consumption_plan discards it —
        # the exact silent discard this guard exists to prevent.
        _seed_disposal_and_lots(db, cost_basis_method=None)
        _set_account_default_method(db, "specific")
        db.conn.execute("DELETE FROM app.securities WHERE security_id = 'sec_1'")
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert exc.value.code == error_codes.INVESTMENT_SECURITY_NOT_IN_CATALOG
        assert _selected_lots(db) == []

    def test_security_specific_beats_an_account_default_of_fifo(
        self, db: Database
    ) -> None:
        _seed_disposal_and_lots(db, cost_basis_method="specific")
        _set_account_default_method(db, "fifo")
        db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert _selected_lots(db) == [("lot_a", Decimal("6.0000000000"))]

    def test_account_default_of_specific_permits_the_selection(
        self, db: Database
    ) -> None:
        _seed_disposal_and_lots(db, cost_basis_method=None)
        _set_account_default_method(db, "specific")
        db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert _selected_lots(db) == [("lot_a", Decimal("6.0000000000"))]

    def test_security_fifo_beats_an_account_default_of_specific(
        self, db: Database
    ) -> None:
        _seed_disposal_and_lots(db, cost_basis_method="fifo")
        _set_account_default_method(db, "specific")
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert exc.value.code == error_codes.INVESTMENT_METHOD_NOT_SPECIFIC

    def test_disposal_with_no_bound_security_names_the_real_cause(
        self, db: Database
    ) -> None:
        # A synced sell whose security link is still pending carries a NULL
        # security_id. The engine skips such events entirely, so the disposal
        # never replays under any method — reporting an elected method (and
        # telling the user to change it) would send them somewhere that cannot
        # help. Setting the account default to 'specific' must not talk them
        # past this guard into a second, unrelated failure.
        _seed_disposal_and_lots(db, cost_basis_method=None)
        _set_account_default_method(db, "specific")
        db.conn.execute(
            "UPDATE core.fct_investment_transactions SET security_id = NULL "
            "WHERE investment_transaction_id = 'sell_1'"  # noqa: S608  # test fixture update, static SQL
        )
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        assert exc.value.code == error_codes.INVESTMENT_SECURITY_NOT_BOUND
        combined = f"{exc.value} {exc.value.hint or ''}".lower()
        assert "security" in combined
        assert "cost basis" not in combined  # never blames the elected method
        assert _selected_lots(db) == []

    def test_clearing_stays_allowed_after_the_method_moves_off_specific(
        self, db: Database
    ) -> None:
        # Clearing under a non-specific method is NOT inert — it deletes. Gating
        # it too would strand rows written while the security was 'specific',
        # removable only by first flipping the election back.
        _seed_disposal_and_lots(db, cost_basis_method="specific")
        svc = db_service(db)
        svc.select_lots("sell_1", [("lot_a", Decimal("6"))], actor="cli")
        _add_security(
            db,
            security_id="sec_1",
            name="Apple Inc.",
            ticker="AAPL",
            cost_basis_method="fifo",
        )
        svc.select_lots("sell_1", [], actor="cli")
        assert _selected_lots(db) == []

    def test_unknown_disposal_hints_at_refresh(self, db: Database) -> None:
        # A just-recorded sell lives in raw until `refresh run` materializes core;
        # the not-found error must point the user at refresh, not read as a dead
        # end for an id the record tool just returned as valid.
        _seed_disposal_and_lots(db)
        with pytest.raises(UserError) as exc:
            db_service(db).select_lots(
                "does_not_exist", [("lot_a", Decimal("1"))], actor="cli"
            )
        combined = f"{exc.value} {exc.value.hint or ''}".lower()
        assert "refresh" in combined


# ---------------------------------------------------------------------------
# Read path (Task 14b): list_events, holdings, lots, gains
# ---------------------------------------------------------------------------


def _seed_read_fixtures(db: Database) -> None:
    """Two accounts + two securities + the core.* stub tables the reads query."""
    _add_account(db, "acct_brokerage")
    _add_account(db, "acct_roth")
    _add_security(db, security_id="sec_1", name="Apple Inc.", ticker="AAPL")
    _add_security(db, security_id="sec_2", name="Vanguard Total", ticker="VTSAX")
    create_core_dim_stub_views(db)


def _insert_event(
    db: Database,
    *,
    investment_transaction_id: str,
    account_id: str = "acct_brokerage",
    security_id: str | None = "sec_1",
    trade_date: date = date(2024, 1, 15),
    type_: str = "buy",
    subtype: str | None = None,
    quantity: Decimal | None = Decimal("10"),
    amount: Decimal | None = Decimal("-1500.00"),
    currency_code: str | None = "USD",
    source_type: str = "manual",
) -> None:
    db.conn.execute(
        """
        INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, trade_date,
             type, subtype, quantity, amount, currency_code, source_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,  # noqa: S608  # test fixture insert, static SQL
        [
            investment_transaction_id,
            account_id,
            security_id,
            trade_date,
            type_,
            subtype,
            quantity,
            amount,
            currency_code,
            source_type,
        ],
    )


def _seed_two_source_ledger(
    db: Database, *, account_id: str = "acct_brokerage"
) -> None:
    """Give one account an investment ledger from two sources at once.

    The state ``core.dim_holdings``'s ``source_overlap_accounts`` keys on:
    more than one ``source_type`` among the account's non-bootstrap ledger
    rows. Seeded on the ledger rather than on a lot or a gain because that is
    where the fault lives — the derived rows carry no marker of their own, which
    is why the ledger has to be consulted at read time.
    """
    _insert_event(
        db,
        investment_transaction_id=f"{account_id}_manual_buy",
        account_id=account_id,
        source_type="manual",
    )
    _insert_event(
        db,
        investment_transaction_id=f"{account_id}_plaid_buy",
        account_id=account_id,
        source_type="plaid",
    )


def _insert_lot(
    db: Database,
    *,
    lot_id: str,
    account_id: str = "acct_brokerage",
    security_id: str = "sec_1",
    acquisition_date: date = date(2024, 1, 15),
    remaining_quantity: Decimal = Decimal("10"),
    cost_basis_remaining: Decimal = Decimal("1500.00"),
    is_open: bool = True,
    basis_incomplete: bool = False,
    currency_code: str | None = "USD",
) -> None:
    db.conn.execute(
        """
        INSERT INTO core.fct_investment_lots
            (lot_id, account_id, security_id, acquisition_date, acquisition_type,
             original_quantity, remaining_quantity, cost_basis_total,
             cost_basis_remaining, cost_basis_method, currency_code, is_open,
             basis_incomplete)
        VALUES (?, ?, ?, ?, 'buy', ?, ?, ?, ?, 'fifo', ?, ?, ?)
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
            currency_code,
            is_open,
            basis_incomplete,
        ],
    )


def _insert_gain(
    db: Database,
    *,
    realized_gain_id: str,
    account_id: str = "acct_brokerage",
    security_id: str = "sec_1",
    disposal_txn_id: str = "sell_1",
    lot_id: str = "lot_a",
    disposal_date: date = date(2024, 6, 12),
    proceeds: Decimal = Decimal("950.00"),
    cost_basis: Decimal = Decimal("750.00"),
    gain_loss: Decimal = Decimal("200.00"),
    term: str = "long",
    basis_incomplete: bool = False,
    currency_code: str | None = "USD",
) -> None:
    db.conn.execute(
        """
        INSERT INTO core.fct_realized_gains
            (realized_gain_id, account_id, security_id, disposal_txn_id, lot_id,
             quantity, acquisition_date, disposal_date, proceeds, cost_basis,
             gain_loss, term, cost_basis_method, basis_incomplete, currency_code)
        VALUES (?, ?, ?, ?, ?, 5, '2024-01-01'::DATE, ?, ?, ?, ?, ?, 'fifo', ?, ?)
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
            currency_code,
        ],
    )


class _Holding(NamedTuple):
    """One ``core.dim_holdings`` fixture row.

    Valuation columns default to the *unpriced* shape (no close resolved), so a
    test that says nothing about valuation gets the honest no-price row rather
    than an invented number.
    """

    account_id: str = "acct_brokerage"
    security_id: str = "sec_1"
    quantity: str = "10"
    cost_basis: str = "1000.00"
    average_cost: str | None = "100.00"
    currency_code: str | None = "USD"
    market_value: str | None = None
    unrealized_gain: str | None = None
    price_date: str | None = None
    price_source: str | None = None
    days_since_observed: str | None = None
    valuation_status: str = "unpriced"


def _holding_select(h: _Holding) -> str:
    """Render one ``_Holding`` as a typed single-row SELECT."""

    def money(v: str | None) -> str:
        return "CAST(NULL AS DECIMAL(18,2))" if v is None else f"{v}::DECIMAL(18,2)"

    def text(v: str | None) -> str:
        return "CAST(NULL AS VARCHAR)" if v is None else f"'{v}'"

    return (
        f"SELECT '{h.account_id}' AS account_id, "
        f"'{h.security_id}' AS security_id, "
        f"{h.quantity}::DECIMAL(28,10) AS quantity, "
        f"{h.cost_basis}::DECIMAL(18,2) AS cost_basis, "
        + (
            "CAST(NULL AS DECIMAL(28,10))"
            if h.average_cost is None
            else f"{h.average_cost}::DECIMAL(28,10)"
        )
        + " AS average_cost, "
        f"{text(h.currency_code)} AS currency_code, "
        f"{money(h.market_value)} AS market_value, "
        f"{money(h.unrealized_gain)} AS unrealized_gain, "
        + ("CAST(NULL AS DATE)" if h.price_date is None else f"DATE '{h.price_date}'")
        + " AS price_date, "
        f"{text(h.price_source)} AS price_source, "
        + (
            "CAST(NULL AS INT)"
            if h.days_since_observed is None
            else f"{h.days_since_observed}::INT"
        )
        + " AS days_since_observed, "
        f"'{h.valuation_status}' AS valuation_status"
    )


def _replace_holdings_view(db: Database, rows: list[_Holding]) -> None:
    """Override the empty core.dim_holdings stub with literal test rows.

    core.dim_holdings is a SQLMesh-managed VIEW; create_core_dim_stub_views
    stubs it as an empty ``WHERE FALSE`` view (matching the dim_categories/
    dim_merchants stub convention), so holdings() tests replace it with
    literal data — mirroring test_definitions.py's _install_balance_drift
    precedent for overriding a stub view with VALUES. Values are literal
    (not user input), per security.md's test-fixture exception.
    """
    if not rows:
        select_sql = _holding_select(_Holding()) + " WHERE FALSE"
    else:
        select_sql = " UNION ALL ".join(_holding_select(h) for h in rows)
    db.execute(  # noqa: S608  # test fixture view, literal test data only
        f"CREATE OR REPLACE VIEW core.dim_holdings AS {select_sql}"
    )


def _priced(
    security_id: str,
    currency_code: str,
    market_value: str,
    *,
    price_date: str = "2026-07-15",
    account_id: str = "acct_brokerage",
) -> _Holding:
    """A valued position — the shape every currency-total test needs."""
    return _Holding(
        account_id=account_id,
        security_id=security_id,
        currency_code=currency_code,
        market_value=market_value,
        unrealized_gain="0.00",
        price_date=price_date,
        price_source="plaid",
        days_since_observed="0",
        valuation_status="valued",
    )


def _set_home_currency(db: Database, currency_code: str) -> None:
    ProfileSettingsRepo(db).set_home_currency(currency_code, actor="test")


def _seed_rate(db: Database, base: str, quote: str, on: date, rate: Decimal) -> None:
    """Put one provider rate in the cache, as a refresh backfill would."""
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
        VALUES (?, ?, ?, ?, 'frankfurter', ?)
        """,
        [base, quote, on, rate, datetime(2026, 7, 16, 12, 0, 0)],
    )


class TestListEvents:
    """Tests for InvestmentService.list_events()."""

    def test_returns_seeded_rows_with_decimal_preserved(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="evt_1", quantity=Decimal("10.5"))
        result = db_service(db).list_events()
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.investment_transaction_id == "evt_1"
        assert row.quantity == Decimal("10.5")
        assert isinstance(row.amount, Decimal)
        assert result.warnings == []

    def test_account_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="evt_brokerage")
        _insert_event(db, investment_transaction_id="evt_roth", account_id="acct_roth")
        result = db_service(db).list_events(account_ref="acct_brokerage")
        assert [r.investment_transaction_id for r in result.rows] == ["evt_brokerage"]

    def test_security_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="evt_aapl", security_id="sec_1")
        _insert_event(db, investment_transaction_id="evt_vtsax", security_id="sec_2")
        result = db_service(db).list_events(security_ref="VTSAX")
        assert [r.investment_transaction_id for r in result.rows] == ["evt_vtsax"]

    def test_type_filter(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="evt_buy", type_="buy")
        _insert_event(
            db,
            investment_transaction_id="evt_sell",
            type_="sell",
            quantity=Decimal("-5"),
            amount=Decimal("750.00"),
        )
        result = db_service(db).list_events(type_filter="sell")
        assert [r.investment_transaction_id for r in result.rows] == ["evt_sell"]

    def test_date_range_filter(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_event(
            db, investment_transaction_id="evt_jan", trade_date=date(2024, 1, 15)
        )
        _insert_event(
            db, investment_transaction_id="evt_jun", trade_date=date(2024, 6, 15)
        )
        result = db_service(db).list_events(
            date_from=date(2024, 3, 1), date_to=date(2024, 12, 31)
        )
        assert [r.investment_transaction_id for r in result.rows] == ["evt_jun"]

    def test_invalid_type_filter_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(ValueError, match="type_filter"):
            db_service(db).list_events(type_filter="frobnicate")

    def test_unknown_account_ref_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(UserError):
            db_service(db).list_events(account_ref="does-not-exist")

    def test_unknown_security_ref_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(SecurityResolutionError):
            db_service(db).list_events(security_ref="nothing-matches-this")


def _staleness_gauge() -> float:
    return REGISTRY.get_sample_value("moneybin_price_staleness_days") or 0.0


def _resolution_status(status: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_price_resolution_status_total", {"status": status}
        )
        or 0.0
    )


class TestHoldingsMetrics:
    """The two portfolio-wide valuation instruments and their filter guard."""

    _ROWS = [
        _Holding(
            security_id="sec_1",
            market_value="2700.00",
            price_date="2026-07-15",
            price_source="plaid",
            days_since_observed="2",
            valuation_status="valued",
        ),
        _Holding(
            security_id="sec_2",
            market_value="500.00",
            price_date="2026-07-09",
            price_source="tiingo",
            days_since_observed="9",
            valuation_status="carried_forward",
        ),
    ]

    def test_an_unfiltered_read_publishes_the_stalest_age(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(db, self._ROWS)

        db_service(db).holdings()

        assert _staleness_gauge() == 9

    def test_an_unfiltered_read_counts_every_valuation_status(
        self, db: Database
    ) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(db, self._ROWS)
        before = (_resolution_status("valued"), _resolution_status("carried_forward"))

        db_service(db).holdings()

        assert _resolution_status("valued") - before[0] == 1
        assert _resolution_status("carried_forward") - before[1] == 1

    def test_a_portfolio_with_no_priced_position_does_not_read_as_fresh(
        self, db: Database
    ) -> None:
        """Zero is the healthiest value this gauge can hold, so absence must not use it.

        `days_since_observed` is 0 on a same-day close, so publishing 0 for "no
        position carries a market value" makes a total pricing outage — an
        expired token, every feed key broken, a fresh profile — indistinguishable
        from a perfectly fresh portfolio. An alert of the shape
        `moneybin_price_staleness_days > 4` then cannot fire in the one case the
        gauge exists to expose. NaN is the Prometheus convention for no-data, and
        it matches `max_days_since_observed`, which already reports None for the
        identical empty set.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    market_value=None,
                    price_date=None,
                    price_source=None,
                    days_since_observed=None,
                    valuation_status="unpriced",
                )
            ],
        )
        PRICE_STALENESS_DAYS.set(0)

        result = db_service(db).holdings()

        assert result.max_days_since_observed is None
        assert math.isnan(_staleness_gauge())

    def test_a_filtered_read_publishes_nothing(self, db: Database) -> None:
        """Both instruments describe the portfolio, so a filtered read must not set them.

        Recording one would make the exported value depend on whichever filter
        the last caller happened to pass: asking for one recently-priced position
        would publish its age as the age of every number in net worth, and the
        stale position it excluded would vanish from the status counts. The
        adversarial partner to the two tests above — same fixture, same service,
        only the filter differs.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(db, self._ROWS)
        PRICE_STALENESS_DAYS.set(0)
        before = _resolution_status("valued")

        db_service(db).holdings(security_ref="sec_1")

        assert _staleness_gauge() == 0
        assert _resolution_status("valued") - before == 0


class TestHoldings:
    """Tests for InvestmentService.holdings()."""

    def test_empty_result_carries_no_warning(self, db: Database) -> None:
        _seed_read_fixtures(db)
        result = db_service(db).holdings()
        assert result.rows == []
        assert result.warnings == []

    def test_returns_seeded_rows_with_decimal_preserved(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    quantity="15",
                    cost_basis="2475.00",
                    average_cost="165.00",
                    market_value="2700.00",
                    unrealized_gain="225.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                )
            ],
        )
        result = db_service(db).holdings()
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.quantity == Decimal("15.0000000000")
        assert row.cost_basis == Decimal("2475.00")
        assert row.average_cost == Decimal("165.0000000000")
        assert isinstance(row.quantity, Decimal)
        # Pillar C: the valuation columns reach the surface, so no caveat fires.
        assert row.market_value == Decimal("2700.00")
        assert row.unrealized_gain == Decimal("225.00")
        assert row.price_date == date(2026, 7, 15)
        assert row.price_source == "plaid"
        assert row.days_since_observed == 0
        assert row.valuation_status == "valued"
        assert result.warnings == []

    def test_unrealized_gain_carries_a_loss_as_a_negative(self, db: Database) -> None:
        """A position below cost reports a signed loss, not its magnitude."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    quantity="10",
                    cost_basis="1000.00",
                    market_value="800.00",
                    unrealized_gain="-200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="1",
                    valuation_status="carried_forward",
                )
            ],
        )
        result = db_service(db).holdings()
        assert result.rows[0].unrealized_gain == Decimal("-200.00")

    def test_unvalued_rows_are_counted_in_a_warning(self, db: Database) -> None:
        """withheld/unpriced rows carry NULL, so the caller is told how many."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
                _Holding(security_id="sec_2", valuation_status="unpriced"),
                _Holding(security_id="sec_3", valuation_status="withheld"),
            ],
        )
        result = db_service(db).holdings()
        assert len(result.warnings) == 1
        warning = result.warnings[0]
        assert "2" in warning
        assert "unpriced" in warning
        assert "withheld" in warning

    def test_source_overlap_is_warned_and_degrades_the_read(self, db: Database) -> None:
        """A mixed-source account gets its own warning and a machine-readable code.

        Counted in the no-market-value warning like any other blank row, so the
        count matches what is on screen — but with a second warning of its own,
        because the remedy shares nothing with the other two. `unpriced` wants a
        price feed and `withheld` wants a share count reconciled; this one wants
        a whole feed removed, and no re-run of the pipeline will clear it.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(security_id="sec_1", valuation_status="unpriced"),
                _Holding(security_id="sec_2", valuation_status="source_overlap"),
            ],
        )
        result = db_service(db).holdings()

        assert len(result.warnings) == 2
        assert "2 position(s) report no market value" in result.warnings[0]
        overlap = result.warnings[1]
        assert "1 position(s)" in overlap
        assert "two sources" in overlap
        assert result.degraded_reason is not None
        assert result.degraded_reason.startswith("investment_source_overlap:")

    def test_no_degraded_reason_without_a_source_overlap(self, db: Database) -> None:
        """The code is set only when the state it names is present.

        A `degraded` flag that rides along on every unpriced portfolio is a flag
        no consumer can act on.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db, [_Holding(security_id="sec_1", valuation_status="withheld")]
        )
        result = db_service(db).holdings()

        assert result.degraded_reason is None
        assert len(result.warnings) == 1

    def test_the_withheld_warning_names_currency_as_a_reason(
        self, db: Database
    ) -> None:
        """`withheld` is not only a share-count verdict, and the wording decides the fix.

        A position is also withheld when its open lots disagree on currency — the
        quantity can be perfectly right and only the denomination unresolved. A
        warning that says the share count is known wrong sends that user to
        reconcile shares, which changes nothing; the fix is `accounts set
        --currency` or a per-event currency.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(db, [_Holding(valuation_status="withheld")])
        warning = db_service(db).holdings().warnings[0]

        assert "currency" in warning.lower()

    def test_max_days_since_observed_reports_the_stalest_priced_position(
        self, db: Database
    ) -> None:
        """The portfolio-level number is the worst age, not the freshest."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
                _Holding(
                    security_id="sec_2",
                    market_value="800.00",
                    unrealized_gain="-200.00",
                    price_date="2026-03-02",
                    price_source="plaid",
                    days_since_observed="135",
                    valuation_status="carried_forward",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.max_days_since_observed == 135

    def test_a_four_month_old_close_reports_its_age_without_a_warning(
        self, db: Database
    ) -> None:
        """The carried_forward regression: staleness discloses as a number.

        Counting carried_forward as unvalued would fire on every weekend, so the
        disclosure is the age itself — always present, never a warning.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-03-02",
                    price_source="plaid",
                    days_since_observed="135",
                    valuation_status="carried_forward",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.max_days_since_observed == 135
        assert result.warnings == []

    def test_max_days_since_observed_is_none_when_nothing_is_priced(
        self, db: Database
    ) -> None:
        """No priced position means the max is undefined — null, not a fresh 0."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(security_id="sec_1", valuation_status="unpriced"),
                _Holding(security_id="sec_2", valuation_status="withheld"),
            ],
        )
        result = db_service(db).holdings()
        assert result.max_days_since_observed is None
        # Paired positive: the same shape yields a number once one row prices.
        _replace_holdings_view(
            db,
            [
                _Holding(security_id="sec_1", valuation_status="unpriced"),
                _Holding(
                    security_id="sec_2",
                    market_value="800.00",
                    unrealized_gain="-200.00",
                    price_date="2026-07-12",
                    price_source="plaid",
                    days_since_observed="3",
                    valuation_status="carried_forward",
                ),
            ],
        )
        assert db_service(db).holdings().max_days_since_observed == 3

    def test_single_currency_portfolio_totals_its_market_value(
        self, db: Database
    ) -> None:
        """One currency across every priced position — the total is safe to sum."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
                _Holding(
                    security_id="sec_2",
                    market_value="800.50",
                    unrealized_gain="-199.50",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value == Decimal("2000.50")
        assert result.market_value_by_currency == {"USD": Decimal("2000.50")}

    def test_mixed_currency_portfolio_refuses_a_total(self, db: Database) -> None:
        """With no home currency, there is no unit to sum into; publish the split.

        Mixing currencies is no longer refused outright — a home currency plus a
        stored rate produces a combined figure, which
        ``test_a_mixed_portfolio_totals_in_the_home_currency`` covers. What this
        profile lacks is the target: nothing names the unit a combined number
        would be in, so stating one would invent it.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    currency_code="USD",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
                _Holding(
                    security_id="sec_2",
                    currency_code="EUR",
                    market_value="900.00",
                    unrealized_gain="100.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value is None
        assert result.market_value_by_currency == {
            "USD": Decimal("1200.00"),
            "EUR": Decimal("900.00"),
        }

    def test_currency_casing_does_not_read_as_a_second_currency(
        self, db: Database
    ) -> None:
        """'usd' and 'USD' are one currency — a total must still publish."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    currency_code="USD",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
                _Holding(
                    security_id="sec_2",
                    currency_code="usd",
                    market_value="800.00",
                    unrealized_gain="-200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value == Decimal("2000.00")
        assert result.market_value_by_currency == {"USD": Decimal("2000.00")}

    def test_unpriced_positions_are_excluded_from_the_total(self, db: Database) -> None:
        """A NULL market value contributes nothing — and no zero-currency key."""
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(
                    security_id="sec_1",
                    market_value="1200.00",
                    unrealized_gain="200.00",
                    price_date="2026-07-15",
                    price_source="plaid",
                    days_since_observed="0",
                    valuation_status="valued",
                ),
                _Holding(
                    security_id="sec_2",
                    currency_code="EUR",
                    valuation_status="unpriced",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value == Decimal("1200.00")
        assert result.market_value_by_currency == {"USD": Decimal("1200.00")}

    def test_nothing_priced_publishes_no_total_and_an_empty_breakdown(
        self, db: Database
    ) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db, [_Holding(security_id="sec_1", valuation_status="unpriced")]
        )
        result = db_service(db).holdings()
        assert result.total_market_value is None
        assert result.market_value_by_currency == {}
        assert result.total_market_value_currency is None

    def test_a_single_currency_total_names_the_currency_it_is_in(
        self, db: Database
    ) -> None:
        """The undisputed case still says which unit the number carries."""
        _seed_read_fixtures(db)
        _replace_holdings_view(db, [_priced("sec_1", "USD", "1200.00")])
        result = db_service(db).holdings()
        assert result.total_market_value == Decimal("1200.00")
        assert result.total_market_value_currency == "USD"

    def test_a_mixed_portfolio_totals_in_the_home_currency(self, db: Database) -> None:
        """EUR priced into USD at its own close's rate, then added to the dollars.

        Hand-derived: 900.00 EUR x 1.10 = 990.00 USD, plus the 1200.00 USD
        position = 2190.00. The per-currency split stays in original units,
        because an original amount is the canonical one.
        """
        _seed_read_fixtures(db)
        _set_home_currency(db, "USD")
        _seed_rate(db, "EUR", "USD", date(2026, 7, 15), Decimal("1.10"))
        _replace_holdings_view(
            db,
            [
                _priced("sec_1", "USD", "1200.00"),
                _priced("sec_2", "EUR", "900.00"),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value == Decimal("2190.00")
        assert result.total_market_value_currency == "USD"
        assert result.market_value_by_currency == {
            "USD": Decimal("1200.00"),
            "EUR": Decimal("900.00"),
        }

    def test_the_converted_total_names_the_rate_behind_it(self, db: Database) -> None:
        """Requirement 10: a converted figure states what priced it.

        The reports surface answers this with ``summary.applied_rates``. A
        holdings total is converted by the same rule at the same layer, so
        publishing the figure without its rate would leave the contract holding
        on one surface and not the other.
        """
        _seed_read_fixtures(db)
        _set_home_currency(db, "USD")
        _seed_rate(db, "EUR", "USD", date(2026, 7, 15), Decimal("1.10"))
        _replace_holdings_view(
            db,
            [
                _priced("sec_1", "USD", "1200.00"),
                _priced("sec_2", "EUR", "900.00"),
            ],
        )

        result = db_service(db).holdings()

        assert [
            (rate.from_currency, rate.to_currency, rate.rate)
            for rate in result.applied_rates
        ] == [("EUR", "USD", Decimal("1.10"))]

    def test_an_unconverted_total_names_no_rate(self, db: Database) -> None:
        """A single-currency portfolio converts nothing, so it priced nothing.

        "Nothing was converted" and "converted at a rate nobody recorded" must
        not read alike, so the empty case stays empty rather than inventing an
        identity rate.
        """
        _seed_read_fixtures(db)
        _replace_holdings_view(db, [_priced("sec_1", "USD", "1200.00")])

        result = db_service(db).holdings()

        assert result.total_market_value == Decimal("1200.00")
        assert result.applied_rates == ()

    def test_each_position_prices_at_its_own_close_date(self, db: Database) -> None:
        """Two EUR positions valued on different days use each day's own rate.

        Hand-derived: 100.00 EUR at 1.10 = 110.00, 100.00 EUR at 1.50 = 150.00,
        plus 1000.00 USD = 1260.00. A single portfolio-wide rate would give
        1220.00 at the earlier date or 1300.00 at the later one, so this asserts
        a number neither shortcut can produce.
        """
        _seed_read_fixtures(db)
        _set_home_currency(db, "USD")
        _seed_rate(db, "EUR", "USD", date(2026, 7, 15), Decimal("1.10"))
        _seed_rate(db, "EUR", "USD", date(2026, 7, 16), Decimal("1.50"))
        _replace_holdings_view(
            db,
            [
                _priced("sec_1", "USD", "1000.00"),
                _priced("sec_2", "EUR", "100.00"),
                _priced(
                    "sec_2",
                    "EUR",
                    "100.00",
                    price_date="2026-07-16",
                    account_id="acct_roth",
                ),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value == Decimal("1260.00")

    def test_an_unpriceable_pair_leaves_the_total_unpublished(
        self, db: Database
    ) -> None:
        """No rate on disk is the segmentation fallback, not an error."""
        _seed_read_fixtures(db)
        _set_home_currency(db, "USD")
        _replace_holdings_view(
            db,
            [
                _priced("sec_1", "USD", "1200.00"),
                _priced("sec_2", "EUR", "900.00"),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value is None
        assert result.total_market_value_currency is None
        assert result.market_value_by_currency == {
            "USD": Decimal("1200.00"),
            "EUR": Decimal("900.00"),
        }

    def test_a_non_iso_position_degrades_the_total_not_the_whole_read(
        self, db: Database
    ) -> None:
        """One unconvertible code costs the combined figure and nothing else.

        ``dim_holdings`` stores a lot's ``currency_code`` verbatim and documents
        that unofficial crypto codes arrive uncased, so a four-letter code is a
        real row rather than a hypothetical. ``resolve_rate`` refuses it with a
        plain ``UserError`` — a different exception from the missing-rate case,
        and one that would otherwise escape ``holdings()`` and fail the entire
        read for every other position too.
        """
        _seed_read_fixtures(db)
        _set_home_currency(db, "USD")
        _seed_rate(db, "EUR", "USD", date(2026, 7, 15), Decimal("1.10"))
        _replace_holdings_view(
            db,
            [
                _priced("sec_1", "USD", "1200.00"),
                _priced("sec_2", "USDT", "500.00"),
            ],
        )

        result = db_service(db).holdings()

        assert result.total_market_value is None
        assert result.total_market_value_currency is None
        # The read itself survives: both positions still report their own value.
        assert result.market_value_by_currency == {
            "USD": Decimal("1200.00"),
            "USDT": Decimal("500.00"),
        }

    def test_a_profile_with_no_home_currency_gets_no_mixed_total(
        self, db: Database
    ) -> None:
        """Nothing names a target, so there is no currency to total into.

        Requirement 9 forbids substituting 'USD' for an unset home currency —
        that would relabel a foreign-currency profile's portfolio.
        """
        _seed_read_fixtures(db)
        _seed_rate(db, "EUR", "USD", date(2026, 7, 15), Decimal("1.10"))
        _replace_holdings_view(
            db,
            [
                _priced("sec_1", "USD", "1200.00"),
                _priced("sec_2", "EUR", "900.00"),
            ],
        )
        result = db_service(db).holdings()
        assert result.total_market_value is None
        assert result.total_market_value_currency is None

    def test_account_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(account_id="acct_brokerage", security_id="sec_1"),
                _Holding(
                    account_id="acct_roth",
                    security_id="sec_2",
                    quantity="20",
                    cost_basis="2000.00",
                ),
            ],
        )
        result = db_service(db).holdings(account_ref="acct_roth")
        assert [(r.account_id, r.security_id) for r in result.rows] == [
            ("acct_roth", "sec_2")
        ]

    def test_security_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(
            db,
            [
                _Holding(security_id="sec_1"),
                _Holding(security_id="sec_2", quantity="20", cost_basis="2000.00"),
            ],
        )
        result = db_service(db).holdings(security_ref="VTSAX")
        assert [(r.account_id, r.security_id) for r in result.rows] == [
            ("acct_brokerage", "sec_2")
        ]

    def test_unknown_account_ref_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(UserError):
            db_service(db).holdings(account_ref="does-not-exist")


class TestLots:
    """Tests for InvestmentService.lots()."""

    def test_default_open_only(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_open", is_open=True)
        _insert_lot(
            db,
            lot_id="lot_closed",
            is_open=False,
            remaining_quantity=Decimal("0"),
            cost_basis_remaining=Decimal("0"),
        )
        result = db_service(db).lots()
        assert [r.lot_id for r in result.rows] == ["lot_open"]
        assert result.warnings == []

    def test_open_only_false_returns_all(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_open", is_open=True)
        _insert_lot(
            db,
            lot_id="lot_closed",
            is_open=False,
            remaining_quantity=Decimal("0"),
            cost_basis_remaining=Decimal("0"),
        )
        result = db_service(db).lots(open_only=False)
        assert {r.lot_id for r in result.rows} == {"lot_open", "lot_closed"}

    def test_decimal_preserved(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(
            db,
            lot_id="lot_1",
            remaining_quantity=Decimal("6.5"),
            cost_basis_remaining=Decimal("500.25"),
        )
        row = db_service(db).lots().rows[0]
        assert row.remaining_quantity == Decimal("6.5")
        assert row.cost_basis_remaining == Decimal("500.25")
        assert isinstance(row.cost_basis_remaining, Decimal)

    def test_account_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_brokerage", account_id="acct_brokerage")
        _insert_lot(db, lot_id="lot_roth", account_id="acct_roth")
        result = db_service(db).lots(account_ref="acct_roth")
        assert [r.lot_id for r in result.rows] == ["lot_roth"]

    def test_security_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_aapl", security_id="sec_1")
        _insert_lot(db, lot_id="lot_vtsax", security_id="sec_2")
        result = db_service(db).lots(security_ref="VTSAX")
        assert [r.lot_id for r in result.rows] == ["lot_vtsax"]

    def test_unknown_security_ref_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(SecurityResolutionError):
            db_service(db).lots(security_ref="nothing-matches-this")

    def test_basis_incomplete_field_and_warning_present(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_complete", basis_incomplete=False)
        _insert_lot(db, lot_id="lot_incomplete", basis_incomplete=True)
        result = db_service(db).lots()
        by_id = {r.lot_id: r for r in result.rows}
        assert by_id["lot_complete"].basis_incomplete is False
        assert by_id["lot_incomplete"].basis_incomplete is True
        assert len(result.warnings) == 1
        assert "1" in result.warnings[0]
        assert "incomplete" in result.warnings[0]

    def test_no_warning_when_all_lots_complete(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_1", basis_incomplete=False)
        _insert_lot(db, lot_id="lot_2", basis_incomplete=False)
        result = db_service(db).lots()
        assert result.warnings == []

    def test_source_overlap_degrades_the_lots_read(self, db: Database) -> None:
        """Lots derived from two interleaved ledgers say so on the envelope.

        ``dim_holdings`` withholds a market value for exactly this account, and
        the holdings response points the caller here for per-lot basis. Without
        this, the caller lands on the double-counted quantities and basis with
        nothing marking them — a correctly-withheld figure traded for an
        uncaveated wrong one.
        """
        _seed_read_fixtures(db)
        _seed_two_source_ledger(db)
        _insert_lot(db, lot_id="lot_1")

        result = db_service(db).lots()

        assert result.degraded_reason is not None
        assert result.degraded_reason.startswith("investment_source_overlap:")
        assert "1 lot(s)" in result.degraded_reason
        assert result.degraded_reason in result.warnings

    def test_a_single_source_ledger_does_not_degrade_the_lots_read(
        self, db: Database
    ) -> None:
        """One source is one ledger: the flag must mean something when set."""
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="only_buy", source_type="plaid")
        _insert_lot(db, lot_id="lot_1")

        result = db_service(db).lots()

        assert result.degraded_reason is None
        assert result.warnings == []

    def test_lots_from_an_unaffected_account_are_not_degraded(
        self, db: Database
    ) -> None:
        """The flag is scoped to the rows returned, not to the database.

        The overlap is account-scoped, so a read filtered to a clean account
        carries no double-counted row and must not inherit another account's
        fault — a `degraded` that rides along on every read is one no consumer
        can act on.
        """
        _seed_read_fixtures(db)
        _seed_two_source_ledger(db, account_id="acct_brokerage")
        _insert_lot(db, lot_id="lot_roth", account_id="acct_roth")

        result = db_service(db).lots(account_ref="acct_roth")

        assert [r.lot_id for r in result.rows] == ["lot_roth"]
        assert result.degraded_reason is None

    def test_an_opening_bootstrap_is_not_a_second_source_ledger(
        self, db: Database
    ) -> None:
        """A reconstructed pre-window lot re-reports nothing, so it overlaps nothing.

        ``prep.int_plaid__opening_positions`` synthesizes a plaid-sourced
        ``transfer_in`` for the gap the in-window transactions leave. Counting
        its source_type would degrade every broker-covered account that also
        holds a manual entry — and would disagree with ``dim_holdings``, whose
        CTE excludes the same subtype.
        """
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="manual_buy", source_type="manual")
        _insert_event(
            db,
            investment_transaction_id="bootstrap",
            source_type="plaid",
            type_="transfer_in",
            subtype="opening_bootstrap",
        )
        _insert_lot(db, lot_id="lot_1")

        result = db_service(db).lots()

        assert result.degraded_reason is None


class TestUnknownCurrencyIsNotRenderedAsText:
    """An unknown currency reaches every read surface as None, never "None".

    ``core.fct_investment_transactions.currency_code`` is NULL when neither the
    event nor its account names a currency (multi-currency.md Requirement 3 and
    Requirement 8: unknown is segmented, not guessed). Each read mapper coerced
    the column with ``str(...)``, which turns that NULL into the literal token
    ``"None"`` — and ``holdings`` upper-cases it to ``"NONE"`` — so the surface
    that exists to stop a fabricated currency would print one of its own. All
    four mappers are separate limbs; each gets its own test.
    """

    def test_list_events_reports_an_unknown_currency_as_none(
        self, db: Database
    ) -> None:
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="evt_1", currency_code=None)
        row = db_service(db).list_events().rows[0]
        assert row.currency_code is None

    def test_holdings_reports_an_unknown_currency_as_none(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _replace_holdings_view(db, [_Holding(currency_code=None)])
        row = db_service(db).holdings().rows[0]
        assert row.currency_code is None

    def test_lots_reports_an_unknown_currency_as_none(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_lot(db, lot_id="lot_1", currency_code=None)
        row = db_service(db).lots().rows[0]
        assert row.currency_code is None

    def test_gains_reports_an_unknown_currency_as_none(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_1", currency_code=None)
        row = db_service(db).gains().rows[0]
        assert row.currency_code is None


class TestGains:
    """Tests for InvestmentService.gains()."""

    def test_returns_seeded_rows_with_decimal_preserved(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_1", gain_loss=Decimal("200.00"))
        result = db_service(db).gains()
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.gain_loss == Decimal("200.00")
        assert isinstance(row.proceeds, Decimal)
        assert result.warnings == []

    def test_basis_incomplete_warning_present_when_any_row_incomplete(
        self, db: Database
    ) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_complete", basis_incomplete=False)
        _insert_gain(db, realized_gain_id="gain_incomplete", basis_incomplete=True)
        result = db_service(db).gains()
        assert len(result.warnings) == 1
        assert "1" in result.warnings[0]
        assert "incomplete" in result.warnings[0]

    def test_no_warning_when_all_rows_complete(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_1", basis_incomplete=False)
        _insert_gain(db, realized_gain_id="gain_2", basis_incomplete=False)
        result = db_service(db).gains()
        assert result.warnings == []

    def test_term_filter(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_short", term="short")
        _insert_gain(db, realized_gain_id="gain_long", term="long")
        result = db_service(db).gains(term="short")
        assert [r.realized_gain_id for r in result.rows] == ["gain_short"]

    def test_invalid_term_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(ValueError, match="term"):
            db_service(db).gains(term="medium")

    def test_date_range_filter(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_jan", disposal_date=date(2024, 1, 15))
        _insert_gain(db, realized_gain_id="gain_jun", disposal_date=date(2024, 6, 15))
        result = db_service(db).gains(
            date_from=date(2024, 3, 1), date_to=date(2024, 12, 31)
        )
        assert [r.realized_gain_id for r in result.rows] == ["gain_jun"]

    def test_account_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_brokerage", account_id="acct_brokerage")
        _insert_gain(db, realized_gain_id="gain_roth", account_id="acct_roth")
        result = db_service(db).gains(account_ref="acct_roth")
        assert [r.realized_gain_id for r in result.rows] == ["gain_roth"]

    def test_security_ref_resolves_and_filters(self, db: Database) -> None:
        _seed_read_fixtures(db)
        _insert_gain(db, realized_gain_id="gain_aapl", security_id="sec_1")
        _insert_gain(db, realized_gain_id="gain_vtsax", security_id="sec_2")
        result = db_service(db).gains(security_ref="VTSAX")
        assert [r.realized_gain_id for r in result.rows] == ["gain_vtsax"]

    def test_unknown_account_ref_raises(self, db: Database) -> None:
        _seed_read_fixtures(db)
        with pytest.raises(UserError):
            db_service(db).gains(account_ref="does-not-exist")

    def test_source_overlap_degrades_the_gains_read(self, db: Database) -> None:
        """Realized gains from two interleaved ledgers are double-counted too.

        Proceeds, basis and gain all come off the same ledger the holdings
        withhold exists to contain, and this is the surface a user files taxes
        from — the last place a wrong figure should arrive uncaveated.
        """
        _seed_read_fixtures(db)
        _seed_two_source_ledger(db)
        _insert_gain(db, realized_gain_id="gain_1")

        result = db_service(db).gains()

        assert result.degraded_reason is not None
        assert result.degraded_reason.startswith("investment_source_overlap:")
        assert "1 realized-gain row(s)" in result.degraded_reason
        assert result.degraded_reason in result.warnings

    def test_a_single_source_ledger_does_not_degrade_the_gains_read(
        self, db: Database
    ) -> None:
        """One source is one ledger: the flag must mean something when set."""
        _seed_read_fixtures(db)
        _insert_event(db, investment_transaction_id="only_buy", source_type="plaid")
        _insert_gain(db, realized_gain_id="gain_1")

        result = db_service(db).gains()

        assert result.degraded_reason is None
        assert result.warnings == []


def db_service(db: Database) -> InvestmentService:
    return InvestmentService(db)
