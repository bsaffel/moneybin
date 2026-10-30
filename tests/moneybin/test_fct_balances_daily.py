"""Unit tests for the core.fct_balances_daily winner-selection helper.

`_select_winning_observations` reduces multiple balance observations for one
account on one date to a single deterministic winner. Wiring Plaid balances in
gives `plaid` the same precedence tier as `ofx` (both institution snapshots),
so the previously-unreachable same-date tie is now reachable and must resolve
deterministically — freshest observation, then source_type as a final key.
"""

from __future__ import annotations

import importlib.util
import typing as t
from collections.abc import Generator, Iterable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from moneybin.database import SQLMESH_ROOT, Database

_MODEL_PATH = SQLMESH_ROOT / "models" / "core" / "fct_balances_daily.py"
_spec = importlib.util.spec_from_file_location("fct_balances_daily_mod", _MODEL_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_select_winning_observations = _mod._select_winning_observations


def _obs(
    balance_date: date, balance: str, source_type: str, updated_at: str
) -> dict[str, object]:
    return {
        "balance_date": balance_date,
        "balance": Decimal(balance),
        "source_type": source_type,
        "updated_at": pd.Timestamp(updated_at),
    }


def test_precedence_wins_over_freshness() -> None:
    """A higher-precedence source wins even when a lower one is fresher."""
    d = date(2026, 4, 8)
    group = pd.DataFrame([
        _obs(d, "1000.00", "assertion", "2026-04-08T00:00:00"),
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),  # fresher, lower tier
    ])
    winners = _select_winning_observations(group)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "assertion"
    assert winners.loc[0, "balance"] == Decimal("1000.00")


def test_ofx_plaid_tie_broken_by_freshness() -> None:
    """Same date, same precedence tier (ofx vs plaid): the freshest wins."""
    d = date(2026, 4, 8)
    plaid_fresher = pd.DataFrame([
        _obs(d, "500.00", "ofx", "2026-04-08T06:00:00"),
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),
    ])
    winners = _select_winning_observations(plaid_fresher)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "plaid"
    assert winners.loc[0, "balance"] == Decimal("1234.56")

    # Reverse the timestamps: ofx now fresher → ofx wins. Deterministic either way.
    ofx_fresher = pd.DataFrame([
        _obs(d, "500.00", "ofx", "2026-04-08T18:00:00"),
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),
    ])
    winners = _select_winning_observations(ofx_fresher)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "ofx"
    assert winners.loc[0, "balance"] == Decimal("500.00")


def test_equal_timestamp_tie_broken_by_source_type() -> None:
    """Identical precedence and timestamp fall back to source_type ascending."""
    d = date(2026, 4, 8)
    group = pd.DataFrame([
        _obs(d, "1234.56", "plaid", "2026-04-08T12:00:00"),
        _obs(d, "500.00", "ofx", "2026-04-08T12:00:00"),
    ])
    winners = _select_winning_observations(group)
    assert len(winners) == 1
    assert winners.loc[0, "source_type"] == "ofx"  # 'ofx' < 'plaid'


def test_distinct_dates_all_survive() -> None:
    """Observations on different dates are not collapsed."""
    group = pd.DataFrame([
        _obs(date(2026, 4, 8), "1234.56", "plaid", "2026-04-08T12:00:00"),
        _obs(date(2026, 4, 9), "1300.00", "plaid", "2026-04-09T12:00:00"),
    ])
    winners = _select_winning_observations(group)
    assert len(winners) == 2
    assert set(winners["balance_date"]) == {date(2026, 4, 8), date(2026, 4, 9)}


def test_simplefin_snapshot_precedes_tabular_but_not_user_assertion() -> None:
    d = date(2026, 9, 20)
    observations = [
        _obs(d, "25.00", "simplefin", "2026-09-20T06:00:00"),
        _obs(d, "50.00", "tabular", "2026-09-20T12:00:00"),
    ]
    winners = _select_winning_observations(pd.DataFrame(observations))
    assert winners.loc[0, "balance"] == Decimal("25.00")
    observations.append(_obs(d, "-100.00", "assertion", "2026-09-20T00:00:00"))
    winners = _select_winning_observations(pd.DataFrame(observations))
    assert winners.loc[0, "balance"] == Decimal("-100.00")


_build_spine_rows = _mod._build_spine_rows
_evidence_sql = _mod._evidence_sql

_OBS_COLUMNS = [
    "account_id",
    "balance_date",
    "balance",
    "source_type",
    "updated_at",
    "currency_code",
]
_TXN_COLUMNS = ["account_id", "d", "currency_code", "net_amount"]
_EVIDENCE_COLUMNS = ["account_id", "first_date", "last_date"]
_ACCOUNT_COLUMNS = [
    "account_id",
    "include_in_net_worth",
    "archived",
    "archived_at",
    "currency_code",
]


def _ts(day: str) -> pd.Timestamp:
    """A DATE cell as fetchdf() returns it."""
    # The stubs type the constructor as Timestamp | NaTType; a literal day is never NaT.
    return t.cast(pd.Timestamp, pd.Timestamp(day))


def _observation(
    account_id: str, day: str, balance: str, currency: str | None = "USD"
) -> tuple[object, ...]:
    return (account_id, _ts(day), balance, "ofx", _ts(day), currency)


def _evidence(account_id: str, first: str, last: str) -> tuple[object, ...]:
    return (account_id, _ts(first), _ts(last))


def _dim(
    account_id: str,
    *,
    include: bool = True,
    archived: bool = False,
    archived_at: str | None = None,
    currency: str | None = "USD",
) -> tuple[object, ...]:
    return (
        account_id,
        include,
        archived,
        _ts(archived_at) if archived_at else pd.NaT,
        currency,
    )


def _spine(
    *,
    obs: Iterable[tuple[object, ...]] = (),
    txns: Iterable[tuple[object, ...]] = (),
    evidence: Iterable[tuple[object, ...]] = (),
    accounts: Iterable[tuple[object, ...]] = (),
) -> list[dict[str, object]]:
    return _build_spine_rows(
        pd.DataFrame(list(obs), columns=t.cast(t.Any, _OBS_COLUMNS)),
        pd.DataFrame(list(txns), columns=t.cast(t.Any, _TXN_COLUMNS)),
        pd.DataFrame(list(evidence), columns=t.cast(t.Any, _EVIDENCE_COLUMNS)),
        pd.DataFrame(list(accounts), columns=t.cast(t.Any, _ACCOUNT_COLUMNS)),
    )


def _rows_for(
    rows: list[dict[str, object]], account_id: str
) -> list[dict[str, object]]:
    return sorted(
        (r for r in rows if r["account_id"] == account_id),
        key=lambda r: t.cast(date, r["balance_date"]),
    )


def test_unanchored_account_gets_null_rows_from_first_evidence_to_spine_end() -> None:
    rows = _spine(
        obs=[_observation("chk", "2026-01-01", "100.00")],
        evidence=[_evidence("csh", "2026-01-03", "2026-01-05")],
        accounts=[_dim("chk"), _dim("csh", currency="EUR")],
    )
    csh = _rows_for(rows, "csh")
    assert [r["balance_date"] for r in csh] == [
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
    ]
    for r in csh:
        assert r["balance"] is None
        assert r["is_observed"] is False
        assert r["observation_source"] is None
        assert r["reconciliation_delta"] is None
        assert r["currency_code"] == "EUR"


def test_unanchored_rows_take_the_account_currency_even_when_null() -> None:
    rows = _spine(
        obs=[_observation("chk", "2026-01-01", "100.00", currency="USD")],
        evidence=[_evidence("csh", "2026-01-01", "2026-01-01")],
        accounts=[_dim("chk"), _dim("csh", currency=None)],
    )
    (csh,) = _rows_for(rows, "csh")
    assert csh["currency_code"] is None


def test_anchored_account_keeps_its_first_observation_as_its_start() -> None:
    """Evidence before the first statement adds no NULL rows (spec §Out of Scope)."""
    rows = _spine(
        obs=[_observation("chk", "2026-01-03", "100.00")],
        evidence=[_evidence("chk", "2025-12-20", "2026-01-03")],
        accounts=[_dim("chk")],
    )
    chk = _rows_for(rows, "chk")
    assert chk[0]["balance_date"] == date(2026, 1, 3)
    assert all(r["balance"] is not None for r in chk)


def test_a_transaction_after_the_newest_observation_moves_the_spine_end() -> None:
    """The carry continues to the newest evidence and applies that transaction."""
    rows = _spine(
        obs=[_observation("chk", "2026-01-01", "100.00")],
        txns=[("chk", _ts("2026-01-03"), "USD", -25.0)],
        evidence=[_evidence("chk", "2026-01-03", "2026-01-03")],
        accounts=[_dim("chk")],
    )
    chk = _rows_for(rows, "chk")
    assert [(r["balance_date"], r["balance"]) for r in chk] == [
        (date(2026, 1, 1), Decimal("100.00")),
        (date(2026, 1, 2), Decimal("100.00")),
        (date(2026, 1, 3), Decimal("75.00")),
    ]


def test_every_account_runs_to_the_same_spine_end() -> None:
    rows = _spine(
        obs=[
            _observation("a", "2026-01-01", "1.00"),
            _observation("b", "2026-01-04", "2.00"),
        ],
        accounts=[_dim("a"), _dim("b")],
    )
    assert _rows_for(rows, "a")[-1]["balance_date"] == date(2026, 1, 4)
    assert _rows_for(rows, "b")[-1]["balance_date"] == date(2026, 1, 4)


def test_a_transactions_only_profile_still_produces_rows() -> None:
    """No observation anywhere: an observation-only end would yield nothing."""
    rows = _spine(
        evidence=[_evidence("csv", "2026-02-01", "2026-02-02")],
        accounts=[_dim("csv")],
    )
    assert [r["balance_date"] for r in _rows_for(rows, "csv")] == [
        date(2026, 2, 1),
        date(2026, 2, 2),
    ]


def test_an_account_with_no_evidence_gets_no_rows() -> None:
    """Requirement 14's residual gap: never funded and not yet observed look alike."""
    rows = _spine(
        obs=[_observation("chk", "2026-01-01", "100.00")],
        accounts=[_dim("chk"), _dim("dormant")],
    )
    assert _rows_for(rows, "dormant") == []


def test_an_empty_profile_builds_no_rows() -> None:
    assert _spine(accounts=[_dim("dormant")]) == []


@pytest.mark.parametrize(
    ("dim", "expected"),
    [
        (_dim("x"), [True, True, True]),
        (_dim("x", include=False), [False, False, False]),
        (_dim("x", archived=True, archived_at="2026-01-02"), [True, True, False]),
        (_dim("x", archived=True), [False, False, False]),
        (None, [False, False, False]),
    ],
    ids=["active", "excluded", "archived-dated", "archived-undated", "orphan"],
)
def test_in_net_worth_per_row(
    dim: tuple[object, ...] | None, expected: list[bool]
) -> None:
    rows = _spine(
        evidence=[_evidence("x", "2026-01-01", "2026-01-03")],
        accounts=[dim] if dim is not None else [],
    )
    assert [r["in_net_worth"] for r in _rows_for(rows, "x")] == expected


def test_in_net_worth_stamps_anchored_rows_too() -> None:
    rows = _spine(
        obs=[_observation("chk", "2026-01-01", "100.00")],
        evidence=[_evidence("chk", "2026-01-03", "2026-01-03")],
        accounts=[_dim("chk", archived=True, archived_at="2026-01-01")],
    )
    assert [r["in_net_worth"] for r in _rows_for(rows, "chk")] == [True, False, False]


@pytest.fixture()
def evidence_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    database = Database(
        tmp_path / "evidence.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    database.execute("CREATE TABLE t_txn (account_id VARCHAR, transaction_date DATE)")
    database.execute("CREATE TABLE t_inv (account_id VARCHAR, trade_date DATE)")
    database.execute(
        "CREATE TABLE t_broker (account_id VARCHAR, has_position BOOLEAN, as_of DATE)"
    )
    yield database
    database.close()


def _evidence_rows(db: Database) -> dict[str, tuple[date, date]]:
    rows = db.execute(_evidence_sql("t_txn", "t_inv", "t_broker")).fetchall()
    return {str(r[0]): (r[1], r[2]) for r in rows}


def test_evidence_each_kind_alone_is_evidence(evidence_db: Database) -> None:
    evidence_db.execute("INSERT INTO t_txn VALUES ('cash', '2026-01-03')")
    evidence_db.execute("INSERT INTO t_inv VALUES ('dividend_only', '2026-01-04')")
    evidence_db.execute("INSERT INTO t_broker VALUES ('broker', TRUE, '2026-01-05')")
    assert _evidence_rows(evidence_db) == {
        "cash": (date(2026, 1, 3), date(2026, 1, 3)),
        "dividend_only": (date(2026, 1, 4), date(2026, 1, 4)),
        "broker": (date(2026, 1, 5), date(2026, 1, 5)),
    }


def test_evidence_a_broker_zero_or_null_is_not_evidence(evidence_db: Database) -> None:
    """A liquidated account still has its ledger; the zero snapshot adds nothing."""
    evidence_db.execute("INSERT INTO t_broker VALUES ('zero', FALSE, '2026-01-05')")
    evidence_db.execute("INSERT INTO t_broker VALUES ('unknown', NULL, '2026-01-05')")
    evidence_db.execute("INSERT INTO t_inv VALUES ('liquidated', '2025-06-01')")
    evidence_db.execute(
        "INSERT INTO t_broker VALUES ('liquidated', FALSE, '2026-01-05')"
    )
    assert _evidence_rows(evidence_db) == {
        "liquidated": (date(2025, 6, 1), date(2025, 6, 1)),
    }


def test_evidence_spans_first_to_last_across_kinds(evidence_db: Database) -> None:
    evidence_db.execute("INSERT INTO t_txn VALUES ('acct', '2026-01-10')")
    evidence_db.execute("INSERT INTO t_inv VALUES ('acct', '2026-01-02')")
    evidence_db.execute("INSERT INTO t_broker VALUES ('acct', TRUE, '2026-01-20')")
    assert _evidence_rows(evidence_db) == {
        "acct": (date(2026, 1, 2), date(2026, 1, 20)),
    }


def test_evidence_drops_a_null_account_id(evidence_db: Database) -> None:
    evidence_db.execute("INSERT INTO t_txn VALUES (NULL, '2026-01-03')")
    assert _evidence_rows(evidence_db) == {}


@pytest.mark.parametrize(
    ("balance", "amount", "expected"),
    [
        ("999999999999999.91", "0.02", "999999999999999.93"),
        ("1.00", "999999999999999.91", "1000000000000000.91"),
    ],
)
def test_execute_preserves_cents_across_pandas_boundary(
    evidence_db: Database, balance: str, amount: str, expected: str
) -> None:
    """The full DECIMAL(18,2) range survives observations and daily net amounts."""
    db = evidence_db
    db.execute("ALTER TABLE t_txn ADD COLUMN currency_code VARCHAR")
    db.execute("ALTER TABLE t_txn ADD COLUMN amount DECIMAL(18,2)")
    db.execute("INSERT INTO t_txn VALUES ('cash', '2026-01-02', 'USD', ?)", [amount])
    db.execute(
        """
        CREATE TABLE t_obs AS SELECT 'cash' AS account_id,
            DATE '2026-01-01' AS balance_date,
            ?::DECIMAL(18,2) AS balance,
            'ofx' AS source_type, TIMESTAMP '2026-01-01' AS updated_at,
            'USD' AS currency_code, 'file' AS source_ref
        """,
        [balance],
    )
    db.execute("""
        CREATE TABLE t_sf (account_id VARCHAR, balance_date DATE, source_file VARCHAR,
            loaded_at TIMESTAMP)
        """)
    db.execute("""
        CREATE TABLE t_accounts AS SELECT 'cash' AS account_id,
            TRUE AS include_in_net_worth, FALSE AS archived,
            NULL::TIMESTAMP AS archived_at, 'USD' AS currency_code
        """)
    tables = {
        "core.fct_balances": "t_obs",
        "prep.stg_simplefin__balances": "t_sf",
        "core.fct_transactions": "t_txn",
        "core.fct_investment_transactions": "t_inv",
        "core.dim_holdings_broker_reported": "t_broker",
        "prep.int_accounts__merged": "t_accounts",
    }

    def fetchdf(sql: str) -> pd.DataFrame:
        return db.execute(sql).fetchdf()

    context = SimpleNamespace(resolve_table=tables.__getitem__, fetchdf=fetchdf)
    frame = next(_mod.execute(context, datetime.min, datetime.min, datetime.min))
    assert list(frame["balance"]) == [
        Decimal(balance),
        Decimal(expected),
    ]
