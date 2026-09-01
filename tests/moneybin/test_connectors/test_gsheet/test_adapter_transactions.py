"""Tests for the gsheet TransactionsAdapter."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from moneybin.connectors.gsheet.adapters.base import GSheetConnection
from moneybin.connectors.gsheet.adapters.transactions import TransactionsAdapter
from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables

FIXTURES = Path(__file__).parent / "fixtures"


def _seed_dim_account(db: Database, *, account_id: str, display_name: str) -> None:
    """Insert a minimal, person-named ``core.dim_accounts`` row.

    Mirrors ``test_account_resolver.py``'s ``_seed_dim_account`` (kept local
    here rather than imported cross-module): ``display_name_is_user_set``
    defaults ``True`` because every candidate this file seeds stands in for
    an account a person actually named.
    """
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name, "
        "display_name_is_user_set) VALUES (?, ?, TRUE)",  # noqa: S608  # test fixture insert
        [account_id, display_name],
    )


def load_fixture(name: str) -> dict[str, Any]:
    return yaml.safe_load((FIXTURES / name).read_text())


def df_from_fixture(fix: dict[str, Any]) -> pl.DataFrame:
    headers = fix["sheet"]["headers"]
    rows = fix["sheet"]["rows"]
    return pl.DataFrame({h: [r[i] for r in rows] for i, h in enumerate(headers)})


def test_detect_tiller_basic_returns_high_confidence() -> None:
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    result = adapter.detect(df, account_name=fix["account_name"])
    assert result.confidence == "high"
    assert result.column_mapping["Date"] == "transaction_date"
    assert result.column_mapping["Amount"] == "amount"
    assert result.column_mapping["Description"] == "description"


def test_detect_includes_pinned_signature_in_order() -> None:
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    result = adapter.detect(df, account_name=fix["account_name"])
    assert result.header_signature == fix["sheet"]["headers"]


def test_load_inserts_rows_first_time(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    transformed = adapter.transform(df, sample_connection, in_memory_db)
    result = adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp1"
    )
    assert result.rows_inserted == 2
    assert result.rows_soft_deleted == 0


def test_load_soft_deletes_missing_rows(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """First pull inserts 2; second pull omits one → soft-delete."""
    adapter = TransactionsAdapter()
    fix = load_fixture("tiller_basic.yaml")
    df1 = df_from_fixture(fix)
    transformed1 = adapter.transform(df1, sample_connection, in_memory_db)
    adapter.load(transformed1, sample_connection, in_memory_db, import_id="imp1")

    # Second pull: drop the Salary row
    fix2 = dict(fix)
    fix2["sheet"] = dict(fix["sheet"])
    fix2["sheet"]["rows"] = [fix["sheet"]["rows"][0]]  # only Whole Foods
    df2 = df_from_fixture(fix2)
    transformed2 = adapter.transform(df2, sample_connection, in_memory_db)
    result = adapter.load(
        transformed2, sample_connection, in_memory_db, import_id="imp2"
    )

    assert result.rows_soft_deleted == 1

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions "
        "WHERE source_origin = ? AND deleted_from_source_at IS NOT NULL",
        [sample_connection.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 1


def test_load_undeletes_returning_row(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Row deleted-then-readded gets deleted_from_source_at reset to NULL."""
    adapter = TransactionsAdapter()
    fix = load_fixture("tiller_basic.yaml")
    df1 = df_from_fixture(fix)
    adapter.load(
        adapter.transform(df1, sample_connection, in_memory_db),
        sample_connection,
        in_memory_db,
        "imp1",
    )

    fix2 = dict(fix)
    fix2["sheet"] = dict(fix["sheet"])
    fix2["sheet"]["rows"] = [fix["sheet"]["rows"][0]]
    df2 = df_from_fixture(fix2)
    adapter.load(
        adapter.transform(df2, sample_connection, in_memory_db),
        sample_connection,
        in_memory_db,
        "imp2",
    )

    df3 = df_from_fixture(fix)
    adapter.load(
        adapter.transform(df3, sample_connection, in_memory_db),
        sample_connection,
        in_memory_db,
        "imp3",
    )

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions "
        "WHERE source_origin = ? AND deleted_from_source_at IS NULL",
        [sample_connection.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 2


def test_detect_low_confidence_on_unrecognized_columns() -> None:
    """Unrecognized headers produce non-high confidence."""
    adapter = TransactionsAdapter()
    df = pl.DataFrame({
        "Col_X": ["x1", "x2"],
        "Col_Y": ["y1", "y2"],
        "Col_Z": ["z1", "z2"],
    })
    result = adapter.detect(df, account_name=None)
    # Confidence should be "low" — no date/amount/description matched.
    assert result.confidence == "low"


def test_check_drift_passes_pinned_signature_through(
    sample_connection: GSheetConnection,
) -> None:
    """Drift report identifies a missing pinned header."""
    adapter = TransactionsAdapter()
    # Current headers drop "Amount" — should appear as missing.
    current_headers = ["Date", "Description", "Category", "Account", "Tags"]
    sample = pl.DataFrame({h: ["v1", "v2"] for h in current_headers})
    report = adapter.check_drift(sample_connection, sample)
    assert report.is_drift is True
    assert "Amount" in report.missing_headers


def test_check_drift_ignores_blank_optional_column(
    sample_connection: GSheetConnection,
) -> None:
    """A mostly-blank OPTIONAL column (Description) must NOT trigger drift.

    Real financial exports routinely leave descriptions/notes blank; flagging
    that as drift would pin the connection in drift_detected forever.
    """
    adapter = TransactionsAdapter()
    sig = sample_connection.header_signature
    sample = pl.DataFrame({h: ["x", "y", "z"] for h in sig})
    # Blank out the optional Description column entirely.
    sample = sample.with_columns(pl.lit("").alias("Description"))
    report = adapter.check_drift(sample_connection, sample)
    assert report.is_drift is False
    assert report.empty_mapped_columns == []


def test_check_drift_flags_blank_required_column(
    sample_connection: GSheetConnection,
) -> None:
    """A mostly-blank REQUIRED column (Amount) still triggers drift."""
    adapter = TransactionsAdapter()
    sig = sample_connection.header_signature
    sample = pl.DataFrame({h: ["x", "y", "z"] for h in sig})
    sample = sample.with_columns(pl.lit("").alias("Amount"))
    report = adapter.check_drift(sample_connection, sample)
    assert report.is_drift is True
    assert "Amount" in report.empty_mapped_columns


def test_transform_applies_sign_convention(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Negative input under negative_is_expense convention stays negative."""
    fix = load_fixture("tiller_basic.yaml")
    adapter = TransactionsAdapter()
    df = df_from_fixture(fix)
    transformed = adapter.transform(df, sample_connection, in_memory_db)
    amounts = transformed["amount"].to_list()
    # Whole Foods is -87.42; Salary is +5000.00 under negative_is_expense.
    assert Decimal("-87.42") in amounts
    assert Decimal("5000.00") in amounts


def test_load_with_empty_df_is_no_op(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Empty df → LoadResult(0,0,0) and does not raise on IN ()."""
    adapter = TransactionsAdapter()
    empty_df = pl.DataFrame(
        {h: [] for h in sample_connection.header_signature},
        schema=dict.fromkeys(sample_connection.header_signature, pl.Utf8),
    )
    transformed = adapter.transform(empty_df, sample_connection, in_memory_db)
    result = adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp_empty"
    )
    assert result.rows_inserted == 0
    assert result.rows_soft_deleted == 0
    assert result.rows_upserted == 0


def test_load_idempotent_when_called_twice_same_data(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Calling load twice with same df → second call: no soft-deletes; same total rows."""
    adapter = TransactionsAdapter()
    fix = load_fixture("tiller_basic.yaml")
    df = df_from_fixture(fix)
    transformed = adapter.transform(df, sample_connection, in_memory_db)
    adapter.load(transformed, sample_connection, in_memory_db, import_id="imp1")
    result = adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp2"
    )
    assert result.rows_soft_deleted == 0

    row = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_origin = ?",
        [sample_connection.connection_id],
    ).fetchone()
    assert row is not None
    assert row[0] == 2


def _multi_account_df() -> pl.DataFrame:
    """Two accounts interleaved in one tab — the shape a Tiller export produces."""
    return pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-01-17"],
        "Description": ["Coffee", "Salary", "Card payment"],
        "Category": ["Dining", "Income", "Transfer"],
        "Amount": ["-4.50", "5000.00", "-120.00"],
        "Account": ["Everyday Checking", "Everyday Checking", "Rewards Card"],
        "Tags": ["", "", ""],
    })


def _unbound(connection: GSheetConnection) -> GSheetConnection:
    """The same pinned mapping with no single destination account bound."""
    return replace(connection, account_id=None, account_name=None)


def test_transform_keys_each_row_by_its_own_account_when_unbound(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """An unbound multi-account connection keys each row by its own account."""
    adapter = TransactionsAdapter()

    transformed = adapter.transform(
        _multi_account_df(), _unbound(sample_connection), in_memory_db
    )

    assert transformed["account_id"].to_list() == [
        "everyday-checking",
        "everyday-checking",
        "rewards-card",
    ]


def test_load_registers_one_account_row_per_distinct_sheet_account(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Each account the sheet names is registered once, under its own label."""
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    df = _multi_account_df()

    transformed = adapter.transform(df, conn, in_memory_db)
    adapter.load(transformed, conn, in_memory_db, import_id="imp1", source_df=df)

    rows = in_memory_db.execute(
        "SELECT account_id, account_name FROM raw.tabular_accounts "
        "WHERE source_origin = ? ORDER BY account_id",
        [conn.connection_id],
    ).fetchall()
    assert rows == [
        ("everyday-checking", "Everyday Checking"),
        ("rewards-card", "Rewards Card"),
    ]


def test_load_does_not_register_accounts_for_a_bound_connection(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A connection bound to one account keeps its existing single-account shape."""
    adapter = TransactionsAdapter()
    df = _multi_account_df()

    transformed = adapter.transform(df, sample_connection, in_memory_db)
    adapter.load(
        transformed, sample_connection, in_memory_db, import_id="imp1", source_df=df
    )

    rows = in_memory_db.execute(
        "SELECT COUNT(*) FROM raw.tabular_accounts WHERE source_origin = ?",
        [sample_connection.connection_id],
    ).fetchone()
    assert rows is not None
    assert rows[0] == 0


def _blank_account_df() -> pl.DataFrame:
    """A named account beside cells the sheet left empty.

    The Sheets API stringifies every cell, so an interior blank arrives as
    ``""`` rather than NULL — the shape a CSV never produces.
    """
    return pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-01-17"],
        "Description": ["Coffee", "Salary", "Card payment"],
        "Category": ["Dining", "Income", "Transfer"],
        "Amount": ["-4.50", "5000.00", "-120.00"],
        "Account": ["Everyday Checking", "", "   "],
        "Tags": ["", "", ""],
    })


def test_blank_account_cells_never_key_a_row_to_the_empty_string(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A blank cell groups under the filler key, never under ``""``.

    An empty key is not a key: every account that produced one would land on
    the same source-native coordinates and be merged into a single account.
    """
    adapter = TransactionsAdapter()

    transformed = adapter.transform(
        _blank_account_df(), _unbound(sample_connection), in_memory_db
    )

    assert transformed["account_id"].to_list() == [
        "everyday-checking",
        "unknown",
        "unknown",
    ]


def test_accounts_named_in_a_non_latin_script_stay_distinct(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Two differently-named accounts never collapse onto one key.

    ``slugify`` keeps only ``[a-z0-9]``, so every label written in a non-Latin
    script reduces to ``""`` and silently merges with the next one.
    """
    df = _multi_account_df().with_columns(
        pl.Series("Account", ["貯金口座", "普通預金", "貯金口座"])
    )
    adapter = TransactionsAdapter()

    keys = adapter.transform(df, _unbound(sample_connection), in_memory_db)[
        "account_id"
    ].to_list()

    assert "" not in keys
    assert keys[0] == keys[2]
    assert keys[0] != keys[1]


def test_a_filler_label_is_not_recorded_as_an_authored_account_label(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """``account_label`` carries only a name a person actually wrote.

    The filler standing in for a blank cell reads exactly like a typed name,
    and an account labelled ``unknown`` is what the tabular path avoids.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    df = _blank_account_df()

    transformed = adapter.transform(df, conn, in_memory_db)
    adapter.load(transformed, conn, in_memory_db, import_id="imp1", source_df=df)

    rows = in_memory_db.execute(
        "SELECT account_id, account_label FROM raw.tabular_accounts "
        "WHERE source_origin = ? ORDER BY account_id",
        [conn.connection_id],
    ).fetchall()
    assert rows == [
        ("everyday-checking", "Everyday Checking"),
        ("unknown", None),
    ]


def test_an_authored_name_outranks_a_filler_that_reached_the_key_first(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A blank cell and a typed "Unknown" share a key; the typed name wins.

    First-label-wins would otherwise record the synthesized filler as the
    account's authored label — the exact promotion the filler is flagged to
    prevent — purely because the blank row came first.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    df = _multi_account_df().with_columns(
        pl.Series("Account", ["", "Unknown", "Unknown"])
    )

    transformed = adapter.transform(df, conn, in_memory_db)
    adapter.load(transformed, conn, in_memory_db, import_id="imp1", source_df=df)

    rows = in_memory_db.execute(
        "SELECT account_id, account_name, account_label "
        "FROM raw.tabular_accounts WHERE source_origin = ?",
        [conn.connection_id],
    ).fetchall()
    assert rows == [("unknown", "Unknown", "Unknown")]


def test_account_name_is_user_set_gates_the_resolvers_name_signal(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """``_link_sheet_accounts`` threads ``account_name_is_user_set`` correctly.

    "Everyday Checking" is text a person typed in the sheet's Account column;
    a blank cell's filler ("unknown") is not, even though it reads exactly
    like a name. Seed a real, person-set candidate account under each label
    and confirm the resolver's weak "name" rung proposes a merge candidate
    only for the authored one -- mirrors
    ``test_a_generated_source_name_is_not_a_name_match`` (test_account_resolver.py)
    at the source side, exercised through the real gsheet ``load()`` wiring
    rather than a directly constructed ``SourceAccount``.
    """
    create_core_tables(in_memory_db)
    _seed_dim_account(
        in_memory_db, account_id="acct_authored", display_name="Everyday Checking"
    )
    _seed_dim_account(in_memory_db, account_id="acct_filler", display_name="unknown")

    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    df = _blank_account_df()

    transformed = adapter.transform(df, conn, in_memory_db)
    adapter.load(transformed, conn, in_memory_db, import_id="imp1", source_df=df)

    # The resolver's separate, weaker "fallback" rung also proposes the
    # unnamed sheet account against every unremarkable existing account
    # regardless of this fix -- orthogonal noise this assertion excludes by
    # scoping to the "name" signal specifically, which is the one
    # account_name_is_user_set gates.
    name_candidates = in_memory_db.execute(
        "SELECT candidate_account_id FROM app.account_link_decisions "
        "WHERE status = 'pending' "
        "AND json_extract_string(match_signals, '$.signal') = 'name'"
    ).fetchall()
    assert name_candidates == [("acct_authored",)]


def test_load_does_not_mint_an_account_for_a_row_the_transform_dropped(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A trailing summary row must not create an account owning no transactions.

    ``transform_dataframe`` drops rows whose date or amount will not parse, so
    registering straight off the raw pull mints an account the ledger never
    references.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    df = pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", ""],
        "Description": ["Coffee", "Salary", "TOTAL"],
        "Category": ["Dining", "Income", ""],
        "Amount": ["-4.50", "5000.00", ""],
        "Account": ["Everyday Checking", "Everyday Checking", ""],
        "Tags": ["", "", ""],
    })

    transformed = adapter.transform(df, conn, in_memory_db)
    adapter.load(transformed, conn, in_memory_db, import_id="imp1", source_df=df)

    rows = in_memory_db.execute(
        "SELECT account_id FROM raw.tabular_accounts WHERE source_origin = ?",
        [conn.connection_id],
    ).fetchall()
    assert rows == [("everyday-checking",)]


def test_check_drift_flags_a_blanked_account_column_when_unbound(
    sample_connection: GSheetConnection,
) -> None:
    """For an unbound connection the account column is load-bearing.

    A deleted header is caught by the pinned signature; blanked *values* are
    not. Every row would land under the filler key, silently re-parenting the
    whole ledger onto one nameless account.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    sample = pl.DataFrame({h: ["x", "y", "z"] for h in conn.header_signature})
    sample = sample.with_columns(pl.lit("").alias("Account"))

    report = adapter.check_drift(conn, sample)

    assert report.is_drift is True
    assert "Account" in report.empty_mapped_columns


def test_check_drift_ignores_a_blanked_account_column_when_bound(
    sample_connection: GSheetConnection,
) -> None:
    """A bound connection keys no row by that column, so blanking it is not drift."""
    adapter = TransactionsAdapter()
    sample = pl.DataFrame({
        h: ["x", "y", "z"] for h in sample_connection.header_signature
    })
    sample = sample.with_columns(pl.lit("").alias("Account"))

    report = adapter.check_drift(sample_connection, sample)

    assert report.is_drift is False


def _account_history_df() -> pl.DataFrame:
    """Two accounts carrying enough history each to evidence a rename.

    A rename is recognised from the transactions the label keeps, so each
    account needs more rows than the coincidence floor. ``_multi_account_df``
    gives one account a single row, which is deliberately below it.
    """
    return pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-01-17", "2026-01-18"],
        "Description": ["Coffee", "Salary", "Card payment", "Groceries"],
        "Category": ["Dining", "Income", "Transfer", "Groceries"],
        "Amount": ["-4.50", "5000.00", "-120.00", "-62.10"],
        "Account": [
            "Everyday Checking",
            "Everyday Checking",
            "Rewards Card",
            "Rewards Card",
        ],
        "Tags": ["", "", "", ""],
    })


def _renamed_account_df(old: str, new: str) -> pl.DataFrame:
    """The two-account history with one account's label rewritten."""
    df = _account_history_df()
    return df.with_columns(
        pl.Series("Account", [new if a == old else a for a in df["Account"].to_list()])
    )


def test_a_renamed_account_keeps_the_key_its_transactions_already_use(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A rename re-labels the account instead of rotating every transaction_id.

    ``transaction_id`` folds the account key, so recomputing that key from the
    current label would soft-delete every row the account already owns and
    re-insert copies under a new id — orphaning the notes and splits keyed to
    the old ids. The key the account already answers to is what stays.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    first = _account_history_df()
    transformed_first = adapter.transform(first, conn, in_memory_db)
    adapter.load(transformed_first, conn, in_memory_db, "imp1", source_df=first)
    ids_before = set(transformed_first["transaction_id"].to_list())

    second = _renamed_account_df("Rewards Card", "Travel Rewards Card")
    transformed_second = adapter.transform(second, conn, in_memory_db)
    result = adapter.load(
        transformed_second, conn, in_memory_db, "imp2", source_df=second
    )

    assert set(transformed_second["transaction_id"].to_list()) == ids_before
    assert result.rows_soft_deleted == 0
    assert transformed_second["account_id"].to_list() == [
        "everyday-checking",
        "everyday-checking",
        "rewards-card",
        "rewards-card",
    ]


def test_a_renamed_account_is_relabelled_rather_than_twinned(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The registered account row carries the new label under the old key."""
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    first = _account_history_df()
    adapter.load(
        adapter.transform(first, conn, in_memory_db),
        conn,
        in_memory_db,
        "imp1",
        source_df=first,
    )

    second = _renamed_account_df("Rewards Card", "Travel Rewards Card")
    adapter.load(
        adapter.transform(second, conn, in_memory_db),
        conn,
        in_memory_db,
        "imp2",
        source_df=second,
    )

    rows = in_memory_db.execute(
        "SELECT account_id, account_name FROM raw.tabular_accounts "
        "WHERE source_origin = ? ORDER BY account_id",
        [conn.connection_id],
    ).fetchall()
    assert rows == [
        ("everyday-checking", "Everyday Checking"),
        ("rewards-card", "Travel Rewards Card"),
    ]


def test_a_remembered_rename_survives_the_pull_after_it(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The adopted key holds on later pulls; only the first one sees a rename."""
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    first = _account_history_df()
    adapter.load(
        adapter.transform(first, conn, in_memory_db),
        conn,
        in_memory_db,
        "imp1",
        source_df=first,
    )
    second = _renamed_account_df("Rewards Card", "Travel Rewards Card")
    adapter.load(
        adapter.transform(second, conn, in_memory_db),
        conn,
        in_memory_db,
        "imp2",
        source_df=second,
    )

    third = _renamed_account_df("Rewards Card", "Travel Rewards Card")
    transformed_third = adapter.transform(third, conn, in_memory_db)
    result = adapter.load(
        transformed_third, conn, in_memory_db, "imp3", source_df=third
    )

    assert transformed_third["account_id"].to_list() == [
        "everyday-checking",
        "everyday-checking",
        "rewards-card",
        "rewards-card",
    ]
    assert result.rows_soft_deleted == 0


def _loaded(
    adapter: TransactionsAdapter,
    conn: GSheetConnection,
    db: Database,
    df: pl.DataFrame,
    import_id: str,
) -> None:
    """Transform and load one pull of ``df`` through ``conn``."""
    adapter.load(adapter.transform(df, conn, db), conn, db, import_id, source_df=df)


def test_two_accounts_renamed_in_one_pull_each_keep_their_own_key(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The rows decide the pairing, so two renames at once are not ambiguous.

    Counting labels cannot pair two departures with two arrivals. The
    transactions can: each arriving label carries the history of exactly one
    departed account, so both keys survive without anything being guessed.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")

    both_renamed = _account_history_df().with_columns(
        pl.Series(
            "Account",
            [
                "Daily Spending",
                "Daily Spending",
                "Travel Rewards Card",
                "Travel Rewards Card",
            ],
        )
    )
    keys = adapter.transform(both_renamed, conn, in_memory_db)["account_id"].to_list()

    assert keys == [
        "everyday-checking",
        "everyday-checking",
        "rewards-card",
        "rewards-card",
    ]


def _swapped_account_df() -> pl.DataFrame:
    """One account closed and a different one opened, in a single pull.

    Everyday Checking keeps its rows. The Rewards Card rows are gone and a
    Travel Card nobody has seen takes its place, so exactly one label departs
    as one arrives — the shape a rename also makes. No transaction is shared.
    """
    return pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-02-02", "2026-02-03"],
        "Description": ["Coffee", "Salary", "Flight", "Hotel"],
        "Category": ["Dining", "Income", "Travel", "Travel"],
        "Amount": ["-4.50", "5000.00", "-310.00", "-220.00"],
        "Account": [
            "Everyday Checking",
            "Everyday Checking",
            "Travel Card",
            "Travel Card",
        ],
        "Tags": ["", "", "", ""],
    })


def test_a_closed_account_does_not_hand_its_key_to_a_newly_opened_one(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Closing one account and opening another must not merge the two.

    By label count this is indistinguishable from a rename. Reusing the closed
    account's key would file the new account's transactions under the old one,
    folding two histories into a single account with nothing deleted to make
    the mistake noticeable. The absent shared history separates the cases.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")

    transformed = adapter.transform(_swapped_account_df(), conn, in_memory_db)

    assert transformed["account_id"].to_list() == [
        "everyday-checking",
        "everyday-checking",
        "travel-card",
        "travel-card",
    ]


def test_a_closed_accounts_transactions_stay_on_their_own_account(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The closed account keeps its rows and the new account never adopts them."""
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")
    _loaded(adapter, conn, in_memory_db, _swapped_account_df(), "imp2")

    owners = in_memory_db.execute(
        "SELECT account_id, COUNT(*) FROM raw.tabular_transactions "
        "WHERE source_origin = ? GROUP BY account_id ORDER BY account_id",
        [conn.connection_id],
    ).fetchall()

    assert owners == [
        ("everyday-checking", 2),
        ("rewards-card", 2),
        ("travel-card", 2),
    ]


def test_one_shared_row_is_too_little_evidence_to_reuse_a_key(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A single coincidental match must not merge two accounts.

    Two cards can carry the same date, amount and description on one day. A
    lone shared row is a coincidence rather than a history, so the arriving
    label mints its own key: the cost of being wrong is a visible extra account
    instead of a silent merge.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")

    # "Card payment" is one of the two rows the Rewards Card owns; "Hotel" is new.
    coincidence = pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-01-17", "2026-02-03"],
        "Description": ["Coffee", "Salary", "Card payment", "Hotel"],
        "Category": ["Dining", "Income", "Transfer", "Travel"],
        "Amount": ["-4.50", "5000.00", "-120.00", "-220.00"],
        "Account": [
            "Everyday Checking",
            "Everyday Checking",
            "Travel Card",
            "Travel Card",
        ],
        "Tags": ["", "", "", ""],
    })
    keys = adapter.transform(coincidence, conn, in_memory_db)["account_id"].to_list()

    assert keys[2:] == ["travel-card", "travel-card"]


def test_a_few_shared_rows_cannot_claim_a_long_history(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Shared rows must be most of the departed account, not a handful of it.

    Otherwise the longer an account's history grows, the easier it is for an
    unrelated account to clear a fixed floor of coincidences and inherit it.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    long_history = pl.DataFrame({
        "Date": [f"2026-03-{day:02d}" for day in range(1, 7)],
        "Description": ["Fuel", "Fuel", "Fuel", "Dining", "Dining", "Dining"],
        "Category": ["Auto"] * 3 + ["Dining"] * 3,
        "Amount": ["-30.00", "-31.00", "-32.00", "-40.00", "-41.00", "-42.00"],
        "Account": ["Rewards Card"] * 6,
        "Tags": [""] * 6,
    })
    _loaded(adapter, conn, in_memory_db, long_history, "imp1")

    # Two of the six rows recur under a label the connection has never seen.
    mostly_new = pl.DataFrame({
        "Date": ["2026-03-01", "2026-03-02", "2026-04-01", "2026-04-02"],
        "Description": ["Fuel", "Fuel", "Flight", "Hotel"],
        "Category": ["Auto", "Auto", "Travel", "Travel"],
        "Amount": ["-30.00", "-31.00", "-310.00", "-220.00"],
        "Account": ["Travel Card"] * 4,
        "Tags": [""] * 4,
    })
    keys = adapter.transform(mostly_new, conn, in_memory_db)["account_id"].to_list()

    assert keys == ["travel-card"] * 4


def test_a_bound_connection_broadcasts_its_account_to_every_row(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A bound account wins even when the sheet also names accounts per row."""
    adapter = TransactionsAdapter()

    transformed = adapter.transform(
        _multi_account_df(), sample_connection, in_memory_db
    )

    assert transformed["account_id"].to_list() == [sample_connection.account_id] * 3


def test_a_pull_never_soft_deletes_another_channels_rows(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The live-mirror diff is scoped to this connection's own channel.

    Rows are identified by the ``(source_type, source_origin)`` pair, and
    ``source_origin`` alone is not unique across channels. A file import whose
    origin string matched this connection's id would otherwise be marked
    deleted-from-source by a sheet pull that never saw it — a write, not just a
    miscount.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    in_memory_db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, source_file,
             source_type, source_origin, import_id)
        VALUES ('csv-t1', 'csv-checking', DATE '2026-01-01', -1.00, '/tmp/x.csv',
                'csv', ?, 'imp-csv')
        """,
        [conn.connection_id],
    )

    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")

    foreign = in_memory_db.execute(
        "SELECT deleted_from_source_at FROM raw.tabular_transactions "
        "WHERE transaction_id = 'csv-t1'"
    ).fetchone()
    assert foreign is not None
    assert foreign[0] is None


def _checking_only_df() -> pl.DataFrame:
    """The history with the card's rows gone, as closing that account leaves it."""
    return _account_history_df().filter(pl.col("Account") == "Everyday Checking")


def _reopened_card_df() -> pl.DataFrame:
    """The checking history beside a different card wearing the old card's label."""
    return pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-03-01", "2026-03-02"],
        "Description": ["Coffee", "Salary", "Hotel", "Flight"],
        "Category": ["Dining", "Income", "Travel", "Travel"],
        "Amount": ["-4.50", "5000.00", "-250.00", "-400.00"],
        "Account": [
            "Everyday Checking",
            "Everyday Checking",
            "Rewards Card",
            "Rewards Card",
        ],
        "Tags": ["", "", "", ""],
    })


def test_a_label_reused_after_an_absence_does_not_inherit_the_closed_account(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Closing an account and naming a new one the same thing keeps them apart.

    A label is remembered for as long as the connection lives, so re-adopting
    its key on sight files a newly opened account's transactions under the
    closed one — the silent merge the row test exists to prevent, reached
    through the one door that skipped it. A label the previous pull did not
    carry has to earn its key back from its rows like any other arrival.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")
    _loaded(adapter, conn, in_memory_db, _checking_only_df(), "imp2")

    reopened = _reopened_card_df()
    keys = adapter.transform(reopened, conn, in_memory_db)["account_id"].to_list()

    assert keys[:2] == ["everyday-checking", "everyday-checking"]
    assert keys[2] == keys[3]
    assert keys[2] != "rewards-card"


def test_a_closed_accounts_transactions_stay_put_when_its_label_is_reused(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The closed account keeps every row it owned, under the key it owned them by.

    Separating the two accounts is only half the rule: the transactions already
    filed under the closed one must neither move to the new account nor vanish.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")
    _loaded(adapter, conn, in_memory_db, _checking_only_df(), "imp2")
    _loaded(adapter, conn, in_memory_db, _reopened_card_df(), "imp3")

    owners = in_memory_db.execute(
        "SELECT description, account_id FROM raw.tabular_transactions "
        "WHERE description IN ('Card payment', 'Groceries', 'Hotel', 'Flight') "
        "ORDER BY description"
    ).fetchall()

    by_description = {str(row[0]): str(row[1]) for row in owners}
    assert by_description["Card payment"] == "rewards-card"
    assert by_description["Groceries"] == "rewards-card"
    assert by_description["Hotel"] != "rewards-card"
    assert by_description["Hotel"] == by_description["Flight"]


def test_a_dormant_account_returning_with_its_history_keeps_its_key(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """An account that simply had one quiet pull is not a different account.

    Revalidating a returning label has to answer both ways. The same account
    coming back carries its own history, so it keeps its key and no
    ``transaction_id`` rotates merely because the sheet skipped it once.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    _loaded(adapter, conn, in_memory_db, _account_history_df(), "imp1")
    _loaded(adapter, conn, in_memory_db, _checking_only_df(), "imp2")

    returned = _account_history_df()
    keys = adapter.transform(returned, conn, in_memory_db)["account_id"].to_list()

    assert keys == [
        "everyday-checking",
        "everyday-checking",
        "rewards-card",
        "rewards-card",
    ]
