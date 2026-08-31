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

FIXTURES = Path(__file__).parent / "fixtures"


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


def _renamed_account_df(old: str, new: str) -> pl.DataFrame:
    """The multi-account sheet with one account's label rewritten."""
    df = _multi_account_df()
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
    first = _multi_account_df()
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
    ]


def test_a_renamed_account_is_relabelled_rather_than_twinned(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """The registered account row carries the new label under the old key."""
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    first = _multi_account_df()
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
    first = _multi_account_df()
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
    ]
    assert result.rows_soft_deleted == 0


def test_two_accounts_renamed_in_one_pull_are_not_paired_by_guesswork(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """Ambiguous renames mint their own keys rather than grafting silently.

    One label gone and one arrived is a rename worth adopting. Two of each is a
    pairing no evidence in the sheet decides, and grafting the wrong pair merges
    two accounts — the one wrong inference "magic stays visible" ranks highest.
    """
    adapter = TransactionsAdapter()
    conn = _unbound(sample_connection)
    first = _multi_account_df()
    adapter.load(
        adapter.transform(first, conn, in_memory_db),
        conn,
        in_memory_db,
        "imp1",
        source_df=first,
    )

    both_renamed = pl.DataFrame({
        "Date": ["2026-01-15", "2026-01-16", "2026-01-17"],
        "Description": ["Coffee", "Salary", "Card payment"],
        "Category": ["Dining", "Income", "Transfer"],
        "Amount": ["-4.50", "5000.00", "-120.00"],
        "Account": ["Daily Spending", "Daily Spending", "Travel Rewards Card"],
        "Tags": ["", "", ""],
    })
    keys = adapter.transform(both_renamed, conn, in_memory_db)["account_id"].to_list()

    assert keys == ["daily-spending", "daily-spending", "travel-rewards-card"]


def test_a_bound_connection_broadcasts_its_account_to_every_row(
    in_memory_db: Database, sample_connection: GSheetConnection
) -> None:
    """A bound account wins even when the sheet also names accounts per row."""
    adapter = TransactionsAdapter()

    transformed = adapter.transform(
        _multi_account_df(), sample_connection, in_memory_db
    )

    assert transformed["account_id"].to_list() == [sample_connection.account_id] * 3
