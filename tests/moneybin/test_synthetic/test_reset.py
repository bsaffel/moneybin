"""Tests for the shared synthetic-reset helper."""

import pytest

from moneybin.database import Database
from moneybin.synthetic.reset import (
    GENERATOR_WRITTEN_TABLES,
    RESET_DELETIONS,
    has_any_user_content,
    has_non_synthetic_data,
    reset_synthetic_rows,
)

_INSERT = (
    "INSERT INTO raw.tabular_transactions "
    "(transaction_id, account_id, transaction_date, amount, "
    "source_file, source_type, source_origin, import_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)


@pytest.mark.unit
def test_reset_synthetic_rows_deletes_only_synthetic(db: Database) -> None:
    # One generator-created row (synthetic:// source_file) and one real import.
    db.execute(
        _INSERT,
        [
            "t1",
            "acct",
            "2025-01-01",
            "10.00",
            "synthetic://basic/42/csv",
            "csv",
            "syn",
            "imp1",
        ],
    )
    db.execute(
        _INSERT,
        ["t2", "acct", "2025-01-01", "20.00", "user-upload.csv", "csv", "user", "imp2"],
    )

    reset_synthetic_rows(db)

    rows = db.execute(
        "SELECT transaction_id FROM raw.tabular_transactions ORDER BY transaction_id"
    ).fetchall()
    assert rows == [("t2",)]


@pytest.mark.unit
def test_reset_deletions_allowlist_is_synthetic_scoped() -> None:
    # Every non-ground_truth deletion is scoped to synthetic:// source files, so
    # the helper can never touch a real user import.
    for table, where in RESET_DELETIONS.items():
        if table.endswith("ground_truth"):
            continue
        assert "synthetic://" in where, f"{table} deletion is not synthetic-scoped"


@pytest.mark.unit
def test_reset_deletions_never_touch_audited_app_tables() -> None:
    # Audited app.* tables (Invariant 10) may only be mutated through their
    # *Repo. The demo preset rebuilds its database instead of deleting these.
    for table in RESET_DELETIONS:
        assert not table.startswith("app."), f"{table} must not be raw-deleted"


@pytest.mark.unit
def test_has_non_synthetic_data_ignores_synthetic_rows(db: Database) -> None:
    db.execute(
        _INSERT,
        [
            "s1",
            "acct",
            "2025-01-01",
            "10.00",
            "synthetic://basic/42/csv",
            "csv",
            "syn",
            "imp1",
        ],
    )
    assert has_non_synthetic_data(db) is False


@pytest.mark.unit
def test_has_non_synthetic_data_detects_real_tabular(db: Database) -> None:
    db.execute(
        _INSERT,
        ["r1", "acct", "2025-01-01", "10.00", "user-upload.csv", "csv", "user", "imp1"],
    )
    assert has_non_synthetic_data(db) is True


@pytest.mark.unit
def test_has_non_synthetic_data_detects_plaid(db: Database) -> None:
    # Plaid rows are never generator-created — any row means real data.
    db.execute(
        "INSERT INTO raw.plaid_transactions "
        "(transaction_id, account_id, transaction_date, amount, source_file, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ["p1", "acct", "2025-01-01", "10.00", "sync_1", "item1"],
    )
    assert has_non_synthetic_data(db) is True


@pytest.mark.unit
def test_has_non_synthetic_data_detects_balance_only_state(db: Database) -> None:
    # Real financial state can exist as balances/assertions with no transactions.
    db.execute(
        "INSERT INTO app.balance_assertions (account_id, assertion_date, balance) "
        "VALUES (?, ?, ?)",
        ["acct", "2025-01-01", "100.00"],
    )
    assert has_non_synthetic_data(db) is True


@pytest.mark.unit
def test_has_non_synthetic_data_detects_gsheet_seeds(db: Database) -> None:
    # Live gsheet-sourced rows land in raw.gsheet_seeds (never generator-written).
    db.execute(
        "INSERT INTO raw.gsheet_seeds "
        "(connection_id, spreadsheet_id, sheet_gid, row_number, row_hash, data, import_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ["c1", "sheet1", 0, 1, "h1", "{}", "imp1"],
    )
    assert has_non_synthetic_data(db) is True


@pytest.mark.unit
def test_has_non_synthetic_data_detects_pdf_seeds(db: Database) -> None:
    # A PDF import is real data even though the table carries a `source_file`
    # column — the generator never writes it, so any row counts.
    db.execute(
        "INSERT INTO raw.pdf_seeds "
        "(alias, row_hash, data, source_file, import_id) VALUES (?, ?, ?, ?, ?)",
        ["statements", "pdf_h1", "{}", "statement.pdf", "imp1"],
    )
    assert has_non_synthetic_data(db) is True


@pytest.mark.unit
def test_has_non_synthetic_data_detects_manual_investment_transactions(
    db: Database,
) -> None:
    db.execute(
        "INSERT INTO raw.manual_investment_transactions "
        "(source_transaction_id, import_id, account_id, type, trade_date, created_by) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ["manual_abc123", "imp1", "acct", "buy", "2025-01-01", "cli"],
    )
    assert has_non_synthetic_data(db) is True


@pytest.mark.unit
def test_has_non_synthetic_data_guards_raw_tables_it_has_never_heard_of(
    db: Database,
) -> None:
    # The structural guarantee. The guard reads the live catalog and treats any
    # raw table outside the generator's own closed write-set as real data, so a
    # NEW import source is protected the day it lands — nobody has to remember to
    # add it here. `pdf_seeds` and `manual_investment_transactions` were both
    # missed exactly because the old guard enumerated the opposite (open) set.
    db.execute("CREATE TABLE raw.some_future_import_source (id VARCHAR)")
    db.execute("INSERT INTO raw.some_future_import_source VALUES ('r1')")
    assert has_non_synthetic_data(db) is True


@pytest.mark.integration
def test_generator_output_is_invisible_to_the_real_data_guard(db: Database) -> None:
    # The inverse of the guarantee above, driven through the real writer. Every
    # table the generator writes must read as synthetic — if it ever starts
    # writing a raw table outside GENERATOR_WRITTEN_TABLES, the guard would call
    # demo's own output "real data" and refuse to rebuild the demo profile.
    from moneybin.synthetic.engine import GeneratorEngine
    from moneybin.synthetic.writer import SyntheticWriter

    generated = GeneratorEngine("basic", seed=42, years=1).generate()
    SyntheticWriter(db).write(generated)

    assert has_non_synthetic_data(db) is False


@pytest.mark.unit
def test_has_any_user_content_is_false_for_a_fresh_database(db: Database) -> None:
    # The invariant the not-ours guard rests on: a freshly-initialized profile has
    # no user content, so ANY row is the user's. If `init_db` ever starts seeding a
    # table, this fails and _INIT_POPULATED_TABLES must be updated deliberately —
    # otherwise `moneybin demo` would refuse to build on a bare `db init` profile.
    assert has_any_user_content(db) is False


@pytest.mark.unit
def test_has_any_user_content_detects_securities(db: Database) -> None:
    # app.securities is user-authored and needs no transaction behind it — the gap
    # that `has_non_synthetic_data`'s raw-only scan could never see.
    db.execute(
        "INSERT INTO app.securities (security_id, name, security_type) "
        "VALUES (?, ?, ?)",
        ["sec_1", "Vanguard S&P 500 ETF", "etf"],
    )
    assert has_any_user_content(db) is True


@pytest.mark.unit
def test_has_any_user_content_detects_app_tables_with_no_raw_rows(
    db: Database,
) -> None:
    # The structural point: budgets are user-authored state with no raw row behind
    # them. Enumerating the app tables that count would go stale exactly the way
    # the raw allowlist did, so the not-ours guard counts any row at all.
    db.execute(
        "INSERT INTO app.budgets (budget_id, category, monthly_amount, start_month) "
        "VALUES (?, ?, ?, ?)",
        ["bud_1", "Dining", "500.00", "2025-01"],
    )
    assert has_any_user_content(db) is True


@pytest.mark.unit
def test_generator_written_tables_forbid_null_source_file(db: Database) -> None:
    # The `NOT (source_file LIKE 'synthetic://%')` predicate reads NULL — not TRUE —
    # for a NULL source_file, which would hide a real row from the guard. NOT NULL
    # on these tables is what makes that unreachable; assert it stays that way.
    rows = db.execute(
        "SELECT table_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND column_name = 'source_file' "
        "AND is_nullable = 'YES'"
    ).fetchall()
    nullable = {f"raw.{r[0]}" for r in rows}
    assert nullable.isdisjoint(GENERATOR_WRITTEN_TABLES)
