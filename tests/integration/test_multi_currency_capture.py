"""End-to-end proof that multi-currency capture (M1K.1 Part A) holds together.

Drives it through the real import path -- no raw.* seeding shortcuts. Covers
multi-currency.md Requirements 1, 2, 3, 8: a non-USD OFX statement's CURDEF is
captured, survives the union without being relabeled USD, and lands correctly
on core.fct_transactions / core.fct_balances via ImportService.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.services.import_service import ImportService
from tests.integration.conftest import make_secret_store

pytestmark = pytest.mark.integration

_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    """Real encrypted Database wired for ImportService's internal refresh.

    The ``db`` fixture in tests/moneybin/conftest.py is not visible here --
    pytest fixtures don't cross sibling package boundaries. This mirrors the
    established tests/integration/ pattern (test_import_service_batch.py,
    test_schema_drift.py): build a real Database directly, then point
    get_settings() at its path so sqlmesh_context() (invoked internally by
    ImportService's refresh=True) reuses this same encrypted connection
    instead of opening an unencrypted one at the default path.
    """
    secret_store = make_secret_store()
    db_path = tmp_path / "multi_currency_eur.duckdb"
    db = Database(db_path, secret_store=secret_store, read_only=False)
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
    return db


@pytest.mark.slow
def test_eur_ofx_statement_currency_survives_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A EUR OFX import's currency reaches core.fct_transactions/fct_balances unmangled."""
    db = _build_db(tmp_path, monkeypatch)
    fixture = _FIXTURES_DIR / "multi_currency_eur.qfx"

    ImportService(db).import_file(fixture, refresh=True)

    # The real account resolver mints an opaque canonical account_id, unknown
    # to this test in advance — but this is a fresh db with exactly one
    # import in it, so an unfiltered query is unambiguous by construction.
    txn_currencies = db.execute(
        "SELECT DISTINCT currency_code FROM core.fct_transactions"
    ).fetchall()
    assert txn_currencies == [("EUR",)], (
        "EUR transactions must not be relabeled USD anywhere in the pipeline"
    )

    balance_currencies = db.execute(
        "SELECT DISTINCT currency_code FROM core.fct_balances "
        "WHERE source_type = 'ofx' AND balance = 5000.00"
    ).fetchall()
    assert balance_currencies == [("EUR",)]


@pytest.mark.slow
def test_eur_ofx_account_currency_reaches_the_accounts_dimension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The account a EUR statement describes is EUR, not a guessed USD.

    ``dim_accounts.currency_code`` is the terminal fallback for the whole
    chain — ``fct_transactions`` and ``fct_balances`` both COALESCE onto it —
    so a literal default here cannot be a local cosmetic issue: it relabels
    every row whose own currency the source omitted (multi-currency.md
    Requirement 3, "never a blind 'USD'").
    """
    db = _build_db(tmp_path, monkeypatch)

    ImportService(db).import_file(
        _FIXTURES_DIR / "multi_currency_eur.qfx", refresh=True
    )

    # Fresh db, one import: an unfiltered query names exactly this account.
    assert db.execute(
        "SELECT DISTINCT currency_code FROM core.dim_accounts"
    ).fetchall() == [("EUR",)]


@pytest.mark.slow
def test_tabular_currency_case_and_whitespace_variants_are_one_currency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`usd`, `USD `, and `USD` from a spreadsheet are one denomination, not three.

    Tabular is the only source whose currency is free text a human typed into a
    column; OFX CURDEF and Plaid iso_currency_code are already canonical. Every
    money-summing reports.* model GROUP BYs currency_code, so an unnormalized
    variant silently splits one currency into several subtotals and makes
    system doctor's currency_integrity report a mixed-currency profile that
    isn't one — the failure is invisible precisely because each subtotal is
    internally correct.
    """
    db = _build_db(tmp_path, monkeypatch)
    for i, raw_currency in enumerate(("usd", "USD ", "USD")):
        db.execute(
            """
            INSERT INTO raw.tabular_accounts
                (account_id, account_name, currency, source_file, source_type,
                 source_origin, import_id, extracted_at, loaded_at)
            VALUES (?, ?, ?, '/tmp/variants.csv', 'csv', 'variant_bank',
                    'imp-variants', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [f"variant-acct-{i}", f"Account {i}", raw_currency],
        )
        db.execute(
            """
            INSERT INTO raw.tabular_transactions
                (transaction_id, account_id, transaction_date, amount, description,
                 currency, source_file, source_type, source_origin, import_id,
                 extracted_at, loaded_at)
            VALUES (?, ?, '2026-01-15', -10.00, 'Variant purchase', ?,
                    '/tmp/variants.csv', 'csv', 'variant_bank', 'imp-variants',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [f"variant-txn-{i}", f"variant-acct-{i}", raw_currency],
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    # Asserted separately so each staging view is isolated: the accounts and
    # transactions views normalize independently, and one fix does not imply
    # the other.
    assert db.execute(
        "SELECT DISTINCT currency_code FROM core.dim_accounts ORDER BY 1"
    ).fetchall() == [("USD",)]
    assert db.execute(
        "SELECT DISTINCT currency_code FROM core.fct_transactions ORDER BY 1"
    ).fetchall() == [("USD",)]


@pytest.mark.slow
def test_plaid_account_currency_falls_back_to_the_unofficial_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plaid reports one of two currency fields, and the dimension needs both.

    Plaid sets ``iso_currency_code`` for fiat and ``unofficial_currency_code``
    for everything it has no ISO code for (crypto), never both. ``dim_accounts``
    COALESCEs the pair, so reading only the ISO column would leave every crypto
    account's currency unknown while fiat accounts look fine — a gap no
    single-account fixture can show. Both branches are asserted in one profile
    so neither can pass by being the only row present.
    """
    db = _build_db(tmp_path, monkeypatch)
    accounts = (("plaid-fiat-acct", "CAD", None), ("plaid-crypto-acct", None, "BTC"))
    for account_id, iso_code, unofficial_code in accounts:
        db.execute(
            """
            INSERT INTO raw.plaid_accounts
                (account_id, account_type, account_subtype, institution_name,
                 source_file, source_type, source_origin, extracted_at, loaded_at)
            VALUES (?, 'depository', 'checking', 'Test Bank', 'sync_1', 'plaid',
                    'item_currency', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [account_id],
        )
        db.execute(
            """
            INSERT INTO raw.plaid_balances
                (account_id, balance_date, current_balance, iso_currency_code,
                 unofficial_currency_code, source_file, source_type, source_origin,
                 extracted_at, loaded_at)
            VALUES (?, '2026-01-15', 100.00, ?, ?, 'sync_1', 'plaid',
                    'item_currency', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture, not executing user SQL
            [account_id, iso_code, unofficial_code],
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    # A dropped COALESCE branch shows up as a NULL row here, not a missing one.
    assert db.execute(
        "SELECT DISTINCT currency_code FROM core.dim_accounts ORDER BY 1"
    ).fetchall() == [("BTC",), ("CAD",)]
