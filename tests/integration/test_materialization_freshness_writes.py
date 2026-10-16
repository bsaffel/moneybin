"""Behavioural guard: every app write leaves the warehouse equal to a full rebuild.

The structural guard (``test_materialization_freshness.py``) proves no
materialized model reads a read-through column, but it follows projected
columns only and cannot see a writer. This one performs each write through its
service, fingerprints every ``core`` and ``reports`` relation, runs a full
``TransformService.apply()``, and asserts nothing moved: a write that leaves a
materialized model stale shows up as a fingerprint the apply changes.
docs/specs/materialization-freshness-contract.md.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.services.account_service import AccountService
from moneybin.services.balance_service import BalanceService
from moneybin.services.categorization import CategorizationService
from moneybin.services.currency_service import CurrencyService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.matching_service import MatchingService
from moneybin.services.profile_settings_service import ProfileSettingsService
from moneybin.services.transaction_service import TransactionService
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService
from moneybin.synthetic.engine import GeneratorEngine
from moneybin.synthetic.writer import SyntheticWriter
from tests.integration.conftest import make_secret_store

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_ACTOR = "test"


@contextmanager
def _open(path: Path) -> Generator[Database, None, None]:
    with pytest.MonkeyPatch.context() as mp:
        settings = MagicMock()
        settings.database.path = path
        mp.setattr("moneybin.database.get_settings", lambda: settings)
        db = Database(path, secret_store=make_secret_store(), read_only=False)
        try:
            yield db
        finally:
            db.close()


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> Generator[Path, None, None]:
    """A built profile with accepted and pending Transfers and FX conversions.

    One account is moved to EUR so the accepted Transfers it takes part in
    become currency conversions; the rest of the pending ones stay pending for
    the accept case.
    """
    clear_settings_cache()
    set_current_profile("test")
    path = tmp_path_factory.mktemp("freshness_writes") / "template.duckdb"
    with _open(path) as db:
        result = GeneratorEngine("family", seed=11, years=1).generate()
        SyntheticWriter(db).write(result)
        assert TransformService(db).apply().applied
        euro_account = _one(
            db,
            "SELECT account_id FROM core.dim_accounts "
            "WHERE account_type = 'depository' ORDER BY account_id DESC LIMIT 1",
        )
        AccountService(db).settings_update(
            euro_account, currency_code="EUR", actor=_ACTOR
        )
        MatchingService(db).run(restate=False)
        pending = db.execute(
            "SELECT match_id FROM app.match_decisions "
            "WHERE match_status = 'pending' ORDER BY match_id"
        ).fetchall()
        for (match_id,) in pending[: len(pending) // 2]:
            MatchingService(db).set_status(match_id, status="accepted", actor=_ACTOR)
        assert TransformService(db).apply().applied
        conversions = _one(db, "SELECT COUNT(*) FROM core.bridge_currency_conversions")
        assert int(conversions) > 0, "the template must exercise the FX models"
    yield path
    clear_settings_cache()


def _fingerprint(db: Database) -> dict[str, tuple[int, str]]:
    relations = db.execute(
        """
        SELECT schema_name || '.' || table_name FROM duckdb_tables()
        WHERE database_name = 'moneybin' AND schema_name IN ('core', 'reports')
        UNION
        SELECT schema_name || '.' || view_name FROM duckdb_views()
        WHERE database_name = 'moneybin' AND schema_name IN ('core', 'reports')
        ORDER BY 1
        """
    ).fetchall()
    prints: dict[str, tuple[int, str]] = {}
    for (relation,) in relations:
        row = db.execute(
            f"""
            SELECT COUNT(*), MD5(COALESCE(STRING_AGG(t::VARCHAR, '|' ORDER BY t::VARCHAR), ''))
            FROM {relation} AS t
            """  # noqa: S608  # relation names come from the DuckDB catalog
        ).fetchone()
        assert row is not None
        prints[relation] = (int(row[0]), str(row[1]))
    return prints


def _one(db: Database, sql: str) -> str:
    row = db.execute(sql).fetchone()
    assert row is not None and row[0] is not None, sql
    return str(row[0])


def _txn(db: Database) -> str:
    return _one(
        db,
        "SELECT transaction_id FROM core.fct_transactions "
        "WHERE amount < 0 ORDER BY transaction_id LIMIT 1",
    )


def _account(db: Database) -> str:
    return _one(
        db, "SELECT account_id FROM core.dim_accounts ORDER BY account_id LIMIT 1"
    )


def _note(db: Database) -> None:
    TransactionService(db).add_note(_txn(db), "freshness check", actor=_ACTOR)


def _tag(db: Database) -> None:
    TransactionService(db).add_tags(_txn(db), ["freshness"], actor=_ACTOR)


def _split(db: Database) -> None:
    txn = _txn(db)
    amount = Decimal(
        _one(
            db,
            f"SELECT amount FROM core.fct_transactions WHERE transaction_id = '{txn}'",  # noqa: S608  # test-built id
        )
    )
    TransactionService(db).add_split(txn, amount / 2, actor=_ACTOR)


def _categorize(db: Database) -> None:
    category = _one(
        db, "SELECT category FROM core.dim_categories ORDER BY category LIMIT 1"
    )
    CategorizationService(db).set_category(_txn(db), category=category, actor=_ACTOR)


def _manual_transaction(db: Database) -> None:
    TransactionService(db).create_manual_batch(
        [
            {
                "account_id": _account(db),
                "transaction_date": "2025-06-15",
                "amount": Decimal("-12.34"),
                "description": "Freshness manual entry",
            }
        ],
        actor=_ACTOR,
    )


def _merchant(db: Database) -> None:
    description = _one(
        db,
        "SELECT description FROM core.fct_transactions "
        "WHERE description IS NOT NULL ORDER BY transaction_id LIMIT 1",
    )
    CategorizationService(db).create_merchant(
        description,
        "Freshness Merchant",
        match_type="exact",
        reapply=True,
        actor=_ACTOR,
    )


def _settings(**fields: object) -> Callable[[Database], None]:
    def write(db: Database) -> None:
        AccountService(db).settings_update(_account(db), actor=_ACTOR, **fields)  # type: ignore[arg-type]  # test passes typed values

    return write


def _assert_balance(db: Database) -> None:
    BalanceService(db).assert_balance(
        _account(db), date(2025, 6, 30), Decimal("1234.56"), actor=_ACTOR
    )


def _home_currency(db: Database) -> None:
    current = ProfileSettingsService(db).get_settings().home_currency
    ProfileSettingsService(db).set_setting(
        "home_currency", "GBP" if current != "GBP" else "EUR", actor=_ACTOR
    )


def _fx_override(db: Database) -> None:
    CurrencyService(db, actor=_ACTOR).set_override(
        "EUR", "USD", date(2025, 3, 3), Decimal("1.2345"), note=None
    )


def _security(db: Database) -> None:
    InvestmentService(db).upsert_security(
        security_id=None, name="Freshness Fund", security_type="etf", actor=_ACTOR
    )


def _accept_pending_match(db: Database) -> None:
    match_id = _one(
        db,
        "SELECT match_id FROM app.match_decisions WHERE match_status = 'pending' "
        "ORDER BY match_id LIMIT 1",
    )
    MatchingService(db).set_status(match_id, status="accepted", actor=_ACTOR)


def _undo_accepted_match(db: Database) -> None:
    match_id = _one(
        db,
        "SELECT match_id FROM app.match_decisions WHERE match_status = 'accepted' "
        "AND reversed_at IS NULL ORDER BY match_id LIMIT 1",
    )
    MatchingService(db).undo(match_id, actor=_ACTOR)


def _undo_currency_change(db: Database) -> None:
    _settings(currency_code="CAD")(db)
    operation_id = _one(
        db,
        "SELECT operation_id FROM app.audit_log WHERE target_table = 'account_settings' "
        "ORDER BY occurred_at DESC LIMIT 1",
    )
    UndoService(db).undo(operation_id, actor=_ACTOR)


_WRITES: dict[str, Callable[[Database], None]] = {
    "note": _note,
    "tag": _tag,
    "split": _split,
    "categorize": _categorize,
    "merchant": _merchant,
    "manual_transaction": _manual_transaction,
    "rename": _settings(display_name="Freshness Renamed"),
    "currency": _settings(currency_code="CAD"),
    "archive": _settings(archived=True),
    "net_worth_toggle": _settings(include_in_net_worth=False),
    "cost_basis_method": _settings(default_cost_basis_method="hifo"),
    "balance_assertion": _assert_balance,
    "home_currency": _home_currency,
    "fx_override": _fx_override,
    "security": _security,
    "accept_pending_match": _accept_pending_match,
    "undo_accepted_match": _undo_accepted_match,
    "undo_currency_change": _undo_currency_change,
}


@pytest.mark.parametrize("write", sorted(_WRITES))
def test_a_write_leaves_nothing_for_a_full_apply_to_change(
    write: str, template: Path, tmp_path: Path
) -> None:
    path = tmp_path / "profile.duckdb"
    shutil.copy(template, path)
    with _open(path) as db:
        before_write = _fingerprint(db)
        _WRITES[write](db)
        after_write = _fingerprint(db)
        assert TransformService(db).apply().applied
        after_apply = _fingerprint(db)

    stale = sorted(
        relation
        for relation in after_apply
        if after_apply[relation] != after_write.get(relation)
    )
    assert not stale, f"{write}: a full apply still changed {stale}"
    # The write has to reach something, or the comparison above is vacuous.
    assert after_write != before_write, f"{write} changed no core or reports relation"
