"""Confirmed account overlap reconciles through the ordinary matching pipeline."""

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.extractors.account_identity import SourceAccount
from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
from moneybin.orchestration.account_mutations import (
    undo_operation,
    update_account_settings,
)
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.matching_service import MatchingService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_extractors.test_simplefin_extractor import load, source
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env

pytestmark = [pytest.mark.scenarios, pytest.mark.slow]


@pytest.mark.parametrize("last_field", ["account_type", "account_subtype"])
def test_classification_after_identity_reconciles_overlap_and_undo_redo(
    last_field: str,
) -> None:
    """Classification must match newly eligible evidence before returning."""
    scenario = Scenario(
        scenario="simplefin-classification-after-identity",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, _env):
        resolver = AccountResolver(db, actor="system")
        first = source()
        load(db, first)
        existing = resolver.resolve(SimpleFINExtractor(db).source_accounts(first)[0])
        assert existing.outcome == "minted_new"
        assert TransformService(db).apply().applied

        second = source(batch=2, account_changes={"id": "reissued"})
        load(db, second, job=2)
        incoming = resolver.resolve(SimpleFINExtractor(db).source_accounts(second)[0])
        assert incoming.outcome == "pending_review"
        assert incoming.account_id != existing.account_id
        decision = db.execute(
            """SELECT decision_id FROM app.account_link_decisions
            WHERE provisional_account_id = ? AND candidate_account_id = ?
              AND status = 'pending'""",
            [incoming.account_id, existing.account_id],
        ).fetchone()
        assert decision is not None
        AccountLinksService(db).set(decision[0], target_account_id=existing.account_id)
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            0,
        )

        if last_field == "account_type":
            update_account_settings(
                db, existing.account_id, actor="cli", account_subtype="checking"
            )
        else:
            update_account_settings(
                db, existing.account_id, actor="cli", account_type="depository"
            )
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            0,
        )

        with operation() as classify_op:
            update_account_settings(
                db,
                existing.account_id,
                actor="cli",
                account_type="depository" if last_field == "account_type" else None,
                account_subtype="checking" if last_field == "account_subtype" else None,
            )
        assert db.execute(
            "SELECT count(*), sum(amount) FROM core.fct_transactions"
        ).fetchone() == (1, Decimal("-12.30"))
        assert db.execute(
            "SELECT sum(txn_count), sum(total_spend) FROM reports.spending_trend"
        ).fetchone() == (1, Decimal("12.30"))
        assert db.execute(
            "SELECT count(*) FROM meta.fct_transaction_provenance"
        ).fetchone() == (2,)
        assert db.execute(
            """SELECT DISTINCT actor FROM app.audit_log
            WHERE operation_id = ? AND target_table = 'match_decisions'""",
            [classify_op],
        ).fetchall() == [("cli",)]

        undo = undo_operation(db, classify_op, actor="cli")
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            0,
        )
        undo_operation(db, undo.undo_operation_id, actor="cli")
        assert db.execute(
            "SELECT count(*), sum(amount) FROM core.fct_transactions"
        ).fetchone() == (1, Decimal("-12.30"))
        assert db.execute(
            "SELECT sum(txn_count), sum(total_spend) FROM reports.spending_trend"
        ).fetchone() == (1, Decimal("12.30"))


def seed_other_source(db: Database, kind: str) -> None:
    if kind == "ofx":
        db.execute("""INSERT INTO raw.ofx_accounts
            (account_id, account_type, source_type, source_origin, source_file, extracted_at)
            VALUES ('other', 'CHECKING', 'ofx', 'bank', 'statement.qfx', CURRENT_TIMESTAMP)""")
        db.execute("""INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, date_posted, amount, payee,
             source_type, source_origin, source_file, currency_code)
            VALUES ('statement', 'other', '2026-09-20', -12.30, 'Synthetic purchase',
                    'ofx', 'bank', 'statement.qfx', 'USD')""")
    elif kind == "plaid":
        db.execute("""INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, source_origin, source_file, extracted_at)
            VALUES ('other', 'depository', 'checking', 'bank', 'sync-other', CURRENT_TIMESTAMP)""")
        db.execute("""INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_origin, source_file, iso_currency_code)
            VALUES ('statement', 'other', '2026-09-20', 12.30, 'Synthetic purchase',
                    'bank', 'sync-other', 'USD')""")
    else:
        db.execute("""INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_type, source_type, source_origin,
             source_file, import_id, extracted_at)
            VALUES ('other', 'Synthetic', 'CHECKING', 'csv', 'bank',
                    'statement.csv', 'import-other', CURRENT_TIMESTAMP)""")
        db.execute("""INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_type, source_origin, source_file, import_id, currency)
            VALUES ('statement', 'other', '2026-09-20', -12.30, 'Synthetic purchase',
                    'csv', 'bank', 'statement.csv', 'import-other', 'USD')""")


@pytest.mark.parametrize("kind", ["ofx", "csv", "plaid"])
def test_confirmed_overlap_preserves_two_purchases_and_three_provenance_rows(
    kind: str,
) -> None:
    scenario = Scenario(
        scenario="simplefin-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, _env):
        seed_other_source(db, kind)
        resolver = AccountResolver(db, actor="system")
        existing = resolver.resolve(
            SourceAccount(
                source_type=kind,
                source_origin="bank",
                source_account_key="other",
                account_name="Synthetic",
                force_standalone=True,
            )
        )
        assert TransformService(db).apply().applied
        data = source()
        load(db, data)
        incoming = resolver.resolve(SimpleFINExtractor(db).source_accounts(data)[0])
        assert incoming.account_id != existing.account_id
        assert incoming.outcome == "pending_review"
        load(
            db,
            source(batch=2, day=2, transaction_changes={"id": "second-purchase"}),
            job=2,
        )
        db.execute(
            """INSERT INTO app.account_settings (account_id, account_type, account_subtype)
            VALUES (?, 'depository', 'checking')""",
            [incoming.account_id],
        )
        assert TransformService(db).apply().applied
        MatchingService(db).run()
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            3,
        )
        decision = db.execute(
            """SELECT decision_id FROM app.account_link_decisions
            WHERE provisional_account_id = ? AND candidate_account_id = ? AND status='pending'""",
            [incoming.account_id, existing.account_id],
        ).fetchone()
        assert decision is not None
        AccountLinksService(db).set(decision[0], target_account_id=existing.account_id)
        assert TransformService(db).apply().applied
        MatchingService(db).run()
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            2,
        )
        assert db.execute(
            "SELECT count(*) FROM meta.fct_transaction_provenance"
        ).fetchone() == (3,)
        assert db.execute(
            "SELECT count(DISTINCT transaction_reference) FROM raw.simplefin_transactions"
        ).fetchone() == (2,)
        MatchingService(db).run()
        assert db.execute("SELECT count(*) FROM core.fct_transactions").fetchone() == (
            2,
        )
