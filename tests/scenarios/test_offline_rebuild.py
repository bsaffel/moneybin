"""Retained encrypted evidence survives recovery and an offline materialization rebuild."""

from pathlib import Path

import pytest

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.secrets import SecretStore
from moneybin.services.transaction_service import TransactionService
from moneybin.services.transform_service import TransformService
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_offline_rebuild_preserves_retained_history_and_curation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.services.database_backup_service import backup_encrypted_database
    from moneybin.services.database_restore_service import restore_encrypted_database

    scenario = load_shipped_scenario("audit-log-idempotency")
    assert scenario is not None
    with scenario_env(scenario) as (db, temporary, env):
        load_fixture_into_db(
            db,
            FixtureSpec(
                path="curation-manual-dedup/imported.csv",
                account="curation-checking",
                source_type="csv",
            ),
        )
        AccountLinksRepo(db).insert(
            link_id="recovery-link",
            account_id="recovery-account",
            ref_kind="source_native",
            ref_value="curation-checking",
            source_type="csv",
            source_origin="fixture",
            decided_by="user",
            actor="cli",
        )
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)
        row = db.execute(
            "SELECT transaction_id FROM core.fct_transactions ORDER BY transaction_id LIMIT 1"
        ).fetchone()
        assert row is not None
        transaction_id = str(row[0])
        service = TransactionService(db)
        service.add_note(transaction_id, "retained statement evidence", actor="cli")
        service.add_tags(transaction_id, ["recovery-check"], actor="cli")
        projection = "SELECT transaction_id, account_id, transaction_date, amount, description, notes, tags FROM core.fct_transactions ORDER BY transaction_id"
        before = db.execute(projection).fetchall()
        notes = db.execute(
            "SELECT * FROM app.transaction_notes ORDER BY note_id"
        ).fetchall()
        links = db.execute(
            "SELECT * FROM app.account_links ORDER BY link_id"
        ).fetchall()
        raw = db.execute(
            "SELECT * FROM raw.tabular_transactions ORDER BY ALL"
        ).fetchall()
        assert before and notes and links and raw
        source = get_settings().database.path
        db.close()
        backup, restored = (
            Path(temporary) / "retained.duckdb",
            Path(temporary) / "restored.duckdb",
        )
        store = SecretStore()
        backup_encrypted_database(source, backup, secret_store=store)
        restore_encrypted_database(
            backup,
            restored,
            secret_store=store,
            backup_path=Path(temporary) / "unused.duckdb",
        )
        # Provider access must not be necessary for retained-history recovery.

        def refuse_network(*args: object, **kwargs: object) -> None:
            raise AssertionError("offline rebuild attempted network access")

        monkeypatch.setattr("httpx.Client.send", refuse_network)
        with Database(restored, read_only=False, secret_store=store) as recovery:
            result = TransformService(recovery).apply()
            assert result.applied, result.error
            assert recovery.execute(projection).fetchall() == before
            assert (
                recovery.execute(
                    "SELECT * FROM raw.tabular_transactions ORDER BY ALL"
                ).fetchall()
                == raw
            )
            assert (
                recovery.execute(
                    "SELECT * FROM app.transaction_notes ORDER BY note_id"
                ).fetchall()
                == notes
            )
            assert (
                recovery.execute(
                    "SELECT * FROM app.account_links ORDER BY link_id"
                ).fetchall()
                == links
            )
