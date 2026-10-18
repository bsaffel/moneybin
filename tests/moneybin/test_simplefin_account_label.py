"""Safe derived labels never replace immutable original SimpleFIN account names."""

import importlib

import pytest

from moneybin.database import Database
from tests.moneybin.migration_helpers import run_migration
from tests.moneybin.test_extractors.test_simplefin_extractor import load, source
from tests.moneybin.test_stg_simplefin import bind_account, materialize


@pytest.mark.integration
@pytest.mark.slow
def test_simplefin_display_uses_existing_masker_without_changing_original_name(
    db: Database,
) -> None:
    name = "Checking 123456789"
    load(db, source(account_changes={"name": name}))
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == ("Checking ****6789",)
    assert db.execute(
        "SELECT name, json_extract_string(original_record, '$.name') FROM raw.simplefin_accounts"
    ).fetchone() == (name, name)


def test_additive_safe_labels_migrations_preserve_prior_observations(
    db: Database,
) -> None:
    columns = db.execute(
        "SELECT column_name FROM duckdb_columns() WHERE schema_name='raw' AND table_name='simplefin_accounts' ORDER BY column_index"
    ).fetchall()
    assert columns[-2:] == [("account_label",), ("institution_label",)]
    db.execute("DROP TABLE raw.simplefin_accounts")
    old = importlib.import_module("moneybin.sql.migrations.V076__add_simplefin_raw")
    run_migration(db, old.migrate)
    db.execute("""INSERT INTO raw.simplefin_accounts
        (batch_id, source_origin, account_id, native_scope, identity_strength,
         observation_version, original_record, fetched_at, source_file, name, currency)
        VALUES ('00000000-0000-0000-0000-000000000001', 'origin', 'native', '{}',
                'weak', 'revision', '{"name":"123456789"}', CURRENT_TIMESTAMP,
                'old-batch', '123456789', 'USD')""")
    migration = importlib.import_module(
        "moneybin.sql.migrations.V078__add_simplefin_account_label"
    )
    run_migration(db, migration.migrate)
    run_migration(db, migration.migrate)
    institution_migration = importlib.import_module(
        "moneybin.sql.migrations.V079__add_simplefin_institution_label"
    )
    run_migration(db, institution_migration.migrate)
    run_migration(db, institution_migration.migrate)
    assert db.execute(
        "SELECT name, account_label, institution_label, original_record FROM raw.simplefin_accounts"
    ).fetchone() == ("123456789", None, None, '{"name":"123456789"}')
    after = db.execute(
        "SELECT column_name FROM duckdb_columns() WHERE schema_name='raw' AND table_name='simplefin_accounts' ORDER BY column_index"
    ).fetchall()
    assert after == columns
