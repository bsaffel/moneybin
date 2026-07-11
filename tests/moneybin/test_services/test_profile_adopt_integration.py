"""`profile create` adopting a real `db init` directory, against a real database.

The unit tests in `test_profile_service.py` stub `_init_database` (they run under an
autouse patch), so they prove `create()` doesn't *call* it and doesn't rewrite the
file — but not that a genuine encrypted DuckDB survives adoption and still opens
with its data intact. That is the whole risk of the repair path, so it gets a test
against the real thing.

Reproduces the exact sequence from the field: `moneybin db init --profile alice`
leaves a directory and an encrypted database but no `config.yaml`, so `profile list`
hides it and it has no inbox.
"""

from pathlib import Path

import pytest

from moneybin.services.profile_service import ProfileService


@pytest.mark.integration
def test_create_adopts_a_db_init_directory_and_preserves_its_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    from moneybin.database import Database, init_db
    from moneybin.secrets import SecretStore

    # 1. What `moneybin db init --profile alice` leaves behind: directory + encrypted
    #    database, no config.yaml. (`db init` does not register the profile.)
    profile_dir = tmp_path / "profiles" / "alice"
    profile_dir.mkdir(parents=True)
    db_path = profile_dir / "moneybin.duckdb"
    init_db(db_path, profile="alice")

    # Put a row in it so "the data survived" is a claim about data, not a file size.
    with Database(
        db_path,
        read_only=False,
        secret_store=SecretStore(profile="alice"),
        no_auto_upgrade=True,
    ) as db:
        db.execute("CREATE TABLE app.canary (id VARCHAR)")
        db.execute("INSERT INTO app.canary VALUES (?)", ["do-not-destroy-me"])

    db_bytes_before = db_path.read_bytes()

    svc = ProfileService()
    assert svc.is_registered("alice") is False
    assert "alice" not in [p["name"] for p in svc.list()]  # hidden by `profile list`

    # 2. The repair. Before this contract, `create()` refused here and no verb could
    #    finish the profile.
    svc.create("alice")

    # 3. Setup is complete...
    assert svc.is_registered("alice") is True
    assert "alice" in [p["name"] for p in svc.list()]
    assert (profile_dir / "config.yaml").exists()

    # 4. ...and the pre-existing database was never rewritten or re-keyed.
    assert db_path.read_bytes() == db_bytes_before
    with Database(
        db_path,
        read_only=False,
        secret_store=SecretStore(profile="alice"),
        no_auto_upgrade=True,
    ) as db:
        row = db.execute("SELECT id FROM app.canary").fetchone()
    assert row is not None
    assert row[0] == "do-not-destroy-me"
