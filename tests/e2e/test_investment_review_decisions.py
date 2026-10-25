"""Subprocesses consume persisted Proposals and audited rejection/undo state."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, invalidate_encryption_key_cache
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from tests.e2e.conftest import TEST_ENCRYPTION_KEY, make_workflow_env_fast, run_cli
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)

pytestmark = pytest.mark.e2e


def test_plan_reject_undo_and_replan_across_processes(
    _mutating_profile_template: Path,
    tmp_path: Path,
) -> None:
    env = make_workflow_env_fast(
        tmp_path, "investment-review", _mutating_profile_template
    )
    db_path = (
        Path(env["MONEYBIN_HOME"])
        / "profiles"
        / env["MONEYBIN_PROFILE"]
        / "moneybin.duckdb"
    )
    store = MagicMock()
    store.get_key.return_value = TEST_ENCRYPTION_KEY
    invalidate_encryption_key_cache()
    try:
        with Database(
            db_path, secret_store=store, no_auto_upgrade=True, read_only=False
        ) as db:
            # Explicit identities are inputs here; the real planner creates the Proposal.
            db.execute(
                "CREATE TABLE core.dim_accounts (account_id VARCHAR, currency_code VARCHAR)"
            )
            db.execute("INSERT INTO core.dim_accounts VALUES ('account', 'USD')")
            SecuritiesRepo(db).upsert(
                security_id="security",
                name="Test Security",
                security_type="equity",
                actor="test",
            )
            AccountLinksRepo(db).insert(
                link_id="account_link",
                account_id="account",
                ref_kind="source_native",
                ref_value="native_account",
                source_type="plaid",
                source_origin="origin",
                decided_by="user",
                actor="test",
            )
            SecurityLinksRepo(db).insert(
                security_id="security",
                ref_kind="plaid_security_id",
                ref_value="native_security",
                source_type="plaid",
                decided_by="user",
                actor="test",
            )
            seed_manual_event(db, "manual")
            seed_plaid_event(db, "native")
            install_comparison_models(db)
    finally:
        invalidate_encryption_key_cache()

    run_cli("investments", "matches", "run", env=env).assert_success()
    pending = run_cli("investments", "matches", "pending", "--output", "json", env=env)
    pending.assert_success()
    rows = json.loads(pending.stdout)["data"]["rows"]
    assert len(rows) == 1
    decision_id = rows[0]["decision_id"]
    accepted = run_cli(
        "review", "--type", "investment-matches", "--confirm", decision_id, env=env
    )
    assert accepted.exit_code == 1
    rejected = run_cli(
        "review",
        "--type",
        "investment-matches",
        "--reject",
        decision_id,
        "--output",
        "json",
        env=env,
    )
    rejected.assert_success()
    outcome = json.loads(rejected.stdout)["data"]
    assert outcome["results"][0]["status"] == "rejected"
    run_cli("investments", "matches", "run", env=env).assert_success()
    suppressed = run_cli(
        "investments", "matches", "pending", "--output", "json", env=env
    )
    suppressed.assert_success()
    assert json.loads(suppressed.stdout)["data"]["rows"] == []
    run_cli(
        "system", "audit", "undo", outcome["operation_id"], "--yes", env=env
    ).assert_success()
    run_cli("investments", "matches", "run", env=env).assert_success()
    restored = run_cli("investments", "matches", "pending", "--output", "json", env=env)
    restored.assert_success()
    assert len(json.loads(restored.stdout)["data"]["rows"]) == 1
