"""Scenario: merchant_entity_id propagates through prep pipeline, not into core.

TDD for M1T Task 5: Carry merchant_entity_id to the resolution layer (prep models).

The plaid_sync_response fixture already contains merchant_entity_id on txn_001
(entity_starbucks_001). After the transform step the value must be visible in
prep.int_transactions__merged and MUST NOT appear in core.fct_transactions.
The resolver (a later task) reads merchant_entity_id from prep and writes only
the canonical merchant_id into core.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

_FIXTURE = (
    Path(__file__).parent.parent
    / "moneybin"
    / "test_extractors"
    / "fixtures"
    / "plaid_sync_response.yaml"
)


@pytest.mark.scenarios
@pytest.mark.slow
def test_merchant_entity_id_reaches_merged_not_core() -> None:
    """merchant_entity_id flows stg_plaid → unioned → matched → merged; absent from fct.

    Fixture txn_001 (entity_starbucks_001) exercises the real pipeline path:
    PlaidExtractor writes to raw.plaid_transactions → stg_plaid__transactions
    already projects merchant_entity_id (V030) → must survive the unioned /
    matched / merged chain.

    Assertion derivation: from the input fixture (1 of 3 transactions carries
    a non-null merchant_entity_id), not from observing the program output.
    """
    scenario = Scenario(
        scenario="merchant-entity-id-pipeline",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        # Load Plaid data — the fixture already carries merchant_entity_id.
        sync_data = SyncDataResponse.model_validate(
            yaml.safe_load(_FIXTURE.read_text())
        )
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        # Set up account_links so the staging JOIN resolves canonical ids;
        # mirrors the pattern from test_stg_plaid.py::db_with_data.
        item_by_account = loader.build_account_to_item_map(sync_data)
        resolver = AccountResolver(db, actor="system")
        for acc in sync_data.accounts:
            resolver.resolve(
                SourceAccount(
                    source_type="plaid",
                    source_origin=item_by_account[acc.account_id],
                    source_account_key=acc.account_id,
                    account_name=acc.official_name or acc.account_id,
                    account_number=None,
                    last_four=acc.mask,
                    institution=acc.institution_name,
                )
            )

        # Run the SQLMesh transform (materialises all prep + core views).
        run_step("transform", scenario.setup, db, env=env)

        # --- Assertion 1: merchant_entity_id IS in prep.int_transactions__merged ---
        merged_cols = {
            r[0]
            for r in db.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name = 'prep' AND table_name = 'int_transactions__merged'"
            ).fetchall()
        }
        assert "merchant_entity_id" in merged_cols, (
            "merchant_entity_id must flow from stg_plaid__transactions "
            "through unioned → matched → merged"
        )

        # --- Assertion 2: merchant_entity_id is NOT in core.fct_transactions ---
        fct_cols = {
            r[0]
            for r in db.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name = 'core' AND table_name = 'fct_transactions'"
            ).fetchall()
        }
        assert "merchant_entity_id" not in fct_cols, (
            "merchant_entity_id must NOT appear in core.fct_transactions — "
            "the resolver (Task 6) reads from prep and writes only merchant_id"
        )

        # --- Assertion 3: at least one non-NULL value survives (the real mechanism) ---
        # Derived from input: txn_001 carries entity_starbucks_001 (hand-verified).
        count = db.execute(
            "SELECT COUNT(*) FROM prep.int_transactions__merged "
            "WHERE merchant_entity_id IS NOT NULL"
        ).fetchone()
        assert count is not None and count[0] > 0, (
            "At least one non-NULL merchant_entity_id must reach merged "
            "(fixture txn_001 carries entity_starbucks_001)"
        )
