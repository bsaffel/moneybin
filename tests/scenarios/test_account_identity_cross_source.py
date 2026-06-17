"""Scenario: cross-source account identity collapse (M1S.6).

The original bug (account-identity-resolution.md): a `.qfx` statement and its
`.csv` twin minted *separate* accounts, so cross-source transaction dedup could
never fire — N rows imported as 2N, every row `source_count = 1`.

This scenario proves the fix end-to-end with two Wells Fargo accounts, each
imported as a `.qfx` + a `.csv` twin (12 raw transactions across 4 source
accounts). Binding each csv twin onto the canonical account the qfx minted (the
account-binding facet, M1S.4) collapses the 4 source accounts to **2 canonical
accounts** and lets the matcher dedup the twins to **6 `fct_transactions` rows
at `source_count = 2`**.

Expected counts are hand-derived from the fixtures (2 accounts x 3 distinct
transactions, each present in both sources), not observed from the pipeline.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.services.import_service import ImportService
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

_FIXTURES = (
    Path(__file__).parent / "data" / "fixtures" / "account-identity-cross-source"
)


def _ofx_canonical_id(db: Database, acctid: str) -> str:
    """Canonical account_id the OFX import minted for a given ACCTID."""
    row = db.execute(
        "SELECT account_id FROM app.account_links "
        "WHERE source_type='ofx' AND ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [acctid],
    ).fetchone()
    assert row is not None, f"no OFX account_link for ACCTID {acctid}"
    return str(row[0])


@pytest.mark.scenarios
@pytest.mark.slow
def test_cross_source_twins_collapse_to_canonical_accounts() -> None:
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)

        # Import the two .qfx statements first — each mints a canonical account
        # (ACCTID 1111 = checking, 2222 = savings). refresh=False groups the
        # transform into the explicit step below.
        svc.import_file(_FIXTURES / "wf_checking.qfx", refresh=False)
        svc.import_file(_FIXTURES / "wf_savings.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        savings_id = _ofx_canonical_id(db, "2222")
        assert checking_id != savings_id

        # Import the .csv twins, binding each onto the qfx-minted account. The
        # binding adopts above detection, so both sources share one canonical id.
        svc.import_file(
            _FIXTURES / "wf_checking.csv",
            account_name="WF Checking",
            account_bindings={"wf-checking": checking_id},
            confirm=True,
            actor_kind="human",
            refresh=False,
        )
        svc.import_file(
            _FIXTURES / "wf_savings.csv",
            account_name="WF Savings",
            account_bindings={"wf-savings": savings_id},
            confirm=True,
            actor_kind="human",
            refresh=False,
        )

        # Materialize core, run cross-source dedup, then re-materialize so the
        # match decisions collapse the twins in core.fct_transactions.
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        # 4 source accounts (2 ofx + 2 csv) collapse to 2 canonical accounts.
        account_ids = [
            r[0]
            for r in db.execute(
                "SELECT account_id FROM core.dim_accounts ORDER BY account_id"
            ).fetchall()
        ]
        assert sorted(account_ids) == sorted([checking_id, savings_id]), account_ids

        # 12 raw transactions (6 unique x 2 sources) dedup to 6 gold records,
        # each contributed by exactly 2 sources (source_count = 2).
        rows = db.execute(
            "SELECT account_id, source_count FROM core.fct_transactions"
        ).fetchall()
        assert len(rows) == 6, f"expected 6 deduped rows, got {len(rows)}"
        assert all(sc == 2 for _aid, sc in rows), rows
        # Every transaction re-keys under a canonical account (no orphan twins).
        assert {aid for aid, _sc in rows} == {checking_id, savings_id}
