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


def _csv_source_native_id(db: Database) -> str:
    """Canonical account_id the (single) CSV source_native link minted."""
    row = db.execute(
        "SELECT account_id FROM app.account_links "
        "WHERE source_type='csv' AND ref_kind='source_native' AND status='accepted'"
    ).fetchone()
    assert row is not None, "no CSV account_link minted"
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


@pytest.mark.scenarios
@pytest.mark.slow
def test_csv_twin_human_import_gates_on_last4_bridge() -> None:
    """A HUMAN importing the Wells-Fargo csv twin (no binding) is GATED.

    The derived last4 ('1111') + filename-resolved institution ('wells_fargo')
    match the ofx account, so the import raises ImportConfirmationRequiredError
    (account_confirmation) instead of silently minting/merging — the bridge fires
    VISIBLY at the gate (Decision 7 human first-contact, "magic stays visible").
    """
    from moneybin.services.import_confirmation import ImportConfirmationRequiredError

    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        svc.import_file(_FIXTURES / "wf_checking.qfx", refresh=False)
        _ofx_canonical_id(db, "1111")  # ensure the ofx account exists
        run_step("transform", scenario.setup, db, env=env)
        with pytest.raises(
            ImportConfirmationRequiredError, match="account_confirmation"
        ):
            svc.import_file(
                _FIXTURES / "wells_fargo_checking.csv",
                account_name="WF Checking (...1111)",
                confirm=True,
                actor_kind="human",
                refresh=False,
            )


@pytest.mark.scenarios
@pytest.mark.slow
def test_csv_twin_agent_import_queues_last4_proposal_without_merge() -> None:
    """An AGENT importing the same csv twin (no binding) does NOT gate.

    It mints a provisional and writes a PENDING institution_last4 decision onto
    the ofx account (review queue, Decision 7 agent path) — never a silent merge,
    and the agent does not self-accept this weak signal.
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        svc.import_file(_FIXTURES / "wf_checking.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        run_step("transform", scenario.setup, db, env=env)
        svc.import_file(
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF Checking (...1111)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )
        decisions = db.execute(
            "SELECT match_reason FROM app.account_link_decisions "
            "WHERE status = 'pending' AND candidate_account_id = ?",
            [checking_id],
        ).fetchall()
        assert [r[0] for r in decisions] == ["institution_last4"], decisions
        merged = db.execute(
            "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
            "AND source_type = 'csv' AND account_id = ?",
            [checking_id],
        ).fetchone()
        assert merged is not None and merged[0] == 0, (
            "weak last4 signal must NOT auto-merge onto the ofx account"
        )


@pytest.mark.scenarios
@pytest.mark.slow
def test_shared_last4_collision_agent_queues_both_never_merges() -> None:
    """Two distinct Wells-Fargo accounts sharing last4 '4267' both get proposals.

    An agent csv import carrying that last4 queues TWO pending institution_last4
    proposals (one per ambiguous account), never an auto-merge — the user
    disambiguates (collision safety; a weak signal is ambiguous by construction).
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        svc.import_file(_FIXTURES / "wf_acct_a_4267.qfx", refresh=False)
        svc.import_file(_FIXTURES / "wf_acct_b_4267.qfx", refresh=False)
        acct_a = _ofx_canonical_id(db, "5114267")
        acct_b = _ofx_canonical_id(db, "6224267")
        assert acct_a != acct_b
        run_step("transform", scenario.setup, db, env=env)
        svc.import_file(
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF (...4267)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )
        cand_ids = {
            r[0]
            for r in db.execute(
                "SELECT candidate_account_id FROM app.account_link_decisions "
                "WHERE status = 'pending' AND match_reason = 'institution_last4'"
            ).fetchall()
        }
        assert cand_ids == {acct_a, acct_b}, (
            f"both same-last4 accounts must be surfaced for review, got {cand_ids}"
        )
        merged = db.execute(
            "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
            "AND source_type = 'csv' AND account_id IN (?, ?)",
            [acct_a, acct_b],
        ).fetchone()
        assert merged is not None and merged[0] == 0, (
            "shared last4 must NOT auto-merge onto either ofx account"
        )


@pytest.mark.scenarios
@pytest.mark.slow
def test_csv_twin_matches_accepts_and_collapses_end_to_end() -> None:
    """End-to-end match->accept->collapse with NO forced binding.

    The FULL automatic chain (no account_bindings): import the .qfx, then its .csv
    twin (agent path) — the matcher fires a pending institution_last4 PROPOSAL, the
    user ACCEPTS it through the real review-queue accept, and the twins then dedup
    to one gold set. This is the path a user who relies on automatic matching
    actually takes; the sibling test_cross_source_twins_collapse_* covers the
    EXPLICIT-binding path. Without this, every cross-source scenario forced the
    account link and the matcher was never exercised end-to-end ("No Shortcuts",
    testing.md).
    """
    from moneybin.services.account_links_service import AccountLinksService

    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        # 1) OFX statement mints the canonical checking account (ACCTID 1111).
        svc.import_file(_FIXTURES / "wf_checking.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        run_step("transform", scenario.setup, db, env=env)  # dim gets derived last4
        # 2) CSV twin via the agent path, NO account_bindings -> the bridge fires a
        #    pending institution_last4 proposal (provisional account + decision).
        svc.import_file(
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF Checking (...1111)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )
        decision = db.execute(
            "SELECT decision_id, candidate_account_id FROM app.account_link_decisions "
            "WHERE status = 'pending' AND match_reason = 'institution_last4'"
        ).fetchone()
        assert decision is not None, "matcher produced no institution_last4 proposal"
        assert decision[1] == checking_id, decision
        # 3) Accept the proposal the way a user reviewing the queue would — this
        #    re-points the csv's source_native link onto the ofx account.
        AccountLinksService(db).set(
            decision[0], target_account_id=checking_id, decided_by="user"
        )
        # 4) Re-materialize -> dedup -> re-materialize: the csv re-keys onto the ofx
        #    account and the twin transactions collapse.
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)
        # Both sources collapsed to ONE canonical account (no orphan twin).
        account_ids = [
            r[0]
            for r in db.execute(
                "SELECT account_id FROM core.dim_accounts ORDER BY account_id"
            ).fetchall()
        ]
        assert account_ids == [checking_id], account_ids
        # The 3 twin transactions (hand-counted from the fixtures) dedup to 3 gold
        # records, each contributed by both sources (source_count = 2).
        rows = db.execute("SELECT source_count FROM core.fct_transactions").fetchall()
        assert len(rows) == 3, f"expected 3 deduped rows, got {len(rows)}"
        assert all(sc == 2 for (sc,) in rows), rows


@pytest.mark.scenarios
@pytest.mark.slow
def test_csv_first_then_ofx_matches_accepts_and_collapses_end_to_end() -> None:
    """End-to-end match->accept->collapse in the CSV-FIRST direction.

    The sibling above lands the OFX first; here the CSV twin arrives FIRST and mints
    the canonical account, then the OFX statement lands second. For the bridge to
    fire in this direction the CSV's dim_accounts row must carry its filename-resolved
    institution (not NULL) so the OFX-second resolver can match on (institution,
    last4). A single NULL institution on the CSV's dim row silently broke this
    direction while the OFX-first direction kept working — exactly the one-directional
    coverage gap the "No Shortcuts" rule warns about (caught in PR #258 review).
    """
    from moneybin.services.account_links_service import AccountLinksService

    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        # 1) CSV twin FIRST via the agent path, NO account_bindings -> mints the
        #    canonical account. dim is empty, so there is no candidate yet (no
        #    proposal); the only thing under test here is that its dim row keeps the
        #    filename institution for the second source to match against.
        svc.import_file(
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF Checking (...1111)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )
        csv_account_id = _csv_source_native_id(db)
        run_step("transform", scenario.setup, db, env=env)  # dim gets institution+last4
        # 2) OFX statement second -> the bridge fires a pending institution_last4
        #    proposal whose candidate is the CSV-minted account.
        svc.import_file(_FIXTURES / "wf_checking.qfx", refresh=False)
        decision = db.execute(
            "SELECT decision_id, candidate_account_id FROM app.account_link_decisions "
            "WHERE status = 'pending' AND match_reason = 'institution_last4'"
        ).fetchone()
        assert decision is not None, "matcher produced no institution_last4 proposal"
        assert decision[1] == csv_account_id, decision
        # 3) Accept the proposal the way a user reviewing the queue would — this
        #    re-points the ofx's source_native link onto the csv-minted account.
        AccountLinksService(db).set(
            decision[0], target_account_id=csv_account_id, decided_by="user"
        )
        # 4) Re-materialize -> dedup -> re-materialize: the ofx re-keys onto the csv
        #    account and the twin transactions collapse.
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)
        # Both sources collapsed to ONE canonical account (no orphan twin).
        account_ids = [
            r[0]
            for r in db.execute(
                "SELECT account_id FROM core.dim_accounts ORDER BY account_id"
            ).fetchall()
        ]
        assert account_ids == [csv_account_id], account_ids
        # Same twin fixtures as the sibling: 3 distinct transactions, each in both
        # sources -> 3 deduped rows at source_count = 2.
        rows = db.execute("SELECT source_count FROM core.fct_transactions").fetchall()
        assert len(rows) == 3, f"expected 3 deduped rows, got {len(rows)}"
        assert all(sc == 2 for (sc,) in rows), rows
