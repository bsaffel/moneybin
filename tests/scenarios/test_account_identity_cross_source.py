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
from moneybin.orchestration.refresh import refresh
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.account_resolution_types import AccountProposalDict
from moneybin.services.import_confirmation import ImportConfirmationRequiredError
from moneybin.services.import_service import ImportService
from tests.import_helpers import import_answering_gate
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

_FIXTURES = (
    Path(__file__).parent / "data" / "fixtures" / "account-identity-cross-source"
)


def _gated_proposal(
    svc: ImportService, path: Path, /, **kwargs: object
) -> AccountProposalDict:
    """Import ``path`` expecting the account gate; return its lone proposal.

    Every channel now stops before load on an unratified account identity, and
    the resolver's candidates ride on the raised proposal instead of a pending
    row in ``app.account_link_decisions``. Imports no longer write to that
    queue at all — it is fed by the backfill link service and by sync — so a
    scenario that wants to see what the matcher found reads it here.
    """
    with pytest.raises(ImportConfirmationRequiredError) as raised:
        svc.import_file(path, **kwargs)  # pyright: ignore[reportArgumentType]
    outcome = raised.value.outcome
    assert outcome.reason == "account_confirmation", outcome.reason
    assert len(outcome.account_proposals) == 1, outcome.account_proposals
    return outcome.account_proposals[0]


def _csv_link_count(db: Database, account_id: str) -> int:
    """CSV source_native links pointing at ``account_id`` — 0 means no merge."""
    row = db.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND source_type = 'csv' AND account_id = ?",
        [account_id],
    ).fetchone()
    assert row is not None
    return int(row[0])


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
        # transform into the explicit step below. Both are first contact against
        # an empty dim, so the gate they raise carries no candidate and the
        # helper answers it with "new"; the collapse under test is the csv
        # binding below.
        import_answering_gate(svc, _FIXTURES / "wf_checking.qfx", refresh=False)
        import_answering_gate(svc, _FIXTURES / "wf_savings.qfx", refresh=False)
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
@pytest.mark.parametrize("actor_kind", ["human", "agent"])
def test_csv_twin_gates_on_the_last4_bridge_for_every_actor(actor_kind: str) -> None:
    """Importing the Wells-Fargo csv twin (no binding) is GATED for BOTH actors.

    The derived last4 ('1111') + filename-resolved institution ('wells_fargo')
    match the ofx account, so the import stops and surfaces that candidate
    rather than acting on it.

    The actor used to decide the shape of this stop: a human was gated, while an
    agent was allowed through to mint a provisional and queue a pending
    ``institution_last4`` decision for later review. That split let the weakest
    signal in the resolver take effect unseen on the surface where nobody is
    watching, so it is gone — the gate is now actor-independent, and the agent
    cannot self-accept it (`design-principles.md`, "magic stays visible").

    Parametrizing over the actor is the assertion: if either path ever regains
    its own behavior, exactly one of these fails.
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        import_answering_gate(svc, _FIXTURES / "wf_checking.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        run_step("transform", scenario.setup, db, env=env)

        proposal = _gated_proposal(
            svc,
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF Checking (...1111)",
            confirm=True,
            actor_kind=actor_kind,
            refresh=False,
        )

        # The bridge fired, and named the ofx account it wants to merge onto.
        assert [c["signal"] for c in proposal["candidates"]] == ["institution_last4"]
        assert [c["account_id"] for c in proposal["candidates"]] == [checking_id]
        # Nothing was written on the way to asking. Gating before the resolver's
        # candidate pass is what makes this stronger than the queue it replaced:
        # there is no provisional account and no pending row to clean up, only
        # an unanswered question.
        assert _csv_link_count(db, checking_id) == 0, (
            "weak last4 signal must NOT auto-merge onto the ofx account"
        )


@pytest.mark.scenarios
@pytest.mark.slow
def test_shared_last4_collision_surfaces_both_candidates_never_merges() -> None:
    """Two distinct Wells-Fargo accounts sharing last4 '1212' are BOTH surfaced.

    A csv import carrying that last4 raises one proposal holding TWO
    institution_last4 candidates, never an auto-merge — the user disambiguates
    (collision safety; a weak signal is ambiguous by construction).
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        import_answering_gate(svc, _FIXTURES / "wf_acct_a_1212.qfx", refresh=False)
        import_answering_gate(svc, _FIXTURES / "wf_acct_b_1212.qfx", refresh=False)
        acct_a = _ofx_canonical_id(db, "5111212")
        acct_b = _ofx_canonical_id(db, "6221212")
        assert acct_a != acct_b
        run_step("transform", scenario.setup, db, env=env)

        proposal = _gated_proposal(
            svc,
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF (...1212)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )

        cand_ids = {c["account_id"] for c in proposal["candidates"]}
        assert cand_ids == {acct_a, acct_b}, (
            f"both same-last4 accounts must be surfaced for review, got {cand_ids}"
        )
        assert {c["signal"] for c in proposal["candidates"]} == {"institution_last4"}
        assert _csv_link_count(db, acct_a) == 0
        assert _csv_link_count(db, acct_b) == 0


@pytest.mark.scenarios
@pytest.mark.slow
def test_csv_twin_matches_accepts_and_collapses_end_to_end() -> None:
    """End-to-end match->accept->collapse with NO forced binding.

    The FULL automatic chain: import the .qfx, then its .csv twin with no
    binding — the resolver's bridge finds the ofx account and the import stops
    to ask, the user ACCEPTS by binding onto the candidate the gate named, and
    the twins then dedup to one gold set. This is the path a user who relies on
    automatic matching actually takes; the sibling
    test_cross_source_twins_collapse_* covers the EXPLICIT-binding path, where
    the user names the target without the matcher having proposed anything.

    The distinction is what keeps this honest ("No Shortcuts", testing.md): the
    binding below is not a shortcut past the matcher, it is the *answer to the
    matcher's own question*, and the assertion above it proves the matcher
    asked. Without this test every cross-source scenario forced the link and
    automatic matching was never exercised end-to-end.
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        # 1) OFX statement mints the canonical checking account (ACCTID 1111).
        import_answering_gate(svc, _FIXTURES / "wf_checking.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        run_step("transform", scenario.setup, db, env=env)  # dim gets derived last4
        # 2) CSV twin, NO account_bindings -> the bridge fires and the import
        #    stops, naming the ofx account as its institution_last4 candidate.
        csv_kwargs = {
            "account_name": "WF Checking (...1111)",
            "confirm": True,
            "actor_kind": "agent",
            "refresh": False,
        }
        proposal = _gated_proposal(
            svc, _FIXTURES / "wells_fargo_checking.csv", **csv_kwargs
        )
        assert [c["signal"] for c in proposal["candidates"]] == ["institution_last4"], (
            "matcher produced no institution_last4 candidate"
        )
        assert proposal["candidates"][0]["account_id"] == checking_id
        # 3) Accept it the way a user answering the gate would: bind this source
        #    key onto the candidate. The re-import then lands the csv's
        #    source_native link on the ofx account instead of minting its own.
        svc.import_file(
            _FIXTURES / "wells_fargo_checking.csv",
            account_bindings={proposal["source_account_key"]: checking_id},
            **csv_kwargs,  # pyright: ignore[reportArgumentType]
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
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        # 1) CSV twin FIRST, NO account_bindings -> mints the canonical account.
        #    dim is empty, so its gate carries no candidate and the helper
        #    answers "new"; the only thing under test here is that its dim row
        #    keeps the filename institution for the second source to match
        #    against.
        import_answering_gate(
            svc,
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="WF Checking (...1111)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )
        csv_account_id = _csv_source_native_id(db)
        run_step("transform", scenario.setup, db, env=env)  # dim gets institution+last4
        # 2) OFX statement second -> the bridge fires and the import stops,
        #    naming the CSV-minted account as its institution_last4 candidate.
        proposal = _gated_proposal(svc, _FIXTURES / "wf_checking.qfx", refresh=False)
        assert [c["signal"] for c in proposal["candidates"]] == ["institution_last4"], (
            "matcher produced no institution_last4 candidate"
        )
        assert proposal["candidates"][0]["account_id"] == csv_account_id
        # 3) Accept it by binding this source key onto the candidate — the ofx's
        #    source_native link lands on the csv-minted account.
        svc.import_file(
            _FIXTURES / "wf_checking.qfx",
            refresh=False,
            account_bindings={proposal["source_account_key"]: csv_account_id},
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


@pytest.mark.scenarios
@pytest.mark.slow
def test_reissued_card_surfaces_a_reissue_candidate_through_real_import() -> None:
    """A replacement card is surfaced for review through the real pipeline.

    Brandon's bank reissues a card: same institution, new last four. The old
    signals both miss by construction — ``institution_last4`` cannot fire
    because the last four is precisely what changed, and the display name is
    filename-derived — so before the reissue signal this minted a second
    account with no proposal and no trace.

    The fixture is chosen so ONLY the reissue guard can catch it (per
    testing.md, "a fixture that trips two guards isolates neither"):

    * last four ``9876`` differs from the ofx account's ``1111`` -> the
      ``institution_last4`` bridge is structurally unable to fire;
    * the account name is deliberately unlike the ofx account's -> the fuzzy
      ``name`` signal stays silent.

    Nothing is pre-wired: the ofx account is minted by a real import, and the
    candidate is whatever ``AccountResolver`` produces when the csv is imported
    the way an agent would import it. Asserting the exact singleton list (not
    ``in``) is what makes the isolation real — if either sibling signal also
    fired, this fails.
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        svc = ImportService(db)
        import_answering_gate(svc, _FIXTURES / "wf_checking.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        run_step("transform", scenario.setup, db, env=env)

        proposal = _gated_proposal(
            svc,
            _FIXTURES / "wells_fargo_checking.csv",
            account_name="Replacement Card (...9876)",
            confirm=True,
            actor_kind="agent",
            refresh=False,
        )

        assert [c["signal"] for c in proposal["candidates"]] == ["institution_reissue"]
        assert [c["account_id"] for c in proposal["candidates"]] == [checking_id]

        # The whole point: a reissue is a PROPOSAL, never a silent merge. The
        # import stops with the replacement unbound until someone answers.
        assert _csv_link_count(db, checking_id) == 0, (
            "a reissue proposal must not auto-merge onto the original card"
        )


@pytest.mark.scenarios
@pytest.mark.slow
def test_tabular_display_name_lands_as_the_canonical_registry_slug() -> None:
    """A sheet's hand-written institution must reach the dim as the registry slug.

    `institution_slug` is the column account matching compares, and OFX fills it
    from the <FID> registry ('us_bank'). A tabular source has only the text its
    Institution column holds — 'U.S. Bank' — and passing that straight through
    would leave one column meaning two different things depending on which
    source wrote the row. The same bank would then fail to match itself across
    sources, because slugifying a display name never reproduces a curated slug
    ('u-s-bank' against 'us-bank').

    U.S. Bank is the fixture precisely because it is the one seeds row where the
    two disagree: Wells Fargo and Chase slugify onto their own slugs and would
    pass with the derivation removed entirely.

    Seeding `raw.*` is the real input to the model under test — the derivation
    being exercised is the staging→core SQL, not the importer that fills raw.
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        db.execute(
            "INSERT INTO raw.tabular_accounts "
            "(account_id, account_name, institution_name, source_file, "
            " source_type, source_origin, import_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "usb-checking",
                "US Bank Checking",
                "U.S. Bank",
                "/tmp/usbank.csv",  # noqa: S108  # test fixture path
                "csv",
                "tiller",
                "test-import",
            ],
        )
        run_step("transform", scenario.setup, db, env=env)
        row = db.execute(
            "SELECT institution_slug FROM core.dim_accounts WHERE account_id = ?",
            ["usb-checking"],
        ).fetchone()

    assert row is not None, "seeded tabular account never reached core.dim_accounts"
    assert row[0] == "us_bank", row[0]


def _account_count(db: Database) -> int:
    """Canonical accounts standing in the ledger."""
    row = db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone()
    return int(row[0]) if row else 0


def _transaction_count(db: Database) -> int:
    """Gold transaction rows — halves when a twin pair collapses."""
    row = db.execute("SELECT COUNT(*) FROM core.fct_transactions").fetchone()
    return int(row[0]) if row else 0


def _match_decision_count(db: Database) -> int:
    """Match decisions of any status — zero means the matcher never looked."""
    row = db.execute("SELECT COUNT(*) FROM app.match_decisions").fetchone()
    return int(row[0]) if row else 0


def _pending_link_decision(db: Database) -> tuple[str, str]:
    """The pending link decision, and the account it proposes merging into.

    Returns the candidate rather than letting the caller name one: which side
    of the pair is provisional and which is the candidate follows the order
    ``run()`` walked ``core.dim_accounts``, so an accept that hard-codes a
    direction is asserting about iteration order rather than about the merge.
    """
    row = db.execute(
        "SELECT decision_id, candidate_account_id FROM app.account_link_decisions "
        "WHERE status = 'pending' ORDER BY decision_id LIMIT 1"
    ).fetchone()
    assert row is not None, "backfill reported a proposal but wrote no pending row"
    return str(row[0]), str(row[1])


@pytest.mark.scenarios
@pytest.mark.slow
def test_accepting_a_link_rematches_without_a_second_manual_step() -> None:
    """The 2026-08-08 regression: accepting a merge must re-run the matcher.

    Reproduces the shape that silently duplicated 377 rows. One card's history
    arrives from two sources; the user declines the merge the import gate offers,
    so the two ledgers sit side by side — the matcher blocks candidate pairs on
    ``account_id`` (``matching/scoring.py``), so it is structurally unable to
    pair them and writes no decision at all. Later the user changes their mind,
    backfills the proposal with ``accounts links run``, and accepts it. Before
    this fix that accept repointed the links and stopped: ``CANONICAL_STEPS``
    runs ``match`` three stages before ``identity``, so no refresh ever saw its
    own accepts and the twins stayed doubled with nothing queued to review.

    The pair shares a last four so ``AccountResolver.propose_existing`` can
    surface it: the backfill path runs with ``reissue=False``, so a genuinely
    reissued card (new last four) raises no proposal to accept at all. That gap
    is real and documented in ``moneybin-doctor.md``; it is not what this test
    covers, and ``test_reissued_card_surfaces_a_reissue_candidate_through_real_import``
    holds the reissue signal's own coverage on the import path.

    The load-bearing detail is what this test does NOT do: there is no
    ``run_step("match", ...)`` after the accept. Every earlier scenario in this
    file drives the matcher by hand, which is exactly why none of them could
    have caught this. If the accept stops triggering the pass, step 6 fails.

    Counts are hand-derived from the fixtures, not observed: ``wf_checking.qfx``
    and ``wells_fargo_checking.csv`` carry the same 3 transactions, so two
    unmerged accounts hold 6 rows and one merged account holds 3.
    """
    scenario = Scenario(
        scenario="account-identity-cross-source",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    # `env` goes unused: this test drives the real refresh() rather than the
    # runner's env-carrying step helpers, deliberately (see step 1).
    with scenario_env(scenario) as (db, _tmp, _env):
        svc = ImportService(db)
        # 1) OFX mints the original card.
        import_answering_gate(svc, _FIXTURES / "wf_checking.qfx", refresh=False)
        checking_id = _ofx_canonical_id(db, "1111")
        # The real refresh, not run_step("transform"). core.dim_accounts is
        # `kind FULL`; the scenario runner's step is `ctx.plan(auto_apply=True)`
        # alone, which applies model *changes* and so rebuilds a materialized
        # model only on the pass that first creates it. TransformService follows
        # its plan with `ctx.run()` and an explicit restate of every FULL model,
        # which is what a user's `moneybin refresh` does — and the only thing
        # that reflects a second import into the accounts dimension.
        refresh(db, steps=["transform"])

        # 2) The CSV twin arrives. The gate fires and names the ofx account —
        #    asserted so that answering it below is an answer, not a shortcut.
        csv_kwargs = {
            "account_name": "WF Checking (...1111)",
            "confirm": True,
            "actor_kind": "agent",
            "refresh": False,
        }
        proposal = _gated_proposal(
            svc, _FIXTURES / "wells_fargo_checking.csv", **csv_kwargs
        )
        assert [c["signal"] for c in proposal["candidates"]] == ["institution_last4"]
        assert proposal["candidates"][0]["account_id"] == checking_id

        # 3) The user answers "different account" — declining the merge the gate
        #    offered. A real answer, not a bypass, and the one that produces the
        #    two-ledger state. Answering "new" here is what the 2026-08-08 card
        #    was in: two accounts holding the same charges, with the matcher
        #    unable to see across them.
        svc.import_file(
            _FIXTURES / "wells_fargo_checking.csv",
            account_bindings={proposal["source_account_key"]: "new"},
            **csv_kwargs,  # pyright: ignore[reportArgumentType]
        )
        refresh(db, steps=["transform", "match"])

        # 4) The precondition the incident ran into: two accounts, both ledgers
        #    intact, and the matcher blind to the pair rather than declining it.
        accounts = db.execute(
            "SELECT account_id, display_name, institution_slug, last_four "
            "FROM core.dim_accounts ORDER BY account_id"
        ).fetchall()
        assert len(accounts) == 2, accounts
        assert _transaction_count(db) == 6, "twins should not have deduped yet"
        assert _match_decision_count(db) == 0, (
            "the matcher blocks on account_id, so it cannot have proposed anything"
        )

        # 5) The user changes their mind: backfill the proposal, then accept it.
        links = AccountLinksService(db, actor="test")
        assert links.run() > 0, "backfill produced no link proposal to accept"
        decision_id, target_id = _pending_link_decision(db)
        rematch = links.set(decision_id, target_account_id=target_id, decided_by="user")

        # 6) No run_step("match") here. The accept is the only thing that ran.
        assert rematch is not None, "accept returned no re-match result"
        assert rematch.matches_auto_merged + rematch.matches_pending_review > 0, (
            "the accept re-ran matching but the pass found nothing"
        )
        assert _account_count(db) == 1, "the merge left two accounts standing"
        assert _transaction_count(db) == 3, (
            "the twins did not collapse — the accept did not re-run the matcher"
        )
