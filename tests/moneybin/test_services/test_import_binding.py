"""M1S.4 — import-time account-binding gate + bindings (service level).

Exercises the conditional gate (interactive human first contact surfaces weak
account-merge candidates; agent / non-interactive load and queue) and the
account_bindings resolution map through the real import_file pipeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.services.import_confirmation import ImportConfirmationRequiredError
from moneybin.services.import_service import ImportService
from tests.moneybin.db_helpers import create_core_tables

_STANDARD_CSV = Path(__file__).parents[2] / "fixtures" / "tabular" / "standard.csv"
_MINIMAL_OFX = Path(__file__).parents[2] / "fixtures" / "ofx" / "sample_minimal.ofx"


def _seed_existing_account(db: Database, *, account_id: str, display_name: str) -> None:
    """Materialize core.dim_accounts with one account the name pass can match."""
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) "  # noqa: S608  # test fixture
        "VALUES (?, ?)",
        [account_id, display_name],
    )


# --- the gate + bindings (via the real import_file pipeline) --------------
# Binding application ("new" -> force_standalone, id -> adopt, unbound -> gate)
# is exercised end-to-end below rather than against the private helper.


def test_human_import_gates_on_weak_account_candidate(
    db: Database,
) -> None:
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
        )
    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    cand_ids = [
        c["account_id"] for p in outcome.account_proposals for c in p["candidates"]
    ]
    assert "wf_existing01" in cand_ids
    # Gate raised before transform/load: no rows landed.
    n = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
    assert n is not None and n[0] == 0


def test_agent_import_gates_on_weak_account_candidate(
    db: Database,
) -> None:
    """An agent never self-picks an account identity — it gets the same confirm.

    Same fixture as the human gate above, only the actor differs. Previously
    ``_gate_account_proposals`` returned immediately for any non-human actor, so
    an agent-driven import resolved straight through and bound the account with
    no confirm and no pre-load stop — the path most likely to run unattended and
    least likely to have anyone notice a wrong binding.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="agent",
        )
    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    cand_ids = [
        c["account_id"] for p in outcome.account_proposals for c in p["candidates"]
    ]
    assert "wf_existing01" in cand_ids
    # Same pre-load guarantee the human path gets: nothing landed.
    n = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
    assert n is not None and n[0] == 0


def test_first_contact_mint_gates_with_no_candidates(
    db: Database,
) -> None:
    """Minting a brand-new account is a visible moment, not a silent side effect.

    The book is empty, so the candidate pass finds nothing and the proposal is
    ``is_new`` with ``adopted_via=None`` — the second clause of
    ``AccountProposal.requires_confirm``, which had no consumer. The gate tested
    ``candidates`` alone, so this import minted an account and loaded rows
    without ever asking, on every channel.
    """
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
        )
    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    proposals = outcome.account_proposals
    assert [p["source_account_key"] for p in proposals] == ["wf-checking"]
    # The clause under test: nothing to merge against, and it still gates.
    assert proposals[0]["candidates"] == []
    assert proposals[0]["is_new"] is True
    assert proposals[0]["requires_confirm"] is True
    n = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
    assert n is not None and n[0] == 0


def test_masked_label_reaches_resolver_as_clean_name(
    db: Database,
) -> None:
    """A masked label must reach the resolver as its cleaned name.

    "Cash (...1789)" must arrive as "Cash"; otherwise the mask text sinks the
    fuzzy-name ratio below threshold (SequenceMatcher("cash (...1789)", "cash")
    ~= 0.44 < 0.6) and a duplicate is silently minted instead of surfacing the
    existing-account candidate. With no institution resolved, name is the only
    signal.
    """
    _seed_existing_account(db, account_id="cash_existing01", display_name="Cash")
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            account_name="Cash (...1789)",
            refresh=False,
            confirm=True,
            actor_kind="human",
        )
    cand_ids = [
        c["account_id"]
        for p in exc.value.outcome.account_proposals
        for c in p["candidates"]
    ]
    assert "cash_existing01" in cand_ids


def test_binding_to_candidate_adopts_and_loads(
    db: Database,
) -> None:
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    result = svc.import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "wf_existing01"},
    )
    assert result.transactions > 0
    # The CSV's source_native ref now maps to the existing account.
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        ["wf-checking"],
    ).fetchone()
    assert row is not None and row[0] == "wf_existing01"
    # Adopted, not proposed: no pending decision.
    n = db.execute(
        "SELECT COUNT(*) FROM app.account_link_decisions WHERE status='pending'"
    ).fetchone()
    assert n is not None and n[0] == 0


def test_binding_new_mints_standalone(
    db: Database,
) -> None:
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    result = svc.import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "new"},
    )
    assert result.transactions > 0
    # Declared new: source_native maps to a fresh id, NOT the candidate.
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        ["wf-checking"],
    ).fetchone()
    assert row is not None and row[0] != "wf_existing01"
    n = db.execute(
        "SELECT COUNT(*) FROM app.account_link_decisions WHERE status='pending'"
    ).fetchone()
    assert n is not None and n[0] == 0


def test_agent_import_loads_after_answering_the_gate(
    db: Database,
) -> None:
    """The agent's round trip: gated first, then loads once it answers.

    Replaces the former ``does_not_gate_and_queues``, which asserted the agent
    bypassed the gate entirely. The gate is now actor-independent, so the agent
    must answer it — and answering is what lets rows land. Proves the stop is a
    stop, not a dead end.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="agent",
        )
    assert db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone() == (
        0,
    )
    result = svc.import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="agent",
        account_bindings={"wf-checking": "wf_existing01"},
    )
    assert result.transactions > 0


def _minted_account_id(db: Database, source_key: str) -> str:
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [source_key],
    ).fetchone()
    assert row is not None
    return str(row[0])


def test_new_binding_captures_account_metadata(
    db: Database,
) -> None:
    """account_metadata for a 'new' binding writes app.account_settings at mint."""
    svc = ImportService(db)
    svc.import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "new"},
        account_metadata={
            "wf-checking": {
                "display_name": "WF Checking",
                "account_subtype": "checking",
                "last_four": "4267",
                "currency_code": "USD",
            }
        },
    )
    minted = _minted_account_id(db, "wf-checking")
    row = db.execute(
        "SELECT display_name, last_four, account_subtype, currency_code "
        "FROM app.account_settings WHERE account_id=?",
        [minted],
    ).fetchone()
    assert row == ("WF Checking", "4267", "checking", "USD")


def test_account_metadata_rejects_unknown_field_before_any_write(
    db: Database,
) -> None:
    """A typo'd metadata key fails up-front — no rows are written (no orphans)."""
    svc = ImportService(db)
    with pytest.raises(ValueError, match="Unknown account_metadata"):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "new"},
            account_metadata={"wf-checking": {"subtype": "checking"}},
        )
    # Validation runs before any DB write — no orphaned account_links, no
    # raw rows, no settings.
    for table in (
        "app.account_links",
        "raw.tabular_transactions",
        "app.account_settings",
    ):
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # constant table name
        assert n is not None and n[0] == 0, table


def test_account_metadata_rejects_invalid_value_before_any_write(
    db: Database,
) -> None:
    """A malformed value (bad last_four) also fails up-front, before any write."""
    svc = ImportService(db)
    with pytest.raises(ValueError, match="last_four"):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "new"},
            account_metadata={"wf-checking": {"last_four": "42"}},
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0


def test_account_bindings_rejects_unknown_source_key(
    db: Database,
) -> None:
    """A binding for a source key not in the file fails loud, before any write."""
    svc = ImportService(db)
    with pytest.raises(
        ValueError, match="account_bindings references unknown source key"
    ):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"typo-key": "new"},
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0


@pytest.mark.parametrize("bad_value", ["", "   ", "\t"])
def test_account_bindings_rejects_empty_value(db: Database, bad_value: str) -> None:
    """An empty or whitespace-only binding value fails loud, not a silent mint.

    A falsy/whitespace `explicit_account_id` would otherwise skip the
    explicit-adopt path and mint fresh as if no binding was given. CLI input is
    not stripped (`_parse_kv` keeps the raw value) and MCP passes JSON as-is, so
    the guard must reject whitespace-only too, not just the empty string.
    """
    svc = ImportService(db)
    with pytest.raises(ValueError, match="empty value"):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": bad_value},
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0


def test_metadata_not_captured_for_an_adopted_account(
    db: Database,
) -> None:
    """Metadata is dropped when the import adopts an account instead of minting.

    Capture is reserved for genuinely-new mints; an adopted account already has
    its own settings and must not have them overwritten by whatever the file
    happened to say. The sibling case the guard also covers — a pending_review
    provisional, whose id a later merge re-points — is no longer reachable from
    an import: the gate answers every weak candidate before ``resolve()`` runs,
    so the candidate pass never fires here. It stays guarded because the same
    ``resolve()`` serves the ungated sync and backfill callers.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    ImportService(db).import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "wf_existing01"},
        account_metadata={"wf-checking": {"display_name": "Renamed"}},
    )
    n = db.execute("SELECT COUNT(*) FROM app.account_settings").fetchone()
    assert n is not None and n[0] == 0


def test_pending_gauge_counts_distinct_provisionals(
    db: Database,
) -> None:
    """The gauge counts review items (distinct provisionals), not decision rows."""
    from prometheus_client import REGISTRY

    from moneybin.repositories.account_link_decisions_repo import (
        AccountLinkDecisionsRepo,
    )
    from moneybin.services.account_resolver import refresh_account_link_pending_gauge

    # One provisional with two candidate decisions (two weak signals).
    repo = AccountLinkDecisionsRepo(db)
    for cand in ("cand_a", "cand_b"):
        repo.insert(
            decision_id=f"dec_{cand}",
            provisional_account_id="prov_1",
            candidate_account_id=cand,
            confidence_score=0.5,
            match_signals={"signal": "name", "value": "WF"},
            decided_by="auto",
            actor="system",
            match_reason="name",
        )
    refresh_account_link_pending_gauge(db)
    # Two decision rows, but one provisional → one review item.
    gauge = REGISTRY.get_sample_value("moneybin_account_link_review_pending")
    assert gauge == 1.0


def test_resolve_emits_account_link_metrics(
    db: Database,
) -> None:
    """A queued candidate observes confidence and refreshes the pending gauge.

    Driven through ``AccountResolver`` directly rather than an import: after the
    propose-then-bind inversion the gate answers every weak candidate before
    ``resolve()`` runs, so no import path reaches the candidate pass. The
    surviving producers are the Plaid sync resolver and the link backfill, which
    resolve without a gate — this is their wiring.
    """
    from prometheus_client import REGISTRY

    from moneybin.services.account_resolution_types import SourceAccount
    from moneybin.services.account_resolver import AccountResolver

    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    before = REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
    resolved = AccountResolver(db, actor="system").resolve(
        SourceAccount(
            source_type="plaid",
            source_origin="wells_fargo",
            source_account_key="plaid-token-1",
            account_name="WF Checking",
        )
    )
    assert resolved.outcome == "pending_review"
    after = REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
    assert after > before  # at least one candidate confidence observed
    # Gauge was just refreshed from this DB's live pending count (one proposal).
    gauge = REGISTRY.get_sample_value("moneybin_account_link_review_pending")
    assert gauge == 1.0


def test_account_gate_observes_the_confidence_of_every_candidate_it_surfaces(
    db: Database,
) -> None:
    """The gate carries the confidence signal the pending queue used to emit.

    ``resolve()`` observed candidate confidence as it queued. The inversion put
    a gate in front of that, so imports now surface the same candidates and
    never queue — the histogram has to be fed where the decision is made or the
    interactive path goes dark.
    """
    from prometheus_client import REGISTRY

    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    before = REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
        )
    surfaced = sum(len(p["candidates"]) for p in exc.value.outcome.account_proposals)
    assert surfaced > 0
    after = REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
    assert after == before + surfaced
    # Nothing was queued, so the review gauge stays where it was.
    assert db.execute("SELECT COUNT(*) FROM app.account_link_decisions").fetchone() == (
        0,
    )


@pytest.mark.parametrize(
    ("fixture", "channel", "import_kwargs"),
    [
        (_STANDARD_CSV, "tabular", {"account_name": "WF Checking"}),
        (_MINIMAL_OFX, "ofx", {}),
    ],
)
def test_account_gate_counts_a_proposed_confirmation_on_every_channel(
    db: Database,
    fixture: Path,
    channel: str,
    import_kwargs: dict[str, Any],
) -> None:
    """Every channel's account gate is counted, labelled by that channel.

    Without this the confirm the user actually sees is invisible to
    observability, and there is no way to tell an unattended agent stalling on
    account identity from one that never imported.
    """
    from prometheus_client import REGISTRY

    create_core_tables(db)
    labels = {"channel": channel, "tier": "high", "outcome": "proposed"}
    before = REGISTRY.get_sample_value("moneybin_import_confirmations_total", labels)
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(
            fixture, refresh=False, confirm=True, actor_kind="human", **import_kwargs
        )
    after = REGISTRY.get_sample_value("moneybin_import_confirmations_total", labels)
    assert after == (before or 0.0) + 1


def test_bare_single_account_surfaces_account_confirmation(
    db: Database,
) -> None:
    """A single-account file with no identity elicits account_confirmation.

    Was: raised a hard ValueError('Single-account files require …').
    """
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            refresh=False,
            confirm=True,
            actor_kind="human",
        )
    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    # One proposal carrying a stable, bindable source key. No candidates here
    # because dim_accounts is empty (first import) — nothing to pick from. The
    # fallback pick-list is exercised in the next test, with accounts present.
    assert len(outcome.account_proposals) == 1
    proposal = outcome.account_proposals[0]
    assert proposal["source_account_key"].startswith("standard-")
    assert proposal["candidates"] == []
    assert proposal["is_new"] is True
    # No rows loaded — the gate raised before transform/load.
    n = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
    assert n is not None and n[0] == 0


def test_bare_single_account_surfaces_fallback_candidates(
    db: Database,
) -> None:
    """Bare file WITH existing accounts: the gate offers them as a fallback pick-list.

    Regression for the candidates: [] AX gap — a bare single-account import with no
    last4/institution/name signal must still surface the user's existing accounts so
    a human picks from a list instead of supplying a raw account_id.
    """
    _seed_existing_account(
        db, account_id="acct_existing1", display_name="Chase Checking"
    )
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")
    proposal = exc.value.outcome.account_proposals[0]
    cand_ids = [c["account_id"] for c in proposal["candidates"]]
    assert cand_ids == ["acct_existing1"], proposal
    assert proposal["is_new"] is True


def test_bare_single_account_binding_new_mints_and_loads(
    db: Database,
) -> None:
    """Bare file: binding its content key to `new` mints a fresh account and loads."""
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]
    result = svc.import_file(
        _STANDARD_CSV,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )
    assert result.transactions > 0
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key],
    ).fetchone()
    assert row is not None and row[0]
    n = db.execute(
        "SELECT COUNT(*) FROM app.account_link_decisions WHERE status='pending'"
    ).fetchone()
    assert n is not None and n[0] == 0


def test_bare_single_account_binding_adopts_existing_and_loads(
    db: Database,
) -> None:
    """Bare file: binding its content key to an existing id adopts that account."""
    _seed_existing_account(db, account_id="acct_chosen01", display_name="Chosen")
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]
    result = svc.import_file(
        _STANDARD_CSV,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "acct_chosen01"},
    )
    assert result.transactions > 0
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key],
    ).fetchone()
    assert row is not None and row[0] == "acct_chosen01"


def test_bare_single_account_surfaces_for_agent_too(
    db: Database,
) -> None:
    """Bare single-account import surfaces account_confirmation for agents too.

    There is no silent fallback to mint a placeholder account.
    """
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            refresh=False,
            confirm=True,
            actor_kind="agent",
        )
    assert exc.value.outcome.reason == "account_confirmation"
    # No silent mint: nothing loaded, no account_links row created.
    n = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
    assert n is not None and n[0] == 0
    links = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert links is not None and links[0] == 0


def test_same_stem_different_content_do_not_merge(db: Database, tmp_path: Path) -> None:
    """Two different-account files sharing a filename must NOT collide.

    The synthetic key is content-derived, so two `statement.csv` files with
    different content get DISTINCT keys → distinct accounts. An explicit `=new`
    for each is honored; neither silently adopts the other.
    """
    create_core_tables(db)
    svc = ImportService(db)

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    file_a = dir_a / "statement.csv"
    file_a.write_text("Date,Description,Amount\n2026-01-01,BANK A COFFEE,-3.50\n")
    dir_b = tmp_path / "b"
    dir_b.mkdir()
    file_b = dir_b / "statement.csv"
    file_b.write_text("Date,Description,Amount\n2026-02-02,BANK B GROCERY,-9.99\n")

    with pytest.raises(ImportConfirmationRequiredError) as exc_a:
        svc.import_file(file_a, refresh=False, confirm=True, actor_kind="human")
    key_a = exc_a.value.outcome.account_proposals[0]["source_account_key"]
    with pytest.raises(ImportConfirmationRequiredError) as exc_b:
        svc.import_file(file_b, refresh=False, confirm=True, actor_kind="human")
    key_b = exc_b.value.outcome.account_proposals[0]["source_account_key"]

    assert key_a.startswith("statement-")
    assert key_b.startswith("statement-")
    assert key_a != key_b  # same stem, different content → different key

    res_a = svc.import_file(
        file_a,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key_a: "new"},
    )
    res_b = svc.import_file(
        file_b,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key_b: "new"},
    )
    assert res_a.transactions > 0 and res_b.transactions > 0
    acct_a = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key_a],
    ).fetchone()
    acct_b = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key_b],
    ).fetchone()
    assert acct_a is not None and acct_b is not None
    assert acct_a[0] != acct_b[0]  # NOT merged


def test_exact_same_file_reimport_adopts_without_reprompt(
    db: Database,
) -> None:
    """Re-importing the EXACT same bare file adopts the prior account silently.

    Content-key idempotency: no second account_confirmation, no duplicate account minted.
    """
    create_core_tables(db)
    svc = ImportService(db)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]
    first = svc.import_file(
        _STANDARD_CSV,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )
    assert first.transactions > 0
    acct = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key],
    ).fetchone()
    assert acct is not None
    acct_id = acct[0]

    # Re-import the exact same file UNBOUND → must NOT raise; adopts acct_id.
    svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")

    rows = db.execute(
        "SELECT DISTINCT account_id FROM app.account_links "
        "WHERE ref_kind='source_native' AND ref_value=? AND status='accepted'",
        [key],
    ).fetchall()
    assert len(rows) == 1 and rows[0][0] == acct_id  # same account
    total = db.execute(
        "SELECT COUNT(DISTINCT account_id) FROM app.account_links "
        "WHERE status='accepted'"
    ).fetchone()
    assert total is not None and total[0] == 1  # no second account minted


def test_source_native_exists_reflects_accepted_link(
    db: Database,
) -> None:
    """source_native_exists() is the short-circuit's idempotency probe.

    True only after an accepted source_native link maps the exact (source_type,
    source_origin, ref_value) tuple. Driven through the real import path — the
    link is created by binding a bare file, never INSERTed directly.
    """
    from moneybin.services.account_resolver import AccountResolver

    create_core_tables(db)
    svc = ImportService(db)
    resolver = AccountResolver(db, actor="system")

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]

    # No accepted source_native link yet → False (the elicit raised pre-Phase-3).
    assert (
        db.execute(
            "SELECT 1 FROM app.account_links WHERE ref_kind='source_native' "
            "AND ref_value=? AND status='accepted'",
            [key],
        ).fetchone()
        is None
    )
    assert not resolver.source_native_exists("csv", "unknown", key)

    svc.import_file(
        _STANDARD_CSV,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )
    link = db.execute(
        "SELECT source_type, source_origin FROM app.account_links "
        "WHERE ref_kind='source_native' AND ref_value=? AND status='accepted'",
        [key],
    ).fetchone()
    assert link is not None
    source_type, source_origin = link[0], link[1]
    # True for the exact tuple the import wrote...
    assert resolver.source_native_exists(source_type, source_origin, key)
    # ...False for a key that was never imported (same source columns).
    assert not resolver.source_native_exists(
        source_type, source_origin, "never-seen-key"
    )


def test_bare_single_account_mistyped_binding_raises(
    db: Database,
) -> None:
    """A binding whose key doesn't match the bare file's content key fails loud.

    A mistyped `--account-binding <typo>=new` must raise a clear ValueError
    naming the unknown key — not silently re-elicit account_confirmation (which
    would loop a scripted confirm flow). "Magic stays visible."
    """
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ValueError, match="unknown source key"):
        svc.import_file(
            _STANDARD_CSV,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"definitely-not-the-content-key": "new"},
        )


def test_rekey_bare_proposals_repoints_to_moved_path(tmp_path: Path) -> None:
    """Repoint a bare content-key proposal to the collision-moved path's key.

    The digest is unchanged (same bytes); only the stem changes, and data-derived
    keys are left untouched. Mirrors the inbox collision-suffix move
    (statement.csv -> statement-1.csv) that changes the stem after the proposal
    was built from the original name.
    """
    from moneybin.services.account_resolution_types import AccountProposal
    from moneybin.services.import_service import (
        _bare_account_key,  # pyright: ignore[reportPrivateUsage]  # tested directly
        rekey_bare_proposals_for_path,
    )

    original = tmp_path / "statement.csv"
    original.write_text("Date,Amount\n2026-01-01,-5.00\n")
    orig_key = _bare_account_key(original)
    moved = tmp_path / "statement-1.csv"
    original.rename(moved)  # same bytes, new stem (the collision-suffix move)

    proposals = [
        AccountProposal(
            source_account_key=orig_key,
            proposed_account_id=None,
            is_new=True,
            candidates=(),
            adopted_via=None,
        ).to_dict(),
        AccountProposal(
            source_account_key="wf-checking",  # a real data-derived key
            proposed_account_id=None,
            is_new=True,
            candidates=(),
            adopted_via=None,
        ).to_dict(),
    ]
    rekey_bare_proposals_for_path(proposals, moved)

    assert proposals[0]["source_account_key"] == _bare_account_key(moved)
    assert proposals[0]["source_account_key"] != orig_key  # stem changed
    assert proposals[0]["source_account_key"].startswith("statement-1-")
    assert proposals[1]["source_account_key"] == "wf-checking"  # untouched


# --- the gate reaches every channel, not just tabular ---------------------


def test_ofx_import_gates_before_raw_ingest(
    db: Database,
) -> None:
    """OFX resolves account identity with a pre-load confirm, like every channel.

    The gap PR #375 named and left open: ``_import_ofx`` ran the resolver *after*
    ``ingest_dataframe``, so an OFX file bound its account identity — minting or
    adopting — with no confirm at all, and the rows were already in raw by the
    time anything was written. Only the tabular path stopped and asked.

    The gate must raise before ``begin_import``, so a gated OFX import leaves no
    ``raw.import_log`` row either: an import that never started should not appear
    in history as a failure.
    """
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True, actor_kind="human")
    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    assert outcome.channel == "ofx"
    # The OFX <ACCTID> is the bindable source key.
    assert [p["source_account_key"] for p in outcome.account_proposals] == ["1111"]
    # Nothing loaded, nothing linked, no batch opened.
    for table, expected in (
        ("raw.ofx_transactions", 0),
        ("raw.ofx_accounts", 0),
        ("app.account_links", 0),
        ("raw.import_log", 0),
    ):
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list, not user input
        assert n is not None and n[0] == expected, table


def test_ofx_binding_new_mints_and_loads(
    db: Database,
) -> None:
    """Answering the OFX gate with `new` mints the account and completes the import."""
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]

    result = svc.import_file(
        _MINIMAL_OFX,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )
    assert result.transactions == 2
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key],
    ).fetchone()
    assert row is not None and row[0]


def test_ofx_reimport_does_not_re_ask(
    db: Database,
) -> None:
    """A remembered binding never re-asks — the confirm costs one answer, once.

    The second import hits ``source_native`` in the resolution ladder, so the
    proposal comes back ``adopted_via='source_native'`` and ``requires_confirm``
    is False. This is what keeps the gate's volume tied to new identities rather
    than to files.
    """
    create_core_tables(db)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]
    svc.import_file(
        _MINIMAL_OFX,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )
    # Same file again (force, since re-import detection is a separate mechanism):
    # no second confirm, because the identity is already bound.
    result = svc.import_file(
        _MINIMAL_OFX, refresh=False, confirm=True, actor_kind="human", force=True
    )
    assert result.transactions == 2
