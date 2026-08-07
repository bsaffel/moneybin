"""M1S.4 — import-time account-binding gate + bindings (service level).

Exercises the conditional gate (interactive human first contact surfaces weak
account-merge candidates; agent / non-interactive load and queue) and the
account_bindings resolution map through the real import_file pipeline.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
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


# One existing account per fixture that the file's own institution + last four
# weakly match. Candidates are what make an account identity a *question*: a
# mint with nothing to merge into proceeds and is reported instead, so any test
# whose subject is the gate has to give the resolver something the file could
# plausibly BE. That is also the case the gate exists for — a silent weak merge
# is the expensive, hard-to-undo mistake.
_OFX_TWIN = {"institution_slug": "sample-bank", "last_four": "1111"}
_PDF_TWIN = {"institution_slug": "chase", "last_four": "1234"}


def _seed_twin(
    db: Database,
    twin: dict[str, str],
    *,
    account_id: str = "acct_twin01",
    display_name: str = "Existing Account",
) -> str:
    """Seed the weak institution+last4 match that makes a file's identity a question."""
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts "  # noqa: S608  # test fixture
        "(account_id, display_name, institution_slug, last_four) VALUES (?, ?, ?, ?)",
        [account_id, display_name, twin["institution_slug"], twin["last_four"]],
    )
    return account_id


def _seed_tabular_twin(db: Database) -> None:
    """The tabular fixtures name their account, so the weak signal is the name."""
    _seed_existing_account(db, account_id="acct_twin01", display_name="WF Checking")


def _seed_ofx_twin(db: Database) -> None:
    _seed_twin(db, _OFX_TWIN)


def _seed_pdf_twin(db: Database) -> None:
    _seed_twin(db, _PDF_TWIN)


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
    # Gate raised before transform/load: nothing landed, and no batch opened.
    # The `raw.import_log` half matters as much as the rows: an import that
    # never started must not appear in history as a failure. Its OFX and PDF
    # siblings assert the same pair, so tabular — the channel that had the
    # confirm first — is not the one left without the guard.
    for table, expected in (
        ("raw.tabular_transactions", 0),
        ("app.account_links", 0),
        ("raw.import_log", 0),
    ):
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list, not user input
        assert n is not None and n[0] == expected, table


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
    # Same pre-load guarantee the human path gets, asserted the same way — the
    # agent path is the one most likely to run unattended, so it is the last
    # place to accept a weaker check than its human twin.
    for table, expected in (
        ("raw.tabular_transactions", 0),
        ("app.account_links", 0),
        ("raw.import_log", 0),
    ):
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list, not user input
        assert n is not None and n[0] == expected, table


def test_a_named_first_contact_mint_loads_without_asking(
    db: Database,
) -> None:
    """A stated identity with nothing to merge into proceeds; the mint is reported.

    The book is empty, so the candidate pass finds nothing and there is no
    second answer the user could give — on a fresh database "new" is the only
    possibility. Gating it made a first import of N files cost N confirms that
    each had exactly one legal answer, which is confirm volume scaling with
    items rather than with uncertainty.

    The caller named this account (``account_name``), so the mint records an
    identity that was stated rather than guessed — unlike a bare CSV, which
    still gates (``identity_unknown``). Contrast
    ``test_human_import_gates_on_weak_account_candidate``: add one account this
    file could plausibly be, and the same import stops.
    """
    create_core_tables(db)
    result = ImportService(db).import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
    )
    assert result.transactions > 0
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        ["wf-checking"],
    ).fetchone()
    assert row is not None and row[0]
    # Reported, not merely allowed through: the account carries the name the
    # caller gave it, so "WF Checking appeared" is answerable from the result
    # alone. See test_a_mint_is_reported_on_every_channel for the invariant.
    assert [(a.account_id, a.display_name) for a in result.accounts_created] == [
        (row[0], "WF Checking")
    ]


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


def test_mint_is_announced_under_the_name_it_will_actually_carry(
    db: Database,
) -> None:
    """The reported name must be the one the user can find afterwards.

    ``_capture_new_account_metadata`` writes ``display_name`` to
    ``app.account_settings``, and ``dim_accounts`` COALESCEs that arm first — so
    announcing the source label instead named a row that does not exist under
    that name anywhere the user looks. This is the visible half of "gate the
    merge, not the mint": a mint is reported rather than confirmed precisely
    because the report is enough to notice and correct it.
    """
    result = ImportService(db).import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "new"},
        account_metadata={"wf-checking": {"display_name": "Joint Checking"}},
    )

    assert [a.display_name for a in result.accounts_created] == ["Joint Checking"]
    minted = _minted_account_id(db, "wf-checking")
    settings = db.execute(
        "SELECT display_name FROM app.account_settings WHERE account_id=?",
        [minted],
    ).fetchone()
    assert settings == ("Joint Checking",)


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

    (_seed_ofx_twin if channel == "ofx" else _seed_tabular_twin)(db)
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


def test_the_gate_writes_no_link_until_the_binding_answers_it(
    db: Database,
) -> None:
    """The accepted source_native link appears only once the caller has answered.

    Two properties in one pass, because they are the same property from either
    side: the gate raises before Phase 3, so an unanswered import leaves nothing
    behind to adopt on a retry; and answering it writes exactly one accepted
    link, keyed by this file's own source key, which is what makes the NEXT
    import of the same file idempotent instead of a second confirm.

    Driven through the real import path — the link is created by binding a bare
    file, never INSERTed directly.
    """
    create_core_tables(db)
    svc = ImportService(db)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_STANDARD_CSV, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]

    def _accepted_native() -> list[tuple[str, ...]]:
        return [
            tuple(row)
            for row in db.execute(
                "SELECT source_type, source_origin, ref_value FROM app.account_links "
                "WHERE ref_kind='source_native' AND status='accepted'"
            ).fetchall()
        ]

    assert _accepted_native() == []

    svc.import_file(
        _STANDARD_CSV,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )

    written = _accepted_native()
    assert len(written) == 1
    assert written[0][2] == key


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
        ).to_dict(proposal_ref="@0"),
        AccountProposal(
            source_account_key="wf-checking",  # a real data-derived key
            proposed_account_id=None,
            is_new=True,
            candidates=(),
            adopted_via=None,
        ).to_dict(proposal_ref="@1"),
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
    _seed_twin(db, _OFX_TWIN)
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
    """Answering the OFX gate with `new` mints a distinct account and loads the rows.

    The seeded twin is what raises the gate, so `new` carries its real meaning
    here: "not that existing account — keep this one separate."
    """
    _seed_twin(db, _OFX_TWIN)
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
    than to files. The seeded twin makes the FIRST import ask; the assertion is
    that the second one does not.
    """
    _seed_twin(db, _OFX_TWIN)
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


# --- positional proposal refs --------------------------------------------
# The binding map is keyed on source_account_key, which every channel derives
# from file *content* (OFX <ACCTID>, PDF issuer+last4, tabular's account-name
# column). That key is an ACCOUNT_IDENTIFIER, so any masking surface hands it
# back as ****1234 and a caller reading the gate cannot reproduce it. The
# proposal therefore also carries a positional referent — "@0" is the file's
# first source account — which names nothing about the account and is
# reproducible from the same bytes on the answering call.


def _pdf_fixture(tmp_path: Path) -> Path:
    from tests.moneybin.pdf_statement_fixtures import write_card_statement_pdf

    return write_card_statement_pdf(tmp_path)


def _standard_csv(_tmp: Path) -> Path:
    return _STANDARD_CSV


def _minimal_ofx(_tmp: Path) -> Path:
    return _MINIMAL_OFX


@pytest.mark.parametrize(
    ("channel", "make_file", "import_kwargs", "seed_twin"),
    [
        ("tabular", _standard_csv, {"account_name": "WF Checking"}, _seed_tabular_twin),
        ("ofx", _minimal_ofx, {}, _seed_ofx_twin),
        ("pdf", _pdf_fixture, {}, _seed_pdf_twin),
    ],
)
def test_a_positional_ref_answers_the_gate_on_every_channel(
    db: Database,
    tmp_path: Path,
    channel: str,
    make_file: Callable[[Path], Path],
    import_kwargs: dict[str, Any],
    seed_twin: Callable[[Database], None],
) -> None:
    """Every channel's gate offers a ref, and binding by it loads the file.

    This is the uniformity assertion: the three channels differ only in the
    content they start from, so one referent has to work identically on all of
    them. Parametrized rather than split so a channel that grows its own
    binding vocabulary fails here.
    """
    seed_twin(db)
    svc = ImportService(db)
    path = make_file(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(path, refresh=False, confirm=True, **import_kwargs)
    proposal = exc.value.outcome.account_proposals[0]
    assert proposal["proposal_ref"] == "@0", proposal

    # Answering by the ref alone — never naming source_account_key — loads.
    svc.import_file(
        path,
        refresh=False,
        confirm=True,
        account_bindings={"@0": "new"},
        **import_kwargs,
    )
    linked = db.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native'"
    ).fetchone()
    assert linked is not None and linked[0] >= 1, f"{channel}: no link written"


def _two_account_csv(tmp_path: Path) -> Path:
    """A CSV whose two accounts are named by a column, not by the caller."""
    path = tmp_path / "two_accounts.csv"
    path.write_text(
        "Date,Description,Amount,Account Name\n"
        "2026-01-15,Coffee Shop,-12.50,Checking\n"
        "2026-01-17,Groceries,-85.30,Savings\n"
    )
    return path


def test_the_ref_indexes_the_files_accounts_in_order(
    db: Database, tmp_path: Path
) -> None:
    """Refs number the file's own account list, and every one of them binds.

    The answering call applies bindings *before* the gate runs, so it cannot
    know which accounts would have been surfaced. Numbering the surfaced subset
    instead would shift every ref the moment one account resolved strongly,
    binding the caller's answer onto the wrong account — silently, which is the
    failure this whole gate exists to prevent.
    """
    # One twin per account in the file, so BOTH gate and both refs are exercised.
    # With only one seeded, the unmatched account would mint straight through and
    # this test would silently assert against a one-element list.
    _seed_existing_account(db, account_id="acct_chk01", display_name="Checking")
    _seed_existing_account(db, account_id="acct_sav01", display_name="Savings")
    svc = ImportService(db)
    csv_path = _two_account_csv(tmp_path)
    with pytest.raises(ImportConfirmationRequiredError) as first:
        svc.import_file(csv_path, refresh=False, confirm=True)
    refs = [p["proposal_ref"] for p in first.value.outcome.account_proposals]
    assert refs == ["@0", "@1"], refs

    svc.import_file(
        csv_path,
        refresh=False,
        confirm=True,
        account_bindings={"@0": "new", "@1": "new"},
    )
    names = {
        r[0]
        for r in db.execute(
            "SELECT ref_value FROM app.account_links WHERE ref_kind = 'source_native'"
        ).fetchall()
    }
    assert len(names) == 2, names


def test_a_raw_source_key_still_binds(db: Database) -> None:
    """The ref is additive — the CLI's existing key= form keeps working."""
    _seed_twin(db, _OFX_TWIN)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True)
    key = exc.value.outcome.account_proposals[0]["source_account_key"]

    svc.import_file(
        _MINIMAL_OFX, refresh=False, confirm=True, account_bindings={key: "new"}
    )
    linked = db.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native'"
    ).fetchone()
    assert linked is not None and linked[0] == 1


@pytest.mark.parametrize(
    ("channel", "make_file", "import_kwargs"),
    [
        ("tabular", _standard_csv, {"account_name": "WF Checking"}),
        ("ofx", _minimal_ofx, {}),
        ("pdf", _pdf_fixture, {}),
    ],
)
def test_a_mistyped_source_key_is_refused_on_every_channel(
    db: Database,
    tmp_path: Path,
    channel: str,
    make_file: Callable[[Path], Path],
    import_kwargs: dict[str, Any],
) -> None:
    """A binding naming no account in the file fails loud, before any write.

    Silently dropping it re-raises the identical gate the caller was answering,
    with nothing to distinguish "your key was wrong" from "you didn't answer" —
    the caller re-sends the same binding and loops. Lived on tabular only until
    the check moved into the shared binding application, so OFX and PDF spent
    the whole gate rollout swallowing typos.
    """
    create_core_tables(db)
    svc = ImportService(db)
    path = make_file(tmp_path)

    with pytest.raises(
        ValueError, match="account_bindings references unknown source key"
    ):
        svc.import_file(
            path,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"typo-key": "new"},
            **import_kwargs,
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0, f"{channel}: wrote a link"


@pytest.mark.parametrize(
    ("channel", "make_file", "import_kwargs"),
    [
        ("tabular", _standard_csv, {"account_name": "WF Checking"}),
        ("ofx", _minimal_ofx, {}),
        ("pdf", _pdf_fixture, {}),
    ],
)
def test_a_mint_is_reported_on_every_channel(
    db: Database,
    tmp_path: Path,
    channel: str,
    make_file: Callable[[Path], Path],
    import_kwargs: dict[str, Any],
) -> None:
    """An unasked mint has to say what it created — this is what replaced the gate.

    Gating a first-contact mint was the old way of making it visible, and it
    cost one confirm per file for a question with exactly one legal answer.
    Dropping that gate is only honest if the result names the accounts the
    import created; otherwise an account materialises with nothing anywhere
    pointing at it. Set equality, not a count: reporting an id the import did
    not bind is as wrong as omitting one it did.
    """
    create_core_tables(db)
    result = ImportService(db).import_file(
        make_file(tmp_path),
        refresh=False,
        confirm=True,
        actor_kind="human",
        **import_kwargs,
    )
    bound = {
        r[0]
        for r in db.execute(
            "SELECT account_id FROM app.account_links "
            "WHERE ref_kind = 'source_native' AND status = 'accepted'"
        ).fetchall()
    }
    assert {a.account_id for a in result.accounts_created} == bound, channel
    # A name the user can recognise the account by. The id alone is opaque, so
    # an empty label would report the mint without identifying it.
    assert all(a.display_name for a in result.accounts_created), channel


def test_a_reimport_reports_no_new_account(db: Database) -> None:
    """Adoption is not creation — only the import that minted names the account.

    Without this the report degenerates into "here are the accounts this file
    touches", printed on every re-import, and stops meaning anything happened.
    """
    create_core_tables(db)
    svc = ImportService(db)
    first = svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True)
    assert len(first.accounts_created) == 1

    # force=True because re-import detection is a separate mechanism; the point
    # here is that the second resolve adopts via source_native and mints nothing.
    again = svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True, force=True)
    assert again.accounts_created == ()


# An account signal each channel's import path cannot forward. The grid is the
# test: a guard written for one signal silently drops the others, and dropping
# one binds the rows to whatever the extractor inferred while the caller
# believes they chose.
_UNHONORED_SIGNALS = [
    ("ofx", _minimal_ofx, {"account_id": "acct_existing1"}),
    ("ofx", _minimal_ofx, {"account_name": "Checking"}),
    ("ofx", _minimal_ofx, {"account_metadata": {"k": {"display_name": "X"}}}),
    ("pdf", _pdf_fixture, {"account_name": "Checking"}),
    ("pdf", _pdf_fixture, {"account_metadata": {"k": {"display_name": "X"}}}),
]


@pytest.mark.parametrize(
    ("channel", "make_file", "signal"),
    _UNHONORED_SIGNALS,
    ids=[f"{c}-{next(iter(s))}" for c, _f, s in _UNHONORED_SIGNALS],
)
def test_an_account_signal_the_channel_cannot_honor_is_refused(
    db: Database,
    tmp_path: Path,
    channel: str,
    make_file: Callable[[Path], Path],
    signal: dict[str, Any],
) -> None:
    """Dropping one silently is the failure that is impossible to notice.

    Each of these reaches only ``_import_tabular`` (``account_id`` also reaches
    PDF). On the other channels the argument was accepted and discarded, so the
    import bound the account the extractor inferred while the caller believed
    they had chosen one — wrong data, no error, and nothing at the call site to
    suggest looking. ``account_bindings`` is the per-account answer that every
    channel does honor, so the refusal names it.
    """
    create_core_tables(db)
    with pytest.raises(UserError, match="account_bindings"):
        ImportService(db).import_file(
            make_file(tmp_path),
            refresh=False,
            confirm=True,
            actor_kind="human",
            **signal,
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0, f"{channel}: wrote a link"


def test_the_channel_that_honors_a_signal_still_takes_it(db: Database) -> None:
    """The guard must refuse by channel, not by signal name.

    Without this the parametrized refusals above stay green if the guard grows
    too broad and starts rejecting tabular's own account arguments.
    """
    create_core_tables(db)
    result = ImportService(db).import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        account_metadata={"wf-checking": {"last_four": "4267"}},
        refresh=False,
        confirm=True,
        actor_kind="human",
    )
    assert result.transactions > 0


def test_an_out_of_range_ref_is_refused(db: Database) -> None:
    """A ref past the end of a 1-account file is an error, not a silent no-op.

    Left unchecked it falls through as "no binding for this account" and the
    import re-gates, telling the caller nothing about why their answer did not
    take.
    """
    create_core_tables(db)
    with pytest.raises(ValueError, match="@7"):
        ImportService(db).import_file(
            _MINIMAL_OFX, refresh=False, confirm=True, account_bindings={"@7": "new"}
        )


def test_a_ref_and_a_raw_key_disagreeing_on_one_account_is_refused(
    db: Database,
) -> None:
    """Two answers for one account must not resolve by precedence.

    Picking a winner silently binds an account to one of two ids the caller
    asked for — the unrecoverable-by-surprise merge this gate exists to stop.
    """
    _seed_twin(db, _OFX_TWIN)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True)
    key = exc.value.outcome.account_proposals[0]["source_account_key"]

    with pytest.raises(ValueError, match="conflicting"):
        svc.import_file(
            _MINIMAL_OFX,
            refresh=False,
            confirm=True,
            account_bindings={key: "new", "@0": "acct-other"},
        )


def test_a_binding_may_not_contradict_an_explicit_account_id(
    db: Database,
) -> None:
    """``account_id`` and ``account_bindings`` are two answers to one question.

    ``_resolve_binding_targets`` already refuses two conflicting bindings for
    one account, for the reason it states — the caller never learns which of the
    ids they named won. A pin plus a contradicting binding is the same conflict
    arriving through two parameters instead of one, and the binding used to win
    silently, discarding the pin.
    """
    svc = ImportService(db)
    with pytest.raises(ValueError, match="pinned"):
        svc.import_file(
            _STANDARD_CSV,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_pinned01",
            account_bindings={"@0": "acct_other02"},
        )


def test_restating_the_pin_as_a_binding_is_not_a_conflict(db: Database) -> None:
    """Re-sending the same id both ways is agreement, not contradiction.

    An agent answering a gate re-sends the parameters it already had alongside
    the new binding, so refusing agreement would punish the normal recovery.
    """
    svc = ImportService(db)
    result = svc.import_file(
        _STANDARD_CSV,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_id="acct_pinned01",
        account_bindings={"@0": "acct_pinned01"},
    )
    assert result.transactions > 0


def test_a_source_key_shaped_like_a_ref_binds_only_its_own_account() -> None:
    """A file whose own key reads as "@0" must not also answer proposal zero.

    ``source_account_key`` is untrusted file content on the OFX channel — it is
    the ``<ACCTID>`` verbatim — so a key of "@0" collided with the positional
    vocabulary and bound BOTH accounts to one id: exactly the silent merge the
    gate exists to prevent, reached through the answer rather than the file.
    """
    from moneybin.services.account_resolution_types import SourceAccount
    from moneybin.services.import_service import (
        _resolve_binding_targets,  # pyright: ignore[reportPrivateUsage]
    )

    def _src(key: str) -> SourceAccount:
        return SourceAccount(
            source_type="ofx",
            source_origin="sample-bank",
            source_account_key=key,
            account_name=f"sample-bank {key}",
        )

    targets = _resolve_binding_targets(
        [_src("1111"), _src("@0")], {"@0": "acct_target01"}
    )
    assert targets == [None, "acct_target01"]


def test_one_account_split_across_statements_asks_once(
    db: Database, tmp_path: Path
) -> None:
    """Two <STMTRS> blocks for one card are one account, not two.

    ofxparse accumulates one ``Account`` per statement response with no ACCTID
    de-dup, so an export that splits a period into two blocks surfaced the same
    card as both "@0" and "@1". Answering "@0" left "@1" unbound and re-raised
    the identical gate; answering both with different ids wrote the native
    mapping for one key under two canonical accounts.
    """
    _seed_twin(db, _OFX_TWIN)
    source = _MINIMAL_OFX.read_text(encoding="utf-8")
    body_start = source.index("<STMTTRNRS>")
    body_end = source.index("</STMTTRNRS>") + len("</STMTTRNRS>")
    split = tmp_path / "two_statements.ofx"
    split.write_text(
        source[:body_end] + source[body_start:body_end] + source[body_end:],
        encoding="utf-8",
    )

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).import_file(split, refresh=False, confirm=True)

    assert len(exc.value.outcome.account_proposals) == 1
