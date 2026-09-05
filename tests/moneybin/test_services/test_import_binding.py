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
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.import_confirmation import ImportConfirmationRequiredError
from moneybin.services.import_service import ImportService
from tests.import_helpers import import_answering_gate
from tests.moneybin.db_helpers import create_core_tables

_STANDARD_CSV = Path(__file__).parents[2] / "fixtures" / "tabular" / "standard.csv"
_MINIMAL_OFX = Path(__file__).parents[2] / "fixtures" / "ofx" / "sample_minimal.ofx"
_MULTI_ACCOUNT_OFX = (
    Path(__file__).parents[2] / "fixtures" / "ofx" / "multi_account_sample.ofx"
)


def _seed_existing_account(db: Database, *, account_id: str, display_name: str) -> None:
    """Materialize core.dim_accounts with one account the name pass can match.

    ``display_name_is_user_set=TRUE``: the resolver's name signal now requires
    a user-chosen name (not a generated descriptor), and every fixture here
    stands in for one.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name, "  # noqa: S608  # test fixture
        "display_name_is_user_set) VALUES (?, ?, TRUE)",
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
        "(account_id, display_name, institution_slug, last_four, "
        "display_name_is_user_set) VALUES (?, ?, ?, ?, TRUE)",
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


# An <ACCTID> pair long enough that no other field could produce either by
# accident, so an assertion that one did NOT reach a surface means what it says.
_FIRST_ACCTID = "000123456789"
_SECOND_ACCTID = "000987654321"


def _multi_account_ofx(tmp_path: Path) -> Path:
    """The two-account fixture, re-keyed to account numbers worth protecting."""
    source = _MULTI_ACCOUNT_OFX.read_text(encoding="utf-8")
    ofx = tmp_path / "two-accounts.ofx"
    ofx.write_text(
        source.replace(
            "<ACCTID>CHECKING1</ACCTID>", f"<ACCTID>{_FIRST_ACCTID}</ACCTID>"
        ).replace("<ACCTID>SAVINGS1</ACCTID>", f"<ACCTID>{_SECOND_ACCTID}</ACCTID>"),
        encoding="utf-8",
    )
    return ofx


def _seed_both_multi_bank_twins(db: Database) -> None:
    """A weak twin for each account, so both identities are real questions."""
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts "  # noqa: S608  # test fixture
        "(account_id, display_name, institution_slug, last_four, "
        "display_name_is_user_set) VALUES (?, ?, ?, ?, TRUE), (?, ?, ?, ?, TRUE)",
        [
            "acct_first01",
            "First Twin",
            "multi-bank",
            _FIRST_ACCTID[-4:],
            "acct_second01",
            "Second Twin",
            "multi-bank",
            _SECOND_ACCTID[-4:],
        ],
    )


def test_an_answered_account_leaves_its_ref_behind_not_its_number(
    db: Database, tmp_path: Path
) -> None:
    """The gate re-keys the caller's answers, because only it can.

    A two-account statement is answered one at a time, and the second call has
    to replay the first answer or the gate re-asks and never converges. But an
    account a binding already decided is skipped by ``_gate_account_proposals``,
    so it is *absent* from ``account_proposals`` — a surface replaying the
    caller's own key has no proposal to re-key it from and echoes it verbatim.
    On OFX that key is the ``<ACCTID>``, an account number, and the CLI renders
    the replay into ``actions[]``, which sits outside the redaction walk.

    ``ratified_bindings`` closes that: the ref is the account's index in the
    file, so this layer is the only one that can name it once the proposal is
    gone.
    """
    ofx = _multi_account_ofx(tmp_path)
    _seed_both_multi_bank_twins(db)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).import_file(
            ofx,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={_FIRST_ACCTID: "acct_first01"},
        )

    outcome = exc.value.outcome
    # The answered account is gone from the gate — that absence is the defect's
    # whole cause, so assert it rather than assume it.
    assert [p["source_account_key"] for p in outcome.account_proposals] == [
        _SECOND_ACCTID
    ]
    assert [p["proposal_ref"] for p in outcome.account_proposals] == ["@1"]
    # ...and this is where its answer survives instead: keyed by ref, never by
    # the number the caller had to type.
    assert outcome.ratified_bindings == {"@0": "acct_first01"}


def test_account_metadata_takes_the_ref_the_confirmation_showed(
    db: Database, tmp_path: Path
) -> None:
    """Metadata has to accept the referent the gate actually handed back.

    The confirmation masks ``source_account_key``, so on the surfaces this PR
    added the ref is the only key a caller can read. ``account_bindings`` takes
    it; ``account_metadata`` compared against source keys only, and its refusal
    then asked for a key the caller had no way to obtain. That makes naming a
    newly minted account unusable on exactly the path the mask created.

    The ref is read off the raised proposal rather than written as ``"@0"``, so
    this asserts the round trip a caller actually performs.
    """
    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")
    svc = ImportService(db)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(csv, refresh=False, confirm=True, actor_kind="human")
    ref = str(exc.value.outcome.account_proposals[0]["proposal_ref"])

    result = svc.import_file(
        csv,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={ref: "new"},
        account_metadata={ref: {"display_name": "Joint Checking"}},
    )

    assert [a.display_name for a in result.accounts_created] == ["Joint Checking"]


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

    "Cash (...7777)" must arrive as "Cash"; otherwise the mask text sinks the
    fuzzy-name ratio below threshold (SequenceMatcher("cash (...7777)", "cash")
    ~= 0.44 < 0.6) and a duplicate is silently minted instead of surfacing the
    existing-account candidate. With no institution resolved, name is the only
    signal.
    """
    _seed_existing_account(db, account_id="cash_existing01", display_name="Cash")
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(
            _STANDARD_CSV,
            account_name="Cash (...7777)",
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
                "last_four": "1212",
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
    assert row == ("WF Checking", "1212", "checking", "USD")


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


def test_padded_metadata_is_normalized_before_it_is_announced_or_stored(
    db: Database,
) -> None:
    """A caller's stray space reaches the name unless something trims it.

    ``account_metadata`` arrives as a raw dict — the MCP parameter hands it to
    the service verbatim, with no CLI-side strip in between — and its two
    fields failed differently. ``display_name`` was padded on both sides at
    once, so nothing *disagreed* and the account was simply named with the
    padding. ``account_subtype`` was the worse half: the mint report's mirror
    trims it (``account_display_name._stated``) while ``dim_accounts``
    COALESCEs the stored column with no ``TRIM``, so the two readers split.
    Normalizing once at the settings boundary closes both.
    """
    result = ImportService(db).import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "new"},
        account_metadata={
            "wf-checking": {
                "display_name": "  Joint Checking  ",
                "account_subtype": "  savings  ",
            }
        },
    )

    assert [a.display_name for a in result.accounts_created] == ["Joint Checking"]
    minted = _minted_account_id(db, "wf-checking")
    settings = db.execute(
        "SELECT display_name, account_subtype FROM app.account_settings"
        " WHERE account_id=?",
        [minted],
    ).fetchone()
    assert settings == ("Joint Checking", "savings")


def test_account_metadata_rejects_the_reserved_label_before_any_write(
    db: Database,
) -> None:
    """The import path enforces the same reservation `accounts set` does.

    `_capture_new_account_metadata` writes straight through
    `AccountSettingsRepo`, whose only validation is
    `AccountSettings.__post_init__` -- lengths and shapes, not vocabulary. Left
    unguarded this path stores the fold `AccountService.settings_update`
    refuses, and re-running the same import re-creates it after any rename.
    Case-folded here so the guard cannot narrow to a byte comparison.
    """
    svc = ImportService(db)
    with pytest.raises(ValueError, match="reserved"):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "new"},
            account_metadata={"wf-checking": {"display_name": "unnamed  account"}},
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_settings").fetchone()
    assert n is not None and n[0] == 0


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


def test_a_binding_to_an_unknown_account_id_loads_nothing(db: Database) -> None:
    """A mistyped account_id is refused, not adopted as an identity nobody made.

    Step 0 of the ladder adopts ``explicit_account_id`` verbatim and reports
    ``outcome="adopted_strong"``, ``is_new=False`` — so a typo used to become a
    canonical account with no mint announced and no ``accounts_created`` entry
    naming it. The statement's rows then landed under an id the caller invented
    by accident, under a name no surface would ever show them. Nothing about
    that is visible afterwards, which is the silent mis-delivery this gate
    exists to stop.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    with pytest.raises(ValueError, match="wf_existng01"):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "wf_existng01"},
        )
    n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 0


@pytest.mark.parametrize("near_miss", ["New", "NEW", "new ", " new"])
def test_a_binding_that_misspells_new_is_refused(db: Database, near_miss: str) -> None:
    """The mint keyword is exact; a near miss is named rather than guessed at.

    Only the lowercase token mints — every other value is read as an existing
    account_id, so a shift key turned "make a new account" into "adopt the
    account called New", which the unknown-id refusal above would then report
    in the vocabulary of ids rather than of the keyword the caller meant.
    Quietly folding case and surrounding space into the keyword is the other
    wrong answer: an account_id is opaque, so ``"New"`` cannot be *proven* to
    be the keyword rather than an id, and guessing is the magic this feature
    exists to remove. Name the near miss instead.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    with pytest.raises(ValueError, match='exactly "new"'):
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": near_miss},
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
            account_name_is_user_set=True,
        )
    )
    assert resolved.outcome == "pending_review"
    after = REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
    assert after > before  # at least one candidate confidence observed
    # Gauge was just refreshed from this DB's live pending count (one proposal).
    gauge = REGISTRY.get_sample_value("moneybin_account_link_review_pending")
    assert gauge == 1.0


def test_the_gate_surfaces_ledger_evidence_without_a_constant_confidence_score(
    db: Database,
) -> None:
    """A surfaced candidate carries measured overlap, never a per-signal constant.

    ``confidence`` was a literal per rung that no input could move, so two
    candidates found on the same rung tied at the same number however differently
    their ledgers overlapped — and the field's name invited whoever answered the
    gate to read that tie as a judgement. The review queue
    (``LinkCandidateRow``) and the decision history (``LinkHistoryRow``) each
    dropped it for exactly that reason; this pins the import gate to the same
    shape, leaving ``signal`` plus the overlap measurement as the evidence.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
        )
    candidates = [
        candidate
        for proposal in exc.value.outcome.account_proposals
        for candidate in proposal["candidates"]
    ]
    assert candidates, "the gate must surface a candidate for this to say anything"
    for candidate in candidates:
        assert "confidence" not in candidate
        assert "signal" in candidate


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


def _second_ofx_statement(tmp_path: Path) -> Path:
    """A later statement for the SAME <ACCTID>, carrying rows the first one lacks.

    The distinct FITIDs are the point: ingest upserts on them, so re-importing
    the original file would leave the row count flat and make the assertion
    that nothing new landed vacuous.
    """
    path = tmp_path / "sample_minimal_february.ofx"
    path.write_text(
        _MINIMAL_OFX
        .read_text()
        .replace("FITID001", "FITID003")
        .replace("FITID002", "FITID004")
    )
    return path


def test_an_ofx_binding_that_contradicts_a_remembered_link_loads_nothing(
    db: Database, tmp_path: Path
) -> None:
    """A binding the resolver will reject must stop before raw ingest, not after.

    ``_gate_account_proposals`` skips any account already carrying
    ``explicit_account_id``, so a binding walks past the gate untouched. OFX
    then ingested all four raw frames *before* ``resolver.resolve()`` reached
    ``_write_native_mapping``'s conflict guard, and that guard's ``except`` only
    finalized the batch ``failed`` — it deleted nothing.
    ``prep.stg_ofx__transactions`` filters on ``_row_num``, never on import
    status, and its LEFT JOIN still found the *pre-existing* link. So the caller
    asked for account B, was told the import failed, and got the rows under
    account A anyway: exactly the silent mis-attribution this gate exists to
    prevent, reached through the one path that skips the gate.
    """
    twin = _seed_twin(db, _OFX_TWIN)
    svc = ImportService(db)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        svc.import_file(_MINIMAL_OFX, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]
    # Answer "new": the <ACCTID> is now remembered against a freshly minted
    # account, which is what the next binding will contradict.
    svc.import_file(
        _MINIMAL_OFX,
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={key: "new"},
    )

    with pytest.raises(ValueError, match="already accepted"):
        svc.import_file(
            _second_ofx_statement(tmp_path),
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={key: twin},
        )

    # The refusal costs nothing downstream: the first import's two rows are all
    # that exist, and no second batch was ever opened.
    for table, expected in (
        ("raw.ofx_transactions", 2),
        ("raw.import_log", 1),
    ):
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list, not user input
        assert n is not None and n[0] == expected, table


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


def test_account_metadata_refuses_one_account_named_by_both_referents(
    db: Database, tmp_path: Path
) -> None:
    """Two metadata sets for one account have no winner worth picking.

    ``_resolve_metadata_keys`` collapses a ref onto the source key it names, so
    sending both spells one account twice with two different field sets. Picking
    either applies fields the caller did not ask for and never reports which
    half was dropped — the metadata twin of the binding conflict already pinned
    by ``test_a_ref_and_a_raw_key_disagreeing_on_one_account_is_refused``.

    Both referents are read off the raised gate rather than written as literals:
    the refusal only means anything if the two spellings really do reach one
    account, and hard-coding either would assert against a fixture detail
    instead of against the resolver.
    """
    _seed_existing_account(db, account_id="acct_chk01", display_name="Checking")
    _seed_existing_account(db, account_id="acct_sav01", display_name="Savings")
    svc = ImportService(db)
    csv_path = _two_account_csv(tmp_path)
    with pytest.raises(ImportConfirmationRequiredError) as gate:
        svc.import_file(csv_path, refresh=False, confirm=True)
    first = gate.value.outcome.account_proposals[0]
    ref, key = str(first["proposal_ref"]), str(first["source_account_key"])

    with pytest.raises(ValueError, match="names the same account twice"):
        svc.import_file(
            csv_path,
            refresh=False,
            confirm=True,
            account_bindings={"@0": "new", "@1": "new"},
            account_metadata={
                ref: {"display_name": "By ref"},
                key: {"display_name": "By source key"},
            },
        )


def test_the_answering_helper_refuses_to_answer_a_real_identity_question(
    db: Database,
) -> None:
    """The helper's one safety property, asserted directly for the first time.

    ``import_answering_gate`` binds "new" for tests whose account is incidental,
    and most of the suite reaches the gate through it. Its whole licence to do
    that is this branch: a proposal carrying merge candidates is a real identity
    question, so the helper re-raises instead of answering. If that condition
    ever flipped, dozens of unrelated tests would quietly start auto-answering
    the matcher — green, and proving nothing — which is precisely the failure
    `.claude/rules/testing.md` ("No Shortcuts") exists to prevent.

    The twin is what makes this fixture isolate the candidates branch rather
    than the reason check above it: the gate raised here *is* an
    ``account_confirmation``, asserted below, so only the candidates arm can be
    what re-raised.
    """
    _seed_twin(db, _OFX_TWIN)
    svc = ImportService(db)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        import_answering_gate(svc, _MINIMAL_OFX, refresh=False, confirm=True)

    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    assert outcome.account_proposals[0]["candidates"], outcome.account_proposals
    # Re-raised unchanged, so the caller sees the question the resolver asked.
    linked = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert linked is not None and linked[0] == 0


def test_the_contradiction_refusal_names_the_parameter_the_caller_used(
    db: Database, tmp_path: Path
) -> None:
    """A refusal that names the wrong parameter sends the caller looking for it.

    The same ``explicit_account_id`` arrives from two places — ``account_bindings``
    and the direct ``account_id`` — and the refusal named the first one either
    way. An agent debugging the message would go hunting for an
    ``account_bindings`` argument it never sent, on a path where the fix is to
    change ``account_id``.
    """
    _seed_existing_account(db, account_id="acct_other01", display_name="Other")
    _seed_existing_account(db, account_id="acct_pinned01", display_name="Pinned")
    svc = ImportService(db)

    # Two statements of the same account derive the same native key
    # (slugify("Shared Label")), and the first one accepts it onto
    # acct_other01. That is what makes the second, pinned elsewhere,
    # contradictory — a real collision between two files, not an artifact of
    # the pin having once overwritten the source key with the canonical id.
    january = tmp_path / "january.csv"
    january.write_text("Date,Description,Amount\n2026-01-15,Coffee,-12.50\n")
    svc.import_file(
        january,
        refresh=False,
        confirm=True,
        account_name="Shared Label",
        account_bindings={"@0": "acct_other01"},
    )

    february = tmp_path / "february.csv"
    february.write_text("Date,Description,Amount\n2026-02-16,Groceries,-20.00\n")
    with pytest.raises(ValueError, match="account_id pins") as exc:
        svc.import_file(
            february,
            refresh=False,
            confirm=True,
            account_name="Shared Label",
            account_id="acct_pinned01",
        )

    # The refusal still says what to do; only the misattribution is gone.
    assert "account_bindings" not in str(exc.value), str(exc.value)
    assert "acct_other01" in str(exc.value)


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


def _ofx_with_acctids(tmp_path: Path, *acctids: str) -> Path:
    """An OFX file whose <ACCTID>s are exactly ``acctids``.

    OFX hands <ACCTID> through verbatim as ``source_account_key``, and it is
    untrusted file content — an issuer can emit any string, including one
    shaped like the positional vocabulary. Only this channel can produce that
    collision: tabular slugifies its account column and PDF derives issuer+last4.
    """
    source = _MULTI_ACCOUNT_OFX if len(acctids) > 1 else _MINIMAL_OFX
    text = source.read_text()
    for placeholder, acctid in zip(
        ("CHECKING1", "SAVINGS1") if len(acctids) > 1 else ("1111",),
        acctids,
        strict=True,
    ):
        text = text.replace(
            f"<ACCTID>{placeholder}</ACCTID>", f"<ACCTID>{acctid}</ACCTID>"
        )
    path = tmp_path / f"acctids_{'_'.join(acctids)}.ofx".replace("@", "at")
    path.write_text(text)
    return path


def test_the_unknown_key_refusal_never_names_the_institutions_account_number(
    db: Database, tmp_path: Path
) -> None:
    """The "which keys are valid?" half of the refusal must offer refs, not keys.

    The obvious way to make this error more helpful is to list what the file
    *does* contain — and on OFX that list is the institution's own <ACCTID>s.
    The message would then leak an account number to whoever mistyped a binding,
    including the model provider on the MCP path, and it would read as a
    usability improvement in review. Pin both directions so it cannot.
    """
    create_core_tables(db)
    acctid = "000123456789"
    path = _ofx_with_acctids(tmp_path, acctid)

    with pytest.raises(ValueError) as excinfo:
        ImportService(db).import_file(
            path,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"typo-key": "new"},
        )

    message = str(excinfo.value)
    assert acctid not in message, message
    # The refusal still has to be answerable: the ref is what the caller can
    # retype, and it is the referent every masking surface leaves readable.
    assert "@0" in message, message


def test_a_source_key_spelled_like_another_accounts_ref_is_refused(
    db: Database, tmp_path: Path
) -> None:
    """`@1` naming account 0 and account 1 at once has no safe reading.

    Source-key-wins is the right precedence — a key of "@0" must not bind both
    its own account and proposal zero — but applied *silently* it delivered the
    answer to the wrong account: "@1" bound account 0 by its raw key, while
    account 1 saw no binding and stayed gated. The caller could not recover
    either, because a masking surface hands ``source_account_key`` back as
    ****1234, so the only referent it can reproduce for account 1 is the ref
    that just missed. Refuse the ambiguity rather than pick a reading nobody
    can see.
    """
    svc = ImportService(db)
    with pytest.raises(ValueError, match="@1"):
        svc.import_file(
            _ofx_with_acctids(tmp_path, "@1", "2222"),
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"@1": "new"},
        )
    # Refused before begin_import, like every other binding rejection.
    for table in ("raw.ofx_transactions", "app.account_links", "raw.import_log"):
        n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list, not user input
        assert n is not None and n[0] == 0, table


def test_a_source_key_that_matches_its_own_ref_still_binds(
    db: Database, tmp_path: Path
) -> None:
    """A lone account keyed "@0" is not ambiguous — both readings are itself.

    The refusal above has to turn on the two readings *disagreeing*, not on the
    spelling. Refusing every collision would strand this file instead, with no
    referent left that names its one account.
    """
    svc = ImportService(db)
    svc.import_file(
        _ofx_with_acctids(tmp_path, "@0"),
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"@0": "new"},
    )
    linked = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = '@0' AND status = 'accepted'"
    ).fetchone()
    assert linked is not None and linked[0]


def test_a_binding_to_an_account_minted_before_any_refresh_is_accepted(
    db: Database, tmp_path: Path
) -> None:
    """An account minted moments ago is bindable before core is rebuilt.

    ``core.dim_accounts`` is SQLMesh-materialized, so an account this import
    just minted is absent from it until a refresh runs — and every import here
    defaults to ``refresh=False``. Validating a binding target against that
    table alone would therefore reject the most natural next step in the
    workflow the gate itself creates: answer "new" for one file, then bind its
    sibling to the id that answer produced. ``app.account_links`` carries the
    id from the instant it is minted, which is why the check reads both — and
    why this test asserts the mint is *absent* from dim_accounts first, so it
    can only pass through the links half.
    """
    _seed_existing_account(db, account_id="wf_existing01", display_name="WF Checking")
    svc = ImportService(db)
    svc.import_file(
        _STANDARD_CSV,
        account_name="WF Checking",
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"wf-checking": "new"},
    )
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = 'wf-checking' AND status = 'accepted'"
    ).fetchone()
    assert row is not None
    minted = str(row[0])
    unmaterialized = db.execute(
        "SELECT COUNT(*) FROM core.dim_accounts WHERE account_id = ?", [minted]
    ).fetchone()
    assert unmaterialized is not None and unmaterialized[0] == 0

    svc.import_file(
        _ofx_with_acctids(tmp_path, "9999"),
        refresh=False,
        confirm=True,
        actor_kind="human",
        account_bindings={"9999": minted},
    )
    adopted = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = '9999' AND status = 'accepted'"
    ).fetchone()
    assert adopted is not None and adopted[0] == minted


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
        account_metadata={"wf-checking": {"last_four": "1212"}},
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

    Both ids are seeded so this can only be the conflict refusal: an unknown
    pin is refused too, by a different guard whose message also says "pinned".
    """
    _seed_existing_account(db, account_id="acct_pinned01", display_name="Pinned")
    _seed_existing_account(db, account_id="acct_other02", display_name="Other")
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
    The id is seeded because a pin naming no account is refused on its own.
    """
    _seed_existing_account(db, account_id="acct_pinned01", display_name="Pinned")
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


def test_a_source_key_shaped_like_another_accounts_ref_is_refused() -> None:
    """A file whose own key reads as "@0" must not quietly answer proposal zero.

    ``source_account_key`` is untrusted file content on the OFX channel — it is
    the ``<ACCTID>`` verbatim — so a key of "@0" collides with the positional
    vocabulary. Letting it bind BOTH accounts would be the silent merge the gate
    exists to prevent. Letting the source key win *silently* is the mirror of
    that mistake: the answer lands on the account the caller was not looking at,
    while the one they meant stays gated behind a ref that no longer reaches it.
    Neither reading is safe, so the ambiguity is refused and named.
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

    with pytest.raises(ValueError, match="ambiguous"):
        _resolve_binding_targets([_src("1111"), _src("@0")], {"@0": "acct_target01"})

    # Same index is not a collision: both readings name the one account, so
    # there is nothing to disambiguate and the file stays bindable.
    assert _resolve_binding_targets([_src("@0")], {"@0": "acct_target01"}) == [
        "acct_target01"
    ]


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


def test_the_ambiguous_ref_refusal_says_where_to_read_the_masked_key() -> None:
    """The ambiguity refusal must not send the caller to a value we mask.

    ``source_account_key`` is untrusted file content on OFX, so a key spelled
    like a positional ref ("@1") can name one account while being another's
    ref. Refusing is right — either reading answers an account the caller was
    not looking at. But the escape it offers is the *other* account's source
    key, and every surface that shows this gate masks that key, so a caller
    reading the confirmation cannot comply. The file is the only place the raw
    key is still legible, and the message has to say so.

    Guards against the wording drifting back to "exactly as the confirmation
    reported it" — true of the unknown-key refusal six lines below, false here.
    """
    # Private by design: this is the pure binding resolver, and reaching it
    # through import_file would need a two-account fixture whose first ACCTID is
    # literally "@1" to exercise one error string.
    from moneybin.services.import_service import (  # noqa: PLC0415
        _resolve_binding_targets,  # pyright: ignore[reportPrivateUsage]
    )

    sources = [
        SourceAccount(
            source_type="ofx",
            source_origin="bank",
            source_account_key="@1",
            account_name="Checking",
        ),
        SourceAccount(
            source_type="ofx",
            source_origin="bank",
            source_account_key="9876543210",
            account_name="Savings",
        ),
    ]

    with pytest.raises(ValueError) as exc:
        _resolve_binding_targets(sources, {"@0": "new", "@1": "new"})

    message = str(exc.value)
    assert "read it from the file itself" in message, message
    # Still never names the sibling's real key — that is the masked value.
    assert "9876543210" not in message, message
