"""SecurityResolver ladder: every rung, exchange normalization, refresh.

The load-bearing tests here are the refuse-to-merge ones: an identifier that
matches more than one catalog entry must NEVER auto-pick a winner, and a
contradicting strong identifier must never be overridden by a weaker signal.
A wrong silent merge fuses two securities' tax lots — irreversibly.
"""

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.security_resolver import SecurityResolver


def _raw_security(db: Database, security_id: str, **overrides: object) -> None:
    row: dict[str, object] = {
        "security_id": security_id,
        "institution_security_id": None,
        "institution_id": None,
        "ticker_symbol": None,
        "market_identifier_code": None,
        "security_name": None,
        "security_type": "equity",
        "cusip": None,
        "isin": None,
        "is_cash_equivalent": False,
        "iso_currency_code": "USD",
        "source_file": "sync_j1",
        "source_origin": "item_1",
    }
    row.update(overrides)
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    db.execute(
        f"INSERT OR REPLACE INTO raw.plaid_securities ({cols}) VALUES ({marks})",  # noqa: S608  # fixed column set, test input
        list(row.values()),
    )


def _catalog(db: Database, name: str, **kw: Any) -> str:
    event = SecuritiesRepo(db).upsert(
        security_id=None,
        name=name,
        security_type=str(kw.pop("security_type", "equity")),
        actor="user",
        **kw,
    )
    assert event.target_id is not None
    return event.target_id


def _seed_mic_registry(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
    db.execute(
        "CREATE TABLE IF NOT EXISTS seeds.exchange_mic_map (alias VARCHAR, mic VARCHAR)"
    )
    db.execute(
        "INSERT INTO seeds.exchange_mic_map VALUES "
        "('XNAS','XNAS'), ('NASDAQ','XNAS'), ('XLON','XLON'), ('LSE','XLON')"
    )


def _bindings(db: Database) -> list[tuple[str, str, str]]:
    return db.execute(
        "SELECT ref_kind, ref_value, security_id FROM app.security_links "
        "WHERE status = 'accepted' ORDER BY ref_value"
    ).fetchall()


def test_rung1_adopts_existing_binding(db: Database) -> None:
    sid = _catalog(db, "Apple Inc.", ticker="AAPL")
    SecurityLinksRepo(db).insert(
        security_id=sid,
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    _raw_security(db, "sec_1", security_name="Apple Inc.")
    counts = SecurityResolver(db).resolve_all()
    assert counts == {"adopted": 1}


def test_rung2_cusip_binds_despite_exchange_mismatch(db: Database) -> None:
    _seed_mic_registry(db)
    sid = _catalog(db, "Apple Inc.", ticker="AAPL", cusip="037833100", exchange="LSE")
    _raw_security(
        db,
        "sec_1",
        security_name="Apple",
        ticker_symbol="AAPL",
        cusip="037833100",
        market_identifier_code="XNAS",
    )
    counts = SecurityResolver(db).resolve_all()
    assert counts == {"auto_bound": 1}
    assert _bindings(db)[0][2] == sid


def test_rung2_ticker_binds_on_normalized_mic_agreement(db: Database) -> None:
    _seed_mic_registry(db)
    sid = _catalog(db, "Apple Inc.", ticker="AAPL", exchange="NASDAQ")
    _raw_security(
        db,
        "sec_1",
        security_name="Apple Inc",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
    )
    assert SecurityResolver(db).resolve_all() == {"auto_bound": 1}
    assert _bindings(db)[0][2] == sid


def test_rung2_unnormalizable_exchange_treated_absent(db: Database) -> None:
    _seed_mic_registry(db)
    _catalog(db, "Apple Inc.", ticker="AAPL", exchange="MAIN STREET EXCHANGE")
    _raw_security(
        db,
        "sec_1",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
        security_name="Apple Inc",
    )
    assert SecurityResolver(db).resolve_all() == {"auto_bound": 1}


def test_mic_contradiction_falls_to_merge_proposal(db: Database) -> None:
    _seed_mic_registry(db)
    sid = _catalog(db, "Apple Inc.", ticker="AAPL", exchange="LSE")
    _raw_security(
        db,
        "sec_1",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
        security_name="Apple Inc.",
    )
    counts = SecurityResolver(db).resolve_all()
    assert counts == {"proposed": 1}
    pending = SecurityLinkDecisionsRepo(db).list_pending()
    assert len(pending) == 1
    assert pending[0]["candidate_security_id"] == sid
    assert pending[0]["match_reason"] == "exchange_contradiction"
    # provisional mint is bound NOW (rows reach the ledger), merge reviewed later
    assert _bindings(db)[0][2] != sid


@pytest.mark.parametrize("n_tied", [2, 3])  # spec: exercised at two AND at three
def test_identifier_tie_surfaces_every_candidate_never_auto_picks(
    db: Database, n_tied: int
) -> None:
    """Refuse-to-merge on ambiguity (R12 / PP DuplicateSecurityException)."""
    sids = {
        _catalog(db, f"Apple Inc. ({i})", ticker=f"AAPL{i}", cusip="037833100")
        for i in range(n_tied)
    }
    _raw_security(db, "sec_1", security_name="Apple", cusip="037833100")
    counts = SecurityResolver(db).resolve_all()
    assert counts == {"proposed": 1}
    pending = SecurityLinkDecisionsRepo(db).list_pending()
    assert {p["candidate_security_id"] for p in pending} == sids
    assert {p["match_reason"] for p in pending} == {"identifier_tie"}
    # bound to the provisional mint — no tied candidate was auto-picked
    assert _bindings(db)[0][2] not in sids


@pytest.mark.parametrize("n_tied", [2, 3])
def test_isin_tie_surfaces_every_candidate(db: Database, n_tied: int) -> None:
    sids = {
        _catalog(db, f"Apple Inc. ({i})", ticker=f"AAPL{i}", isin="US0378331005")
        for i in range(n_tied)
    }
    _raw_security(db, "sec_1", security_name="Apple", isin="US0378331005")
    assert SecurityResolver(db).resolve_all() == {"proposed": 1}
    pending = SecurityLinkDecisionsRepo(db).list_pending()
    assert {p["candidate_security_id"] for p in pending} == sids
    assert {p["match_reason"] for p in pending} == {"identifier_tie"}
    assert _bindings(db)[0][2] not in sids


@pytest.mark.parametrize("n_tied", [2, 3])
def test_ticker_tie_surfaces_every_candidate(db: Database, n_tied: int) -> None:
    """A duplicated ticker in the catalog never auto-picks, even with MIC agreement."""
    _seed_mic_registry(db)
    sids = {
        _catalog(db, f"Apple Inc. ({i})", ticker="AAPL", exchange="NASDAQ")
        for i in range(n_tied)
    }
    _raw_security(
        db,
        "sec_1",
        security_name="Apple",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
    )
    assert SecurityResolver(db).resolve_all() == {"proposed": 1}
    pending = SecurityLinkDecisionsRepo(db).list_pending()
    assert {p["candidate_security_id"] for p in pending} == sids
    assert {p["match_reason"] for p in pending} == {"identifier_tie"}
    assert _bindings(db)[0][2] not in sids


def test_contradicting_cusip_disqualifies_ticker_automatch(db: Database) -> None:
    """A ticker+MIC agreement never overrides a CUSIP that says 'different instrument'."""
    _seed_mic_registry(db)
    sid = _catalog(
        db, "Apple Inc.", ticker="AAPL", exchange="NASDAQ", cusip="037833100"
    )
    _raw_security(
        db,
        "sec_1",
        security_name="Apple Inc.",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
        cusip="999999999",  # a DIFFERENT instrument, whatever the ticker says
    )
    assert SecurityResolver(db).resolve_all() == {"minted": 1}
    assert _bindings(db)[0][2] != sid
    assert SecurityLinkDecisionsRepo(db).count_pending() == 0


def test_contradicting_isin_disqualifies_ticker_automatch(db: Database) -> None:
    _seed_mic_registry(db)
    sid = _catalog(
        db, "Apple Inc.", ticker="AAPL", exchange="NASDAQ", isin="US0378331005"
    )
    _raw_security(
        db,
        "sec_1",
        security_name="Apple Inc.",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
        isin="US9999999999",
    )
    assert SecurityResolver(db).resolve_all() == {"minted": 1}
    assert _bindings(db)[0][2] != sid


def test_contradicting_cusip_disqualifies_fuzzy_name_proposal(db: Database) -> None:
    _catalog(
        db,
        "Vanguard Total Stock Market ETF",
        security_type="etf",
        cusip="922908769",
    )
    _raw_security(
        db,
        "sec_1",
        security_name="Vanguard Total Stock Mkt ETF",
        security_type="etf",
        cusip="111111111",
    )
    assert SecurityResolver(db).resolve_all() == {"minted": 1}
    assert SecurityLinkDecisionsRepo(db).count_pending() == 0


def test_fuzzy_name_mints_provisionally_and_proposes(db: Database) -> None:
    sid = _catalog(db, "Vanguard Total Stock Market ETF", security_type="etf")
    _raw_security(
        db,
        "sec_1",
        security_name="Vanguard Total Stock Mkt ETF",
        security_type="etf",
    )
    counts = SecurityResolver(db).resolve_all()
    assert counts == {"proposed": 1}
    row = db.execute(
        "SELECT COUNT(*) FROM app.securities WHERE created_by = 'plaid'"
    ).fetchone()
    assert row is not None and row[0] == 1
    pending = SecurityLinkDecisionsRepo(db).list_pending()
    assert pending[0]["candidate_security_id"] == sid
    assert pending[0]["match_reason"] == "fuzzy_name"


def test_no_candidate_mints_and_binds(db: Database) -> None:
    _raw_security(
        db, "sec_1", security_name="Obscure Widget Corp", security_type="fixed income"
    )
    assert SecurityResolver(db).resolve_all() == {"minted": 1}
    row = db.execute("SELECT security_type, created_by FROM app.securities").fetchone()
    assert row == ("bond", "plaid")  # defensive type mapping
    assert len(_bindings(db)) == 1


def test_rejected_pairing_never_reproposed(db: Database) -> None:
    sid = _catalog(db, "Vanguard Total Stock Market ETF", security_type="etf")
    SecurityLinkDecisionsRepo(db).insert(
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        candidate_security_id=sid,
        status="pending",
        actor="system",
    )
    decision = SecurityLinkDecisionsRepo(db).list_pending()[0]
    SecurityLinkDecisionsRepo(db).update_status(
        decision["decision_id"], status="rejected", decided_by="user", actor="user"
    )
    _raw_security(
        db,
        "sec_1",
        security_name="Vanguard Total Stock Mkt ETF",
        security_type="etf",
    )
    assert SecurityResolver(db).resolve_all() == {"minted": 1}
    assert SecurityLinkDecisionsRepo(db).count_pending() == 0


def test_rejected_tie_candidate_is_not_reproposed(db: Database) -> None:
    """A user-rejected tie member drops out; the surviving tie member re-proposes."""
    keep = _catalog(db, "Apple Inc. (A)", ticker="AAPLA", cusip="037833100")
    drop = _catalog(db, "Apple Inc. (B)", ticker="AAPLB", cusip="037833100")
    SecurityLinkDecisionsRepo(db).insert(
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        candidate_security_id=drop,
        status="pending",
        actor="system",
    )
    decision = SecurityLinkDecisionsRepo(db).list_pending()[0]
    SecurityLinkDecisionsRepo(db).update_status(
        decision["decision_id"], status="rejected", decided_by="user", actor="user"
    )
    _raw_security(db, "sec_1", security_name="Apple", cusip="037833100")
    assert SecurityResolver(db).resolve_all() == {"proposed": 1}
    pending = SecurityLinkDecisionsRepo(db).list_pending()
    assert [p["candidate_security_id"] for p in pending] == [keep]


def test_under_review_ref_is_left_alone(db: Database) -> None:
    sid = _catalog(db, "Apple Inc.", ticker="AAPL")
    SecurityLinkDecisionsRepo(db).insert(
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        candidate_security_id=sid,
        status="pending",
        actor="system",
    )
    _raw_security(db, "sec_1", security_name="Apple Inc.")
    assert SecurityResolver(db).resolve_all() == {"pending": 1}
    assert _bindings(db) == []


def test_adopt_refreshes_plaid_minted_attributes(db: Database) -> None:
    _raw_security(db, "sec_1", security_name="Vangard Total")
    SecurityResolver(db).resolve_all()  # mints with the typo'd name
    _raw_security(
        db,
        "sec_1",
        security_name="Vanguard Total Stock Market ETF",
        security_type="etf",
    )
    assert SecurityResolver(db).resolve_all() == {"adopted": 1}
    row = db.execute("SELECT name, security_type FROM app.securities").fetchone()
    assert row == ("Vanguard Total Stock Market ETF", "etf")


def test_adopt_never_refreshes_user_authored_row(db: Database) -> None:
    sid = _catalog(db, "Apple Inc.", ticker="AAPL")
    SecurityLinksRepo(db).insert(
        security_id=sid,
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        decided_by="user",
        actor="user",
    )
    _raw_security(db, "sec_1", security_name="APPLE INC (PLAID)", ticker_symbol="AAPL")
    assert SecurityResolver(db).resolve_all() == {"adopted": 1}
    row = db.execute(
        "SELECT name FROM app.securities WHERE security_id = ?", [sid]
    ).fetchone()
    assert row == ("Apple Inc.",)


def test_institution_composite_ref_bound_alongside(db: Database) -> None:
    _raw_security(
        db,
        "sec_1",
        security_name="Obscure Widget Corp",
        institution_id="ins_9",
        institution_security_id="WID-1",
    )
    SecurityResolver(db).resolve_all()
    refs = [(r[0], r[1]) for r in _bindings(db)]
    assert ("institution_security_id", "ins_9:WID-1") in refs
    assert ("plaid_security_id", "sec_1") in refs


def test_churned_plaid_id_adopts_via_institution_ref(db: Database) -> None:
    """A corporate action churns plaid_security_id; the institution ref carries identity."""
    _raw_security(
        db,
        "sec_1",
        security_name="Obscure Widget Corp",
        institution_id="ins_9",
        institution_security_id="WID-1",
    )
    assert SecurityResolver(db).resolve_all() == {"minted": 1}
    minted = _bindings(db)[0][2]
    _raw_security(
        db,
        "sec_2",  # Plaid re-issued the id
        security_name="Obscure Widget Corp",
        institution_id="ins_9",
        institution_security_id="WID-1",
    )
    # both raw rows resolve now: sec_1 by its own binding, sec_2 via the
    # institution ref — the churned id adopts instead of minting a twin.
    assert SecurityResolver(db).resolve_all() == {"adopted": 2}
    rows = db.execute("SELECT COUNT(*) FROM app.securities").fetchone()
    assert rows is not None and rows[0] == 1  # no duplicate catalog entry
    assert {r[2] for r in _bindings(db)} == {minted}


def test_ticker_exchange_suffix_strips_to_catalog_match(db: Database) -> None:
    """A provider ticker carrying an exchange suffix (VOD.L) matches catalog VOD."""
    _seed_mic_registry(db)
    sid = _catalog(db, "Vodafone Group PLC", ticker="VOD", exchange="LSE")
    _raw_security(
        db,
        "sec_1",
        security_name="Vodafone Group",
        ticker_symbol="VOD.L",
        market_identifier_code="XLON",
    )
    assert SecurityResolver(db).resolve_all() == {"auto_bound": 1}
    assert _bindings(db)[0][2] == sid


def test_exact_ticker_wins_over_suffix_strip(db: Database) -> None:
    """Exact-first: a real dotted ticker binds to its own entry, not the stripped stem."""
    _seed_mic_registry(db)
    _catalog(db, "Berkshire Hathaway Inc. Class A", ticker="BRK", exchange="NASDAQ")
    class_b = _catalog(
        db, "Berkshire Hathaway Inc. Class B", ticker="BRK.B", exchange="NASDAQ"
    )
    _raw_security(
        db,
        "sec_1",
        security_name="Berkshire Hathaway Inc. Class B",
        ticker_symbol="BRK.B",
        market_identifier_code="XNAS",
    )
    assert SecurityResolver(db).resolve_all() == {"auto_bound": 1}
    assert _bindings(db)[0][2] == class_b


def test_nameless_securities_never_propose_a_placeholder_merge(db: Database) -> None:
    """Two nameless securities mint distinctly.

    Their identical placeholder names must never fuzzy-match each other into a
    merge proposal.
    """
    _raw_security(db, "sec_1", security_name=None)
    _raw_security(db, "sec_2", security_name=None)
    assert SecurityResolver(db).resolve_all() == {"minted": 2}
    assert SecurityLinkDecisionsRepo(db).count_pending() == 0
    rows = db.execute(
        "SELECT COUNT(DISTINCT security_id) FROM app.securities"
    ).fetchone()
    assert rows is not None and rows[0] == 2


def test_conflicting_refs_never_rewrite_a_binding(
    db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """Two refs on one row pointing at different securities: adopt one, rewrite neither.

    A repoint is a reviewed merge, never a sync-time side effect.
    """
    first = _catalog(db, "Widget Corp A")
    second = _catalog(db, "Widget Corp B")
    links = SecurityLinksRepo(db)
    links.insert(
        security_id=first,
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    links.insert(
        security_id=second,
        ref_kind="institution_security_id",
        ref_value="ins_9:WID-1",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    _raw_security(
        db,
        "sec_1",
        security_name="Widget Corp",
        institution_id="ins_9",
        institution_security_id="WID-1",
    )
    with caplog.at_level("WARNING"):
        assert SecurityResolver(db).resolve_all() == {"adopted": 1}
    assert "security ref conflict" in caplog.text
    # both bindings survive exactly as they were — neither was silently repointed
    assert _bindings(db) == [
        ("institution_security_id", "ins_9:WID-1", second),
        ("plaid_security_id", "sec_1", first),
    ]


def test_missing_seed_registry_degrades_to_absent(db: Database) -> None:
    # no seeds schema at all — exchange comparison must degrade, not raise
    _catalog(db, "Apple Inc.", ticker="AAPL", exchange="NASDAQ")
    _raw_security(
        db,
        "sec_1",
        ticker_symbol="AAPL",
        market_identifier_code="XNAS",
        security_name="Apple Inc.",
    )
    assert SecurityResolver(db).resolve_all() == {"auto_bound": 1}


def test_no_raw_securities_returns_empty_counts(db: Database) -> None:
    assert SecurityResolver(db).resolve_all() == {}
