"""Overlap follows the latest received transaction revision before transforms."""

import re
from datetime import date
from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.investments.source_overlap import (
    INVESTMENT_SOURCE_TYPES,
    SourceEvidence,
    evidence_phrase,
    history_phrase,
    investment_source_choice_counts,
    investment_source_evidence,
    investment_source_overlap,
    stale_source_choice_accounts,
    trade_count_phrase,
)
from tests.moneybin.db_helpers import CORE_FCT_INVESTMENT_TRANSACTIONS_DDL


def _manual_row(db: Database, *, account_id: str, txn_id: str, trade_date: str) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, type, trade_date,
            created_by
        ) VALUES (?, 'manual_import', ?, 'buy', ?::DATE, 'cli')
        """,
        [txn_id, account_id, trade_date],
    )


def _plaid_trade(
    db: Database,
    *,
    account_id: str,
    txn_id: str,
    trade_date: str,
    origin: str = "item",
    version: str = "v1",
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, transaction_date,
            amount, observation_version, source_origin
        ) VALUES (?, ?, ?::DATE, 100, ?, ?)
        """,
        [txn_id, account_id, trade_date, version, origin],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transaction_receipts (
            investment_transaction_id, source_origin, source_file,
            observation_version, ingestion_sequence, extracted_at
        ) VALUES (?, ?, 'sync_1', ?, 1, '2026-01-02')
        """,
        [txn_id, origin, version],
    )


def _link(db: Database, *, canonical: str, native: str) -> None:
    db.execute(
        """
        INSERT INTO app.account_links (
            link_id, account_id, ref_kind, ref_value, source_type,
            source_origin, status, decided_by, decided_at
        ) VALUES (?, ?, 'source_native', ?, 'plaid', 'item', 'accepted',
                  'user', CURRENT_TIMESTAMP)
        """,
        [f"link_{native}", canonical, native],
    )


def _overlapping_account(db: Database, account_id: str = "canonical_x") -> None:
    """Two manual trades and three Plaid trades on one resolved account."""
    _link(db, canonical=account_id, native="native_x")
    _manual_row(db, account_id=account_id, txn_id="m1", trade_date="2019-03-04")
    _manual_row(db, account_id=account_id, txn_id="m2", trade_date="2025-11-21")
    for n, day in enumerate(("2024-09-03", "2025-01-15", "2026-09-30")):
        _plaid_trade(db, account_id="native_x", txn_id=f"p{n}", trade_date=day)


@pytest.mark.parametrize("returned_to_original", [False, True])
def test_overlap_uses_current_receipt_after_account_correction(
    db: Database, returned_to_original: bool
) -> None:
    for account in ("old", "corrected"):
        db.execute(
            """
            INSERT INTO raw.manual_investment_transactions (
                source_transaction_id, import_id, account_id, type, trade_date,
                created_by
            ) VALUES (?, 'manual_import', ?, 'buy', '2026-01-01', 'cli')
            """,
            [f"manual_{account}", f"canonical_{account}"],
        )
        db.execute(
            """
            INSERT INTO app.account_links (
                link_id, account_id, ref_kind, ref_value, source_type,
                source_origin, status, decided_by, decided_at
            ) VALUES (?, ?, 'source_native', ?, 'plaid', 'item', 'accepted',
                      'user', CURRENT_TIMESTAMP)
            """,
            [f"link_{account}", f"canonical_{account}", account],
        )
        db.execute(
            """
            INSERT INTO raw.plaid_investment_transactions (
                investment_transaction_id, account_id, transaction_date,
                amount, observation_version, source_origin
            ) VALUES ('same_transaction', ?, '2026-01-01', 100, ?, 'item')
            """,
            [account, f"version_{account}"],
        )
    versions = ["old", "corrected"]
    if returned_to_original:
        versions.append("old")
    for sequence, version in enumerate(versions, start=1):
        db.execute(
            """
            INSERT INTO raw.plaid_investment_transaction_receipts (
                investment_transaction_id, source_origin, source_file,
                observation_version, ingestion_sequence, extracted_at
            ) VALUES ('same_transaction', 'item', ?, ?, ?, '2026-01-02')
            """,
            [f"sync_{sequence}", f"version_{version}", sequence],
        )

    assert investment_source_overlap(db) == [f"canonical_{versions[-1]}"]
    assert db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone() == (2,)


def test_investment_source_types_match_the_ledger_branches() -> None:
    """Every ledger union branch is a source type; a new one must be choosable."""
    model = (
        Path(__file__).parents[3]
        / "src/moneybin/sqlmesh/models/core/fct_investment_transactions.sql"
    ).read_text()
    branches = set(re.findall(r"FROM\s+(prep\.\w+)", model))
    assert branches == {
        "prep.stg_manual__investment_transactions",
        "prep.stg_plaid__investment_transactions",
        "prep.stg_plaid__opening_lots",
    }, "a new ledger branch means a new source type; update INVESTMENT_SOURCE_TYPES"
    assert frozenset({"manual", "plaid"}) == INVESTMENT_SOURCE_TYPES


def test_chosen_account_is_not_reported(db: Database) -> None:
    _overlapping_account(db)
    assert investment_source_overlap(db) == ["canonical_x"]

    db.execute(
        "INSERT INTO app.account_settings (account_id, investment_source_type) "
        "VALUES ('canonical_x', 'manual')"
    )

    assert investment_source_overlap(db) == []


def test_unchosen_settings_row_does_not_hide_the_overlap(db: Database) -> None:
    _overlapping_account(db)
    db.execute(
        "INSERT INTO app.account_settings (account_id, display_name) "
        "VALUES ('canonical_x', 'Choice Brokerage')"
    )

    assert investment_source_overlap(db) == ["canonical_x"]


def _ledger_row(db: Database, *, account_id: str, source_type: str, n: int) -> None:
    db.execute(
        "INSERT INTO core.fct_investment_transactions "
        "(investment_transaction_id, account_id, source_type) VALUES (?, ?, ?)",
        [f"itx_{source_type}_{n}", account_id, source_type],
    )


def _choose(db: Database, account_id: str, choice: str | None) -> None:
    db.execute(
        "INSERT INTO app.account_settings (account_id, investment_source_type) "
        "VALUES (?, ?)",
        [account_id, choice],
    )


def test_stale_choice_reported_while_ledger_holds_the_other_source(
    db: Database,
) -> None:
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _choose(db, "canonical_x", "manual")
    _ledger_row(db, account_id="canonical_x", source_type="manual", n=1)
    _ledger_row(db, account_id="canonical_x", source_type="plaid", n=1)

    assert stale_source_choice_accounts(db) == ["canonical_x"]


def test_choice_is_not_stale_once_ledger_holds_only_the_chosen_source(
    db: Database,
) -> None:
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _choose(db, "canonical_x", "manual")
    _ledger_row(db, account_id="canonical_x", source_type="manual", n=1)
    # An unchosen account may legitimately hold both sources.
    _ledger_row(db, account_id="canonical_y", source_type="manual", n=1)
    _ledger_row(db, account_id="canonical_y", source_type="plaid", n=1)

    assert stale_source_choice_accounts(db) == []


def test_cleared_choice_is_not_stale(db: Database) -> None:
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _choose(db, "canonical_x", None)
    _ledger_row(db, account_id="canonical_x", source_type="plaid", n=1)

    assert stale_source_choice_accounts(db) == []


def test_stale_detector_is_empty_before_the_ledger_is_built(db: Database) -> None:
    _choose(db, "canonical_x", "manual")

    assert stale_source_choice_accounts(db) == []


def test_stale_detector_survives_an_unmigrated_catalog(db: Database) -> None:
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    _ledger_row(db, account_id="canonical_x", source_type="plaid", n=1)
    db.execute(
        "ALTER TABLE app.account_settings DROP COLUMN investment_source_type_changed_at"
    )
    db.execute("ALTER TABLE app.account_settings DROP COLUMN investment_source_type")

    assert stale_source_choice_accounts(db) == []


def test_detector_survives_an_unmigrated_catalog(db: Database) -> None:
    _overlapping_account(db)
    db.execute(
        "ALTER TABLE app.account_settings DROP COLUMN investment_source_type_changed_at"
    )
    db.execute("ALTER TABLE app.account_settings DROP COLUMN investment_source_type")

    assert investment_source_overlap(db) == ["canonical_x"]
    assert investment_source_choice_counts(db) == {"manual": 0, "plaid": 0}


def test_evidence_counts_and_ranges(db: Database) -> None:
    _overlapping_account(db)

    assert investment_source_evidence(db, ["canonical_x"]) == {
        "canonical_x": [
            SourceEvidence("manual", 2, date(2019, 3, 4), date(2025, 11, 21), False),
            SourceEvidence("plaid", 3, date(2024, 9, 3), date(2026, 9, 30), False),
        ]
    }


def test_evidence_for_no_accounts_is_empty(db: Database) -> None:
    assert investment_source_evidence(db, []) == {}


def test_evidence_covers_only_the_requested_accounts(db: Database) -> None:
    _overlapping_account(db)
    _manual_row(db, account_id="canonical_other", txn_id="m9", trade_date="2020-01-01")

    assert set(investment_source_evidence(db, ["canonical_x"])) == {"canonical_x"}


def test_holdings_only_plaid_evidence(db: Database) -> None:
    _link(db, canonical="canonical_x", native="native_x")
    _manual_row(db, account_id="canonical_x", txn_id="m1", trade_date="2019-03-04")
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings (
            account_id, security_id, holdings_date, quantity,
            transactions_window_start, source_file, source_origin
        ) VALUES ('native_x', 'sec_1', '2026-01-01', 5, '2026-01-01', 'sync_1',
                  'item')
        """
    )

    plaid = investment_source_evidence(db, ["canonical_x"])["canonical_x"][1]

    assert plaid == SourceEvidence("plaid", 0, None, None, True)
    assert evidence_phrase(plaid) == "a holdings snapshot, no trades"


def test_evidence_follows_the_current_receipt(db: Database) -> None:
    """A superseded observation of a trade is not counted."""
    _link(db, canonical="canonical_x", native="native_x")
    _manual_row(db, account_id="canonical_x", txn_id="m1", trade_date="2019-03-04")
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, transaction_date,
            amount, observation_version, source_origin
        ) VALUES ('same_txn', 'native_x', '2024-01-01', 100, 'v1', 'item'),
                 ('same_txn', 'native_x', '2024-02-02', 100, 'v2', 'item')
        """
    )
    for sequence, version in ((1, "v1"), (2, "v2")):
        db.execute(
            """
            INSERT INTO raw.plaid_investment_transaction_receipts (
                investment_transaction_id, source_origin, source_file,
                observation_version, ingestion_sequence, extracted_at
            ) VALUES ('same_txn', 'item', ?, ?, ?, '2026-01-02')
            """,
            [f"sync_{sequence}", version, sequence],
        )

    plaid = investment_source_evidence(db, ["canonical_x"])["canonical_x"][1]

    assert plaid == SourceEvidence(
        "plaid", 1, date(2024, 2, 2), date(2024, 2, 2), False
    )


def test_choice_counts(db: Database) -> None:
    for account_id, choice in (
        ("acc_a", "manual"),
        ("acc_b", "manual"),
        ("acc_c", "plaid"),
        ("acc_d", None),
    ):
        db.execute(
            "INSERT INTO app.account_settings (account_id, investment_source_type) "
            "VALUES (?, ?)",
            [account_id, choice],
        )

    assert investment_source_choice_counts(db) == {"manual": 2, "plaid": 1}


def test_choice_counts_include_zeros(db: Database) -> None:
    assert investment_source_choice_counts(db) == {"manual": 0, "plaid": 0}


def test_history_phrases() -> None:
    assert history_phrase("manual") == "the recorded history"
    assert history_phrase("plaid") == "the connection's history"
    assert history_phrase("ofx") == "the ofx history"


@pytest.mark.parametrize(
    ("evidence", "phrase"),
    [
        (
            SourceEvidence("manual", 412, date(2019, 3, 4), date(2025, 11, 21), False),
            "412 trades, 2019-03-04 → 2025-11-21",
        ),
        (
            SourceEvidence("plaid", 1, date(2024, 1, 2), date(2024, 1, 2), False),
            "1 trade, 2024-01-02 → 2024-01-02",
        ),
        (
            SourceEvidence("plaid", 0, None, None, True),
            "a holdings snapshot, no trades",
        ),
    ],
)
def test_evidence_phrase(evidence: SourceEvidence, phrase: str) -> None:
    assert evidence_phrase(evidence) == phrase


@pytest.mark.parametrize(
    ("evidence", "phrase"),
    [
        (SourceEvidence("manual", 412, None, None, False), "412 recorded trades"),
        (SourceEvidence("manual", 1, None, None, False), "1 recorded trade"),
        (SourceEvidence("plaid", 1, None, None, False), "1 synced trade"),
        (SourceEvidence("plaid", 3, None, None, False), "3 synced trades"),
        (SourceEvidence("plaid", 0, None, None, True), "a synced holdings snapshot"),
    ],
)
def test_trade_count_phrase(evidence: SourceEvidence, phrase: str) -> None:
    assert trade_count_phrase(evidence) == phrase
