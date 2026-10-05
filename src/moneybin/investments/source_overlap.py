"""Live overlap evidence for sync warnings and doctor findings."""

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from moneybin.database import Database, has_column
from moneybin.investments.identity import manual_identity_sql
from moneybin.tables import (
    ACCOUNT_LINKS,
    ACCOUNT_SETTINGS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_TRANSACTION_RECEIPTS,
    PLAID_INVESTMENT_TRANSACTIONS,
)

#: The source types core.fct_investment_transactions unions, and so the only
#: values app.account_settings.investment_source_type accepts. No DDL CHECK:
#: a new investment importer becomes choosable by adding its source type here.
INVESTMENT_SOURCE_TYPES: frozenset[str] = frozenset({"manual", "plaid"})

_ADJECTIVES = {"manual": "recorded", "plaid": "synced"}


@dataclass(frozen=True)
class SourceEvidence:
    """What one source type holds for one account, from current raw rows."""

    source_type: str  # 'manual' | 'plaid'
    trade_count: (
        int  # current raw observations; review routing happens later in staging
    )
    first_trade_date: date | None
    last_trade_date: date | None
    holdings_only: bool  # plaid evidence is a holdings snapshot with no trades


def _scope_ctes() -> str:
    """The raw scope the detector and the evidence share, so they cannot disagree.

    ``plaid_resolved`` is one row per current Plaid trade or holdings row, on
    the canonical account id when an accepted link resolves it.
    """
    return f"""
        manual_identity AS ({manual_identity_sql()}), current_receipts AS (
            SELECT * FROM {PLAID_INVESTMENT_TRANSACTION_RECEIPTS.full_name}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY investment_transaction_id, source_origin
                ORDER BY extracted_at DESC, ingestion_sequence DESC
            ) = 1
        ), plaid_resolved AS (
            SELECT COALESCE(al.account_id, p.account_id) AS account_id,
                   p.is_trade, p.trade_date
            FROM (
                SELECT t.account_id, t.source_origin,
                       t.transaction_date AS trade_date, TRUE AS is_trade
                FROM {PLAID_INVESTMENT_TRANSACTIONS.full_name} AS t
                JOIN current_receipts AS r
                    USING (investment_transaction_id, source_origin, observation_version)
                UNION ALL
                SELECT h.account_id, h.source_origin,
                       NULL::DATE AS trade_date, FALSE AS is_trade
                FROM {PLAID_INVESTMENT_HOLDINGS.full_name} AS h
            ) AS p
            LEFT JOIN {ACCOUNT_LINKS.full_name} AS al
              ON al.status = 'accepted' AND al.ref_kind = 'source_native'
              AND al.source_type = 'plaid' AND al.source_origin = p.source_origin
              AND al.ref_value = p.account_id
        )
    """  # noqa: S608  # TableRef constants


def investment_source_overlap(db: Database) -> list[str]:
    """Accounts with manual history and Plaid evidence, minus chosen accounts.

    An account whose ``investment_source_type`` is set has settled the overlap:
    its ledger carries one source, so neither the sync warning nor the doctor
    reports it. Reads ``app.account_settings`` directly because it runs before
    any transform.
    """
    unchosen = ""
    if has_column(db, ACCOUNT_SETTINGS, "investment_source_type"):
        unchosen = f"""
        AND NOT EXISTS (
            SELECT 1 FROM {ACCOUNT_SETTINGS.full_name} AS s
            WHERE s.account_id = p.account_id
              AND s.investment_source_type IS NOT NULL
        )
        """  # noqa: S608  # TableRef constant
    rows = db.execute(
        f"""
        WITH {_scope_ctes()}
        SELECT DISTINCT p.account_id
        FROM plaid_resolved AS p
        WHERE EXISTS (
            SELECT 1 FROM manual_identity AS m
            WHERE COALESCE(m.account_id, m.frozen_account_id) = p.account_id
        ) {unchosen}
        ORDER BY p.account_id
        """,  # noqa: S608  # TableRef constants
    ).fetchall()
    return [str(row[0]) for row in rows]


def investment_source_evidence(
    db: Database, account_ids: Sequence[str]
) -> dict[str, list[SourceEvidence]]:
    """Per-source trade counts and date ranges, each list sorted by source type.

    Read from the raw scope the detector uses, not the ledger, so it describes
    an account that has not been transformed yet. A Plaid source with holdings
    rows and no trades is reported as ``holdings_only``.
    """
    if not account_ids:
        return {}
    marks = ", ".join("?" for _ in account_ids)
    rows = db.execute(
        f"""
        WITH {_scope_ctes()}, manual_rows AS (
            SELECT COALESCE(i.account_id, i.frozen_account_id) AS account_id,
                   t.trade_date
            FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name} AS t
            JOIN manual_identity AS i USING (source_transaction_id)
        )
        SELECT account_id, 'manual' AS source_type, COUNT(*) AS trade_count,
               MIN(trade_date), MAX(trade_date)
        FROM manual_rows
        WHERE account_id IN ({marks})
        GROUP BY account_id
        UNION ALL
        SELECT account_id, 'plaid', COUNT(*) FILTER (WHERE is_trade),
               MIN(trade_date), MAX(trade_date)
        FROM plaid_resolved
        WHERE account_id IN ({marks})
        GROUP BY account_id
        """,  # noqa: S608  # TableRef constants; ids are ? parameters
        [*account_ids, *account_ids],
    ).fetchall()
    evidence: dict[str, list[SourceEvidence]] = defaultdict(list)
    for account_id, source_type, count, first, last in rows:
        evidence[str(account_id)].append(
            SourceEvidence(
                source_type=str(source_type),
                trade_count=int(count),
                first_trade_date=first,
                last_trade_date=last,
                holdings_only=source_type == "plaid" and int(count) == 0,
            )
        )
    return {
        account_id: sorted(found, key=lambda e: e.source_type)
        for account_id, found in evidence.items()
    }


def investment_source_choice_counts(db: Database) -> dict[str, int]:
    """Accounts per chosen source type; every accepted type is a key, zeros included."""
    counts: dict[str, int] = {}
    if has_column(db, ACCOUNT_SETTINGS, "investment_source_type"):
        rows = db.execute(
            f"""
            SELECT investment_source_type, COUNT(*)
            FROM {ACCOUNT_SETTINGS.full_name}
            WHERE investment_source_type IS NOT NULL
            GROUP BY 1
            """  # noqa: S608  # TableRef constant
        ).fetchall()
        counts = {str(source): int(n) for source, n in rows}
    return {source: counts.get(source, 0) for source in sorted(INVESTMENT_SOURCE_TYPES)}


def history_phrase(source_type: str) -> str:
    """The user-facing name of a source's history."""
    if source_type == "manual":
        return "the recorded history"
    if source_type == "plaid":
        return "the connection's history"
    return f"the {source_type} history"


def _trades(count: int) -> str:
    return f"{count:,} trade{'s' if count != 1 else ''}"


def evidence_phrase(evidence: SourceEvidence) -> str:
    """Count and date range, e.g. ``412 trades, 2019-03-04 → 2025-11-21``."""
    if evidence.holdings_only:
        return "a holdings snapshot, no trades"
    phrase = _trades(evidence.trade_count)
    if evidence.first_trade_date and evidence.last_trade_date:
        phrase += (
            f", {evidence.first_trade_date.isoformat()} → "
            f"{evidence.last_trade_date.isoformat()}"
        )
    return phrase


def source_adjective(source_type: str) -> str:
    """The user-facing word for a source's trades, e.g. ``recorded``."""
    return _ADJECTIVES.get(source_type, source_type)


def trade_count_phrase(evidence: SourceEvidence) -> str:
    """Count in the user's words, e.g. ``412 recorded trades``."""
    adjective = source_adjective(evidence.source_type)
    if evidence.holdings_only:
        return f"a {adjective} holdings snapshot"
    return _trades(evidence.trade_count).replace(" trade", f" {adjective} trade")
