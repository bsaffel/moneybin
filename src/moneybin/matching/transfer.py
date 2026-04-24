"""Transfer detection: candidate blocking and confidence scoring.

Tier 4 of the matching pipeline. Finds transactions from different accounts
with opposite signs and the same absolute amount, scores them on four signals,
and returns scored candidate pairs for 1:1 assignment.
"""

import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.matching import UNIONED_TABLE, quote_table_ref

logger = logging.getLogger(__name__)

_TRANSFER_KEYWORDS = frozenset({
    "TRANSFER",
    "XFER",
    "ACH",
    "DIRECT DEP",
    "WIRE",
    # Compound directional phrases — matched atomically (longest-first) so
    # sub-phrases like "TRANSFER" or "FROM CHK" don't double-count.
    "ONLINE TRANSFER TO SAV",
    "ONLINE TRANSFER TO CHK",
    "ONLINE TRANSFER TO SAVINGS",
    "ONLINE TRANSFER TO CHECKING",
    "ONLINE TRANSFER FROM SAV",
    "ONLINE TRANSFER FROM CHK",
    "ONLINE TRANSFER",
    "MOBILE TRANSFER",
    "INTERNAL TRANSFER",
    "TRANSFER FROM CHECKING",
    "TRANSFER FROM SAVINGS",
    "TRANSFER TO CHECKING",
    "TRANSFER TO SAVINGS",
    "TRANSFER FROM CHK",
    "TRANSFER FROM SAV",
    "TRANSFER TO CHK",
    "TRANSFER TO SAV",
    "FROM CHK",
    "FROM SAV",
    "TO CHK",
    "TO SAV",
    "FROM CHECKING",
    "FROM SAVINGS",
    "TO CHECKING",
    "TO SAVINGS",
})

# Regex that matches keywords longest-first, non-overlapping left-to-right.
# Built once at import time.
_KEYWORD_PATTERN = re.compile(
    "|".join(
        r"\b" + re.escape(kw) + r"\b"
        for kw in sorted(_TRANSFER_KEYWORDS, key=len, reverse=True)
    )
)


@dataclass(frozen=True)
class TransferCandidatePair:
    """A scored transfer candidate pair from blocking + scoring."""

    source_transaction_id_a: str
    source_type_a: str
    source_origin_a: str
    account_id_a: str
    source_transaction_id_b: str
    source_type_b: str
    source_origin_b: str
    account_id_b: str
    amount: Decimal
    date_distance_days: int
    description_a: str
    description_b: str
    date_distance_score: float
    keyword_score: float
    amount_roundness_score: float
    pair_frequency_score: float
    confidence_score: float


def compute_keyword_score(desc_a: str, desc_b: str) -> float:
    """Score based on transfer-indicating keywords in either description.

    Uses a longest-first non-overlapping regex so that compound phrases like
    "ONLINE TRANSFER" consume their span before shorter sub-keywords like
    "TRANSFER" can match again within that same span.
    """
    combined = f"{desc_a} {desc_b}".upper()
    matches = len(_KEYWORD_PATTERN.findall(combined))
    if matches >= 3:
        return 1.0
    if matches >= 2:
        return 0.8
    if matches >= 1:
        return 0.5
    return 0.0


def compute_amount_roundness(amount: Decimal) -> float:
    """Score based on how round the transfer amount is."""
    abs_amount = abs(amount)
    if abs_amount % 100 == 0:
        return 1.0
    if abs_amount % 10 == 0:
        return 0.7
    if abs_amount % 1 == 0:
        return 0.5
    return 0.3


def compute_pair_frequency(
    account_id_a: str,
    account_id_b: str,
    pair_counts: dict[tuple[str, str], int],
    max_count: int,
) -> float:
    """Score based on how often this account pair appears in the batch."""
    sorted_ids = sorted([account_id_a, account_id_b])
    key: tuple[str, str] = (sorted_ids[0], sorted_ids[1])
    count = pair_counts.get(key, 0)
    return min(1.0, count / max(max_count, 1))


def compute_date_score(date_distance_days: int, date_window_days: int) -> float:
    """Score based on temporal proximity within the date window."""
    if date_window_days <= 0:
        return 1.0
    return max(0.0, 1.0 - (date_distance_days / date_window_days))


def compute_transfer_confidence(
    *,
    date_score: float,
    keyword_score: float,
    amount_roundness: float,
    pair_frequency: float,
    weights: dict[str, float],
) -> float:
    """Compute transfer confidence from four pre-computed, weighted signals."""
    return (
        weights["date_distance"] * date_score
        + weights["keyword"] * keyword_score
        + weights["roundness"] * amount_roundness
        + weights["pair_frequency"] * pair_frequency
    )


def get_candidates_transfers(
    db: Database,
    *,
    table: str = UNIONED_TABLE,
    date_window_days: int = 3,
    excluded_ids: set[tuple[str, str, str]] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
    signal_weights: dict[str, float],
) -> list[TransferCandidatePair]:
    """Find transfer candidate pairs (Tier 4).

    Blocking: different accounts, opposite signs, exact absolute amount,
    date within window. Side A is always the debit (negative amount).
    """
    safe_ref = quote_table_ref(table)

    query = f"""
        SELECT
            a.source_transaction_id AS stid_a,
            a.source_type AS st_a,
            a.source_origin AS so_a,
            a.account_id AS acct_a,
            a.description AS desc_a,
            a.amount AS amount_a,
            b.source_transaction_id AS stid_b,
            b.source_type AS st_b,
            b.source_origin AS so_b,
            b.account_id AS acct_b,
            b.description AS desc_b,
            b.amount AS amount_b,
            ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) AS date_dist
        FROM {safe_ref} AS a
        JOIN {safe_ref} AS b
            ON a.account_id != b.account_id
            AND a.amount < 0
            AND b.amount > 0
            AND ABS(a.amount) = b.amount
            AND a.currency_code = b.currency_code
            AND ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) <= ?
        ORDER BY date_dist ASC
    """  # noqa: S608 — table name validated above; date_window_days is parameterized

    rows = db.execute(query, [date_window_days]).fetchall()

    rejected_set: set[tuple[str, ...]] = set()
    if rejected_pairs:
        for rp in rejected_pairs:
            acct_a = rp.get("account_id") or ""
            acct_b = rp.get("account_id_b") or ""
            rejected_set.add((
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                acct_a,
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                acct_b,
            ))
            rejected_set.add((
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                acct_b,
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                acct_a,
            ))

    # Pass 1: filter rows and build pair-frequency counts (needed for scoring).
    filtered: list[dict[str, Any]] = []
    pair_counts: dict[tuple[str, str], int] = {}

    for row in rows:
        stid_a, st_a, so_a, acct_a, desc_a, amount_a = row[:6]
        stid_b, st_b, so_b, acct_b, desc_b, _amount_b, date_dist = row[6:]

        if excluded_ids and (
            (stid_a, st_a, acct_a) in excluded_ids
            or (stid_b, st_b, acct_b) in excluded_ids
        ):
            continue

        if (st_a, stid_a, acct_a, st_b, stid_b, acct_b) in rejected_set:
            continue

        filtered.append({
            "stid_a": stid_a,
            "st_a": st_a,
            "so_a": so_a,
            "acct_a": acct_a,
            "desc_a": desc_a,
            "amount_a": amount_a,
            "stid_b": stid_b,
            "st_b": st_b,
            "so_b": so_b,
            "acct_b": acct_b,
            "desc_b": desc_b,
            "date_dist": int(date_dist),
        })
        sorted_accts = sorted([acct_a, acct_b])
        freq_key: tuple[str, str] = (sorted_accts[0], sorted_accts[1])
        pair_counts[freq_key] = pair_counts.get(freq_key, 0) + 1

    max_count = max(pair_counts.values()) if pair_counts else 1
    # Pass 2: score each candidate using pair-frequency data from pass 1.
    results: list[TransferCandidatePair] = []
    for p in filtered:
        abs_amount = abs(p["amount_a"])
        kw_score = compute_keyword_score(p["desc_a"] or "", p["desc_b"] or "")
        roundness = compute_amount_roundness(abs_amount)
        pair_freq = compute_pair_frequency(
            p["acct_a"], p["acct_b"], pair_counts, max_count
        )
        d_score = compute_date_score(p["date_dist"], date_window_days)
        confidence = compute_transfer_confidence(
            date_score=d_score,
            keyword_score=kw_score,
            amount_roundness=roundness,
            pair_frequency=pair_freq,
            weights=signal_weights,
        )

        results.append(
            TransferCandidatePair(
                source_transaction_id_a=p["stid_a"],
                source_type_a=p["st_a"],
                source_origin_a=p["so_a"],
                account_id_a=p["acct_a"],
                source_transaction_id_b=p["stid_b"],
                source_type_b=p["st_b"],
                source_origin_b=p["so_b"],
                account_id_b=p["acct_b"],
                amount=abs_amount,
                date_distance_days=p["date_dist"],
                description_a=p["desc_a"] or "",
                description_b=p["desc_b"] or "",
                date_distance_score=d_score,
                keyword_score=kw_score,
                amount_roundness_score=roundness,
                pair_frequency_score=pair_freq,
                confidence_score=confidence,
            )
        )

    return results
