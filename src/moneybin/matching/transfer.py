"""Transfer detection: candidate blocking and confidence scoring.

Tier 4 of the matching pipeline. Finds transactions from different accounts
with opposite signs and the same absolute amount, scores them on four signals,
and returns scored candidate pairs for 1:1 assignment.
"""

import logging
import re
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

UNIONED_TABLE = "prep.int_transactions__unioned"

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
    "|".join(re.escape(kw) for kw in sorted(_TRANSFER_KEYWORDS, key=len, reverse=True))
)

_DEFAULT_WEIGHTS: dict[str, float] = {
    "date_distance": 0.4,
    "keyword": 0.3,
    "roundness": 0.15,
    "pair_frequency": 0.15,
}


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
    key = tuple(sorted([account_id_a, account_id_b]))
    count = pair_counts.get(key, 0)
    return min(1.0, count / max(max_count, 1))


def compute_transfer_confidence(
    *,
    date_distance_days: int,
    date_window_days: int,
    keyword_score: float,
    amount_roundness: float,
    pair_frequency: float,
    weights: dict[str, float] | None = None,
) -> float:
    """Compute transfer confidence from four weighted signals."""
    w = weights or _DEFAULT_WEIGHTS
    date_score = (
        max(0.0, 1.0 - (date_distance_days / date_window_days))
        if date_window_days > 0
        else 1.0
    )
    return (
        w["date_distance"] * date_score
        + w["keyword"] * keyword_score
        + w["roundness"] * amount_roundness
        + w["pair_frequency"] * pair_frequency
    )
