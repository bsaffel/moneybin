"""Candidate blocking and confidence scoring for transaction matching.

Blocking: SQL queries against DuckDB that return narrow candidate sets
based on exact account, exact amount, and date-window constraints.

Scoring: Combines date distance and description similarity into a single
confidence score. Weights are tunable but defaults are spec-compliant.
"""

import logging
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.matching import UNIONED_TABLE, quote_table_ref
from moneybin.matching.persistence import MatchTier

logger = logging.getLogger(__name__)

# Scoring weights — sum to 1.0.
# Description outweighs date deliberately. The blocking query has already
# required the same account, an exact amount match, and a date inside the
# window, so description is the only signal left that discriminates; date only
# modulates. The weight is also load-bearing for a correctness property:
# date_score decays to 0 at the window edge, so a pair sitting there scores
# _WEIGHT_DESCRIPTION * similarity and nothing more. With the description weight
# below the review threshold, the edge of the window is a dead zone — admitted
# as a candidate, mathematically unable to be reviewed even with identical
# descriptions. Keep _WEIGHT_DESCRIPTION >= MatchingSettings.review_threshold.
_WEIGHT_DATE = 0.30
_WEIGHT_DESCRIPTION = 0.70


def _normalized_description(column: str) -> str:
    """SQL expression canonicalizing a description for the agreement test.

    Case-folds, reduces punctuation to a space, collapses internal whitespace
    runs, and trims. Sources pad descriptions to fixed column widths, so the same
    merchant string arrives with different runs of spaces; without the collapse,
    two renderings of one transaction fail to relate at all.

    Punctuation is reduced rather than deleted, which is the difference between
    `WAL-MART` matching `WAL MART` (it does) and matching `WALMART` (it does
    not). Deleting it would silently merge pairs whose word boundaries genuinely
    differ; reducing it only absorbs the separator characters one source prints
    and another omits — `STARBUCKS #1234` against `STARBUCKS 1234`. Since
    agreement is now the sole route to an auto-merge (see
    ``TransactionMatcher._classify_pair``), a single `#` deciding that question
    was costing a human decision on the ordinary case this feature exists for.
    A digit is not punctuation, so `SHELL 1234` and `SHELL 1235` still disagree.

    Deliberately *only* canonicalization — no token stripping. Running the
    categorization normalizer here was measured on live data and rejected: it
    moved average similarity by +0.011 and produced more low-similarity pairs,
    because its trailing-location pattern needs a plain capitalized word before
    the state code and statement layouts do not supply one.
    """
    depunctuated = f"REGEXP_REPLACE(UPPER(COALESCE({column}, '')), '[^\\p{{L}}\\p{{N}}]', ' ', 'g')"
    return f"TRIM(REGEXP_REPLACE({depunctuated}, '\\s+', ' ', 'g'))"


# Words describing *what kind* of movement a row is, never *who* was paid.
# Containment on these alone is not merchant evidence: `DEBIT` sits inside most
# card descriptions, so a source that renders a row as bare boilerplate would
# agree with any unrelated row it happened to collide with on amount inside the
# window — and auto-merge it, destroying a real transaction with no review entry.
# Single tokens, because the test splits on whitespace: `ACH DEBIT` is covered by
# ACH plus DEBIT. Adding a token here widens what merges silently, so the set is
# pinned by equality in test_scoring.py rather than by a membership check.
_BOILERPLATE_TOKENS = frozenset({
    "ACH",
    "ATM",
    "AUTHORIZED",
    "CARD",
    "CHECK",
    "CREDIT",
    "DEBIT",
    "DEPOSIT",
    "EFT",
    "ELECTRONIC",
    "FEE",
    "PAYMENT",
    "POS",
    "PURCHASE",
    "RECURRING",
    "TRANSACTION",
    "TRANSFER",
    "WITHDRAWAL",
})


def _carries_a_merchant_token(normalized: str) -> str:
    """SQL expression: this description has at least one word naming a merchant.

    One merchant word is enough. Sources prepend their own transaction-type words
    to a real merchant string, so rejecting any description that *contains*
    boilerplate would throw away the agreements this gate exists to find; only a
    description carrying nothing else is uninformative.

    A qualifying token must carry a letter. Absent that, a card or reference
    number satisfies the rule while naming nobody: `POS 1234` is boilerplate plus
    a number, and it sits inside every longer description from the other source
    that prints the same digits — so two distinct charges on one card, equal in
    amount and inside the window, would auto-merge and one would vanish with no
    review entry. The requirement is a letter *somewhere in the token*, not the
    absence of digits: real merchant strings are full of them (`7ELEVEN`).
    """
    tokens = ", ".join(f"'{token}'" for token in sorted(_BOILERPLATE_TOKENS))
    return (
        f"len(list_filter(string_split({normalized}, ' '), "
        f"token -> NOT list_contains([{tokens}], token) "
        f"AND regexp_matches(token, '\\p{{L}}'))) > 0"
    )


def _contains_on_word_boundaries(container: str, contained: str) -> str:
    """SQL expression: ``contained`` sits inside ``container`` on word boundaries.

    Raw substring containment reads `ARCO` as agreeing with `MARCOS PIZZA`,
    naming a merchant in both and meaning a different one in each. Padding both
    sides with spaces is what makes a match start where a word starts.

    The end is the harder half, because the shape this feature exists to absorb —
    a source truncating a shared string — ends mid-word by definition, and so does
    a coincidence (`SHELL` beginning `SHELLYS CAFE`). Nothing in the strings tells
    them apart. What differs is how much matched *whole*: a real truncation
    usually cuts several words in, and those earlier whole words are the evidence
    a bare one-word prefix does not have. So the match must either end on a word
    boundary too, or carry a complete word before its partial tail.

    That complete word must itself name something. Counting any word would let
    boilerplate stand in as evidence — `CARD SHELL` against `CARD SHELLYS CAFE`
    shares nothing but a word every card description carries and a cut-off
    fragment, which is the one-word-prefix hole again with a prefix on it. So the
    test runs over the contained side minus its final token: everything that
    matched whole, and nothing that did not.
    """
    whole_prefix = f"array_to_string(string_split({contained}, ' ')[1:-2], ' ')"
    padded = f"contains(' ' || {container} || ' ', ' ' || {contained} || ' ')"
    partial_tail = (
        f"contains(' ' || {container}, ' ' || {contained}) "
        f"AND {_carries_a_merchant_token(whole_prefix)}"
    )
    return f"({padded} OR ({partial_tail}))"


@dataclass(frozen=True)
class CandidatePair:
    """A scored candidate pair from blocking + scoring."""

    source_transaction_id_a: str
    source_type_a: str
    source_origin_a: str
    source_transaction_id_b: str
    source_type_b: str
    source_origin_b: str
    account_id: str
    date_distance_days: int
    description_similarity: float
    confidence_score: float
    description_a: str
    description_b: str
    # Whether one description contains the other — the signal that earns a
    # cross-source auto-merge. Recorded so a silent merge can be explained after
    # the fact rather than re-derived from the two strings.
    descriptions_agree: bool = False
    # Source file per side — the cardinality unit for the assign_components guard
    # (two rows from the same file are always distinct txns, never duplicates).
    # None when unknown (e.g. unit-test fixtures); the guard then does not fire.
    source_file_a: str | None = None
    source_file_b: str | None = None
    # Transfer-only; dedup pairs leave these None (used by _claim_key for slot scoping).
    account_id_a: str | None = None
    account_id_b: str | None = None


def compute_confidence(
    *,
    date_distance_days: int,
    description_similarity: float,
    date_window_days: int = 3,
    agreement_floor: float | None = None,
    descriptions_agree: bool = False,
) -> float:
    """Compute a confidence score from matching signals.

    When ``agreement_floor`` is set and the two descriptions agree, confidence is
    lifted into ``[agreement_floor, 1.0]`` so the pair auto-merges. Description
    agreement — not the date — is what earns a silent merge: blocking has already
    fixed the account and the exact amount, so description is the only remaining
    evidence, while a date gap is just posting lag and says nothing about whether
    two rows are the same transaction.

    ``description_similarity`` stays a monotonic *tiebreaker* above the floor — it
    orders which 1:1 pairing wins in ``assign_components`` — never an accept/reject
    gate. Pairs whose descriptions disagree fall through to the weighted formula
    and land in review, which is where an ambiguous inference belongs.
    """
    date_score = (
        max(0.0, 1.0 - (date_distance_days / date_window_days))
        if date_window_days > 0
        else 1.0
    )
    if agreement_floor is not None and descriptions_agree:
        return agreement_floor + (1.0 - agreement_floor) * description_similarity
    return (_WEIGHT_DATE * date_score) + (_WEIGHT_DESCRIPTION * description_similarity)


def get_candidates_cross_source(
    db: Database,
    *,
    table: str = UNIONED_TABLE,
    date_window_days: int = 3,
    excluded_ids: set[tuple[str, str]] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
    high_confidence_threshold: float | None = None,
) -> list[CandidatePair]:
    """Find cross-source candidate pairs (Tier 3).

    Blocking: same account_id, same amount, date within window,
    different source_type OR different source_origin.

    When ``high_confidence_threshold`` is supplied, pairs whose descriptions
    agree — one containing the other, after normalization — are scored at/above it
    so they auto-merge at any date gap inside the window (see
    ``compute_confidence``). Pairs whose descriptions disagree keep the weighted
    formula and land in review.
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="3",
        excluded_ids=excluded_ids,
        rejected_pairs=rejected_pairs,
        high_confidence_threshold=high_confidence_threshold,
    )


def get_candidates_within_source(
    db: Database,
    *,
    table: str = UNIONED_TABLE,
    date_window_days: int = 3,
    excluded_ids: set[tuple[str, str]] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
) -> list[CandidatePair]:
    """Find within-source candidate pairs (Tier 2b).

    Same as cross-source but requires same source_origin AND source_type,
    different source_file.
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="2b",
        excluded_ids=excluded_ids,
        rejected_pairs=rejected_pairs,
    )


def _get_candidates(
    db: Database,
    *,
    table: str,
    date_window_days: int,
    tier: MatchTier,
    excluded_ids: set[tuple[str, str]] | None,
    rejected_pairs: list[dict[str, Any]] | None,
    high_confidence_threshold: float | None = None,
) -> list[CandidatePair]:
    """Internal: run blocking + scoring query for a given tier."""
    # Description-agreement auto-merge is a cross-source-only rule (Tier 3).
    # Across sources each side lists a transaction once, so two agreeing rows are
    # one transaction rendered twice. Inside one source the rendering is
    # consistent, so two rows written *differently* are two transactions — the
    # floor there would silently delete one. Tier 2b keeps the weighted formula.
    agreement_floor = high_confidence_threshold if tier == "3" else None
    if tier == "2b":
        source_filter = """
            AND a.source_type = b.source_type
            AND a.source_origin = b.source_origin
            AND a.source_file != b.source_file
        """
    else:
        source_filter = """
            AND (a.source_type != b.source_type OR a.source_origin != b.source_origin)
        """

    table = quote_table_ref(table)
    norm_a = _normalized_description("a.description")
    norm_b = _normalized_description("b.description")
    merchant_a = _carries_a_merchant_token(norm_a)
    merchant_b = _carries_a_merchant_token(norm_b)
    contains_ab = _contains_on_word_boundaries(norm_a, norm_b)
    contains_ba = _contains_on_word_boundaries(norm_b, norm_a)

    # Manual-source exemption: per transaction-curation spec Req 6, manual rows
    # are excluded as candidates in *either* direction — never matched against
    # imported rows, never matched against other manual rows. Predicate is
    # applied to both sides of the self-join.
    query = f"""
        SELECT
            a.source_transaction_id AS stid_a,
            a.source_type AS st_a,
            a.source_origin AS so_a,
            a.source_file AS sf_a,
            a.description AS desc_a,
            b.source_transaction_id AS stid_b,
            b.source_type AS st_b,
            b.source_origin AS so_b,
            b.source_file AS sf_b,
            b.description AS desc_b,
            a.account_id,
            ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) AS date_dist,
            jaro_winkler_similarity(
                COALESCE(a.description, ''),
                COALESCE(b.description, '')
            ) AS desc_sim,
            -- Descriptions agree when one contains the other. That is the literal
            -- mechanism: sources carry a shared merchant string, truncate it at
            -- different lengths, and wrap it in their own preamble and trailing
            -- detail, so the common text is not always at the front. Structural,
            -- so it needs no similarity cutoff to tune.
            -- Both sides must be non-empty: contains(x, '') is TRUE, so a source
            -- that omits descriptions would otherwise agree with every row it met
            -- and merge an entire account silently.
            -- The *contained* side is the shared evidence, so it is the side that
            -- must carry a merchant token. An empty description is the extreme
            -- case of the same defect; bare boilerplate is the merely-generic one.
            -- Equality is exempt from that requirement on the same day: the
            -- merchant token guards against a short *fragment* hiding inside a
            -- longer string, and two identical descriptions have no longer string
            -- to hide inside. A bank writing `DEPOSIT` in both exports cannot name
            -- a merchant, and demanding one would refuse the plainest duplicate
            -- there is. The date bound is what keeps that exemption from becoming
            -- a hole now the window is five days wide: two *different* charges of
            -- one amount, days apart, are both `DEBIT` too, and nothing else in
            -- the row tells them apart. Same-day is where the carve-out was
            -- validated, and a generic pair at a gap still reaches Tier 3 review.
            -- Containment is word-bounded on both ends (see
            -- _contains_on_word_boundaries): a match starts where a word starts,
            -- and either ends where one ends or carries a whole word before its
            -- partial tail. That refuses `ARCO` inside `MARCOS PIZZA` and `SHELL`
            -- inside `SHELLYS CAFE`, while a truncating source that cuts several
            -- words in still relates to the full string.
            (
                {norm_a} <> ''
                AND {norm_b} <> ''
                AND (
                    (
                        {norm_a} = {norm_b}
                        AND ({merchant_a} OR a.transaction_date = b.transaction_date)
                    )
                    OR ({contains_ab} AND {merchant_b})
                    OR ({contains_ba} AND {merchant_a})
                )
            ) AS desc_agree
        FROM {table} AS a
        JOIN {table} AS b
            ON a.account_id = b.account_id
            AND a.amount = b.amount
            AND ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) <= ?
            AND a.source_type != 'manual'
            AND b.source_type != 'manual'
            AND (
                a.source_type,
                a.source_origin,
                a.source_transaction_id
            ) < (
                b.source_type,
                b.source_origin,
                b.source_transaction_id
            )
            {source_filter}
        ORDER BY desc_sim DESC
    """  # noqa: S608 — table name validated above; date_window_days is parameterized

    rows = db.execute(query, [date_window_days]).fetchall()

    # Build rejected pair set for fast lookup
    rejected_set: set[tuple[str, ...]] = set()
    if rejected_pairs:
        for rp in rejected_pairs:
            rejected_set.add((
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                rp["account_id"],
            ))
            rejected_set.add((
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                rp["account_id"],
            ))

    results: list[CandidatePair] = []
    for row in rows:
        (
            stid_a,
            st_a,
            so_a,
            sf_a,
            desc_a,
            stid_b,
            st_b,
            so_b,
            sf_b,
            desc_b,
            acct,
            date_dist,
            desc_sim,
            desc_agree,
        ) = row

        if excluded_ids and (
            (stid_a, acct) in excluded_ids or (stid_b, acct) in excluded_ids
        ):
            continue

        if (st_a, stid_a, st_b, stid_b, acct) in rejected_set:
            continue

        confidence = compute_confidence(
            date_distance_days=int(date_dist),
            description_similarity=float(desc_sim),
            date_window_days=date_window_days,
            agreement_floor=agreement_floor,
            descriptions_agree=bool(desc_agree),
        )

        results.append(
            CandidatePair(
                source_transaction_id_a=stid_a,
                source_type_a=st_a,
                source_origin_a=so_a,
                source_transaction_id_b=stid_b,
                source_type_b=st_b,
                source_origin_b=so_b,
                account_id=acct,
                date_distance_days=int(date_dist),
                description_similarity=float(desc_sim),
                confidence_score=confidence,
                description_a=desc_a or "",
                description_b=desc_b or "",
                descriptions_agree=bool(desc_agree),
                source_file_a=sf_a,
                source_file_b=sf_b,
            )
        )

    return results
