"""Ledger-overlap probe: do two accounts already hold the same transactions?

An account-link proposal fires on an identifier — an institution plus a masked
last four, or a name that slugifies the same way. None of that is evidence about
the *ledgers*, and the reviewer ratifying a whole-account merge has no other way
to tell a genuine cross-source twin from two distinct accounts that happen to
share a signal. The discriminating evidence is already in ``core`` and was never
consulted.

Deliberately keyed on **two account ids** rather than a decision id. The
matcher cannot answer this question — ``matching/scoring.py`` blocks candidate
pairs on the same ``account_id``, so it can only compute overlap after the merge
that overlap is meant to justify. Keeping the probe's signature free of the
review queue also leaves it reachable for a pair that has no proposal at all,
which is the shape ``system doctor``'s ``duplicate_account_overlap`` audit
detects and currently has no merge path for.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import duckdb

from moneybin.database import Database
from moneybin.metrics.registry import ACCOUNT_LINK_OVERLAP_PROBES_TOTAL
from moneybin.tables import FCT_TRANSACTIONS

logger = logging.getLogger(__name__)

#: Days of posting lag tolerated before two rows stop being the same transaction.
#:
#: A statement carries the transaction date and a feed the posting date, so the
#: same purchase is dated differently by each source. Measured on a live twin
#: pair, amount within ±3 days matched 345 of 346 rows against the true twin and
#: 0 of 346 against each of two controls; requiring an exact date collapsed that
#: to 23%. The window is what makes the probe discriminate rather than the amount.
#:
#: Deliberately **not** ``settings.matching.date_window_days`` (default 5), which
#: `system doctor`'s ``duplicate_account_overlap`` does reuse for a similar-looking
#: job. Two reasons, and they are about what the number is for rather than what it
#: measures. It is a calibration, not a preference: the 0-of-346 control result is
#: the whole claim that this ratio discriminates, and it was measured at this
#: width — a user who widens the window gets a higher number that means less,
#: with nothing on the surface saying so. And the two call sites carry different
#: consequences: doctor's window tunes what an audit *flags*, while this one
#: supplies the evidence a human ratifies an irreversible whole-ledger merge on.
#: Callers that genuinely need another width pass ``window_days``; consolidating
#: the two would require re-running the twin-and-controls measurement at the
#: shared width first, which is the part that cannot be assumed.
DEFAULT_POSTING_LAG_DAYS = 3

_EMPTY_WINDOW_ROW = (0, 0, None, None)


@dataclass(frozen=True)
class LedgerOverlap:
    """How much of one account's ledger the other account already holds.

    ``comparable`` counts only the rows that *could* match — those inside the
    period the other ledger covers. Rows outside it are excluded rather than
    counted as misses: a statement archive predating a feed's download window
    would otherwise render as "0 of 400 match", which reads as evidence against
    a correct merge when it is only evidence the other account was not there yet.

    ``comparable == 0`` therefore means *no comparable period*, not *no overlap*.
    The two must stay distinguishable to a reader — one is absence of evidence
    and the other is evidence of absence — which is what ``measurable`` names.
    """

    comparable: int
    matched: int
    window_start: date | None
    window_end: date | None
    window_days: int = DEFAULT_POSTING_LAG_DAYS
    """The posting-lag tolerance ``matched`` was counted at, carried to the surface.

    A reader shown "345 of 346" cannot tell whether that is exact-date agreement
    or agreement within a window, and the two support very different
    conclusions: at this width a measured control pair scored 0 of 346, while
    requiring an exact date collapsed the true twin to 23%. The number is only
    evidence once the tolerance travels with it.
    """

    @property
    def measurable(self) -> bool:
        """Whether the two ledgers share a period this probe could compare at all."""
        return self.comparable > 0


@dataclass(frozen=True)
class LedgerSpan:
    """The period one account's ledger covers, first row to last.

    Coarser than :class:`LedgerOverlap` and answering a different question. The
    overlap probe asks whether two accounts hold the *same rows*; this asks
    whether they were *alive at the same time*. A merge proposal that claims one
    account replaced another is making the second claim, and it can be refuted
    without matching a single transaction.
    """

    first_date: date
    last_date: date

    def concurrent_with(self, other: LedgerSpan, *, tolerance_days: int) -> bool:
        """Whether both ledgers ran at once for longer than ``tolerance_days``.

        Deliberately asymmetric about what it proves. A pair that clears the
        tolerance is positively concurrent — both accounts were posting
        transactions over the same months, so neither replaced the other. A pair
        that does not clear it has only failed to be concurrent, which is what
        every sequential pair looks like and also what a pair with one thin
        ledger looks like; the caller must not read it as evidence *for* a
        merge.
        """
        shared = min(self.last_date, other.last_date) - max(
            self.first_date, other.first_date
        )
        return shared.days > tolerance_days


def fetch_ledger_spans(
    db: Database, account_ids: Sequence[str]
) -> dict[str, LedgerSpan]:
    """First and last transaction date per account, for those that have any.

    An account with no rows is **absent** from the result rather than mapped to
    an empty span, so a caller cannot accidentally read "no ledger yet" as "a
    ledger that overlaps nothing". At import time that is the normal state: the
    account was minted seconds ago and no transform has published it.
    """
    ids = sorted({account_id for account_id in account_ids if account_id})
    if not ids:
        return {}
    placeholders = ", ".join("?" * len(ids))
    try:
        rows = db.execute(
            f"SELECT account_id, MIN(transaction_date), MAX(transaction_date) "  # noqa: S608  # TableRef constant + parameterized values
            f"FROM {FCT_TRANSACTIONS.full_name} "
            f"WHERE account_id IN ({placeholders}) GROUP BY account_id",
            list(ids),
        ).fetchall()
    except duckdb.CatalogException:
        logger.debug("core.fct_transactions unavailable; ledger spans unknown")
        return {}
    return {
        str(row[0]): LedgerSpan(first_date=row[1], last_date=row[2])
        for row in rows
        if row[1] is not None and row[2] is not None
    }


@dataclass(frozen=True)
class IncomingTransaction:
    """One normalized incoming row available before an account is resolved."""

    transaction_date: date
    amount: Decimal
    currency_code: str | None


def probe_incoming_ledger_overlap(
    db: Database,
    *,
    transactions: Sequence[IncomingTransaction],
    against_account_id: str,
    window_days: int = DEFAULT_POSTING_LAG_DAYS,
) -> LedgerOverlap:
    """Compare incoming rows with one existing account without binding them first.

    An unstated currency is unknown, not a disagreement. Only two stated,
    different currencies veto an otherwise matching incoming transaction.
    """
    if not transactions:
        return _record_probe(LedgerOverlap(*_EMPTY_WINDOW_ROW, window_days=window_days))

    # Callers pass one statement's extracted rows, not an unbounded ledger.
    values_sql = ", ".join(["(?, ?, ?, ?)"] * len(transactions))
    parameters: list[object] = []
    for index, transaction in enumerate(transactions):
        parameters.extend([
            index,
            transaction.transaction_date,
            transaction.amount,
            transaction.currency_code,
        ])
    parameters.extend([against_account_id, window_days, window_days, window_days])
    try:
        row = db.execute(
            f"""
            WITH incoming_values(
                incoming_id, transaction_date, amount, currency_code
            ) AS (
                VALUES {values_sql}
            ),
            against AS (
                SELECT transaction_date, amount, currency_code
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE account_id = ?
            ),
            span AS (
                SELECT MIN(transaction_date) AS lo, MAX(transaction_date) AS hi
                FROM against
            ),
            comparable AS (
                SELECT incoming.*
                FROM incoming_values AS incoming, span
                WHERE span.lo IS NOT NULL
                  AND incoming.transaction_date
                      BETWEEN span.lo - CAST(? AS INTEGER)
                          AND span.hi + CAST(? AS INTEGER)
            )
            SELECT
                (SELECT COUNT(*) FROM comparable),
                (SELECT COUNT(*) FROM (
                    SELECT c.incoming_id
                    FROM comparable AS c
                    JOIN against AS a
                      ON a.amount = c.amount
                     AND (
                         NULLIF(UPPER(TRIM(a.currency_code)), '') IS NULL
                         OR NULLIF(UPPER(TRIM(c.currency_code)), '') IS NULL
                         OR NULLIF(UPPER(TRIM(a.currency_code)), '') =
                            NULLIF(UPPER(TRIM(c.currency_code)), '')
                     )
                     AND ABS(DATE_DIFF('day', a.transaction_date, c.transaction_date))
                         <= CAST(? AS INTEGER)
                    GROUP BY c.incoming_id
                )),
                (SELECT MIN(transaction_date) FROM comparable),
                (SELECT MAX(transaction_date) FROM comparable)
            """,  # noqa: S608  # TableRef and VALUES shape are code-owned; values parameterized
            parameters,
        ).fetchone()
    except duckdb.CatalogException:
        logger.debug("core.fct_transactions unavailable; ledger overlap unmeasurable")
        row = None
    comparable, matched, start, end = row or _EMPTY_WINDOW_ROW
    return _record_probe(
        LedgerOverlap(
            comparable=int(comparable),
            matched=int(matched),
            window_start=start,
            window_end=end,
            window_days=window_days,
        )
    )


def _record_probe(overlap: LedgerOverlap) -> LedgerOverlap:
    """Record one overlap probe and return its result."""
    ACCOUNT_LINK_OVERLAP_PROBES_TOTAL.labels(
        result="measurable" if overlap.measurable else "unmeasurable"
    ).inc()
    return overlap


def probe_ledger_overlap(
    db: Database,
    *,
    account_id: str,
    against_account_id: str,
    window_days: int = DEFAULT_POSTING_LAG_DAYS,
) -> LedgerOverlap:
    """Count how many of ``account_id``'s transactions ``against_account_id`` holds.

    Directional on purpose: the question a merge asks is what happens to the
    absorbed account's history, so ``account_id`` is the ledger being probed and
    ``against_account_id`` is the one it is checked against. Reversing them
    answers a different question and yields a different ratio.

    Matching is amount-equal *in the same currency* within ``window_days``, by
    existence rather than by a one-to-one assignment: a repeated amount inside
    the window can be answered by the same counterpart twice, which can only
    inflate the ratio. The controls measured 0 of 346 even so, and a genuine
    assignment costs a join this read does not need to be worth showing.

    The currency comparison is NULL-safe, which is a deliberate asymmetry: only
    a *stated* disagreement vetoes a match. ``currency_code`` is nullable — it
    is the transaction's own captured currency, else the one inherited from
    ``dim_accounts`` — so a plain ``=`` would score every pair of
    unknown-currency ledgers at zero overlap and turn a genuine twin's evidence
    off silently. Treating NULL as a value rather than a wildcard costs a true
    twin its evidence only where one source states a currency and the other
    does not, and refuses the case that matters: a nominal amount is not a sum
    of money until a currency names it, so a USD row and a EUR row are never
    the same transaction however equal they look.

    Both sides are folded to a bare upper-case code first, because the veto has
    to fire on the currency and not on its spelling. Tabular ``currency`` is a
    pass-through extractor field copied out of the source cell verbatim, while
    OFX and Plaid carry ISO codes — so the cross-source pair this probe exists
    to judge is exactly where ``usd`` meets ``USD``. ``NULLIF(…, '')`` extends
    the same reading to a blank cell, which a statement produces whenever it
    fills its currency column only on foreign rows: an empty string is silence,
    and silence already means unstated here.

    Returns an unmeasurable overlap — rather than raising — when ``core`` is not
    yet materialized, which is a first import before any transform.
    """
    try:
        row = db.execute(
            f"""
            WITH against AS (
                SELECT transaction_date, amount, currency_code
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE account_id = ?
            ),
            span AS (
                SELECT MIN(transaction_date) AS lo, MAX(transaction_date) AS hi
                FROM against
            ),
            comparable AS (
                SELECT
                    probe.transaction_id,
                    probe.transaction_date,
                    probe.amount,
                    probe.currency_code
                FROM {FCT_TRANSACTIONS.full_name} AS probe, span
                WHERE probe.account_id = ?
                  AND span.lo IS NOT NULL
                  AND probe.transaction_date
                      BETWEEN span.lo - CAST(? AS INTEGER)
                          AND span.hi + CAST(? AS INTEGER)
            )
            SELECT
                (SELECT COUNT(*) FROM comparable),
                (SELECT COUNT(*) FROM (
                    SELECT c.transaction_id
                    FROM comparable AS c
                    JOIN against AS a
                      ON a.amount = c.amount
                     AND NULLIF(UPPER(TRIM(a.currency_code)), '')
                         IS NOT DISTINCT FROM
                         NULLIF(UPPER(TRIM(c.currency_code)), '')
                     AND ABS(DATE_DIFF('day', a.transaction_date, c.transaction_date))
                         <= CAST(? AS INTEGER)
                    GROUP BY c.transaction_id
                )),
                (SELECT MIN(transaction_date) FROM comparable),
                (SELECT MAX(transaction_date) FROM comparable)
            """,  # noqa: S608  # TableRef constants + parameterized values
            [
                against_account_id,
                account_id,
                window_days,
                window_days,
                window_days,
            ],
        ).fetchone()
    except duckdb.CatalogException:
        logger.debug("core.fct_transactions unavailable; ledger overlap unmeasurable")
        row = None
    comparable, matched, start, end = row or _EMPTY_WINDOW_ROW
    overlap = LedgerOverlap(
        comparable=int(comparable),
        matched=int(matched),
        window_start=start,
        window_end=end,
        window_days=window_days,
    )
    # Counted here rather than at the two call sites so a third one cannot forget:
    # the failure this instruments is every probe going unmeasurable at once, and
    # a counter that misses one caller cannot distinguish that from a quiet week.
    return _record_probe(overlap)
