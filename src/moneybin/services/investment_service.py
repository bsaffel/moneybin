# src/moneybin/services/investment_service.py
"""Investment read/write path: resolution, event recording, lot selection, and reads.

Business logic behind the ``investments`` CLI group and the ``investments_*``
MCP tools. Composes ``SecuritiesRepo`` and ``LotSelectionsRepo`` (Invariant 10)
and writes manual events to ``raw.manual_investment_transactions`` mirroring the
``TransactionService.create_manual_batch`` raw-write path. The read methods
(``list_events``, ``holdings``, ``lots``, ``gains``) are parameterized SELECTs
over the derived ``core.*`` investment models, mirroring
``AccountService.list_accounts``'s SELECT-fetchall-dataclass pattern.

Correctness contracts implemented here (see
``docs/specs/investments-data-model.md``):

- **Req 3 — resolution chain.** ``resolve_security`` resolves a single free-text
  reference CUSIP/ISIN → ticker (exchange-suffix stripped) → name. Identifier
  collisions raise :class:`SecurityResolutionError` naming the colliding
  attribute; never auto-merge (identifiers.md Guard 2). A name match to a
  candidate that carries a strong identifier (cusip/isin/ticker) is rejected —
  the single-string adaptation of Portfolio Performance's name-contradiction
  guard: a strongly-identified security must be referenced by its identifier,
  and since the reference reached the name rung it is known *not* to be that id.
- **Req 6 — sign conventions.** Validated before any write. ``quantity`` is
  positive for acquisitions, negative for disposals, NULL for cash-only events;
  ``amount`` is negative for cash out (buy/reinvest) and positive for cash in
  (sell/dividend/interest), total *including* fees. ``buy``/``sell`` with a NULL
  ``amount`` are rejected (the engine would degrade a null-amount sell to a
  full-basis loss — Task-8 review finding).
- **Reinvest pairing (Req 6).** A ``reinvest`` writes two rows in one
  transaction sharing a minted ``event_group_id``: the acquisition leg
  (positive quantity, negative amount) and a paired income row whose type
  derives from ``subtype`` (``dividend`` default / ``interest`` /
  ``capital_gain`` → ``capital_gain_distribution``).
- **Split (D6).** ``split`` carries the multiplier in ``quantity``
  (validated present and positive); price/amount/fees are NULL — the multiplier
  is a ratio, not a signed acquisition quantity.
- **transfer_in.** ``acquired`` → ``original_acquisition_date`` (holding period
  transfers with the shares); ``basis`` → the row's ``amount`` (negative =
  supplied basis).
- **Req 12 — method election.** ``upsert_security`` validates ``average`` to
  ``mutual_fund``/``etf``; ``fifo``/``hifo``/``specific`` are unrestricted.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    INVESTMENT_EVENTS_RECORDED_TOTAL,
    SECURITY_RESOLUTION_OUTCOMES_TOTAL,
)
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.account_service import COST_BASIS_METHODS
from moneybin.services.audit_service import AuditService
from moneybin.tables import (
    DIM_HOLDINGS,
    DIM_SECURITIES,
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_REALIZED_GAINS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    SECURITIES,
)

logger = logging.getLogger(__name__)

# ── Closed taxonomy (Req 5) ──────────────────────────────────────────────────

TAXONOMY: frozenset[str] = frozenset({
    "buy",
    "sell",
    "reinvest",
    "dividend",
    "interest",
    "capital_gain_distribution",
    "transfer_in",
    "transfer_out",
    "deposit",
    "withdrawal",
    "split",
    "fee",
    "return_of_capital",
    "other",
})

# Closed vocabulary (Req 12) — mirrors the app.securities.security_type CHECK
# constraint. Hard-validated in upsert_security(); the DB CHECK is the backstop,
# not the primary contract (same pattern as account_service.COST_BASIS_METHODS).
SECURITY_TYPES: frozenset[str] = frozenset({
    "equity",
    "etf",
    "mutual_fund",
    "bond",
    "crypto",
    "cash",
    "other",
})

# Per-type refinement vocabulary for USER-AUTHORABLE subtypes; a type absent from
# this map admits no subtype. Gates _validate_event on manual writes ONLY — the
# sync pipeline writes provider-derived rows straight into core (SQLMesh, not
# record_events) and is not bound by this map. See _PIPELINE_EMITTED_SUBTYPES: the
# pipeline emits a STRICT SUPERSET of this vocabulary, and the two must never be
# merged into one map — a subtype that claims "provider-derived reconstruction"
# (e.g. transfer_in/opening_bootstrap) must stay impossible for a user to hand-author.
_SUBTYPE_VOCAB: dict[str, frozenset[str]] = {
    "dividend": frozenset({"qualified", "non_qualified"}),
    "capital_gain_distribution": frozenset({"short_term", "long_term"}),
    "fee": frozenset({"tax_withheld"}),
    "reinvest": frozenset({"dividend", "interest", "capital_gain"}),
}

# Subtypes the sync pipeline emits directly into core.fct_investment_transactions,
# bypassing _validate_event entirely (e.g. prep.stg_plaid__opening_lots writes
# subtype='opening_bootstrap' under type='transfer_in' via SQLMesh). NOT
# user-authorable — see the _SUBTYPE_VOCAB comment above. The ledger-wide closed
# vocabulary is _SUBTYPE_VOCAB ∪ _PIPELINE_EMITTED_SUBTYPES; pinned by
# test_ledger_subtype_vocabulary_is_closed in test_investment_service.py.
_PIPELINE_EMITTED_SUBTYPES: dict[str, frozenset[str]] = {
    "transfer_in": frozenset({"opening_bootstrap"}),
}

# ── Sign rules (Req 6) ───────────────────────────────────────────────────────

_QTY_POSITIVE: frozenset[str] = frozenset({"buy", "reinvest", "transfer_in"})
_QTY_NEGATIVE: frozenset[str] = frozenset({"sell", "transfer_out"})
# Cash-only / basis-only events carry no share movement.
_QTY_NULL: frozenset[str] = frozenset({
    "dividend",
    "interest",
    "capital_gain_distribution",
    "deposit",
    "withdrawal",
    "fee",
    "return_of_capital",
    "other",
})
# 'split' is handled separately: quantity is the multiplier, not a signed qty.

# amount must be present (non-NULL) — a NULL degrades the cost-basis engine.
_AMOUNT_REQUIRED: frozenset[str] = frozenset({"buy", "sell", "reinvest"})
_AMOUNT_NEGATIVE: frozenset[str] = frozenset({"buy", "reinvest", "withdrawal", "fee"})
_AMOUNT_POSITIVE: frozenset[str] = frozenset({
    "sell",
    "deposit",
    "dividend",
    "interest",
    "capital_gain_distribution",
    "return_of_capital",
})

# security_id presence per type.
_SECURITY_REQUIRED: frozenset[str] = frozenset({
    "buy",
    "sell",
    "reinvest",
    "transfer_in",
    "transfer_out",
    "split",
    "return_of_capital",
})
_SECURITY_FORBIDDEN: frozenset[str] = frozenset({"deposit", "withdrawal"})

# reinvest income leg: subtype (funding source) → income row type.
_REINVEST_INCOME_TYPE: dict[str, str] = {
    "dividend": "dividend",
    "interest": "interest",
    "capital_gain": "capital_gain_distribution",
}

_VALID_CREATED_BY: frozenset[str] = frozenset({"cli", "mcp"})

# raw.manual_investment_transactions schema DEFAULTs feed the gold-key hash.
_SOURCE_TYPE = "manual"
_SOURCE_ORIGIN = "user"
_IMPORT_FORMAT_NAME = "manual_investment_entry"

_AUDIT_TARGET = (
    MANUAL_INVESTMENT_TRANSACTIONS.schema,
    MANUAL_INVESTMENT_TRANSACTIONS.name,
)

# record_event writes only raw.*; the disposal row and its tax lots materialize
# into core.* (which select_lots validates against) only on the next refresh.
# Surface that so a not-found error on a just-recorded id isn't a dead end.
_REFRESH_MATERIALIZE_HINT = (
    "💡 Newly recorded events materialize into the ledger only after a refresh — "
    "run 'moneybin refresh' (MCP: refresh_run), then retry."
)


class SecurityResolutionError(UserError):
    """A security reference is ambiguous or unresolvable.

    Names the colliding attribute on ambiguity (never auto-merges — Guard 2);
    points at ``investments securities add`` when nothing matches.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = error_codes.MUTATION_NOT_FOUND,
        hint: str | None = None,
    ) -> None:
        """Store a user-safe message + code (``MUTATION_AMBIGUOUS`` on collision)."""
        super().__init__(message, code=code, hint=hint)


def _predict_investment_gold_key(source_transaction_id: str, account_id: str) -> str:
    """Content-hash the canonical ``investment_transaction_id`` at INSERT time.

    Mirrors ``_predict_manual_gold_key`` in ``transaction_service`` — SHA256 over
    the immutable source identity, truncated to 16 hex. The manual investment
    staging model passes this id through unchanged (no matcher pipeline for
    investments in v1), so the service is its sole author. Uniqueness rides on
    the freshly-minted ``source_transaction_id``.
    """
    raw = f"{_SOURCE_TYPE}|{_SOURCE_ORIGIN}|{account_id}|{source_transaction_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ── Read-path result shapes ───────────────────────────────────────────────
# One row dataclass + one result wrapper per read method, mirroring
# AccountSummary/AccountListPayload (privacy/payloads/accounts.py). No
# Annotated[T, DataClass.X] privacy metadata here — that's applied when the
# MCP/CLI surface wraps these in a payload at the boundary (not yet wired).


@dataclass(frozen=True, slots=True)
class EventRow:
    """One row of the canonical investment-transaction ledger (Req 5/6 shape)."""

    investment_transaction_id: str
    account_id: str
    security_id: str | None
    trade_date: date
    settlement_date: date | None
    original_acquisition_date: date | None
    type: str
    subtype: str | None
    event_group_id: str | None
    quantity: Decimal | None
    price: Decimal | None
    amount: Decimal | None
    fees: Decimal | None
    currency_code: str
    description: str | None


@dataclass(frozen=True, slots=True)
class EventsResult:
    """Result of :meth:`InvestmentService.list_events`."""

    rows: list[EventRow]
    warnings: list[str]


@dataclass(frozen=True, slots=True)
class RecordEventsResult:
    """Result of :meth:`InvestmentService.record_events` — the batch write.

    ``investment_transaction_ids`` are the ids written (one per row; two for a
    reinvest). ``error_details`` carries one ``{"index", "reason"}`` entry per
    event soft-skipped for an unresolved/ambiguous security.
    """

    investment_transaction_ids: list[str]
    error_details: list[dict[str, str]]


@dataclass(frozen=True, slots=True)
class HoldingRow:
    """One current position — cost basis plus the Pillar-C valuation.

    ``market_value``/``unrealized_gain`` are NULL — never zero — whenever
    ``valuation_status`` is ``unpriced`` or ``withheld``; a zero is
    indistinguishable from a worthless position. ``unrealized_gain`` is
    signed (negative below cost); ``market_value`` is not.
    """

    account_id: str
    security_id: str
    quantity: Decimal
    cost_basis: Decimal
    average_cost: Decimal | None
    currency_code: str
    market_value: Decimal | None
    unrealized_gain: Decimal | None
    price_date: date | None
    price_source: str | None
    days_since_observed: int | None
    valuation_status: str


@dataclass(frozen=True, slots=True)
class HoldingsResult:
    """Result of :meth:`InvestmentService.holdings`."""

    rows: list[HoldingRow]
    warnings: list[str]


@dataclass(frozen=True, slots=True)
class LotRow:
    """One tax lot (open or closed) produced by the cost-basis engine."""

    lot_id: str
    account_id: str
    security_id: str
    acquisition_date: date
    acquisition_type: str
    original_quantity: Decimal
    remaining_quantity: Decimal
    cost_basis_total: Decimal
    cost_basis_remaining: Decimal
    cost_basis_method: str
    currency_code: str
    is_open: bool
    basis_incomplete: bool


@dataclass(frozen=True, slots=True)
class LotsResult:
    """Result of :meth:`InvestmentService.lots`."""

    rows: list[LotRow]
    warnings: list[str]


@dataclass(frozen=True, slots=True)
class RealizedGainRow:
    """One (disposal, consumed lot) realized gain/loss slice — the 1099-B grain."""

    realized_gain_id: str
    account_id: str
    security_id: str
    disposal_txn_id: str
    lot_id: str
    quantity: Decimal
    acquisition_date: date
    disposal_date: date
    proceeds: Decimal
    cost_basis: Decimal
    gain_loss: Decimal
    term: str
    cost_basis_method: str
    basis_incomplete: bool
    currency_code: str


@dataclass(frozen=True, slots=True)
class GainsResult:
    """Result of :meth:`InvestmentService.gains`."""

    rows: list[RealizedGainRow]
    warnings: list[str]


@dataclass(frozen=True, slots=True)
class SecurityRow:
    """One catalog entry from ``core.dim_securities`` (projects ``app.securities``)."""

    security_id: str
    name: str
    security_type: str
    ticker: str | None
    exchange: str | None
    cusip: str | None
    isin: str | None
    figi: str | None
    coingecko_id: str | None
    is_cash_equivalent: bool | None
    currency_code: str


@dataclass(frozen=True, slots=True)
class SecuritiesResult:
    """Result of :meth:`InvestmentService.list_securities`."""

    rows: list[SecurityRow]
    warnings: list[str]


@dataclass(frozen=True, slots=True)
class _ExistingSecurity:
    """Full ``app.securities`` row — the merge base for ``set_security``.

    Distinct from :class:`SecurityRow` (the ``core.dim_securities``
    read-projection shape, which omits ``cost_basis_method``) — private to
    this module, not a surface-crossing return type.
    """

    name: str
    security_type: str
    ticker: str | None
    exchange: str | None
    cusip: str | None
    isin: str | None
    figi: str | None
    coingecko_id: str | None
    is_cash_equivalent: bool | None
    cost_basis_method: str | None
    currency_code: str


class InvestmentService:
    """Investment read/write path — resolution, event recording, lot selection, and reads."""

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Initialize with an open Database; lazily build ``AuditService``.

        ``audit`` is shared with the composed repos so every mutation and its
        paired audit row land on one connection/transaction.
        """
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)
        self._securities_repo = SecuritiesRepo(db, audit=self._audit)
        self._lot_selections_repo = LotSelectionsRepo(db, audit=self._audit)

    # ------------------------------------------------------------------
    # Security resolution (Req 3)
    # ------------------------------------------------------------------

    def resolve_security(self, ref: str) -> str:
        """Resolve a free-text reference to a unique ``security_id``.

        Chain: CUSIP → ISIN (exact) → ticker (exchange suffix stripped,
        disambiguated by ``exchange``) → name (case-insensitive exact). A name
        match to a candidate carrying any strong identifier is rejected. Raises
        :class:`SecurityResolutionError` on collision (naming the attribute) or
        no match.
        """
        ref_clean = ref.strip()
        if not ref_clean:
            self._record_resolution("unresolved")
            raise SecurityResolutionError(
                "Empty security reference cannot be resolved.",
                hint="💡 Pass a ticker, CUSIP, ISIN, or catalog name.",
            )

        # Rung 1a — CUSIP exact.
        sid = self._resolve_by_identifier("cusip", ref_clean)
        if sid is not None:
            return self._record_resolution("cusip", sid)

        # Rung 1b — ISIN exact.
        sid = self._resolve_by_identifier("isin", ref_clean)
        if sid is not None:
            return self._record_resolution("isin", sid)

        # Rung 2 — ticker (exchange suffix stripped, disambiguated by exchange).
        sid = self._resolve_by_ticker(ref_clean)
        if sid is not None:
            return self._record_resolution("ticker", sid)

        # Rung 3 — name (case-insensitive exact), with the contradiction guard.
        sid = self._resolve_by_name(ref_clean)
        if sid is not None:
            return self._record_resolution("name", sid)

        self._record_resolution("unresolved")
        raise SecurityResolutionError(
            f"No security matches {ref!r}.",
            hint="💡 Run 'moneybin investments securities add' to add it.",
        )

    def _resolve_by_identifier(self, attribute: str, ref: str) -> str | None:
        """Return the sole ``security_id`` matching ``attribute`` (cusip|isin).

        Raises on collision naming ``attribute``; returns ``None`` on no match.
        """
        column = "cusip" if attribute == "cusip" else "isin"
        rows = self._db.execute(
            f"SELECT security_id FROM {SECURITIES.full_name} "  # noqa: S608  # column from a fixed 2-value allowlist
            f"WHERE UPPER({column}) = UPPER(?)",
            [ref],
        ).fetchall()
        if len(rows) > 1:
            self._record_resolution("ambiguous")
            raise SecurityResolutionError(
                f"{attribute} {ref!r} matches {len(rows)} securities "
                f"({', '.join(str(r[0]) for r in rows)}). "
                "Resolve the collision before recording events.",
                code=error_codes.MUTATION_AMBIGUOUS,
            )
        return str(rows[0][0]) if rows else None

    def _resolve_by_ticker(self, ref: str) -> str | None:
        """Resolve by ticker, honoring dotted tickers and exchange suffixes.

        Tries the FULL reference as a stored ticker first, so a ticker that
        legitimately contains a dot (``BRK.B``, ``BF.B``, ``RDS.A``) resolves by
        its own ticker. Only when the full ticker matches nothing does a
        ``.SUFFIX`` fall back to the exchange-disambiguation reading
        (``UMAX.AX`` → ticker ``UMAX`` on exchange ``AX``). Raises on collision
        naming ``ticker``.
        """
        # Rung 2a — exact full ticker (dots included).
        full = self._query_ticker(ref, None, ref)
        if full is not None:
            return full

        # Rung 2b — exchange-suffix fallback: only if the ref carries a suffix
        # and the full-ticker match found nothing.
        base, _, suffix = ref.partition(".")
        if not suffix:
            return None
        return self._query_ticker(base, suffix, ref)

    def _query_ticker(self, ticker: str, exchange: str | None, ref: str) -> str | None:
        """Return the sole ``security_id`` for a ticker (+optional exchange).

        Raises :class:`SecurityResolutionError` naming ``ticker`` on collision;
        returns ``None`` on no match.
        """
        params: list[object] = [ticker]
        exchange_filter = ""
        if exchange is not None:
            exchange_filter = "AND UPPER(exchange) = UPPER(?)"
            params.append(exchange)
        rows = self._db.execute(
            f"SELECT security_id FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef + static filter
            f"WHERE UPPER(ticker) = UPPER(?) {exchange_filter}",
            params,
        ).fetchall()
        if len(rows) > 1:
            self._record_resolution("ambiguous")
            raise SecurityResolutionError(
                f"ticker {ref!r} matches {len(rows)} securities "
                f"({', '.join(str(r[0]) for r in rows)}). "
                "Append a `.EXCHANGE` suffix (e.g. `UMAX.AX`) to disambiguate.",
                code=error_codes.MUTATION_AMBIGUOUS,
            )
        return str(rows[0][0]) if rows else None

    def _resolve_by_name(self, ref: str) -> str | None:
        """Resolve by case-insensitive exact name, applying the contradiction guard.

        Raises on an ambiguous name (naming ``name``) or when the single match
        carries a strong identifier (it must be referenced by that identifier).
        Returns ``None`` on no match.
        """
        rows = self._db.execute(
            f"SELECT security_id, cusip, isin, ticker FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef constant
            "WHERE LOWER(name) = LOWER(?)",
            [ref],
        ).fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            self._record_resolution("ambiguous")
            raise SecurityResolutionError(
                f"name {ref!r} matches {len(rows)} securities "
                f"({', '.join(str(r[0]) for r in rows)}). "
                "Reference one by its ticker, CUSIP, or ISIN.",
                code=error_codes.MUTATION_AMBIGUOUS,
            )
        security_id, cusip, isin, ticker = rows[0]
        strong = next(
            (
                (label, value)
                for label, value in (
                    ("cusip", cusip),
                    ("isin", isin),
                    ("ticker", ticker),
                )
                if value
            ),
            None,
        )
        if strong is not None:
            self._record_resolution("unresolved")
            raise SecurityResolutionError(
                f"{ref!r} matches a security that carries a {strong[0]} "
                f"({strong[1]!r}); reference it by that identifier instead of "
                "its name.",
            )
        return str(security_id)

    @staticmethod
    def _record_resolution(rung: str, security_id: str | None = None) -> str:
        """Increment the resolution-outcome metric; echo ``security_id`` back."""
        SECURITY_RESOLUTION_OUTCOMES_TOTAL.labels(rung=rung).inc()
        return security_id if security_id is not None else ""

    # ------------------------------------------------------------------
    # Security catalog upsert (Req 12)
    # ------------------------------------------------------------------

    def upsert_security(
        self,
        *,
        security_id: str | None,
        name: str,
        security_type: str,
        ticker: str | None = None,
        exchange: str | None = None,
        cusip: str | None = None,
        isin: str | None = None,
        figi: str | None = None,
        coingecko_id: str | None = None,
        is_cash_equivalent: bool | None = None,
        cost_basis_method: str | None = None,
        currency_code: str = "USD",
        actor: str,
    ) -> str:
        """Create-or-update one catalog entry; return its ``security_id``.

        ``security_type`` and ``cost_basis_method`` are hard-validated against
        their closed vocabularies (:data:`SECURITY_TYPES`,
        :data:`account_service.COST_BASIS_METHODS`) before the DB write — the
        column CHECK constraints are the backstop, not the primary contract
        (same pattern as ``AccountService.settings_update``). Also validates
        ``average`` cost basis to ``mutual_fund``/``etf`` (Req 12) — electing it
        on any other type raises a :class:`UserError`. Delegates the row +
        audit write to :class:`SecuritiesRepo`; the resulting id (minted when
        ``security_id`` is ``None``) is recovered from the returned
        ``AuditEvent.target_id`` (Decision D1).
        """
        if security_type not in SECURITY_TYPES:
            valid = ", ".join(sorted(SECURITY_TYPES))
            raise UserError(
                f"Invalid security type: {security_type!r}. Valid types: {valid}.",
                code=error_codes.MUTATION_INVALID_INPUT,
                hint=f"Valid types: {valid}.",
            )
        if (
            cost_basis_method is not None
            and cost_basis_method not in COST_BASIS_METHODS
        ):
            valid = ", ".join(sorted(COST_BASIS_METHODS))
            raise UserError(
                f"Invalid cost-basis method: {cost_basis_method!r}. "
                f"Valid methods: {valid}.",
                code=error_codes.MUTATION_INVALID_INPUT,
                hint=f"Valid methods: {valid}.",
            )
        if cost_basis_method == "average" and security_type not in {
            "mutual_fund",
            "etf",
        }:
            raise UserError(
                "Cost-basis method 'average' is valid only for mutual_fund or "
                f"etf securities, not {security_type!r}.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        event = self._securities_repo.upsert(
            security_id=security_id,
            name=name,
            security_type=security_type,
            ticker=ticker,
            exchange=exchange,
            cusip=cusip,
            isin=isin,
            figi=figi,
            coingecko_id=coingecko_id,
            is_cash_equivalent=is_cash_equivalent,
            cost_basis_method=cost_basis_method,
            currency_code=currency_code,
            actor=actor,
        )
        if event.target_id is None:  # defensive — upsert always sets target_id
            raise RuntimeError("securities.upsert returned no security_id")
        logger.info(
            f"securities.upsert security_id={event.target_id} "
            f"type={security_type} actor={actor}"
        )
        return event.target_id

    def set_security(
        self,
        security_id: str,
        *,
        name: str | None = None,
        ticker: str | None = None,
        exchange: str | None = None,
        cusip: str | None = None,
        isin: str | None = None,
        figi: str | None = None,
        coingecko_id: str | None = None,
        is_cash_equivalent: bool | None = None,
        cost_basis_method: str | None = None,
        currency_code: str | None = None,
        actor: str,
    ) -> str:
        """Partially update one existing catalog entry; return its ``security_id``.

        ``SecuritiesRepo.upsert`` always writes the full row (by design —
        partial-field editing is a service-layer concern per its docstring),
        so this method fetches the current row, merges the caller's non-``None``
        overrides onto it, and delegates the merged full row to
        :meth:`upsert_security` (which re-validates the ``average`` /
        mutual_fund-etf constraint on every call). ``security_type`` is not
        settable here — the CLI/MCP ``set`` surfaces carry no flag for it (no
        spec requirement to change it post-creation) — and is always carried
        through unchanged from the existing row. Raises :class:`UserError`
        (``MUTATION_NOT_FOUND``) when ``security_id`` doesn't exist.
        """
        existing = self._fetch_security(security_id)
        if existing is None:
            raise UserError(
                f"Security {security_id!r} not found.",
                code=error_codes.MUTATION_NOT_FOUND,
                hint="💡 Run 'moneybin investments securities list' to see known securities.",
            )
        return self.upsert_security(
            security_id=security_id,
            name=name if name is not None else existing.name,
            security_type=existing.security_type,
            ticker=ticker if ticker is not None else existing.ticker,
            exchange=exchange if exchange is not None else existing.exchange,
            cusip=cusip if cusip is not None else existing.cusip,
            isin=isin if isin is not None else existing.isin,
            figi=figi if figi is not None else existing.figi,
            coingecko_id=(
                coingecko_id if coingecko_id is not None else existing.coingecko_id
            ),
            is_cash_equivalent=(
                is_cash_equivalent
                if is_cash_equivalent is not None
                else existing.is_cash_equivalent
            ),
            cost_basis_method=(
                cost_basis_method
                if cost_basis_method is not None
                else existing.cost_basis_method
            ),
            currency_code=(
                currency_code if currency_code is not None else existing.currency_code
            ),
            actor=actor,
        )

    def _fetch_security(self, security_id: str) -> _ExistingSecurity | None:
        """Return the full ``app.securities`` row (incl. ``cost_basis_method``) or ``None``.

        Reads ``app.securities`` directly rather than ``core.dim_securities``
        (the read-projection view) because the view deliberately omits
        ``cost_basis_method`` (Req 12 is an app-layer-only field); a merge
        read sourced from the view would silently null it out on every
        :meth:`set_security` call that doesn't also pass ``--method``.
        """
        row = self._db.execute(
            f"""
            SELECT name, security_type, ticker, exchange, cusip, isin, figi,
                   coingecko_id, is_cash_equivalent, cost_basis_method, currency_code
              FROM {SECURITIES.full_name}
             WHERE security_id = ?
            """,  # noqa: S608  # TableRef constant
            [security_id],
        ).fetchone()
        if row is None:
            return None
        return _ExistingSecurity(
            name=str(row[0]),
            security_type=str(row[1]),
            ticker=row[2],
            exchange=row[3],
            cusip=row[4],
            isin=row[5],
            figi=row[6],
            coingecko_id=row[7],
            is_cash_equivalent=row[8],
            cost_basis_method=row[9],
            currency_code=str(row[10]),
        )

    # ------------------------------------------------------------------
    # Event recording (Req 5/6, reinvest pairing, split, transfer_in)
    # ------------------------------------------------------------------

    def record_event(
        self,
        *,
        account_ref: str,
        security_ref: str | None,
        type_: str,
        subtype: str | None,
        trade_date: date,
        quantity: Decimal | None,
        price: Decimal | None,
        amount: Decimal | None,
        fees: Decimal | None,
        acquired: date | None,
        basis: Decimal | None,
        event_group_id: str | None,
        currency_code: str,
        description: str | None,
        actor: str,
        created_by: str,
    ) -> list[str]:
        """Record one investment event; return the ``investment_transaction_id``(s).

        Validates taxonomy, subtype, sign conventions, and security presence
        *before* writing; resolves ``account_ref`` and ``security_ref`` at the
        boundary (Guard 2). A ``reinvest`` writes the acquisition + income row
        pair sharing a minted ``event_group_id`` and returns both ids. All rows
        for one event land in a single DuckDB transaction under one
        ``raw.import_log`` batch, mirroring the manual-cash-transaction path.
        """
        if created_by not in _VALID_CREATED_BY:
            raise UserError(
                f"created_by must be one of {sorted(_VALID_CREATED_BY)}, "
                f"got {created_by!r}.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        self._validate_event(
            type_=type_,
            subtype=subtype,
            quantity=quantity,
            amount=amount,
            security_ref=security_ref,
        )

        # Resolve free-text at the boundary; both may raise UserError-family.
        from moneybin.services.account_service import AccountService

        account_id = AccountService(self._db).resolve_strict(account_ref)
        security_id = (
            self.resolve_security(security_ref) if security_ref is not None else None
        )

        rows = self._build_rows(
            account_id=account_id,
            security_id=security_id,
            security_ref=security_ref,
            type_=type_,
            subtype=subtype,
            trade_date=trade_date,
            quantity=quantity,
            price=price,
            amount=amount,
            fees=fees,
            acquired=acquired,
            basis=basis,
            event_group_id=event_group_id,
            currency_code=currency_code,
            description=description,
            created_by=created_by,
        )
        return self._write_rows(
            account_id=account_id, type_=type_, rows=rows, actor=actor
        )

    def record_events(
        self, events: list[dict[str, Any]], *, actor: str, created_by: str
    ) -> RecordEventsResult:
        """Record a batch of events atomically; return written ids + soft errors.

        The batch analogue of :meth:`record_event` for the MCP ``investments_record``
        tool. Each dict carries the same fields ``record_event`` takes as kwargs
        (``account_ref``, ``security_ref``, ``type_``, ``trade_date``, …).

        Two passes with a hard atomicity boundary between them:

        - **Pass 1 (validate + resolve, no writes).** Every event is validated
          (taxonomy/sign/subtype/presence) and its account resolved — either is
          a HARD failure that raises and aborts the whole batch with nothing
          written. An unresolved/ambiguous *security* is SOFT: that event is
          skipped, recorded in ``error_details`` by its index, and the batch
          continues. Each event's ``account``/``security`` is resolved exactly
          once here (not again at write time).
        - **Pass 2 (write, one transaction).** Every surviving event's rows are
          inserted under ONE ``raw.import_log`` batch in ONE DuckDB transaction
          with ONE audit event. A failure part-way rolls the whole batch back,
          so the tool's "nothing written / safe to retry" contract holds even
          against an infra error mid-write.
        """
        if created_by not in _VALID_CREATED_BY:
            raise UserError(
                f"created_by must be one of {sorted(_VALID_CREATED_BY)}, "
                f"got {created_by!r}.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        from moneybin.services.account_service import AccountService

        account_svc = AccountService(self._db)
        groups: list[tuple[str, list[dict[str, object]]]] = []
        error_details: list[dict[str, str]] = []
        for index, ev in enumerate(events):
            type_ = ev["type_"]
            security_ref = ev["security_ref"]
            # HARD: taxonomy/sign/subtype/presence + account resolution.
            self._validate_event(
                type_=type_,
                subtype=ev["subtype"],
                quantity=ev["quantity"],
                amount=ev["amount"],
                security_ref=security_ref,
            )
            account_id = account_svc.resolve_strict(ev["account_ref"])
            # SOFT: an unresolved/ambiguous security skips just this event.
            try:
                security_id = (
                    self.resolve_security(security_ref)
                    if security_ref is not None
                    else None
                )
            except SecurityResolutionError as exc:
                error_details.append({"index": str(index), "reason": str(exc)})
                continue
            rows = self._build_rows(
                account_id=account_id,
                security_id=security_id,
                security_ref=security_ref,
                type_=type_,
                subtype=ev["subtype"],
                trade_date=ev["trade_date"],
                quantity=ev["quantity"],
                price=ev["price"],
                amount=ev["amount"],
                fees=ev["fees"],
                acquired=ev["acquired"],
                basis=ev["basis"],
                event_group_id=ev["event_group_id"],
                currency_code=ev["currency_code"],
                description=ev["description"],
                created_by=created_by,
            )
            groups.append((account_id, rows))

        written = self._write_batch(groups, actor=actor)
        return RecordEventsResult(
            investment_transaction_ids=written, error_details=error_details
        )

    def _validate_event(
        self,
        *,
        type_: str,
        subtype: str | None,
        quantity: Decimal | None,
        amount: Decimal | None,
        security_ref: str | None,
    ) -> None:
        """Raise :class:`UserError` on any taxonomy/subtype/sign/presence violation."""
        if type_ not in TAXONOMY:
            raise UserError(
                f"Unknown event type {type_!r}; must be one of {sorted(TAXONOMY)}.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if subtype is not None:
            allowed = _SUBTYPE_VOCAB.get(type_)
            if allowed is None or subtype not in allowed:
                allowed_desc = sorted(allowed) if allowed else "no subtype"
                raise UserError(
                    f"subtype {subtype!r} is not valid for type {type_!r} "
                    f"(allowed: {allowed_desc}).",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )

        # Security presence.
        if type_ in _SECURITY_FORBIDDEN and security_ref is not None:
            raise UserError(
                f"{type_} is an external cash event and must not name a security.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if type_ in _SECURITY_REQUIRED and security_ref is None:
            raise UserError(
                f"{type_} requires a security reference.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )

        self._validate_quantity(type_, quantity)
        self._validate_amount(type_, amount)

    @staticmethod
    def _validate_quantity(type_: str, quantity: Decimal | None) -> None:
        """Enforce the per-type ``quantity`` sign convention (Req 6, D6 for split)."""
        if type_ == "split":
            # D6: quantity is the split multiplier — present and positive.
            if quantity is None or quantity <= 0:
                raise UserError(
                    "A split's quantity is the multiplier and must be positive "
                    "(e.g. 2 for a 2:1 split).",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            return
        if type_ in _QTY_POSITIVE:
            if quantity is None or quantity <= 0:
                raise UserError(
                    f"{type_} requires a positive quantity.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
        elif type_ in _QTY_NEGATIVE:
            if quantity is None or quantity >= 0:
                raise UserError(
                    f"{type_} requires a negative quantity.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
        elif type_ in _QTY_NULL and quantity is not None:
            raise UserError(
                f"{type_} is a cash-only event and must not carry a quantity.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )

    @staticmethod
    def _validate_amount(type_: str, amount: Decimal | None) -> None:
        """Enforce the per-type ``amount`` sign convention (Req 6)."""
        if type_ == "split":
            return  # split carries no amount
        if type_ in _AMOUNT_REQUIRED and amount is None:
            raise UserError(
                f"{type_} requires an amount (a NULL amount degrades the "
                "cost-basis engine).",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if amount is None:
            return
        if type_ in _AMOUNT_NEGATIVE and amount >= 0:
            raise UserError(
                f"{type_} moves cash out; amount must be negative.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if type_ in _AMOUNT_POSITIVE and amount <= 0:
            raise UserError(
                f"{type_} brings cash in; amount must be positive.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )

    def _build_rows(
        self,
        *,
        account_id: str,
        security_id: str | None,
        security_ref: str | None,
        type_: str,
        subtype: str | None,
        trade_date: date,
        quantity: Decimal | None,
        price: Decimal | None,
        amount: Decimal | None,
        fees: Decimal | None,
        acquired: date | None,
        basis: Decimal | None,
        event_group_id: str | None,
        currency_code: str,
        description: str | None,
        created_by: str,
    ) -> list[dict[str, object]]:
        """Materialize the 1 (or 2 for reinvest) raw rows for one economic event."""

        def base_row(
            *,
            row_type: str,
            row_subtype: str | None,
            row_quantity: Decimal | None,
            row_price: Decimal | None,
            row_amount: Decimal | None,
            row_fees: Decimal | None,
            row_group_id: str | None,
            row_oad: date | None,
        ) -> dict[str, object]:
            return {
                "account_id": account_id,
                "security_id": security_id,
                "security_ref": security_ref,
                "type": row_type,
                "subtype": row_subtype,
                "event_group_id": row_group_id,
                "trade_date": trade_date,
                "settlement_date": None,
                "original_acquisition_date": row_oad,
                "quantity": row_quantity,
                "price": row_price,
                "amount": row_amount,
                "fees": row_fees,
                "currency_code": currency_code,
                "description": description,
                "created_by": created_by,
            }

        if type_ == "reinvest":
            # Share a minted event_group_id across the acquisition + income legs.
            group_id = event_group_id or uuid.uuid4().hex[:12]
            income_type = _REINVEST_INCOME_TYPE[subtype or "dividend"]
            acquisition = base_row(
                row_type="reinvest",
                row_subtype=subtype,
                row_quantity=quantity,
                row_price=price,
                row_amount=amount,  # negative — cash redeployed
                row_fees=fees,
                row_group_id=group_id,
                row_oad=None,
            )
            # Income leg: NULL quantity, positive amount, carries the security.
            # `amount` is fee-inclusive (Req 6); the fee is a transaction cost,
            # not part of the taxable dividend/interest/capital-gain income, so
            # it's added back before negating (amount is negative, fees is not).
            income_amount = (
                -(amount + (fees or Decimal("0"))) if amount is not None else None
            )
            income = base_row(
                row_type=income_type,
                row_subtype=None,
                row_quantity=None,
                row_price=None,
                row_amount=income_amount,
                row_fees=None,
                row_group_id=group_id,
                row_oad=None,
            )
            return [acquisition, income]

        if type_ == "split":
            # D6: multiplier in quantity; price/amount/fees NULL.
            return [
                base_row(
                    row_type="split",
                    row_subtype=subtype,
                    row_quantity=quantity,
                    row_price=None,
                    row_amount=None,
                    row_fees=None,
                    row_group_id=event_group_id,
                    row_oad=None,
                )
            ]

        if type_ == "transfer_in":
            # acquired → original_acquisition_date; basis → negative amount.
            row_amount = -abs(basis) if basis is not None else amount
            return [
                base_row(
                    row_type="transfer_in",
                    row_subtype=subtype,
                    row_quantity=quantity,
                    row_price=price,
                    row_amount=row_amount,
                    row_fees=fees,
                    row_group_id=event_group_id,
                    row_oad=acquired,
                )
            ]

        return [
            base_row(
                row_type=type_,
                row_subtype=subtype,
                row_quantity=quantity,
                row_price=price,
                row_amount=amount,
                row_fees=fees,
                row_group_id=event_group_id,
                row_oad=acquired,
            )
        ]

    def _insert_event_row(
        self, *, import_id: str, account_id: str, row: dict[str, object]
    ) -> str:
        """Insert one raw investment row; return its predicted gold-key id.

        No transaction management — the caller owns the enclosing transaction so
        multi-row / multi-event writes stay atomic.
        """
        source_transaction_id = "manual_" + uuid.uuid4().hex[:12]
        investment_transaction_id = _predict_investment_gold_key(
            source_transaction_id, account_id
        )
        self._db.conn.execute(
            f"""
            INSERT INTO {MANUAL_INVESTMENT_TRANSACTIONS.full_name} (
                source_transaction_id, import_id, account_id, security_id,
                security_ref, type, subtype, event_group_id, trade_date,
                settlement_date, original_acquisition_date, quantity,
                price, amount, fees, currency_code, description,
                created_by, investment_transaction_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,  # noqa: S608  # TableRef + parameterized values
            [
                source_transaction_id,
                import_id,
                row["account_id"],
                row["security_id"],
                row["security_ref"],
                row["type"],
                row["subtype"],
                row["event_group_id"],
                row["trade_date"],
                row["settlement_date"],
                row["original_acquisition_date"],
                row["quantity"],
                row["price"],
                row["amount"],
                row["fees"],
                row["currency_code"],
                row["description"],
                row["created_by"],
                investment_transaction_id,
            ],
        )
        return investment_transaction_id

    def _write_rows(
        self,
        *,
        account_id: str,
        type_: str,
        rows: list[dict[str, object]],
        actor: str,
    ) -> list[str]:
        """Insert one event's rows + one audit event under a single import batch.

        Mirrors ``TransactionService.create_manual_batch``: allocate one
        ``raw.import_log`` row, insert every row in one transaction, emit one
        ``investment.record`` audit event, and mark the batch failed on rollback
        so a crashed write leaves no orphaned ``importing`` batch. The
        multi-event analogue is :meth:`_write_batch`.
        """
        from moneybin.services.import_service import ImportService

        import_id = ImportService(self._db).allocate_import_log(
            source_type=_SOURCE_TYPE,
            format_name=_IMPORT_FORMAT_NAME,
            actor=actor,
        )

        written: list[str] = []
        self._db.begin()
        try:
            for row in rows:
                written.append(
                    self._insert_event_row(
                        import_id=import_id, account_id=account_id, row=row
                    )
                )
            self._audit.record_audit_event(
                action="investment.record",
                target=(*_AUDIT_TARGET, import_id),
                before=None,
                after={"row_count": len(written), "type": type_},
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            from moneybin.loaders import import_log

            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=0,
                rows_imported=0,
            )
            raise

        for row in rows:
            INVESTMENT_EVENTS_RECORDED_TOTAL.labels(type=str(row["type"])).inc()

        logger.info(
            f"investment.record import_id={import_id} type={type_} "
            f"row_count={len(written)} actor={actor}"
        )
        return written

    def _write_batch(
        self,
        groups: list[tuple[str, list[dict[str, object]]]],
        *,
        actor: str,
    ) -> list[str]:
        """Insert many events' rows atomically under one import batch.

        The multi-event analogue of :meth:`_write_rows`: every group's rows
        (``(account_id, rows)``) are inserted under ONE ``raw.import_log`` batch
        in ONE transaction with ONE ``investment.record`` audit event. A failure
        part-way rolls the whole batch back and marks the import failed, so a
        retry can't double-insert events that would otherwise have committed
        before the failure.
        """
        if not groups:
            return []
        from moneybin.services.import_service import ImportService

        import_id = ImportService(self._db).allocate_import_log(
            source_type=_SOURCE_TYPE,
            format_name=_IMPORT_FORMAT_NAME,
            actor=actor,
        )

        written: list[str] = []
        self._db.begin()
        try:
            for account_id, rows in groups:
                for row in rows:
                    written.append(
                        self._insert_event_row(
                            import_id=import_id, account_id=account_id, row=row
                        )
                    )
            self._audit.record_audit_event(
                action="investment.record",
                target=(*_AUDIT_TARGET, import_id),
                before=None,
                after={"row_count": len(written), "event_count": len(groups)},
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            from moneybin.loaders import import_log

            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=0,
                rows_imported=0,
            )
            raise

        for _account_id, rows in groups:
            for row in rows:
                INVESTMENT_EVENTS_RECORDED_TOTAL.labels(type=str(row["type"])).inc()

        logger.info(
            f"investment.record import_id={import_id} event_count={len(groups)} "
            f"row_count={len(written)} actor={actor}"
        )
        return written

    # ------------------------------------------------------------------
    # Lot selection (Req 13)
    # ------------------------------------------------------------------

    def select_lots(
        self,
        disposal_txn_id: str,
        selections: list[tuple[str, Decimal]],
        *,
        actor: str,
    ) -> None:
        """Set the full specific-identification selection for a disposal.

        Validates that the disposal exists and is a ``sell``, that each selected
        lot exists, and that the selected quantities sum to no more than the
        disposal's magnitude — raising a :class:`UserError` naming the problem —
        then delegates to :meth:`LotSelectionsRepo.set_for_disposal` (the
        declarative set; ``selections=[]`` clears all overrides → FIFO).
        """
        row = self._db.execute(
            f"SELECT account_id, security_id, type, quantity, trade_date "  # noqa: S608  # TableRef constant
            f"FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} "
            "WHERE investment_transaction_id = ?",
            [disposal_txn_id],
        ).fetchone()
        if row is None:
            raise UserError(
                f"Disposal {disposal_txn_id!r} not found in the investment ledger.",
                code=error_codes.MUTATION_NOT_FOUND,
                hint=_REFRESH_MATERIALIZE_HINT,
            )
        (
            disposal_account_id,
            disposal_security_id,
            disposal_type,
            disposal_quantity,
            disposal_date,
        ) = row
        if disposal_type != "sell":
            raise UserError(
                f"{disposal_txn_id!r} is a {disposal_type!r}, not a disposal; "
                "lot selection applies only to sells.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )

        if selections:
            self._validate_selection_lots(
                selections,
                disposal_account_id,
                disposal_security_id,
                disposal_txn_id,
                disposal_date,
            )
            total = sum((qty for _, qty in selections), Decimal("0"))
            available = abs(Decimal(str(disposal_quantity)))
            if total > available:
                raise UserError(
                    f"Selected quantity {total} exceeds the disposal's "
                    f"{available} units.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )

        self._lot_selections_repo.set_for_disposal(
            investment_transaction_id=disposal_txn_id,
            selections=selections,
            actor=actor,
        )
        logger.info(
            f"lot_selections.set disposal={disposal_txn_id} "
            f"count={len(selections)} actor={actor}"
        )

    def _validate_selection_lots(
        self,
        selections: list[tuple[str, Decimal]],
        account_id: str,
        security_id: str | None,
        disposal_txn_id: str,
        disposal_date: date,
    ) -> None:
        """Raise unless every selected lot has enough quantity open at replay time.

        A lot is valid only if it exists in ``core.fct_investment_lots``,
        belongs to the disposal's ``(account_id, security_id)``, was acquired
        on or before ``disposal_date``, AND has enough quantity available as of
        this disposal's position in the replay. All four are load-bearing: the
        engine's chronological replay (``compute_lots_and_gains``) mutates each
        lot's ``remaining_quantity`` in place as it consumes disposals in
        trade-date order, so a lot from another position, one acquired after
        this disposal, or one already exhausted (fully or partially) by an
        earlier disposal isn't available for the full requested quantity in
        ``_consumption_plan``'s ``by_lot_id`` at replay time — the selection
        (or its unavailable remainder) is silently dropped to a FIFO fallback,
        producing a wrong 1099-B with no error. Same-day acquisitions ARE
        valid: ``_SAME_DAY_TYPE_ORDER`` processes acquisitions before disposals
        on a shared trade date.

        Availability is derived from ``l.remaining_quantity`` — the lot's
        *current* leftover after every disposal recorded against it, sell or
        ``transfer_out`` alike — rather than from ``original_quantity`` minus a
        sum over ``core.fct_realized_gains``. ``transfer_out`` consumes lots
        but realizes no gain (``cost_basis.py``'s ``_consume``), so it never
        writes a ``fct_realized_gains`` row; a check built only from that table
        is blind to an earlier ``transfer_out``'s draw-down. Starting from
        ``remaining_quantity`` bakes in every disposal type's consumption, then
        adds back whatever this disposal itself (or any disposal NOT strictly
        earlier — a later date, or a tie broken by ``investment_transaction_id``)
        drew via ``fct_realized_gains``, since only sells are addressable that
        way. A later ``transfer_out``'s consumption can't be added back the
        same way and stays folded into ``remaining_quantity``'s reduction —
        conservative by construction (can only under-state availability, never
        over-state it), so the check fails closed toward a clean rejection
        rather than a silent bad selection.
        """
        for lot_id, qty in selections:
            if qty <= 0:
                raise UserError(
                    f"Lot selection quantity for {lot_id!r} must be positive.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
        requested = dict(selections)
        lot_ids = list(requested)
        placeholders = ", ".join("?" * len(lot_ids))
        rows = self._db.execute(
            f"SELECT l.lot_id, l.remaining_quantity, "  # noqa: S608  # TableRef constant
            "COALESCE(SUM(g.quantity), 0) "
            f"FROM {FCT_INVESTMENT_LOTS.full_name} l "
            f"LEFT JOIN {FCT_REALIZED_GAINS.full_name} g "
            "ON g.lot_id = l.lot_id "
            "AND (g.disposal_date > ? "
            "OR (g.disposal_date = ? AND g.disposal_txn_id >= ?)) "
            f"WHERE l.lot_id IN ({placeholders}) "
            "AND l.account_id = ? AND l.security_id = ? "
            "AND l.acquisition_date <= ? "
            "GROUP BY l.lot_id, l.remaining_quantity",
            [
                disposal_date,
                disposal_date,
                disposal_txn_id,
                *lot_ids,
                account_id,
                security_id,
                disposal_date,
            ],
        ).fetchall()
        available = {str(r[0]): Decimal(str(r[1])) + Decimal(str(r[2])) for r in rows}
        problems = [
            lot_id
            for lot_id in lot_ids
            if lot_id not in available or requested[lot_id] > available[lot_id]
        ]
        if problems:
            raise UserError(
                "Lot(s) not part of this disposal's position, or already "
                "exhausted by an earlier disposal, as of its trade date "
                f"(account/security/acquisition date/available quantity): "
                f"{', '.join(repr(m) for m in problems)}.",
                code=error_codes.MUTATION_NOT_FOUND,
                hint=_REFRESH_MATERIALIZE_HINT,
            )

    # ------------------------------------------------------------------
    # Read path — list_events, holdings, lots, gains
    # ------------------------------------------------------------------

    # Statuses that publish no market value. Mirrors dim_holdings.sql's
    # valuation_status vocabulary; 'valued'/'carried_forward' both carry a number.
    _UNVALUED_STATUSES: frozenset[str] = frozenset({"unpriced", "withheld"})
    _VALID_TERMS: frozenset[str] = frozenset({"short", "long"})

    def _resolve_filters(
        self, account_ref: str | None, security_ref: str | None
    ) -> tuple[str | None, str | None]:
        """Resolve free-text account/security refs to ids at the boundary.

        Guard 2 (identifiers.md): the caller's WHERE binds to the returned ids,
        never the free text. Both resolvers raise UserError-family errors
        (``AccountNotFoundError``/``AmbiguousAccountError``/
        ``SecurityResolutionError``) on no-match or ambiguity, which propagate.
        """
        from moneybin.services.account_service import AccountService

        account_id = (
            AccountService(self._db).resolve_strict(account_ref)
            if account_ref is not None
            else None
        )
        security_id = (
            self.resolve_security(security_ref) if security_ref is not None else None
        )
        return account_id, security_id

    def list_events(
        self,
        *,
        account_ref: str | None = None,
        security_ref: str | None = None,
        type_filter: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> EventsResult:
        """List ledger events from ``core.fct_investment_transactions``.

        All filters are optional and AND-combined; ``account_ref``/
        ``security_ref`` accept free text and resolve to ids before binding
        (Guard 2). Carries no mandatory warning.
        """
        if type_filter is not None and type_filter not in TAXONOMY:
            raise ValueError(
                f"type_filter must be one of {sorted(TAXONOMY)}, got {type_filter!r}"
            )
        account_id, security_id = self._resolve_filters(account_ref, security_ref)

        where: list[str] = []
        params: list[object] = []
        if account_id is not None:
            where.append("account_id = ?")
            params.append(account_id)
        if security_id is not None:
            where.append("security_id = ?")
            params.append(security_id)
        if type_filter is not None:
            where.append("type = ?")
            params.append(type_filter)
        if date_from is not None:
            where.append("trade_date >= ?")
            params.append(date_from)
        if date_to is not None:
            where.append("trade_date <= ?")
            params.append(date_to)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        rows = self._db.execute(
            f"""
            SELECT investment_transaction_id, account_id, security_id, trade_date,
                   settlement_date, original_acquisition_date, type, subtype,
                   event_group_id, quantity, price, amount, fees, currency_code,
                   description
              FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
              {where_sql}
             ORDER BY trade_date, investment_transaction_id
            """,  # noqa: S608  # TableRef + parameterized values; where_sql built from literal fragments above
            params,
        ).fetchall()
        return EventsResult(
            rows=[
                EventRow(
                    investment_transaction_id=str(r[0]),
                    account_id=str(r[1]),
                    security_id=r[2],
                    trade_date=r[3],
                    settlement_date=r[4],
                    original_acquisition_date=r[5],
                    type=str(r[6]),
                    subtype=r[7],
                    event_group_id=r[8],
                    quantity=r[9],
                    price=r[10],
                    amount=r[11],
                    fees=r[12],
                    currency_code=str(r[13]),
                    description=r[14],
                )
                for r in rows
            ],
            warnings=[],
        )

    def holdings(
        self,
        *,
        account_ref: str | None = None,
        security_ref: str | None = None,
    ) -> HoldingsResult:
        """Current positions from ``core.dim_holdings``, valued where possible.

        Each row carries cost basis and — when a close resolved — market value,
        unrealized gain, the date of the close used, and how many days old it
        is. A position whose value is ``unpriced`` or ``withheld`` reports NULL
        rather than zero; the count of those rows is named in a warning, the
        same shape ``lots()`` uses for ``basis_incomplete``.
        """
        account_id, security_id = self._resolve_filters(account_ref, security_ref)

        where: list[str] = []
        params: list[object] = []
        if account_id is not None:
            where.append("account_id = ?")
            params.append(account_id)
        if security_id is not None:
            where.append("security_id = ?")
            params.append(security_id)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        rows = self._db.execute(
            f"""
            SELECT account_id, security_id, quantity, cost_basis, average_cost,
                   currency_code, market_value, unrealized_gain, price_date,
                   price_source, days_since_observed, valuation_status
              FROM {DIM_HOLDINGS.full_name}
              {where_sql}
             ORDER BY account_id, security_id
            """,  # noqa: S608  # TableRef + parameterized values; where_sql built from literal fragments above
            params,
        ).fetchall()
        holding_rows = [
            HoldingRow(
                account_id=str(r[0]),
                security_id=str(r[1]),
                quantity=r[2],
                cost_basis=r[3],
                average_cost=r[4],
                currency_code=str(r[5]),
                market_value=r[6],
                unrealized_gain=r[7],
                price_date=r[8],
                price_source=None if r[9] is None else str(r[9]),
                days_since_observed=None if r[10] is None else int(r[10]),
                valuation_status=str(r[11]),
            )
            for r in rows
        ]
        warnings: list[str] = []
        unvalued = sum(
            1 for row in holding_rows if row.valuation_status in self._UNVALUED_STATUSES
        )
        if unvalued:
            warnings.append(
                f"{unvalued} position(s) report no market value — see each row's "
                "valuation_status: 'unpriced' (no close resolved) or 'withheld' "
                "(the share count is known wrong)."
            )
        return HoldingsResult(rows=holding_rows, warnings=warnings)

    def lots(
        self,
        *,
        account_ref: str | None = None,
        security_ref: str | None = None,
        open_only: bool = True,
    ) -> LotsResult:
        """Tax lots from ``core.fct_investment_lots``; open lots only by default.

        Pass ``open_only=False`` for the full open+closed history. Carries a
        warning naming the count of ``basis_incomplete`` rows (e.g. a
        ``transfer_in`` recorded with no supplied basis) when any are present.
        """
        account_id, security_id = self._resolve_filters(account_ref, security_ref)

        where: list[str] = []
        params: list[object] = []
        if account_id is not None:
            where.append("account_id = ?")
            params.append(account_id)
        if security_id is not None:
            where.append("security_id = ?")
            params.append(security_id)
        if open_only:
            where.append("is_open = TRUE")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        rows = self._db.execute(
            f"""
            SELECT lot_id, account_id, security_id, acquisition_date,
                   acquisition_type, original_quantity, remaining_quantity,
                   cost_basis_total, cost_basis_remaining, cost_basis_method,
                   currency_code, is_open, basis_incomplete
              FROM {FCT_INVESTMENT_LOTS.full_name}
              {where_sql}
             ORDER BY acquisition_date, lot_id
            """,  # noqa: S608  # TableRef + parameterized values; where_sql built from literal fragments above
            params,
        ).fetchall()
        lot_rows = [
            LotRow(
                lot_id=str(r[0]),
                account_id=str(r[1]),
                security_id=str(r[2]),
                acquisition_date=r[3],
                acquisition_type=str(r[4]),
                original_quantity=r[5],
                remaining_quantity=r[6],
                cost_basis_total=r[7],
                cost_basis_remaining=r[8],
                cost_basis_method=str(r[9]),
                currency_code=str(r[10]),
                is_open=bool(r[11]),
                basis_incomplete=bool(r[12]),
            )
            for r in rows
        ]
        warnings: list[str] = []
        incomplete_count = sum(1 for row in lot_rows if row.basis_incomplete)
        if incomplete_count:
            warnings.append(
                f"{incomplete_count} lot(s) have incomplete cost basis "
                "(e.g. a transfer_in with no supplied basis) — figures are "
                "conservative."
            )
        return LotsResult(rows=lot_rows, warnings=warnings)

    def gains(
        self,
        *,
        account_ref: str | None = None,
        security_ref: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        term: str | None = None,
    ) -> GainsResult:
        """Realized gain/loss (the 1099-B surface) from ``core.fct_realized_gains``.

        Carries a warning naming the count of ``basis_incomplete`` rows
        (oversold / missing acquisition) when any are present in the result.
        """
        if term is not None and term not in self._VALID_TERMS:
            raise ValueError(
                f"term must be one of {sorted(self._VALID_TERMS)}, got {term!r}"
            )
        account_id, security_id = self._resolve_filters(account_ref, security_ref)

        where: list[str] = []
        params: list[object] = []
        if account_id is not None:
            where.append("account_id = ?")
            params.append(account_id)
        if security_id is not None:
            where.append("security_id = ?")
            params.append(security_id)
        if date_from is not None:
            where.append("disposal_date >= ?")
            params.append(date_from)
        if date_to is not None:
            where.append("disposal_date <= ?")
            params.append(date_to)
        if term is not None:
            where.append("term = ?")
            params.append(term)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        rows = self._db.execute(
            f"""
            SELECT realized_gain_id, account_id, security_id, disposal_txn_id,
                   lot_id, quantity, acquisition_date, disposal_date, proceeds,
                   cost_basis, gain_loss, term, cost_basis_method,
                   basis_incomplete, currency_code
              FROM {FCT_REALIZED_GAINS.full_name}
              {where_sql}
             ORDER BY disposal_date, realized_gain_id
            """,  # noqa: S608  # TableRef + parameterized values; where_sql built from literal fragments above
            params,
        ).fetchall()
        gain_rows = [
            RealizedGainRow(
                realized_gain_id=str(r[0]),
                account_id=str(r[1]),
                security_id=str(r[2]),
                disposal_txn_id=str(r[3]),
                lot_id=str(r[4]),
                quantity=r[5],
                acquisition_date=r[6],
                disposal_date=r[7],
                proceeds=r[8],
                cost_basis=r[9],
                gain_loss=r[10],
                term=str(r[11]),
                cost_basis_method=str(r[12]),
                basis_incomplete=bool(r[13]),
                currency_code=str(r[14]),
            )
            for r in rows
        ]
        warnings: list[str] = []
        incomplete_count = sum(1 for row in gain_rows if row.basis_incomplete)
        if incomplete_count:
            warnings.append(
                f"{incomplete_count} realized-gain row(s) have incomplete cost "
                "basis (oversold / missing acquisition) — figures are "
                "conservative."
            )
        return GainsResult(rows=gain_rows, warnings=warnings)

    def list_securities(self, *, security_type: str | None = None) -> SecuritiesResult:
        """List the securities catalog from ``core.dim_securities``.

        Optional ``security_type`` filter; no mandatory warning.
        """
        if security_type is not None and security_type not in SECURITY_TYPES:
            raise ValueError(
                f"security_type must be one of {sorted(SECURITY_TYPES)}, "
                f"got {security_type!r}"
            )
        where_sql = ""
        params: list[object] = []
        if security_type is not None:
            where_sql = "WHERE security_type = ?"
            params.append(security_type)

        rows = self._db.execute(
            f"""
            SELECT security_id, name, security_type, ticker, exchange, cusip,
                   isin, figi, coingecko_id, is_cash_equivalent, currency_code
              FROM {DIM_SECURITIES.full_name}
              {where_sql}
             ORDER BY name, security_id
            """,  # noqa: S608  # TableRef + parameterized values; where_sql built from literal fragment above
            params,
        ).fetchall()
        return SecuritiesResult(
            rows=[
                SecurityRow(
                    security_id=str(r[0]),
                    name=str(r[1]),
                    security_type=str(r[2]),
                    ticker=r[3],
                    exchange=r[4],
                    cusip=r[5],
                    isin=r[6],
                    figi=r[7],
                    coingecko_id=r[8],
                    is_cash_equivalent=r[9],
                    currency_code=str(r[10]),
                )
                for r in rows
            ],
            warnings=[],
        )
