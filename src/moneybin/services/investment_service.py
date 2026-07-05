# src/moneybin/services/investment_service.py
"""Investment write path: security resolution, event recording, lot selection.

Business logic behind the ``investments`` CLI group and the ``investments_*``
MCP tools. Composes ``SecuritiesRepo`` and ``LotSelectionsRepo`` (Invariant 10)
and writes manual events to ``raw.manual_investment_transactions`` mirroring the
``TransactionService.create_manual_batch`` raw-write path.

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
from datetime import date
from decimal import Decimal

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    INVESTMENT_EVENTS_RECORDED_TOTAL,
    SECURITY_RESOLUTION_OUTCOMES_TOTAL,
)
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.audit_service import AuditService
from moneybin.tables import (
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
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

# Per-type refinement vocabulary; a type absent from this map admits no subtype.
_SUBTYPE_VOCAB: dict[str, frozenset[str]] = {
    "dividend": frozenset({"qualified", "non_qualified"}),
    "capital_gain_distribution": frozenset({"short_term", "long_term"}),
    "fee": frozenset({"tax_withheld"}),
    "reinvest": frozenset({"dividend", "interest", "capital_gain"}),
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


class InvestmentService:
    """Investment write path — resolution, event recording, lot selection."""

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
        """Resolve by ticker, stripping an exchange suffix (``UMAX.AX`` → UMAX).

        When the reference carries a ``.SUFFIX`` the suffix disambiguates by
        ``exchange``. Raises on collision naming ``ticker``.
        """
        base, _, suffix = ref.partition(".")
        params: list[object] = [base]
        exchange_filter = ""
        if suffix:
            exchange_filter = "AND UPPER(exchange) = UPPER(?)"
            params.append(suffix)
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

        Validates ``average`` cost basis to ``mutual_fund``/``etf`` (Req 12) —
        electing it on any other type raises a :class:`UserError`. Delegates the
        row + audit write to :class:`SecuritiesRepo`; the resulting id (minted
        when ``security_id`` is ``None``) is recovered from the returned
        ``AuditEvent.target_id`` (Decision D1). The ``security_type`` /
        ``cost_basis_method`` CHECK constraints are enforced by the DDL.
        """
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
            income_amount = -amount if amount is not None else None
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

    def _write_rows(
        self,
        *,
        account_id: str,
        type_: str,
        rows: list[dict[str, object]],
        actor: str,
    ) -> list[str]:
        """Insert the prepared rows + one audit event under a single import batch.

        Mirrors ``TransactionService.create_manual_batch``: allocate one
        ``raw.import_log`` row, insert every row in one transaction, emit one
        ``investment.record`` audit event, and mark the batch failed on rollback
        so a crashed write leaves no orphaned ``importing`` batch.
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
                written.append(investment_transaction_id)

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
            f"SELECT type, quantity FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant
            "WHERE investment_transaction_id = ?",
            [disposal_txn_id],
        ).fetchone()
        if row is None:
            raise UserError(
                f"Disposal {disposal_txn_id!r} not found in the investment ledger.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        disposal_type, disposal_quantity = row
        if disposal_type != "sell":
            raise UserError(
                f"{disposal_txn_id!r} is a {disposal_type!r}, not a disposal; "
                "lot selection applies only to sells.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )

        if selections:
            self._validate_selection_lots(selections)
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

    def _validate_selection_lots(self, selections: list[tuple[str, Decimal]]) -> None:
        """Raise if any selected lot is unknown or carries a non-positive quantity."""
        for lot_id, qty in selections:
            if qty <= 0:
                raise UserError(
                    f"Lot selection quantity for {lot_id!r} must be positive.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
        lot_ids = [lot_id for lot_id, _ in selections]
        placeholders = ", ".join("?" * len(lot_ids))
        found = {
            str(r[0])
            for r in self._db.execute(
                f"SELECT lot_id FROM {FCT_INVESTMENT_LOTS.full_name} "  # noqa: S608  # TableRef constant
                f"WHERE lot_id IN ({placeholders})",
                lot_ids,
            ).fetchall()
        }
        missing = [lot_id for lot_id in lot_ids if lot_id not in found]
        if missing:
            raise UserError(
                f"Unknown lot(s): {', '.join(repr(m) for m in missing)}.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
