"""Atomic preflight and dispatch for consolidated review decisions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast

import duckdb
from pydantic import JsonValue

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.persistence import get_match_decision
from moneybin.mcp.write_contracts import (
    AccountLinkDecisionRequest,
    CategorizationDecisionRequest,
    IdentityDecisionRequest,
    MatchDecisionRequest,
    MerchantLinkDecisionRequest,
    OrdinaryReviewDecisionRequest,
    SecurityLinkDecisionRequest,
)
from moneybin.repositories.categorization_decisions_repo import (
    CategorizationDecisionsRepo,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services._text import build_match_inputs
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.security_links_service import SecurityLinksService
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    CATEGORIES,
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
    LOT_SELECTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    MERCHANT_LINK_DECISIONS,
    MERCHANT_LINKS,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
    TRANSACTION_CATEGORIES,
    USER_MERCHANTS,
)

_IdentityRequest = (
    AccountLinkDecisionRequest
    | MerchantLinkDecisionRequest
    | SecurityLinkDecisionRequest
)


@dataclass(frozen=True, slots=True)
class OrdinaryDecisionPlanItem:
    """One fully resolved ordinary review decision."""

    request: CategorizationDecisionRequest | MatchDecisionRequest
    changed: bool
    status: str
    transaction_id: str | None = None
    category_id: str | None = None
    category_changed: bool = False
    merchant_changed: bool = False
    merchant_id: str | None = None
    match_text: str | None = None
    merchant_group_key: tuple[str, str, str | None] | None = None


@dataclass(frozen=True, slots=True)
class IdentityDecisionPlanItem:
    """One resolved identity target with exact persisted before-state."""

    request: _IdentityRequest
    changed: bool
    status: Literal["accepted", "rejected"]
    source_id: str
    target_id: str
    group_key: tuple[str, ...]
    before_state: JsonValue
    affected_ids: dict[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class IdentityDecisionPlan:
    """Complete ordered identity batch plan."""

    items: tuple[IdentityDecisionPlanItem, ...]

    @property
    def changed_count(self) -> int:
        """Return the number of material decision transitions."""
        return sum(item.changed for item in self.items)

    @property
    def destructive(self) -> bool:
        """Return whether the material batch contains an identity merge accept."""
        return self.changed_count > 0 and any(
            item.request.decision == "accept" for item in self.items
        )


def _json_safe(value: Any) -> JsonValue:
    """Normalize database values into deterministic confirmation JSON."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        return {str(key): _json_safe(item) for key, item in mapping.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _query_json_rows(
    db: Database,
    sql: str,
    params: list[Any],
) -> list[dict[str, JsonValue]]:
    """Return complete selected rows as stable JSON-shaped dictionaries."""
    cursor = db.execute(sql, params)
    columns = [str(description[0]) for description in cursor.description]
    return [
        {column: _json_safe(value) for column, value in zip(columns, row, strict=True)}
        for row in cursor.fetchall()
    ]


def _decision_row(
    row: dict[str, Any] | None,
    decision_id: str,
    *,
    kind: str,
) -> dict[str, Any]:
    """Require one exact decision row or raise a sanitized not-found error."""
    if row is None:
        raise UserError(
            f"No {kind} decision exists for this id.",
            code=error_codes.MUTATION_NOT_FOUND,
        )
    return row


def _query_ids(db: Database, sql: str, params: list[Any]) -> tuple[str, ...]:
    """Return stable distinct logical IDs for a confirmation blast radius."""
    try:
        rows = db.execute(sql, params).fetchall()
    except duckdb.CatalogException:
        return ()
    return tuple(dict.fromkeys(str(row[0]) for row in rows if row[0] is not None))


class ReviewDecisionsService:
    """Preflight and atomically dispatch consolidated identity decisions."""

    def __init__(self, db: Database, *, actor: str) -> None:
        """Bind the batch service to one database connection and audit actor."""
        self._db = db
        self._actor = actor

    @staticmethod
    def _ordinary_error(
        *,
        index: int,
        request: CategorizationDecisionRequest | MatchDecisionRequest,
        code: str,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "index": index,
            "kind": request.kind,
            "decision_id": request.decision_id,
            "code": code,
            "reason": reason,
        }

    def _prepare_categorization(
        self,
        request: CategorizationDecisionRequest,
    ) -> OrdinaryDecisionPlanItem:
        decision_repo = CategorizationDecisionsRepo(self._db)
        decision = decision_repo.fetch_by_id(request.decision_id)
        if decision is not None and decision["status"] != "pending":
            raise UserError(
                "The categorization decision is already decided.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        transaction_id = (
            str(decision["transaction_id"]) if decision is not None else None
        )
        row = self._db.execute(
            f"""
            SELECT tx.transaction_id, tc.category, tc.subcategory,
                   tx.description, tx.memo
            FROM {FCT_TRANSACTIONS.full_name} AS tx
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} AS tc
              ON tc.transaction_id = tx.transaction_id
            WHERE (
                (? IS NOT NULL AND tx.transaction_id = ?)
                OR (
                    ? IS NULL
                    AND 'cat_' || substr(sha256(tx.transaction_id), 1, 16)
                        = left(?, 20)
                )
            )
            LIMIT 1
            """,  # noqa: S608  # TableRef constants + parameterized value
            [
                transaction_id,
                transaction_id,
                transaction_id,
                request.decision_id,
            ],
        ).fetchone()
        if row is None:
            raise UserError(
                "No pending categorization decision exists for this id.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        transaction_id = str(row[0])
        projected = decision_repo.project_pending_attempts([transaction_id]).get(
            transaction_id
        )
        if projected is None or str(projected["decision_id"]) != request.decision_id:
            raise UserError(
                "No pending categorization decision exists for this id.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        current_category = cast(str | None, row[1])
        if request.decision == "reject":
            if current_category is not None:
                raise UserError(
                    "The transaction was categorized after this proposal was created.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            return OrdinaryDecisionPlanItem(
                request=request,
                changed=True,
                status="rejected",
                transaction_id=transaction_id,
            )

        category_row = self._db.execute(
            f"""
            SELECT category_id
            FROM {CATEGORIES.full_name}
            WHERE category = ?
              AND subcategory IS NOT DISTINCT FROM ?
              AND is_active
            LIMIT 1
            """,  # noqa: S608  # TableRef constant + parameterized values
            [request.category, request.subcategory],
        ).fetchone()
        if category_row is None:
            raise UserError(
                "The requested category target does not exist or is inactive.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        if current_category is not None:
            raise UserError(
                "The transaction was categorized after this proposal was created.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )

        merchant_id: str | None = None
        merchant_changed = False
        match_text: str | None = None
        if request.canonical_merchant_name is not None:
            match_text, _description, _memo = build_match_inputs(
                cast(str | None, row[3]),
                cast(str | None, row[4]),
            )
            if match_text:
                categorization = CategorizationService(self._db)
                merchant_id = categorization.find_review_merchant(
                    request.canonical_merchant_name,
                    category=cast(str, request.category),
                    subcategory=request.subcategory,
                )
                if merchant_id is None:
                    merchant_changed = True
                else:
                    exemplar_row = self._db.execute(
                        f"""
                        SELECT list_contains(exemplars, ?)
                        FROM {USER_MERCHANTS.full_name}
                        WHERE merchant_id = ?
                        """,  # noqa: S608  # TableRef constant + parameterized values
                        [match_text, merchant_id],
                    ).fetchone()
                    merchant_changed = not bool(exemplar_row and exemplar_row[0])
        return OrdinaryDecisionPlanItem(
            request=request,
            changed=True,
            status="accepted",
            transaction_id=transaction_id,
            category_id=str(category_row[0]),
            category_changed=True,
            merchant_changed=merchant_changed,
            merchant_id=merchant_id,
            match_text=match_text,
            merchant_group_key=(
                request.canonical_merchant_name,
                cast(str, request.category),
                request.subcategory,
            )
            if request.canonical_merchant_name is not None
            else None,
        )

    def _prepare_match(
        self,
        request: MatchDecisionRequest,
    ) -> OrdinaryDecisionPlanItem:
        row = get_match_decision(self._db, request.decision_id)
        if row is None:
            raise UserError(
                "No match decision exists for this id.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        target = "accepted" if request.decision == "accept" else "rejected"
        current = str(row["match_status"])
        if current != "pending":
            raise UserError(
                "The match decision is already decided.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        return OrdinaryDecisionPlanItem(
            request=request,
            changed=True,
            status=target,
        )

    def plan_ordinary(
        self,
        decisions: list[OrdinaryReviewDecisionRequest],
    ) -> tuple[OrdinaryDecisionPlanItem, ...]:
        """Resolve an entire ordered ordinary batch and report every invalid id."""
        if not decisions:
            raise UserError(
                "decisions must contain at least one review decision.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        errors: list[dict[str, Any]] = []
        prepared: list[OrdinaryDecisionPlanItem] = []
        seen: set[tuple[str, str]] = set()
        for index, request in enumerate(decisions):
            key = (request.kind, request.decision_id)
            if key in seen:
                errors.append(
                    self._ordinary_error(
                        index=index,
                        request=request,
                        code=error_codes.MUTATION_INVALID_INPUT,
                        reason="The same decision appears more than once in the batch.",
                    )
                )
                continue
            seen.add(key)
            try:
                item = (
                    self._prepare_categorization(request)
                    if isinstance(request, CategorizationDecisionRequest)
                    else self._prepare_match(request)
                )
            except UserError as exc:
                errors.append(
                    self._ordinary_error(
                        index=index,
                        request=request,
                        code=exc.code,
                        reason=exc.message,
                    )
                )
            else:
                prepared.append(item)
        if errors:
            raise UserError(
                "Review decision preflight failed.",
                code=error_codes.MUTATION_INVALID_INPUT,
                details={"errors": errors},
            )
        return tuple(prepared)

    def apply_ordinary(
        self,
        decisions: list[OrdinaryReviewDecisionRequest],
    ) -> tuple[OrdinaryDecisionPlanItem, ...]:
        """Revalidate and atomically apply an ordinary decision batch."""
        initial = self.plan_ordinary(decisions)
        created_merchant_ids: list[str] = []
        touched_merchant_ids: list[str] = []
        self._db.begin()
        try:
            decision_repo = CategorizationDecisionsRepo(self._db)
            for item in initial:
                if isinstance(item.request, CategorizationDecisionRequest):
                    decision_repo.ensure_pending(
                        cast(str, item.transaction_id),
                        actor=self._actor,
                        expected_decision_id=item.request.decision_id,
                        in_outer_txn=True,
                    )
            live = self.plan_ordinary(decisions)
            category_service = CategorizationService(self._db)
            match_repo = MatchDecisionsRepo(self._db)
            batch_merchants: dict[tuple[str, str, str | None], str] = {}
            for item in live:
                if not item.changed:
                    continue
                request = item.request
                if isinstance(request, CategorizationDecisionRequest):
                    merchant_id = item.merchant_id
                    if request.decision == "accept":
                        if item.merchant_group_key is not None:
                            merchant_id = batch_merchants.get(
                                item.merchant_group_key,
                                merchant_id,
                            )
                        creates_merchant = merchant_id is None and item.merchant_changed
                        merchant_id = (
                            category_service.apply_review_categorization_in_active_txn(
                                cast(str, item.transaction_id),
                                category=cast(str, request.category),
                                subcategory=request.subcategory,
                                canonical_merchant_name=request.canonical_merchant_name,
                                match_text=item.match_text,
                                existing_merchant_id=merchant_id,
                                category_changed=item.category_changed,
                                merchant_changed=item.merchant_changed,
                                actor=self._actor,
                            )
                        )
                        if (
                            merchant_id is not None
                            and item.merchant_group_key is not None
                        ):
                            batch_merchants[item.merchant_group_key] = merchant_id
                        if merchant_id is not None and item.merchant_changed:
                            touched_merchant_ids.append(merchant_id)
                            if creates_merchant:
                                created_merchant_ids.append(merchant_id)
                    decision_repo.update_status(
                        request.decision_id,
                        status=cast(Literal["accepted", "rejected"], item.status),
                        category_id=item.category_id,
                        merchant_id=merchant_id,
                        decided_by="user",
                        actor=self._actor,
                        in_outer_txn=True,
                    )
                else:
                    match_repo.update_status(
                        request.decision_id,
                        status=item.status,
                        decided_by="user",
                        actor=self._actor,
                        in_outer_txn=True,
                    )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        if touched_merchant_ids:
            category_service.record_committed_review_merchants(
                created_merchant_ids=tuple(created_merchant_ids),
                touched_merchant_ids=tuple(touched_merchant_ids),
            )
        return live

    def _prepare_account(
        self,
        request: AccountLinkDecisionRequest,
    ) -> IdentityDecisionPlanItem:
        service = AccountLinksService(self._db, actor=self._actor)
        decision = _decision_row(
            service.decision_by_id(request.decision_id),
            request.decision_id,
            kind="account-link",
        )
        source_id = str(decision["provisional_account_id"])
        candidate_id = str(decision["candidate_account_id"])
        target_id = request.target_id or candidate_id
        status: Literal["accepted", "rejected"] = (
            "accepted" if request.decision == "accept" else "rejected"
        )
        current = str(decision["status"])
        if current == status:
            if request.decision == "accept" and request.target_id != candidate_id:
                raise UserError(
                    "target_id does not match the accepted account-link candidate.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            changed = False
        elif current != "pending":
            raise UserError(
                "The account-link decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        else:
            changed = True
            if request.decision == "accept":
                impact = service.accept_impact(
                    request.decision_id,
                    target_account_id=cast(str, request.target_id),
                )
                source_id = impact.provisional_account_id
                target_id = impact.candidate_account_id
        decisions = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {ACCOUNT_LINK_DECISIONS.full_name}
            WHERE (provisional_account_id = ? OR candidate_account_id = ?)
              AND status = 'pending' AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef constants + parameterized values
            [source_id, source_id],
        )
        links = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {ACCOUNT_LINKS.full_name}
            WHERE account_id = ? AND status = 'accepted'
            ORDER BY link_id
            """,  # noqa: S608  # TableRef constant + parameterized value
            [source_id],
        )
        material_accept = changed and request.decision == "accept"
        transactions = (
            _query_ids(
                self._db,
                f"""
                SELECT DISTINCT transaction_id
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE account_id = ?
                ORDER BY transaction_id
                """,  # noqa: S608  # TableRef constant + parameterized value
                [source_id],
            )
            if material_accept
            else ()
        )
        return IdentityDecisionPlanItem(
            request=request,
            changed=changed,
            status=status,
            source_id=source_id,
            target_id=target_id,
            group_key=("account", source_id),
            before_state=_json_safe({
                "decision": _json_safe(decision),
                "affected_decisions": decisions,
                "accepted_links": links,
            }),
            affected_ids={
                "accounts": tuple(dict.fromkeys((source_id, target_id)))
                if material_accept
                else (),
                "merchants": (),
                "securities": (),
                "transactions": transactions,
                "lots": (),
            },
        )

    def _prepare_merchant(
        self,
        request: MerchantLinkDecisionRequest,
    ) -> IdentityDecisionPlanItem:
        service = MerchantLinksService(self._db, actor=self._actor)
        decision = _decision_row(
            service.decision_by_id(request.decision_id),
            request.decision_id,
            kind="merchant-link",
        )
        source_id = (
            f"{decision['source_type']}:merchant_entity_id:{decision['ref_value']}"
        )
        candidate_id = str(decision["candidate_merchant_id"])
        target_id = request.target_id or source_id
        status: Literal["accepted", "rejected"] = (
            "accepted" if request.decision == "accept" else "rejected"
        )
        current = str(decision["status"])
        if current == status:
            if request.decision == "accept" and request.target_id != candidate_id:
                raise UserError(
                    "target_id does not match the accepted merchant-link candidate.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            changed = False
        elif current != "pending":
            raise UserError(
                "The merchant-link decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        else:
            changed = True
            if request.decision == "accept":
                impact = service.accept_impact(
                    request.decision_id,
                    target_merchant_id=cast(str, request.target_id),
                )
                target_id = impact.candidate_merchant_id
        decisions = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {MERCHANT_LINK_DECISIONS.full_name}
            WHERE source_type = ? AND ref_value = ?
              AND status = 'pending' AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["source_type"], decision["ref_value"]],
        )
        links = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {MERCHANT_LINKS.full_name}
            WHERE source_type = ? AND ref_value = ?
            ORDER BY link_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["source_type"], decision["ref_value"]],
        )
        material_accept = changed and request.decision == "accept"
        return IdentityDecisionPlanItem(
            request=request,
            changed=changed,
            status=status,
            source_id=source_id,
            target_id=target_id,
            group_key=(
                "merchant",
                str(decision["source_type"]),
                str(decision["ref_value"]),
            ),
            before_state=_json_safe({
                "decision": _json_safe(decision),
                "affected_decisions": decisions,
                "existing_links": links,
            }),
            affected_ids={
                "accounts": (),
                "merchants": (target_id,) if material_accept else (),
                "securities": (),
                "transactions": (),
                "lots": (),
            },
        )

    def _prepare_security(
        self,
        request: SecurityLinkDecisionRequest,
    ) -> IdentityDecisionPlanItem:
        service = SecurityLinksService(self._db, actor=self._actor)
        decision = _decision_row(
            service.decision_by_id(request.decision_id),
            request.decision_id,
            kind="security-link",
        )
        binding = self._db.execute(
            f"""
            SELECT security_id FROM {SECURITY_LINKS.full_name}
            WHERE ref_kind = ? AND ref_value = ? AND source_type = ?
              AND status = 'accepted'
            LIMIT 1
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["ref_kind"], decision["ref_value"], decision["source_type"]],
        ).fetchone()
        source_id = str(binding[0]) if binding is not None else request.decision_id
        candidate_id = str(decision["candidate_security_id"])
        target_id = request.target_id or candidate_id
        status: Literal["accepted", "rejected"] = (
            "accepted" if request.decision == "accept" else "rejected"
        )
        current = str(decision["status"])
        selection_disposal_ids: tuple[str, ...] = ()
        if current == status:
            if request.decision == "accept" and request.target_id != candidate_id:
                raise UserError(
                    "target_id does not match the accepted security-link candidate.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            changed = False
        elif current != "pending":
            raise UserError(
                "The security-link decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        else:
            changed = True
            if request.decision == "accept":
                impact = service.accept_impact(
                    request.decision_id,
                    into=cast(str, request.target_id),
                )
                source_id = impact.provisional_security_id
                target_id = impact.candidate_security_id
                selection_disposal_ids = impact.lot_selection_disposal_ids
        decisions = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {SECURITY_LINK_DECISIONS.full_name}
            WHERE source_type = ? AND ref_kind = ? AND ref_value = ?
              AND status = 'pending' AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["source_type"], decision["ref_kind"], decision["ref_value"]],
        )
        links = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {SECURITY_LINKS.full_name}
            WHERE security_id = ? AND status = 'accepted'
            ORDER BY link_id
            """,  # noqa: S608  # TableRef constant + parameterized value
            [source_id],
        )
        securities = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {SECURITIES.full_name}
            WHERE security_id IN (?, ?)
            ORDER BY security_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [source_id, target_id],
        )
        manual = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name}
            WHERE security_id = ?
            ORDER BY source_transaction_id
            """,  # noqa: S608  # TableRef constant + parameterized value
            [source_id],
        )
        selections = [
            row
            for disposal_id in selection_disposal_ids
            for row in _query_json_rows(
                self._db,
                f"""
                SELECT * FROM {LOT_SELECTIONS.full_name}
                WHERE investment_transaction_id = ?
                ORDER BY lot_id
                """,  # noqa: S608  # TableRef constant + parameterized value
                [disposal_id],
            )
        ]
        material_accept = changed and request.decision == "accept"
        core_transactions = (
            _query_ids(
                self._db,
                f"""
                SELECT DISTINCT investment_transaction_id
                FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
                WHERE security_id = ?
                ORDER BY investment_transaction_id
                """,  # noqa: S608  # TableRef constant + parameterized value
                [source_id],
            )
            if material_accept
            else ()
        )
        manual_transactions = (
            _query_ids(
                self._db,
                f"""
                SELECT DISTINCT COALESCE(
                    investment_transaction_id,
                    source_transaction_id
                )
                FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name}
                WHERE security_id = ?
                ORDER BY 1
                """,  # noqa: S608  # TableRef constant + parameterized value
                [source_id],
            )
            if material_accept
            else ()
        )
        transactions = tuple(dict.fromkeys((*core_transactions, *manual_transactions)))
        lots = (
            _query_ids(
                self._db,
                f"""
                SELECT DISTINCT lot_id
                FROM {FCT_INVESTMENT_LOTS.full_name}
                WHERE security_id = ?
                ORDER BY lot_id
                """,  # noqa: S608  # TableRef constant + parameterized value
                [source_id],
            )
            if material_accept
            else ()
        )
        return IdentityDecisionPlanItem(
            request=request,
            changed=changed,
            status=status,
            source_id=source_id,
            target_id=target_id,
            group_key=(
                "security",
                str(decision["source_type"]),
                str(decision["ref_kind"]),
                str(decision["ref_value"]),
            ),
            before_state=_json_safe({
                "decision": _json_safe(decision),
                "affected_decisions": decisions,
                "accepted_links": links,
                "securities": securities,
                "manual_investment_transactions": manual,
                "lot_selections": selections,
            }),
            affected_ids={
                "accounts": (),
                "merchants": (),
                "securities": tuple(dict.fromkeys((source_id, target_id)))
                if material_accept
                else (),
                "transactions": transactions,
                "lots": lots,
            },
        )

    def plan_identity(
        self,
        decisions: list[IdentityDecisionRequest],
    ) -> IdentityDecisionPlan:
        """Resolve a complete ordered identity batch before its first write."""
        if not decisions:
            raise UserError(
                "decisions must contain at least one identity decision.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        errors: list[dict[str, Any]] = []
        prepared: list[IdentityDecisionPlanItem] = []
        seen_ids: set[tuple[str, str]] = set()
        seen_groups: set[tuple[str, ...]] = set()
        for index, request in enumerate(decisions):
            key = (request.kind, request.decision_id)
            if key in seen_ids:
                errors.append({
                    "index": index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": error_codes.MUTATION_INVALID_INPUT,
                    "reason": "The same decision appears more than once in the batch.",
                })
                continue
            seen_ids.add(key)
            try:
                if isinstance(request, AccountLinkDecisionRequest):
                    item = self._prepare_account(request)
                elif isinstance(request, MerchantLinkDecisionRequest):
                    item = self._prepare_merchant(request)
                else:
                    item = self._prepare_security(request)
            except UserError as exc:
                errors.append({
                    "index": index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": exc.code,
                    "reason": exc.message,
                })
                continue
            if item.group_key in seen_groups:
                errors.append({
                    "index": index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": error_codes.MUTATION_INVALID_INPUT,
                    "reason": (
                        "Multiple decisions target one identity review group; "
                        "submit exactly one terminal decision for that group."
                    ),
                })
                continue
            seen_groups.add(item.group_key)
            prepared.append(item)
        indexed = [(decisions.index(item.request), item) for item in prepared]
        graph_error_ids: set[str] = set()
        for accept_index, accept in indexed:
            if not accept.changed or accept.request.decision != "accept":
                continue
            domain_type = (
                AccountLinkDecisionRequest
                if isinstance(accept.request, AccountLinkDecisionRequest)
                else (
                    SecurityLinkDecisionRequest
                    if isinstance(accept.request, SecurityLinkDecisionRequest)
                    else None
                )
            )
            if domain_type is None:
                continue
            for other_index, other in indexed:
                if (
                    other is accept
                    or not other.changed
                    or not isinstance(other.request, domain_type)
                ):
                    continue
                overlaps_destroyed_source = accept.source_id in {
                    other.source_id,
                    other.target_id,
                }
                consumes_surviving_target = accept.target_id == other.source_id
                if not overlaps_destroyed_source and not consumes_surviving_target:
                    continue
                error_index, request = max(
                    (accept_index, accept.request),
                    (other_index, other.request),
                    key=lambda pair: pair[0],
                )
                if request.decision_id in graph_error_ids:
                    continue
                graph_error_ids.add(request.decision_id)
                errors.append({
                    "index": error_index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": error_codes.MUTATION_INVALID_INPUT,
                    "reason": (
                        "Identity merges cannot consume a source or intermediate "
                        "target referenced by another changed decision."
                    ),
                })
        if errors:
            raise UserError(
                "Identity decision preflight failed.",
                code=error_codes.MUTATION_INVALID_INPUT,
                details={"errors": errors},
            )
        return IdentityDecisionPlan(items=tuple(prepared))

    def apply_identity(
        self,
        decisions: list[IdentityDecisionRequest],
        *,
        verify: Callable[[IdentityDecisionPlan], None],
    ) -> IdentityDecisionPlan:
        """Re-preflight then apply every identity decision in one transaction."""
        self._db.begin()
        try:
            plan = self.plan_identity(decisions)
            verify(plan)
            if plan.changed_count == 0:
                raise UserError(
                    "Every identity decision is already satisfied.",
                    code=error_codes.MUTATION_NOTHING_TO_DO,
                )
            account_service = AccountLinksService(self._db, actor=self._actor)
            merchant_service = MerchantLinksService(self._db, actor=self._actor)
            security_service = SecurityLinksService(self._db, actor=self._actor)
            for item in plan.items:
                if not item.changed:
                    continue
                request = item.request
                decided_by = "user" if request.decision == "accept" else "auto"
                if isinstance(request, AccountLinkDecisionRequest):
                    account_service.set(
                        request.decision_id,
                        target_account_id=request.target_id,
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
                elif isinstance(request, MerchantLinkDecisionRequest):
                    merchant_service.set(
                        request.decision_id,
                        target_merchant_id=request.target_id,
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
                elif request.decision == "accept":
                    security_service.accept_merge(
                        request.decision_id,
                        into=cast(str, request.target_id),
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
                else:
                    security_service.reject_merge(
                        request.decision_id,
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        account_changed = any(
            item.changed and isinstance(item.request, AccountLinkDecisionRequest)
            for item in plan.items
        )
        merchant_outcomes = tuple(
            item.status
            for item in plan.items
            if item.changed and isinstance(item.request, MerchantLinkDecisionRequest)
        )
        security_outcomes = tuple(
            item.status
            for item in plan.items
            if item.changed and isinstance(item.request, SecurityLinkDecisionRequest)
        )
        if account_changed:
            account_service.record_committed_outer_decisions()
        if merchant_outcomes:
            merchant_service.record_committed_outer_outcomes(merchant_outcomes)
        if security_outcomes:
            security_service.record_committed_outer_outcomes(security_outcomes)
        return plan
