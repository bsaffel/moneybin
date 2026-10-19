"""Assess observed source scopes locally, including those absent from core."""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import date, datetime
from functools import cache
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from moneybin.connectors.sync_models import (
    ScopeOutcome,
    SimpleFINHistorySnapshot,
    SimpleFINSourceResult,
    SimpleFINV1OrgScope,
    SimpleFINV2ConnectionScope,
    SyncJobDetails,
)
from moneybin.database import SQLMESH_ROOT, Database, has_column
from moneybin.extractors.simplefin.identity import source_origin
from moneybin.privacy.payloads.sync_completeness import (
    SyncCompleteness,
    SyncCompletenessKind,
    SyncCompletenessReason,
)
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    ACCOUNT_SETTINGS,
    DIM_ACCOUNTS,
    SIMPLEFIN_ACCOUNTS,
    SIMPLEFIN_BALANCES,
    SIMPLEFIN_RESPONSES,
    SIMPLEFIN_SYNC_RECEIPTS,
    SIMPLEFIN_TRANSACTIONS,
    SYNC_JOB_STATE,
)

type _Scope = tuple[str, str]
_OUTCOMES = TypeAdapter(list[ScopeOutcome])
_SKIPPED_CHILD_ERRORS = frozenset({
    "invalid_transactions",
    "invalid_identity",
    "ambiguous_identity",
})
_RETRIEVAL: dict[str, SyncCompletenessKind] = {
    "partial": "retrieval_partial",
    "failed": "retrieval_failed",
    "deferred": "retrieval_deferred",
}


@cache
def _currencies() -> frozenset[str]:
    """Use the staging projection's seed even before the first transform."""
    with (SQLMESH_ROOT / "models/seeds/iso_currencies.csv").open() as stream:
        return frozenset(
            row["currency_code"]
            for row in csv.DictReader(stream)
            if row["minor_unit"].strip()
        )


@dataclass(frozen=True)
class _Receipt:
    job_id: UUID
    started_at: datetime
    status: str
    source: SimpleFINSourceResult
    local_outcomes: list[ScopeOutcome]


def _native_scope(
    source: SimpleFINSourceResult,
    native_scope: SimpleFINV1OrgScope | SimpleFINV2ConnectionScope,
    account_id: str,
) -> _Scope:
    native = (
        native_scope.org.model_dump(by_alias=True, exclude_none=True)
        if isinstance(native_scope, SimpleFINV1OrgScope)
        else native_scope.model_dump(by_alias=True)
    )
    return source_origin(
        source.connection_id, source.protocol_version, native
    ), account_id


class SyncCompletenessService:
    """Read immutable receipts, native observations, bindings and settings."""

    def __init__(
        self,
        db: Database,
        *,
        net_worth: bool = True,
        include_archived: bool = False,
        from_date: date | None = None,
    ) -> None:
        """Use the report's resolved account inclusion policy for local evidence."""
        self.db = db
        self.net_worth = net_worth
        self.include_archived = include_archived
        self.from_date = from_date

    def assess(self, *, account_ids: Collection[str] | None = None) -> SyncCompleteness:
        """Assess local sync evidence; missing native identity remains in scope."""
        receipts: list[_Receipt] = []
        issues: dict[SyncCompletenessKind, set[_Scope]] = defaultdict(set)
        rows = self.db.execute(
            f"""SELECT job_id, started_at, status, source_metadata, local_outcomes
                FROM {SIMPLEFIN_SYNC_RECEIPTS.full_name}"""  # trusted TableRef
        ).fetchall()
        observations = self.db.execute(
            f"""SELECT r.connection_id, a.source_origin, a.account_id,
                    a.currency, a.validation_error
                FROM {SIMPLEFIN_ACCOUNTS.full_name} AS a
                JOIN {SIMPLEFIN_RESPONSES.full_name} AS r USING (batch_id)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY a.source_origin, a.account_id
                    ORDER BY a.fetched_at DESC, a.batch_id DESC,
                        a.observation_version DESC
                ) = 1"""  # trusted TableRef
        ).fetchall()
        if not rows and not observations:
            return SyncCompleteness(complete=True, reasons=(), applicable=False)
        for job_id, started_at, status, metadata, outcomes in rows:
            try:
                receipts.append(
                    _Receipt(
                        UUID(str(job_id)),
                        started_at,
                        str(status),
                        SimpleFINSourceResult.model_validate_json(metadata),
                        _OUTCOMES.validate_json(outcomes),
                    )
                )
            except ValidationError:
                issues["evidence_unavailable"].add((str(job_id), ""))

        bindings = {
            (str(origin), str(native)): str(canonical)
            for origin, native, canonical in self.db.execute(
                f"""SELECT source_origin, ref_value, account_id
                    FROM {ACCOUNT_LINKS.full_name}
                    WHERE source_type = 'simplefin' AND ref_kind = 'source_native'
                    AND status = 'accepted'"""  # trusted TableRef
            ).fetchall()
        }
        settings = self._settings()

        def included_account(canonical: str) -> bool:
            if account_ids is not None and canonical not in account_ids:
                return False
            setting = settings.get(canonical, {})
            if self.net_worth and not setting.get("include_in_net_worth", True):
                return False
            if setting.get("archived", False):
                archived_at = setting.get("archived_at")
                return bool(
                    self.include_archived
                    and archived_at is not None
                    and (self.from_date is None or self.from_date <= archived_at)
                )
            return True

        def included(scope: _Scope) -> bool:
            canonical = bindings.get(scope)
            return canonical is None or included_account(canonical)

        def add(kind: SyncCompletenessKind, scope: _Scope) -> None:
            if included(scope):
                issues[kind].add(scope)

        observed_scopes: dict[UUID, set[_Scope]] = defaultdict(set)
        for connection, origin, native, currency, error in observations:
            scope = (str(origin), str(native))
            observed_scopes[UUID(str(connection))].add(scope)
            if not included(scope):
                continue
            canonical = bindings.get(scope)
            if canonical is None:
                add("unresolved_identity", scope)
            else:
                setting = settings.get(canonical, {})
                kind, subtype = (
                    setting.get("account_type"),
                    setting.get("account_subtype"),
                )
                if kind is None:
                    add("unresolved_account_type", scope)
                elif not (
                    kind == "depository"
                    and subtype in {"checking", "savings", "cash"}
                    or kind == "credit"
                    and subtype == "credit card"
                ):
                    add("unsupported_account_type", scope)
            if currency not in _currencies():
                add("unsupported_currency", scope)
            if error:
                add("invalid_observation", scope)

        # Inspect latest evidence before eligibility filtering; older valid rows
        # may still project while a later malformed correction remains unresolved.
        # A different balance instant cannot repair that correction; NULL
        # instants stay conservative rather than acquiring an inferred identity.
        for table, partition in (
            (SIMPLEFIN_BALANCES, "source_origin, account_id, balance_at"),
            (SIMPLEFIN_TRANSACTIONS, "transaction_reference"),
        ):
            for origin, native, error, currency in self.db.execute(
                f"""SELECT source_origin, account_id, validation_error, currency
                    FROM {table.full_name}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition}
                        ORDER BY fetched_at DESC, batch_id DESC,
                            observation_version DESC) = 1"""  # fixed TableRefs and partition expressions
            ).fetchall():
                scope = (str(origin), str(native))
                if error == "unsupported_precision":
                    add("unsupported_precision", scope)
                elif error not in {None, "pending", "nonpositive_posted"}:
                    add("invalid_observation", scope)
                if currency not in _currencies():
                    add("unsupported_currency", scope)

        self._receipt_issues(receipts, add, observed_scopes)
        # Accepted targets can still be legacy native IDs; bindings do not
        # establish mint provenance, so diagnostics expose counts only.
        reasons = tuple(
            SyncCompletenessReason(
                kind=kind,
                affected_account_ids=(),
                affected_account_count=len({
                    bindings[scope] for scope in scopes if scope in bindings
                }),
                unresolved_scope_count=sum(scope not in bindings for scope in scopes),
            )
            for kind, scopes in sorted(issues.items())
            if scopes
        )
        overlap_ids: set[str] = set()
        bound_accounts = set(bindings.values())
        for provisional, candidate in self.db.execute(
            f"""SELECT provisional_account_id, candidate_account_id
                FROM {ACCOUNT_LINK_DECISIONS.full_name}
                WHERE status = 'pending' AND reversed_at IS NULL"""  # trusted TableRef
        ).fetchall():
            pair = {str(provisional), str(candidate)}
            if pair & bound_accounts:
                overlap_ids.update(
                    account for account in pair if included_account(account)
                )
        if overlap_ids:
            reasons += (
                SyncCompletenessReason(
                    kind="suspected_overlap",
                    affected_account_ids=(),
                    affected_account_count=len(overlap_ids),
                    unresolved_scope_count=0,
                ),
            )
        return SyncCompleteness(
            complete=not reasons,
            reasons=reasons,
            applicable=bool(rows or observations),
        )

    def _settings(self) -> dict[str, dict[str, Any]]:
        settings: dict[str, dict[str, Any]] = {}
        if has_column(self.db, DIM_ACCOUNTS, "account_id"):
            subtype = (
                "account_subtype"
                if has_column(self.db, DIM_ACCOUNTS, "account_subtype")
                else "NULL AS account_subtype"
            )
            cursor = self.db.execute(
                f"""SELECT account_id, account_type, {subtype}, archived,
                    archived_at, include_in_net_worth FROM {DIM_ACCOUNTS.full_name}"""  # trusted TableRef and fixed column alternatives
            )
            names = [column[0] for column in cursor.description or []]
            settings = {
                str(row[0]): dict(zip(names, row, strict=True))
                for row in cursor.fetchall()
            }
        cursor = self.db.execute(
            f"""SELECT account_id, account_type, account_subtype, archived,
                archived_at, include_in_net_worth FROM {ACCOUNT_SETTINGS.full_name}"""  # trusted TableRef
        )
        names = [column[0] for column in cursor.description or []]
        for row in cursor.fetchall():
            setting = settings.setdefault(str(row[0]), {})
            for name, value in zip(names, row, strict=True):
                if value is not None or name == "archived_at":
                    setting[name] = value
        return settings

    def _receipt_issues(
        self,
        receipts: list[_Receipt],
        add: Callable[[SyncCompletenessKind, _Scope], None],
        observed_scopes: dict[UUID, set[_Scope]],
    ) -> None:
        generations: dict[UUID, int] = {}
        retained: dict[UUID, list[_Receipt]] = defaultdict(list)
        for receipt in receipts:
            source = receipt.source
            retained[source.connection_id].append(receipt)
            generations[source.connection_id] = max(
                generations.get(source.connection_id, 0), source.credential_generation
            )
        current = [
            receipt
            for receipt in receipts
            if receipt.source.credential_generation
            == generations[receipt.source.connection_id]
        ]
        by_connection: dict[UUID, list[_Receipt]] = defaultdict(list)
        for receipt in current:
            by_connection[receipt.source.connection_id].append(receipt)
        acknowledgements: dict[UUID, SyncJobDetails] = {}
        if current:
            for job_id, details in self.db.execute(
                f"SELECT job_id, job_details FROM {SYNC_JOB_STATE.full_name}"  # trusted TableRef
            ).fetchall():
                try:
                    acknowledgements[UUID(str(job_id))] = (
                        SyncJobDetails.model_validate_json(details)
                    )
                except ValidationError:
                    add("evidence_unavailable", (str(job_id), ""))
        for connection, candidates in by_connection.items():
            general = (str(connection), "")
            newest = max(receipt.started_at for receipt in candidates)
            # Credential replacement changes broker checkpoints, not the native
            # identity of skipped data. Retain its holds across generations.
            for receipt in retained[connection]:
                for outcome in receipt.local_outcomes:
                    if (
                        outcome.error_code in _SKIPPED_CHILD_ERRORS
                        or outcome.error_code == "provider_error"
                    ) and not self._local_hold_resolved(
                        receipt, outcome, retained[connection]
                    ):
                        add(
                            "retrieval_partial"
                            if outcome.error_code == "provider_error"
                            else "invalid_observation",
                            self._outcome_scope(receipt.source, outcome),
                        )
            for receipt in candidates:
                # The receipt status belongs to the whole job. Explicit source
                # outcomes locate its failures; another source may have failed.
                if (
                    receipt.started_at == newest
                    and receipt.status in _RETRIEVAL
                    and not receipt.source.outcomes
                ):
                    add(_RETRIEVAL[receipt.status], general)

            outcomes: dict[_Scope, list[tuple[datetime, ScopeOutcome]]] = defaultdict(
                list
            )
            latest_scopes: set[_Scope] = set()
            histories: list[tuple[SimpleFINSourceResult, SimpleFINHistorySnapshot]] = []
            for receipt in candidates:
                source = receipt.source
                for outcome in source.outcomes:
                    scope = self._outcome_scope(source, outcome)
                    outcomes[scope].append((receipt.started_at, outcome))
                    if receipt.started_at == newest:
                        latest_scopes.add(scope)
                for outcome in receipt.local_outcomes:
                    scope = self._outcome_scope(source, outcome)
                    if outcome.error_code == "provider_error":
                        outcomes[scope].append((receipt.started_at, outcome))
                    if outcome.account_id is None:
                        if receipt.started_at == newest:
                            add("invalid_observation", scope)
                history = source.history
                details = acknowledgements.get(receipt.job_id)
                if details is not None and details.result.ack_state == "confirmed":
                    ack = details.acknowledgement
                    if ack is not None and ack.job_id == receipt.job_id:
                        for confirmed in ack.results:
                            if (
                                isinstance(confirmed, SimpleFINSourceResult)
                                and confirmed.connection_id == connection
                                and confirmed.credential_generation
                                == source.credential_generation
                                and confirmed.protocol_version
                                == source.protocol_version
                            ):
                                history = confirmed.history
                if history is not None:
                    histories.append((source, history))
            for scope, observations in outcomes.items():
                latest = max(stamp for stamp, _ in observations)
                for stamp, outcome in observations:
                    if stamp == latest and outcome.status in _RETRIEVAL:
                        if scope == general and self._general_failure_resolved(
                            stamp,
                            outcome,
                            candidates,
                            observed_scopes.get(connection, set()),
                        ):
                            continue
                        add(_RETRIEVAL[outcome.status], scope)
            if not histories:
                for scope in observed_scopes.get(connection) or {general}:
                    add("history_unknown", scope)
                continue
            observed_at = max(history.observed_at for _, history in histories)
            for source, history in histories:
                if history.observed_at != observed_at:
                    continue
                if (
                    history.replay_required
                    or history.discovery_history_target is not None
                ):
                    add("history_pending", general)
                known = {
                    _native_scope(source, account.native_scope, account.account_id)
                    for account in history.accounts
                }
                # Older retrievals cannot establish that the newest request
                # covered every account in the current acknowledged history.
                for scope in known - latest_scopes:
                    add("evidence_unavailable", scope)
                missing = observed_scopes.get(connection, set()) - known
                for scope in missing:
                    add("history_unknown", scope)
                if not known and not missing:
                    add("history_unknown", general)
                for account in history.accounts:
                    scope = _native_scope(
                        source, account.native_scope, account.account_id
                    )
                    if account.history_target is not None:
                        add("history_pending", scope)
                    if not account.acknowledged_intervals:
                        add("history_unknown", scope)
                    elif len(account.acknowledged_intervals) > 1:
                        add("history_gap", scope)
                    elif any(
                        outcome.status == "complete"
                        and outcome.window is not None
                        and not account.has_acknowledged_through(outcome.window.end)
                        for _, outcome in outcomes.get(scope, [])
                    ):
                        add("history_pending", scope)

    def _general_failure_resolved(
        self,
        failed_at: datetime,
        failed: ScopeOutcome,
        candidates: list[_Receipt],
        required_scopes: set[_Scope],
    ) -> bool:
        if failed.window is None:
            return False
        for receipt in candidates:
            if receipt.started_at <= failed_at or not receipt.source.outcomes:
                continue
            if any(outcome.status != "complete" for outcome in receipt.source.outcomes):
                continue
            covered = {
                self._outcome_scope(receipt.source, outcome)
                for outcome in receipt.source.outcomes
                if outcome.account_id is not None
                and outcome.native_scope is not None
                and outcome.window is not None
                and outcome.window.start <= failed.window.start
                and outcome.window.end >= failed.window.end
            }
            if covered and required_scopes <= covered:
                return True
        return False

    def _local_hold_resolved(
        self,
        held: _Receipt,
        outcome: ScopeOutcome,
        candidates: list[_Receipt],
    ) -> bool:
        window = outcome.window
        # Unknown identity cannot be matched to later evidence of recovery.
        if window is None or outcome.account_id is None or outcome.native_scope is None:
            return False
        scope = self._outcome_scope(held.source, outcome)
        for candidate in candidates:
            if candidate.started_at <= held.started_at:
                continue
            if any(
                local.account_id is None
                or (
                    (
                        local.error_code in _SKIPPED_CHILD_ERRORS
                        or local.error_code == "provider_error"
                    )
                    and self._outcome_scope(candidate.source, local) == scope
                )
                for local in candidate.local_outcomes
            ):
                continue
            if any(
                later.status == "complete"
                and self._outcome_scope(candidate.source, later) == scope
                and later.window is not None
                and later.window.start <= window.start
                and later.window.end >= window.end
                for later in candidate.source.outcomes
            ):
                return True
        return False

    @staticmethod
    def _outcome_scope(source: SimpleFINSourceResult, outcome: ScopeOutcome) -> _Scope:
        scope = outcome.native_scope
        if isinstance(scope, (SimpleFINV1OrgScope, SimpleFINV2ConnectionScope)):
            return _native_scope(source, scope, outcome.account_id or "")
        return str(source.connection_id), ""
