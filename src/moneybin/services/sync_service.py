"""Business logic for sync operations.

CLI and MCP layers are thin wrappers around this service. Owns:
- pull() orchestration (trigger → fetch → load)
- link() with optional auto-pull
- list_connections() with error-code → user-guidance mapping
- disconnect() with institution name → connection_id resolution

State source: server. No local connection-state mirror. See design Section 4.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from uuid import UUID

import duckdb
from pydantic import TypeAdapter, ValidationError

from moneybin import error_codes
from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    LinkInitiateResponse,
    LinkResult,
    NativeScope,
    PlaidSource,
    PlaidSyncDataResponse,
    ProviderSourceResult,
    PullResult,
    SimpleFINHistorySnapshot,
    SimpleFINSource,
    SimpleFINSourceResult,
    SimpleFINV1OrgScope,
    SyncAckV2Response,
    SyncConnectionView,
    SyncDisconnectResult,
    SyncHistoryAccountView,
    SyncHistoryView,
    SyncJobDetails,
    SyncJobSource,
    SyncScopeView,
    SyncSourceView,
)
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.account_identity import SourceAccount
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.extractors.simplefin.extractor import (
    SimpleFINExtractor,
    SimpleFINJobContext,
)
from moneybin.extractors.simplefin.identity import source_origin
from moneybin.investments.source_overlap import investment_source_overlap
from moneybin.metrics.observations import MetricObservations
from moneybin.metrics.registry import (
    ACCOUNT_LINK_OUTCOMES_TOTAL,
    SYNC_CONNECT_OUTCOMES,
    SYNC_INSTITUTION_ERRORS_TOTAL,
    SYNC_PULL_DURATION_SECONDS,
    SYNC_PULL_OUTCOMES_TOTAL,
    SYNC_PULL_TRANSACTIONS_LOADED,
)
from moneybin.orchestration.refresh import refresh as _refresh
from moneybin.orchestration.refresh import step_outcome as _step_outcome
from moneybin.progress import ProgressEvent
from moneybin.repositories.sync_job_state_repo import SyncJobState, SyncJobStateRepo
from moneybin.services.account_resolver import AccountResolver, fetch_display_name
from moneybin.services.security_resolver import SecurityResolver
from moneybin.tables import (
    ACCOUNT_LINKS,
    FCT_INVESTMENT_TRANSACTIONS,
    PLAID_ACCOUNTS,
    SIMPLEFIN_BALANCES,
    SIMPLEFIN_RESPONSES,
    SIMPLEFIN_SYNC_RECEIPTS,
)

logger = logging.getLogger(__name__)

_PROVIDER = "plaid"
_SOURCE_RESULT: TypeAdapter[ProviderSourceResult] = TypeAdapter(ProviderSourceResult)
_SOURCE_RESULTS: TypeAdapter[list[ProviderSourceResult]] = TypeAdapter(
    list[ProviderSourceResult]
)


_ERROR_GUIDANCE: dict[str, str] = {
    "ITEM_LOGIN_REQUIRED": "{institution} needs re-authentication — run `moneybin sync link --institution {institution}`",
    "ITEM_NOT_FOUND": "{institution} connection was revoked. Run `moneybin sync link` to reconnect.",
    "INSTITUTION_NOT_RESPONDING": "{institution} is temporarily unavailable. Try again later.",
    "INSTITUTION_DOWN": "{institution} is down for maintenance. Try again later.",
    "RATE_LIMIT_EXCEEDED": "Rate limit reached. Sync will resume on the next scheduled run.",
    "PRODUCTS_NOT_READY": "{institution} is still processing initial data. Try again in a few minutes.",
    "PRODUCT_NOT_READY": "{institution} is still processing investment data. Try again in a few minutes.",
    "PRODUCTS_NOT_SUPPORTED": "{institution} doesn't provide investment data through Plaid. Cash accounts still sync normally.",
    "INVALID_PRODUCT": "{institution} was linked before investment access — run `moneybin sync link` to re-consent.",
}


_SIMPLEFIN_ERROR_CODES = frozenset({
    "authorization_revoked",
    "payment_required",
    "rate_limited",
    "upstream_unavailable",
    "unsupported_protocol",
    "invalid_payload",
    "partial_data",
    "claim_outcome_unknown",
    "claim_rejected",
    "request_budget",
    "connection_unavailable",
    "account_absent",
    "provider_error",
    "response_too_large",
    "invalid_account",
    "invalid_amount",
    "invalid_identity",
    "invalid_pending",
    "invalid_timestamp",
    "invalid_transactions",
    "future_timestamp",
    "nonpositive_posted",
    "unsupported_precision",
})


def _safe_error_code(value: str | None) -> str | None:
    """Keep admitted broker codes; arbitrary provider text remains private."""
    if value is None:
        return None
    prefix = value.partition(":")[0]
    return (
        prefix
        if prefix in _SIMPLEFIN_ERROR_CODES or prefix in _ERROR_GUIDANCE
        else "provider_error"
    )


def validate_pull_selectors(
    *,
    institution: str | None = None,
    provider: str | None = None,
    connection_id: str | None = None,
    history_start: str | None = None,
    job_id: str | None = None,
    local_only: bool = False,
    force: bool = False,
) -> date | None:
    """Reject conflicting public pull modes before opening a database or transport."""
    if local_only and job_id is None:
        raise UserError(
            "local_only requires job_id", code=error_codes.MUTATION_INVALID_INPUT
        )
    if job_id is not None and (
        institution or provider or connection_id or history_start or force
    ):
        raise UserError(
            "Job recovery cannot be combined with retrieval selectors",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if institution and connection_id or force and history_start:
        raise UserError(
            "Conflicting sync selectors", code=error_codes.MUTATION_INVALID_INPUT
        )
    if provider not in {None, "plaid", "simplefin"}:
        raise UserError(
            "Choose plaid or simplefin", code=error_codes.MUTATION_INVALID_INPUT
        )
    try:
        if job_id is not None:
            UUID(job_id)
        if connection_id is not None:
            UUID(connection_id)
        return date.fromisoformat(history_start) if history_start else None
    except ValueError:
        raise UserError(
            "Use a valid job/connection UUID and ISO history date",
            code=error_codes.MUTATION_INVALID_INPUT,
        ) from None


class SyncService:
    """Coordinate provider jobs, durable local phases and grant management."""

    def __init__(
        self,
        *,
        client: SyncClient | None,
        db: Database,
        loader: PlaidExtractor,
        consumer_id: str | None = None,
    ) -> None:
        """Bind the service to a SyncClient, open Database, and PlaidExtractor."""
        self._client = client
        self._consumer_id = consumer_id
        self.db = db
        self.loader = loader

    @property
    def client(self) -> SyncClient:
        """Require transport only for broker operations, never offline repair."""
        if self._client is None:
            raise SyncAPIError("Broker access is unavailable during local-only repair")
        return self._client

    @property
    def consumer_id(self) -> str:
        """Use the active local profile identity without requiring broker authentication."""
        return (
            self._consumer_id
            if self._consumer_id is not None
            else self.client.consumer_id
        )

    # ------------------------------ Pull ------------------------------

    def pull(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        provider: str | None = None,
        connection_id: str | None = None,
        history_start: date | None = None,
        force: bool = False,
        refresh: bool = True,
        progress: Callable[[ProgressEvent], None] | None = None,
    ) -> PullResult:
        """Load a whole accepted broker job durably before ack, setup and refresh."""
        started = time.monotonic()
        counts = {provider if provider in {"plaid", "simplefin"} else "all": 0}
        try:
            result = self._pull_job(
                institution=institution,
                provider_item_id=provider_item_id,
                provider=provider,
                connection_id=connection_id,
                history_start=history_start,
                force=force,
                refresh=refresh,
                progress=progress,
                counts=counts,
            )
        except Exception:
            for name in counts:
                SYNC_PULL_OUTCOMES_TOTAL.labels(provider=name, status="failed").inc()
            raise
        else:
            for name, count in counts.items():
                sources = [
                    source
                    for source in result.source_results
                    if source.provider == name
                ]
                outcomes = [
                    outcome for source in sources for outcome in source.outcomes
                ]
                complete = result.job_status == "completed" and all(
                    outcome.status == "complete" for outcome in outcomes
                )
                SYNC_PULL_OUTCOMES_TOTAL.labels(
                    provider=name, status="success" if complete else "failed"
                ).inc()
                SYNC_PULL_TRANSACTIONS_LOADED.labels(provider=name).inc(count)
            for institution_result in result.institutions:
                if (
                    institution_result.status == "failed"
                    and institution_result.error_code
                ):
                    SYNC_INSTITUTION_ERRORS_TOTAL.labels(
                        error_code=institution_result.error_code
                    ).inc()
            return result
        finally:
            for name in counts:
                SYNC_PULL_DURATION_SECONDS.labels(provider=name).observe(
                    time.monotonic() - started
                )

    def _pull_job(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        provider: str | None = None,
        connection_id: str | None = None,
        history_start: date | None = None,
        force: bool = False,
        refresh: bool = True,
        progress: Callable[[ProgressEvent], None] | None = None,
        counts: dict[str, int],
    ) -> PullResult:
        """Load a whole accepted broker job durably before ack, setup and refresh."""
        if (
            sum(
                value is not None
                for value in (institution, provider_item_id, connection_id)
            )
            > 1
        ):
            raise UserError(
                "Choose one institution or exact connection selector",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if force and history_start is not None:
            raise UserError(
                "force and history_start are mutually exclusive",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if institution is not None:
            matched = self._find_institution(institution)
            if matched is None:
                raise UserError(
                    "Connection not found", code=error_codes.MUTATION_NOT_FOUND
                )
            connection_id = matched.id
        if progress is not None:
            progress(ProgressEvent("Syncing institutions"))
        trigger = self.client.trigger_sync(
            provider_item_id=provider_item_id,
            reset_cursor=force,
            provider=provider,
            connection_id=connection_id,
            history_start=history_start,
        )
        job_id = str(trigger.job_id)
        data = self.client.get_data(job_id)
        counts.clear()
        counts.update({source.provider: 0 for source in data.sources})
        context = self.client.get_status(job_id)
        if (
            str(context.job_id) != job_id
            or str(data.job_id) != job_id
            or context.status != data.status
            or trigger.status != data.status
            or context.started_at is None
            or context.completed_at is None
        ):
            raise SyncAPIError("Sync job metadata does not match the accepted result")
        job_context = SimpleFINJobContext(
            status=data.status,
            started_at=context.started_at,
            completed_at=context.completed_at,
        )
        expected = [
            _SOURCE_RESULT.validate_python(source.model_dump(exclude={"batches"}))
            for source in data.sources
        ]
        observed = _SOURCE_RESULTS.validate_python(context.results)
        if self._source_keys(expected) != self._source_keys(observed):
            raise SyncAPIError(
                "Sync source metadata does not match the accepted result"
            )
        # Validate provider payload shapes before any mutations; never expose Pydantic input dumps.
        plaid_data: dict[UUID, PlaidSyncDataResponse] = {}
        try:
            for source in data.sources:
                if isinstance(source, PlaidSource):
                    for batch in source.batches:
                        plaid_data[batch.batch_id] = (
                            PlaidSyncDataResponse.model_validate(batch.payload)
                        )
        except ValidationError:
            raise SyncAPIError("Sync provider payload validation failed") from None
        result = PullResult(
            job_id=job_id,
            job_status=data.status,
            transactions_loaded=0,
            accounts_loaded=0,
            balances_loaded=0,
            transactions_removed=0,
            institutions=[],
            source_results=expected,
            refresh_state="pending" if refresh else "deferred",
        )
        if progress is not None:
            progress(ProgressEvent("Loading synced data"))
        receipt_writes = 0
        observations = MetricObservations()
        previous_observations = self.loader.observations
        self.loader.observations = observations
        repo = SyncJobStateRepo(self.db)
        references: list[SyncJobSource] = []
        self.db.begin()
        try:
            for source, source_result in zip(data.sources, expected, strict=True):
                item_ids: set[str] = set()
                if isinstance(source, SimpleFINSource):
                    loaded = SimpleFINExtractor(self.db).load(
                        source, job_id=data.job_id, job_context=job_context
                    )
                    receipt_writes += 1
                    result.accounts_loaded += loaded.accounts
                    result.transactions_loaded += loaded.transactions
                    counts[source.provider] += loaded.transactions
                    result.balances_loaded += loaded.balances
                    source_result.outcomes = loaded.outcomes
                else:
                    for batch in source.batches:
                        native = plaid_data[batch.batch_id]
                        item_ids.update(
                            self.loader.build_account_to_item_map(native).values()
                        )
                        item_ids.update(
                            inst.provider_item_id
                            for inst in native.metadata.institutions
                        )
                        result.transactions_removed += (
                            self.loader.handle_removed_transactions(
                                native.removed_transactions
                            )
                        )
                        loaded_plaid = self.loader.load(
                            native, job_id, batch_id=batch.batch_id, in_outer_txn=True
                        )
                        receipt_writes += loaded_plaid.holdings_snapshots_loaded
                        counts[source.provider] += loaded_plaid.transactions_loaded
                        for field in (
                            "accounts_loaded",
                            "transactions_loaded",
                            "balances_loaded",
                            "securities_loaded",
                            "investment_transactions_loaded",
                            "holdings_loaded",
                            "holding_lots_loaded",
                            "security_prices_loaded",
                        ):
                            setattr(
                                result,
                                field,
                                getattr(result, field) + getattr(loaded_plaid, field),
                            )
                        result.institutions.extend(
                            inst.model_copy(
                                update={
                                    "error": "Provider retrieval incomplete"
                                    if inst.status == "failed"
                                    else None,
                                    "error_code": inst.error_code
                                    if inst.error_code in _ERROR_GUIDANCE
                                    else "provider_error"
                                    if inst.status == "failed"
                                    else None,
                                }
                            )
                            for inst in native.metadata.institutions
                        )
                references.append(
                    SyncJobSource(
                        source=_SOURCE_RESULT.validate_python(
                            source.model_dump(exclude={"batches"})
                        ),
                        batch_ids=[batch.batch_id for batch in source.batches],
                        plaid_item_ids=sorted(item_ids),
                    )
                )
            if not receipt_writes and not any(
                getattr(result, field)
                for field in (
                    "accounts_loaded",
                    "transactions_loaded",
                    "balances_loaded",
                    "transactions_removed",
                    "securities_loaded",
                    "investment_transactions_loaded",
                    "holdings_loaded",
                    "holding_lots_loaded",
                    "security_prices_loaded",
                )
            ):
                result.refresh_state = "complete"
            repo.ensure(
                job_id,
                self.consumer_id,
                SyncJobDetails(sources=references, result=result),
            )
            state = repo.get(job_id, self.consumer_id)
            if state is None:
                raise RuntimeError("Sync recovery evidence is missing")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            observations.flush("rollback")
            raise
        finally:
            self.loader.observations = previous_observations
        observations.flush()
        finished = self._finish_job(
            state, refresh=refresh, progress=progress, recover=False
        )
        return finished.model_copy(
            update={
                name: getattr(result, name)
                for name in (
                    "transactions_loaded",
                    "accounts_loaded",
                    "balances_loaded",
                    "transactions_removed",
                    "securities_loaded",
                    "investment_transactions_loaded",
                    "holdings_loaded",
                    "holding_lots_loaded",
                    "security_prices_loaded",
                )
            }
        )

    @staticmethod
    def _source_keys(
        sources: list[ProviderSourceResult],
    ) -> list[tuple[str, str, int, str | None]]:
        return sorted(
            (
                source.provider,
                str(source.connection_id),
                source.credential_generation,
                source.protocol_version
                if isinstance(source, SimpleFINSourceResult)
                else None,
            )
            for source in sources
        )

    def recover(
        self,
        job_id: str,
        *,
        refresh: bool = True,
        local_only: bool = False,
        progress: Callable[[ProgressEvent], None] | None = None,
    ) -> PullResult:
        """Resume an ingested job using status and local Raw, never provider retrieval."""
        state = SyncJobStateRepo(self.db).get(job_id, self.consumer_id)
        if state is None:
            raise UserError(
                "No durably ingested sync job matches this profile",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        return self._finish_job(
            state,
            refresh=refresh,
            progress=progress,
            recover=True,
            local_only=local_only,
        )

    def _finish_job(
        self,
        state: SyncJobState,
        *,
        refresh: bool,
        progress: Callable[[ProgressEvent], None] | None,
        recover: bool,
        local_only: bool = False,
    ) -> PullResult:
        repo = SyncJobStateRepo(self.db)
        result = state.details.result
        job_id = str(state.job_id)
        if not local_only and result.ack_state not in {"confirmed", "unavailable"}:
            try:
                ack = None
                if recover:
                    status = self.client.get_status(job_id)
                    if status.acked_at is not None:
                        ack = SyncAckV2Response(
                            job_id=state.job_id,
                            contract_version=2,
                            status="acked",
                            acked_at=status.acked_at,
                            results=status.results,
                        )
                if ack is None:
                    ack = SyncAckV2Response.model_validate(
                        self.client.ack(job_id), from_attributes=True
                    )
                if ack.job_id != state.job_id or self._source_keys(
                    ack.results
                ) != self._source_keys([item.source for item in state.details.sources]):
                    raise ValueError("Mismatched acknowledgement evidence")
                result.ack_state, result.ack_error_code = "confirmed", None
                state.acked_at = ack.acked_at
                state.details.acknowledgement = ack
                self._save_recovery(repo, state)
            except Exception as error:
                unavailable = isinstance(error, SyncAPIError) and error.status_code in {
                    404,
                    410,
                }
                result.ack_state = "unavailable" if unavailable else "unknown"
                result.ack_error_code = (
                    "ack_result_unavailable"
                    if unavailable
                    else "ack_confirmation_unavailable"
                )
                logger.warning(
                    f"Sync acknowledgement confirmation unavailable (job_id={job_id})"
                )
                self._save_recovery(repo, state)
        if result.setup_state != "complete":
            try:
                account_binding_changed = self._repair_accounts(state)
                resolver = SecurityResolver(self.db, actor="system")
                result.security_resolution = resolver.resolve_all()
                result.security_resolution_error = None
                result.setup_state, result.setup_error_code = "complete", None
                # Newly accepted bindings affect derived rows even after a zero-write replay.
                if (
                    resolver.writes or account_binding_changed
                ) and result.refresh_state == "complete":
                    result.refresh_state = "pending"
            except Exception:
                result.setup_state, result.setup_error_code = (
                    "failed",
                    "local_setup_failed",
                )
                logger.warning(f"Sync local setup requires repair (job_id={job_id})")
            self._save_recovery(repo, state)
        if refresh and result.refresh_state != "complete":
            try:
                if progress is not None:
                    progress(ProgressEvent("Refreshing reports"))
                refreshed = _refresh(
                    self.db,
                    steps=[
                        "match",
                        "investment_match",
                        "transform",
                        "categorize",
                        "identity",
                    ]
                    if recover
                    else None,
                    progress=progress,
                )
                result.transforms_applied = refreshed.applied
                result.transforms_duration_seconds = refreshed.duration_seconds
                result.transforms_error = (
                    "Local refresh failed; retry this job locally"
                    if refreshed.error
                    else None
                )
                result.transfers_retired = refreshed.transfers_retired
                result.refresh_steps = _step_outcome(refreshed)
                result.refresh_state = (
                    "failed"
                    if refreshed.error or result.refresh_steps.has_failure
                    else "complete"
                )
            except Exception:
                result.refresh_state = "failed"
                result.transforms_error = "Local refresh failed; retry this job locally"
                logger.warning(f"Sync local refresh requires repair (job_id={job_id})")
            self._save_recovery(repo, state)
        result.inspection_error_code = None
        sources: list[SyncSourceView] = []
        try:
            if result.setup_state == "complete":
                result.investment_source_overlap_accounts = (
                    self._investment_source_overlap()
                )
            if result.refresh_state == "complete":
                result.opening_bootstrap_rows = self._count_bootstrap_rows()
            sources = self._public_sources(state)
        except Exception:
            result.inspection_error_code = "local_inspection_failed"
            logger.warning(f"Sync local inspection requires retry (job_id={job_id})")
        self._save_recovery(repo, state)
        public_result = result.model_copy(deep=True)
        public_result.sources = sources
        return public_result

    def _account_reference(
        self, connection_id: UUID, protocol: str, scope: NativeScope, native_id: str
    ) -> tuple[str | None, str]:
        """Reuse native identity and accepted bindings without projecting their inputs."""
        origin = source_origin(
            connection_id,
            protocol,
            scope.org.model_dump(by_alias=True, exclude_none=True)
            if isinstance(scope, SimpleFINV1OrgScope)
            else scope.model_dump(by_alias=True),
        )
        row = self.db.execute(
            f"""SELECT account_id FROM {ACCOUNT_LINKS.full_name}
                WHERE source_type = 'simplefin' AND source_origin = ? AND ref_kind = 'source_native'
                  AND ref_value = ? AND status = 'accepted'""",  # trusted TableRef and parameterized identity
            [origin, native_id],
        ).fetchone()
        if row is None:
            return None, "Unresolved account"
        account_id = str(row[0])
        return account_id, fetch_display_name(self.db, account_id) or "Account"

    def _history_view(
        self,
        snapshot: SimpleFINHistorySnapshot | None,
        connection_id: UUID,
        protocol: str,
    ) -> SyncHistoryView | None:
        if snapshot is None:
            return None
        accounts: list[SyncHistoryAccountView] = []
        for account in snapshot.accounts:
            account_id, name = self._account_reference(
                connection_id, protocol, account.native_scope, account.account_id
            )
            accounts.append(
                SyncHistoryAccountView(
                    account_id=account_id,
                    display_name=name,
                    acknowledged_intervals=account.acknowledged_intervals,
                    history_target=account.history_target,
                )
            )
        return SyncHistoryView(
            observed_at=snapshot.observed_at,
            replay_required=snapshot.replay_required,
            discovery_history_target=snapshot.discovery_history_target,
            accounts=accounts,
        )

    def _public_sources(self, state: SyncJobState) -> list[SyncSourceView]:
        acknowledgement = state.details.acknowledgement
        confirmed = (
            state.details.result.ack_state == "confirmed"
            and acknowledgement is not None
        )
        sources = (
            acknowledgement.results
            if confirmed and acknowledgement is not None
            else [item.source for item in state.details.sources]
        )
        projected: list[SyncSourceView] = []
        for source in sources:
            simplefin = isinstance(source, SimpleFINSourceResult)
            protocol = source.protocol_version if simplefin else None
            history = (
                self._history_view(
                    source.history, source.connection_id, source.protocol_version
                )
                if simplefin
                else None
            )
            outcomes: list[SyncScopeView] = []
            local_source = next(
                (
                    item
                    for item in state.details.result.source_results
                    if item.connection_id == source.connection_id
                ),
                source,
            )
            for outcome in local_source.outcomes:
                account_id, name = (None, "Unresolved account")
                if (
                    simplefin
                    and outcome.account_id is not None
                    and outcome.native_scope is not None
                ):
                    account_id, name = self._account_reference(
                        source.connection_id,
                        source.protocol_version,
                        outcome.native_scope,
                        outcome.account_id,
                    )
                outcomes.append(
                    SyncScopeView(
                        account_id=account_id,
                        display_name=name,
                        scope_kind="account"
                        if outcome.account_id is not None
                        else "connection"
                        if outcome.native_scope is not None
                        else "general",
                        status=outcome.status,
                        error_code=_safe_error_code(outcome.error_code),
                        window=outcome.window,
                    )
                )
            projected.append(
                SyncSourceView(
                    provider=source.provider,
                    connection_id=str(source.connection_id),
                    credential_generation=source.credential_generation,
                    protocol_version=protocol,
                    history_phase="unknown"
                    if history is None
                    else "post_ack"
                    if confirmed
                    else "pre_ack",
                    history=history,
                    outcomes=outcomes,
                )
            )
        return projected

    @staticmethod
    def _save_recovery(repo: SyncJobStateRepo, state: SyncJobState) -> None:
        try:
            repo.save(state)
        except Exception:
            # The pending row committed with Raw still identifies the recoverable job.
            logger.warning(
                f"Sync recovery state update unavailable (job_id={state.job_id})"
            )

    def _repair_accounts(self, state: SyncJobState) -> bool:
        """Rebuild resolver inputs from durable native evidence after payload expiry."""
        accounts: list[SourceAccount] = []
        for reference in state.details.sources:
            source = reference.source
            if isinstance(source, SimpleFINSourceResult):
                receipt = self.db.execute(
                    f"""SELECT source_metadata, batch_ids FROM {SIMPLEFIN_SYNC_RECEIPTS.full_name}
                        WHERE job_id = ? AND connection_id = ?""",
                    [str(state.job_id), str(source.connection_id)],
                ).fetchone()
                if receipt is None:
                    raise ValueError("Durable SimpleFIN receipt missing")
                rows = self.db.execute(
                    f"""SELECT batch_id, fetched_at, window_start, window_end, original_payload
                        FROM {SIMPLEFIN_RESPONSES.full_name}
                        WHERE batch_id IN (SELECT UNNEST(?::UUID[])) ORDER BY batch_id""",
                    [json.loads(receipt[1])],
                ).fetchall()
                restored = SimpleFINSource.model_validate({
                    **json.loads(receipt[0]),
                    "batches": [
                        {
                            "batch_id": row[0],
                            "fetched_at": row[1].replace(tzinfo=UTC),
                            "window": {
                                "start": row[2].replace(tzinfo=UTC),
                                "end": row[3].replace(tzinfo=UTC),
                            },
                            "payload": json.loads(row[4]),
                        }
                        for row in rows
                    ],
                })
                if len(restored.batches) != len(reference.batch_ids):
                    raise ValueError("Durable SimpleFIN batch evidence missing")
                accounts.extend(SimpleFINExtractor(self.db).source_accounts(restored))
            else:
                rows = self.db.execute(
                    f"""SELECT account_id, source_origin, name, official_name, institution_name,
                               mask, persistent_account_id FROM {PLAID_ACCOUNTS.full_name}
                        WHERE source_origin IN (SELECT UNNEST(?::VARCHAR[])) ORDER BY source_origin, account_id""",
                    [reference.plaid_item_ids],
                ).fetchall()
                accounts.extend(
                    SourceAccount(
                        source_type="plaid",
                        source_origin=row[1],
                        source_account_key=row[0],
                        account_name=row[2]
                        or row[3]
                        or (f"{row[4]} account" if row[4] else row[0]),
                        account_name_is_user_set=bool(row[2]),
                        last_four=row[5],
                        institution=row[4],
                        persistent_token=row[6],
                    )
                    for row in rows
                )
        observations = MetricObservations()
        resolver = AccountResolver(self.db, actor="system", observations=observations)
        changed = False
        self.db.begin()
        try:
            for account in accounts:
                previous = resolver.accepted_native_account_id(account)
                resolved = resolver.resolve(account, in_outer_txn=True)
                observations.counter(
                    ACCOUNT_LINK_OUTCOMES_TOTAL, labels={"result": resolved.outcome}
                )
                changed = changed or previous is None
            self.db.commit()
        except BaseException:
            self.db.rollback()
            observations.flush("rollback")
            raise
        observations.flush()
        return changed

    def _investment_source_overlap(self) -> list[str]:
        """Canonical accounts with manual and Plaid transaction or holdings evidence."""
        try:
            return investment_source_overlap(self.db)
        except duckdb.CatalogException:  # tables may not exist on fresh DBs
            return []

    def _count_bootstrap_rows(self) -> int:
        """Count synthetic opening-lot bootstrap rows (spec: counted in the sync envelope).

        Cumulative across all history, not scoped to this pull — bootstrap
        writes once per (account, source_origin), so a later sync with zero
        new bootstrap activity still reports every lot seeded on any earlier
        pull. The CLI wording must read as a standing count, not a per-pull
        delta (see sync.py's "opening lot(s) seeded" line).
        """
        try:
            row = self.db.execute(
                f"SELECT COUNT(*) FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} "  # TableRef constant
                "WHERE subtype = 'opening_bootstrap'"
            ).fetchone()
        except duckdb.CatalogException:  # core view absent before first transform
            return 0
        return int(row[0]) if row else 0

    # ------------------------------ Link ------------------------------

    def initiate_link(
        self,
        *,
        institution: str | None = None,
        return_to: str | None = None,
        provider: str | None = None,
        connection_id: str | None = None,
    ) -> LinkInitiateResponse:
        """Start a provider-mediated browser link for a new or exact existing grant."""
        validate_pull_selectors(
            institution=institution, provider=provider, connection_id=connection_id
        )
        existing = (
            self._find_connection(connection_id)
            if connection_id
            else self._find_institution(institution)
            if institution
            else None
        )
        if connection_id and existing is None:
            raise UserError("Connection not found", code=error_codes.MUTATION_NOT_FOUND)
        if existing is not None:
            if provider is not None and provider != existing.provider:
                raise UserError(
                    "Provider does not match connection",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            provider, connection_id = existing.provider, existing.id
        if (provider or "plaid") == "plaid":
            return self.client.initiate_link(
                provider="plaid",
                provider_item_id=existing.provider_item_id
                if existing is not None
                else None,
                return_to=return_to,
            )
        return self.client.initiate_link(
            provider="simplefin", connection_id=connection_id
        )

    def link(
        self,
        *,
        institution: str | None = None,
        auto_pull: bool = True,
        provider: str | None = None,
        connection_id: str | None = None,
        return_to: str | None = None,
        on_initiate: Callable[[LinkInitiateResponse], None] | None = None,
    ) -> LinkResult:
        """Link new institution OR re-authenticate existing one.

        When `institution` matches an existing connection, runs Plaid update mode
        against that item. When it matches none, falls through to a new-connection
        request (per design Section 8); the server's Link flow handles naming.
        Ambiguous matches (same name on multiple connections) raise.

        `on_initiate` is invoked synchronously with the LinkInitiateResponse before
        the service starts polling. The CLI uses this hook to display `link_url`
        and optionally open the user's browser. Without it, the service blocks on
        polling without surfacing the URL — only safe for callers that surface it
        themselves (MCP returns the URL in its envelope and never enters this path).
        """
        initiate = self.initiate_link(
            institution=institution,
            return_to=return_to,
            provider=provider,
            connection_id=connection_id,
        )
        if on_initiate is not None:
            on_initiate(initiate)
        try:
            status = self.client.poll_link_status(initiate.session_id)
        except Exception:
            # poll_link_status raises SyncLinkError on terminal 'failed'
            # status, and SyncTimeoutError when the user abandons the browser.
            # Surface both as failed-link outcomes; the CLI/MCP layer
            # re-raises with the specific exception type.
            SYNC_CONNECT_OUTCOMES.labels(status="failed").inc()
            raise
        SYNC_CONNECT_OUTCOMES.labels(status=status.status or "linked").inc()
        pull_result: PullResult | None = None
        if auto_pull and status.status == "linked":
            try:
                pull_result = (
                    self.pull(provider="simplefin", connection_id=status.connection_id)
                    if status.provider == "simplefin" and status.connection_id
                    else self.pull(provider_item_id=status.provider_item_id)
                )
            except Exception:
                logger.warning("Auto-pull failed after link; inspect sync status")
        return LinkResult(
            status=status.status,
            action=status.action,
            provider=status.provider,
            connection_id=status.connection_id,
            provider_item_id=status.provider_item_id or "",
            institution_name=status.institution_name,
            pull_result=pull_result,
        )

    # ------------------------------ Status and disconnect ------------------------------

    def recovery_jobs(self) -> list[PullResult]:
        """Expose local repair obligations independently of current remote grants."""
        return [
            state.details.result
            for state in SyncJobStateRepo(self.db).for_profile(self.consumer_id)
            if state.details.result.ack_state in {"pending", "unknown"}
            or state.details.result.setup_state != "complete"
            or state.details.result.refresh_state != "complete"
            or state.details.result.inspection_error_code is not None
        ]

    def list_connections(self) -> list[SyncConnectionView]:
        """Keep credential status, provider timestamps, history and local phases separate."""
        states = SyncJobStateRepo(self.db).for_profile(self.consumer_id)
        result: list[SyncConnectionView] = []
        for item in self.client.list_institutions():
            history = (
                self._history_view(item.history, UUID(item.id), item.protocol_version)
                if item.provider == "simplefin" and item.protocol_version is not None
                else None
            )
            phase = next(
                (
                    state.details.result
                    for state in states
                    if any(
                        str(reference.source.connection_id) == item.id
                        and reference.source.credential_generation
                        == item.credential_generation
                        for reference in state.details.sources
                    )
                ),
                None,
            )
            balance_at: datetime | None = None
            if item.history is not None and item.protocol_version is not None:
                for account in item.history.accounts:
                    origin = source_origin(
                        UUID(item.id),
                        item.protocol_version,
                        account.native_scope.org.model_dump(
                            by_alias=True, exclude_none=True
                        )
                        if isinstance(account.native_scope, SimpleFINV1OrgScope)
                        else account.native_scope.model_dump(by_alias=True),
                    )
                    row = self.db.execute(
                        f"""SELECT MAX(balance_at) FROM {SIMPLEFIN_BALANCES.full_name} WHERE source_origin = ? AND account_id = ? AND validation_error IS NULL""",
                        [origin, account.account_id],
                    ).fetchone()  # trusted TableRef and bound native identity
                    if (
                        row
                        and row[0] is not None
                        and (balance_at is None or row[0] < balance_at)
                    ):
                        balance_at = row[0]
            result.append(
                SyncConnectionView(
                    **item.model_copy(
                        update={"error_code": _safe_error_code(item.error_code)}
                    ).model_dump(),
                    safe_history=history,
                    history_phase="current" if history is not None else "unknown",
                    provider_balance_at=balance_at,
                    setup_state=phase.setup_state if phase else "unknown",
                    refresh_state=phase.refresh_state if phase else "unknown",
                    guidance=self._guidance_for(
                        status=item.status,
                        error_code=item.error_code,
                        connection_id=item.id,
                        provider=item.provider,
                    ),
                )
            )
        return result

    def disconnect(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        connection_id: str | None = None,
    ) -> SyncDisconnectResult:
        """Resolve a target (name or exact item) to a connection and disconnect it."""
        inst = self.plan_disconnect(
            institution=institution,
            provider_item_id=provider_item_id,
            connection_id=connection_id,
        )
        response = self.client.disconnect(
            inst.id,
            credential_generation=inst.credential_generation,
            accounts=inst.accounts,
        )
        return SyncDisconnectResult(
            **inst.model_copy(update={"accounts": response.accounts}).model_dump(),
            remote_revocation=response.remote_revocation,
        )

    def plan_disconnect(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        connection_id: str | None = None,
    ) -> ConnectedInstitution:
        """Resolve the exact live connection that a disconnect targets.

        `provider_item_id` targets one connection exactly — set when an
        institution has more than one (e.g. after a relink), where
        `institution` alone is ambiguous and `_find_institution` refuses it.
        Mutually exclusive with `institution`, mirroring `pull()`'s guard.
        """
        if (
            sum(
                value is not None
                for value in (institution, provider_item_id, connection_id)
            )
            > 1
        ):
            raise UserError(
                "institution and provider_item_id are mutually exclusive — "
                "pass exactly one",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if connection_id is not None:
            connection = self._find_connection(connection_id)
            if connection is None:
                raise UserError(
                    "Connection not found", code=error_codes.MUTATION_NOT_FOUND
                )
            return connection
        if provider_item_id is not None:
            inst = self._find_institution_by_item(provider_item_id)
            if inst is None:
                raise UserError(
                    f"no connected institution with provider_item_id "
                    f"'{provider_item_id}' — run `moneybin sync status` to "
                    f"list connected banks",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
            return inst
        if institution is None:
            raise UserError(
                "institution or provider_item_id is required to disconnect — "
                "run `moneybin sync status` to list connected banks",
                code=error_codes.SYNC_INSTITUTION_REQUIRED,
            )
        inst = self._find_institution(institution)
        if inst is None:
            raise UserError(
                f"no connected institution matching '{institution}' — "
                f"run `moneybin sync status` to list connected banks",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        return inst

    def disconnect_confirmed(
        self,
        *,
        institution: str | None = None,
        provider_item_id: str | None = None,
        connection_id: str | None = None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> SyncDisconnectResult:
        """Verify the live target immediately before deleting the connection."""
        inst = self.plan_disconnect(
            institution=institution,
            provider_item_id=provider_item_id,
            connection_id=connection_id,
        )
        verify(inst)
        response = self.client.disconnect(
            inst.id,
            credential_generation=inst.credential_generation,
            accounts=inst.accounts,
        )
        return SyncDisconnectResult(
            **inst.model_copy(update={"accounts": response.accounts}).model_dump(),
            remote_revocation=response.remote_revocation,
        )

    def _guidance_for(
        self, *, status: str, error_code: str | None, connection_id: str, provider: str
    ) -> str | None:
        if status == "active":
            return None
        link = f"moneybin sync link --connection-id {connection_id}"
        if status == "revoked" or error_code in {
            "ITEM_NOT_FOUND",
            "authorization_revoked",
        }:
            return f"this connection was revoked. Inspect the grant, then use `{link}` for re-authentication."
        if status != "error":
            return None
        if error_code == "payment_required":
            return "this connection requires attention to the SimpleFIN Bridge subscription before another pull."
        if error_code in {"RATE_LIMIT_EXCEEDED", "rate_limited", "request_budget"}:
            return "this connection is rate limited. Wait before explicitly requesting another pull."
        if error_code in {
            "INSTITUTION_NOT_RESPONDING",
            "INSTITUTION_DOWN",
            "upstream_unavailable",
        }:
            return "this connection is temporarily unavailable. Try again later."
        if error_code in {"PRODUCTS_NOT_READY", "PRODUCT_NOT_READY"}:
            return "this connection is still processing initial data. Try again in a few minutes."
        if error_code == "PRODUCTS_NOT_SUPPORTED":
            return "this connection does not provide investment data; cash accounts can still sync."
        if provider == "simplefin" and error_code == "claim_outcome_unknown":
            return "The claim outcome is unknown. Disable the token in SimpleFIN Bridge before starting a new link; do not retry that claim."
        return f"this connection needs re-authentication or attention. Inspect it with `{link}`."

    # ------------------------------ Helpers ------------------------------

    def _find_institution(self, name: str) -> ConnectedInstitution | None:
        """Look up a connected institution by case-insensitive name match.

        Returns None when no connection matches (caller decides what to do).
        Raises UserError when multiple connections share the name — the name
        is ambiguous and must be disambiguated by the caller before any
        action runs.
        """
        institutions = self.client.list_institutions()
        matches = [
            inst
            for inst in institutions
            if inst.institution_name and inst.institution_name.lower() == name.lower()
        ]
        if len(matches) > 1:
            candidates = ", ".join(
                f"{item.id} (linked {item.created_at.date().isoformat()})"
                for item in matches
            )
            raise UserError(
                f"Multiple connections match this institution: {candidates}. Select one with --connection-id or connection_id.",
                code=error_codes.SYNC_INSTITUTION_AMBIGUOUS,
            )
        return matches[0] if matches else None

    def _find_connection(self, connection_id: str) -> ConnectedInstitution | None:
        """Select one tenant-owned broker grant by its UUID."""
        return next(
            (
                item
                for item in self.client.list_institutions()
                if item.id == connection_id
            ),
            None,
        )

    def _find_institution_by_item(
        self, provider_item_id: str
    ) -> ConnectedInstitution | None:
        """Look up a connected institution by its exact provider_item_id.

        Unambiguous by construction — unlike institution name, the server
        assigns one item id per connection, so this never raises.
        """
        institutions = self.client.list_institutions()
        for inst in institutions:
            if inst.provider_item_id == provider_item_id:
                return inst
        return None

    def _resolve_institution(self, name: str) -> str:
        """Map a human-readable institution name to its provider_item_id.

        Strict — raises if no connection matches. Reads from GET /institutions.
        """
        inst = self._find_institution(name)
        if inst is None:
            raise UserError(
                f"no connected institution matching '{name}' — "
                f"run `moneybin sync status` to list connected banks",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        if inst.provider_item_id is None:
            raise UserError(
                "Use the exact connection selector for this grant",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        return inst.provider_item_id
