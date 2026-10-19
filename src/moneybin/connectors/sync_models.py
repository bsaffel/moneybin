"""Pydantic models for SyncClient ↔ moneybin-sync API contract.

Single source of truth for request/response shapes. All server responses
validated at the boundary. Service-layer result types live here too so
related types live together.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal, Self, cast
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    model_validator,
)

# Runtime import, not TYPE_CHECKING: Pydantic resolves field types at class
# construction. Safe on the CLI's cold path because that module is stdlib-only
# by design — see its docstring.
from moneybin.services.refresh_outcome import RefreshStepOutcome
from moneybin.temporal import as_utc

UTCInstant = Annotated[AwareDatetime, AfterValidator(as_utc)]
_MAX_BROKER_CLOCK_SKEW = timedelta(minutes=5)


def _validate_job_time(value: datetime) -> datetime:
    if value > datetime.now(UTC) + _MAX_BROKER_CLOCK_SKEW:
        raise ValueError("Broker job time exceeds allowed clock skew")
    return value


BrokerJobInstant = Annotated[AwareDatetime, AfterValidator(_validate_job_time)]


def _blank_to_none(value: object) -> object:
    """An empty or whitespace-only string means absent, not ''."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


BlankToNone = Annotated[str | None, BeforeValidator(_blank_to_none)]
"""An optional wire string whose blanks normalise to None.

Nothing upstream promises a field the institution left empty arrives as JSON
null rather than ``""`` or ``"  "`` — ``prep.stg_plaid__accounts`` wraps every
free-text Plaid column in ``NULLIF(TRIM(...), '')`` for that reason, and
``SourceAccount`` canonicalises ``mask`` for it. A blank must not reach the
resolver truthy: ``persistent_token`` is a globally-scoped strong ref that
auto-adopts with no review, so one whitespace token shared by two institutions'
accounts merges their ledgers silently.
"""


def _removed_transaction_id(value: object) -> object:
    """A removed transaction arrives as a provider-native record, not an id."""
    if isinstance(value, dict):
        return cast(dict[str, object], value).get("transaction_id")
    return value


RemovedTransactionId = Annotated[str, BeforeValidator(_removed_transaction_id)]
"""One removed transaction's id, read from the provider-native record.

moneybin-sync widened this field from a bare id to a full record while the
client kept ``list[str]``, so every pull for an institution with removals
aborted at validation before anything was written — only institutions that
happened to have no removals still synced. Only the id is needed downstream:
it goes straight to ``handle_removed_transactions`` as a SQL parameter. A bare
string still validates, so the client can outlive a server that predates the
widening.
"""


CurrencyCode = BlankToNone
"""An optional ISO/unofficial currency code, with blanks normalised to None.

Plaid returns ``""`` for a currency it does not carry, and the two consumers of
these fields collapse the iso/unofficial pair differently: Python's ``or``
treats ``""`` as absent and falls through, while SQL ``COALESCE`` only falls
through on NULL and yields ``""``. Left unnormalised, one security's price row
stores ``'USD'`` while its own lot yields ``''``, the valuation join on
``quote_currency`` finds nothing, and the position reports ``unpriced`` with a
"no close resolved" warning while that exact close sits queryable in
``core.fct_security_prices``. Normalising at the boundary makes both sides
agree, so neither has to know about the other's fall-through rule.
"""

# ---- Server response models ----


class AuthToken(BaseModel):
    """Response from POST /auth/device/token and POST /auth/refresh."""

    access_token: str
    refresh_token: str
    expires_in: int = Field(gt=0)
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105  # literal constant, not a hardcoded password


class DeviceAuthorizationChallenge(BaseModel):
    """Safe user challenge plus the secret device code retained by the client."""

    device_code: SecretStr
    user_code: str
    verification_uri: str | None = None
    verification_uri_complete: str | None = None
    expires_in: int = Field(gt=0)
    interval: float = Field(default=5.0, ge=0)

    @model_validator(mode="after")
    def _require_verification_uri(self) -> Self:
        if self.verification_uri is None and self.verification_uri_complete is None:
            raise ValueError("A verification URI is required")
        return self


class LoginPollResult(BaseModel):
    """One nonblocking device-authorization poll outcome."""

    status: Literal["pending", "slow_down", "authenticated"]


class LinkInitiateResponse(BaseModel):
    """Response from POST /sync/link/initiate."""

    session_id: str = Field(min_length=1, max_length=128)
    link_url: str
    link_type: Literal["widget_flow", "token_paste"]
    expiration: UTCInstant


class LinkStatusResponse(BaseModel):
    """Response from GET /sync/link/status."""

    session_id: str
    status: Literal["pending", "claiming", "linked", "failed", "claim_outcome_unknown"]
    provider: Literal["plaid", "simplefin"] = "plaid"
    connection_id: str | None = None
    action: (
        Literal[
            "check_setup_token",
            "disable_token_and_relink",
            "resume_setup",
            "start_new_link",
        ]
        | None
    ) = None
    provider_item_id: str | None = None
    institution_name: str | None = None
    error: str | None = None
    expiration: UTCInstant


class SyncTriggerResponse(BaseModel):
    """Response from POST /sync/trigger (synchronous)."""

    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    transaction_count: int | None = None


class SyncAckResponse(BaseModel):
    """Response from POST /sync/ack."""

    job_id: str
    status: Literal["acked"]


class PlaidSyncAccount(BaseModel):
    """One account entry in GET /sync/data response.

    Every field the broker sends must be declared here. Pydantic's default
    ``extra='ignore'`` destroys an undeclared key at validation with no error
    and no log line, so a field this model forgets is indistinguishable from
    one the broker never sent. That is how ``persistent_account_id`` — the key
    that survives a Plaid relink — reached nothing downstream between the first
    Plaid sync release (#134) and the change that declared it here.
    """

    account_id: str
    persistent_account_id: BlankToNone = None
    account_type: str | None = None
    account_subtype: str | None = None
    institution_name: BlankToNone = None
    name: BlankToNone = None
    official_name: BlankToNone = None
    mask: str | None = Field(default=None, max_length=8)


class PlaidSyncTransaction(BaseModel):
    """One transaction in GET /sync/data response.

    NOTE: amount preserves Plaid convention (positive = expense). The sign
    flip happens in prep.stg_plaid__transactions, NOT here, NOT in the loader.
    Every field below ``pending`` is Plaid's additional default-returned data
    (location/PFC flattened to scalars by the broker); each is optional so a
    broker that predates the capture change still validates.
    """

    transaction_id: str
    account_id: str
    transaction_date: date
    amount: Decimal
    description: str | None = None
    merchant_name: str | None = None
    category: str | None = None
    pending: bool = False
    original_description: str | None = None
    iso_currency_code: CurrencyCode = None
    authorized_date: date | None = None
    pending_transaction_id: str | None = None
    payment_channel: str | None = None
    check_number: str | None = None
    merchant_entity_id: str | None = None
    location_address: str | None = None
    location_city: str | None = None
    location_region: str | None = None
    location_postal_code: str | None = None
    location_country: str | None = None
    location_latitude: float | None = None
    location_longitude: float | None = None
    category_detailed: str | None = None
    category_confidence: str | None = None


class PlaidSyncBalance(BaseModel):
    """One balance snapshot in GET /sync/data response.

    Every field the broker sends must be declared here, for the reason
    :class:`PlaidSyncAccount` states above. ``margin_loan_amount`` is the one that
    carries money: Plaid reports an investment account's ``current_balance`` as
    the total value of *assets*, with the funds borrowed against them in a
    separate field, so ``core.fct_balances`` has to net the two. While the field
    went undeclared a margin account overstated net worth by its whole loan.

    ``balance_limit`` renames the wire's ``limit`` — a SQL reserved word, and a
    different fact from ``app.account_settings.credit_limit``, which is the
    limit the *user* asserted rather than the one the institution reports.
    """

    model_config = ConfigDict(populate_by_name=True)

    account_id: str
    balance_date: date
    current_balance: Decimal | None = None
    available_balance: Decimal | None = None
    balance_limit: Decimal | None = Field(default=None, alias="limit")
    margin_loan_amount: Decimal | None = None
    iso_currency_code: CurrencyCode = None
    unofficial_currency_code: CurrencyCode = None
    last_updated_datetime: UTCInstant | None = None


class SyncSecurity(BaseModel):
    """One security in GET /sync/data response (Plaid Security, item-scoped).

    Field names match the raw.plaid_securities columns; wire names arrive via
    aliases so model_dump() feeds the loader without renaming.
    """

    model_config = ConfigDict(populate_by_name=True)

    security_id: str
    provider_item_id: str
    institution_security_id: str | None = None
    institution_id: str | None = None
    ticker_symbol: str | None = None
    market_identifier_code: str | None = None
    security_name: str | None = Field(default=None, alias="name")
    security_type: str | None = Field(default=None, alias="type")
    close_price: Decimal | None = None
    close_price_as_of: date | None = None
    iso_currency_code: CurrencyCode = None
    unofficial_currency_code: CurrencyCode = None
    cusip: str | None = None
    isin: str | None = None
    is_cash_equivalent: bool | None = None


class SyncInvestmentTransaction(BaseModel):
    """One investment ledger event in GET /sync/data response.

    CAUTION: amount keeps Plaid's convention (positive = cash out); the sign
    flip happens only in prep.stg_plaid__investment_transactions.
    """

    model_config = ConfigDict(populate_by_name=True)

    investment_transaction_id: str
    account_id: str
    provider_item_id: str
    security_id: str | None = None
    transaction_date: date = Field(alias="date")
    transaction_datetime: UTCInstant | None = None
    transaction_name: str | None = Field(default=None, alias="name")
    quantity: Decimal | None = None
    amount: Decimal
    price: Decimal | None = None
    fees: Decimal | None = None
    iso_currency_code: CurrencyCode = None
    unofficial_currency_code: CurrencyCode = None
    investment_transaction_type: str | None = Field(default=None, alias="type")
    investment_transaction_subtype: str | None = Field(default=None, alias="subtype")


class SyncHoldingTaxLot(BaseModel):
    """One HoldingTaxLot entry inside SyncHolding.tax_lots[]."""

    model_config = ConfigDict(populate_by_name=True)

    institution_lot_id: str | None = None
    original_purchase_datetime: UTCInstant | None = None
    quantity: Decimal | None = None
    purchase_price: Decimal | None = None
    cost_basis: Decimal | None = None
    current_value: Decimal | None = None
    position_type: str | None = None


class SyncHolding(BaseModel):
    """One holdings-snapshot position in GET /sync/data response."""

    model_config = ConfigDict(populate_by_name=True)

    account_id: str
    provider_item_id: str
    security_id: str
    institution_price: Decimal | None = None
    institution_price_as_of: date | None = None
    institution_value: Decimal | None = None
    cost_basis: Decimal | None = None
    quantity: Decimal | None = None
    iso_currency_code: CurrencyCode = None
    unofficial_currency_code: CurrencyCode = None
    vested_quantity: Decimal | None = None
    vested_value: Decimal | None = None
    tax_lots: list[SyncHoldingTaxLot] = Field(default_factory=list)


class InstitutionResult(BaseModel):
    """Per-institution result inside SyncMetadata.institutions[]."""

    provider_item_id: str
    institution_name: str | None = None
    status: Literal["completed", "failed"]
    transaction_count: int | None = None
    error: str | None = None
    error_code: str | None = None
    transactions_window_start: date | None = Field(
        default=None,
        description=(
            "ISO date the server used as the /investments/transactions/get start "
            "boundary for THIS item; required whenever the item has holdings rows."
        ),
    )


class SyncMetadata(BaseModel):
    """metadata block in GET /sync/data response."""

    job_id: str
    synced_at: UTCInstant
    institutions: list[InstitutionResult]

    @model_validator(mode="after")
    def _validate_sync_time(self) -> Self:
        if self.synced_at > datetime.now(UTC) + _MAX_BROKER_CLOCK_SKEW:
            raise ValueError("Broker sync time exceeds allowed clock skew")
        return self


class PlaidSyncDataResponse(BaseModel):
    """Full response shape from GET /sync/data."""

    accounts: list[PlaidSyncAccount]
    transactions: list[PlaidSyncTransaction]
    balances: list[PlaidSyncBalance]
    removed_transactions: list[RemovedTransactionId]
    securities: list[SyncSecurity] = Field(default_factory=list)
    investment_transactions: list[SyncInvestmentTransaction] = Field(
        default_factory=list
    )
    investment_holdings: list[SyncHolding] = Field(default_factory=list)
    metadata: SyncMetadata


JobTerminalStatus = Literal["completed", "partial", "failed", "deferred"]


class _ContractModel(BaseModel):
    """Reject undeclared broker fields instead of silently dropping them."""

    model_config = ConfigDict(extra="forbid", serialize_by_alias=True)


class UTCWindow(_ContractModel):
    """Requested half-open UTC interval."""

    start: AwareDatetime
    end: AwareDatetime

    @model_validator(mode="after")
    def _validate_utc_window(self) -> Self:
        if self.start.utcoffset() != timedelta(0) or self.end.utcoffset() != timedelta(
            0
        ):
            raise ValueError("Window boundaries must be UTC")
        if self.start >= self.end:
            raise ValueError("Window end must be after start")
        return self


class ProviderBatch(_ContractModel):
    """One immutable provider-native result for a requested window."""

    batch_id: UUID
    fetched_at: AwareDatetime
    window: UTCWindow | None
    payload: dict[str, object]

    @model_validator(mode="after")
    def _validate_fetch_time(self) -> Self:
        # Bound broker clock skew without expiring historical replay evidence.
        if self.fetched_at > datetime.now(UTC) + _MAX_BROKER_CLOCK_SKEW:
            raise ValueError("Broker fetch time exceeds allowed clock skew")
        return self


class PlaidItemScope(_ContractModel):
    """Native Plaid item containing an account outcome."""

    kind: Literal["plaid_item"]
    provider_item_id: str = Field(min_length=1)


class SimpleFINV1Organization(_ContractModel):
    """Version-1 organization fields retained as native identity evidence."""

    sfin_url: str = Field(alias="sfin-url", min_length=1)
    id: str | None = None
    domain: str | None = None
    name: str | None = None
    url: str | None = None

    @model_validator(mode="after")
    def _require_label(self) -> Self:
        if not self.domain and not self.name:
            raise ValueError("Organization requires domain or name")
        return self


class SimpleFINV1OrgScope(_ContractModel):
    """Version-1 organization containing an account outcome."""

    kind: Literal["simplefin_v1_org"]
    org: SimpleFINV1Organization


class SimpleFINV2ConnectionScope(_ContractModel):
    """Version-2 upstream connection containing an account outcome."""

    kind: Literal["simplefin_v2_connection"]
    conn_id: str = Field(min_length=1)


NativeScope = Annotated[
    PlaidItemScope | SimpleFINV1OrgScope | SimpleFINV2ConnectionScope,
    Field(discriminator="kind"),
]


class ScopeOutcome(_ContractModel):
    """Retrieval result for one native account or an unscoped error."""

    account_id: str | None
    native_scope: NativeScope | None
    window: UTCWindow | None
    status: Literal["complete", "partial", "failed", "deferred"]
    error_code: str | None
    retry_at: AwareDatetime | None = None


class PlaidSource(_ContractModel):
    """Plaid source with its existing provider-native wire values."""

    provider: Literal["plaid"]
    connection_id: UUID
    credential_generation: int = Field(gt=0)
    batches: list[ProviderBatch]
    outcomes: list[ScopeOutcome]

    @model_validator(mode="after")
    def _validate_outcome_scopes(self) -> Self:
        if any(item.window is not None for item in [*self.batches, *self.outcomes]):
            raise ValueError("Plaid results require a null window")
        for outcome in self.outcomes:
            scope = outcome.native_scope
            if scope is None and outcome.account_id is not None:
                raise ValueError("Account outcome requires native scope")
            if scope is not None and not isinstance(scope, PlaidItemScope):
                raise ValueError("Plaid outcome requires Plaid item scope")
        return self


class SimpleFINAccountHistory(_ContractModel):
    """Broker-acknowledged complete-day intervals and remaining older intent."""

    account_id: str = Field(min_length=1)
    native_scope: Annotated[
        SimpleFINV1OrgScope | SimpleFINV2ConnectionScope, Field(discriminator="kind")
    ]
    acknowledged_intervals: list[UTCWindow] = Field(max_length=2)
    history_target: date | None

    def has_acknowledged_through(self, end: datetime) -> bool:
        """Forward credit need not include incidental windows for other accounts."""
        return any(interval.end >= end for interval in self.acknowledged_intervals)

    @model_validator(mode="after")
    def _validate_intervals(self) -> Self:
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        previous = None
        for interval in self.acknowledged_intervals:
            if any(
                (point.hour, point.minute, point.second, point.microsecond)
                != (0, 0, 0, 0)
                for point in (interval.start, interval.end)
            ):
                raise ValueError("Acknowledged intervals require complete UTC days")
            if interval.end > today:
                raise ValueError("Acknowledged intervals cannot extend beyond today")
            if previous is not None and interval.start <= previous:
                raise ValueError("Acknowledged intervals must be ordered and disjoint")
            previous = interval.end
        return self


class SimpleFINHistorySnapshot(_ContractModel):
    """Last observed broker credit; absence of a target does not certify completeness."""

    observed_at: AwareDatetime
    replay_required: bool
    discovery_history_target: date | None
    accounts: list[SimpleFINAccountHistory]

    @model_validator(mode="after")
    def _validate_accounts(self) -> Self:
        if self.observed_at.utcoffset() != timedelta(0):
            raise ValueError("History observation time must be UTC")
        if self.observed_at > datetime.now(UTC) + _MAX_BROKER_CLOCK_SKEW:
            raise ValueError("History observation time exceeds allowed clock skew")
        keys = [
            (account.native_scope.model_dump_json(), account.account_id)
            for account in self.accounts
        ]
        if len(keys) != len(set(keys)):
            raise ValueError("History requires unique scoped accounts")
        return self


class SimpleFINSource(_ContractModel):
    """SimpleFIN source with its pinned upstream protocol version."""

    provider: Literal["simplefin"]
    connection_id: UUID
    credential_generation: int = Field(gt=0)
    protocol_version: Literal["1.0.7", "2.0.0-draft"]
    batches: list[ProviderBatch]
    outcomes: list[ScopeOutcome]
    history: SimpleFINHistorySnapshot | None = None

    @model_validator(mode="after")
    def _validate_outcome_scopes(self) -> Self:
        if any(item.window is None for item in [*self.batches, *self.outcomes]):
            raise ValueError("SimpleFIN results require a requested window")
        scope_type = (
            SimpleFINV1OrgScope
            if self.protocol_version == "1.0.7"
            else SimpleFINV2ConnectionScope
        )
        if self.history is not None and any(
            account.native_scope.kind
            != (
                "simplefin_v1_org"
                if self.protocol_version == "1.0.7"
                else "simplefin_v2_connection"
            )
            for account in self.history.accounts
        ):
            raise ValueError("SimpleFIN history scope does not match protocol")
        for outcome in self.outcomes:
            scope = outcome.native_scope
            if scope is None and outcome.account_id is not None:
                raise ValueError("Account outcome requires native scope")
            if scope is not None and not isinstance(scope, scope_type):
                raise ValueError("SimpleFIN outcome scope does not match protocol")
        return self


ProviderSource = Annotated[
    PlaidSource | SimpleFINSource, Field(discriminator="provider")
]


class SyncEnvelopeV2(_ContractModel):
    """Completed broker result, independent of upstream protocol version."""

    job_id: UUID
    contract_version: Literal[2]
    status: JobTerminalStatus
    sources: list[ProviderSource]


class SyncAccountScope(BaseModel):
    """Native account scope currently discovered under a broker credential grant."""

    account_id: str
    native_scope: NativeScope
    name: str

    @property
    def identity(self) -> tuple[str, ...]:
        """Native account identity, independent of display labels and order."""
        scope = self.native_scope
        if isinstance(scope, SimpleFINV1OrgScope):
            org = scope.org
            kind, value = (
                ("id", org.id)
                if org.id
                else ("domain", org.domain)
                if org.domain
                else ("name", org.name)
            )
            return (scope.kind, org.sfin_url, kind, value or "", self.account_id)
        if isinstance(scope, SimpleFINV2ConnectionScope):
            return (scope.kind, scope.conn_id, self.account_id)
        return (scope.kind, scope.provider_item_id, self.account_id)


class SyncHistoryAccountView(BaseModel):
    """Coverage for an accepted local account, or an explicitly unresolved scope."""

    account_id: str | None
    display_name: str
    acknowledged_intervals: list[UTCWindow]
    history_target: date | None


class SyncHistoryView(BaseModel):
    """Safe observed history; absence and pending intent remain distinct."""

    observed_at: UTCInstant
    replay_required: bool
    discovery_history_target: date | None
    accounts: list[SyncHistoryAccountView]


class SyncScopeView(BaseModel):
    """A retrieval scope using local account identity, never provider-native text."""

    account_id: str | None
    display_name: str
    scope_kind: Literal["account", "connection", "general"]
    status: str
    error_code: str | None
    window: UTCWindow | None
    retry_at: UTCInstant | None = None


class SyncSourceView(BaseModel):
    """One broker source's retrieval and independently confirmed history."""

    provider: str
    connection_id: str
    credential_generation: int
    protocol_version: str | None
    history_phase: Literal["unknown", "pre_ack", "post_ack", "current"]
    history: SyncHistoryView | None
    outcomes: list[SyncScopeView] = Field(default_factory=list)


class ConnectedInstitution(BaseModel):
    """One entry in GET /institutions response."""

    id: str
    provider_item_id: str | None
    provider: str
    institution_name: str | None = None
    status: Literal["active", "error", "revoked"]
    last_sync: UTCInstant | None = None
    created_at: UTCInstant
    error_code: str | None = Field(
        default=None,
        description="Provider error code (e.g. ITEM_LOGIN_REQUIRED). Advisory — treat as None when absent.",
    )

    credential_generation: int = Field(default=1, gt=0)
    protocol_version: str | None = None
    accounts: list[SyncAccountScope] = Field(default_factory=list)
    history: SimpleFINHistorySnapshot | None = None
    history_unavailable_reason: Literal["unbound", "consumer_mismatch"] | None = None


# ---- Service-layer result types ----


class SyncDisconnectResponse(BaseModel):
    """Broker custody removal and the remaining upstream revocation obligation."""

    connection_id: UUID
    credential_generation: int = Field(gt=0)
    accounts: list[SyncAccountScope]
    remote_revocation: Literal["revoked", "user_action_required"]


class SyncDisconnectResult(ConnectedInstitution):
    """The selected grant and the broker's actual revocation outcome."""

    remote_revocation: Literal["revoked", "user_action_required"]


class PullResult(BaseModel):
    """Return value from SyncService.pull()."""

    job_id: str
    job_status: JobTerminalStatus = "completed"
    ingestion_state: Literal["committed"] = "committed"
    ack_state: Literal["pending", "confirmed", "unknown", "unavailable"] = "pending"
    ack_error_code: str | None = None
    setup_state: Literal["pending", "complete", "failed"] = "pending"
    setup_error_code: str | None = None
    inspection_error_code: Literal["local_inspection_failed"] | None = None
    refresh_state: Literal["pending", "deferred", "complete", "failed"] = "pending"
    source_results: list[ProviderSourceResult] = Field(default_factory=list)
    sources: list[SyncSourceView] = Field(default_factory=list)
    transactions_loaded: int
    accounts_loaded: int
    balances_loaded: int
    transactions_removed: int
    securities_loaded: int = 0
    investment_transactions_loaded: int = 0
    holdings_loaded: int = 0
    holding_lots_loaded: int = 0
    security_prices_loaded: int = 0
    institutions: list[InstitutionResult]
    transforms_applied: bool = False
    transforms_duration_seconds: float | None = None
    transforms_error: str | None = None
    # Not a transform statistic like the three above: a pull runs the full
    # refresh cascade, so its match step can reverse a transfer the user had
    # accepted. That is the one outcome here the user did not ask for, so it
    # rides back with the rest rather than living only in the audit log.
    transfers_retired: int = 0
    # Same reason as the field above, widened: a pull runs the whole cascade,
    # so its matcher, categorizer, identity pass and network rate backfill all
    # act on the user's behalf and can each fail on their own. `transforms_error`
    # covers only a blocking failure (SQLMesh apply, or an investment_match
    # crash that kept apply from running), so without this the other four
    # best-effort steps are silent.
    # None means no refresh ran, distinct from one that ran clean.
    refresh_steps: RefreshStepOutcome | None = None
    opening_bootstrap_rows: int = 0
    investment_source_overlap_accounts: list[str] = Field(default_factory=list)
    security_resolution: dict[str, int] = Field(default_factory=dict)
    security_resolution_error: str | None = None

    @property
    def complete(self) -> bool:
        """Whether retrieval, acknowledgement and requested local work are complete."""
        return (
            bool(self.source_results)
            and all(source.outcomes for source in self.source_results)
            and self.inspection_error_code is None
            and self.job_status == "completed"
            and self.ack_state == "confirmed"
            and self.setup_state == "complete"
            and self.refresh_state in {"complete", "deferred"}
            and not self.transforms_error
            and not self.security_resolution_error
            and not (self.refresh_steps and self.refresh_steps.has_failure)
            and {
                (
                    source.connection_id,
                    source.credential_generation,
                    source.protocol_version,
                )
                for source in self.sources
                if source.provider == "simplefin"
            }
            == {
                (
                    str(source.connection_id),
                    source.credential_generation,
                    source.protocol_version,
                )
                for source in self.source_results
                if isinstance(source, SimpleFINSourceResult)
            }
            and all(
                source.provider != "simplefin"
                or (
                    source.history is not None
                    and not source.history.replay_required
                    and source.history.discovery_history_target is None
                    and bool(source.history.accounts)
                    and all(
                        account.account_id is not None
                        and account.history_target is None
                        and len(account.acknowledged_intervals) == 1
                        for account in source.history.accounts
                    )
                    and any(
                        isinstance(native, SimpleFINSourceResult)
                        and str(native.connection_id) == source.connection_id
                        and native.credential_generation == source.credential_generation
                        and native.protocol_version == source.protocol_version
                        and native.outcomes_cover_history
                        for native in self.source_results
                    )
                )
                for source in self.sources
            )
            and all(item.status != "failed" for item in self.institutions)
            and all(
                outcome.status == "complete"
                for source in self.source_results
                for outcome in source.outcomes
            )
        )


class LinkResult(BaseModel):
    """Return value from SyncService.link()."""

    status: Literal[
        "pending", "claiming", "linked", "failed", "claim_outcome_unknown"
    ] = "linked"
    action: str | None = None
    provider: str = "plaid"
    connection_id: str | None = None
    provider_item_id: str
    institution_name: str | None = None
    pull_result: PullResult | None = None


class SyncConnectionView(ConnectedInstitution):
    """Current broker grant with safe client guidance."""

    guidance: str | None = None
    safe_history: SyncHistoryView | None = None
    history_phase: Literal["unknown", "current"] = "unknown"
    provider_balance_at: UTCInstant | None = None
    setup_state: str = "unknown"
    refresh_state: str = "unknown"


class PlaidSourceResult(_ContractModel):
    """Durable source outcome without its expiring native batch payload."""

    provider: Literal["plaid"]
    connection_id: UUID
    credential_generation: int = Field(gt=0)
    outcomes: list[ScopeOutcome]


class SimpleFINSourceResult(_ContractModel):
    """Durable SimpleFIN outcome and the history observed before or after ack."""

    provider: Literal["simplefin"]
    connection_id: UUID
    credential_generation: int = Field(gt=0)
    protocol_version: Literal["1.0.7", "2.0.0-draft"]
    outcomes: list[ScopeOutcome]
    history: SimpleFINHistorySnapshot | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> Self:
        SimpleFINSource.model_validate({**self.model_dump(), "batches": []})
        return self

    @property
    def outcomes_cover_history(self) -> bool:
        """Every known native account needs retrieval evidence, even after merging."""
        if self.history is None or not self.history.accounts:
            return False
        expected = {
            SyncAccountScope(
                account_id=account.account_id,
                native_scope=account.native_scope,
                name="",
            ).identity: account
            for account in self.history.accounts
        }
        observed: set[tuple[str, ...]] = set()
        for outcome in self.outcomes:
            if (
                outcome.status != "complete"
                or outcome.account_id is None
                or outcome.native_scope is None
            ):
                continue
            identity = SyncAccountScope(
                account_id=outcome.account_id,
                native_scope=outcome.native_scope,
                name="",
            ).identity
            account = expected.get(identity)
            if (
                account is None
                or outcome.window is None
                or not account.has_acknowledged_through(outcome.window.end)
            ):
                return False
            observed.add(identity)
        return expected.keys() <= observed


ProviderSourceResult = Annotated[
    PlaidSourceResult | SimpleFINSourceResult, Field(discriminator="provider")
]


class SyncTriggerV2Response(_ContractModel):
    """Explicit broker v2 negotiation, independent of provider protocol."""

    job_id: UUID
    contract_version: Literal[2]
    status: JobTerminalStatus
    transaction_count: int


class SyncStatusV2Response(_ContractModel):
    """Durable authenticated job metadata available after held-data expiry."""

    job_id: UUID
    contract_version: Literal[2]
    status: Literal["pending", "running", "completed", "partial", "failed", "deferred"]
    transaction_count: int
    error: str | None
    started_at: BrokerJobInstant | None
    completed_at: BrokerJobInstant | None
    acked_at: UTCInstant | None
    results: list[ProviderSourceResult]


class SyncAckV2Response(_ContractModel):
    """Stored post-ack evidence, shared with the durable status result."""

    job_id: UUID
    contract_version: Literal[2]
    status: Literal["acked"]
    acked_at: UTCInstant
    results: list[ProviderSourceResult]


class SyncJobSource(BaseModel):
    """Durable references needed to resume local setup without a held payload."""

    source: ProviderSourceResult
    batch_ids: list[UUID]
    plaid_item_ids: list[str] = Field(default_factory=list)


class SyncJobDetails(BaseModel):
    """Protected local operational evidence; source-native records stay in Raw."""

    sources: list[SyncJobSource]
    result: PullResult
    acknowledgement: SyncAckV2Response | None = None


PullResult.model_rebuild()
