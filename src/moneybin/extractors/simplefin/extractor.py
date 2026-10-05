"""Append immutable observations inside the caller's complete job transaction."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any, Self, cast
from uuid import UUID

import polars as pl
from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator

from moneybin.connectors.sync_models import (
    JobTerminalStatus,
    ProviderBatch,
    ScopeOutcome,
    SimpleFINSource,
    SimpleFINV1Organization,
    SimpleFINV1OrgScope,
    SimpleFINV2ConnectionScope,
    UTCWindow,
)
from moneybin.database import Database
from moneybin.extractors.account_identity import (
    UNNAMED_ACCOUNT_LABEL,
    AccountNameFacts,
    SourceAccount,
    mask_embedded_account_number,
)
from moneybin.extractors.simplefin.identity import (
    scope_identity_strength,
    source_origin,
    transaction_reference,
)
from moneybin.tables import (
    SIMPLEFIN_ACCOUNTS,
    SIMPLEFIN_BALANCES,
    SIMPLEFIN_RESPONSES,
    SIMPLEFIN_SYNC_RECEIPTS,
    SIMPLEFIN_TRANSACTIONS,
    TableRef,
)


class SimpleFINJobContext(BaseModel):
    """Authenticated status metadata for the already verified job identity."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    status: JobTerminalStatus
    started_at: AwareDatetime
    completed_at: AwareDatetime

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("Job completion precedes start")
        return self


@dataclass(frozen=True)
class SimpleFINLoadResult:
    """Newly persisted row counts and safe scope outcomes; never an ack."""

    accounts: int
    transactions: int
    balances: int
    batch_ids: list[UUID]
    outcomes: list[ScopeOutcome]


def _json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _revision(value: object) -> str:
    return sha256(_json(value).encode()).hexdigest()


def _money(value: object) -> tuple[Decimal | None, str | None]:
    if not isinstance(value, str):
        return None, "invalid_amount"
    try:
        amount = Decimal(value)
        if not amount.is_finite():
            return None, "invalid_amount"
        if amount.copy_abs() >= Decimal(
            "10000000000000000"
        ) or amount != amount.quantize(Decimal("0.01")):
            return None, "unsupported_precision"
        return amount, None
    except InvalidOperation:
        return None, "invalid_amount"


def _instant(
    value: object, fetched_at: datetime, *, posted: bool = False
) -> tuple[datetime | None, str | None]:
    if not isinstance(value, int) or isinstance(value, bool):
        return None, "invalid_timestamp"
    if posted and value <= 0:
        return None, "nonpositive_posted"
    try:
        timestamp = datetime.fromtimestamp(value, UTC)
    except (ValueError, OverflowError, OSError):
        return None, "invalid_timestamp"
    if timestamp > fetched_at:
        return None, "future_timestamp"
    return timestamp, None


def _validation_error(*errors: str | None) -> str | None:
    """Structural failures dominate validated but financially ineligible values."""
    holds = {"unsupported_precision", "pending", "nonpositive_posted"}
    return next(
        (error for error in errors if error and error not in holds),
        next((error for error in errors if error), None),
    )


def _record(value: object) -> dict[str, Any] | None:
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _institution_label(native: dict[str, Any], payload: dict[str, Any]) -> str | None:
    institution = _text(native.get("name"))
    if native.get("conn_id"):
        value = payload.get("connections")
        connections = cast(list[object], value) if isinstance(value, list) else []
        institutions: set[str] = set()
        for value in connections:
            item = _record(value)
            if item is None or item.get("conn_id") != native["conn_id"]:
                continue
            name = _text(item.get("name"))
            if name is not None:
                institutions.add(name)
        institution = next(iter(institutions)) if len(institutions) == 1 else None
    institution = institution.strip() if institution else None
    return mask_embedded_account_number(institution) if institution else None


class SimpleFINExtractor:
    """Retain batch evidence losslessly without controlling transaction lifetime."""

    def __init__(self, db: Database) -> None:
        """Use the caller-owned warehouse connection."""
        self.db = db

    def source_accounts(self, source: SimpleFINSource) -> list[SourceAccount]:
        """Build resolver inputs from retained scopes touched by this delivery."""
        if not source.batches:
            return []
        rows = self.db.execute(
            f"""SELECT a.source_origin, a.account_id, a.name, a.native_scope,
                       r.original_payload, a.source_file
                FROM {SIMPLEFIN_ACCOUNTS.full_name} AS a
                JOIN {SIMPLEFIN_RESPONSES.full_name} AS r USING (batch_id)
                WHERE (a.source_origin, a.account_id) IN (
                    SELECT source_origin, account_id
                    FROM {SIMPLEFIN_ACCOUNTS.full_name}
                    WHERE batch_id IN (SELECT UNNEST(?::UUID[]))
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY a.source_origin, a.account_id
                    ORDER BY a.fetched_at DESC, a.batch_id DESC,
                             a.observation_version DESC
                ) = 1
                ORDER BY a.source_origin, a.account_id""",  # noqa: S608  # trusted TableRef constants, bound batch IDs
            [[str(batch.batch_id) for batch in source.batches]],
        ).fetchall()
        accounts: list[SourceAccount] = []
        for origin, native_id, name, native_json, payload_json, source_file in rows:
            native = json.loads(native_json)
            institution = _institution_label(native, json.loads(payload_json))
            label = mask_embedded_account_number(name.strip()) if name else None
            accounts.append(
                SourceAccount(
                    source_type="simplefin",
                    source_origin=origin,
                    source_account_key=native_id,
                    account_name=label or UNNAMED_ACCOUNT_LABEL,
                    account_name_is_user_set=bool(label),
                    institution=institution,
                    source_file=source_file,
                    name_facts=AccountNameFacts(
                        source_label=label,
                        institution_name=institution,
                    ),
                )
            )
        return accounts

    def _write(self, table: TableRef, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        overrides = {
            name: pl.Decimal(18, 2)
            for name in ("amount", "balance", "available_balance")
            if name in rows[0]
        }
        return self.db.ingest_dataframe(
            table.full_name,
            pl.from_dicts(rows, schema_overrides=overrides, infer_schema_length=None),
            on_conflict="ignore",
        )

    def _check_immutable(
        self, table: TableRef, keys: dict[str, str], digest: str
    ) -> None:
        predicate = " AND ".join(f"{key} = ?" for key in keys)
        row = self.db.execute(
            f"SELECT content_digest FROM {table.full_name} WHERE {predicate}",  # noqa: S608  # internal registry and fixed caller keys
            list(keys.values()),
        ).fetchone()
        if row is not None and row[0] != digest:
            raise ValueError("Conflicting immutable SimpleFIN receipt")

    def load(
        self, source: SimpleFINSource, *, job_id: UUID, job_context: SimpleFINJobContext
    ) -> SimpleFINLoadResult:
        """Validate first, then append within Task 10's surrounding transaction."""
        # Revalidate mutable model instances at the ingestion boundary.
        source = SimpleFINSource.model_validate(source.model_dump(mode="json"))
        job_context = SimpleFINJobContext.model_validate(job_context.model_dump())
        outcomes: list[ScopeOutcome] = []
        accounts: list[dict[str, Any]] = []
        transactions: list[dict[str, Any]] = []
        balances: list[dict[str, Any]] = []
        responses: list[dict[str, Any]] = []
        seen_batches: dict[UUID, str] = {}
        for batch in source.batches:
            digest = _revision({
                "connection_id": str(source.connection_id),
                "credential_generation": source.credential_generation,
                "protocol_version": source.protocol_version,
                "batch": batch.model_dump(mode="json"),
            })
            if batch.batch_id in seen_batches:
                if seen_batches[batch.batch_id] != digest:
                    raise ValueError("Conflicting immutable SimpleFIN batch")
                continue
            seen_batches[batch.batch_id] = digest
            self._check_immutable(
                SIMPLEFIN_RESPONSES, {"batch_id": str(batch.batch_id)}, digest
            )
            window = cast(UTCWindow, batch.window)
            payload = batch.payload
            native_accounts = payload.get("accounts")
            if not isinstance(native_accounts, list):
                raise ValueError("Invalid SimpleFIN Account Set")
            responses.append({
                "batch_id": str(batch.batch_id),
                "connection_id": str(source.connection_id),
                "credential_generation": source.credential_generation,
                "protocol_version": source.protocol_version,
                "fetched_at": batch.fetched_at,
                "window_start": window.start,
                "window_end": window.end,
                "original_payload": _json(payload),
                "source_errors": _json({
                    key: payload[key] for key in ("errors", "errlist") if key in payload
                }),
                "content_digest": digest,
                "source_file": f"sync_{job_id}",
            })
            records = [_record(item) for item in cast(list[object], native_accounts)]
            identified = [self._account_identity(source, item) for item in records]
            counts = Counter(
                (item[0], item[1]) for item in identified if item is not None
            )
            for record, identity in zip(records, identified, strict=True):
                if identity is None:
                    outcomes.append(
                        self._outcome(batch, None, None, "invalid_identity")
                    )
                    continue
                origin, account_id, native_scope, scope = identity
                if counts[(origin, account_id)] > 1:
                    outcomes.append(
                        self._outcome(batch, account_id, scope, "ambiguous_identity")
                    )
                    continue
                record = cast(dict[str, Any], record)
                original = {
                    key: value for key, value in record.items() if key != "transactions"
                }
                common = {
                    "batch_id": str(batch.batch_id),
                    "source_origin": origin,
                    "account_id": account_id,
                    "native_scope": _json(native_scope),
                    "identity_strength": scope_identity_strength(
                        source.protocol_version, native_scope
                    ),
                    "fetched_at": batch.fetched_at,
                    "source_file": f"sync_{job_id}",
                }
                account_error = (
                    None
                    if isinstance(record.get("name"), str)
                    and isinstance(record.get("currency"), str)
                    else "invalid_account"
                )
                accounts.append({
                    **common,
                    "observation_version": _revision(original),
                    "original_record": _json(original),
                    "validation_error": account_error,
                    "name": _text(record.get("name")),
                    "account_label": (
                        mask_embedded_account_number(record["name"].strip())
                        if isinstance(record.get("name"), str)
                        else None
                    ),
                    "institution_label": _institution_label(native_scope, payload),
                    "currency": _text(record.get("currency")),
                })
                balance, balance_error = _money(record.get("balance"))
                available, available_error = (
                    _money(record["available-balance"])
                    if "available-balance" in record
                    else (None, None)
                )
                balance_at, timestamp_error = _instant(
                    record.get("balance-date"), batch.fetched_at
                )
                error = _validation_error(
                    account_error, balance_error, available_error, timestamp_error
                )
                balance_record = {
                    key: value
                    for key, value in original.items()
                    if key
                    in (
                        "id",
                        "conn_id",
                        "org",
                        "currency",
                        "balance",
                        "available-balance",
                        "balance-date",
                    )
                }
                balances.append({
                    **common,
                    "observation_version": _revision(balance_record),
                    "original_record": _json(balance_record),
                    "validation_error": error,
                    "balance": balance,
                    "balance_original": _text(record.get("balance")),
                    "available_balance": available,
                    "available_balance_original": _text(
                        record.get("available-balance")
                    ),
                    "balance_at": balance_at,
                    "currency": _text(record.get("currency")),
                })
                if error:
                    outcomes.append(self._outcome(batch, account_id, scope, error))
                native_transactions = record.get("transactions", [])
                if not isinstance(native_transactions, list):
                    outcomes.append(
                        self._outcome(batch, account_id, scope, "invalid_transactions")
                    )
                    continue
                transaction_records = [
                    _record(item) for item in cast(list[object], native_transactions)
                ]
                transaction_counts = Counter(
                    item.get("id")
                    for item in transaction_records
                    if item is not None and isinstance(item.get("id"), str)
                )
                for transaction in transaction_records:
                    if (
                        transaction is None
                        or not isinstance(transaction.get("id"), str)
                        or not transaction["id"].strip()
                    ):
                        outcomes.append(
                            self._outcome(batch, account_id, scope, "invalid_identity")
                        )
                        continue
                    native_id = transaction["id"]
                    if transaction_counts[native_id] > 1:
                        outcomes.append(
                            self._outcome(
                                batch, account_id, scope, "ambiguous_identity"
                            )
                        )
                        continue
                    amount, amount_error = _money(transaction.get("amount"))
                    posted_at, posted_error = _instant(
                        transaction.get("posted"), batch.fetched_at, posted=True
                    )
                    transacted_at, transacted_error = (
                        _instant(transaction["transacted_at"], batch.fetched_at)
                        if "transacted_at" in transaction
                        else (None, None)
                    )
                    pending = transaction.get("pending")
                    error = _validation_error(
                        account_error,
                        amount_error,
                        posted_error,
                        transacted_error,
                        ("pending" if pending is True else None),
                        (
                            "invalid_pending"
                            if pending is not None and not isinstance(pending, bool)
                            else None
                        ),
                    )
                    transactions.append({
                        **common,
                        "transaction_id": native_id,
                        "transaction_reference": transaction_reference(
                            origin, account_id, native_id
                        ),
                        "observation_version": _revision({
                            "record": transaction,
                            "currency": record.get("currency"),
                        }),
                        "original_record": _json(transaction),
                        "validation_error": error,
                        "amount": amount,
                        "amount_original": _text(transaction.get("amount")),
                        "posted_at": posted_at,
                        "transacted_at": transacted_at,
                        "pending": pending if isinstance(pending, bool) else None,
                        "description": _text(transaction.get("description")),
                        "currency": _text(record.get("currency")),
                    })
                    if error:
                        outcomes.append(self._outcome(batch, account_id, scope, error))
        receipt = {
            "job_id": str(job_id),
            "connection_id": str(source.connection_id),
            "credential_generation": source.credential_generation,
            "protocol_version": source.protocol_version,
            "status": job_context.status,
            "started_at": job_context.started_at,
            "completed_at": job_context.completed_at,
            "batch_ids": _json([str(batch) for batch in seen_batches]),
            "source_metadata": _json(
                source.model_dump(mode="json", exclude={"batches"})
            ),
            "local_outcomes": _json([
                outcome.model_dump(mode="json") for outcome in outcomes
            ]),
        }
        receipt["content_digest"] = _revision({
            **receipt,
            "started_at": job_context.started_at.isoformat(),
            "completed_at": job_context.completed_at.isoformat(),
        })
        self._check_immutable(
            SIMPLEFIN_SYNC_RECEIPTS,
            {"job_id": str(job_id), "connection_id": str(source.connection_id)},
            receipt["content_digest"],
        )
        self._write(SIMPLEFIN_RESPONSES, responses)
        accepted_accounts = self._write(SIMPLEFIN_ACCOUNTS, accounts)
        accepted_transactions = self._write(SIMPLEFIN_TRANSACTIONS, transactions)
        accepted_balances = self._write(SIMPLEFIN_BALANCES, balances)
        self._write(SIMPLEFIN_SYNC_RECEIPTS, [receipt])
        return SimpleFINLoadResult(
            accepted_accounts,
            accepted_transactions,
            accepted_balances,
            list(seen_batches),
            [*source.outcomes, *outcomes],
        )

    @staticmethod
    def _account_identity(
        source: SimpleFINSource, record: dict[str, Any] | None
    ) -> (
        tuple[
            str, str, dict[str, str], SimpleFINV1OrgScope | SimpleFINV2ConnectionScope
        ]
        | None
    ):
        if (
            record is None
            or not isinstance(record.get("id"), str)
            or not record["id"].strip()
        ):
            return None
        try:
            if source.protocol_version == "1.0.7":
                org = SimpleFINV1Organization.model_validate(record.get("org"))
                native = org.model_dump(mode="json", by_alias=True, exclude_none=True)
                scope = SimpleFINV1OrgScope(kind="simplefin_v1_org", org=org)
            else:
                scope = SimpleFINV2ConnectionScope.model_validate({
                    "kind": "simplefin_v2_connection",
                    "conn_id": record.get("conn_id"),
                })
                native = {"conn_id": scope.conn_id}
            return (
                source_origin(source.connection_id, source.protocol_version, native),
                record["id"],
                native,
                scope,
            )
        except ValueError:
            return None

    @staticmethod
    def _outcome(
        batch: ProviderBatch,
        account_id: str | None,
        scope: SimpleFINV1OrgScope | SimpleFINV2ConnectionScope | None,
        reason: str,
    ) -> ScopeOutcome:
        return ScopeOutcome(
            account_id=account_id,
            native_scope=scope,
            window=batch.window,
            status="partial",
            error_code=reason,
        )
