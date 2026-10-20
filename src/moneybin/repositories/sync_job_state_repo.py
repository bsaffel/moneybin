"""Operational sync recovery bookkeeping, following ImportLogRepo (no user undo)."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from moneybin.connectors.sync_models import SyncJobDetails
from moneybin.database import Database
from moneybin.tables import SYNC_JOB_STATE


class SyncJobState(BaseModel):
    """One durably ingested job and its independent remaining obligations."""

    job_id: UUID
    consumer_id: str
    ingested_at: datetime
    acked_at: datetime | None
    details: SyncJobDetails


class SyncJobStateRepo:
    """Own all operational app writes; remote acknowledgement is never undoable."""

    def __init__(self, db: Database) -> None:
        """Bind to the caller's open warehouse."""
        self.db = db

    def get(self, job_id: str, consumer_id: str) -> SyncJobState | None:
        """Read one job, rejecting a different profile's recovery identity."""
        row = self.db.execute(
            f"""SELECT job_id, consumer_id, ingested_at, acked_at, job_details
                FROM {SYNC_JOB_STATE.full_name} WHERE job_id = ?""",  # noqa: S608  # trusted TableRef; values parameterized
            [job_id],
        ).fetchone()
        if row is None:
            return None
        if row[1] != consumer_id:
            raise ValueError("Sync recovery job belongs to another profile")
        return SyncJobState(
            job_id=row[0],
            consumer_id=row[1],
            ingested_at=row[2],
            acked_at=row[3],
            details=SyncJobDetails.model_validate_json(row[4]),
        )

    def ensure(self, job_id: str, consumer_id: str, details: SyncJobDetails) -> None:
        """Join the ingestion transaction; redelivery never resets confirmed work."""
        existing = self.get(job_id, consumer_id)
        if existing is not None:
            if existing.details.sources != details.sources:
                raise ValueError("Conflicting immutable sync job source evidence")
            return
        for source in details.sources:
            if source.source.provider == "plaid" and (
                set(source.batch_digests) != set(source.batch_ids)
                or any(
                    len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)
                    for digest in source.batch_digests.values()
                )
            ):
                raise ValueError(
                    "New sync job requires complete Plaid batch digest evidence"
                )
        result = details.result
        self.db.execute(
            f"""INSERT INTO {SYNC_JOB_STATE.full_name}
                (job_id, consumer_id, ack_state, setup_state, refresh_state, job_details)
                VALUES (?, ?, ?, ?, ?, ?)""",  # noqa: S608  # trusted TableRef; values parameterized
            [
                job_id,
                consumer_id,
                result.ack_state,
                result.setup_state,
                result.refresh_state,
                details.model_dump_json(),
            ],
        )

    def save(self, state: SyncJobState) -> None:
        """Atomically save phase outcomes and protected acknowledgement evidence."""
        previous = self.get(str(state.job_id), state.consumer_id)
        if previous is None:
            raise ValueError("Sync job was not durably ingested")
        if previous.details.sources != state.details.sources:
            raise ValueError("Conflicting immutable sync job source evidence")
        if previous.details.result.ack_state == "confirmed":
            state.details.result.ack_state = "confirmed"
            state.details.result.ack_error_code = None
            state.acked_at = previous.acked_at
            state.details.acknowledgement = previous.details.acknowledgement
        result = state.details.result
        self.db.execute(
            f"""UPDATE {SYNC_JOB_STATE.full_name}
                SET ack_state = ?, acked_at = ?, setup_state = ?, refresh_state = ?,
                    job_details = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ? AND consumer_id = ?""",  # noqa: S608  # trusted TableRef; values parameterized
            [
                result.ack_state,
                state.acked_at,
                result.setup_state,
                result.refresh_state,
                state.details.model_dump_json(),
                str(state.job_id),
                state.consumer_id,
            ],
        )

    def unfinished(self, consumer_id: str) -> list[str]:
        """Find already-ingested jobs requiring acknowledgement or local repair."""
        return [
            str(row[0])
            for row in self.db.execute(
                f"""SELECT job_id FROM {SYNC_JOB_STATE.full_name}
                WHERE consumer_id = ? AND (ack_state IN ('pending', 'unknown')
                    OR setup_state != 'complete' OR refresh_state != 'complete')
                ORDER BY ingested_at, job_id""",  # noqa: S608  # trusted TableRef; values parameterized
                [consumer_id],
            ).fetchall()
        ]

    def for_profile(self, consumer_id: str) -> list[SyncJobState]:
        """Read latest-first operational observations for current-grant status."""
        rows = self.db.execute(
            f"""SELECT job_id, ingested_at, acked_at, job_details FROM {SYNC_JOB_STATE.full_name} WHERE consumer_id = ? ORDER BY ingested_at DESC, job_id DESC""",  # noqa: S608  # trusted TableRef and parameterized profile
            [consumer_id],
        ).fetchall()  # trusted TableRef and bound profile identity
        return [
            SyncJobState(
                job_id=row[0],
                consumer_id=consumer_id,
                ingested_at=row[1],
                acked_at=row[2],
                details=SyncJobDetails.model_validate_json(row[3]),
            )
            for row in rows
        ]
