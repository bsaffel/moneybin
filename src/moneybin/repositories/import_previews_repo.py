"""Audited lifecycle for persisted staged-import previews."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from moneybin.errors import UserError
from moneybin.repositories.base import BaseRepo
from moneybin.tables import IMPORT_PREVIEW_SNAPSHOTS, IMPORT_PREVIEWS

_COLUMNS = (
    "preview_id",
    "file_path",
    "file_sha256",
    "file_size_bytes",
    "channel",
    "snapshot_json",
    "issued_at",
    "expires_at",
    "consumed_at",
    "import_id",
    "updated_at",
)


def _decode(row: tuple[Any, ...]) -> dict[str, Any]:
    values: dict[str, Any] = dict(zip(_COLUMNS, row, strict=True))
    snapshot = values["snapshot_json"]
    if isinstance(snapshot, str):
        values["snapshot_json"] = json.loads(snapshot)
    return values


def _db_time(value: datetime) -> datetime:
    """Normalize an aware/naive UTC timestamp to DuckDB's naive UTC form."""
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


class ImportPreviewsRepo(BaseRepo):
    """Issue and consume one exact import preview with paired audit rows."""

    repository = "import_previews"
    table_ref = IMPORT_PREVIEWS
    pk_columns = ("preview_id",)

    def _fetch_row(self, preview_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            IMPORT_PREVIEWS,
            _COLUMNS,
            "preview_id",
            preview_id,
            decode=_decode,
        )

    def get(self, preview_id: str) -> dict[str, Any] | None:
        """Return one preview trust-state row."""
        return self._fetch_row(preview_id)

    def issue(
        self,
        *,
        file_path: str,
        file_sha256: str,
        file_size_bytes: int,
        channel: Literal["tabular", "pdf", "ofx"],
        source_bytes: bytes,
        snapshot: dict[str, Any],
        issued_at: datetime,
        expires_at: datetime,
        actor: str,
        in_outer_txn: bool = False,
    ) -> str:
        """Persist a new opaque preview and its complete canonical snapshot."""
        preview_id = uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {IMPORT_PREVIEWS.full_name} (
                    preview_id, file_path, file_sha256, file_size_bytes,
                    channel, snapshot_json, issued_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    preview_id,
                    file_path,
                    file_sha256,
                    file_size_bytes,
                    channel,
                    json.dumps(snapshot, sort_keys=True, separators=(",", ":")),
                    _db_time(issued_at),
                    _db_time(expires_at),
                ],
            )
            self._db.execute(
                f"""
                INSERT INTO {IMPORT_PREVIEW_SNAPSHOTS.full_name} (
                    preview_id, source_bytes, created_at
                ) VALUES (?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [preview_id, source_bytes, _db_time(issued_at)],
            )
            after = self._fetch_row(preview_id)
            self._emit_audit(
                action="import_preview.issue",
                target=(*self._audit_target, preview_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
            )
        return preview_id

    def get_source_bytes(self, preview_id: str) -> bytes | None:
        """Return the exact encrypted-at-rest byte object for one preview."""
        row = self._db.execute(
            f"""
            SELECT source_bytes
            FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
            WHERE preview_id = ?
            """,  # noqa: S608  # TableRef + parameterized value
            [preview_id],
        ).fetchone()
        return None if row is None else bytes(row[0])

    def consume(
        self,
        preview_id: str,
        *,
        file_sha256: str,
        file_size_bytes: int,
        now: datetime,
        actor: str,
        in_outer_txn: bool = False,
    ) -> dict[str, Any]:
        """Consume an unchanged, live preview inside the caller's transaction."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(preview_id)
            if before is None:
                raise UserError(
                    "Import preview was not found.",
                    code="IMPORT_PREVIEW_NOT_FOUND",
                )
            if before["consumed_at"] is not None:
                raise UserError(
                    "Import preview has already been consumed.",
                    code="IMPORT_PREVIEW_CONSUMED",
                )
            if _db_time(now) >= before["expires_at"]:
                raise UserError(
                    "Import preview has expired.",
                    code="IMPORT_PREVIEW_EXPIRED",
                )
            if (
                before["file_sha256"] != file_sha256
                or before["file_size_bytes"] != file_size_bytes
            ):
                raise UserError(
                    "The previewed file changed before confirmation.",
                    code="IMPORT_PREVIEW_CHANGED",
                )
            self._db.execute(
                f"""
                UPDATE {IMPORT_PREVIEWS.full_name}
                SET consumed_at = ?, updated_at = ?
                WHERE preview_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [_db_time(now), _db_time(now), preview_id],
            )
            self._db.execute(
                f"""
                DELETE FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
                WHERE preview_id = ?
                """,  # noqa: S608  # TableRef + parameterized value
                [preview_id],
            )
            after = self._fetch_row(preview_id)
            self._emit_audit(
                action="import_preview.consume",
                target=(*self._audit_target, preview_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
            )
            return self._require(after, "preview_id", preview_id)

    def has_expired_or_orphaned(self, *, now: datetime) -> bool:
        """Return whether preview retention maintenance needs a writer."""
        tables = self._db.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE (table_schema = ? AND table_name = ?)
               OR (table_schema = ? AND table_name = ?)
            """,
            [
                IMPORT_PREVIEWS.schema,
                IMPORT_PREVIEWS.name,
                IMPORT_PREVIEW_SNAPSHOTS.schema,
                IMPORT_PREVIEW_SNAPSHOTS.name,
            ],
        ).fetchone()
        if tables is None or int(tables[0]) == 0:
            return False
        if int(tables[0]) == 1:
            return True

        cutoff = _db_time(now)
        row = self._db.execute(
            f"""
            SELECT
                EXISTS (
                    SELECT 1
                    FROM {IMPORT_PREVIEWS.full_name}
                    WHERE consumed_at IS NULL
                      AND (
                          expires_at <= ?
                          OR preview_id NOT IN (
                              SELECT preview_id
                              FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
                          )
                      )
                )
                OR EXISTS (
                    SELECT 1
                    FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
                    WHERE preview_id NOT IN (
                        SELECT preview_id FROM {IMPORT_PREVIEWS.full_name}
                    )
                )
            """,  # noqa: S608  # TableRefs + parameterized cutoff
            [cutoff],
        ).fetchone()
        return bool(row and row[0])

    def purge_expired(
        self,
        *,
        now: datetime,
        actor: str,
        in_outer_txn: bool = False,
    ) -> int:
        """Delete expired previews and either side's unusable orphan rows."""
        cutoff = _db_time(now)
        with self._transaction(in_outer_txn=in_outer_txn):
            rows = self._db.execute(
                f"""
                SELECT preview_id
                FROM {IMPORT_PREVIEWS.full_name}
                WHERE consumed_at IS NULL
                  AND (
                       expires_at <= ?
                       OR preview_id NOT IN (
                           SELECT preview_id
                           FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
                       )
                  )
                ORDER BY preview_id
                """,  # noqa: S608  # TableRefs + parameterized cutoff
                [cutoff],
            ).fetchall()
            preview_ids = [str(row[0]) for row in rows]
            for preview_id in preview_ids:
                before = self._require(
                    self._fetch_row(preview_id),
                    "preview_id",
                    preview_id,
                )
                self._db.execute(
                    f"""
                    DELETE FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
                    WHERE preview_id = ?
                    """,  # noqa: S608  # TableRef + parameterized value
                    [preview_id],
                )
                self._db.execute(
                    f"""
                    DELETE FROM {IMPORT_PREVIEWS.full_name}
                    WHERE preview_id = ?
                    """,  # noqa: S608  # TableRef + parameterized value
                    [preview_id],
                )
                self._emit_audit(
                    action="import_preview.expire",
                    target=(*self._audit_target, preview_id),
                    before=self._serialize_for_audit(before),
                    after=None,
                    actor=actor,
                )
            self._db.execute(
                f"""
                DELETE FROM {IMPORT_PREVIEW_SNAPSHOTS.full_name}
                WHERE preview_id NOT IN (
                    SELECT preview_id FROM {IMPORT_PREVIEWS.full_name}
                )
                """,  # noqa: S608  # TableRefs are code constants
            )
            return len(preview_ids)

    def record_result(
        self,
        preview_id: str,
        *,
        import_id: str,
        actor: str,
        in_outer_txn: bool = False,
    ) -> None:
        """Attach the completed import ID before the caller commits."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(preview_id),
                "preview_id",
                preview_id,
            )
            if before["consumed_at"] is None:
                raise ValueError("cannot record a result for an unconsumed preview")
            self._db.execute(
                f"""
                UPDATE {IMPORT_PREVIEWS.full_name}
                SET import_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE preview_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [import_id, preview_id],
            )
            after = self._fetch_row(preview_id)
            self._emit_audit(
                action="import_preview.complete",
                target=(*self._audit_target, preview_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
            )
