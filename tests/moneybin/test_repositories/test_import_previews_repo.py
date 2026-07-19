"""Tests for audited persisted import-preview trust state."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta

import pytest

from moneybin.database import Database
from moneybin.errors import UserError

NOW = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)


def _repo(db: Database) -> object:
    module = importlib.import_module("moneybin.repositories.import_previews_repo")
    return module.ImportPreviewsRepo(db)


def _issue(
    repo: object,
    *,
    issued_at: datetime = NOW,
    expires_at: datetime | None = None,
) -> str:
    return repo.issue(  # type: ignore[attr-defined]
        file_path="/Users/example/statement.csv",
        file_sha256="a" * 64,
        file_size_bytes=128,
        channel="tabular",
        snapshot={
            "mapping": {
                "transaction_date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
            "confidence": "high",
        },
        issued_at=issued_at,
        expires_at=expires_at or NOW + timedelta(minutes=5),
        actor="mcp",
    )


def test_issue_persists_complete_snapshot_and_audit(db: Database) -> None:
    repo = _repo(db)

    preview_id = _issue(repo)

    assert len(preview_id) == 12
    row = repo.get(preview_id)  # type: ignore[attr-defined]
    assert row is not None
    assert row["file_path"] == "/Users/example/statement.csv"
    assert row["file_sha256"] == "a" * 64
    assert row["file_size_bytes"] == 128
    assert row["snapshot_json"]["confidence"] == "high"
    audit = db.execute(
        """
        SELECT before_value, after_value
        FROM app.audit_log
        WHERE action = 'import_preview.issue' AND target_id = ?
        """,
        [preview_id],
    ).fetchone()
    assert audit is not None
    assert audit[0] is None
    assert audit[1] is not None


def test_consume_binds_exact_file_and_records_result(db: Database) -> None:
    repo = _repo(db)
    preview_id = _issue(repo)

    db.begin()
    try:
        consumed = repo.consume(  # type: ignore[attr-defined]
            preview_id,
            file_sha256="a" * 64,
            file_size_bytes=128,
            now=NOW + timedelta(seconds=1),
            actor="mcp",
            in_outer_txn=True,
        )
        repo.record_result(  # type: ignore[attr-defined]
            preview_id,
            import_id="imp_123",
            actor="mcp",
            in_outer_txn=True,
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise

    assert consumed["preview_id"] == preview_id
    row = repo.get(preview_id)  # type: ignore[attr-defined]
    assert row["consumed_at"] is not None
    assert row["import_id"] == "imp_123"


@pytest.mark.parametrize(
    ("expires_at", "consume_at", "sha256", "code"),
    [
        (
            NOW - timedelta(seconds=1),
            NOW,
            "a" * 64,
            "IMPORT_PREVIEW_EXPIRED",
        ),
        (
            NOW + timedelta(minutes=5),
            NOW,
            "b" * 64,
            "IMPORT_PREVIEW_CHANGED",
        ),
    ],
)
def test_consume_refuses_expired_or_changed_preview(
    db: Database,
    expires_at: datetime,
    consume_at: datetime,
    sha256: str,
    code: str,
) -> None:
    repo = _repo(db)
    preview_id = _issue(
        repo,
        issued_at=min(NOW, expires_at - timedelta(minutes=5)),
        expires_at=expires_at,
    )

    with pytest.raises(UserError) as exc_info:
        repo.consume(  # type: ignore[attr-defined]
            preview_id,
            file_sha256=sha256,
            file_size_bytes=128,
            now=consume_at,
            actor="mcp",
        )

    assert exc_info.value.code == code
    assert repo.get(preview_id)["consumed_at"] is None  # type: ignore[attr-defined,index]


def test_consume_is_single_use(db: Database) -> None:
    repo = _repo(db)
    preview_id = _issue(repo)
    repo.consume(  # type: ignore[attr-defined]
        preview_id,
        file_sha256="a" * 64,
        file_size_bytes=128,
        now=NOW + timedelta(seconds=1),
        actor="mcp",
    )

    with pytest.raises(UserError) as exc_info:
        repo.consume(  # type: ignore[attr-defined]
            preview_id,
            file_sha256="a" * 64,
            file_size_bytes=128,
            now=NOW + timedelta(seconds=2),
            actor="mcp",
        )

    assert exc_info.value.code == "IMPORT_PREVIEW_CONSUMED"


def test_outer_transaction_rollback_restores_unconsumed_preview(db: Database) -> None:
    repo = _repo(db)
    preview_id = _issue(repo)

    db.begin()
    try:
        repo.consume(  # type: ignore[attr-defined]
            preview_id,
            file_sha256="a" * 64,
            file_size_bytes=128,
            now=NOW + timedelta(seconds=1),
            actor="mcp",
            in_outer_txn=True,
        )
        raise RuntimeError("simulated import failure")
    except RuntimeError:
        db.rollback()

    assert repo.get(preview_id)["consumed_at"] is None  # type: ignore[attr-defined,index]
