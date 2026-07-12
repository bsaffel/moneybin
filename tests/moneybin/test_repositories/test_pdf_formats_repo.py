"""Tests for ``PdfFormatsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior recipe (Req 9a: recipe versioning via audit log).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

_RECIPE_V1: dict[str, Any] = {
    "fields": [
        {"name": "date", "anchor": "Date", "type": "date"},
        {"name": "amount", "anchor": "Amount", "type": "decimal"},
    ],
    "routing": "transactions",
}

_RECIPE_V2: dict[str, Any] = {
    "fields": [
        {"name": "date", "anchor": "Transaction Date", "type": "date"},
        {"name": "amount", "anchor": "Amount", "type": "decimal"},
        {"name": "description", "anchor": "Description", "type": "string"},
    ],
    "routing": "transactions",
}

_FINGERPRINT: dict[str, Any] = {
    "issuer": "chase",
    "headers": ["Date", "Description", "Amount"],
    "page_bucket": "2-3",
}


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def _save_new(
    repo: PdfFormatsRepo,
    name: str = "chase_checking_pdf",
    recipe: dict[str, Any] | None = None,
    fingerprint: dict[str, Any] | None = None,
    **overrides: Any,
) -> None:
    kwargs: dict[str, Any] = {
        "institution_name": "Chase",
        "document_kind": "checking_statement",
        "front_end": "text",
        "routing": "transactions",
        "actor": "cli",
    }
    kwargs.update(overrides)
    repo.save_new(
        name,
        recipe if recipe is not None else _RECIPE_V1,
        fingerprint=fingerprint if fingerprint is not None else _FINGERPRINT,
        **kwargs,
    )


def test_save_new_emits_audit_row(db: Database) -> None:
    repo = PdfFormatsRepo(db)

    _save_new(repo)

    rows = db.conn.execute(
        "SELECT action, after_value FROM app.audit_log "
        "WHERE target_table = 'pdf_formats' AND action = 'pdf_format.save'"
    ).fetchall()
    assert len(rows) == 1
    after = json.loads(rows[0][1])
    assert after["institution_name"] == "Chase"


def test_save_new_persists_row_at_version_1(db: Database) -> None:
    repo = PdfFormatsRepo(db)

    _save_new(repo)

    row = db.conn.execute(
        "SELECT institution_name, document_kind, version, routing "
        "FROM app.pdf_formats WHERE name = ?",
        ["chase_checking_pdf"],
    ).fetchone()
    assert row == ("Chase", "checking_statement", 1, "transactions")


def test_save_new_audit_before_value_is_none(db: Database) -> None:
    """Initial save has no prior row — before_value must be NULL."""
    repo = PdfFormatsRepo(db)

    _save_new(repo)

    audit = _audit_rows_for(db, "chase_checking_pdf")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, _actor, _ = audit[0]
    assert action == "pdf_format.save"
    assert (schema, table, target_id) == ("app", "pdf_formats", "chase_checking_pdf")
    assert before is None
    after_json = json.loads(after)
    assert after_json["extraction_recipe"] == _RECIPE_V1


def test_bump_version_increments_and_audits_prior(db: Database) -> None:
    """save_new at v1, bump → v2; version column increments; audit row emitted."""
    repo = PdfFormatsRepo(db)
    _save_new(repo, recipe=_RECIPE_V1)

    repo.bump_version(
        "chase_checking_pdf",
        _RECIPE_V2,
        reason="added description field",
        actor="cli",
    )

    row = db.conn.execute(
        "SELECT version FROM app.pdf_formats WHERE name = ?",
        ["chase_checking_pdf"],
    ).fetchone()
    assert row == (2,)

    audit = _audit_rows_for(db, "chase_checking_pdf")
    bump_rows = [r for r in audit if r[0] == "pdf_format.bump_version"]
    assert len(bump_rows) == 1


def test_bump_version_audit_before_value_is_recoverable_recipe(db: Database) -> None:
    """The undo consumer reads before_value to restore — verify it's the full prior recipe."""
    repo = PdfFormatsRepo(db)
    _save_new(repo, recipe=_RECIPE_V1)

    repo.bump_version(
        "chase_checking_pdf",
        _RECIPE_V2,
        reason="added description field",
        actor="cli",
    )

    audit = _audit_rows_for(db, "chase_checking_pdf")
    bump_row = next(r for r in audit if r[0] == "pdf_format.bump_version")
    before_value = json.loads(bump_row[4])
    # The full prior row is captured in before_value; extraction_recipe is the v1 recipe
    assert before_value["extraction_recipe"] == _RECIPE_V1
    assert before_value["version"] == 1
    # After value reflects the new recipe
    after_value = json.loads(bump_row[5])
    assert after_value["extraction_recipe"] == _RECIPE_V2
    assert after_value["version"] == 2


def test_bump_version_mirrors_the_new_recipes_sign_convention(db: Database) -> None:
    """The column tracks the recipe automatically — no caller has to remember it.

    ``import formats show`` reads the ``sign_convention`` column, not the recipe.
    A bump that changes the recipe's convention while the column keeps reporting
    the old one is a silent lie to every reader of the column, and it was a
    caller's responsibility to prevent — so a third bump site could reintroduce it.
    """
    repo = PdfFormatsRepo(db)
    _save_new(
        repo,
        recipe={**_RECIPE_V1, "sign_convention": "negative_is_income"},
        sign_convention="negative_is_income",
    )

    repo.bump_version(
        "chase_checking_pdf",
        {**_RECIPE_V2, "sign_convention": "negative_is_expense"},
        reason="user corrected a false-positive card detection",
        actor="import",
    )

    row = db.conn.execute(
        "SELECT sign_convention FROM app.pdf_formats WHERE name = ?",
        ["chase_checking_pdf"],
    ).fetchone()
    assert row == ("negative_is_expense",)


def test_bump_version_leaves_the_column_alone_when_the_recipe_omits_it(
    db: Database,
) -> None:
    """A recipe with no ``sign_convention`` key must not NULL the column."""
    repo = PdfFormatsRepo(db)
    _save_new(repo, recipe=_RECIPE_V1, sign_convention="negative_is_expense")

    repo.bump_version(
        "chase_checking_pdf", _RECIPE_V2, reason="added description field", actor="cli"
    )

    row = db.conn.execute(
        "SELECT sign_convention FROM app.pdf_formats WHERE name = ?",
        ["chase_checking_pdf"],
    ).fetchone()
    assert row == ("negative_is_expense",)


def test_get_by_fingerprint_returns_match(db: Database) -> None:
    repo = PdfFormatsRepo(db)
    _save_new(repo, fingerprint=_FINGERPRINT)

    result = repo.get_by_fingerprint(_FINGERPRINT)

    assert result is not None
    assert result.name == "chase_checking_pdf"
    assert result.institution_name == "Chase"
    assert result.version == 1
    assert result.extraction_recipe == _RECIPE_V1


def test_get_by_fingerprint_returns_highest_version(db: Database) -> None:
    """When multiple versions exist, get_by_fingerprint returns the highest."""
    repo = PdfFormatsRepo(db)
    _save_new(repo, recipe=_RECIPE_V1)
    repo.bump_version("chase_checking_pdf", _RECIPE_V2, reason="v2", actor="cli")

    result = repo.get_by_fingerprint(_FINGERPRINT)

    assert result is not None
    assert result.version == 2
    assert result.extraction_recipe == _RECIPE_V2


def test_get_by_fingerprint_returns_none_on_miss(db: Database) -> None:
    repo = PdfFormatsRepo(db)

    result = repo.get_by_fingerprint({"issuer": "nonexistent", "headers": []})

    assert result is None


def test_list_all_returns_inserted_rows(db: Database) -> None:
    repo = PdfFormatsRepo(db)
    _save_new(repo, "chase_checking_pdf")
    _save_new(
        repo,
        "wellsfargo_savings_pdf",
        fingerprint={"issuer": "wellsfargo", "headers": ["Date", "Desc", "Amount"]},
        institution_name="Wells Fargo",
        document_kind="savings_statement",
    )

    formats = repo.list_all()

    names = {f.name for f in formats}
    assert "chase_checking_pdf" in names
    assert "wellsfargo_savings_pdf" in names
    assert len(formats) == 2


def test_list_all_returns_empty_for_no_rows(db: Database) -> None:
    repo = PdfFormatsRepo(db)
    assert repo.list_all() == []


def test_record_use_bumps_times_used_and_stamps_last_used_at(db: Database) -> None:
    """save_new initialises times_used=1; record_use bumps it and stamps the timestamp."""
    repo = PdfFormatsRepo(db)
    _save_new(repo)

    before = db.conn.execute(
        "SELECT times_used, last_used_at FROM app.pdf_formats WHERE name = ?",
        ["chase_checking_pdf"],
    ).fetchone()
    assert before is not None
    assert before[0] == 1  # save_new initialises to 1

    repo.record_use("chase_checking_pdf")

    after = db.conn.execute(
        "SELECT times_used, last_used_at FROM app.pdf_formats WHERE name = ?",
        ["chase_checking_pdf"],
    ).fetchone()
    assert after is not None
    assert after[0] == 2  # bumped exactly once
    assert after[1] is not None  # last_used_at stamped


def test_record_use_does_not_audit(db: Database) -> None:
    """Per-import counter bumps are observability — no audit row, by design."""
    repo = PdfFormatsRepo(db)
    _save_new(repo)
    audit_before = _audit_rows_for(db, "chase_checking_pdf")

    repo.record_use("chase_checking_pdf")

    audit_after = _audit_rows_for(db, "chase_checking_pdf")
    # No new audit row from record_use (only the prior save_new row remains).
    assert len(audit_after) == len(audit_before)


def test_save_new_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = PdfFormatsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _save_new(repo, "ghost_format")

    rows = db.conn.execute(
        "SELECT 1 FROM app.pdf_formats WHERE name = ?", ["ghost_format"]
    ).fetchall()
    assert rows == []
    # Rollback contract: audit row must also be absent (write + audit are atomic).
    assert _audit_rows_for(db, "ghost_format") == []


def test_save_new_propagates_parent_audit_id(db: Database) -> None:
    """parent_audit_id from caller must thread into the emitted audit row."""
    repo = PdfFormatsRepo(db)

    _save_new(repo, parent_audit_id="parent-audit-xyz")

    audit = _audit_rows_for(db, "chase_checking_pdf")
    assert len(audit) == 1
    _, _, _, _, _, _, _, parent = audit[0]
    assert parent == "parent-audit-xyz"
