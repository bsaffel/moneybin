"""The shipped schema comment plans must match the DDL and never apply stale."""

from __future__ import annotations

import pytest

from moneybin import schema
from scripts.generate_schema_comment_plans import render


def test_checked_in_comment_plans_are_current() -> None:
    """Fails when DDL changed without `make generate-schema-comment-plans`."""
    assert schema.COMMENT_PLANS_PATH.read_text() == render(), (
        "Stale comment_plans.json — run `make generate-schema-comment-plans`"
    )


def test_shipped_plan_equals_parsed_plan() -> None:
    """Every core file's shipped plan round-trips to the parsed plan."""
    for path in schema._all_schema_files():  # pyright: ignore[reportPrivateUsage]
        sql = path.read_text()
        digest = schema.sha256(sql.encode()).hexdigest()
        shipped = schema._precomputed_plan(path, digest)  # pyright: ignore[reportPrivateUsage]
        assert shipped == schema._derive_comment_plan(sql), path.name  # pyright: ignore[reportPrivateUsage]


def test_hash_mismatch_falls_back_to_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Edited SQL text never receives the shipped plan for the old text."""
    path = schema._all_schema_files()[0]  # pyright: ignore[reportPrivateUsage]
    edited = path.read_text() + "\n-- edited\n"
    schema._COMMENT_PLAN_CACHE.clear()  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(schema, "_derive_comment_plan", lambda _sql: ())  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]
    assert schema._comment_plan(path, edited) == ()  # pyright: ignore[reportPrivateUsage]
    schema._COMMENT_PLAN_CACHE.clear()  # pyright: ignore[reportPrivateUsage]
