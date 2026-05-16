"""Envelope shape tests for the transforms block on ``system_status``."""

from __future__ import annotations

import pytest

from moneybin.database import get_database
from moneybin.mcp.tools.system import system_status


def _seed_pending_import(import_id: str = "IMP_PENDING_001") -> None:
    """Insert raw account row + import_log strictly newer than template's dim.

    freshness() compares MAX(raw.X_accounts.extracted_at) to
    MAX(core.dim_accounts.extracted_at); a bare import_log row no longer
    triggers pending. Seed a raw.ofx_accounts row with a future
    extracted_at so the new signal fires.
    """
    with get_database() as db:
        db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                format_name, account_names, status, rows_total, rows_imported,
                completed_at
            ) VALUES (?, 'inline', 'manual', 'manual',
                      'manual', '[]'::JSON, 'complete', 0, 0,
                      CURRENT_TIMESTAMP)
            """,
            [import_id],
        )
        db.execute(
            """
            INSERT INTO raw.ofx_accounts
            (account_id, source_file, extracted_at, import_id)
            VALUES (?, 'inline', TIMESTAMP '2099-01-01 00:00:00', ?)
            """,
            [f"ACC_{import_id}", import_id],
        )


@pytest.mark.unit
async def test_system_status_envelope_has_transforms_block(mcp_db: object) -> None:
    """Envelope.data exposes transforms.{pending,last_apply_at}."""
    env = await system_status()
    data = env.to_dict()["data"]
    assert "transforms" in data
    transforms = data["transforms"]
    assert "pending" in transforms
    assert "last_apply_at" in transforms


@pytest.mark.unit
async def test_pending_state_adds_action_hint(mcp_db: object) -> None:
    """When pending=True, actions includes a transform_apply hint."""
    _seed_pending_import()
    env = await system_status()
    parsed = env.to_dict()
    assert parsed["data"]["transforms"]["pending"] is True
    assert any("transform_apply" in a for a in parsed["actions"])


@pytest.mark.unit
async def test_not_pending_omits_action_hint(mcp_db: object) -> None:
    """No pending imports → no transform_apply hint."""
    env = await system_status()
    parsed = env.to_dict()
    assert parsed["data"]["transforms"]["pending"] is False
    assert not any("transform_apply" in a for a in parsed["actions"])


@pytest.mark.unit
async def test_system_status_omits_schema_drift_when_healthy(mcp_db: object) -> None:
    """No schema_drift key when all core tables match EXPECTED_CORE_COLUMNS."""
    env = await system_status()
    data = env.to_dict()["data"]
    assert "schema_drift" not in data


@pytest.mark.unit
async def test_system_status_surfaces_schema_drift_when_columns_missing(
    mcp_db: object,
) -> None:
    """schema_drift block lists missing columns when drift is detected."""
    with get_database() as db:
        db.execute("ALTER TABLE core.dim_accounts DROP COLUMN display_name")
    env = await system_status()
    data = env.to_dict()["data"]
    assert "schema_drift" in data
    tables = data["schema_drift"]["tables"]
    entry = next(t for t in tables if t["name"] == "core.dim_accounts")
    assert "display_name" in entry["missing_columns"]
    assert data["schema_drift"]["remediation"] == "moneybin transform apply"


@pytest.mark.unit
async def test_system_status_action_hint_for_schema_drift(mcp_db: object) -> None:
    """Actions array includes a transform_apply remediation hint when drift detected."""
    with get_database() as db:
        db.execute("ALTER TABLE core.dim_accounts DROP COLUMN display_name")
    env = await system_status()
    actions = env.to_dict()["actions"]
    assert any("drifted" in a for a in actions)


@pytest.mark.unit
def testcheck_schema_at_boot_self_heals_drift(mcp_db: object, mocker: object) -> None:
    """Drift triggers one apply() attempt and boot completes when it resolves."""
    from moneybin.mcp.server import check_schema_at_boot
    from moneybin.services.transform_service import ApplyResult, TransformService

    with get_database() as db:
        db.execute("ALTER TABLE core.dim_accounts DROP COLUMN display_name")

    def _fake_apply(svc: TransformService) -> ApplyResult:
        # Simulate SQLMesh restoring the dropped column.
        svc._db.execute(  # pyright: ignore[reportPrivateUsage]  # test mock reaches into service
            "ALTER TABLE core.dim_accounts ADD COLUMN display_name VARCHAR"
        )
        return ApplyResult(applied=True, duration_seconds=0.01)

    mocker.patch.object(TransformService, "apply", _fake_apply)  # type: ignore[attr-defined]

    check_schema_at_boot()


@pytest.mark.unit
def testcheck_schema_at_boot_raises_when_heal_does_not_resolve(
    mcp_db: object, mocker: object
) -> None:
    """apply() succeeds but post-verify still finds drift → SchemaDriftError."""
    from moneybin.database import SchemaDriftError
    from moneybin.mcp.server import check_schema_at_boot
    from moneybin.services.transform_service import ApplyResult, TransformService

    with get_database() as db:
        db.execute("ALTER TABLE core.dim_accounts DROP COLUMN display_name")

    def _fake_apply(svc: TransformService) -> ApplyResult:
        return ApplyResult(applied=True, duration_seconds=0.01)

    mocker.patch.object(TransformService, "apply", _fake_apply)  # type: ignore[attr-defined]

    with pytest.raises(SchemaDriftError, match="persist after auto-heal"):
        check_schema_at_boot()


@pytest.mark.unit
def testcheck_schema_at_boot_propagates_apply_failure(
    mcp_db: object, mocker: object
) -> None:
    """apply() soft-fails (applied=False) → RuntimeError mentions the apply error."""
    from moneybin.mcp.server import check_schema_at_boot
    from moneybin.services.transform_service import ApplyResult, TransformService

    with get_database() as db:
        db.execute("ALTER TABLE core.dim_accounts DROP COLUMN display_name")

    def _failing_apply(svc: TransformService) -> ApplyResult:
        return ApplyResult(applied=False, duration_seconds=0.01, error="PlanError")

    mocker.patch.object(TransformService, "apply", _failing_apply)  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="PlanError"):
        check_schema_at_boot()


@pytest.mark.unit
def testcheck_schema_at_boot_silent_on_healthy(mcp_db: object) -> None:
    """check_schema_at_boot returns silently when no drift detected."""
    from moneybin.mcp.server import check_schema_at_boot

    check_schema_at_boot()
