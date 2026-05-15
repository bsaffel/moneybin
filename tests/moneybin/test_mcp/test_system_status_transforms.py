"""Envelope shape tests for the transforms block on ``system_status``."""

from __future__ import annotations

import pytest

from moneybin.database import get_database
from moneybin.mcp.tools.system import system_status


def _seed_pending_import(import_id: str = "IMP_PENDING_001") -> None:
    """Insert an import strictly newer than the template's dim_accounts.updated_at."""
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
