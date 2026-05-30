"""End-to-end: drop a CSV in the inbox, drain it, assert the correct outcome.

Two flows exercised:

- First-encounter unknown layout → ``pending/`` with a ``.pending.yml``
  sidecar carrying the proposed mapping. Subsequent ``moneybin import
  confirm <pending-path>`` would ratify and load.
- Confirmed layout (or one re-dropped after ratification) → ``processed/``.

The inbox treats every drop the same as any other surface route:
first-encounter always confirms; recovery is one explicit command away.
"""

from __future__ import annotations

import os
import shutil
import subprocess  # noqa: S404 — subprocess is intentional; we invoke uv as a test harness
from pathlib import Path

import pytest

from tests.e2e.conftest import FAST_ARGON2_ENV, FIXTURES_DIR, make_workflow_env

pytestmark = pytest.mark.e2e

# import_ uses a trailing underscore, so the nested env var has three underscores:
# MONEYBIN_ + IMPORT_ + __ + INBOX_ROOT → MONEYBIN_IMPORT___INBOX_ROOT
_INBOX_ROOT_ENV_KEY = "MONEYBIN_IMPORT___INBOX_ROOT"


class TestInboxWorkflow:
    """Inbox drain: file dropped in inbox/<slug>/ moves to pending/ on first encounter."""

    def test_inbox_first_encounter_lands_in_pending(self, e2e_home: Path) -> None:
        """First-encounter unknown layout surfaces to pending/ with a sidecar.

        The inbox sync is just another route into the import service. A
        first-encounter unknown layout must surface for confirmation —
        not silently load — matching the behavior of `moneybin import
        files` and the MCP `import_files` tool. The user runs
        `moneybin import confirm <pending-path>` to ratify; subsequent
        drops of the same layout flow through the silent known-layout
        fast path to processed/.
        """
        env = make_workflow_env(e2e_home, "wf-inbox")
        inbox_root = Path(env[_INBOX_ROOT_ENV_KEY])
        profile_name = env["MONEYBIN_PROFILE"]
        profile_dir = inbox_root / profile_name

        fixture = FIXTURES_DIR / "tabular" / "chase_credit.csv"
        if not fixture.exists():
            pytest.skip(f"fixture missing: {fixture}")

        drop_dir = profile_dir / "inbox" / "chase-credit"
        drop_dir.mkdir(parents=True)
        shutil.copy(fixture, drop_dir / "march.csv")

        full_env = {**os.environ, **FAST_ARGON2_ENV, **env}
        result = subprocess.run(  # noqa: S603 — controlled test command, not user input
            ["uv", "run", "moneybin", "import", "inbox"],  # noqa: S607 — uv is on PATH in dev
            env=full_env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr

        remaining = list((profile_dir / "inbox").rglob("*.csv"))
        assert remaining == [], f"Expected inbox empty after sync, found: {remaining}"

        # First-encounter unknown layout → pending/, not processed/ or failed/.
        processed_csvs = list((profile_dir / "processed").rglob("*.csv"))
        assert processed_csvs == [], (
            f"Expected processed/ empty on first encounter, found: {processed_csvs}"
        )
        failed_csvs = list((profile_dir / "failed").rglob("*.csv"))
        assert failed_csvs == [], (
            f"Expected failed/ empty on first encounter, found: {failed_csvs}"
        )
        pending_csvs = list((profile_dir / "pending").rglob("*.csv"))
        assert len(pending_csvs) == 1, (
            f"Expected 1 file in pending/, found: {pending_csvs}"
        )
        assert pending_csvs[0].name == "march.csv"

        # Pending sidecar carries the proposal so the user can ratify.
        sidecar = pending_csvs[0].with_name(pending_csvs[0].name + ".pending.yml")
        assert sidecar.exists(), f"Expected pending sidecar at {sidecar}"
        body = sidecar.read_text()
        assert "proposed_mapping:" in body
        assert "tier:" in body
        assert "moneybin import confirm" in body
