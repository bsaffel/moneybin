"""End-to-end: drop a CSV in the inbox, drain it, assert it lands in processed/."""

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
    """Inbox drain: file dropped in inbox/<slug>/ moves to processed/ after sync."""

    def test_inbox_workflow(self, e2e_home: Path) -> None:
        """CSV placed in inbox/<account-slug>/ is imported and archived to processed/."""
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

        processed_csvs = list((profile_dir / "processed").rglob("*.csv"))
        assert len(processed_csvs) == 1, (
            f"Expected 1 file in processed/, found: {processed_csvs}"
        )
        assert processed_csvs[0].name == "march.csv"
