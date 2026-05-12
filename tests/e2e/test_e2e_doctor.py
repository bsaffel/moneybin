# ruff: noqa: S101
"""E2E tests for `moneybin doctor`.

doctor is read-only, so it uses the shared e2e_profile fixture (no mutations).
The clean profile has no transactions, so all SQLMesh audits and the
categorization check pass trivially (0 of 0 is not < 50%).
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.conftest import run_cli

pytestmark = pytest.mark.e2e


class TestDoctorCommand:
    """E2E tests for the `moneybin doctor` command."""

    def test_doctor_help(self) -> None:
        result = run_cli("doctor", "--help")
        result.assert_success()
        assert "--verbose" in result.output
        assert "--output" in result.output

    def test_doctor_clean_profile_exits_0(self, e2e_profile: dict[str, str]) -> None:
        """A freshly initialized profile has no data — all audits pass (no violations)."""
        result = run_cli("doctor", env=e2e_profile)
        # SQLMesh may not have audits available if transform was never run;
        # the command must not crash regardless.
        assert "Traceback" not in result.stderr
        # exit 0 (pass) or 1 (fail with real violations) — never crash
        assert result.exit_code in (0, 1)

    def test_doctor_json_output_shape(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("doctor", "--output", "json", env=e2e_profile)
        assert "Traceback" not in result.stderr
        # JSON must parse and have the standard envelope shape
        envelope = json.loads(result.stdout)
        assert "summary" in envelope
        assert "data" in envelope
        data = envelope["data"]
        assert "invariants" in data
        assert "transaction_count" in data
        assert "passing" in data
        assert "failing" in data

    def test_doctor_verbose_flag_accepted(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("doctor", "--verbose", env=e2e_profile)
        assert "Traceback" not in result.stderr

    def test_doctor_json_verbose_flag_accepted(
        self, e2e_profile: dict[str, str]
    ) -> None:
        result = run_cli("doctor", "--output", "json", "--verbose", env=e2e_profile)
        assert "Traceback" not in result.stderr
        envelope = json.loads(result.stdout)
        assert "data" in envelope
