"""Tests for the OFX institution resolution chain."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest import LogCaptureFixture

from moneybin.extractors.institution_resolution import (
    InstitutionResolutionError,
    resolve_institution,
)


def _ofx_with(org: str | None = None, fid: str | None = None) -> MagicMock:
    """Build a mock parsed-OFX object with one account whose institution has the given org/fid."""
    inst = MagicMock()
    inst.organization = org
    inst.fid = fid
    account = MagicMock()
    account.institution = inst
    ofx = MagicMock()
    ofx.accounts = [account]
    return ofx


class TestResolveInstitution:
    """Tests for the resolve_institution resolution chain."""

    def test_uses_fi_org_when_present(self) -> None:
        ofx = _ofx_with(org="WELLS FARGO BANK", fid="3000")
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/whatever.qfx"),  # noqa: S108
            cli_override=None,
            interactive=False,
        )
        assert result == "wells_fargo_bank"

    def test_falls_back_to_fid_lookup(self) -> None:
        ofx = _ofx_with(org=None, fid="3000")  # 3000 = wells_fargo per static table
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/whatever.qfx"),  # noqa: S108
            cli_override=None,
            interactive=False,
        )
        assert result == "wells_fargo"

    def test_falls_back_to_filename_heuristic(self) -> None:
        ofx = _ofx_with(org=None, fid=None)
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/chase_2026.qfx"),  # noqa: S108
            cli_override=None,
            interactive=False,
        )
        assert result == "chase"

    def test_uses_cli_override_when_chain_empty(self) -> None:
        ofx = _ofx_with(org=None, fid=None)
        result = resolve_institution(
            ofx,
            file_path=Path("/tmp/anonymous.qfx"),  # noqa: S108
            cli_override="Local Credit Union",
            interactive=False,
        )
        assert result == "local_credit_union"

    def test_cli_override_logs_ignored_when_file_has_org(
        self, caplog: LogCaptureFixture
    ) -> None:
        ofx = _ofx_with(org="Wells Fargo", fid=None)
        with caplog.at_level("INFO"):
            result = resolve_institution(
                ofx,
                file_path=Path("/tmp/x.qfx"),  # noqa: S108
                cli_override="Other Bank",
                interactive=False,
            )
        assert result == "wells_fargo"
        assert any("ignored" in r.message.lower() for r in caplog.records)

    def test_raises_in_non_interactive_mode_when_chain_empty(self) -> None:
        ofx = _ofx_with(org=None, fid=None)
        with pytest.raises(InstitutionResolutionError):
            resolve_institution(
                ofx,
                file_path=Path("/tmp/anonymous.qfx"),  # noqa: S108
                cli_override=None,
                interactive=False,
            )
