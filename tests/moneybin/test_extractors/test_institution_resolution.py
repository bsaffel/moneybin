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


def test_resolve_institution_tabular_filename_then_unknown(tmp_path: Path) -> None:
    """Tabular institution is best-effort: filename heuristic hits; unknown is allowed.

    Unlike the OFX chain, an unknown institution returns None rather than raising —
    institution is optional metadata for tabular imports (spec Decision 3 / 7).
    """
    from moneybin.extractors.institution_resolution import resolve_institution_tabular

    hit = resolve_institution_tabular(
        file_path=Path("wells_fargo_2024.csv"),
        format_institution=None,
        cli_override=None,
    )
    assert hit == "wells_fargo"

    miss = resolve_institution_tabular(
        file_path=Path("export.csv"), format_institution=None, cli_override=None
    )
    assert miss is None  # unknown allowed — no InstitutionResolutionError


class TestSharedInstitutionRegistry:
    """The FID mapping has one home, shared with the seeds.institutions model.

    core.dim_accounts joins the same CSV to resolve a human-readable
    institution_name. A second copy in Python would drift the first time
    someone added a bank to one and not the other.
    """

    def test_every_registry_fid_resolves(self) -> None:
        """Each FID in the shared CSV resolves via the FID branch of the chain."""
        from moneybin.extractors.institution_resolution import (
            _fid_to_slug,  # noqa: PLC0415  # pyright: ignore[reportPrivateUsage]
        )

        registry = _fid_to_slug()
        assert registry, "institution registry loaded empty"

        for fid, slug in registry.items():
            result = resolve_institution(
                _ofx_with(org=None, fid=fid),
                file_path=Path("/tmp/anonymous.qfx"),  # noqa: S108
                cli_override=None,
                interactive=False,
            )
            assert result == slug, f"FID {fid!r} resolved to {result!r}, not {slug!r}"

    def test_registry_carries_a_display_name_for_every_slug(self) -> None:
        """seeds.institutions is also the display-name source; no row may lack one."""
        import csv  # noqa: PLC0415
        import io  # noqa: PLC0415
        from importlib import resources  # noqa: PLC0415

        raw = (
            resources
            .files("moneybin")
            .joinpath("sqlmesh/models/seeds/institutions.csv")
            .read_text()
        )
        rows = list(csv.DictReader(io.StringIO(raw)))
        assert rows, "institution registry CSV is empty"
        for row in rows:
            assert row["display_name"].strip(), (
                f"FID {row['fid']!r} has no display_name; core.dim_accounts would "
                "fall back to the raw <ORG> code"
            )

    def test_registry_fids_survive_numeric_csv_inference(self) -> None:
        """Every fid must round-trip through pandas' numeric column inference.

        SQLMesh reads the seed CSV with pandas, which types an all-digit column
        as int64 regardless of the model's declared `fid TEXT`, then casts back
        to string. Two ways that silently breaks the dim_accounts join, both
        invisible without this guard:

        - A leading zero is lost (`0301` -> `301`), so that one institution
          never matches its file's <FID>.
        - A single blank fid flips the whole column to float64, so EVERY value
          gains a `.0` and the join misses for *all* institutions at once —
          every OFX account silently reverts to showing the raw <ORG> code.

        Keeping fids strictly all-digit with no leading zero makes the
        round-trip lossless. If an institution ever needs a non-numeric fid,
        this guard must be replaced by forcing the column's dtype, not relaxed.
        """
        import csv  # noqa: PLC0415
        import io  # noqa: PLC0415
        from importlib import resources  # noqa: PLC0415

        raw = (
            resources
            .files("moneybin")
            .joinpath("sqlmesh/models/seeds/institutions.csv")
            .read_text()
        )
        fids = [row["fid"] for row in csv.DictReader(io.StringIO(raw))]
        assert fids, "institution registry CSV is empty"

        for fid in fids:
            assert fid.strip(), (
                "a blank fid flips the seed column to float64 and breaks the "
                f"FID join for every institution; got {fids!r}"
            )
            assert fid.isdigit(), f"fid {fid!r} is not all-digit"
            assert fid == str(int(fid)), (
                f"fid {fid!r} has a leading zero, which pandas strips on read"
            )
