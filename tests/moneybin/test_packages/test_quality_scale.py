"""Tests for Quality Scale tier validation."""

from pathlib import Path
from textwrap import dedent

import pytest

from moneybin.packages._framework.discovery import PackageInfo
from moneybin.packages._framework.manifest import PackageManifest
from moneybin.packages._framework.quality_scale import (
    validate_quality_scale,
)


def _make_package(
    tmp_path: Path, *, tier: str, code_owner: str | None = "Test Owner"
) -> PackageInfo:
    # Quoted so a whitespace-only value survives YAML parsing intact.
    code_owner_line = f'code_owner: "{code_owner}"\n' if code_owner is not None else ""
    manifest_yaml = f"""
name: test_synthetic
display_name: Test Synthetic
version: 1.0.0
quality_scale: {tier}
owns_prefix: test_synthetic
publisher: {{name: Test, verified: false}}
{code_owner_line}description: Test
capabilities: {{writes: [], reads: [], network: [], secrets: []}}
requires: {{moneybin: ">=1.0.0"}}
entry_points:
  tools: x:y
  cli: x:y
  models: x
  schema: x
"""
    (tmp_path / "moneybin_package.yaml").write_text(dedent(manifest_yaml).strip())
    manifest = PackageManifest.from_yaml(tmp_path / "moneybin_package.yaml")
    return PackageInfo(manifest=manifest, root=tmp_path)


def test_bronze_with_valid_manifest_passes(tmp_path: Path) -> None:
    info = _make_package(tmp_path, tier="bronze")
    violations = validate_quality_scale(info, claimed_tier="bronze")
    assert violations == []


def test_silver_requires_readme_and_tests(tmp_path: Path) -> None:
    info = _make_package(tmp_path, tier="silver")
    violations = validate_quality_scale(info, claimed_tier="silver")
    messages = [v.missing_evidence for v in violations]
    assert any("README.md" in m for m in messages)
    assert any("tests/" in m for m in messages)


def test_silver_with_readme_and_tests_passes(tmp_path: Path) -> None:
    info = _make_package(tmp_path, tier="silver")
    (tmp_path / "README.md").write_text("docs")
    (tmp_path / "tests").mkdir()
    violations = validate_quality_scale(info, claimed_tier="silver")
    assert violations == []


def test_gold_claim_reports_lower_tier_violations_cumulatively(tmp_path: Path) -> None:
    """Claiming gold with no silver AND no gold evidence reports both tiers' gaps."""
    info = _make_package(tmp_path, tier="gold")
    # No README/tests (silver), no metrics.py (gold).
    violations = validate_quality_scale(info, claimed_tier="gold")
    tiers = {v.claimed_tier for v in violations}
    assert "silver" in tiers, "cumulative check must surface the silver-tier gap"
    assert "gold" in tiers, "the claimed gold-tier gap must also surface"


def test_silver_whitespace_code_owner_rejected(tmp_path: Path) -> None:
    """A whitespace-only code_owner does not satisfy the Silver requirement."""
    info = _make_package(tmp_path, tier="silver", code_owner="   ")
    (tmp_path / "README.md").write_text("docs")
    (tmp_path / "tests").mkdir()
    violations = validate_quality_scale(info, claimed_tier="silver")
    assert any("code_owner" in v.missing_evidence for v in violations)


def test_silver_requires_code_owner(tmp_path: Path) -> None:
    """Silver fails when the manifest omits code_owner, even with README + tests."""
    info = _make_package(tmp_path, tier="silver", code_owner=None)
    (tmp_path / "README.md").write_text("docs")
    (tmp_path / "tests").mkdir()
    violations = validate_quality_scale(info, claimed_tier="silver")
    assert any("code_owner" in v.missing_evidence for v in violations)


def test_gold_requires_metrics_module(tmp_path: Path) -> None:
    info = _make_package(tmp_path, tier="gold")
    (tmp_path / "README.md").write_text("docs")
    (tmp_path / "tests").mkdir()
    violations = validate_quality_scale(info, claimed_tier="gold")
    assert any("metrics.py" in v.missing_evidence for v in violations)


def test_platinum_requires_scenarios_and_fixtures(tmp_path: Path) -> None:
    info = _make_package(tmp_path, tier="platinum")
    (tmp_path / "README.md").write_text("docs")
    tests = tmp_path / "tests"
    tests.mkdir()
    (tmp_path / "metrics.py").write_text("# observability")
    violations = validate_quality_scale(info, claimed_tier="platinum")
    msgs = [v.missing_evidence for v in violations]
    assert any("tests/scenarios" in m for m in msgs)
    assert any("tests/fixtures" in m for m in msgs)


def test_claiming_higher_than_declared_fails(tmp_path: Path) -> None:
    """A package claiming a tier above its manifest declaration is rejected."""
    info = _make_package(tmp_path, tier="bronze")
    with pytest.raises(ValueError, match="claimed tier 'gold' exceeds manifest"):
        validate_quality_scale(info, claimed_tier="gold")
