"""Quality Scale tier validation per spec §"Type-specific requirements".

Each tier name (bronze/silver/gold/platinum) is paired with mechanical checks
the framework can run at registration: file existence, directory structure,
module-level metrics registration. Harder checks (signed releases, scenario
coverage thresholds) defer to post-launch hardening per spec §"Launch
shipping posture".

The current validator covers analysis packages. Type-specific variants
(reports, providers) follow the same pattern but differ in evidence; their
implementations land alongside the standalone-report scaffolding (Plan 3)
and provider Platinum-ifying (Plan 6) respectively.
"""

from __future__ import annotations

from collections.abc import Callable

from moneybin.packages._framework.discovery import PackageInfo
from moneybin.packages._framework.errors import QualityScaleViolation
from moneybin.packages._framework.manifest import QualityTier

_TIER_ORDER: dict[str, int] = {
    "bronze": 1,
    "silver": 2,
    "gold": 3,
    "platinum": 4,
}


def _bronze_checks(info: PackageInfo) -> list[QualityScaleViolation]:  # noqa: ARG001  # signature parity with other tier checks
    # Bronze is implicit: manifest parsed + capabilities + prefix validated upstream.
    # Returning [] keeps the symmetry — caller can iterate tiers uniformly.
    return []


def _silver_checks(info: PackageInfo) -> list[QualityScaleViolation]:
    violations: list[QualityScaleViolation] = []
    if not (info.root / "README.md").exists():
        violations.append(
            QualityScaleViolation(
                package_name=info.manifest.name,
                message="Silver requires a README.md at the package root",
                claimed_tier="silver",
                missing_evidence="README.md not found",
            )
        )
    if not (info.root / "tests").is_dir():
        violations.append(
            QualityScaleViolation(
                package_name=info.manifest.name,
                message="Silver requires a tests/ directory at the package root",
                claimed_tier="silver",
                missing_evidence="tests/ directory not found",
            )
        )
    return violations


def _gold_checks(info: PackageInfo) -> list[QualityScaleViolation]:
    violations: list[QualityScaleViolation] = []
    # Gold requires observability — a metrics module the package owns.
    # The package's metrics module registers via moneybin.metrics.registry
    # (see docs/specs/observability.md). At registration we mechanically
    # check the file exists; runtime emit-detection is post-launch hardening.
    if not (info.root / "metrics.py").exists():
        violations.append(
            QualityScaleViolation(
                package_name=info.manifest.name,
                message="Gold requires a metrics.py module emitting via moneybin.metrics.registry",
                claimed_tier="gold",
                missing_evidence="metrics.py not found",
            )
        )
    # User guide at docs/guides/packages/<name>/ — checked at the repo level,
    # not against info.root, because the docs tree is shared.
    # For Plan 2 we skip this; Plan 4 wires it when the user-guide convention
    # is in scope. Flagging as a known-deferred check below in the
    # registry.py comment block.
    return violations


def _platinum_checks(info: PackageInfo) -> list[QualityScaleViolation]:
    violations: list[QualityScaleViolation] = []
    tests = info.root / "tests"
    if not (tests / "scenarios").is_dir():
        violations.append(
            QualityScaleViolation(
                package_name=info.manifest.name,
                message="Platinum requires tests/scenarios/ for scenario-test coverage",
                claimed_tier="platinum",
                missing_evidence="tests/scenarios/ not found",
            )
        )
    if not (tests / "fixtures").is_dir():
        violations.append(
            QualityScaleViolation(
                package_name=info.manifest.name,
                message=(
                    "Platinum requires tests/fixtures/ holding version-pinned "
                    "regression data"
                ),
                claimed_tier="platinum",
                missing_evidence="tests/fixtures/ not found",
            )
        )
    return violations


_CHECKS: dict[QualityTier, Callable[[PackageInfo], list[QualityScaleViolation]]] = {
    "bronze": _bronze_checks,
    "silver": _silver_checks,
    "gold": _gold_checks,
    "platinum": _platinum_checks,
}


def validate_quality_scale(
    info: PackageInfo, claimed_tier: str
) -> list[QualityScaleViolation]:
    """Validate the package's evidence at `claimed_tier`.

    claimed_tier must not exceed the manifest's declared quality_scale (the
    validator cannot inflate a claim; claiming a lower tier is allowed). Each
    tier's check is cumulative: claiming Gold runs Bronze + Silver + Gold
    checks. Returns every violation found so contributors see the full gap.

    Raises:
        ValueError: if claimed_tier is unknown or exceeds the manifest's
            declared quality_scale (callers cannot ask for a tier higher
            than what the package itself claims).
    """
    if claimed_tier not in _TIER_ORDER:
        raise ValueError(
            f"Unknown tier '{claimed_tier}'; expected bronze/silver/gold/platinum"
        )
    declared_rank = _TIER_ORDER[info.manifest.quality_scale]
    claimed_rank = _TIER_ORDER[claimed_tier]
    if claimed_rank > declared_rank:
        raise ValueError(
            f"claimed tier '{claimed_tier}' exceeds manifest declaration "
            f"'{info.manifest.quality_scale}'"
        )

    violations: list[QualityScaleViolation] = []
    for tier in sorted(_TIER_ORDER, key=_TIER_ORDER.__getitem__):
        if _TIER_ORDER[tier] > claimed_rank:
            break
        violations.extend(_CHECKS[tier](info))  # type: ignore[literal-required]  # tier is str from sorted(); _CHECKS is keyed by QualityTier Literal
    return violations
