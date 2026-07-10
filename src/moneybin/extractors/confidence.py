"""Channel-agnostic confidence contract for smart-import detection.

A uniform shape that every smart-import channel (tabular, gsheet, PDF)
emits for its column-mapping or extraction result. Downstream code
(gating, prompt ergonomics, agent autonomy) branches on this contract
rather than on which channel produced it.
"""

from dataclasses import dataclass
from typing import Literal

Tier = Literal["high", "medium", "low"]


def tier_for(score: float, *, t_high: float, t_med: float) -> Tier:
    """Band a normalized score into a tier using shared thresholds.

    Args:
        score: Normalized confidence in [0, 1].
        t_high: Lower bound of the `high` band (inclusive).
        t_med: Lower bound of the `medium` band (inclusive).

    Raises:
        ValueError: If score is outside [0, 1] or t_high < t_med.
    """
    if not 0.0 <= score <= 1.0:
        raise ValueError(f"score must be in [0, 1], got {score}")
    if t_high < t_med:
        raise ValueError(
            f"t_high must be >= t_med (got t_high={t_high}, t_med={t_med})"
        )
    if score >= t_high:
        return "high"
    if score >= t_med:
        return "medium"
    return "low"


def resolve_tier(
    score: float, *, t_high: float, t_med: float, structural_red_flag: bool = False
) -> Tier:
    """Band a score into a tier, but force ``low`` on a structural red flag.

    The single place the structural-override rule lives, so ``map_columns``
    (which stores the tier) and ``MappingResult.to_confidence`` (which recomputes
    it) can never disagree. A structural red flag (e.g. the consumed header row
    parses as a transaction) means the mapping is untrustworthy regardless of
    how well content matching scored — route it to the confirm gate.

    Args:
        score: Normalized confidence in [0, 1].
        t_high: Lower bound of the `high` band (inclusive).
        t_med: Lower bound of the `medium` band (inclusive).
        structural_red_flag: When True, return `low` regardless of score.
    """
    if structural_red_flag:
        return "low"
    return tier_for(score, t_high=t_high, t_med=t_med)


@dataclass(frozen=True)
class Confidence:
    """Cross-channel confidence value.

    `score` drives gating math; `tier` drives ergonomics + agent autonomy.
    `flagged` names fields a human should eyeball (matched weakly).
    `missing_required` names required destination fields not resolved at all —
    a `low` result MUST list these so callers know what to supply.
    """

    score: float
    tier: Tier
    flagged: tuple[str, ...]
    missing_required: tuple[str, ...]
