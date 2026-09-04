"""Sensitivity tiers, tool-call audit stubs, and the shared response-row cap.

Every classified surface — MCP tools, CLI ``--output json``, the reports
framework — declares a tier here, records the call, and caps how many rows
it may return. Homed beside the rest of ``moneybin.privacy`` rather than
under a transport package so no surface has to import another one to
classify its own output.
"""

import logging
from enum import StrEnum

from moneybin.config import get_settings
from moneybin.privacy.taxonomy import Tier

logger = logging.getLogger(__name__)


class Sensitivity(StrEnum):
    """Data sensitivity tier a classified response declares.

    Every MCP tool declares its maximum, and the privacy middleware uses it
    to enforce consent gates and response filtering. The same four values
    reach the CLI and the reports framework, which read them off
    ``summary.sensitivity`` on the shared envelope.

    See ``mcp-architecture.md`` section 5 for tier definitions.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


_TIER_TO_SENSITIVITY: dict[Tier, Sensitivity] = {
    Tier.LOW: Sensitivity.LOW,
    Tier.MEDIUM: Sensitivity.MEDIUM,
    Tier.HIGH: Sensitivity.HIGH,
    Tier.CRITICAL: Sensitivity.CRITICAL,
}

# Sensitivity itself is a StrEnum (its values are the wire literals), so it
# carries no numeric ordering of its own. Tier is already the codebase's one
# canonical severity ordering (`privacy/taxonomy.py`) — reuse it instead of
# defining a second ranking here.
_SENSITIVITY_TO_TIER: dict[Sensitivity, Tier] = {
    v: k for k, v in _TIER_TO_SENSITIVITY.items()
}


def tier_to_sensitivity(tier: Tier) -> Sensitivity:
    """Map a privacy ``Tier`` (numeric) to the MCP ``Sensitivity`` enum."""
    return _TIER_TO_SENSITIVITY[tier]


def sensitivity_to_tier(sensitivity: Sensitivity) -> Tier:
    """Map a ``Sensitivity`` back to its ``Tier`` for ordering comparisons.

    Used to floor (never override) one sensitivity value against another —
    e.g. an envelope's derived sensitivity against a tool's static declared
    ceiling — without inventing a second severity ranking beside ``Tier``.
    """
    return _SENSITIVITY_TO_TIER[sensitivity]


def log_tool_call(tool_name: str, sensitivity: Sensitivity) -> None:
    """Log an MCP tool invocation with its sensitivity tier.

    This is a privacy middleware stub. In v1, it only logs.
    When the consent management and audit log specs are implemented,
    this will check consent status, apply redaction, and write to
    the audit table.

    Args:
        tool_name: The registered tool name (underscore-joined).
        sensitivity: The tool's declared sensitivity tier.
    """
    logger.debug(f"MCP tool call: {tool_name} (sensitivity={sensitivity.value})")


def audit_log(
    *,
    tool: str,
    sensitivity: str,
    metadata: dict[str, object],
) -> None:
    """Record a privacy-relevant tool invocation with structured metadata.

    Stub implementation — writes a structured log entry at INFO level.
    When the audit log spec (privacy-data-protection.md) is implemented,
    this will persist to the audit table.

    Only counts, version strings, and filter parameters are allowed in
    metadata — never descriptions, IDs, or per-record content.

    Args:
        tool: Tool or command name (e.g. "transactions_categorize_assist").
        sensitivity: Sensitivity tier string ("low", "medium", "high").
        metadata: Count/parameter metadata. No PII or financial content.
    """
    logger.info(f"audit: tool={tool} sensitivity={sensitivity} metadata={metadata}")


def get_max_rows() -> int:
    """Return the configured cap for MCP query and report responses."""
    return get_settings().mcp.max_rows
