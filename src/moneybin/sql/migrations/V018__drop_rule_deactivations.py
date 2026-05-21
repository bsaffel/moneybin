"""Drop the retired app.rule_deactivations table.

The rule_deactivations table tracked Phase 3 metadata for the auto-rule
override-threshold flow: when a rule was deactivated due to user overrides,
the system recorded the "winning bucket" (most common alternative category)
here to support automatic re-proposal of a replacement rule.

Phase 3 (re-proposal logic) was removed because the system didn't yet have
signal on what shape the replacement rule should take. The safety property
(deactivate bad rules after N user corrections) is preserved; deactivation
events are now emitted to app.audit_log as 'rule_deactivated' events.

Idempotent: DROP TABLE IF EXISTS is a no-op when the table is already absent.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop app.rule_deactivations."""
    conn.execute("DROP TABLE IF EXISTS app.rule_deactivations")  # type: ignore[union-attr]  # noqa: S608  # allowlisted literal
    logger.info("V018: dropped app.rule_deactivations")
