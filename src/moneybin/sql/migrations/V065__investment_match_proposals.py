"""V065: add durable review-only investment-match Proposals."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def migrate(conn: Any) -> None:
    """Add the Proposal lifecycle without modifying source or Golden rows."""
    logger.debug("V065: CREATE TABLE app.investment_match_decisions")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app.investment_match_decisions (
            proposal_id VARCHAR PRIMARY KEY,
            algorithm_version VARCHAR NOT NULL,
            relationship_fingerprint VARCHAR NOT NULL,
            candidate_graph_fingerprint VARCHAR NOT NULL,
            confidence_band VARCHAR NOT NULL CHECK (confidence_band IN ('native', 'exact', 'fuzzy')),
            status VARCHAR NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected', 'stale', 'reversed')),
            auto_eligible BOOLEAN NOT NULL,
            is_competing BOOLEAN NOT NULL,
            proposal JSON NOT NULL,
            actor VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            decided_at TIMESTAMP,
            accepted_at TIMESTAMP
        )
    """)
