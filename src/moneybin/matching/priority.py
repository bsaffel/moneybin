"""Rebuild app.seed_source_priority from MatchingSettings.

The priority table is a SQL-accessible projection of the config-only
source_priority list. It is rebuilt on every matcher run so config is
always the sole source of truth.
"""

import logging

from moneybin.config import MatchingSettings
from moneybin.database import Database

logger = logging.getLogger(__name__)


def seed_source_priority(db: Database, settings: MatchingSettings) -> None:
    """Rebuild the source priority table from config.

    Deletes all existing rows and reinserts from the settings list.
    This is safe because the table is never user-edited — config owns it.
    """
    db.execute("DELETE FROM app.seed_source_priority")
    rows = [
        [source_type, rank]
        for rank, source_type in enumerate(settings.source_priority, start=1)
    ]
    if rows:
        db.executemany(
            "INSERT INTO app.seed_source_priority (source_type, priority) VALUES (?, ?)",
            rows,
        )
    logger.debug(
        f"Seeded source priority: {len(settings.source_priority)} source types"
    )
