"""Rebuild app.seed_source_priority from MatchingSettings.

The priority table is a SQL-accessible projection of the config-only
source_priority list. It is rebuilt on every matcher run so config is
always the sole source of truth.
"""

import logging

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.tables import SEED_SOURCE_PRIORITY

logger = logging.getLogger(__name__)


def seed_source_priority(db: Database, settings: MatchingSettings) -> None:
    """Rebuild the source priority table from config.

    Deletes all existing rows and reinserts from the settings list.
    This is safe because the table is never user-edited — config owns it.
    """
    db.execute(f"DELETE FROM {SEED_SOURCE_PRIORITY.full_name}")  # noqa: S608  # TableRef constant
    rows = [
        [source_type, rank]
        for rank, source_type in enumerate(settings.source_priority, start=1)
    ]
    if rows:
        db.executemany(
            f"INSERT INTO {SEED_SOURCE_PRIORITY.full_name} (source_type, priority) VALUES (?, ?)",  # noqa: S608  # TableRef constant
            rows,
        )
    logger.debug(
        f"Seeded source priority: {len(settings.source_priority)} source types"
    )
