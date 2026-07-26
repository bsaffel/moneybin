"""V044: create app.profile_settings.

Profile-level user settings, holding the home currency
(``docs/specs/multi-currency.md`` Requirement 4). The setting must be
DB-resident rather than YAML config because the no-blend guard and the
report views that read it are SQLMesh models.

Numbered V044 rather than V042: the unlanded ``feat/investment-price-feeds-c2``
branch already claims V042 and V043.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.profile_settings (
    scope         VARCHAR NOT NULL PRIMARY KEY DEFAULT 'profile'
                  CHECK (scope = 'profile'),
    home_currency VARCHAR,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "scope",
        "Singleton guard: exactly one settings row per profile database",
    ),
    (
        "home_currency",
        "ISO 4217 home currency; NULL means not yet chosen, never an implied USD",
    ),
    ("updated_at", "Last settings mutation timestamp"),
]


def migrate(conn: object) -> None:
    """Create app.profile_settings and apply catalog comments."""
    logger.debug("V044: CREATE TABLE IF NOT EXISTS app.profile_settings")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.profile_settings.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.debug("V044: app.profile_settings ready")
