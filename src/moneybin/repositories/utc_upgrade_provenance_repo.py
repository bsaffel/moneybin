"""Bootstrap clock metadata, following the unaudited operational repository boundary."""

import duckdb

from moneybin.tables import UTC_UPGRADE_PROVENANCE


class UTCUpgradeProvenanceRepo:
    """Initialize immutable clock attribution without exposing user undo or edits."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Bind to the bootstrap connection before Database construction completes."""
        self._connection = connection

    def initialize_fresh(self) -> None:
        """Record the empty policy only after the caller verifies fresh UTC bootstrap."""
        self._connection.execute(
            f"""INSERT INTO {UTC_UPGRADE_PROVENANCE.full_name}
                (contract_version, rules) VALUES (?, ?)""",  # noqa: S608  # TableRef constant; values parameterized
            [2, "[]"],
        )
