"""SimpleFIN native events remain distinct across immutable delivery batches."""

from collections.abc import Iterable

from moneybin.database import Database
from moneybin.tables import SIMPLEFIN_TRANSACTIONS


def native_event_scopes(
    db: Database, references: Iterable[str]
) -> dict[str, tuple[str, str]]:
    """Read only needed native identities, including observations currently held."""
    values = sorted(set(references))
    if not values:
        return {}
    rows = db.execute(
        f"""SELECT DISTINCT transaction_reference, source_origin, account_id
            FROM {SIMPLEFIN_TRANSACTIONS.full_name}
            WHERE transaction_reference IN (SELECT UNNEST(?::VARCHAR[]))""",  # noqa: S608  # trusted TableRef and bound native references
        [values],
    ).fetchall()
    scopes: dict[str, tuple[str, str]] = {}
    ambiguous: set[str] = set()
    for reference, origin, account in rows:
        scope = (str(origin), str(account))
        if reference in scopes and scopes[reference] != scope:
            ambiguous.add(reference)
        scopes[reference] = scope
    return {ref: scope for ref, scope in scopes.items() if ref not in ambiguous}
