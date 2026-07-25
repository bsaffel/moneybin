"""Read the price-staging model's source -> ref_kind mapping out of the model file.

Shared by the staging model's own tests and by the price service's, because the
guard that matters spans both: `PriceService` writes `raw.security_prices` rows,
and `prep.stg_security_prices` resolves them through a per-`source_type` CASE
with an INNER JOIN. A source the CASE does not map is discarded silently and
permanently. Deriving the mapping from the model — rather than restating it —
is what makes editing the CASE extend what the tests exercise.
"""

from __future__ import annotations

import re
from pathlib import Path

import moneybin

MODEL_PATH = (
    Path(moneybin.__file__).parent
    / "sqlmesh"
    / "models"
    / "prep"
    / "stg_security_prices.sql"
)

FCT_PRICES_PATH = (
    Path(moneybin.__file__).parent
    / "sqlmesh"
    / "models"
    / "core"
    / "fct_security_prices.sql"
)


def trade_implied_types() -> set[str]:
    """The ledger types `core.fct_security_prices` accepts as a market close.

    Parsed rather than restated for the same reason as ``ref_kind_mapping``: the
    set the model actually applies is the only one worth asserting against, and
    reading it from the file makes a change to the filter change what the tests
    exercise.
    """
    sql = FCT_PRICES_PATH.read_text()
    block = re.search(r"trade_implied AS \((.*?)\n\), ", sql, re.DOTALL)
    assert block is not None, (
        f"no `trade_implied AS (...)` CTE found in {FCT_PRICES_PATH.name}; the "
        "ledger-type filter these tests guard may have moved or been renamed"
    )
    predicate = re.search(r"t\.type IN \(([^)]*)\)", block.group(1), re.DOTALL)
    assert predicate is not None, (
        "the trade_implied CTE applies no `t.type IN (...)` filter, so every "
        "ledger event carrying a security and a price becomes a market close — "
        "including dividend, fee, and capital_gain_distribution, whose `price` "
        "is a per-share distribution rate, not a traded price"
    )
    return set(re.findall(r"'([^']+)'", predicate.group(1)))


def ref_kind_mapping() -> dict[str, str]:
    """The (source_type -> ref_kind) pairs the model's CASE actually maps."""
    sql = MODEL_PATH.read_text()
    case_blocks = re.findall(r"CASE\s+p\.source_type(.*?)\bEND\b", sql, re.DOTALL)
    assert len(case_blocks) == 1, (
        f"expected exactly one `CASE p.source_type` in {MODEL_PATH.name}; a second one "
        f"means ref_kind resolution forked and these tests no longer cover it: "
        f"{case_blocks}"
    )
    mapping = dict(re.findall(r"WHEN\s+'([^']+)'\s+THEN\s+'([^']+)'", case_blocks[0]))
    assert mapping, "no WHEN ... THEN pairs parsed out of the ref_kind CASE"
    return mapping
