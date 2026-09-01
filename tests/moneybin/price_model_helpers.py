"""Read the price models' declared vocabulary rather than restating it in tests.

Shared by the staging model's own tests and by the price service's, because the
guard that matters spans both: `PriceService` writes `raw.security_prices` rows,
and `prep.stg_security_prices` resolves them through the ref_kind
`seeds.price_source_map` declares, with an INNER JOIN. A source the registry
does not map is discarded silently and permanently. Deriving from the registry
and from the model files — rather than restating either — is what makes editing
one extend what the tests exercise.
"""

from __future__ import annotations

import re
from pathlib import Path

import moneybin
from moneybin.price_sources import REF_KIND_BY_SOURCE_TYPE

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


def close_source_ctes() -> set[str]:
    """The CTEs whose rows are unioned into `core.fct_security_prices.close`.

    Parsed rather than restated because this set is exactly what determines what
    the resolved `close` column can contain, and therefore how it must be
    classified. `trade_implied` was added without revisiting that classification,
    which put the user's own execution price behind a column documented as public
    reference data — deriving the set means the next union member cannot land the
    same way silently.
    """
    sql = FCT_PRICES_PATH.read_text()
    block = re.search(r"\), candidates AS \((.*?)\n\), ", sql, re.DOTALL)
    assert block is not None, (
        f"no `candidates AS (...)` CTE found in {FCT_PRICES_PATH.name}; the union "
        "that decides what `close` can contain may have moved or been renamed"
    )
    sources = set(re.findall(r"\n  FROM (\w+)\b", block.group(1)))
    assert sources, "no `FROM <cte>` branches parsed out of the candidates union"
    return sources


def historical_reversal_actor() -> str:
    """The ``reversed_by`` value the staging model treats as a retirement.

    A retired feed key and a user-rejected one are both ``status = 'reversed'``,
    and only ``reversed_by`` separates them: retirement is bookkeeping after a
    ticker moved, so the closes stored under the old symbol were still this
    security's prices; a user reversal is a judgement that the pairing was wrong,
    so its closes must drop. The model names that actor as a SQL literal while
    the service writes it from ``price_service._AUTO_REVERSAL``. Parsing it back
    out is what stops the two from drifting into silently resurrecting — or
    silently erasing — a price series.

    Two predicates name it — the join arm that resolves a retired key's own
    history, and the handover CTE that hands the interval after a retirement to
    the next owner. Returning a single value asserts they agree, because they
    fail in opposite directions: the join arm drifting reads as "this security
    has no history", while the handover CTE drifting turns a *user's* rejection
    into a transfer of ownership, giving the next holder a series the user
    disowned. A model that deliberately wants two different actors has made a
    design change this helper should stop.
    """
    sql = MODEL_PATH.read_text()
    literals = set(re.findall(r"\breversed_by\s*=\s*'([^']+)'", sql))
    assert literals, (
        f"no `reversed_by = '...'` predicate found in {MODEL_PATH.name}; "
        "either the retired-link arm was dropped — which erases a renamed "
        "security's price history from core — or it no longer discriminates "
        "retirement from a user's rejection, which resurrects prices the user "
        "rejected"
    )
    assert len(literals) == 1, (
        f"{MODEL_PATH.name} names more than one reversal actor {sorted(literals)}; "
        "the retired-history arm and the handover CTE must agree on which "
        "reversal transfers ownership, or one of them is silently reading a "
        "user's rejection as a rename"
    )
    return literals.pop()


def ref_kind_mapping() -> dict[str, str]:
    """The (source_type -> ref_kind) pairs the model actually resolves through.

    Read from the registry rather than parsed out of the model, because the
    model no longer restates the mapping: it joins ``seeds.price_source_map``,
    which SQLMesh builds from the same CSV ``moneybin.price_sources`` loads.
    The property the parsing version bought — that editing the mapping edits
    what these tests exercise — is stronger this way, since the registry is now
    the only place either side can be edited.
    """
    assert REF_KIND_BY_SOURCE_TYPE, "the price-source registry maps no ref_kinds"
    sql = MODEL_PATH.read_text()
    assert re.search(r"\bJOIN\s+seeds\.price_source_map\b", sql), (
        f"{MODEL_PATH.name} no longer JOINs the registry, so a mapping read from "
        "it is no longer evidence about what the model resolves. Matching the "
        "bare table name would not catch this — it appears in this model's prose "
        "independently of the join."
    )
    assert not re.search(r"CASE\s+p\.source_type", sql), (
        f"{MODEL_PATH.name} resolves ref_kind through a restated CASE again; the "
        "registry and the model can now disagree, which is the split that "
        "discarded every row C.2's adapters wrote"
    )
    return dict(REF_KIND_BY_SOURCE_TYPE)
