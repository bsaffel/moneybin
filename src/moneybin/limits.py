"""Dependency-neutral field bounds.

A cap belongs to two layers at once: the request contract declares it as a
``max_length``, and the service validator enforces it on the way to DuckDB,
whose ``VARCHAR`` is unbounded. Homed beside ``vocabulary`` so both can read
the same number without either importing the other — a contract that reached
into ``services`` for its own bounds would invert the layering, and a second
copy of the number is how the two spellings drift apart.

The same argument holds for a numeric range that more than one boundary has to
enforce, so those live here too.
"""

from decimal import ROUND_HALF_UP, Decimal

IDENTIFIER_MAX_LEN = 64
CATEGORY_NAME_MAX_LEN = 100
MERCHANT_NAME_MAX_LEN = 200
MERCHANT_PATTERN_MAX_LEN = 500
DESCRIPTION_MAX_LEN = 2000
#: Also bounds a report's stored reclassify ``reason`` — an audit annotation.
NOTE_MAX_LEN = 2000
SLUG_MAX_LEN = 100
#: A saved report's stored SELECT. Generous next to the others because a real
#: analytical query with CTEs legitimately runs to a few thousand characters —
#: but bounded, because DuckDB's VARCHAR is not, and every catalog read,
#: `reports explain`, and export receipt renders this text again.
REPORT_QUERY_MAX_LEN = 20_000
#: A saved report's serialized `params` declaration block, not one field of it:
#: a declared default, a help string, and the number of parameters all land in
#: the same JSON column, and the total is what the row stores, the catalog
#: republishes, and every later mutation copies into its audit images. One
#: parameter serializes to roughly 60-100 characters, so this admits dozens with
#: generous defaults while keeping all three bounded.
REPORT_PARAMS_MAX_LEN = 4_000
#: A saved report's serialized `class_downgrades` block, not one entry of it.
#: `reason` is already bounded per entry by `NOTE_MAX_LEN`, but the map grows one
#: entry per downgraded column and the whole of it is copied into the before/after
#: images every later mutation audits. An entry serializes to roughly 60
#: characters plus its reason, so this admits four maximum-length reasons or
#: around sixty ordinary one-sentence ones — well past any report that has had a
#: human confirm a downgrade per column.
REPORT_DOWNGRADES_MAX_LEN = 8_000
#: Categorization-rule priority, lower runs first. Declared once because four
#: boundaries enforce it — the rule-creation model, the declarative target
#: contract, the MCP conflict-resolution request, and both CLI resolution paths.
RULE_PRIORITY_MIN = 0
RULE_PRIORITY_MAX = 10_000


#: The grain ``app.categorization_rules`` and ``app.rule_conflicts`` store an
#: amount bound at (``DECIMAL(18,2)``).
AMOUNT_GRAIN = Decimal("0.01")


def to_amount_grain(value: Decimal | float | int | None) -> Decimal | None:
    """Coerce an amount bound to the grain DuckDB will store it at.

    Every write path calls this before binding, so the column only ever sees a
    value already at its own scale and DuckDB never rounds. That collapse is
    load-bearing, not tidiness: DuckDB's ``DECIMAL(18,2)`` cast rounds a
    ``DOUBLE`` and a ``DECIMAL`` differently at a half cent — ``5.015`` bound as
    a float scales through ``5.015 * 100`` and stores ``5.01``, bound as a
    ``Decimal`` it stores ``5.02`` — so a bound's stored value used to depend on
    which surface wrote it, and no single rounding mode could reproduce both.
    With one path, matcher identity can round the same way and match, instead of
    canonicalizing a rule to a bound it does not actually store.

    ``Decimal(str(value))`` reads a float's shortest round-trip text, as
    ``write_contracts._coerce_finite_json_number`` does, so ``5.005`` is read as
    ``"5.005"`` rather than its ``5.00499…`` binary expansion. ``ROUND_HALF_UP``
    is the house money mode (``CurrencyService``, ``cost_basis``) and is what
    DuckDB's decimal cast does.
    """
    if value is None:
        return None
    return Decimal(str(value)).quantize(AMOUNT_GRAIN, rounding=ROUND_HALF_UP)
