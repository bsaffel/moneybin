"""Dependency-neutral field-length caps.

A cap belongs to two layers at once: the request contract declares it as a
``max_length``, and the service validator enforces it on the way to DuckDB,
whose ``VARCHAR`` is unbounded. Homed beside ``vocabulary`` so both can read
the same number without either importing the other — a contract that reached
into ``services`` for its own bounds would invert the layering, and a second
copy of the number is how the two spellings drift apart.
"""

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
