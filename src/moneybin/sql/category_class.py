"""Shared accounting-class-from-category_id backfill rule.

Both ``V032__add_category_source_map_and_class.py`` (initial backfill of
``seeds.categories.class``) and ``moneybin.seeds.refresh_views`` (pre-V032
shape tolerance for databases opened with migrations skipped) need to derive
``class`` from a ``category_id`` prefix identically. This constant is the
single source of truth for that rule so the two call sites never drift apart
— see ``.claude/rules/design-principles.md`` "Coherence."
"""

from __future__ import annotations

CATEGORY_CLASS_FROM_ID_CASE_SQL = """CASE
    WHEN category_id LIKE 'INC%' THEN 'income'
    WHEN category_id LIKE 'TRN%' THEN 'transfer'
    WHEN category_id LIKE 'LNP%' THEN 'debt'
    ELSE 'expense'
END"""
