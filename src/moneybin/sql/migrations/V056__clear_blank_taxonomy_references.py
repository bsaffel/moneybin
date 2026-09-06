"""V056: remove blank taxonomy rows, and every reference to them.

V054 and V055 null a blank ``category``/``subcategory`` on
``app.transaction_splits``, ``app.user_merchants`` and
``app.transaction_categories``. Those are the columns V014 marks
``DEPRECATED`` — Phase 1 display snapshots — while ``category_id`` is
documented as the canonical reference and is what Phase 2 keeps. Every
consumer already prefers it::

    core.dim_merchants      COALESCE(dc.category, um.category)
    core.fct_transactions   COALESCE(dc.category, c.category, t.category)
    core.fct_transactions   splits: COALESCE(sdc.category, s.category)

so a row whose ``category_id`` still points at a blank ``app.user_categories``
entry renders the blank regardless of the nulled snapshot: ``COALESCE('   ',
NULL)`` is ``'   '``. Without this migration V054 and V055 repair the column
that is going away and leave the one that matters, and their effect
disappears entirely when Phase 2 drops the snapshots.

A blank taxonomy row is reachable on any database written before this PR:
``create_category`` had no text rule, and ``resolve_category_id`` matches on
exact text, so a blank category name resolved to it and stamped its id onto
every writer that named it.

Blank on either axis makes the row unusable: ``dim_categories`` projects the
whitespace on whichever axis carries it, and the pair is the one
``validate_category_hierarchy`` now refuses on write.

**Reference surface.** The seven tables swept here are exactly the seven
``plan_category_delete`` enumerates. ``app.categorization_decisions`` is
deliberately excluded there and here: it is an immutable decision log whose
columns are documented as snapshots, and its
``CHECK (status != 'accepted' OR category_id IS NOT NULL)`` makes an accepted
row's reference un-nullable in any case. History recording a category that was
later removed is the normal lifecycle, not a defect.

**Repair, don't cascade.** ``plan_category_delete``'s ``force`` path
cascade-*deletes* every referencing row. That is right for a user saying
"remove this category and everything that uses it"; it is wrong here. A
merchant, a budget, or a categorization rule whose *default* happened to be
blank is otherwise perfectly good user data, and the user never asked to
delete it. So a nullable reference is cleared and the row kept. Deletion is
used only where no repair exists:

- ``budgets.category``, ``categorization_rules.category`` and
  ``proposed_rules.category`` are ``NOT NULL``, so a blank there cannot be
  nulled and the row names nothing.
- ``category_source_map.category_id`` is ``NOT NULL``, so a mapping onto a
  removed category cannot hold the absent value.

**Deleting a rule unlinks its proposals.** ``proposed_rules.rule_id`` records
which rule a proposal was approved into, and doctor's
``_check_orphaned_proposed_rule_refs`` reports one that no longer resolves. A
rule deleted for its blank category would leave exactly that, so the link is
cleared first. The proposal is kept — it is sound, and only reverts to "not
approved into a live rule".

**No ``updated_at`` bump.** Every migration before this pair leaves the column
alone, and it means "set on UPDATE by service writes". Bumping it also walks
each repaired row into ``DoctorService._run_app_audit_coverage``, which flags a
row whose watermark is recent with no paired ``app.audit_log`` row — so a fresh
timestamp makes ``doctor`` report every row this migration fixes as an
unaudited mutation. Nothing reads the column for staleness: ``dim_merchants``
only projects it.

**Order matters.** The reference clears read ``app.user_categories`` to find
the blank ids, so the taxonomy rows themselves are deleted last, and the
proposal unlink precedes the rule delete that would orphan it. Running the
whole body twice is a no-op: after the first pass no blank row remains, so
every ``IN`` subquery is empty.

A separate migration rather than an edit to V054/V055: both ship in this same
PR, but a database that already applied them would never re-run an amended
body, and this half would silently skip exactly the installs most likely to
hold a legacy row.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: Entirely-whitespace test, defined to equal Python ``str.strip()`` exactly.
#: Byte-identical to V054's and V055's by construction —
#: ``test_blank_whitespace_definition`` reads every site's source and fails
#: when two of them disagree. Do not maintain it by adding the character that
#: last leaked; that test enumerates all 29 codepoints ``str.isspace()``
#: accepts and names any the class misses.
_BLANK = r"[\p{Z}\s\x0B\x1C-\x1F\x85]*"

#: A taxonomy row nothing should resolve to. ``category`` is ``NOT NULL`` so it
#: needs no NULL arm; ``subcategory`` is nullable, and a NULL one is absent
#: rather than blank — ``REGEXP_FULL_MATCH(NULL, ...)`` is ``NULL`` and
#: ``FALSE OR NULL`` is not ``TRUE``, which excludes it, as intended.
_UNUSABLE_TAXONOMY_IDS = """
    SELECT category_id
    FROM app.user_categories
    WHERE REGEXP_FULL_MATCH(category, ?)
       OR REGEXP_FULL_MATCH(subcategory, ?)
"""

#: Tables whose blank ``NOT NULL`` category snapshot no UPDATE can repair.
_UNREPAIRABLE_TEXT_TABLES = ("budgets", "categorization_rules", "proposed_rules")

#: Tables carrying a nullable blank ``subcategory`` snapshot V054/V055 did not
#: reach. ``budgets`` has no subcategory column.
_BLANK_SUBCATEGORY_TABLES = ("categorization_rules", "proposed_rules")

#: Tables whose nullable ``category_id`` is cleared rather than cascaded.
#:
#: ``updated_at`` is deliberately NOT bumped on the ones that have it. Every
#: migration before this pair leaves it alone, and the column means "set on
#: UPDATE by service writes" — a migration is not one. Bumping it also walks a
#: repaired row into ``DoctorService._run_app_audit_coverage``, which flags a
#: row whose watermark is recent with no paired ``app.audit_log`` row, so a
#: fresh timestamp would make ``doctor`` report every row this fixes as an
#: unaudited mutation. Nothing reads the column for staleness; ``dim_merchants``
#: only projects it.
_NULLABLE_REFERENCES = (
    "user_merchants",
    "transaction_splits",
    "transaction_categories",
    "budgets",
    "categorization_rules",
    "proposed_rules",
)


def migrate(conn: object) -> None:
    """Clear every reference to a blank taxonomy row, then remove the rows."""
    logger.debug(
        "V056: clear app category_id references to whitespace-only "
        "app.user_categories rows, then delete those rows"
    )

    # 1. A rule about to go in step 2 may be the one a proposal approved into.
    #    `proposed_rules.rule_id` is that approval link, and doctor's
    #    `_check_orphaned_proposed_rule_refs` reports one that no longer
    #    resolves — so unlink first. The proposal itself is sound and is kept;
    #    it simply reverts to "not approved into a live rule".
    conn.execute(  # type: ignore[union-attr]
        """
        UPDATE app.proposed_rules
        SET rule_id = NULL
        WHERE rule_id IN (
            SELECT rule_id
            FROM app.categorization_rules
            WHERE REGEXP_FULL_MATCH(category, ?)
        )
        """,
        [_BLANK],
    )

    # 2. Rows whose own NOT NULL category snapshot is blank. These name nothing
    #    and no UPDATE repairs them, so deletion is the only available repair.
    for table in _UNREPAIRABLE_TEXT_TABLES:
        conn.execute(  # type: ignore[union-attr]
            f"DELETE FROM app.{table} WHERE REGEXP_FULL_MATCH(category, ?)",  # noqa: S608  # module-level constant, not user input
            [_BLANK],
        )

    # 3. A blank subcategory under a real category is nulled, not deleted — the
    #    row itself is sound. Same rule V054 and V055 apply to their tables.
    for table in _BLANK_SUBCATEGORY_TABLES:
        conn.execute(  # type: ignore[union-attr]
            f"""
            UPDATE app.{table}
            SET subcategory = NULL
            WHERE REGEXP_FULL_MATCH(subcategory, ?)
            """,  # noqa: S608  # module-level constant, not user input
            [_BLANK],
        )

    # 4. Clear the canonical reference wherever the column can hold NULL, so the
    #    row survives with its default simply absent.
    for table in _NULLABLE_REFERENCES:
        conn.execute(  # type: ignore[union-attr]
            f"""
            UPDATE app.{table}
            SET category_id = NULL
            WHERE category_id IN ({_UNUSABLE_TAXONOMY_IDS})
            """,  # noqa: S608  # module-level constants, not user input
            [_BLANK, _BLANK],
        )

    # 5. category_source_map.category_id is NOT NULL, so a mapping onto a
    #    removed category cannot hold the absent value and the mapping goes.
    conn.execute(  # type: ignore[union-attr]
        f"""
        DELETE FROM app.category_source_map
        WHERE category_id IN ({_UNUSABLE_TAXONOMY_IDS})
        """,  # noqa: S608  # module-level constant, not user input
        [_BLANK, _BLANK],
    )

    # 6. Last, once nothing points at them: a category that renders as an empty
    #    name and that resolve_category_id can never usefully match.
    conn.execute(  # type: ignore[union-attr]
        """
        DELETE FROM app.user_categories
        WHERE REGEXP_FULL_MATCH(category, ?)
           OR REGEXP_FULL_MATCH(subcategory, ?)
        """,
        [_BLANK, _BLANK],
    )
