"""Teach the categorization engine about the merchants a synthetic run invented.

The generator draws transaction descriptions from category-organized merchant
catalogs (``synthetic/data/merchants/*.yaml``), but it only ever wrote them into
``raw.*`` descriptions — never into ``app.user_merchants``. A freshly generated
profile therefore had nothing for ``CategorizationService`` to match against and
landed **0% categorized**, while ``system doctor`` still reported clean (its
coverage check is warn-only). That silently broke the demo preset's headline
promise: a *categorized* profile.

Seeding the same catalog the run drew from makes the REAL categorization engine
do the work, so the demo demonstrates categorization instead of faking it. It is
scoped to the synthetic sandbox — the profile being generated into — and is not a
return of the retired global seed merchant catalog.

Ordering: this must run AFTER the SQLMesh transform, because ``category_id``
resolves against ``core.dim_categories``, and before ``categorize``.
"""

import logging

from moneybin.database import Database
from moneybin.repositories.user_merchants_repo import UserMerchantsRepo
from moneybin.services.categorization._shared import resolve_category_id
from moneybin.synthetic.category_mapping import to_canonical
from moneybin.synthetic.models import GenerationResult
from moneybin.tables import USER_MERCHANTS

logger = logging.getLogger(__name__)

# Patterns are embedded verbatim inside the generated description ("GREAT CLIPS
# #482", "DIRECT DEP Acme Corp"), so a substring match is exactly right.
_MATCH_TYPE = "contains"
# A distinct provenance, not 'rule'/'user': the real-data guard needs to tell OUR
# seeded merchants (destroy freely on rebuild) from merchants the user authored in
# the demo profile (never destroy). See `reset.py::_OURS_IN_APP`.
_CREATED_BY = "synthetic"
_ACTOR = "synthetic"


def seed_merchant_catalog(db: Database, result: GenerationResult) -> int:
    """Register this run's merchants in `app.user_merchants`; return rows written.

    Idempotent by `raw_pattern`: re-seeding an already-seeded profile is a no-op,
    so a `synthetic generate` into an existing sandbox doesn't duplicate merchants.
    """
    existing = {
        row[0]
        for row in db.execute(
            f"SELECT raw_pattern FROM {USER_MERCHANTS.full_name} "  # noqa: S608  # TableRef constant
            f"WHERE raw_pattern IS NOT NULL"
        ).fetchall()
    }

    # A pattern that two catalogs claim cannot be resolved by a `contains` rule: the
    # shipped data has `AMZN MKTP` in shopping, education AND gifts, and `freelancer`
    # loads two of them. Letting the first seed win would silently file Amazon Books
    # purchases under Shopping *while still counting them categorized* — a wrong
    # answer dressed up as a right one. Leave them uncategorized instead; that is
    # honest, and it is what the descriptions actually support.
    categories_per_pattern: dict[str, set[str]] = {}
    for merchant in result.merchant_seeds:
        categories_per_pattern.setdefault(merchant.pattern, set()).add(
            merchant.synthetic_category
        )
    ambiguous = {p for p, cats in categories_per_pattern.items() if len(cats) > 1}
    if ambiguous:
        logger.warning(
            f"Skipping {len(ambiguous)} merchant pattern(s) claimed by more than one "
            f"category: {', '.join(sorted(ambiguous))}"
        )

    repo = UserMerchantsRepo(db)
    category_ids: dict[str, str | None] = {}
    written = 0

    for merchant in result.merchant_seeds:
        if merchant.pattern in existing or merchant.pattern in ambiguous:
            continue

        synthetic_category = merchant.synthetic_category
        if synthetic_category not in category_ids:
            canonical = to_canonical(synthetic_category)
            category_ids[synthetic_category] = resolve_category_id(db, canonical, None)
        category_id = category_ids[synthetic_category]

        if category_id is None:
            # An unmapped category would produce a merchant that matches
            # transactions but assigns nothing — worse than not seeding it.
            logger.warning(
                f"Skipping synthetic merchant {merchant.canonical_name!r}: "
                f"no category for {synthetic_category!r}"
            )
            continue

        repo.insert(
            raw_pattern=merchant.pattern,
            match_type=_MATCH_TYPE,
            canonical_name=merchant.canonical_name,
            category=to_canonical(synthetic_category),
            subcategory=None,
            category_id=category_id,
            created_by=_CREATED_BY,
            exemplars=None,
            actor=_ACTOR,
        )
        existing.add(merchant.pattern)
        written += 1

    logger.info(f"⚙️  Seeded {written} synthetic merchants for categorization")
    return written
