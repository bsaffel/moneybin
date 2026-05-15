"""Seed `app.merchants` from a synthetic persona's catalog data.

Scenarios need merchant rules to evaluate categorization — without them
``categorize_pending`` finds no matches and writes zero categories. The
synthetic merchant catalogs already encode ``description_prefix → category``;
this module materializes those mappings as `app.merchants` rows so the
categorize step has something to work against.

Scope: scenario-runner only. Real product flow seeds merchants via the
auto-rule pipeline / user actions, not from synthetic data.
"""

from __future__ import annotations

import logging

from moneybin.database import Database
from moneybin.services.categorization_service import CategorizationService
from moneybin.synthetic.category_mapping import to_canonical
from moneybin.synthetic.models import (
    PersonaConfig,
    load_merchant_catalog,
)

logger = logging.getLogger(__name__)


def seed_merchants_from_persona(db: Database, persona: PersonaConfig) -> int:
    """Seed `app.merchants` with rules derived from a persona definition.

    Three sources contribute rules:

    1. **Spending merchants** — for each spending category, walk the merchant
       catalog and create a contains-match on each ``description_prefix``
       (or merchant name when no prefix) → ``category.name``.
    2. **Recurring charges** — each recurring entry's ``description`` →
       ``recurring.category``.
    3. **Income** — each income config's ``description_template`` head
       (everything before the first ``{`` placeholder) → ``"income"``.

    Returns the number of merchants created.
    """
    service = CategorizationService(db)
    seen: set[str] = set()
    created = 0

    for cat in persona.spending.categories:
        catalog = load_merchant_catalog(cat.merchant_catalog)
        for entry in catalog.merchants:
            pattern = entry.description_prefix or entry.name
            if not pattern or pattern in seen:
                continue
            seen.add(pattern)
            service.create_merchant(
                pattern,
                entry.name,
                match_type="contains",
                category=to_canonical(cat.name),
                created_by="ai",
            )
            created += 1

    for rec in persona.recurring:
        pattern = rec.description.strip()
        if not pattern or pattern in seen:
            continue
        seen.add(pattern)
        service.create_merchant(
            pattern,
            pattern,
            match_type="contains",
            category=to_canonical(rec.category),
            created_by="ai",
        )
        created += 1

    for inc in persona.income:
        template = inc.description_template or ""
        head = template.split("{", 1)[0].strip()
        if not head or head in seen:
            continue
        seen.add(head)
        service.create_merchant(
            head,
            head,
            match_type="contains",
            category=to_canonical("income"),
            created_by="ai",
        )
        created += 1

    logger.info(f"Seeded {created} merchant rules from persona {persona.persona!r}")
    return created
