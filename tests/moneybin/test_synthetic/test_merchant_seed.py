"""Tests for the synthetic merchant seeder."""

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.synthetic.merchant_seed import seed_merchant_catalog
from moneybin.synthetic.models import GenerationResult, MerchantSeed


def _result(*seeds: MerchantSeed) -> GenerationResult:
    import datetime

    return GenerationResult(
        persona="basic",
        seed=42,
        accounts=[],
        transactions=[],
        start_date=datetime.date(2025, 1, 1),
        end_date=datetime.date(2025, 12, 31),
        merchant_seeds=list(seeds),
    )


@pytest.fixture(autouse=True)
def _resolve_categories(mocker: Any) -> None:  # pyright: ignore[reportUnusedFunction]
    """Resolve every category to a fixed id.

    `core.dim_categories` only exists after a transform; the seeder's conflict and
    dedup logic is what these tests are about.
    """
    mocker.patch(
        "moneybin.synthetic.merchant_seed.resolve_category_id", return_value="cat_1"
    )


@pytest.mark.integration
def test_skips_patterns_claimed_by_two_categories(db: Database) -> None:
    # The shipped catalogs put `AMZN MKTP` in shopping, education AND gifts, and
    # `freelancer` loads two of them. A `contains` rule cannot tell those apart, so
    # seeding the first would file Amazon Books under Shopping while still counting
    # it categorized — a wrong answer dressed as a right one. Leave it unseeded.
    written = seed_merchant_catalog(
        db,
        _result(
            MerchantSeed("AMZN MKTP", "Amazon", "shopping"),
            MerchantSeed("AMZN MKTP", "Amazon Books", "education"),
            MerchantSeed("GREAT CLIPS", "Great Clips", "personal_care"),
        ),
    )

    assert written == 1
    rows = db.execute(
        "SELECT raw_pattern FROM app.user_merchants ORDER BY raw_pattern"
    ).fetchall()
    assert [r[0] for r in rows] == ["GREAT CLIPS"]


@pytest.mark.integration
def test_seeding_is_idempotent(db: Database) -> None:
    result = _result(MerchantSeed("GREAT CLIPS", "Great Clips", "personal_care"))

    assert seed_merchant_catalog(db, result) == 1
    assert seed_merchant_catalog(db, result) == 0  # re-seeding writes nothing

    rows = db.execute("SELECT COUNT(*) FROM app.user_merchants").fetchone()
    assert rows is not None
    assert rows[0] == 1
