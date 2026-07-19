"""Build-time derivation of reports.*/core.* column classes (no DB connection)."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.privacy.redaction import MaskStrength, mask_strength
from moneybin.privacy.report_class_derivation import (
    ReportDerivationError,
    derive_core_view_classes,
    derive_report_classes,
)
from moneybin.privacy.taxonomy import DataClass, Tier


def _derive_one(model_sql: str, models_root: Path) -> dict[tuple[str, str], object]:
    """Derive a single hand-written model from an isolated models root."""
    (models_root / "probe.sql").write_text(model_sql)
    return derive_report_classes(models_root=models_root)  # type: ignore[return-value]


def test_derives_every_deployed_reports_view() -> None:
    derived = derive_report_classes()
    names = {view for (_schema, view) in derived}
    assert names == {
        "balance_drift",
        "cash_flow",
        "large_transactions",
        "merchant_activity",
        "net_worth",
        "recurring_subscriptions",
        "spending_trend",
    }


def test_account_id_derives_from_classification_not_the_gap_fallback() -> None:
    """The exact column #330 leaked — but not to the class the bridge guessed.

    #330 left uncategorized_queue.account_id with no declared class at all, so
    it fell through to the unmasked AGGREGATE fallback. uncategorized_queue
    itself has since moved to core.* (reports-foundation.md R5, Task 5) and no
    longer appears in this map at all — but every reports.* view that still
    selects account_id unchanged derives the identical answer, because
    core.fct_transactions.account_id (and every other account_id column) was
    deliberately reclassified to RECORD_ID in spec D6 (commit c465f181,
    "account_id is now opaque by construction, which makes the RECORD_ID
    privacy unmask safe"): it's a minted surrogate key, not PII — see
    taxonomy.py's CLASSIFICATION and
    docs/specs/privacy-data-classification.md. cash_flow's runner over-declares
    it ACCOUNT_IDENTIFIER anyway — safe because RECORD_ID is LOW and this
    over-declares ACROSS tiers, not because over-declaring is safe in general
    (at equal CRITICAL tier it is not; see ``_declaration_is_safe``). This pins
    derivation's own answer, which is exactly the drift Task 3/4 exist to
    surface.
    """
    derived = derive_report_classes()
    assert derived[("reports", "cash_flow")]["account_id"] is DataClass.RECORD_ID


def test_counting_aggregate_is_not_over_classified() -> None:
    """COUNT(DISTINCT account_id) is AGGREGATE, not account_id's own class.

    Raw lineage would trace this to account_id. The shared classifier's
    counting-aggregate rule is why we reuse it instead of writing a deriver.
    """
    derived = derive_report_classes()
    assert derived[("reports", "net_worth")]["account_count"] is DataClass.AGGREGATE


def test_balance_columns_derive_from_app_schema() -> None:
    """balance_drift reads app.balance_assertions, which has no SQLMesh model.

    Asserts the CLASS, not the tier: BALANCE shares Tier.HIGH with TXN_AMOUNT,
    so a tier-only assertion passed even if the column derived as a transaction
    amount — exactly the confusion this test exists to rule out.
    """
    drift = derive_report_classes()[("reports", "balance_drift")]
    assert drift["asserted_balance"] is DataClass.BALANCE


def test_derivation_never_falls_back_silently(tmp_path: Path) -> None:
    """Derivation must raise, not log-and-guess, on an unresolvable projection.

    The spec's binding constraint. ``resolve_output_classes`` answers a
    projection it cannot resolve with a conservative fallback — correct for user
    SQL, wrong for a map that claims to be *verified* — so the deriver runs it
    with ``strict=True``. This pins the wiring: without it, a model could drift
    into an unresolvable shape and the derived map would absorb the guess with
    only a WARNING to show for it.
    """
    model = """
        MODEL (name reports.unresolvable_probe, kind VIEW);
        SELECT l.x AS probe
        FROM core.fct_transactions AS t,
             LATERAL (SELECT t.amount AS x) AS l
    """
    with pytest.raises(ReportDerivationError, match="probe"):
        _derive_one(model, tmp_path)


def test_derivation_rejects_star_in_a_cte_body(tmp_path: Path) -> None:
    """``SELECT *`` is rejected anywhere, not just in the final projection.

    A star in a CTE body is equally underivable — nothing expands it, so
    ``_output_index`` cannot name-match through it and the column degrades to a
    silent fallback instead of the hard error this check exists to raise.
    """
    model = """
        MODEL (name reports.star_cte_probe, kind VIEW);
        WITH b AS (SELECT * FROM core.fct_transactions)
        SELECT b.amount AS amount FROM b
    """
    with pytest.raises(ReportDerivationError, match="SELECT \\*"):
        _derive_one(model, tmp_path)


# ---------------------------------------------------------------------------
# derive_core_view_classes: the generalized engine, applied to core.* views
#
# Unlike reports.* (every model must derive — see report_class_derivation.py's
# module docstring), most of core.* is out of scope for this connectionless
# deriver: 2 kind-FULL SQL models, 3 Python models, and most of the remaining
# kind-VIEW models read prep.*/seeds.* (ordinary medallion data flow) or use a
# shape (bare SELECT *, an unaliased single-table projection, UNNEST(...)
# struct access) this no-qualify() deriver was never built to resolve. The
# tests below pin BOTH sides of that split exactly, so a change to either
# (a model starting to derive that used not to, or vice versa) requires a
# deliberate edit here rather than silently passing or silently vanishing.
# ---------------------------------------------------------------------------


def test_derives_every_derivable_core_view() -> None:
    derived, _excluded = derive_core_view_classes()
    assert set(derived) == {
        ("core", "dim_merchants"),
        ("core", "uncategorized_queue"),
    }


def test_core_excludes_materialized_tables_by_kind() -> None:
    """A kind-FULL SQL model is excluded before derivation is even attempted."""
    _derived, excluded = derive_core_view_classes()
    assert "kind=FULL" in excluded["core.dim_accounts"]
    assert "kind=FULL" in excluded["core.fct_investment_transactions"]


def test_core_excludes_python_models_by_filename() -> None:
    """A SQLMesh Python model has no SQL text; excluded without being loaded."""
    _derived, excluded = derive_core_view_classes()
    for stem in ("fct_balances_daily", "fct_investment_lots", "fct_realized_gains"):
        assert "python model" in excluded[stem]


def test_core_excludes_views_the_deriver_cannot_resolve() -> None:
    """kind-VIEW models that fail derivation are excluded with the real reason.

    Each of these legitimately reads prep.*/seeds.* (outside CLASSIFICATION's
    core/app ground truth) or uses a shape this deriver can't walk without
    ``qualify()`` (bare ``SELECT *``, or ``UNNEST(...)`` struct-field access) —
    none is a bug to fix here; each is a real, stated scope boundary.
    """
    _derived, excluded = derive_core_view_classes()
    unresolvable = {
        "core.bridge_category_source_map",  # reads seeds.category_source_map
        "core.bridge_transfers",  # reads prep.int_transactions__matched/merged
        "core.dim_categories",  # reads seeds.categories
        "core.dim_holdings",  # reads prep.stg_plaid__investment_holdings*
        "core.dim_securities",  # unaliased single-table SELECT (no qualify())
        "core.fct_balances",  # bare SELECT * inside a UNION ALL branch
        "core.fct_transaction_lines",  # UNNEST(t.splits) struct-field access
        "core.fct_transactions",  # reads prep.int_transactions__merged
    }
    # kind=FULL and python-model exclusions are pinned by
    # test_core_excludes_materialized_tables_by_kind and
    # test_core_excludes_python_models_by_filename respectively; filter them out
    # so this is an exact-set comparison for its own category (a kind=VIEW model
    # that fails derivation) — a subset check here would let a newly-excluded
    # core view escape review silently, which is exactly what this test exists
    # to catch.
    view_derivation_failures = {
        name: reason
        for name, reason in excluded.items()
        if not reason.startswith("kind=") and "python model" not in reason
    }
    assert set(view_derivation_failures) == unresolvable
    # Each exclusion must name WHY it could not be derived. "no CLASSIFICATION
    # ground truth" is the schema-contract refusal (`_assert_acyclic` rejecting
    # a read of seeds/prep/raw/meta); the other two are resolution failures. A
    # bare "excluded" with no stated cause is the silent skip this whole
    # mechanism exists to prevent.
    for name in unresolvable:
        reason = view_derivation_failures[name]
        assert (
            "no CLASSIFICATION ground truth" in reason
            or "not resolvable" in reason
            or "SELECT *" in reason
        ), f"{name}: exclusion reason does not say why: {reason!r}"


def test_uncategorized_queue_age_days_and_priority_score_derive_correctly() -> None:
    """The two columns #330's finding singled out as the least-verified.

    ``age_days`` is ``CURRENT_DATE - transaction_date`` (a date arithmetic
    expression referencing only ``transaction_date``) and ``priority_score``
    is ``ABS(amount) * age_days`` (referencing both ``amount`` and
    ``transaction_date``, whose classes are TXN_AMOUNT/HIGH and
    TXN_DATE/MEDIUM respectively) — derivation's max-tier-referenced-column
    rule must answer TXN_DATE and TXN_AMOUNT, matching what taxonomy.py
    declares.
    """
    derived, _excluded = derive_core_view_classes()
    queue = derived[("core", "uncategorized_queue")]
    assert queue["age_days"] is DataClass.TXN_DATE
    assert queue["priority_score"] is DataClass.TXN_AMOUNT


# ---------------------------------------------------------------------------
# Declared-vs-derived tier comparison — moved here from
# tests/scenarios/test_reports_classification.py (previously @pytest.mark.
# scenarios). Both derive_report_classes/derive_core_view_classes (above) and
# reports_class_map()/CLASSIFICATION (the declared side) are connectionless —
# this comparison needs no database, so it belongs in the default unit gate
# (`make check test`), not behind `make test-scenarios`. The one test that
# genuinely needs a real, deployed database — enumerating every column a
# *deployed* reports.* view exposes, from the DuckDB catalog — stays behind
# in test_reports_classification.py as
# test_reports_declared_classes_cover_real_views.
# ---------------------------------------------------------------------------


def _all_class_downgrades() -> dict[tuple[str, str], dict[str, str]]:
    """(schema, table) -> {column: reason}, from every ``@report`` runner.

    Runner-less views (``reports/definitions/_derived_classes.py``, generated)
    carry no ``class_downgrades`` — a generated entry is derivation's own
    answer, not a decorator-attached spec with an author-supplied override, so
    there is nothing to downgrade *from* here.
    """
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS

    out: dict[tuple[str, str], dict[str, str]] = {}
    for runner in ALL_REPORTS:
        spec = spec_of(runner)
        out[(spec.view.schema, spec.view.name)] = dict(spec.class_downgrades)
    return out


def _orphaned_downgrades(
    derived: dict[tuple[str, str], dict[str, DataClass]],
    downgrades: dict[tuple[str, str], dict[str, str]],
) -> list[str]:
    """Downgrade entries naming a column the model no longer outputs.

    The declared-vs-derived comparison walks ``derived``, so it can only judge
    downgrades for columns that still exist. Rename or drop a column from the
    SQLMesh model's SELECT list and its ``class_downgrades`` entry stops being
    visited by anything — it survives indefinitely, and if a *later* column is
    ever added back under the same name, that abandoned reason silently
    pre-authorizes whatever class it declares without a fresh review. This
    walks the other direction so an entry must name a live column to survive.

    Only keys derivation actually resolved are checked: a model in
    ``derive_report_classes``' excluded set contributes no columns, and
    treating its downgrades as orphaned would report an exclusion as staleness.
    """
    problems: list[str] = []
    for key, cols in downgrades.items():
        derived_cols = derived.get(key)
        if derived_cols is None:
            continue
        for column in cols:
            if column not in derived_cols:
                problems.append(
                    f"{key[0]}.{key[1]}.{column}: class_downgrades entry names "
                    "a column that derivation does not produce — the model's "
                    "SELECT list no longer has it (renamed or removed). "
                    "Delete this class_downgrades entry"
                )
    return problems


def _declaration_is_safe(declared: DataClass, derived: DataClass) -> bool:
    """True when ``declared`` hides a value at least as well as ``derived``.

    Ordered on ``(tier, mask_strength)`` lexicographically. Tier alone is NOT
    sufficient, and the gap it leaves is the whole reason this helper exists:
    all four CRITICAL classes share ``Tier.CRITICAL`` but do not share a
    transform, so a tier-only comparison rates ACCOUNT_IDENTIFIER (masks
    PARTIAL, ``"****" + value[-4:]``) an adequate stand-in for ROUTING_NUMBER
    (masks WHOLE). Runtime masking keys off the DECLARED class, so that
    declaration publishes the real routing number's last four digits while the
    guard reports nothing.

    Below CRITICAL every transform is passthrough, so every strength there is
    equal and this reduces to exactly the tier comparison it replaces — an
    over-declaration like TXN_AMOUNT-where-CATEGORY-was-derived still passes.
    Demanding class identity there would only manufacture false failures over
    classes that mask identically.

    Strength comes from ``redaction.mask_strength``, which measures each
    class's own ``_TRANSFORMS`` entry rather than consulting a list of
    whole-masking classes; a list would rot silently the first time a
    ``DataClass`` was added without updating it.
    """
    return (declared.tier, mask_strength(declared)) >= (
        derived.tier,
        mask_strength(derived),
    )


def _is_unwaivable_weakening(declared: DataClass, derived: DataClass) -> bool:
    """True when a ``class_downgrades`` reason must NOT be allowed to waive this.

    ``class_downgrades`` exists because derivation systematically
    over-classifies *computed* columns — an author asserts "this z-score
    reveals no amount", a claim about information content that lineage cannot
    make. That argument is unavailable at equal tier: both classes already
    agree the value is this sensitive, and only the display transform differs.
    Waiving there would not be correcting an over-classification, it would be
    electing to publish the last four characters of a value everyone agrees is
    CRITICAL.

    The legitimate case needs no waiver: ``dim_accounts.last_four`` genuinely
    IS an institution account number, so derivation returns the partial-masking
    class too and there is no mismatch to excuse.

    Below CRITICAL every transform is passthrough, so strengths are equal and
    this never fires.
    """
    return declared.tier == derived.tier and mask_strength(declared) < mask_strength(
        derived
    )


def _weakness(declared: DataClass, derived: DataClass) -> str:
    """Describe HOW ``declared`` falls short of ``derived``, for the failure text."""
    if declared.tier < derived.tier:
        return (
            f"declared {declared.name} (tier {declared.tier.name}) is below "
            f"derived {derived.name} (tier {derived.tier.name})"
        )
    return (
        f"declared {declared.name} masks {mask_strength(declared).name} but "
        f"derived {derived.name} masks {mask_strength(derived).name} at the "
        f"same tier ({declared.tier.name}) — the declared class drives runtime "
        "masking, so this weakens it"
    )


def test_declared_classes_match_derivation() -> None:
    """Every declared class is derivation-matched, explicitly downgraded, or stale.

    ``derive_report_classes`` (build-time, no DB — see ADR-013 follow-up in
    ``report_class_derivation.py``) recomputes each column's class from the
    SQLMesh model source; this compares it against the declared contract via
    ``_declaration_is_safe`` — **tier, then mask strength**, not class
    identity. Over-declaring is safe below CRITICAL (every transform there is
    passthrough) but not automatically safe at CRITICAL, where the four classes
    share a tier and differ in transform; see that helper. Only a declaration
    that masks strictly more weakly than derivation requires an explicit,
    reasoned ``class_downgrades`` entry.

    The inverse must also fail: a ``class_downgrades`` entry for a column whose
    declaration is NOT actually weaker than derivation is a *stale*
    declaration — the downgrade it once justified no longer applies (e.g. a
    future window-partition-key carve-out in the deriver could make several of
    today's downgrades unnecessary). Left unchecked, a stale entry would sit
    in the tree forever with nothing ever failing to prompt its removal.
    """
    from moneybin.privacy.report_class_derivation import derive_report_classes
    from moneybin.privacy.sql_lineage import reports_class_map

    derived = derive_report_classes()
    declared = reports_class_map()
    downgrades = _all_class_downgrades()

    problems: list[str] = []
    for key, derived_cols in derived.items():
        for column, derived_class in derived_cols.items():
            declared_class = declared.get(key, {}).get(column)
            if declared_class is None:
                problems.append(f"{key[0]}.{key[1]}.{column}: undeclared")
                continue
            reason = downgrades.get(key, {}).get(column)
            if _declaration_is_safe(declared_class, derived_class):
                if reason:
                    problems.append(
                        f"{key[0]}.{key[1]}.{column}: class_downgrades entry "
                        f"is stale — declared {declared_class.name} (tier "
                        f"{declared_class.tier.name}, masks "
                        f"{mask_strength(declared_class).name}) is not weaker "
                        f"than derived {derived_class.name} (tier "
                        f"{derived_class.tier.name}, masks "
                        f"{mask_strength(derived_class).name}); delete this "
                        "class_downgrades entry"
                    )
                continue
            if _is_unwaivable_weakening(declared_class, derived_class):
                problems.append(
                    f"{key[0]}.{key[1]}.{column}: "
                    f"{_weakness(declared_class, derived_class)}. A "
                    "class_downgrades reason CANNOT waive an equal-tier "
                    "transform weakening — declare the derived class instead"
                )
            elif not reason:
                problems.append(
                    f"{key[0]}.{key[1]}.{column}: "
                    f"{_weakness(declared_class, derived_class)} "
                    "with no class_downgrades reason"
                )
    problems.extend(_orphaned_downgrades(derived, downgrades))
    assert not problems, "Class declarations disagree with derivation:\n" + "\n".join(
        problems
    )


def test_orphaned_class_downgrade_is_flagged() -> None:
    """A downgrade entry for a column the model dropped must not survive.

    The main comparison walks ``derived``, so nothing it does can visit a
    downgrade whose column vanished from the SELECT list. This pins the
    opposite direction independently of whether the real report set currently
    happens to contain such an entry.
    """
    key: tuple[str, str] = ("reports", "spending_trend")
    derived: dict[tuple[str, str], dict[str, DataClass]] = {
        key: {"live_column": DataClass.TXN_AMOUNT}
    }
    live: dict[tuple[str, str], dict[str, str]] = {key: {"live_column": "reason"}}
    dropped: dict[tuple[str, str], dict[str, str]] = {key: {"dropped_column": "reason"}}

    assert _orphaned_downgrades(derived, live) == []

    problems = _orphaned_downgrades(derived, dropped)
    assert len(problems) == 1
    assert "dropped_column" in problems[0]
    assert "renamed or removed" in problems[0]


def test_orphaned_downgrade_check_ignores_underived_models() -> None:
    """An excluded model's downgrades are unjudgeable, not orphaned.

    ``derive_report_classes`` omits models it cannot resolve connectionlessly.
    Reporting their downgrades as stale would turn an exclusion into a false
    staleness failure, so the check skips keys derivation never produced.
    """
    derived: dict[tuple[str, str], dict[str, DataClass]] = {
        ("reports", "derivable"): {"col": DataClass.TXN_AMOUNT}
    }
    downgrades: dict[tuple[str, str], dict[str, str]] = {
        ("reports", "excluded_model"): {"col": "reason"}
    }
    assert _orphaned_downgrades(derived, downgrades) == []


def test_core_declared_classes_match_derivation() -> None:
    """Every derivable core.* view's CLASSIFICATION entry is mask-safe.

    Generalizes ``test_declared_classes_match_derivation`` above to core.*
    (see ``derive_core_view_classes``'s scoping rule: only the core view
    models this connectionless deriver can actually resolve are compared —
    most of core.* reads prep.*/seeds.* or uses a shape the deriver can't
    walk, and is excluded rather than compared; see the pinned exclusion set
    above).

    Unlike reports.*, CLASSIFICATION has no ``class_downgrades`` mechanism:
    there is no reasoned-override channel to invent, so ANY declaration that
    masks more weakly than derivation — whether by tier or, at equal tier, by
    transform strength (see ``_declaration_is_safe``) — is unconditionally a
    problem here.
    """
    from moneybin.privacy.report_class_derivation import derive_core_view_classes
    from moneybin.privacy.taxonomy import CLASSIFICATION

    derived, _excluded = derive_core_view_classes()

    problems: list[str] = []
    for key, derived_cols in derived.items():
        declared_cols = CLASSIFICATION.get(key, {})
        for column, derived_class in derived_cols.items():
            declared_class = declared_cols.get(column)
            if declared_class is None:
                problems.append(f"{key[0]}.{key[1]}.{column}: undeclared")
                continue
            if not _declaration_is_safe(declared_class, derived_class):
                problems.append(
                    f"{key[0]}.{key[1]}.{column}: "
                    f"{_weakness(declared_class, derived_class)} — "
                    "CLASSIFICATION has no downgrade-with-reason mechanism"
                )
    assert not problems, "core.* declarations disagree with derivation:\n" + "\n".join(
        problems
    )


# ---------------------------------------------------------------------------
# _declaration_is_safe — the comparison BOTH guards above run on every column.
#
# These cases are the guards' own unit tests. The equal-CRITICAL pair is not
# hypothetical: under the previous tier-only comparison
# (``declared.tier >= derived.tier``) a declared ACCOUNT_IDENTIFIER against a
# derived ROUTING_NUMBER answered True and both guards passed in silence, while
# runtime masking — which keys off the DECLARED class — turned '021000021' into
# '****0021' and published the real routing number's last four digits.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("declared", "derived", "expected", "why"),
    [
        (
            DataClass.ACCOUNT_IDENTIFIER,
            DataClass.ROUTING_NUMBER,
            False,
            "equal CRITICAL tier, PARTIAL declared over WHOLE derived — the leak",
        ),
        (
            DataClass.INSTITUTION_ACCOUNT_NUMBER,
            DataClass.UNRESOLVED,
            False,
            "the same leak through the other partial-masking CRITICAL class",
        ),
        (
            DataClass.ROUTING_NUMBER,
            DataClass.ACCOUNT_IDENTIFIER,
            True,
            "equal tier, declared masks WHOLE where derived masks PARTIAL",
        ),
        (
            DataClass.ACCOUNT_IDENTIFIER,
            DataClass.ACCOUNT_IDENTIFIER,
            True,
            "same class is always safe",
        ),
        (
            DataClass.ROUTING_NUMBER,
            DataClass.UNRESOLVED,
            True,
            "different CRITICAL classes that mask identically stay interchangeable",
        ),
        (
            DataClass.ACCOUNT_IDENTIFIER,
            DataClass.INSTITUTION_ACCOUNT_NUMBER,
            True,
            "two PARTIAL classes at equal tier — identity is NOT required",
        ),
        (
            DataClass.TXN_AMOUNT,
            DataClass.CATEGORY,
            True,
            "below-CRITICAL over-declare: every transform there is passthrough",
        ),
        (
            DataClass.ACCOUNT_IDENTIFIER,
            DataClass.TXN_AMOUNT,
            True,
            "over-declare ACROSS tiers still passes — tier dominates strength",
        ),
        (
            DataClass.CATEGORY,
            DataClass.TXN_AMOUNT,
            False,
            "a genuine tier downgrade is still caught",
        ),
    ],
)
def test_declaration_safety_compares_mask_strength_not_only_tier(
    declared: DataClass, derived: DataClass, expected: bool, why: str
) -> None:
    assert _declaration_is_safe(declared, derived) is expected, why


def test_below_critical_comparison_is_unchanged_by_the_strength_rule() -> None:
    """Adding strength must not start demanding class identity below CRITICAL.

    Every transform below CRITICAL is passthrough, so strength is constant
    there and the ordering must still reduce to the plain tier comparison. A
    rule that tightened here would fail every one of the many legitimate
    over-declarations in the tree (``account_id`` declared ACCOUNT_IDENTIFIER
    where derivation says RECORD_ID, and so on) — noise, not signal.
    """
    below_critical = [dc for dc in DataClass if dc.tier is not Tier.CRITICAL]
    assert below_critical, "fixture assumption: some classes sit below CRITICAL"
    for dc in below_critical:
        assert mask_strength(dc) is MaskStrength.PASSTHROUGH
    for declared in below_critical:
        for derived in below_critical:
            assert _declaration_is_safe(declared, derived) is (
                declared.tier >= derived.tier
            )


def test_every_data_class_has_a_measurable_mask_strength() -> None:
    """No ``DataClass`` may reach the guards without a knowable strength.

    ``mask_strength`` measures ``_TRANSFORMS`` and raises rather than defaulting
    for an unmapped class, so a new class added without a transform fails loudly
    here instead of being ranked as strong as a whole mask and quietly weakening
    ``_declaration_is_safe`` for every column that uses it.
    """
    for dc in DataClass:
        assert isinstance(mask_strength(dc), MaskStrength)


def test_mask_strength_refuses_to_default_an_unmapped_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The no-default contract, exercised — not merely asserted in a docstring."""
    from moneybin.privacy import redaction

    monkeypatch.delitem(
        redaction._TRANSFORMS,  # pyright: ignore[reportPrivateUsage]
        DataClass.ROUTING_NUMBER,
    )
    with pytest.raises(KeyError, match="no _TRANSFORMS entry"):
        mask_strength(DataClass.ROUTING_NUMBER)
