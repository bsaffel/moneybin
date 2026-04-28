# Follow-ups

Tracking deferred work and known limitations from shipped features.

## Auto-rule splitting (post-PR #58)

The current auto-rule generator proposes one `(merchant_pattern, category, subcategory)` per
normalized merchant. When a single merchant gets categorized multiple ways by the user
(e.g., Amazon → Groceries for food orders, Amazon → Shopping for everything else), the
proposal pipeline picks the dominant category and the others are abandoned or override the
proposed rule later.

A richer model would let one merchant produce **multiple** proposals, each scoped by an
additional discriminator beyond the merchant pattern.

### Discriminator detection

Candidate signals to mine from `fct_transactions` per merchant cluster:

- **Amount band**: bimodal/multimodal amounts (small grocery vs large electronics on Amazon)
- **Account type**: credit-card recurring vs debit one-off
- **Day-of-week / day-of-month**: payroll on the 15th, gym on the 1st
- **Description fragments**: tokens that co-occur with one category but not another
  (e.g., `AMZN MKTP` vs `AMAZON PRIME`)

Detection algorithm sketch:
1. Group user-categorized transactions by normalized merchant.
2. If categories disagree, try splits along each candidate discriminator.
3. Accept a split if each branch has high category purity (>=90%) and meets the
   trigger threshold (current `auto_rule_min_count`).

### Richer proposal model

Replace the single `(pattern, category)` tuple with a list of `(pattern, filters, category)`
tuples. `filters` is a structured predicate — initially a small allowlist
(`amount_lt`, `amount_gte`, `account_id_in`, `description_contains`) so it can be
serialized into a rule and re-applied deterministically by `CategorizationService`.

Schema impact:
- `app_proposed_rules` already keys on `proposed_rule_id`; adding a `filters` JSON
  column is additive.
- `app_categorization_rules` would need the same column. `_match_rules_for_uncategorized`
  in `CategorizationService` would join the filter predicate into its WHERE clause.

### Review UX

Today `auto-review` lists each proposal as one line. With splits, a single merchant
could produce 2–4 proposals — they should be grouped under the merchant in the table
view, with the discriminator shown alongside the pattern (e.g.
`AMAZON  amount<$50  → Groceries  ×42` /
`AMAZON  amount>=$50 → Shopping   ×17`).

### Why `find_matching_rule` is the substrate

`CategorizationService.find_matching_rule(transaction_id)` (added in PR #58) returns
the first active rule that would match a transaction. Splitting requires asking
"would this rule cover that transaction?" for many candidate rules during proposal
mining — `find_matching_rule` is the single SQL surface that answers it, so the
splitter can be built on top without re-implementing match semantics.

## Auto-rule items deferred from PR #58 review

Items 1–5 and 7 from this list were addressed in commits after the original
review; they're left here as historical notes. Item 6 remains open.

1. **`AutoRuleService.confirm()` does not deduplicate approve/reject overlap.**
   ✅ Fixed: `confirm()` now does `approve_set -= reject_set` before delegating.

2. **`_merchant_mapping_covers` ignores merchant `match_type` and subcategory.**
   ✅ Fixed: rewritten in Python using `matches_pattern` against
   `(raw_pattern, match_type)`; subcategory is now part of the equality check.

3. **`_find_pending_proposal` not keyed on `match_type`.**
   ✅ Fixed: query now joins on `(merchant_pattern, match_type)`.

4. **Auto-rule SQL didn't honor `normalize_description` (P1).**
   ✅ Fixed: `_categorize_existing_with_rule` and `check_overrides` now match in
   Python against both raw and `normalize_description`-cleaned descriptions,
   mirroring `apply_rules` semantics. The `_description_match_sql` helper is
   removed; the regex case-sensitivity gap went with it.

5. **`set_current_profile("test")` state leak in two tests.**
   ✅ Fixed: tests now `monkeypatch.setattr(config_module, "_current_profile",
   None)` and `_current_settings = None` so teardown restores module-level state.

6. **claude[bot] cannot dismiss its own CHANGES_REQUESTED reviews.**
   The GitHub Action running claude[bot] is sandboxed and blocks
   `gh pr review` / `gh api` write calls without explicit permission. As a
   result, the bot can flag CHANGES_REQUESTED but cannot clear it after a
   re-review confirms fixes — the author has to dismiss it manually
   (`gh api -X PUT /repos/{owner}/{repo}/pulls/{n}/reviews/{id}/dismissals`).
   Fix: grant the workflow permission to call `gh pr review` by adding to
   `.claude/settings.json` (or the workflow's allowlist):
   ```json
   { "permissions": { "allow": ["Bash(gh pr review:*)", "Bash(gh api:*)"] } }
   ```
   See PR #58 conversation for the exchange where this came up.

7. **`pytestmark = pytest.mark.unit` missing**
   (`tests/moneybin/test_services/test_auto_rule_service.py`). `uv run pytest -m
   unit` silently skips this file. `test_categorization_service.py` uses
   `@pytest.mark.unit` per test; mirror that or add `pytestmark` at module
   scope.
