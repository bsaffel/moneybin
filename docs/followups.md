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

## Auto-rule P2 items deferred from PR #58 review

P2-level edge cases flagged by claude[bot] re-review of PR #58. None are blocking
under current usage (auto-rules start as `contains` patterns) but they will matter
once `exact`/`regex` proposals enter the pipeline (pillar D) or once MCP callers
invoke `AutoRuleService.confirm()` directly.

1. **`AutoRuleService.confirm()` does not deduplicate approve/reject overlap**
   (`auto_rule_service.py:162`). The CLI strips overlap before calling, so today's
   path is safe; MCP callers passing overlapping lists would silently approve a
   "rejected" ID. Fix: add `approve_set -= reject_set` at the top of `confirm()`
   so the service is safe regardless of caller.

2. **`_merchant_mapping_covers` ignores merchant `match_type`**
   (`auto_rule_service.py:587–596`). Always uses substring matching regardless of
   the merchant's match_type. An `exact` merchant for `STARBUCKS` would
   incorrectly suppress a valid proposal for pattern `STARBUCKS RESERVE`. Low risk
   today; produces false negatives once `exact` merchants are common.

3. **`_find_pending_proposal` not keyed on `match_type`**
   (`auto_rule_service.py:602–613`). Two proposals with the same
   `merchant_pattern` text but different `match_type` would collide. Low
   probability today since description-derived patterns always produce
   `contains`.

4. **`_description_match_sql` doesn't check normalized descriptions**
   (`auto_rule_service.py:616–622`). The rule engine matches against both
   `description` and `normalize_description(description)` for `exact`/`regex`;
   the SQL fragment only checks raw `t.description`. Affects back-fill accuracy
   for rules derived from normalized merchant names. Harmless for `contains`.

5. **`set_current_profile("test")` state leak in two tests**
   (`tests/moneybin/test_services/test_auto_rule_service.py:159, 229`).
   `monkeypatch.setenv` reverts env vars, but `set_current_profile("test")`
   mutates a module-level global that isn't restored. Tests running after these
   may pick up the "test" profile. Fix: also
   `monkeypatch.setattr(config_module, "_current_profile", None)` (and
   `_current_settings`) so teardown restores module-level state.

6. **`pytestmark = pytest.mark.unit` missing**
   (`tests/moneybin/test_services/test_auto_rule_service.py`). `uv run pytest -m
   unit` silently skips this file. `test_categorization_service.py` uses
   `@pytest.mark.unit` per test; mirror that or add `pytestmark` at module
   scope.
