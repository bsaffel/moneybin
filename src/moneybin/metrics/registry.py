"""Metric definitions for MoneyBin.

All metrics use the ``moneybin_`` prefix. Each metric is a module-level
constant bound to the default prometheus_client registry.

Adding a new metric: define it here, then either use ``@tracked`` at the
call site or record manually (e.g. ``CATEGORIZATION_AUTO_RATE.set(0.78)``).
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Import pipeline ──────────────────────────────────────────────────────────

IMPORT_RECORDS_TOTAL = Counter(
    "moneybin_import_records_total",
    "Total records imported across all sources",
    ["source_type"],
)

IMPORT_DURATION_SECONDS = Histogram(
    "moneybin_import_duration_seconds",
    "Duration of import operations in seconds",
    ["source_type"],
)

IMPORT_ERRORS_TOTAL = Counter(
    "moneybin_import_errors_total",
    "Total import errors by source and error type",
    ["source_type", "error_type"],
)

INBOX_SYNC_TOTAL = Counter(
    "moneybin_inbox_sync_total",
    "Inbox file outcomes per sync",
    ["outcome"],
)

INBOX_SYNC_DURATION_SECONDS = Histogram(
    "moneybin_inbox_sync_duration_seconds",
    "Duration of one inbox drain (seconds)",
)

IMPORT_BATCH_SIZE = Histogram(
    "moneybin_import_batch_size",
    "Files per ImportService.import_files() call",
    buckets=(1, 2, 5, 10, 20, 50, 100, 500),
)

# ── Tabular import ───────────────────────────────────────────────────────────

TABULAR_FORMAT_MATCHES = Counter(
    "moneybin_tabular_format_matches_total",
    "Tabular format matches by format name and source",
    ["format_name", "format_source"],
)

TABULAR_DETECTION_CONFIDENCE = Counter(
    "moneybin_tabular_detection_confidence_total",
    "Column mapping detection confidence distribution",
    ["confidence"],
)

TABULAR_IMPORT_BATCHES = Counter(
    "moneybin_tabular_import_batches_total",
    "Import batch lifecycle events",
    ["status"],
)

OFX_IMPORT_BATCHES = Counter(
    "moneybin_ofx_import_batches_total",
    "OFX/QFX/QBO import batches by status (complete, partial, failed).",
    labelnames=("status",),
)

# No label: counts raw transaction rows whose non-unique FITID was rewritten
# with a content-hash suffix so a shared-FITID pair isn't collapsed to one row.
# A non-zero rate flags an institution that violates OFX's per-account FITID
# uniqueness promise (queryable in aggregate, not just log-greppable).
OFX_FITID_COLLISION_REPAIRED_TOTAL = Counter(
    "moneybin_ofx_fitid_collision_repaired_total",
    "OFX transaction rows disambiguated after sharing a non-unique FITID.",
)

# Outcomes: "transactions" (routed deterministic), "seed" (Phase 1 fallback),
# "failed" (extraction error or zero rows), "unsupported" (scanned / image-only
# PDF with no text layer — needs a vision-capable backend, Req 5).
PDF_IMPORT_TOTAL = Counter(
    "moneybin_pdf_import_total",
    "PDF imports by outcome and rung.",
    ["outcome", "rung"],
)

PDF_EXTRACTION_CONFIDENCE = Histogram(
    "moneybin_pdf_extraction_confidence",
    "Confidence score from PDF recipe execution (0.0–1.0).",
    buckets=(0.1, 0.3, 0.5, 0.7, 0.85, 0.95, 1.0),
)

# Label values bounded to two: replay_success and replay_failed.
# Keeping cardinality low ensures dashboards + alerts stay stable as user recipe
# counts grow.
PDF_RECIPE_HIT_TOTAL = Counter(
    "moneybin_pdf_recipe_hit_total",
    "PDF imports where a saved format matched the layout fingerprint.",
    ["outcome"],  # values: "replay_success", "replay_failed"
)

# Separate counter (no label) so a Prometheus alert can fire on raw replay
# failures without depending on label cardinality or label filtering.
PDF_REPLAY_GUARD_FAILURE_TOTAL = Counter(
    "moneybin_pdf_replay_guard_failure_total",
    "Saved PDF format matched but reconciliation failed (recipe drift signal).",
)

# What happened AFTER a replay-guard failure. Without this, a fleet where every
# failure self-heals is indistinguishable from one where every failure seeds —
# PDF_REPLAY_GUARD_FAILURE_TOTAL fires before the repair is attempted, so it
# counts the trigger, not the outcome. Cardinality is fixed at 5 by the literal
# label set below.
PDF_SELF_HEAL_TOTAL = Counter(
    "moneybin_pdf_self_heal_total",
    "Outcome of re-deriving a saved PDF recipe that stopped reconciling.",
    ["outcome"],  # repaired | repaired_pending_sign | refused_not_detected
    # | underivable | still_unreconciled
)

# Phase 1: cardinality bounded by distinct PDF aliases per user (~dozens).
# Revisit before multi-user hosted launch (M3E).
PDF_SEED_ROWS_TOTAL = Counter(
    "moneybin_pdf_seed_rows_total",
    "Rows written to raw.pdf_seeds.",
    ["alias"],
)

# Phase 2b — bridge egress events (Req 14). One increment per hand-off to the
# driving agent. Outcomes: "proposed" (egress occurred, no response yet),
# "applied" (agent returned a vetted recipe + rows that landed), "declined"
# (agent or user rejected the proposal), "invalid" (response failed
# parse_bridge_response / Recipe.model_validate). Labels stay bounded for stable
# dashboards.
PDF_BRIDGE_EGRESS_TOTAL = Counter(
    "moneybin_pdf_bridge_egress_total",
    # Unit is tool invocations, NOT unique documents: "proposed" bumps once per
    # escalating call, so one document previewed then imported emits two
    # "proposed" increments. Do not normalize as a per-document count.
    "PDF bridge hand-offs (per escalating tool call) to the driving agent by outcome.",
    ["outcome"],  # values: "proposed", "applied", "declined", "invalid"
)

# Sign-convention gate outcomes (ImportService._gate_pdf_sign_convention). A
# false-positive card detection would silently invert a real ledger, so this
# safety-critical gate needs the same visibility as the bridge egress above.
# "proposed" = an auto-derived negative_is_income inversion raised for
# confirmation; "confirmed" = ratified via confirm=True; "overridden" = a
# caller supplied an explicit sign= that overruled the detector.
PDF_SIGN_GATE_TOTAL = Counter(
    "moneybin_pdf_sign_gate_total",
    "PDF sign-convention gate outcomes by resolution.",
    ["outcome"],  # values: "proposed", "confirmed", "overridden"
)

# Tabular imports can infer the same whole-ledger inversion from a ``credit``
# header. Keep its outcomes separate from PDFs so an unexpected rise points to
# the channel whose detector needs attention.
TABULAR_SIGN_GATE_TOTAL = Counter(
    "moneybin_tabular_sign_gate_total",
    "Tabular sign-convention gate outcomes by resolution.",
    ["outcome"],  # values: "proposed", "confirmed", "overridden"
)

# ── Smart import confirmation ────────────────────────────────────────────────

IMPORT_CONFIRMATIONS_TOTAL = Counter(
    "moneybin_import_confirmations_total",
    "First-encounter confirms by channel, tier, and outcome.",
    ("channel", "tier", "outcome"),
)

IMPORT_DETECTION_SCORE = Histogram(
    "moneybin_import_detection_score",
    "Distribution of normalized confidence score across all detections.",
    # _score_mapping in column_mapper.py emits a discrete set today:
    # {0.40, 0.75, 0.85, 1.0}. Buckets are aligned to that distribution so
    # the histogram's high-band buckets aren't permanently empty (which
    # would make tuning t_high above 0.85 functionally equivalent to 0.86).
    # If _score_mapping evolves to a continuous distribution, re-fan these.
    buckets=(0.0, 0.4, 0.75, 0.85, 1.0),
)

IMPORT_SELF_ACCEPT_TOTAL = Counter(
    "moneybin_import_self_accept_total",
    "Agent self-accepts at `high` (zero until calibration gate opens).",
    ("channel",),
)

IMPORT_OVERRIDE_TOTAL = Counter(
    "moneybin_import_override_total",
    "Confirms that supplied a mapping override; high values flag weak detection.",
    ("channel",),
)

IMPORT_KNOWN_FORMAT_REUSE_TOTAL = Counter(
    "moneybin_import_known_format_reuse_total",
    "Silent reuses of a confirmed layout (mastery-curve KPI).",
    ("channel",),
)

IMPORT_REVALIDATION_FAILURE_TOTAL = Counter(
    "moneybin_import_revalidation_failure_total",
    "Known layout that failed the replay/validation guard and re-surfaced.",
    ("channel",),
)
# Declared but not yet incremented — the matched_format path (see
# ImportService._import_tabular) currently trusts the saved layout
# without a structural replay check (column presence, header drift).
# The .inc() call wires in when the replay guard lands; declaring the
# counter now keeps dashboards/alerting stable across that change.


# ── SQLMesh transforms ───────────────────────────────────────────────────────

SQLMESH_RUN_DURATION_SECONDS = Histogram(
    "moneybin_sqlmesh_run_duration_seconds",
    "Duration of SQLMesh model runs in seconds",
    ["model"],
)

# ── Deduplication ─────────────────────────────────────────────────────────────

DEDUP_MATCHES_TOTAL = Counter(
    "moneybin_dedup_matches_total",
    "Total duplicate records matched and merged",
    ["match_tier", "decided_by"],
)

DEDUP_PAIRS_SCORED = Counter(
    "moneybin_dedup_pairs_scored_total",
    "Total candidate pairs scored by the matching engine",
)

DEDUP_REVIEW_PENDING = Gauge(
    "moneybin_dedup_review_pending",
    "Number of match proposals awaiting user review",
)

DEDUP_MATCH_CONFIDENCE = Histogram(
    "moneybin_dedup_match_confidence",
    "Distribution of match confidence scores",
)

# ── Transfer detection ───────────────────────────────────────────────────────

TRANSFER_PAIRS_SCORED = Counter(
    "moneybin_transfer_pairs_scored_total",
    "Total transfer candidate pairs scored by the matching engine",
)

TRANSFER_MATCHES_PROPOSED = Counter(
    "moneybin_transfer_matches_proposed_total",
    "Total transfer pairs proposed for review",
)

TRANSFER_MATCH_CONFIDENCE = Histogram(
    "moneybin_transfer_match_confidence",
    "Distribution of transfer match confidence scores",
)

# ── Categorization ────────────────────────────────────────────────────────────

CATEGORIZATION_AUTO_RATE = Gauge(
    "moneybin_categorization_auto_rate",
    "Fraction of transactions auto-categorized (0.0–1.0)",
)

CATEGORIZATION_RULES_FIRED_TOTAL = Counter(
    "moneybin_categorization_rules_fired_total",
    "Total categorization rule firings by rule",
    ["rule_id"],
)

CATEGORIZE_ITEMS_TOTAL = Counter(
    "moneybin_categorize_items_total",
    "Number of items processed by categorize_items, by outcome",
    ["outcome"],
)

CATEGORIZE_DURATION_SECONDS = Histogram(
    "moneybin_categorize_duration_seconds",
    "Wall-clock duration of CategorizationService.categorize_items calls",
)

CATEGORIZE_ERRORS_TOTAL = Counter(
    "moneybin_categorize_errors_total",
    "Number of categorize_items calls that raised before returning a result",
)

CATEGORIZE_ASSIST_CALLS_TOTAL = Counter(
    "moneybin_categorize_assist_calls_total",
    "Number of categorize_assist invocations (MCP + CLI)",
    ["surface"],
)

CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL = Counter(
    "moneybin_categorize_assist_txns_returned_total",
    "Total redacted transactions returned across all categorize_assist calls",
)

CATEGORIZE_ASSIST_DURATION_SECONDS = Histogram(
    "moneybin_categorize_assist_duration_seconds",
    "Duration of categorize_assist server-side processing (excludes LLM time)",
)

CATEGORIZE_MATCH_OUTCOME_TOTAL = Counter(
    "moneybin_categorize_match_outcome_total",
    "Categorization matcher outcome by lookup shape and signal source.",
    # outcome: exemplar | exact | contains | regex | none
    # shape: description_only | memo_only | both
    ["outcome", "shape"],
)

CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL = Counter(
    "moneybin_categorize_write_skipped_precedence_total",
    "Categorization writes skipped because a higher-priority source already "
    "categorized the row.",
    ["src_existing", "src_attempted"],
)

CATEGORIZE_PROVIDER_NATIVE_TOTAL = Counter(
    "moneybin_categorize_provider_native_total",
    "Categorizations assigned from a provider's native categorization. "
    "trigger='sweep' is the automatic apply_plaid_categories pass over "
    "still-uncategorized rows; trigger='backfill' is the explicit "
    "improve_ai_categories upgrade pass over categorized_by='ai' rows.",
    ["source_type", "trigger"],
)

AUTO_RULE_PATTERN_DOWNGRADED_TOTAL = Counter(
    "moneybin_auto_rule_pattern_downgraded_total",
    "Machine-invented auto-rule patterns proposed as 'exact' rather than "
    "'contains' because they fell below the minimum contains length.",
)

AUTO_RULE_BROAD_PENDING = Gauge(
    "moneybin_auto_rule_broad_pending",
    "Pending auto-rule proposals flagged broad — blast radius disproportionate "
    "to evidence. Set on each review() call.",
)

AUTO_RULE_BROAD_ACCEPT_BLOCKED_TOTAL = Counter(
    "moneybin_auto_rule_broad_accept_blocked_total",
    "Accept attempts on a broad auto-rule proposal refused for want of an "
    "explicit allow_broad override.",
)

AUTO_RULE_UNSELECTIVE_ACCEPT_BLOCKED_TOTAL = Counter(
    "moneybin_auto_rule_unselective_accept_blocked_total",
    "Accept attempts on a proposal whose 'contains' pattern is too short to "
    "discriminate, refused for want of an explicit allow_broad override. "
    "Catches proposals already in app.proposed_rules from before the "
    "proposal-time downgrade (_invented_match_type) shipped.",
)

RULE_CREATE_UNSELECTIVE_CONTAINS_BLOCKED_TOTAL = Counter(
    "moneybin_rule_create_unselective_contains_blocked_total",
    "Direct rule-creation attempts refused because a 'contains' pattern was "
    "too short to discriminate — it would match unrelated merchants. Blocked "
    "unless the caller passes allow_broad.",
)

CATEGORIZE_SKIPPED_CONFIDENCE_TOTAL = Counter(
    "moneybin_categorize_skipped_confidence_total",
    "Provider-native categorizations skipped at the confidence gate. "
    "reason='below_gate' is a genuine low-confidence rejection (gate-tuning "
    "signal); reason='unknown' is an absent/unmapped confidence level (a "
    "data-quality signal) — kept distinct so one isn't misread as the other. "
    "trigger distinguishes the sweep pass (apply_plaid_categories) from the "
    "backfill pass (improve_ai_categories) — see CATEGORIZE_PROVIDER_NATIVE_TOTAL.",
    ["source_type", "reason", "trigger"],
)

# Metric names retain the historical `apply` prefix even after the MCP tool
# was renamed to `transactions_categorize_commit` (2026-05-17). Prometheus
# metric renames break downstream dashboards and alerts; treat the name as
# part of the public surface and only rename if a coordinated rollout is
# planned.
CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS = Histogram(
    "moneybin_categorize_apply_post_commit_duration_seconds",
    "Latency of the snowball categorize_pending call triggered after every "
    "transactions_categorize_commit invocation.",
)

CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED = Histogram(
    "moneybin_categorize_apply_post_commit_rows_affected",
    "Number of rows the snowball fan-out categorized per batch.",
    # Default Prometheus buckets target seconds (max 10) and collapse all
    # batch sizes >10 into +Inf. These buckets span the expected 0–50k row
    # range so the distribution stays useful on dashboards.
    buckets=(0, 1, 5, 25, 100, 500, 2_500, 10_000, 50_000, float("inf")),
)

# Per-merchant labels can grow with the number of system-created merchants
# (categorization-matching-mechanics.md §Open questions: "no cap in v1"). The
# metric is gauge-only and written only when an exemplar is appended, so
# label cardinality is bounded by the merchant population — acceptable for v1.
MERCHANT_EXEMPLAR_COUNT = Gauge(
    "moneybin_merchant_exemplar_count",
    "Per-merchant exemplar set size; alarm if any merchant exceeds 200 — "
    "may indicate need for graduation to a generalized pattern.",
    ["merchant_id"],
)

# ── Account identity resolution ──────────────────────────────────────────────

ACCOUNT_LINK_OUTCOMES_TOTAL = Counter(
    "moneybin_account_link_outcomes_total",
    "Outcomes of cross-source account identity resolution (AccountResolver)",
    ["result"],
)

ACCOUNT_LINK_REVIEW_PENDING = Gauge(
    "moneybin_account_link_review_pending",
    "Current count of pending account_link_decisions.",
)

ACCOUNT_LINK_CONFIDENCE = Histogram(
    "moneybin_account_link_confidence",
    "Resolution confidence for account-link candidate proposals.",
)

# ── Merchant identity resolution ─────────────────────────────────────────────

MERCHANT_LINK_REVIEW_PENDING = Gauge(
    "moneybin_merchant_link_review_pending",
    "Current count of pending merchant_link_decisions (distinct provider ids).",
)

MERCHANT_LINK_CONFIDENCE = Histogram(
    "moneybin_merchant_link_confidence",
    "Resolution confidence for merchant-link candidate proposals.",
)

MERCHANT_RESOLUTION_OUTCOME_TOTAL = Counter(
    "moneybin_merchant_resolution_outcome_total",
    "Merchant entity-id resolution ladder outcome per resolved transaction.",
    # outcome: adopted | auto_bound | proposed | minted
    ["outcome"],
)

MERCHANT_LINK_OUTCOMES_TOTAL = Counter(
    "moneybin_merchant_link_outcomes_total",
    "Outcomes of merchant-link review decisions via merchants_links_set.",
    # outcome: accepted | rejected
    ["outcome"],
)

# ── Investments ──────────────────────────────────────────────────────────────

INVESTMENT_EVENTS_RECORDED_TOTAL = Counter(
    "moneybin_investment_events_recorded_total",
    "Investment ledger rows written to raw.manual_investment_transactions, by "
    "taxonomy type. A reinvest increments twice (acquisition + income leg).",
    labelnames=("type",),
)

SECURITY_RESOLUTION_OUTCOMES_TOTAL = Counter(
    "moneybin_security_resolution_outcomes_total",
    "Security-reference resolution outcomes by winning rung: cusip | isin | "
    "ticker | name (resolved), or unresolved | ambiguous (raised).",
    labelnames=("rung",),
)


# ── MCP server ────────────────────────────────────────────────────────────────

MCP_TOOL_CALLS_TOTAL = Counter(
    "moneybin_mcp_tool_calls_total",
    "Total MCP tool invocations by tool name",
    ["tool_name"],
)

MCP_TOOL_DURATION_SECONDS = Histogram(
    "moneybin_mcp_tool_duration_seconds",
    "Duration of MCP tool calls in seconds",
    ["tool_name"],
)

# ── Synthetic data ───────────────────────────────────────────────────────────

SYNTHETIC_GENERATED_TRANSACTIONS_TOTAL = Counter(
    "moneybin_synthetic_generated_transactions_total",
    "Total synthetic transactions generated",
    ["persona"],
)

SYNTHETIC_GENERATION_DURATION_SECONDS = Histogram(
    "moneybin_synthetic_generation_duration_seconds",
    "Duration of synthetic data generation runs in seconds",
    ["persona"],
)

SYNTHETIC_RESET_TOTAL = Counter(
    "moneybin_synthetic_reset_total",
    "Total synthetic dataset resets performed",
    ["persona"],
)

DEMO_RUN_TOTAL = Counter(
    "moneybin_demo_run_total",
    "Total `moneybin demo` preset runs performed",
    ["persona"],
)

# ── Database ──────────────────────────────────────────────────────────────────

DB_QUERY_DURATION_SECONDS = Histogram(
    "moneybin_db_query_duration_seconds",
    "Duration of database queries in seconds",
    ["operation"],
)

# Per-operation_type cardinality bounded by the OperationType Literal vocabulary
# (interactive | migration | transform_apply | backup). A new operation class
# requires a typed update at both call sites — pyright catches mis-spellings.
DB_WRITE_LOCK_TIMEOUT_TOTAL = Counter(
    "moneybin_db_write_lock_timeout_total",
    "Write-lock acquisitions that exhausted the 10s timeout, by operation type.",
    ["operation_type"],
)

# Per-reason cardinality bounded by CheckpointReason Literal vocabulary
# (post_migration | post_transform | pre_backup | post_compact |
# post_large_import). New boundaries require updating both this label vocab
# and the typed Literal in src/moneybin/db_lock/_types.py.
DB_CHECKPOINT_TOTAL = Counter(
    "moneybin_db_checkpoint_total",
    "CHECKPOINT calls at durable boundaries, by reason.",
    ["reason"],
)

# ── Audit log ────────────────────────────────────────────────────────────────

audit_events_emitted_total = Counter(
    "moneybin_audit_events_emitted_total",
    "Audit log events written to app.audit_log.",
    ["action", "actor"],
)

app_mutation_audit_emitted_total = Counter(
    "moneybin_app_mutation_audit_emitted_total",
    "Protected app.* mutations that emitted a paired audit row, by repository "
    "and action. Counts at the *Repo boundary; audit_events_emitted_total counts "
    "at the AuditService boundary. A repo that mutates without going through "
    "BaseRepo._emit_audit() shows up as a gap between the two — the "
    "contract-violation signal Invariant 10 exists to catch.",
    ["repository", "action"],
)

audit_undo_total = Counter(
    "moneybin_audit_undo_total",
    "system_audit_undo invocations by outcome (success, not_found, "
    "already_undone, cascade_blocked, no_path). One increment per undo attempt.",
    ["outcome"],
)

audit_undo_rows_reversed_total = Counter(
    "moneybin_audit_undo_rows_reversed_total",
    "Audit rows inverted by successful undos (markers and no-ops excluded).",
)

# ── Sync (moneybin-sync pull/connect lifecycle) ────────────────────────────

SYNC_PULL_DURATION_SECONDS = Histogram(
    "moneybin_sync_pull_duration_seconds",
    "End-to-end duration of SyncService.pull() (trigger + fetch + load + remove).",
    ["provider"],
)

SYNC_PULL_OUTCOMES_TOTAL = Counter(
    "moneybin_sync_pull_outcomes_total",
    "Pull outcomes by provider and status (success or failed).",
    ["provider", "status"],
)

SYNC_PULL_TRANSACTIONS_LOADED = Counter(
    "moneybin_sync_pull_transactions_loaded_total",
    "Transactions loaded into raw.{provider}_transactions per pull.",
    ["provider"],
)

SYNC_INSTITUTION_ERRORS_TOTAL = Counter(
    "moneybin_sync_institution_errors_total",
    "Per-institution sync errors by Plaid error_code (ITEM_LOGIN_REQUIRED, "
    "INSTITUTION_DOWN, RATE_LIMIT_EXCEEDED, etc.).",
    ["error_code"],
)

SYNC_AUTH_REFRESH_OUTCOMES = Counter(
    "moneybin_sync_auth_refresh_outcomes_total",
    "Refresh-token rotation outcomes: success (rotated and retry succeeded), "
    "failed (refresh endpoint rejected the token, user must re-login), or "
    "second_401 (refresh succeeded but retry still got 401 — token-store drift).",
    ["outcome"],
)

SYNC_CONNECT_OUTCOMES = Counter(
    "moneybin_sync_connect_outcomes_total",
    "Connect-flow outcomes by terminal status (connected, failed, timeout).",
    ["status"],
)

# ── Investments sync ─────────────────────────────────────────────────────────

SYNC_INVESTMENTS_RECORDS_LOADED = Counter(
    "moneybin_sync_investments_records_loaded_total",
    "Investment records loaded per sync by raw table",
    ["table"],
)

INVESTMENT_AMOUNT_DRIFT_ROWS_TOTAL = Counter(
    "moneybin_investment_amount_drift_rows_total",
    "Plaid investment rows whose |amount| reconciles under neither fee convention",
)

PRICE_ROWS_WRITTEN_TOTAL = Counter(
    "moneybin_price_rows_written_total",
    # Rows the append-only insert actually wrote — a re-reported observation is
    # dropped and must not count, so a flat counter means a stalled price feed.
    "Price observations written to raw.security_prices, by source_type",
    ["source_type"],
)

SECURITY_LINK_OUTCOMES_TOTAL = Counter(
    "moneybin_security_link_outcomes_total",
    "SecurityResolver ladder outcomes per resolved security",
    ["result"],
)

SECURITY_LINK_DECISION_OUTCOMES_TOTAL = Counter(
    "moneybin_security_link_decision_outcomes_total",
    "Outcomes of security-link merge review decisions via SecurityLinksService.",
    # outcome: accepted | rejected
    ["outcome"],
)

SECURITY_LINK_REVIEW_PENDING = Gauge(
    "moneybin_security_link_review_pending",
    "Current count of pending security_link_decisions.",
)
