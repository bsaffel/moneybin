"""V030: account-identity clean re-mint (M1S.3, account-identity-resolution.md §Migration).

Brings pre-migration dogfooding data into the canonical-account-id world in
ONE transaction (the runner wraps ``migrate()`` in ``BEGIN``/``COMMIT``):

1. **Re-mint accounts.** For every distinct source account in
   ``raw.ofx_accounts`` / ``raw.tabular_accounts`` / ``raw.plaid_accounts``,
   mint a canonical ``account_id`` (``uuid4().hex[:12]``) and write an accepted
   ``source_native`` ``app.account_links`` row mapping
   ``(source_type, source_origin, ref_value=native)`` → canonical. **No
   cross-source collapse happens here**: ``core.dim_accounts.last_four`` is not
   populated until M1S.4, so the resolver's weak-signal pass can't fire — one
   canonical per existing source account, no ``app.account_link_decisions``.
2. **Re-point ``app.*`` ``account_id`` FKs** native → canonical.
3. **Re-key transaction-keyed curation.** ``transaction_id`` changed in B4
   (ADR-015): the OLD hash keyed on ``account_id``, the NEW hash keys on the
   immutable source identity. The old→new map is built by replaying BOTH hash
   formulas over ``raw.*`` + ``app.match_decisions`` (NOT the prep/core views —
   those are SQLMesh VIEWs whose catalog definitions still reflect the
   *pre-feature* model at migration time, since the migration runs at DB-open
   before the next ``sqlmesh run``). ``app.transaction_id_aliases`` is seeded
   and the curation FKs are rewritten in the same transaction.

**Why the replay is self-contained over ``raw.*``.** Pre-branch staging read
``account_id`` straight from ``raw.<src>_transactions.account_id`` (the
source-native key) with no minting or link translation, and the OLD
``transaction_id`` hashed that same value. So for all pre-migration data the
OLD-hash ``account_id`` slot equals ``source_account_key`` equals
``raw.<src>_transactions.account_id`` — one value, no archaeology. Matching
blocks on ``a.account_id = b.account_id`` (``matching/scoring.py``), so every
dedup group shares one ``account_id`` (== ``source_account_key``), which is why
the group reconstruction below is sound.

**Hash formulas** (kept in lockstep with the model snapshots; a migration is a
frozen artifact, so this replica is the historical record of the one-time
re-key, not living code that must track the models forever):

- OLD unmatched: ``SHA256(source_type|source_transaction_id|account_id)[:16]``.
- OLD matched (gold key): ``SHA256(LISTAGG(source_type|source_transaction_id|
  account_id sorted, '|'))[:16]`` over the dedup group's members.
- NEW (both): the group's priority **anchor** member hashed as
  ``SHA256(source_type|source_origin|source_account_key|source_transaction_id)
  [:16]`` (anchor = argmin over members by ``(stability_rank, loaded_at,
  source_type, source_origin, source_account_key, source_transaction_id)``;
  ``stability_rank``: ofx/plaid=0, manual=1, else=2). Mirrors
  ``sqlmesh/models/prep/int_transactions__matched.sql``.

**Audit (Invariant 10).** Every ``app.*`` mutation here is paired with an
``app.audit_log`` row (``actor='system'``); ``account_links`` rows also carry
``decided_by='system'``. Mirrors ``AccountLinksRepo.insert`` /
``AuditService.record_audit_event`` so the doctor audit-coverage check does not
flag these as orphaned mutations.

Idempotent on replay: account minting skips source accounts that already carry
an accepted ``source_native`` link; FK re-points and curation re-keys only
touch rows whose value is still a pre-migration key; aliases use
``ON CONFLICT DO NOTHING``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast

logger = logging.getLogger(__name__)

# stability_rank by source_type — mirrors int_transactions__matched.sql group_members.
_STABILITY_RANK = {"ofx": 0, "plaid": 0, "manual": 1}
_STABILITY_RANK_DEFAULT = 2  # tabular/csv/tsv/excel/gsheet/...

# Per-source SELECTs reconstructing the unioned source identity directly from
# raw — replicating the staging source_origin / source_account_key /
# source_transaction_id derivations so the NEW hash matches what the models will
# produce after the next refresh. Columns: source_type, source_origin,
# source_account_key, source_transaction_id, loaded_at.
_SOURCE_ROW_SQL = """
WITH ofx AS (
  SELECT
    'ofx' AS source_type,
    COALESCE(t.source_origin, a.institution_org, 'ofx_unknown') AS source_origin,
    t.account_id AS source_account_key,
    t.source_transaction_id AS source_transaction_id,
    t.loaded_at AS loaded_at
  FROM raw.ofx_transactions AS t
  LEFT JOIN raw.ofx_accounts AS a ON t.account_id = a.account_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.source_transaction_id, t.account_id ORDER BY t.loaded_at DESC
  ) = 1
), tabular AS (
  SELECT
    t.source_type,
    t.source_origin,
    t.account_id AS source_account_key,
    t.transaction_id AS source_transaction_id,
    t.loaded_at AS loaded_at
  FROM raw.tabular_transactions AS t
  WHERE t.deleted_from_source_at IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.transaction_id, t.account_id ORDER BY t.loaded_at DESC
  ) = 1
), plaid AS (
  SELECT 'plaid', t.source_origin, t.account_id, t.transaction_id, t.loaded_at
  FROM raw.plaid_transactions AS t
), manual AS (
  SELECT 'manual', t.source_origin, t.account_id, t.source_transaction_id, t.created_at
  FROM raw.manual_transactions AS t
)
SELECT * FROM ofx
UNION ALL SELECT * FROM tabular
UNION ALL SELECT * FROM plaid
UNION ALL SELECT * FROM manual
"""

# Distinct source accounts per raw account table. ofx source_origin is nullable
# (legacy pre-V028 rows); COALESCE to the staging sentinel so the NOT NULL
# account_links column is writable. tabular/plaid source_origin are NOT NULL.
_ACCOUNT_SOURCE_SQL = [
    "SELECT DISTINCT source_type, COALESCE(source_origin, 'ofx_unknown'), account_id "
    "FROM raw.ofx_accounts",
    "SELECT DISTINCT source_type, source_origin, account_id FROM raw.tabular_accounts",
    "SELECT DISTINCT source_type, source_origin, account_id FROM raw.plaid_accounts",
]


def _hash(text: str) -> str:
    """16-hex-char truncated SHA-256 — DuckDB ``SUBSTRING(SHA256(x), 1, 16)`` equivalent."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _anchor_sort_key(row: dict[str, Any]) -> tuple[int, datetime, str, str, str, str]:
    """Group-anchor ordering — mirrors int_transactions__matched.sql group_anchor."""
    loaded_at = row["loaded_at"]
    return (
        row["stability_rank"],
        loaded_at if loaded_at is not None else datetime.max,
        row["source_type"],
        row["source_origin"],
        row["source_account_key"],
        row["source_transaction_id"],
    )


def _json_default(value: object) -> str:
    """Serialize datetimes / Decimals / etc. in audit before/after row images."""
    return str(value)


def _emit_audit(
    conn: Any,
    *,
    operation_id: str,
    occurred_at: datetime,
    action: str,
    target_table: str,
    target_id: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
) -> str:
    """Insert one ``app.audit_log`` row mirroring ``AuditService.record_audit_event``.

    ``actor='system'``; full before/after row images (Invariant 10). Returns the
    audit_id. ``occurred_at`` is pinned to one transaction-stable timestamp.
    """
    audit_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO app.audit_log (
            audit_id, occurred_at, actor, action,
            target_schema, target_table, target_id,
            before_value, after_value, parent_audit_id, operation_id,
            context_json, is_undo, undoes_operation_id
        ) VALUES (?, ?, 'system', ?, 'app', ?, ?, ?, ?, NULL, ?, NULL, FALSE, NULL)
        """,
        [
            audit_id,
            occurred_at,
            action,
            target_table,
            target_id,
            json.dumps(before, default=_json_default) if before is not None else None,
            json.dumps(after, default=_json_default) if after is not None else None,
            operation_id,
        ],
    )
    return audit_id


def _remint_accounts(
    conn: Any, *, operation_id: str, occurred_at: datetime
) -> dict[str, str]:
    """Mint one canonical id + accepted source_native link per source account.

    Returns a native ``ref_value`` → canonical ``account_id`` map for the FK
    re-point. Ambiguous natives (same ref_value minted under two distinct
    ``(source_type, source_origin)`` source accounts → two canonicals) are left
    OUT of the map and logged: ``app.*`` tables store only the bare native key,
    so such a value can't be re-pointed unambiguously (the spec's re-import
    fallback covers this pre-launch edge).
    """
    ref_to_canonical: dict[str, set[str]] = {}
    for select_sql in _ACCOUNT_SOURCE_SQL:
        for source_type, source_origin, native in conn.execute(select_sql).fetchall():
            existing = conn.execute(
                "SELECT account_id FROM app.account_links "
                "WHERE status = 'accepted' AND ref_kind = 'source_native' "
                "AND source_type = ? AND source_origin = ? AND ref_value = ? LIMIT 1",
                [source_type, source_origin, native],
            ).fetchone()
            if existing is not None:
                canonical = cast(str, existing[0])  # idempotent replay: reuse link
            else:
                canonical = uuid.uuid4().hex[:12]
                link_id = uuid.uuid4().hex[:12]
                conn.execute(
                    """
                    INSERT INTO app.account_links (
                        link_id, account_id, ref_kind, ref_value, source_type,
                        source_origin, status, decided_by, decided_at
                    ) VALUES (?, ?, 'source_native', ?, ?, ?, 'accepted', 'system', ?)
                    """,
                    [
                        link_id,
                        canonical,
                        native,
                        source_type,
                        source_origin,
                        occurred_at,
                    ],
                )
                after = _row_dict_by_pk(conn, "account_links", "link_id", link_id)
                _emit_audit(
                    conn,
                    operation_id=operation_id,
                    occurred_at=occurred_at,
                    action="account_link.insert",
                    target_table="account_links",
                    target_id=link_id,
                    before=None,
                    after=after,
                )
            ref_to_canonical.setdefault(native, set()).add(canonical)

    mapping: dict[str, str] = {}
    for native, canonicals in ref_to_canonical.items():
        if len(canonicals) == 1:
            mapping[native] = next(iter(canonicals))
        else:
            logger.warning(
                f"V030: native account ref maps to {len(canonicals)} canonical ids "
                f"across distinct source accounts; skipping FK re-point for it "
                f"(re-import from source to resolve)."
            )
    return mapping


def _row_dict_by_pk(
    conn: Any, table: str, pk_col: str, pk_value: str
) -> dict[str, Any]:
    """Fetch one ``app.<table>`` row by a single-column PK as a dict."""
    cur = conn.execute(
        f"SELECT * FROM app.{table} WHERE {pk_col} = ?",  # noqa: S608  # table/pk_col are constants
        [pk_value],
    )
    columns = [d[0] for d in cur.description]
    return dict(zip(columns, cur.fetchone(), strict=True))


def _audited_remap(
    conn: Any,
    *,
    operation_id: str,
    occurred_at: datetime,
    table: str,
    column: str,
    mapping: dict[str, str],
    action: str,
    target_id_fn: Callable[[dict[str, Any]], str],
) -> int:
    """Re-point ``app.<table>.<column>`` old→new via ``mapping``, per-row audited.

    Captures full before-images up front (rowid is not stable across DuckDB
    UPDATEs), then applies one set-based UPDATE per distinct old value (the
    mapping is injective here — no cross-source collapse — so a value never
    chains). The after-image is the before-image with ``column`` replaced. Skips
    a re-point that would collide with an existing row (defensive guard, not an
    expected path). Returns the number of rows re-pointed.
    """
    cur = conn.execute(
        f"SELECT * FROM app.{table} WHERE {column} IS NOT NULL"  # noqa: S608  # constants
    )
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, r, strict=True)) for r in cur.fetchall()]

    affected = [
        row
        for row in rows
        if mapping.get(row[column]) is not None and mapping[row[column]] != row[column]
    ]
    if not affected:
        return 0

    # Collision guard: an old→new re-point whose target already exists on a row
    # NOT being re-pointed would violate a PK/unique. Cannot happen without
    # collapse; warn + skip those old values.
    existing_values = {row[column] for row in rows}
    remapped_olds = {row[column] for row in affected}
    safe: list[dict[str, Any]] = []
    for row in affected:
        new_value = mapping[row[column]]
        if new_value in existing_values and new_value not in remapped_olds:
            logger.warning(
                f"V030: skipping {table}.{column} re-point {row[column]!r} → target "
                f"already present (would violate uniqueness)."
            )
            continue
        safe.append(row)

    for old_value in {row[column] for row in safe}:
        conn.execute(
            f"UPDATE app.{table} SET {column} = ? WHERE {column} = ?",  # noqa: S608  # constants
            [mapping[old_value], old_value],
        )
    for before in safe:
        after = {**before, column: mapping[before[column]]}
        _emit_audit(
            conn,
            operation_id=operation_id,
            occurred_at=occurred_at,
            action=action,
            target_table=table,
            target_id=target_id_fn(after),
            before=before,
            after=after,
        )
    return len(safe)


def _repoint_account_fks(
    conn: Any, *, operation_id: str, occurred_at: datetime, mapping: dict[str, str]
) -> None:
    """Re-point every ``app.*`` ``account_id`` FK native → canonical (per the grep).

    FK set verified against the live schema (``grep account_id
    src/moneybin/sql/schema/ src/moneybin/repositories/``): account_settings,
    balance_assertions, match_decisions (account_id AND account_id_b),
    categorization_rules, gsheet_connections.
    """

    def assertion_target(row: dict[str, Any]) -> str:
        # Mirrors balance_assertions_repo._target_id composite.
        return f"{row['account_id']}|{row['assertion_date']}"

    _audited_remap(
        conn,
        operation_id=operation_id,
        occurred_at=occurred_at,
        table="account_settings",
        column="account_id",
        mapping=mapping,
        action="account.remint",
        target_id_fn=lambda r: str(r["account_id"]),
    )
    _audited_remap(
        conn,
        operation_id=operation_id,
        occurred_at=occurred_at,
        table="balance_assertions",
        column="account_id",
        mapping=mapping,
        action="account.remint",
        target_id_fn=assertion_target,
    )
    _audited_remap(
        conn,
        operation_id=operation_id,
        occurred_at=occurred_at,
        table="match_decisions",
        column="account_id",
        mapping=mapping,
        action="account.remint",
        target_id_fn=lambda r: str(r["match_id"]),
    )
    _audited_remap(
        conn,
        operation_id=operation_id,
        occurred_at=occurred_at,
        table="match_decisions",
        column="account_id_b",
        mapping=mapping,
        action="account.remint",
        target_id_fn=lambda r: str(r["match_id"]),
    )
    _audited_remap(
        conn,
        operation_id=operation_id,
        occurred_at=occurred_at,
        table="categorization_rules",
        column="account_id",
        mapping=mapping,
        action="account.remint",
        target_id_fn=lambda r: str(r["rule_id"]),
    )
    _audited_remap(
        conn,
        operation_id=operation_id,
        occurred_at=occurred_at,
        table="gsheet_connections",
        column="account_id",
        mapping=mapping,
        action="account.remint",
        target_id_fn=lambda r: str(r["connection_id"]),
    )


def _build_transaction_id_map(conn: Any) -> dict[str, str]:
    """Replay OLD and NEW transaction_id formulas → old→new map (old != new only).

    Self-contained over raw.* + app.match_decisions. See module docstring for
    the formulas and why the views can't be used at migration time.
    """
    # node = (account_id, source_type, source_transaction_id); account_id is the
    # shared blocking account == source_account_key for pre-migration data.
    rows = conn.execute(_SOURCE_ROW_SQL).fetchall()
    node_row: dict[tuple[str, str, str], dict[str, Any]] = {}
    for source_type, source_origin, sak, stid, loaded_at in rows:
        node = (sak, source_type, stid)
        node_row[node] = {
            "source_type": source_type,
            "source_origin": source_origin,
            "source_account_key": sak,
            "source_transaction_id": stid,
            "loaded_at": loaded_at,
            "stability_rank": _STABILITY_RANK.get(source_type, _STABILITY_RANK_DEFAULT),
        }

    # Union-find over accepted dedup match edges (mirror active_matches filter).
    parent: dict[tuple[str, str, str], tuple[str, str, str]] = {}

    def find(node: tuple[str, str, str]) -> tuple[str, str, str]:
        parent.setdefault(node, node)
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    def union(a: tuple[str, str, str], b: tuple[str, str, str]) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    edges = conn.execute(
        "SELECT account_id, source_type_a, source_transaction_id_a, "
        "source_type_b, source_transaction_id_b FROM app.match_decisions "
        "WHERE match_status = 'accepted' AND reversed_at IS NULL AND match_type = 'dedup'"
    ).fetchall()
    for aid, st_a, stid_a, st_b, stid_b in edges:
        union((aid, st_a, stid_a), (aid, st_b, stid_b))

    # Group nodes by union-find root. A node may come from a match edge without a
    # corresponding source row (orphan edge) — it still contributes to the OLD
    # LISTAGG but not to the NEW anchor (which requires a real source row),
    # mirroring the model's group_members JOIN to the unioned set.
    groups: dict[tuple[str, str, str], set[tuple[str, str, str]]] = {}
    all_nodes = set(node_row) | set(parent)
    for node in all_nodes:
        groups.setdefault(find(node), set()).add(node)

    id_map: dict[str, str] = {}
    for members in groups.values():
        if len(members) > 1:
            # OLD gold key: LISTAGG(source_type|source_transaction_id|account_id,
            # '|' ORDER BY source_type, source_transaction_id, account_id) over all
            # group members (account_id == node[0]). Order by the (st, stid, aid)
            # TUPLE — not the concatenated string — to match the model's ORDER BY
            # exactly even when a tabular source_transaction_id contains '|'.
            ordered = sorted(members, key=lambda n: (n[1], n[2], n[0]))
            parts = [f"{st}|{stid}|{aid}" for (aid, st, stid) in ordered]
            old_id = _hash("|".join(parts))
        else:
            aid, st, stid = next(iter(members))
            old_id = _hash(f"{st}|{stid}|{aid}")
        # NEW id: priority anchor over members that have a real source row.
        sourced: list[dict[str, Any]] = [node_row[n] for n in members if n in node_row]
        if not sourced:
            continue  # purely-orphan edge group; no output row to re-key
        anchor = min(sourced, key=_anchor_sort_key)
        new_id = _hash(
            f"{anchor['source_type']}|{anchor['source_origin']}|"
            f"{anchor['source_account_key']}|{anchor['source_transaction_id']}"
        )
        if old_id != new_id:
            id_map[old_id] = new_id
    return id_map


def _rekey_transactions(
    conn: Any, *, operation_id: str, occurred_at: datetime, id_map: dict[str, str]
) -> None:
    """Seed app.transaction_id_aliases and rewrite transaction-keyed curation FKs."""
    if not id_map:
        logger.info("V030: no transaction_id changes to re-key.")
        return

    for old_id, new_id in id_map.items():
        existing = conn.execute(
            "SELECT 1 FROM app.transaction_id_aliases "
            "WHERE old_transaction_id = ? LIMIT 1",
            [old_id],
        ).fetchone()
        if existing is not None:
            continue  # idempotent replay: alias already seeded + audited
        conn.execute(
            "INSERT INTO app.transaction_id_aliases "
            "(old_transaction_id, new_transaction_id, created_at) VALUES (?, ?, ?)",
            [old_id, new_id, occurred_at],
        )
        # app.* mutation → paired audit (Invariant 10), mirroring
        # TransactionIdAliasesRepo.insert so doctor audit-coverage stays clean.
        after = _row_dict_by_pk(
            conn, "transaction_id_aliases", "old_transaction_id", old_id
        )
        _emit_audit(
            conn,
            operation_id=operation_id,
            occurred_at=occurred_at,
            action="transaction_id_alias.insert",
            target_table="transaction_id_aliases",
            target_id=old_id,
            before=None,
            after=after,
        )

    curation: tuple[tuple[str, Callable[[dict[str, Any]], str]], ...] = (
        ("transaction_categories", lambda r: str(r["transaction_id"])),
        ("transaction_notes", lambda r: str(r["note_id"])),
        ("transaction_tags", lambda r: f"{r['transaction_id']}:{r['tag']}"),
        ("transaction_splits", lambda r: str(r["split_id"])),
    )
    for table, target_id_fn in curation:
        _audited_remap(
            conn,
            operation_id=operation_id,
            occurred_at=occurred_at,
            table=table,
            column="transaction_id",
            mapping=id_map,
            action="transaction.rekey",
            target_id_fn=target_id_fn,
        )

    # Keep the denormalized doctor-suppression prediction (V026) in sync. raw,
    # not app.* — no audit (Invariant 10 governs app.* only), mirroring V026.
    for old_id, new_id in id_map.items():
        conn.execute(
            "UPDATE raw.manual_transactions SET transaction_id = ? WHERE transaction_id = ?",
            [new_id, old_id],
        )


def _already_applied(conn: Any) -> bool:
    """True if every distinct source account already carries an accepted link.

    Guards manual/test re-invocation. The migration's transaction replay derives
    match groups from ``app.match_decisions.account_id`` while it still holds the
    source-native key; once the FK re-point rewrites it to canonical (i.e. the
    migration committed once), a second pass would mis-derive. The runner already
    runs V030 exactly once — this makes a bare ``migrate()`` re-call a clean
    no-op too. ``app.account_links`` is empty pre-migration (M1S.1 table), so any
    full coverage means a prior successful run.
    """
    linked = 0
    total = 0
    for select_sql in _ACCOUNT_SOURCE_SQL:
        for source_type, source_origin, native in conn.execute(select_sql).fetchall():
            total += 1
            row = conn.execute(
                "SELECT 1 FROM app.account_links WHERE status = 'accepted' "
                "AND ref_kind = 'source_native' AND source_type = ? "
                "AND source_origin = ? AND ref_value = ? LIMIT 1",
                [source_type, source_origin, native],
            ).fetchone()
            if row is not None:
                linked += 1
    return total > 0 and linked == total


def migrate(conn: object) -> None:
    """Re-mint accounts, re-point ``account_id`` FKs, re-key transaction curation."""
    if _already_applied(conn):
        logger.info("V030: already applied (all source accounts linked); skipping.")
        return
    operation_id = f"op_{uuid.uuid4().hex}"
    # One transaction-stable timestamp for every row this migration writes.
    occurred_at = cast(
        datetime,
        conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0],  # type: ignore[union-attr]
    )
    logger.info("V030: re-minting account identities + re-keying transaction curation")
    # Build the transaction old→new map FIRST, against the pre-mutation state:
    # the replay's match-group reconstruction relies on app.match_decisions
    # carrying the source-native account_id (== raw source_account_key), which
    # the FK re-point below rewrites to canonical.
    id_map = _build_transaction_id_map(conn)
    mapping = _remint_accounts(conn, operation_id=operation_id, occurred_at=occurred_at)
    _repoint_account_fks(
        conn, operation_id=operation_id, occurred_at=occurred_at, mapping=mapping
    )
    _rekey_transactions(
        conn, operation_id=operation_id, occurred_at=occurred_at, id_map=id_map
    )
