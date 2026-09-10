MODEL (
  name prep.stg_ofx__transactions,
  kind VIEW
);

/* Rows whose id the extractor derived by suffixing another id, paired with the id
   they supersede. `_disambiguate_colliding_fitids` appends '#<hash>' to EVERY
   member of a colliding FITID group, so a file first imported before the
   collision appeared keeps a bare row the later import cannot overwrite — the raw
   PK carries the id, and the dedup window below partitions on it, so both reach
   core and one real transaction is counted twice.

   Keyed on `fitid_repaired`, which the extractor writes on exactly the rows it
   rewrote. The marker is NOT evidence: the OFX spec does not reserve '#', so an
   institution may legitimately mint both 'X' and 'X#reference' for two distinct
   transactions, and content equality cannot separate that from a repair —
   identifiers.md is explicit that two genuinely distinct transactions can carry
   identical content. Inferring provenance from the marker therefore deletes a
   real transaction, silently and with no review entry. Suppress on proof.

   The superseded id is everything before the LAST marker, derived by position
   rather than by matching a pattern against the id: '_' and '%' are LIKE
   wildcards, and a FITID containing either would let a pattern borrow an
   unrelated id's suffixed row and drop a transaction that was never superseded.
   Splitting the id here (instead of prefix-matching in the anti-join) also keeps
   the join a plain equality — a correlated prefix test is quadratic per account. */
WITH superseding AS (
  SELECT
    account_id,
    source_origin,
    LEFT(
      source_transaction_id,
      LENGTH(source_transaction_id) - STRPOS(REVERSE(source_transaction_id), '#')
    ) AS superseded_transaction_id,
    transaction_type,
    date_posted,
    amount,
    payee,
    memo,
    check_number
  FROM raw.ofx_transactions
  WHERE
    fitid_repaired AND CONTAINS(source_transaction_id, '#')
), ranked AS (
  SELECT
    t.source_transaction_id,
    t.account_id,
    t.transaction_type,
    t.date_posted::DATE AS posted_date,
    t.amount,
    TRIM(t.payee) AS payee,
    TRIM(t.memo) AS memo,
    t.check_number,
    t.currency_code,
    t.to_currency,
    t.to_amount,
    t.source_file,
    t.extracted_at,
    t.loaded_at,
    t.import_id,
    'ofx' AS source_type,
    t.source_origin,
    ROW_NUMBER() OVER (PARTITION BY t.source_transaction_id, t.account_id ORDER BY t.loaded_at DESC) AS _row_num
  FROM raw.ofx_transactions AS t
  /* Content equality on exactly the fields the extractor hashes into the suffix
     (extractor.py::_FITID_SIGNATURE_FIELDS): members of one collision group
     differ by construction, so matching all six identifies the twin that replaced
     this row — and a bare row that is a genuinely third transaction matches none
     of them and survives. Raw values, untrimmed, because that is what the
     extractor hashed.

     Exact equality is the point, not a shortcut. A bare row whose payee drifted
     between exports (pending → posted) is NOT suppressed here: nothing at this
     layer can tell a re-export apart from a distinct transaction, and dropping a
     row we cannot prove superseded is the silent data loss the extractor already
     refuses to risk. That case falls through to the matcher, where an ambiguous
     pair belongs.

     Scoped to one source origin as well as one account. The account key here is
     source-native, not canonical: nothing scopes an OFX ACCTID globally, so two
     institutions can both mint 'ACC1'. Without the origin predicate, one bank's
     suffixed row could delete the other's bare row whenever all six hashed
     fields happened to agree. Requiring the same origin costs a legitimate
     suppression only if one institution's exports arrive under two origin slugs
     — which double-counts a row, where the alternative deletes one. */
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM superseding AS s
      WHERE
        s.account_id = t.account_id
        AND s.source_origin IS NOT DISTINCT FROM t.source_origin
        AND s.superseded_transaction_id = t.source_transaction_id
        AND s.transaction_type IS NOT DISTINCT FROM t.transaction_type
        AND s.date_posted IS NOT DISTINCT FROM t.date_posted
        AND s.amount IS NOT DISTINCT FROM t.amount
        AND s.payee IS NOT DISTINCT FROM t.payee
        AND s.memo IS NOT DISTINCT FROM t.memo
        AND s.check_number IS NOT DISTINCT FROM t.check_number
    )
)
SELECT
  COALESCE(links.account_id, ranked.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  ranked.account_id AS source_account_key,
  ranked.source_transaction_id,
  ranked.transaction_type,
  ranked.posted_date,
  ranked.amount,
  ranked.payee,
  ranked.memo,
  ranked.check_number,
  ranked.currency_code,
  ranked.to_currency,
  ranked.to_amount,
  ranked.source_file,
  ranked.extracted_at,
  ranked.loaded_at,
  ranked.import_id,
  ranked.source_type,
  ranked.source_origin
FROM ranked
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = ranked.source_type
  AND links.source_origin = ranked.source_origin
  AND links.ref_value = ranked.account_id
WHERE
  ranked._row_num = 1
