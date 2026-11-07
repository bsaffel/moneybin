# Feature: PDF Disclosure Consent

> Companions: [`smart-import-pdf.md`](smart-import-pdf.md) (the bridge this
> spec gates; **supersedes its Requirement 14**),
> [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) (AI data-flow tiers,
> vendor profiles, consent ledger; **replaces its bridge trust model**),
> [`privacy-data-classification.md`](privacy-data-classification.md) (consent
> ledger + Critical redaction), [`smart-import-confirmation.md`](smart-import-confirmation.md)
> (`import_confirm`, unchanged), [`surface-design.md`](../../.claude/rules/surface-design.md),
> [`services.md`](../../.claude/rules/services.md) (confirmation binding).
> Tracking: MB-59. Verification: MB-237.

## Status
<!-- draft | ready | in-progress | implemented -->
implemented

## Intent

When local parsing cannot read a PDF statement, MoneyBin hands the document to
the AI the user is driving it with (the *bridge*). Today the bridge response
carries the transparency notice **and** the verbatim document text in the same
payload (`ImportService._raise_pdf_bridge_escalation`). That is disclosure, not
consent: by the time anyone can read the warning, the AI host already holds the
data.

This spec makes the bridge a two-phase exchange. The first response says that
AI help is needed and asks for consent, and carries no document content. Content
leaves only after a person has accepted a plain statement of the risk: this
statement's financial data goes, unredacted except for the Critical fields
MoneyBin parsed, to a named AI host whose data-use terms are summarized.

Finance plus AI leaves no trust budget. The existing notice reads as a consent
step but cannot act as one, which is worse than none.

## Goals

- No document text, table values, page images or layout fingerprint in any
  MoneyBin emission before consent: MCP responses, CLI stdout/stderr, logs,
  audit rows or errors.
- One consent rule across MCP, CLI and (future) web UI, with a per-surface
  interaction.
- A "remember" choice that is explicit, tied to one AI host, visible and
  revocable, and invalidated when that host's summarized terms change.
- Approval, denial and actual disclosure are each audited without a copy of the
  payload.
- Critical fields MoneyBin parsed (account numbers, SSNs, routing numbers) are
  redacted in the disclosed payload.

## Non-goals

- An in-process LLM backend (`AIBackend`): still deferred.
- AI fallback for non-PDF formats (MB-60).
- Changing deterministic-first parsing, reconciliation, sign-convention
  confirmation or `import_confirm`.
- Building a web UI. None exists in `src/`; this spec sets the rule any future
  web AI hand-off must follow.

## Anti-goals (a technically working build that still fails)

- A consent prompt sent in the same payload as the content.
- `--yes`, a generic confirm, or an agent's own judgment counted as consent
  without the risk statement reaching a person. The tools-only relay path below
  is the single accepted exception, and it is audited as such.
- A remembered consent covering an AI host the person never chose ("any AI").
- A remembered consent created from an approval an agent carried. A fabricated
  or prompt-injected "the person said yes" must cost at most one request.
- Product text implying MoneyBin controls a copy of the PDF the user gave
  their AI host directly.

## Honest control boundary

MoneyBin controls and audits only what it emits. If the user attached the PDF
to their AI chat, or the file sits in a directory the agent can read, the AI
host already has it and nothing here changes that. MoneyBin cannot detect it.
The consent text and docs say so in one line: *"This covers what MoneyBin
sends. If you already shared this file with your AI app directly, it already
has the contents."* No response or test claims MoneyBin refused a disclosure
it could not have prevented.

## Decisions (settled)

1. **Two phases.** Local deterministic parsing first. A bridge-eligible
   outcome returns a content-free *AI assistance required* result with a
   consent request. Content is released only after consent.
2. **Per-surface interaction, one rule.** The person is told exactly what the
   risk is and must accept it. Where the person answers MoneyBin directly, they
   may also remember the choice.
3. **Tools-only MCP clients:** when the client cannot prompt (no elicitation),
   the agent may relay the risk statement and obtain approval in chat.
4. **Agent-driven CLI:** agents cannot answer an interactive prompt, so a
   command-line flag approves. The flag takes the `request_id` from the
   content-free run, so the risk statement is always issued first. Flag
   approval covers that one request.
5. **"Remember" is tied to the AI host** (the app: Claude Desktop, Claude Code,
   ChatGPT/Codex, Cursor), which is what people expect from app permission
   prompts and what MoneyBin can observe. The AI vendor's data-use terms are
   shown as information on the consent screen.
6. **Unidentified recipient → one-time only.** No remember option when
   MoneyBin cannot name the AI host.
7. **Critical fields MoneyBin parsed are redacted** before disclosure. Page
   images or the raw file cannot be redacted. If an image path ever ships, the
   consent text says plainly that images go unredacted.
8. **Agent-carried approval → one-time only.** The MCP relay token and the CLI
   `--allow-ai-disclosure` flag never create a remembered grant. On those paths
   MoneyBin hears only the agent's claim that the person approved, so a
   fabricated or prompt-injected approval must cost at most one request, never
   every future PDF. Remembering is available only where the person answers
   MoneyBin directly: the MCP elicitation form and the interactive
   `moneybin privacy grant smart-import-parsing --recipient <host>` (refused
   inside an agent shell). For an identified host, the relay and flag results
   name that `privacy grant` command so the agent can pass it on.

## Design

### Flow

```mermaid
sequenceDiagram
    participant H as Person
    participant A as AI host / agent
    participant M as MoneyBin
    A->>M: import_preview / import_files (PDF)
    M->>M: local parse (deterministic)
    alt deterministic success
        M-->>A: rows (no bridge)
    else bridge-eligible, active remembered consent for this AI host
        M->>M: audit disclosure (grant_id)
        M-->>A: bridge payload (Critical redacted) + "sent under remembered consent" notice
    else bridge-eligible, no consent
        alt client supports elicitation
            M->>H: consent prompt (risk, AI host, vendor terms, remember?)
            H-->>M: accept / decline / no answer
            M->>M: audit consent outcome
            M-->>A: bridge payload on accept; content-free refusal otherwise
        else tools-only client
            M-->>A: ai_assistance_required (content-free) + consent request + token
            A->>H: relays risk statement verbatim
            H-->>A: yes / no in chat
            A->>M: same call + confirmation_token (one-time only)
            M->>M: verify binding, audit consent (agent_relay), audit disclosure
            M-->>A: bridge payload
        end
    end
    A->>M: import_confirm(preview_id=..., bridge_response=...) — unchanged
```

### 1. The content-free result

On a bridge-eligible outcome without valid consent, `pdf_preview` and
`_import_pdf` stop raising `ImportConfirmationRequiredError` with a
`BridgePayload`. They return a new confirmation channel, `ai_assistance_required`,
whose payload carries:

- `consent_request`: a binding id, `expires_at`, the risk statement, the
  `recipient` (`host_name`, `host_version`, `identified: bool`, `source:
  "reported_by_client" | "cli_environment" | "none"`), the vendor terms summary
  (or `null` with a reason such as "model depends on your app settings"),
  and `what_is_sent` (document text, first rows of each table, Critical fields
  redacted). It carries no remember option: the request is answered through an
  agent on the relay and flag paths, which never remember (Decision 8).
- `file`: the caller-supplied path and the routing `decision_reason`, which is
  needed so the agent can explain why local parsing failed.
- `actions[]`: per surface, how to consent and how to decline.

**Excluded:** `document_text`, `tables_preview`, header cells, the layout
fingerprint, `saved_recipe_for_re_derive`, row counts tied to content, and any
image. The payload type declares its data classes like every typed payload, and
a test pins that it carries no financial value class.

After consent, the `fingerprint` that `import_preview` returns for
deterministic and non-bridge outcomes is redacted with the same policy as the
bridge payload, and `saved_recipe_for_re_derive` has its string values redacted
before release; digit masking leaves labels and anchors intact.

Scanned or image-only PDFs keep today's explicit unsupported outcome. Nothing
leaves, so no consent is asked.

**Where the gate lives.** The service stays synchronous. `pdf_preview` and
`_import_pdf` gain a consent argument (an already-verified consent: a grant id
or a verified binding). Without one they return the content-free result and
never call `build_bridge_request`. The async MCP tool layer runs elicitation or
verifies a relay token, then calls the service again with the verified consent.
The CLI does the same around its prompt or flag. Building the payload is the
release point, so it happens only on the consented call.

**Unattended paths.** Inbox processing already drains PDFs as a human actor
(`inbox_service`), so a bridge-eligible PDF takes the seed fallback and never
reaches the bridge. That stays: no unattended path builds a bridge request, and
a remembered consent does **not** fire on one. A test pins it. Disclosure
happens only when someone runs the import through MCP or `import preview` on
the CLI.

### 2. Binding

A consent request binds, through the existing `ConfirmationBinding` /
`confirmation_broker` machinery: the SHA-256 of each PDF's bytes, the
`request_kind`, the recipient key, the profile, the transport, and the redaction
policy version. It is short-lived (the broker's configured TTL) and single-use.
At release time MoneyBin recomputes the binding from the live call and refuses
on mismatch, distinguishing expired, replayed and mismatched outcomes, as
`services.md` requires.

Mapping onto the existing `ConfirmationBinding` (fixed fields, `extra="forbid"`):
`operation_kind = "pdf_disclosure"`, `resolved_ids` = the PDF SHA-256s (sorted),
`arguments` = `request_kind`, recipient key and redaction policy version,
`authorization_context` = the transport, and `profile` / `actor` as today.
`blast_radius` = `{files: n}`. No new binding type. An unidentified host has
no recipient key, so its token's `arguments` also carry the reported client
name and the MCP session id, and another client on the same server cannot
redeem it. A 2026-07-28 connection mints a session id per request, so there
the token binds the client name only.

Tokens are **MCP-only**. The broker is process-local, so a CLI result carries
no token; the CLI consents through its prompt or flag (§3). Revoking a
recipient must invalidate that recipient's outstanding tokens, so the broker
gains a recipient → digests index, or the release step re-checks that no
revocation postdates the token's issue time. The builder picks one.

A batch call (`import_files` over several PDFs) produces **one** consent covering
every bridge-eligible file in that call, listed by name. One user action gets one
prompt (`privacy-and-ai-trust.md` §Tier 3). Deterministic files in the batch
import without a prompt, and after consent `import_files` stages one preview per
released file and returns each `preview_id`, so a single decision reaches
`import_confirm(preview_id=..., bridge_response=...)`.

**Consent is bound to the file bytes.** A one-time consent carries the SHA-256s
it was given for, and the service refuses to release a file whose live bytes are
not in that set (the content-free result is returned instead; nothing is built,
audited as a release or counted). A remembered grant is not file-bound. The CLI
reads the file once and uses those bytes for both the request id and the
release; the MCP preview path re-uses the snapshot bytes.

### 3. Per-surface consent

| Surface | How consent is given | Remember | Audited as |
|---|---|---|---|
| MCP, client supports elicitation | Elicitation prompt: risk statement, AI host, vendor terms, accept boolean + remember boolean. No token is issued on this path. | Offered when the host is identified | `prompt` |
| MCP, tools-only client | Phase 1 returns the consent request + token. The agent shows the risk statement verbatim, gets a yes in chat, and repeats the call with the token. | **Never** (agent-carried); for an identified host the actions name `privacy grant --recipient` for the person to run | `agent_relay` |
| CLI, interactive terminal (stdin, stdout and stderr are TTYs), host not identified | Typer prompt showing the same text. One-time only: no remember prompt exists, because an identified host never reaches this row (see below). | Never | `cli_prompt` |
| CLI, non-interactive, or inside an identified agent shell | Without the flag: the content-free result (exit 0; JSON `status` is `ok` with `data.status = "ai_assistance_required"`), carrying a `request_id` and the flag in its hint. With `--allow-ai-disclosure <request_id>`: consent + disclosure. A bare flag is a usage error (exit 2); a non-matching id is refused (exit 1, `import_pdf_disclosure_request_mismatch`). | **Never** (agent-carried); for an identified host the result names `privacy grant --recipient` for the person to run | `cli_flag` |
| Remembered consent present | No prompt; disclosure carries a visible notice naming the grant and the revoke command | — | `remembered` |
| Web UI (future) | A dialog equivalent to the elicitation prompt. No AI hand-off exists today. | Same rules | `web_prompt` |

- Elicitation reuses `elicit_bounded` (site label `pdf_disclosure`). Timeout,
  decline and cancel all release nothing and return the content-free result
  with a refusal reason.
- Elicitation is the stronger path. When the client supports it, a supplied
  token is refused. Otherwise an agent could skip the prompt by relaying.
- The CLI `request_id` is derived from the PDF SHA-256s, the recipient key, the
  `terms_version` and the redaction policy version, so it survives across
  separate CLI processes and changes whenever any of them changes. It is
  deterministic for the same file, recipient, terms and redaction policy. It is
  not a secret and not single-use, unlike the MCP token: anyone who can compute
  it can replay it for as long as those inputs hold. It makes the agent receive
  the risk statement before it can consent, the same guarantee the MCP relay
  token gives. It does not prove a person read it.
- **The CLI prompt faces only unidentified hosts.** An agent shell MoneyBin
  identifies (today `CLAUDECODE=1`) gets no prompt, whatever its TTY state: an
  agent with a pty could otherwise answer the prompt itself. It gets the
  receipt and `request_id`, and consent arrives through `--allow-ai-disclosure`
  after the agent asks the person. The same rule makes `privacy grant
  --recipient` refuse inside an identified agent shell. So the "Remember for
  <host>?" prompt is not shipped: the one place a CLI prompt appears is an
  unidentified host, which gets one-time consent. An agent MoneyBin cannot
  identify, if it has a pty, could still answer that prompt. That gives it no
  more release power than the flag, but its approval is attributed as
  `cli_prompt`, not `cli_flag`.
- **Agent-carried approvals never remember** (Decision 8). Neither the relay
  token nor the flag accepts a remember choice, and neither writes a grant. For
  an identified host the content-free result instead names
  `moneybin privacy grant smart-import-parsing --recipient <host>`, which the
  person runs in their own terminal; it grants the same recipient key the gate
  checks (`local:<host>`).
- The `agent_relay` path is weaker by construction: MoneyBin cannot prove a
  person said yes. The audit row says so, and the one-time scope bounds what a
  false yes can release to the files that token was bound to.

**The CLI disclosure path is new.** Today the CLI never sends PDF content.
`import preview` on a bridge-eligible PDF prints a content-free receipt, and
`import files` runs as a human actor and takes the seed fallback. The receipt's
hint points at `import confirm --bridge-response`, which an agent in a shell
cannot use because it never received the material to write a response. This
spec adds the missing step so shell-driven agents reach the same outcome as MCP
clients:

- `import preview <pdf>` gains `--output json`. Before consent it emits the
  content-free result (§1) as an envelope. After consent (prompt, flag or
  remembered grant) it emits the bridge request, the same payload MCP returns.
  Text output after consent prints the document text and table preview, which
  a person can hand to any AI tool themselves.
- `import confirm <pdf> --bridge-response <file> --confirm` already applies a
  response and is unchanged.
- `import files` keeps its human-actor seed fallback. It never discloses.

The current `import preview` also writes the `smart_import_parse` audit row for
a disclosure that never happened. §9 removes that write.

### 4. Identifying the AI host

- **MCP:** the `clientInfo.name` / `version` the host reports at initialize. A
  release-maintained table maps known names to a display name and a vendor
  profile (one vendor, or "varies by model" for multi-model hosts such as
  Cursor). An unknown name is shown as reported but counts as **unidentified**
  for remembering.
- **CLI:** environment markers known agent hosts set for their shell tools (for
  example Claude Code sets `CLAUDECODE=1`). Same table, one row per host. No
  marker → unidentified.
- The recipient key is the normalized host name. `host_version` is display
  and audit only, so a host upgrade does not invalidate grants.
- Nothing in `src/` reads `clientInfo` today. The builder confirms FastMCP
  exposes it (expected: `ctx.session.client_params.clientInfo`). If it is
  absent, every MCP host is unidentified (one-time only), not guessed.
- The shipped table maps `claude-ai` (Claude Desktop), `cursor-vscode`
  (Cursor) and the `CLAUDECODE=1` shell marker (Claude Code). Claude Code's own
  MCP `clientInfo.name` is not verified, so Claude Code over MCP is
  unidentified: one-time consent only, no remember option, until its name is
  verified. Hosts not verified from a primary source are left out.
- Both are self-reported. Text says "as reported by your AI app". Tying consent
  to the host prevents accidental reuse across apps, not a host that lies.
- **Network MCP transports** (`sse`, `streamable-http`): only stdio can
  identify a host. Network sessions carry no authenticated principal today,
  so a known `clientInfo.name` over them is still **unidentified**: one-time
  consent only, no remember option, no grant written, and no grant consulted.
  Remembering over a remote transport waits on authenticated transports
  (MB-32, MB-57), which bind the authenticated principal into the recipient
  key. The key already carries the transport class (`remote:` vs `local:`),
  so a grant made over local stdio never covers a remote session.

### 5. Remembered consent in the ledger

A remembered consent is a `persistent` row in `app.ai_consent_grants` with
`feature_category = 'smart-import-parsing'`. The row gains (migration, pre-launch
schema change):

- `recipient_kind` — `ai_backend` (existing rows) or `ai_host` (new), so the
  `backend` column does not silently hold two kinds of thing. The builder may
  instead rename `backend` → `recipient`. Pick one and update the column
  comments and `CONTEXT.md` **Consent** in the same change.
- `terms_version` — the vendor-profile version the person saw. A release that
  changes that profile's version makes the grant inactive. The next disclosure
  asks again, and the old row stays as history.

This makes `smart-import-parsing` the **first enforced consent category**. The
ledger has been "ledger-first, gate deferred" until now. Update the schema
comment, `ConsentService` docstring and `CONTEXT.md` to say which category is
enforced.

- The uniqueness rule becomes one active grant per
  `(feature_category, recipient_kind, recipient)`. An `ai_host` row never
  collides with, revokes, or is revoked by an `ai_backend` row.
- A legacy `smart-import-parsing` grant for an AI backend (`anthropic`) does
  **not** satisfy this gate. The recipient here is the host.
- `grant_prompt` stores the shared risk statement (§10) verbatim, followed by
  the sentence on remembering that the person was shown (the elicitation
  message, or the `privacy grant` statement). That replaces
  `ConsentService._build_prompt` for `ai_host` grants.
- `moneybin privacy grant/revoke` gain `--recipient <host>` beside `--backend`
  (mutually exclusive). `privacy status` lists `ai_host` grants with
  `approved_via` and `terms_version`. `default_backend` fallback never applies
  to `ai_host`. Granting
ahead of time (`moneybin privacy grant smart-import-parsing --recipient <host>`)
remembers consent before the first disclosure. The gate matches it only when a
session identifies that same host (§4): an unidentified host always gets
one-time consent, whatever grants exist, so a pre-grant cannot make consent
stick for a host MoneyBin cannot identify. The grant is keyed to a local
recipient, so it never covers a network MCP session. It
requires an interactive terminal and shows the risk statement. MCP
`privacy_consent_set` cannot grant an `ai_host` consent, because an agent would
be granting itself standing access without the statement ever being issued.
Every remembered `ai_host` grant therefore comes from a flow the person answers
directly: the MCP elicitation form or the interactive `privacy grant`. A relay
token and a CLI `request_id` approve one request and never write a grant. An
agent driving a pseudo-terminal outside a marked agent shell can still answer a
terminal prompt; that residual limit is accepted.

One-time consents are **not** written to the ledger. They live only as the
consumed binding plus their audit row.

### 6. Vendor terms summary

Extend `privacy-and-ai-trust.md`'s provider profiles into a release-shipped
registry keyed by vendor, each entry carrying: retention, training use, policy
URL, a plan caveat ("terms differ for consumer, business and API plans; MoneyBin
cannot see yours"), and `terms_version`. Summaries state the range of plans,
never the best case. A multi-model host shows "depends on the model selected in
<host>" plus the host's own policy link.

### 7. Redaction of parsed Critical fields

Before `build_bridge_request`, `document_text` and `tables_preview` pass
through a **new, narrow redactor**. It redacts:

- every occurrence of the account number MoneyBin identified for this document
  (`extractors/pdf/identity.py`), in any separator form, `.` included. This
  fails closed: an amount whose digits are exactly the identifier's
  (`123456.78` for `12345678`) is indistinguishable from the dotted identifier
  and is masked, which costs at most a non-reconciling row; and
- strictly formatted SSNs (`NNN-NN-NNNN`) and nine-digit routing numbers that
  pass the ABA checksum.

It does **not** reuse the log maskers (`log_sanitizer.mask_pii_shaped`). Those
mask any long digit run by design, which would mangle unseparated amounts and
break recipe replay. A Critical value MoneyBin did not identify can still go out.
The consent text names exactly what is redacted, never "all sensitive
numbers", and says everything else is sent as-is.

**Risk the builder must close:** the agent derives a recipe from redacted text,
but `execute_recipe` replays it against the local, unredacted text. Redaction
must preserve token shape: each masked digit becomes `X`, while length,
separators and the last four digits are kept, so that agent-authored patterns
still match locally and account identity (last four) still resolves. An
identified account number that contains letters is matched as it appears
(letters in any case, the same separator tolerance) and its letters become `X`
too, since they are part of the number. That letter-bearing match only adds
coverage: the identifier's bare digits are always matched as well, and a
captured label word (`Acct`, `CHK`) or leading mask (`XXXX`, `****`) never
becomes part of the pattern. Replacing
digits with `X` changes their character class; this is acceptable because
identifiers rarely sit inside the row tokens a recipe matches. Tests must replay a bridge recipe derived from
redacted text against the original and reconcile.

The redaction scope is exactly the account numbers, routing numbers and SSNs
MoneyBin identified; nothing else is masked, and the consent text, the
`import_preview` tool description and the post-consent hint all say so in the
same words.

If a page-image path ever ships, images are unredactable. Its consent text says
"page images are sent with nothing masked", and that path cannot reuse a grant
made under text-only terms (the redaction policy version is part of the
binding and the grant).

### 8. Lifecycle

| Event | Behavior |
|---|---|
| Decline / cancel | Nothing released. Audit `declined`. Retrying issues a fresh request. |
| Elicitation timeout | Same as decline, reason `timeout`. |
| Token expired / replayed / mismatched | Refused with the distinct broker error. Nothing released. Rerun to get a fresh request. |
| Revocation | Takes effect for the next call. Also invalidates outstanding unexpired tokens for that recipient: a token is refused if any grant for that recipient was revoked at or after the token was issued. Tokens are process-local; one whose issue record is gone is refused as expired. |
| Interrupted call (transport drop mid-call) | Content is released only in the response of the call that verified consent. A drop before verification records nothing. A drop after records the disclosure (it may have left). |
| Retry after disclosure | With a remembered consent: discloses again, audited again. With one-time consent: asks again. |
| Terms version change | Grant inactive; asks again. |
| Consent given | Does **not** authorize the import. `import_confirm` and sign confirmation are unchanged. |

### 9. Audit and metrics

- `smart_import_consent` audit row per decision: outcome (`approved`,
  `declined`, `timeout`, `expired`), `approved_via` (table above), recipient
  key, `identified`, `terms_version`, `remembered`, `grant_id` when one is
  created, `binding_id` for a one-time decision (joining it to its
  `smart_import_parse` row), `released` (whether any content left after an
  approval; `false` when the file changed after consent or the release
  failed), file SHA-256s, and the `decision_reason`. A person's approval is
  recorded after the release attempt whether or not anything was released;
  `remembered` and `grant_id` are set only when it was. A remembered grant that
  releases nothing records no row, since no one decided anything. No content. A refused
  relay token is recorded as `expired` with `refusal_code`, the broker error
  code (expired, replayed or mismatched), so abuse stays distinguishable. A
  token refused because its recipient was revoked currently records
  `MUTATION_CONFIRMATION_EXPIRED`, indistinguishable from expiry. A
  mismatched CLI `request_id` is recorded as `outcome="expired"`,
  `approved_via="cli_flag"`, `refusal_code="import_pdf_disclosure_request_mismatch"`.
  The outcome enum has no separate `refused` value.
- `smart_import_parse` (existing action) is now written **only when content is
  actually released**, carrying the consent reference (grant id or binding
  id) and `approved_via`. The current pre-release write in
  `_raise_pdf_bridge_escalation` (which records the layout fingerprint) is
  removed. The consent row never carries the fingerprint.
- Metrics: `pdf_bridge_egress_total{outcome}` keeps counting releases. A new
  `pdf_disclosure_consent_total{outcome, approved_via}` counts decisions.
  Both are registered in `metrics/registry.py` as
  `moneybin_pdf_bridge_egress_total` and `moneybin_pdf_disclosure_consent_total`.
- Logging: no document content reaches a log record. The `pdfminer` logger is
  pinned to WARNING in `logging/config.py`, because at DEBUG it writes raw PDF
  tokens (document text) to the root logger, including under `--verbose`.

### 10. Wording

The risk statement is one constant, shared by every surface and stored verbatim
in `grant_prompt`. It must say: what is sent (statement text and table rows,
unredacted except for account numbers, SSNs and routing numbers), to whom (the
AI host, as reported), what that vendor's terms say, that MoneyBin cannot pull it
back, the control-boundary line above, and how to revoke a remembered consent.

## Required tests (threat-oriented)

Synthetic PDFs seeded with canary values (description, amount, a full account
number, an SSN):

1. Pre-consent: no canary in the MCP `import_preview` / `import_files` responses,
   CLI text and JSON output, log records, audit rows or error messages, for
   native-text, replay-failure and batch cases.
2. Post-consent payload: account-number and SSN canaries redacted; description
   and amount canaries present.
3. Elicitation decline, cancel and timeout → no canary anywhere, and the audit
   outcome is correct.
4. Token: expired, replayed, mismatched file bytes, mismatched recipient, and a
   token supplied to an elicitation-capable client → all refused, nothing
   released.
5. `import preview` without the disclosure flag, in text or JSON, releases
   nothing, and `import files --yes` never discloses. A bare
   `--allow-ai-disclosure`, a stale `request_id` (file bytes, recipient or
   terms version changed), or another file's `request_id` is refused with
   nothing released.
6. Unidentified host: remember is not offered, the flag does not create a
   grant, and a ledger grant is never created implicitly.
7. Remembered consent: discloses with notice; revoke stops the next call; a
   `terms_version` bump stops the next call; a stdio grant does not cover a
   different transport.
8. `privacy_consent_set` refuses an `ai_host` grant.
9. A recipe derived from redacted text replays and reconciles against the
   original document.
10. Scanned PDF: unsupported outcome unchanged, no consent prompt.
11. Inbox drain of a bridge-eligible PDF builds no bridge request, even with a
    remembered grant present.
12. Agent-carried approval: a relay token or `--allow-ai-disclosure` for an
    identified host releases once, writes no grant, and the next call asks
    again; the consent request advertises no remember option, and the
    `privacy grant --recipient` command it names grants the key the gate checks.

## Spec amendments on delivery

- `smart-import-pdf.md` Requirement 14 and the bridge "transparency notice"
  are replaced by this spec. `TRANSPARENCY_NOTICE` and the
  `BridgeRequest.transparency_notice` field are removed, and
  `ImportConfirmationBridgePayload` (`privacy/payloads/imports.py`) follows.
- `privacy-and-ai-trust.md` §bridge trust model ("no new egress relationship …
  no new privacy stance applies") is replaced. The bridge is a consented
  disclosure to an identified AI host.
- `CONTEXT.md` **Consent** is updated to the recipient model (AI backend or AI
  host). **AI host** is added as a term.

## Left to the builder

- Exact field, error-code and confirmation-channel names, within
  `surface-design.md` (no new MCP tool: consent rides the existing
  `import_preview` / `import_files` arguments).
- The CLI flag name. Proposed: `--allow-ai-disclosure <request_id>`. It must
  stay distinct from `import files --yes`, which already auto-accepts fuzzy
  account matches.
- The CLI exit code for "consent needed".
- The host-name and environment-marker table contents, verified against each
  host's docs.
- Shape-preserving redaction details (§7) and the migration shape (§5).

## As delivered

- Module map: host table and risk statement in `privacy/disclosure.py`; grants
  in `ConsentService` (`active_host_grant`, `grant_host_consent`); the service
  gate in `ImportService.pdf_preview` / `record_disclosure_decision`; MCP
  consent in `mcp/pdf_disclosure.py`; shared pure helpers in
  `adapters/pdf_disclosure_adapters.py`; CLI consent in
  `cli/commands/_import_pdf_disclosure.py`.
- MCP surface: no new tool. `import_preview` and `import_files` gain
  `confirmation_token` (one-time only; no remember parameter, per Decision 8).
  `privacy_consent_set` refuses `ai_host` recipients. On both surfaces a
  person's approval is audited after the release attempt with `released` (§9),
  and an elicitation remember choice creates a grant only when content was
  released; the grant stores the elicitation message. The tools run file reads,
  imports, the refresh and consent database calls off the event loop, so the
  per-tool timeout still applies; only the elicitation wait stays on it.
- CLI surface: `import preview <pdf> --allow-ai-disclosure <request_id>` and
  `-o json`; `privacy grant/revoke --recipient <host>`. `import files --yes`
  never consents.
- Not shipped: a CLI "Remember for <host>?" prompt (see §3), and a typed payload
  for the released CLI JSON bridge (its privacy-log classes come from
  `ImportPdfBridgePreviewPayload`).
