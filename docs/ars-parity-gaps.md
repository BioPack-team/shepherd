# ARS Parity — Gap Analysis & Work Items

Second-pass comparison of Shepherd's ARS implementation against the third-party
ARS spec sheet (NCATS Translator **Relay @ `c87c130`**). Purpose: a working
document to drive complete parity, one gap at a time. Each gap is a self-contained
card:

- **ARS** — what the reference does (spec section).
- **Shepherd today** — what we do now, with code references.
- **Difference & impact** — what actually diverges and why it matters.
- **Proposed approach** — concrete plan (files, functions, reuse).
- **Open questions** — decisions to make before implementing.

Status legend: 🔴 not implemented · 🟡 partial / diverges · 🟢 matches ·
🏗️ intentional architectural divergence.

Groups: **A** structural divergences (context) · **B** correctness gaps ·
**C** notifications/subscriptions · **D** completion/timeout.

---

## A. Structural divergences (intentional — decide if in scope)

These follow decisions already made. They change *structure*, not usually *output*.
Listed so the "do we match this?" decision is explicit per item.

### A1 — Orchestration: worker streams vs ORM signals 🏗️
- **ARS:** fan-out and completion are `post_save`/`pre_save` side effects of
  `Message.save()` (`signals.py`); `_skip_post_save` guards recursion.
- **Shepherd:** Redis-Streams workers (`workers/ars`, `workers/ars_accumulate`);
  completion is event-driven via `ars_children` + `claim_ars_tail`.
- **Impact:** none on output; different failure/observability model.
- **Decision:** keep (equivalent). No work unless exact structural parity is required.

### A2 — Fan-out: internal workflows vs external HTTP + SmartAPI 🏗️
- **ARS:** `send_message` POSTs to each ARA's SmartAPI-resolved endpoint with a
  per-child callback URL; actors matched by **channel** intersection.
- **Shepherd:** `workers/ars/worker.py::ars` enqueues onto internal ARA streams
  from the static `settings.ars_aras` list; no channel matching, no SmartAPI
  endpoint/param resolution used for routing.
- **Impact:** the set of ARAs is static config, not registry/channel-driven.
- **Decision:** keep (your chosen internal-reuse model). Revisit only if you want
  registry-driven routing.

### A3 — Data model: flat tables vs Message tree 🏗️
- **ARS:** self-referential `Message` (head + child-per-ARA-response +
  child-per-merged-version; `merged_version`, `merged_versions_list`).
- **Shepherd:** `shepherd_brain` + `ars_children` (one row per parent×ARA).
- **Impact:** no first-class "merged version" objects; the trace API exposes
  per-ARA children, not a version chain.
- **Decision:** tied to A4.

### A4 — Merge cadence: batch vs incremental merged-versions 🏗️
- **ARS:** each ARA response creates a **new** `ars-ars-agent` merged child that is
  fully post-processed; clients can fetch progressively-better merged versions.
- **Shepherd:** wait for all ARAs → one cross-ARA merge → one post-merge tail.
- **Impact:** no mid-flight merged versions; final result is equivalent in content
  but there's a single merged output, not a version history.
- **Decision:** the largest structural item; likely out of scope unless full
  fidelity is required. Note several C/D gaps (per-merge notifications, merged_list
  in status) depend on this existing.

### A5 — Merge concurrency gate: per-parent lock vs expensive-token semaphore 🏗️
- **ARS:** cluster-wide Redis-ZSET semaphore ("expensive tokens", 12 concurrent,
  3-min lease) + per-parent DB `merge_semaphore`.
- **Shepherd:** per-worker `TASK_LIMIT` + per-parent Redis lock
  (`acquire_lock(parent_response_id)`).
- **Impact:** throughput throttling differs; correctness of per-parent
  serialization is preserved.
- **Decision:** keep unless cluster-wide throttling is needed.

---

## B. Correctness gaps (affect output)

### B1 — Final ranking (Sugeno integral + weighted mean) 🟢 DONE · priority: HIGH · effort: M
> **Done:** ported ARS `scoring.py` to `shepherd_utils/ars_scoring.py`
> (`compute_from_results`, faithful; lambda solved once + `weight_sets` via an
> explicit dict, output cross-checked bit-for-bit against the reference). Runs at
> the end of `answer_appraise`, writing `sugeno`/`weighted_mean`/`rank` and sorting
> by rank so `filter_results_top_n` trims the ARS-ordered results.
- **ARS:** `scoring.compute_from_results(results)` computes a Sugeno integral and a
  weighted mean from each result's `ordering_components`, writes `rank`, `sugeno`,
  `weighted_mean` onto each result, and **sorts results by rank** (spec §9.5).
- **Shepherd:** no Sugeno anywhere (`sugeno`/`weighted_mean`/`compute_from_results`
  = 0 matches). `workers/answer_appraise/worker.py::normalize_scores` percentile-
  ranks the **max analysis score** (`_result_score`) into `normalized_score`; the
  appraiser's `ordering_components` are merged onto results but never fed into any
  score or ordering.
- **Difference & impact:** the **final answer ordering is not the ARS ordering.**
  This is the single most output-visible gap.
- **Proposed approach:** port ARS `scoring.py` to `shepherd_utils/ars_scoring.py`
  (pure: `compute_from_results(results)`); add a tail step (either inside
  `answer_appraise` after `appraise`, or a new `ars_score` op before
  `filter_results_top_n`) that ranks + sorts by Sugeno. Replace/990 retire the
  percentile `normalize_scores` for the ARS path (see B3).
- **Open questions:** exact Sugeno fuzzy-measure weights + component set the ARS
  uses (need `scoring.py` verbatim); do we keep `normalized_score` at all, or emit
  `rank`/`sugeno`/`weighted_mean` only? Does `filter_results_top_n` need to sort by
  the new `rank` instead of score?

### B2 — Normalize-before-merge → cross-ARA dedup correctness 🟢 DONE · priority: HIGH · effort: M
> **Done (option a):** canonicalization moved to `shepherd_utils/ars_norm.py` and
> now runs **per ARA response before the merge** in `ars_accumulate` (outside the
> parent lock). The merge then sees canonical ids, so `merge_result_maps` collapses
> the same entity returned by two ARAs under different curies. `node_norm` dropped
> from `ARS_TAIL_WORKFLOW` (tail now starts at `ars_blocklist`); the `node_norm`
> worker remains a valid standalone op delegating to `ars_norm`.
- **ARS:** `pre_merge_process` canonicalizes each ARA response's node ids **before**
  the merge, so the same entity returned by two ARAs under different curies collapses
  to one node/answer during merge (spec §6.2, §13.3).
- **Shepherd:** cross-ARA merge runs on **raw** ids. `ars_merge.result_key` keys on
  the raw first-binding ids; `node_norm` runs later on the merged graph and re-keys
  nodes + rewrites bindings but **does not re-run result dedup**.
- **Difference & impact:** two ARAs returning the same answer under different curies
  produce **two results** that never merge (their raw `result_key`s differ). After
  `node_norm` they bind the same canonical id but remain separate results.
- **Proposed approach:** two options —
  (a) **Normalize per-ARA-response before accumulate** (closest to ARS): run node
  normalization in/just-before `ars_accumulate` so merge sees canonical ids; or
  (b) **Re-dedup after node_norm:** add a `merge_result_maps`-style re-dedup pass at
  the end of `node_norm` (reuse `shepherd_utils/ars_merge.py`).
  Option (a) is more faithful but reshapes the pipeline; (b) is localized.
- **Open questions:** (a) vs (b)? If (a), do we normalize each child response as it
  arrives (more calls to the normalizer) or normalize the accumulating parent
  incrementally? Interaction with B1 (dedup changes scores).

### B3 — `normalized_score` computed twice / dead value 🟢 DONE · priority: MED · effort: S
> **Done:** removed the dead merge-time `average_result_scores` call (+ its extra
> lock round-trip) in `ars_accumulate`. Result scoring is now owned solely by the
> appraiser tail (`normalize_scores` + Sugeno `compute_from_results`).
- **ARS:** `normalized_score` is accumulated as a list across merges and **averaged
  once** at the end of a merge fold (spec §8, §14).
- **Shepherd:** `ars_merge.average_result_scores` averages it during accumulate, then
  `answer_appraise.normalize_scores` **overwrites** it with a fresh percentile rank.
  The averaged value never reaches output.
- **Difference & impact:** wasted computation + inconsistent semantics; resolves
  naturally with B1 (pick one score source).
- **Proposed approach:** decide the single source of truth for result scoring
  (Sugeno per B1). Either keep the merge-time average and drop the appraise-time
  overwrite, or vice-versa. Remove the dead path.
- **Open questions:** folded into B1's decision.

### B4 — `scrub_null_attributes` 🔴 · priority: MED · effort: S
- **ARS:** removes `None` entries from node/edge attribute lists; ensures edge
  `sources` have non-null `resource_id` (drops bad sources) and list-valued
  `upstream_resource_ids`; defaults aux graphs to `attributes:[]` (spec §6.1, §9.2).
- **Shepherd:** none (`scrub_null` = 0 matches).
- **Impact:** malformed attributes/sources can flow through to merge/validation.
- **Proposed approach:** add `scrub_null_attributes(message)` to
  `shepherd_utils/ars_merge.py` (or a new `ars_clean.py`); call it per-response
  (pre-merge, if B2(a)) and/or at the head of the tail.
- **Open questions:** where to run it (per-response vs on merged) depends on B2.

### B5 — `decorate_edges_with_infores` 🟡 · priority: MED · effort: S
- **ARS:** ensures each KG edge has a `sources` entry for the responding ARA's
  inforesid — add a `primary_knowledge_source` if none exist, else add self as
  `aggregator_knowledge_source` (spec §6.3).
- **Shepherd:** `shared.py::merge_kgraph` only appends a single hard-coded
  `infores:shepherd` **aggregator** source, and only to edges that already have
  `sources`; edges with no `sources` are not backfilled with a primary; the specific
  ARA inforesid is never recorded.
- **Difference & impact:** provenance loses per-ARA attribution; edges lacking
  sources stay source-less.
- **Proposed approach:** add `decorate_edges_with_infores(message, inforesid)`;
  needs the per-ARA inforesid (map `settings.ars_aras` entry → `infores:<ara>`), so
  it must run **per response** (ties to B2(a)) where the source ARA is known.
- **Open questions:** requires per-response processing to know which ARA an edge
  came from — if we stay batch (B2(b)), we lose the per-edge ARA attribution and can
  only tag `infores:shepherd`. Acceptable?

### B6 — Node canonization `biolink:xref` / `biolink:same_as` 🟢 DONE · priority: MED · effort: S
> **Done:** `get_normalized_nodes` now keeps the normalizer's
> `equivalent_identifiers`; on re-keying a node `canonize_message` appends a
> `biolink:xref` (original id) and `biolink:same_as` (equivalent ids) attribute in
> the ARS `canonizeMessage` shapes (`metatype:NodeIdentifier`).
- **ARS:** on re-keying a node to its canonical id, adds `biolink:xref` (original id)
  and `biolink:same_as` (equivalent identifiers) attributes; backfills categories
  (spec §6.2).
- **Shepherd:** `node_norm` backfills categories, rewrites ids/edges/bindings, but
  **discards** the normalizer's `equivalent_identifiers` — no xref/same_as emitted.
- **Impact:** downstream consumers lose original-id + synonym provenance.
- **Proposed approach:** in `node_norm.get_normalized_nodes` keep
  `equivalent_identifiers`; in `canonize_message`, when `old_id != canonical`, append
  `biolink:xref` (old id) and `biolink:same_as` (equivalent ids) attributes.
- **Open questions:** attribute value shapes (list of curies? attribute objects?) —
  confirm against a real normalizer response.

### B7 — `remove_phantom_support_graphs` 🔴 · priority: LOW · effort: S
- **ARS:** before validation, strips `biolink:support_graphs` references to aux
  graphs that don't exist (spec §4.8, §5.6).
- **Shepherd:** none. `shared.py::validate_message` only *logs* dangling aux refs
  (and has a `attibutes` typo so the loop never runs); call sites are commented out.
- **Impact:** phantom support-graph refs can fail TRAPI validation (once B8 lands).
- **Proposed approach:** implement `remove_phantom_support_graphs(message)`; fix the
  `attibutes` typo while there. Run just before validation (B8).
- **Open questions:** none significant.

### B8 — TRAPI validation (reasoner-pydantic) 🔴 · priority: MED · effort: M
- **ARS:** validates every ARA response (and merged message) via reasoner-pydantic;
  invalid → `code=422`, `status='E'`, notify `ara_failed_validation`, don't merge
  (spec §4.8, §5.6).
- **Shepherd:** no schema validation on the ARS path (`reasoner-pydantic` not a
  dependency).
- **Impact:** malformed ARA responses are merged as-is; no `ara_failed_validation`.
- **Proposed approach:** add `reasoner-pydantic` to a worker; validate each response
  (pre-merge) and mark the child ERROR on failure (feeds C1 event + D1 completion).
- **Open questions:** which TRAPI version to pin; validate per-response only, or the
  merged message too; performance cost on large messages.

### B9 — `result_stat` / `ScoreStatCalc` 🔴 · priority: LOW · effort: S
- **ARS:** computes median/mean/stdev/min/max over per-result scores, stored in
  `result_stat`, and surfaced in status `stats` (spec §6, §12).
- **Shepherd:** none.
- **Impact:** the `stats` block in notifications/status is absent (see C1, C5).
- **Proposed approach:** `ScoreStatCalc(results)` helper; store alongside the
  message (needs a place — a new column or in the message envelope). Depends on where
  status/stats are surfaced (C5).
- **Open questions:** where to persist result_stat given our flat model.

### B10 — Async-callback idempotency 🟢 DONE · priority: HIGH · effort: S
> **Done:** `ars_callback` rejects duplicates (already-DONE→200, has results→409,
> ERROR→400); `ars_accumulate` re-checks status under the parent lock and marks
> DONE inside it so concurrent duplicates serialize and can't double-merge.
- **ARS:** the async ingest endpoint rejects duplicates — child already Done → 200
  "already received"; child already has `result_count>0` → 409; child already Error →
  400 (spec §5.5).
- **Shepherd:** `base_routes.ars_callback` never checks child status and always
  enqueues `ars_accumulate`; `set_ars_child_status` is an unconditional UPDATE. A
  **duplicate callback double-merges** into the parent.
- **Difference & impact:** duplicate/retried ARA callbacks corrupt the merged result
  (double-counted results/edges).
- **Proposed approach:** in `ars_callback` (or at the top of `ars_accumulate`),
  read the `ars_children` row; short-circuit if the ARA is already DONE/ERROR or the
  child already recorded results. Add a `WHERE status NOT IN ('DONE','ERROR')` guard
  to the merge/`set_ars_child_status` path so the merge is idempotent even under a
  race.
- **Open questions:** return codes to mirror (200/409/400) on `/ars/callback`, or just
  silently drop duplicates internally?

---

## C. Notification / subscription gaps

### C1 — Event vocabulary + completion event 🟡 · priority: MED · effort: M
- **ARS:** emits `ara_response_complete`, `ara_failed_validation`,
  `merged_version_begun`, `merged_version_available`, `last_merged_completed`, and
  `admin`/`ars_error` with `complete:true` (spec §12).
- **Shepherd:** emits only `status: DONE|merging|timeout` and **nothing on final
  completion** (`finish_query` publishes no event).
- **Impact:** subscribers can't distinguish lifecycle stages; no "query complete"
  push. (Per-merged-version events depend on A4.)
- **Proposed approach:** standardize the `publish_ars_event` payloads to ARS event
  names; add a completion event in `finish_query` (and an error event on
  ABANDONED/timeout). `merged_version_*` only meaningful if A4 is adopted.
- **Open questions:** which events matter without incremental merged-versions (A4)?
  Minimum viable: `ara_response_complete`, `last_merged_completed`/`complete`,
  `ars_error`.

### C2 — Signed outbound notifications 🔴 · priority: MED · effort: S
- **ARS:** each notification POST carries HMAC-SHA256 `x-event-signature` over
  canonical (sorted-keys, no-space) JSON using the client's decrypted secret
  (spec §12.3).
- **Shepherd:** `ars_ws._notify_subscribers` POSTs unsigned; only *inbound* verify
  exists (`ars_clients.py`).
- **Impact:** subscriber callbacks can't authenticate our notifications.
- **Proposed approach:** add `sign_event(body, secret)` to `ars_clients.py` (reuse
  `decrypt_secret`); look up the subscriber's client secret and attach the header in
  `_notify_subscribers`.
- **Open questions:** subscribers are stored by `callback_url` (+ optional
  `client_id`); need the client's secret at notify time — join `ars_subscribers` →
  `ars_clients`.

### C3 — Auto-unsubscribe on completion 🔴 · priority: LOW · effort: S
- **ARS:** on terminal parent, calls `query_event_unsubscribe(None, pk)` to detach
  all clients (spec §10.5, §12).
- **Shepherd:** only explicit `/query_event_unsubscribe`; completion path never
  unsubscribes.
- **Impact:** subscriber rows linger until the retention janitor.
- **Proposed approach:** in `finish_query`, delete `ars_subscribers` for the parent
  (add `remove_all_subscribers(parent_qid)` to `db.py`).
- **Open questions:** none significant.

### C4 — Delivery concurrency + retries 🟡 · priority: LOW · effort: S
- **ARS:** one task per client, concurrent, 10s timeout, exponential backoff, max 8
  retries (spec §12.3).
- **Shepherd:** sequential, single-try, 30s timeout in `_notify_subscribers`.
- **Impact:** one slow/failed callback delays others; no delivery retries.
- **Proposed approach:** `asyncio.gather` the per-subscriber POSTs; add a small
  retry/backoff wrapper; drop timeout to ~10s.
- **Open questions:** retry budget; dead-letter handling for permanently-failing
  callbacks.

### C5 — Batch `POST /get_status` 🟡 · priority: LOW · effort: S
- **ARS:** `POST /get_status` returns per-PK `{pk, status, merged_list, stats}`
  (spec §12).
- **Shepherd:** only `GET /status/{pk}` → `{status, state, children}` (no
  `merged_list`, no `stats`).
- **Impact:** clients can't batch-poll; missing merged_list/stats fields.
- **Proposed approach:** add `POST /ars/get_status` (list of pks) returning the ARS
  shape; `stats` depends on B9, `merged_list` depends on A4 (or synthesize from
  `ars_children`).
- **Open questions:** what to put in `merged_list` without incremental
  merged-versions — the single final response id?

---

## D. Completion & timeout gaps

### D1 — Completion math + empty-query handling 🟡 · priority: MED · effort: M
- **ARS:** parent Done iff `finished and merge_count == orig_count` (every ARA with
  results has been merged), with `code==444` merged-errors still counting and other
  merged-errors decrementing; if **nobody** returns results, a **synthetic empty
  merged message** is created and the parent is Done (spec §10).
- **Shepherd:** Done iff `get_pending_ars_children == []` (all ARAs terminal). No
  orig/merge-count reconciliation, no 444 logic, no synthetic empty-merged step
  (the empty accumulating message just flows through the tail).
- **Difference & impact:** functionally reaches completion, but the semantics
  (which children count, empty-query artifact) differ; edge cases (partial merge
  failures) may complete differently.
- **Proposed approach:** if A4 is **not** adopted, our pending-set completion is a
  reasonable equivalent; add an explicit synthetic-empty handling only if the empty
  artifact matters. If A4 **is** adopted, port the count reconciliation.
- **Open questions:** does the empty-query output need to look like ARS's synthetic
  merged message? Do partial-failure completion semantics matter for your clients?

### D2 — Timeout tiers + query_type 🟡 · priority: MED · effort: S
- **ARS:** per-tier timeouts via `catch_timeout_async` beat — standard 5 min,
  pathfinder (own threshold), merged 8 min; **parent exempt**; straggler →
  `code=598`/`E` (spec §11). `query_type` (standard/pathfinder) derived at submit
  drives the threshold.
- **Shepherd:** single flat `ars_overall_timeout_sec = 360` on the **parent**;
  `examine_query`'s `pathfinder` flag is never used at the ARS layer; timed-out
  parents still finish `OK` (no distinct timeout status). Children → `ERROR`/504.
- **Difference & impact:** coarser timeout behavior; no pathfinder-specific budget;
  no `598` terminal code.
- **Proposed approach:** derive+store `query_type` at submit (from
  `query_graph.paths`); add per-tier timeouts (config: standard/pathfinder/merged);
  set a distinct timeout code on forced-fail. Reuse the existing watchdog
  (`workers/ars/worker.py`).
- **Open questions:** the concrete thresholds you want; whether to distinguish a
  timed-out completion from a clean one in the final callback.

---

## Suggested sequencing

1. **B10** (idempotency) and **B3** (score double-compute) — small, prevent
   corruption / dead code.
2. **B1** (Sugeno) + **B2** (dedup ordering) + **B6** (xref/same_as) — the
   output-correctness core; do together since they interact on scoring/dedup.
3. **B4/B5/B7** (scrub/decorate/phantom) + **B8** (validation) — response hygiene;
   decide per-response vs batch (depends on the B2 choice).
4. **C1–C3** (events, signing, auto-unsubscribe) + **D2** (timeout tiers) —
   lifecycle parity.
5. **B9/C4/C5/D1** — stats, delivery robustness, batch status, completion math.
6. **A3/A4/A5/A2** — structural, only if full fidelity is required.

Each card is meant to be picked up individually; the **Open questions** are the
inputs to resolve before implementing that card.
