# ARS Migration — Shepherd as the Autonomous Relay System

This document describes everything added to Shepherd to move the full **ARS**
(Autonomous Relay System, the NCATSTranslator/Relay Django app) role into
Shepherd. It is a reference for the new ARS surface: the runtime pipeline, every
new endpoint, worker, utility module, database object, and configuration setting,
with a per-function description.

---

## 1. Overview

Previously Shepherd was an *upstream ARA platform*: the external ARS received a
submitter's TRAPI query, fanned it out to ARAs, merged the responses, normalized
and annotated nodes, appraised answers, and notified the submitter.

Shepherd now **owns that entire role**. A submitter POSTs one query to a single
endpoint and Shepherd:

1. **Fans out** the query to each ARA (Aragorn, BTE, ARAX, SIPR) by reusing each
   ARA's existing internal Shepherd workflow (not external HTTP).
2. **Merges** every ARA response into one TRAPI message, **deduplicating answers**
   that are identical across ARAs.
3. **Normalizes** node curies, then **annotates** nodes with Biothings data.
4. **Appraises** answers (ordering components) and percentile-normalizes scores.
5. **Filters** blocked sources/nodes and trims results.
6. **Notifies** the submitter (callback) when the query is done.

Plus full ARS parity: a message-tree/trace API, an actor/agent/channel registry
with SmartAPI auto-discovery, subscribers + WebSocket status push, retain /
block / filter, a blocklist, signature verification, a timeout watchdog, and a
subscriber-client auth model.

### The reuse lever

Every query carries a `workflow` list (e.g. `[{"id":"node_norm"}, ...]`). When a
worker finishes, `shepherd_utils/shared.py::wrap_up_task()` pops the head op and
`add_task()`s the next op's Redis stream; when the list empties it routes to
`finish_query`. **New ARS stages are therefore just new workers + new operation
IDs appended to a workflow list.** The only genuinely new control-flow shape is
the *cross-ARA join* (fan out to N ARAs, wait for all), which is tracked in a new
`ars_children` table — the per-ARA analog of the existing `callbacks` completion
counter.

---

## 2. Runtime pipeline

```
POST /ars/query (or /asyncquery)
        │
        ▼
   [ars worker]  ── fans out one child query per ARA onto the existing
        │            aragorn / arax / bte / sipr streams (each runs its
        │            normal internal workflow unchanged)
        │
        ▼  each ARA's finish_query POSTs its result to
   POST /ars/callback/{id}
        │
        ▼
 [ars_accumulate worker]  ── folds each child response into the parent under a
        │                     Redis lock: merge_kgraph (KG) + result dedup
        │                     (ars_merge) + aux merge. Marks the ARA DONE.
        │
        │  when all ARAs are DONE, a single latch winner (claim_ars_tail)
        │  averages accumulated scores and launches the post-merge tail:
        ▼
 node_norm → node_annotate → answer_appraise → ars_blocklist →
 filter_results_top_n → finish_query  ──►  POST result to submitter callback
```

A **watchdog** loop inside the `ars` worker forces timed-out parents through the
tail with whatever partial results arrived, so the submitter is always notified.
Status transitions are published to a Redis channel and broadcast by the
`ars_ws` service to WebSocket clients and registered subscriber callbacks.

---

## 3. HTTP endpoints (the `/ars` sub-app)

All routes are on the `ARS` FastAPI sub-app (`shepherd_server/ars.py`), mounted at
`/ars` in `shepherd_server/server.py`. Mirrors the ARS `/ars/api/...` surface.

| Method & path | Purpose |
|---|---|
| `POST /ars/query` | Submit a query; block until the merged, appraised result is ready (sync). |
| `POST /ars/asyncquery` | Submit a query; the merged result is POSTed to the caller's `callback`. |
| `POST /ars/callback/{callback_id}` | Receive a per-ARA child response (internal). |
| `GET /ars/messages` | List recent parent ARS queries. |
| `GET /ars/messages/{pk}` | The accumulated/merged response for a parent query. |
| `GET /ars/messages/{pk}/trace` | Parent + per-ARA child rows (the message tree). |
| `GET /ars/status/{pk}` | Overall status (Running / Merging / Done / Error). |
| `GET /ars/filter/{pk}` | Filter results by `hop` / `score` / `node_type` / `spec_node`; returns a derived response id. |
| `POST /ars/block/{pk}` | Remove given node curies (and dependent edges); returns a derived response id. |
| `GET /ars/retain/{pk}` | Retain a query (and its children) from the purge janitor. |
| `GET /ars/latest_pk/{n}` | Per-day parent counts over n days + latest/running pks. |
| `GET /ars/report/{inforesid}` | Per-child 24h performance stats for an ARA. |
| `GET /ars/health` | Datastore + broker liveness (503 if down). |
| `GET /ars/actors` · `POST /ars/actors` | List / register actors (ARAs). |
| `POST /ars/actors/discover` | Refresh the actor registry from SmartAPI. |
| `GET /ars/agents` · `POST /ars/agents` · `GET /ars/agents/{name}` | Agent registry. |
| `GET /ars/channels` · `POST /ars/channels` | Channel registry. |
| `POST /ars/subscribe/{pk}` | Lightweight callback subscription (no signature). |
| `GET /ars/subscribers/{pk}` | List subscriber callbacks (internal). |
| `POST /ars/clients` | Provision a subscriber client (id + encrypted secret). |
| `POST /ars/query_event_subscribe` | Signature-verified subscribe to a set of pks. |
| `GET /ars/query_event_subscribe` | Signature-verified listing of a client's subscriptions. |
| `POST /ars/query_event_unsubscribe` | Signature-verified unsubscribe. |

In `shepherd_server/base_routes.py`:
- **`run_query`** — extended to accept the `"ars"` target and flag the query as an
  ARS parent (`set_query_ars_parent`); also registers the six new tail op IDs in
  the workflow allowlist.
- **`ars_callback(callback_id, request)`** — resolves a per-ARA response to its
  parent + ARA (`get_ars_child_by_callback`), verifies the callback signature when
  enabled, stashes the child response, and enqueues an `ars_accumulate` task.

---

## 4. Workers

Each worker is its own Docker service (`compose.yml`) and follows the standard
pattern: `poll_for_tasks()` reads its Redis stream, `process_task()` runs the
worker fn via `run_task_lifecycle` (which auto-chains the workflow) or marks the
task complete directly for non-chaining workers.

### `workers/ars` — fan-out orchestrator + watchdog (`STREAM="ars"`)
- **`ars(task, logger)`** — for each ARA in `settings.ars_aras`, create a child
  `query_id`/`response_id`, point the child's callback at `/ars/callback/{id}`,
  insert an `ars_children` row, and dispatch onto the existing ARA stream with a
  null (ARA-defined) workflow. Does not auto-chain.
- **`run_watchdog_once(logger)`** — find parents past `ars_overall_timeout_sec`
  with ARAs still pending, mark those ARAs ERROR, and (winning the same single
  launch latch) force the post-merge tail on partial results.
- **`watchdog_loop()`** — periodic sweep calling `run_watchdog_once` every
  `ars_watchdog_interval_sec`.
- **`main()`** — runs the poll loop and the watchdog concurrently.

### `workers/ars_accumulate` — cross-ARA merge + completion gate (`STREAM="ars_accumulate"`)
- **`merge_child_into_parent(parent_msg, child_msg, source, logger)`** — fold one
  child response into the parent: `merge_kgraph` for the KG, `merge_result_maps`
  for result dedup, `merge_aux_graphs` for aux graphs. Returns `(parent_msg,
  child_result_count)`; pure/CPU-bound so it runs via `asyncio.to_thread`.
- **`ars_accumulate(task, logger)`** — acquire the parent lock, merge the child,
  mark the ARA DONE, publish a status event; when no ARAs remain pending, the
  single `claim_ars_tail` winner averages scores (`average_result_scores`) and
  enqueues the tail (`ARS_TAIL_WORKFLOW`).

### `workers/node_norm` — node normalization (`STREAM="node_norm"`)
- **`get_normalized_nodes(curies, logger)`** — POST batches of curies to
  `settings.node_norm` (`/get_normalized_nodes`); returns a `curie → {id, label,
  categories}` canonical map (empty on failure → pass-through).
- **`canonize_message(message, cmap, logger)`** — rewrite node ids to canonical
  form, merge nodes that collapse to the same id (categories union + attribute
  dedup via `combine_unique_dicts`), and rewrite edge subject/object + result
  node bindings.
- **`node_norm(task, logger)`** — load the response, normalize, save (or pass
  through unchanged on a normalizer failure).

### `workers/node_annotate` — node annotation (`STREAM="node_annotate"`)
- **`get_annotations(curies, logger)`** — POST curies to `settings.annotator_url`;
  returns a `curie → annotation` map (empty on failure).
- **`annotate_message(message, annotations)`** — attach each annotation to its
  node as a `biothings_annotations` attribute; returns the count annotated.
- **`node_annotate(task, logger)`** — filter to valid CURIEs, annotate, save (or
  pass through on failure).

### `workers/answer_appraise` — answer appraiser (`STREAM="answer_appraise"`)
- **`appraise(message, logger)`** — zstd-POST the message to
  `settings.appraiser_url`, merge returned `ordering_components` onto each result;
  on failure set default components (`novelty/confidence/clinical_evidence = 0`).
- **`normalize_scores(results)`** — percentile-rank result scores onto a 0–100
  `normalized_score` via `scipy.stats.rankdata`.
- **`apply_defaults(results)`** / **`_result_score(result)`** — helpers for the
  default-on-failure path and per-result score extraction.
- **`answer_appraise(task, logger)`** — appraise + normalize the merged message.

### `workers/ars_blocklist` — blocklist filtering (`STREAM="ars_blocklist"`)
- **`load_blocklist(path, logger)`** — load `(blocked_sources, blocked_nodes)` from
  `config/blocklist.json` (list form or `{knowledge_sources, nodes}` form).
- **`apply_blocklist(message, blocked_sources, blocked_nodes, logger)`** — drop
  edges from a blocked source or touching a blocked node, plus blocked nodes;
  returns the number removed. **`_edge_is_blocked(...)`** is the per-edge test.
- **`ars_blocklist(task, logger)`** — apply the blocklist then
  `filter_kgraph_orphans`.

### `workers/ars_ws` — subscriber / WebSocket fan-out (FastAPI, port 5441)
- **`_broadcast(event)`** — send an event to every connected WebSocket; drop dead
  sockets.
- **`_notify_subscribers(event)`** — POST the event to each callback registered
  for the event's parent query.
- **`_event_loop()`** — consume `ars_events` from Redis and fan out (broadcast +
  notify).
- **`ws_endpoint(websocket)`** / **`health()`** / **`lifespan(app)`** — the
  `/ws` socket handler, `/api/health`, and the app lifespan that starts the event
  loop.

---

## 5. New `shepherd_utils` modules

### `shepherd_utils/ars_merge.py` — cross-ARA result dedup (Relay `mergeDicts` parity)
- **`result_key(result)`** — the dedup key: `frozenset` of the first node-binding
  id per qnode (Relay's `getResultMap` key).
- **`merge_two_results(merged, current, logger)`** — merge two results that bind
  the same answer: concatenate `analyses`, accumulate differing scalar score
  fields into a list (associative across incremental callbacks), `score`
  collisions rename to `scores`.
- **`merge_result_maps(parent_results, child_results, logger)`** — dedup parent +
  child results by `result_key`, merging collisions; returns the deduped list.
- **`average_result_scores(results)`** — replace any accumulated
  `normalized_score` list with its arithmetic mean (Relay's finalize step).
- **`merge_aux_graphs(parent_aux, child_aux)`** — id-keyed auxiliary-graph merge
  with edge union.

### `shepherd_utils/ars_filter.py` — result filters (Relay parity, verbatim)
- **`hop_level_filter(results, hop_limit)`** — keep results with fewer than
  `hop_limit` bound qnodes.
- **`score_filter(results, score_range)`** — keep results whose `normalized_score`
  is strictly between `range[0]` and `range[1]`.
- **`node_type_filter(kg_nodes, results, forbidden_category)`** — drop results
  binding a node whose category (biolink prefix stripped) is forbidden.
- **`specific_node_filter(results, forbidden_node)`** — drop results binding any
  forbidden curie.
- **`apply_filters(message, filters, logger)`** — apply an ordered list of
  `(filter_type, value)` to a message in place; returns the final result count.

### `shepherd_utils/ars_clients.py` — subscriber-client signature verification
- **`canonize_url(url_str)`** — canonical signature string for a GET url
  (`scheme|netloc|path|sorted_query`).
- **`decrypt_secret(encrypted_secret, master_key)`** — AES-CBC (IV-prefixed,
  PKCS7-padded) base64 secret decryption (lazy pycryptodome import).
- **`verify_post_signature(signature, body, client_id, logger)`** — HMAC-SHA256 of
  the raw POST body under the decrypted client secret.
- **`verify_get_signature(signature, url, client_id, logger)`** — HMAC over the
  canonicalized url; returns `{verified, pks, client_id}`.
- **`_master_key()`** / **`_client_secret(client_id, logger)`** — internal helpers
  to decode the configured master key and fetch+decrypt a client's secret.

### `shepherd_utils/ars_notify.py` — ARS status pub/sub
- **`publish_ars_event(event, logger)`** — publish a status event to the Redis
  `ars_events` channel (best-effort; never raises into the caller).
- **`subscribe_ars_events()`** — async generator yielding status events as they
  are published.

### `shepherd_utils/ars_workflow.py`
- **`ARS_TAIL_WORKFLOW`** — the shared post-merge workflow constant
  `[node_norm, node_annotate, answer_appraise, ars_blocklist,
  filter_results_top_n]` (`finish_query` is auto-appended).

### `shepherd_utils/signature.py` — inbound ARS callback signatures
- **`compute_signature(body, secret)`** — hex HMAC-SHA256 of a body under a secret.
- **`verify_signature(signature, body, secret)`** — constant-time signature check.
- **`is_signature_valid(headers, body)`** — verify a request using configured
  settings; returns True when verification is disabled (single gate for callers).

### `shepherd_utils/smartapi.py` — actor auto-discovery
- **`parse_smartapi_hits(hits)`** — turn SmartAPI registry hits into a flat list of
  actor dicts (one per infores × server maturity).
- **`fetch_smartapi_hits(logger)`** — fetch raw hits from
  `settings.smartapi_registry_url` (empty on failure).
- **`refresh_actors(logger)`** — fetch + upsert actors (and their agents/channels)
  from SmartAPI; returns the number upserted.

---

## 6. New database helpers (`shepherd_utils/db.py`)

All follow the existing retry-loop style (exponential backoff on
`OperationalError`, explicit disk-full handling).

**Cross-ARA orchestration / completion**
- **`set_query_ars_parent(qid, logger)`** — flag a query as a top-level ARS parent.
- **`add_ars_children(parent_qid, children, logger)`** — insert the per-ARA child
  rows (QUEUED) with their callback ids + otel carriers.
- **`set_ars_child_status(parent_qid, ara, status, logger, ...)`** — update one ARA
  child row (stamps `stop_time` on terminal states).
- **`get_pending_ars_children(parent_qid, logger)`** — ARAs not yet terminal; empty
  list = the cross-ARA completion gate.
- **`get_ars_children(parent_qid, logger)`** — all child rows (for the trace API).
- **`get_ars_child_by_callback(callback_id, logger)`** — resolve a callback id to
  its parent + ARA + child response id + otel carrier.
- **`claim_ars_tail(parent_qid, logger)`** — atomically claim the right to launch
  the post-merge tail (single winner via `UPDATE ... RETURNING`).
- **`list_ars_parents(logger, limit)`** — list parent queries for `/ars/messages`.

**Retain**
- **`set_query_retained(qid, logger)`** — Relay `retain_all` parity: resolve a
  child pk to its parent, refuse while running, set `retain=TRUE` on the parent +
  all child rows; returns a Relay-shaped result dict. (`purge_old_queries` now
  guards both deletes with `AND retain = FALSE`.)

**Actor / agent / channel registry**
- **`upsert_actor(...)`** / **`list_actors(logger)`** / **`list_actor_field(field,
  logger)`** — register and list actors (and distinct agent/channel values).
- **`upsert_agent(name, logger, ...)`** / **`list_agents(logger)`** /
  **`get_agent(name, logger)`** — agent registry.
- **`upsert_channel(name, logger, ...)`** / **`list_channels(logger)`** — channel
  registry.

**Subscriber clients / subscriptions**
- **`get_client(client_id, logger)`** / **`upsert_client(...)`** — the subscriber
  Client row (encrypted secret, callback, subscriptions, active).
- **`set_client_subscriptions(client_id, subscriptions, logger)`** — replace a
  client's subscribed-pk list.
- **`add_subscriber(parent_qid, callback_url, logger, client_id=None)`** /
  **`remove_subscriber(parent_qid, client_id, logger)`** /
  **`list_subscribers(parent_qid, logger)`** — manage per-query subscriptions.

**Reporting / health**
- **`get_latest_pks(n, logger)`** — per-day parent counts over n days, latest n
  pks, and 24h running pks (Relay `latest_pk`).
- **`get_ars_report(inforesid, logger)`** — per-child 24h stats for an ARA (Relay
  `get_report`).
- **`ping_db(logger)`** / **`ping_redis(logger)`** — liveness probes for `/health`.

**Watchdog**
- **`get_timed_out_ars_parents(timeout_sec, logger)`** — parents past budget with
  pending ARAs.
- **`mark_ars_children_errored(parent_qid, aras, logger)`** — mark pending child
  ARAs ERROR on timeout.

---

## 7. Database schema (`shepherd_db/init_db.sql`)

All additions are **append-only** (the `shepherd_brain` row is read positionally
elsewhere, so columns are only ever added at the end).

| Object | Purpose |
|---|---|
| `shepherd_brain.is_ars_parent` (col) | Marks a top-level ARS parent query. |
| `shepherd_brain.ars_tail_launched` (col) | Single-winner latch for the post-merge tail. |
| `shepherd_brain.retain` (col) | Excludes the query (and children) from purge. |
| `ars_children` (table) | One row per parent × ARA: status, callback id, otel, result count — the cross-ARA completion counter. `ON DELETE CASCADE` from the parent. |
| `ars_actors` (table) | Actor registry (infores, url, channel, agent, maturity, active). |
| `ars_agents` (table) | Agent registry (name, description, uri, contact). |
| `ars_channels` (table) | Channel registry (name, description). |
| `ars_subscribers` (table) | Per-query subscriber callbacks (+ optional client_id). |
| `ars_clients` (table) | Subscriber clients: AES-encrypted secret, callback, subscriptions, active. |

---

## 8. Configuration (`shepherd_utils/config.py`)

New `Settings` fields:

| Setting | Default | Purpose |
|---|---|---|
| `ars_aras` | `["aragorn","arax","bte","sipr"]` | ARAs to fan out to. |
| `annotator_url` | `https://biothings.ncats.io/curie` | Biothings annotator. |
| `appraiser_url` | `https://answerappraiser.ci.transltr.io/get_appraisal` | Answer appraiser. |
| `ars_overall_timeout_sec` | `360` | Watchdog timeout for a parent query. |
| `ars_blocklist_path` | `/app/config/blocklist.json` | Blocklist asset path. |
| `ars_signature_verify` | `False` | Verify inbound callback signatures. |
| `ars_signature_secret` | `""` | Shared secret for callback HMAC. |
| `aes_master_key` | `""` | Base64 AES key to decrypt client secrets. |
| `ars_subscriber_host` | `http://shepherd_ars_ws:5441` | WS service location. |
| `ars_ws_port` | `5441` | WS service port. |
| `ars_events_channel` | `ars_events` | Redis pub/sub channel for status events. |
| `ars_watchdog_interval_sec` | `30.0` | Watchdog sweep cadence. |
| `smartapi_registry_url` | SmartAPI ARA query | Actor discovery source. |

(`node_norm` already existed and is now used by `node_norm`.) A new
`pycryptodome` dependency (`pyproject.toml`) backs the client-secret decryption.

---

## 9. Tests

New unit tests under `tests/unit/` (full suite green — 479 passed):

| File | Covers |
|---|---|
| `test_db_ars.py` | ARS children helpers + claim/latch + retain SQL. |
| `test_db_ars_registry.py` | actor/agent/channel/client helpers, latest_pk, watchdog helpers. |
| `test_ars_orchestrator.py` | fan-out: child queries + callbacks + dispatch. |
| `test_ars_accumulate.py` | merge + partial/full gating + single-winner tail launch. |
| `test_ars_merge.py` | result dedup, analyses concat, score averaging, aux merge. |
| `test_node_norm.py` / `test_node_annotate.py` | canonicalize / annotate + pass-through on failure. |
| `test_answer_appraise.py` | appraise + default-on-failure + percentile ranking. |
| `test_ars_blocklist.py` | source/node blocking + blocklist loading. |
| `test_ars_filter.py` | hop/score/node_type/spec_node + chaining. |
| `test_ars_clients.py` | canonize_url, AES decrypt roundtrip, POST/GET HMAC verify. |
| `test_signature.py` | callback HMAC verify. |
| `test_smartapi.py` | registry parsing + actor refresh. |
| `test_ars_watchdog.py` | timeout → error children + forced tail. |
| `test_retain.py` | retain terminal/running/child-pk + purge guard. |
| `test_ars_messages_api.py` | FastAPI TestClient over every `/ars` read/registry/filter/subscribe endpoint. |

Run with `tox` (Python 3.12) or `pytest -q`.

---

## 10. Commit history (branch `claude/ars-shepherd-migration-plan-bhmbn1`)

1. **Phase 0** — foundation: config, schema, ARS db helpers.
2. **Phase 1** — fan-out orchestrator + `/ars` endpoints (`ars`, `ars_accumulate`).
3. **Phases 2–4** — node norm/annotate, appraiser, blocklist + curation endpoints.
4. **Phases 5–6** — actor registry/SmartAPI, subscribers/WebSocket, signatures,
   timeout watchdog.
5. **Phase 7** — exact parity: cross-ARA result dedup + retain.
6. **Gap pass** — full filter semantics, latest_pk/report/health, agents/channels
   registry, subscriber-client auth + signed subscribe/unsubscribe.

---

## 11. Known caveats / follow-ups

- **Live service contracts** — the node-normalizer, annotator, appraiser, and
  SmartAPI request/response shapes are coded to their documented APIs but have not
  been exercised against the running services. Validate end-to-end via
  `docker compose up --build` + `scripts/test_shepherd.py` (an `ars-local` target
  at `http://localhost:5439/ars`).
- **Intentionally not ported** (non-functional in the ARS): `timeoutTest`
  (placeholder), `post_process` (debug), `filters` (help text), `index` /
  `api_redirect` (Shepherd serves OpenAPI at `/ars/docs`). `merge` is automatic
  (`ars_accumulate`) rather than a manual trigger, by design.
- **Cross-ARA KG merge** uses Shepherd's optimized `merge_kgraph` (id-keyed with
  attribute union) rather than a verbatim port of Relay's recursive `mergeDicts`
  on the KG — functionally equivalent for node/edge dedup.
