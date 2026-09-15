# ARS Parity Register

The behavior contract for Shepherd's hosted port of the Translator ARS
([NCATSTranslator/Relay](https://github.com/NCATSTranslator/Relay)).

**Pinned upstream commit:** `3e65975db287a73afa4388b7dbaf3c64d0d218c4`
(master, re-pinned 2026-09-05 from the original 2026-09-01 pin at
`dd1e71b8284de746f9d11e4fc823bf57861e081f`; byte-exact reference copies
under `/home/user/ncatstranslator/relay` during development, key files
mirrored in the golden-generation tooling).

Behavioral changes accepted with the 2026-09-05 re-pin (everything else in
the range was OpenTelemetry/gunicorn/celery tuning):

- **Relay PR #884 (removeAppraiser)**: `post_process` no longer calls the
  external Appraiser or the Sugeno `compute_from_results` pass. When the
  merged message has results, `appraise_confidence` computes
  `ordering_components` locally (`confidence = 1 - prod(1 - score)` over
  each result's scored analyses; novelty/clinical_evidence 0.0), and its
  failures are logged and swallowed. `result_count`/`ScoreStatCalc` moved
  inside the results-non-empty guard, keeping the E/444 early return. The
  final-save block always runs (202 flips to D/200; save failure E/422).
- **Relay PR #883 (key-error fix)**: `mergeDicts` keys qualifier dicts by
  `qualifier_type_id`, so qualifier lists actually merge instead of the
  old swallowed-KeyError no-op.
- **Relay PR #882**: `Symptom` (NCIT:C4876) added to the blocklist.

**Golden regeneration:** goldens are produced by *running the upstream code*
over the corpus, never hand-written:

```console
$ python3.11 -m venv .venv-relay && .venv-relay/bin/pip install \
    django==4.2.23 celery==5.5.3 scipy==1.10.1 sympy==1.13.3 "numpy<2" \
    zstandard==0.23.0 objsize requests pyyaml pycryptodome pymysql \
    reasoner-pydantic==5.1.1 pydantic==1.10.22 opentelemetry-api \
    opentelemetry-sdk opentelemetry-instrumentation-celery \
    opentelemetry-instrumentation-django opentelemetry-instrumentation-httpx \
    opentelemetry-instrumentation-requests \
    opentelemetry-exporter-otlp-proto-grpc "redis>=5,<6"
$ python scripts/ars_parity/build_corpus.py
$ PYTHONHASHSEED=0 .venv-relay/bin/python scripts/ars_parity/generate_goldens.py \
    --relay /path/to/relay-checkout
```

Re-pinning to a newer Relay commit means: update the SHA here and in
`generate_goldens.py`, regenerate, review the golden diff, and consciously
accept each behavioral change.

## Test layers

| Layer | What it pins | Where |
|---|---|---|
| 1. Golden function parity | merge/premerge/filters/scoring/blocklist/validation outputs, byte-compared to upstream runs | `tests/unit/ars/test_golden_parity.py` |
| 2. Lifecycle & state machine | status letters/coercion, completion arithmetic, orchestration, worker state machines | `test_statuses.py`, `test_completion.py`, `test_ars_lifecycle.py`, `test_ars_fanout.py`, `test_ars_merge_worker.py`, `test_ars_postprocess.py`, `test_ars_watchdog.py`, `test_ars_notify.py` |
| 3. API contract | paths, methods, status codes, error bodies, envelope shapes | `test_envelope.py`, `test_ars_api_contract.py` |
| 3b. Deliberate divergences | the upstream bugs the port does NOT reproduce | `test_upstream_bugfixes.py` |
| 4. Differential end-to-end | both stacks against the same mocked world | `tests/parity_e2e/` (run on demand; see its README) |

## Invariant index (register rows referenced from tests)

- **P-ST-1..4** — the six status letters, terminal set `{D,S,E,U}`,
  long-name mapping, save-time code coercion (R→202, D→200) with the
  `_skip_post_save` escape hatch.
- **P-ENV-1..6** — Django-serializer envelopes: DjangoJSONEncoder datetime
  format, exact model field order, long-form statuses, inline-decompressed
  `data`, FK/pk shapes.
- **P-LC-1..6** — parent-completion counting verbatim from
  `message_post_save`: `finished` over the terminal set;
  `orig_count`/`merge_count` from result-bearing `ar*` agents; the
  `'E'/444` merge child satisfying its origin while any other merge error
  decrements; the empty-completion branch synthesizing an empty merged
  message (and, faithfully, *not* clearing subscriptions there).
- **P-NT-1..5** — `notify_subscribers` field overrides ('D'→admin,
  'E'→ars_error), stats attachment, the `{pk, timestamp, code}` payload
  base, `last_merged_completed` forcing code 200, per-client HMAC-SHA256
  over compact sorted-key JSON. Recipients are resolved when the event is
  emitted and carried in the `ars.notify` task (`client_pks`): the
  completion path clears the parent's subscriptions immediately after its
  final events, so a worker resolving them later would deliver to nobody.
- **Callback guard order** — dup-Done → 200 text; repeated results → 409;
  errored child → 400; decode failure → 500 `Can not decode json...`;
  validation failure → 422 `Problem with TRAPI Validation` with the child
  E/422; header `tr_ars.message.status` override; `results: null` → 
  `result_count = 0` while `results: []` leaves it None.
- **Timeouts** — parents exempt; merge children 8 min; everything else
  **5 min including pathfinder** (upstream's code, not its log message);
  code 598. Upstream's 15-minute ceiling on *creation* time is **not**
  reproduced (see the divergences below); the sweep is bounded by
  `ars_timeout_scan_limit` rows per pass instead.
- **Known-broken endpoints reproduced** — `POST /ars/api/messages` (500),
  `POST /ars/api/actors` (creates the actor, then 400
  `Not a valid json format`), `GET /ars/api/merge/<pk>` (creates the shell
  merge child, then 500), `timeoutTest`/`post_process` debug (500).
  `GET /ars/api/block/<pk>` is **not served at all** — see the divergences
  below.
- **Upstream error-behavior parity** — a failed merge fold leaves the shell
  merge child Running for the watchdog. The crash-and-drop behaviors this
  row used to list (`decorate_edges_with_infores`'s UnboundLocalError,
  `normalizeScores`'s IndexError, the `node_bindings` for/else, the
  `attributes`/`analyses` early returns) are **no longer reproduced** — see
  "Deliberate divergences from upstream" below.

## Documented deviations (all consciously accepted)

Infrastructure substitutions (behavior-preserving by definition):

| Upstream | Port |
|---|---|
| Celery on RabbitMQ (+beat) | Redis Streams workers (`ars.fanout`, `ars.premerge`, `ars.merge`, `ars.postprocess`, `ars.notify`) + the `ars_watchdog` loop |
| MySQL rows with inline zstd blobs | Postgres `ars_*` rows; blobs in Redis (hot) + `ars_message.data` bytea (durable, written at terminal status) |
| `merge_semaphore` + `select_for_update` + celery retry | broker lock per parent (semaphore column still maintained for envelope parity) |
| `expensive_gate` 12-token redis ZSET | per-worker `TASK_LIMIT` / pool sizing |
| self-proxy views `/ara-*/api/runquery` | direct POST to the SmartAPI-resolved remote (same body; proxy endpoints not served) |

Behavioral deviations:

1. **reasoner-pydantic replaced with Shepherd pydantic-v2 models**
   (`shepherd_utils/ars/trapi.py`), per project direction. Field
   requirements were dumped from the installed upstream package and verdict
   parity is golden-tested over valid + broken corpora.
2. **Annotator**: the in-process `biothings_annotator` package, as
   upstream, with two deltas. (a) *Version pinning*: Relay installs the
   package as an unpinned git dependency off master, so its annotation
   logic shifts per image build; the port pins commit `82d3acc` in
   `workers/ars_postprocess/requirements.txt` and `test-requirements.txt`
   -- bump deliberately when re-pinning. (b) *Invocation*: the async
   worker awaits `annotate_curie_list` directly. Upstream's event-loop
   dance has two branches: celery's sync workers always take
   `run_until_complete` (to which the direct await is equivalent), while
   the `loop.is_running()` branch would hand back a Future and crash the
   consumption loop -- a branch a sync celery worker never takes, not
   reproduced. The consumption loop itself is verbatim (notfound-list
   skip, empty-dict skip, direct node indexing, quirky crash modes ->
   E/444).
3. **Watchdog intent fixes**: upstream indexes the Agent table with the
   *actor's* pk (outcome depends on row-id coincidence) — the port joins
   actor→agent properly; and `ars-workflow-agent` parents are exempted
   alongside `ars-default-agent` (upstream would 598 workflow parents at 5
   minutes).
4. **Watchdog cadence**: 60s sweep vs. 3-minute beat. A timed-out message is
   marked *sooner after* its threshold; the thresholds themselves are
   identical.
5. **Callback micro-paths**: the async-200 self-GET race probe is dropped
   (upstream persists nothing on that path); the dead branch that upstream
   hits when a child already has stored `data` (a `str in bytes` TypeError
   → 500) is not reproduced.
6. **merge failure retries**: upstream celery-retries `merge_and_post_process`
   up to 20× (creating a fresh merge child per attempt); the port fails once
   and leaves the shell merge child for the watchdog — the terminal outcome
   (E/598 merge child decrementing `orig_count`) is the same shape.
7. **notify stats on a custom-fields-less call**: upstream raises TypeError
   when `result_count` is set and no fields dict exists; the port carries
   the stats in a fresh dict.
8. **Empty-completion robustness**: when the parent's payload is missing
   (Redis TTL + no durable copy), the port synthesizes the empty merged
   message from `{"message": {}}` instead of upstream's KeyError.
9. **404 body for non-UUID pks**: Django returns its HTML 404 page (URL
   resolution fails); the port returns the endpoint's own
   `Unknown message: <pk>` text. Status code identical.
10. **Retention**: upstream never purges (out-of-band cleanup honors
    `retain`); the port nulls durable payload copies after
    `ars_data_retention_days` for non-retained terminal messages, keeping
    row metadata. Trees that back a live response-cache entry (item 13)
    are exempt while their cache generation is current.
11. **`GET /ars/api/messages` payload inclusion** and other list endpoints
    load payloads from the blob store; a payload evicted from Redis with no
    durable copy renders `fields.data: null` (upstream MySQL always had it
    inline).
12. **Notification delivery retries** run in-process with upstream's backoff
    envelope (cap 300s, jitter, 8 attempts) instead of celery re-delivery,
    detached from the stream task that emitted them and bounded by
    `ars_notify_max_inflight` / `ars_notify_max_delivery_sec`. Retrying
    inline let one unreachable client callback pin a `TASK_LIMIT` slot for
    the whole ladder and stall every other query's notifications.
13. **Response cache** (Shepherd-native; upstream has none —
    `shepherd_utils/ars/cache.py`, design in
    `docs/ARS_RESPONSE_CACHE_PLAN.md`). With `ars_cache_enabled` there is
    one message tree per distinct query per cache generation. A submit
    whose structurally canonical query graph (node/edge/path ids treated
    as labels, key order and null/missing/empty ignored, lists as sets)
    plus non-empty `workflow` matches a completed prior submit is answered
    with **that tree's pk**: the `201` envelope is the source parent's
    (already `Done/200`, `merged_version` set) with the caller's own
    submit body as `data`, like any fresh parent; the merged response is
    fetched via `merged_version` as usual and keeps the source's
    node/edge/path labels and, once the tree is the cached answer, carries
    a stored `logs` line naming the cache source, key and generation. A
    submit matching an in-flight query is handed the leader's pk while it
    is still Running. No rows or payloads are written on a hit. Message
    GETs splice the stored payload bytes into the envelope without parsing
    them (the recent-messages list likewise). Because a hit's pk is already
    finished when the client subscribes to it, `query_event_subscribe` on a
    terminal pk **replays that message's completion notifications to the
    subscribing client** (`last_merged_completed` then `admin/complete`, or
    `ars_error`) and reports success, where upstream refused with "Query
    already complete"; with the cache disabled the refusal is preserved.
    Opt out per query with TRAPI `bypass_cache` (no read, no write);
    refresh one entry with `parameters.overwrite_cache` (no read, forced
    write); flush all by bumping the cache generation
    (`scripts/ars_cache.py invalidate` or the token-gated
    `POST /ars/api/cache/invalidate`). Source parents record their role in
    `params.cache` (`leader` / `overwrite` / `bypass` / `uncached`). Never
    cached: an empty merged result where an ARA child errored. Shared-pk
    consequences (retain/block act for all readers; timestamps and name are
    the first submitter's) are accepted. Tests:
    `tests/unit/ars/test_cache_key.py`, `test_cache_flow.py`, and the cache
    section of `test_ars_api_contract.py`.
13. **normalized_score is a plain float**: upstream stores rankdata's
    numpy.float64 through stdlib json (which accepts it as a float
    subclass); Shepherd's orjson blob codec rejects numpy scalars, so the
    port casts via ``.tolist()`` at the production site. Identical numeric
    values; regression-tested against the blob codec round-trip.
14. **Pre-merge processing is asynchronous** (post-parity change, accepted
    2026-09-08 after load testing): upstream runs pre_merge_process +
    phantom removal + TRAPI validation inline in its callback view; the
    port runs them in the `ars_premerge` worker because the inline CPU work
    saturated the server at 40 concurrent queries. Consequences for the
    callback response: a result-bearing POST always answers 201 with the
    child still Running (result_count/result_stat are set synchronously so
    the repeated-results 409 guard still holds); upstream's inline HTTP 422
    on validation failure becomes
    an async child E/422 with the same ara_failed_validation notification;
    upstream's inline-crash HTTP 500 becomes an async child E/500 with the
    same "Internal ARS Server Error" log entry. Terminal child states,
    notifications, merge inputs, and completion arithmetic are unchanged
    (`tests/unit/ars/test_ars_premerge.py`); a child stuck in premerge is
    covered by the watchdog's standard 5-minute 598.
15. **Callback responses do not echo the payload** (post-parity change,
    accepted 2026-09-08 with the same load testing): upstream's callback
    view answers with the full stored message inline in `fields.data`; the
    port answers both callback branches (result-bearing and no-results)
    with `fields.data: null`. Serializing the multi-MB payload back at the
    ARA -- which never reads the response body -- was the largest
    per-callback CPU cost on the server's event loop. Envelope shape,
    status codes, and every other field are unchanged; `GET` on the message
    still returns the payload. Alongside this, the remaining synchronous
    CPU on the callback path (request-body `json.loads`, `ScoreStatCalc`,
    the zstd blob encode in `save_message`) moved to threads, and the
    server runs 4 uvicorn worker processes (`WEB_CONCURRENCY` in
    `shepherd_server/Dockerfile`) vs. upstream's 8 gunicorn workers x 4
    threads.
16. **Shepherd-hosted ARAs are dispatched internally** (post-parity change,
    accepted 2026-09-10): for actors in
    `shepherd_utils/ars/internal.INTERNAL_ARA_TARGETS` (infores:shepherd-*),
    ars_fanout enqueues the ARA's worker task directly -- persisting the
    same query record `POST /{ara}/asyncquery` would have -- with a
    `shepherd-ars://callback/<child_pk>` sentinel callback, and
    finish_query recognizes the sentinel and enqueues
    `{intake_child_pk, response_id}` on `ars.premerge` instead of POSTing
    the response to `/ars/api/messages/<child_pk>`. The premerge worker's
    `intake_internal_response` then runs the callback endpoint's exact
    state machine (guard order, result_count/result_stat, the
    `ara_response_complete` notification, the no-results terminal rules,
    the generic-failure E/500 with its log entry) before premerging in the
    same task, so no multi-MB body crosses the network in either
    direction. Differences in kind: HTTP-level answers nobody read (the
    dup-200 text, the 409, the 400) become logged skips; a dispatch
    failure is the same child E/500 as a failed POST; an intake whose
    response blob is missing leaves the child Running for the watchdog
    (the shape of a callback that never arrived); and a `get_logs` failure
    delivers the response without spliced logs instead of failing
    delivery. External actors, external callers, and both public endpoint
    surfaces are unchanged; `settings.ars_internal_dispatch=false`
    restores HTTP dispatch for everything (already-issued sentinels still
    deliver internally, since they are not POSTable).

## Deliberate divergences from upstream (upstream bugs NOT reproduced)

Everything above is parity work. This section is the opposite: places where
the port used to reproduce an upstream defect faithfully and no longer does.
Each one is pinned by `tests/unit/ars/test_upstream_bugfixes.py`, and the
four that change a golden are declared in the `_divergences` block of
`tests/fixtures/ars_goldens/goldens.json` (a regeneration re-records them
from upstream and will fail the suite until they are re-applied — that
failure is the prompt to re-decide each one, not a bug).

### Crashes on ordinary input

| Upstream | Port |
|---|---|
| `decorate_edges_with_infores` read `has_primary`, only ever assigned inside its loop → `UnboundLocalError` on any non-empty `sources` with no `primary_knowledge_source`, failing the whole callback | `has_primary` is initialized; the agent adds itself as primary, which is what the unreachable branch intended |
| `decorate_edges_with_infores` shared ONE `self_source` dict across the whole graph, so the last edge to need a role rewrote the role of every earlier edge | a fresh source dict per edge (`_self_source`) |
| `normalizeScores` ranked only score-bearing results but popped one rank per RESULT → `IndexError` on any mixed response, after misassigning the ranks it did hand out | each rank goes to the result it was computed from; unscored results get no `normalized_score` |
| `scrub_null_attributes` iterated `get_safe(edge, "sources")` directly → `TypeError` on an edge with no `sources` key | treated as empty |
| `remove_blocked` bound `nodes_to_remove` / `edges_to_remove` / `aux_graphs_to_remove` / `results_to_remove` inside conditional branches and read them unconditionally → `UnboundLocalError` on any response without `auxiliary_graphs` (an ordinary shape), without a knowledge graph, or with a null `edges` map | every accumulator bound up front |
| `remove_blocked`'s pathfinder branch raised `UnboundLocalError` on an empty `path_bindings` | the removal loop is inside the per-path loop, so there is nothing to leave unbound |
| `QueryGraph` / `KnowledgeGraph` / `Results` returned early on a `None` input, leaving every attribute unset → `AttributeError` from the next getter | they initialize empty |

### Silently wrong results

| Upstream | Port |
|---|---|
| `mergeDicts` `return`ed out of its `attributes` and `analyses` branches, abandoning every key it had not reached yet (e.g. an edge's `qualifiers`) | `continue`; the remaining keys merge |
| `mergeDicts`' `node_bindings` branch hung its `else` off the `for` rather than the `if`, so only the LAST current-only binding was carried — into a local map it never wrote back — and bindings past index 0 were ignored entirely | bindings are unioned per query-graph node, merging matching ids and appending new ones |
| `TranslatorMessage.to_dict` emitted `"results": {}` for a message with no results | `[]`, as TRAPI requires |
| `remove_blocked` pruned pathfinder `path_bindings` for only the last path id (its removal loop was dedented out of the per-path loop and shadowed the dict with its own values) | every path id is pruned |
| `remove_blocked` matched analysis-level `support_graphs` (aux graph ids) against removed EDGE ids — never a match — so support graphs that really had gone away stayed on the analysis | matched against removed aux graph ids |
| The notification `stats` block keyed off the parent's `result_count`, which nothing ever set, and counted aux graphs from a `data` argument no caller passed — so stats were never attached, and would have read 0 if they had been | `ars_postprocess` carries the merge's result count up to the parent; the aux count comes from the `params.stats` the merge already recorded |

### Stuck or unbounded state

| Upstream | Port |
|---|---|
| The `tr_ars.message.status` request header was written verbatim into the `CHAR(1)` status column. A one-character nonsense value is not terminal (so the parent never completes) and is not `'R'` (so the watchdog never scans it) — the message was stranded for good, from an unauthenticated callback | `coerce_status` clamps at every ingress (callback header, fanout response header, task payload) and `validate_letter` rejects at the db layer |
| The timeout sweep examined only messages created in the last 15 minutes, so a message that outlived the window could never be timed out again: a sweep outage longer than (window − threshold) stranded every message it missed, and their parents with them | no creation-time ceiling by default (`ars_timeout_scan_window_sec = 0`); bounded by `ars_timeout_scan_limit` rows per pass, oldest first |
| `check_parent_completion` guarded the completion branch with a read-then-act status check. It runs from the server and four workers, so two children going terminal at once double-fired every completion notification and synthesized two empty merged messages | the flip to `'D'` is an atomic conditional UPDATE (`claim_terminal_transition`); losers return without re-notifying, and the empty branch discards the merged message it had built |
| — (Shepherd-native) The merge lock's 45s TTL was shorter than a large fold, so a lapsed lock let two `ars_merge` tasks fold the same parent and the later UPDATE dropped the other ARA's merge | the lock is refreshed every 15s for as long as it is held |
| — (Shepherd-native) Ready cache entries never expired, and a live entry exempts its whole tree from the payload purge, so the cache pinned the payloads of every distinct query it had ever seen | `ars_cache_ready_max_age_sec` (7 days) retires them; expired entries stop answering immediately and the watchdog purges them |
| — (Shepherd-native) The cache key covered only the query graph and `workflow`, but the fanout forwards `parameters` verbatim to every ARA, so two submits differing only there shared an entry and one got the other's answer | `parameters` is part of the key (minus `overwrite_cache`, which steers the cache, not the query); `CACHE_KEY_VERSION` bumped to `2` |

### Endpoints

| Upstream | Port |
|---|---|
| `GET /ars/api/block/<pk>` ran the blocklist cascade over an arbitrary stored message and saved the result in place — an unauthenticated destructive edit of a shared tree, which also 500s on any response without `auxiliary_graphs` | **not served**, and dropped from the `api/` index. Blocklist removal still runs where it belongs, in `ars_postprocess` over each merged message |
| `GET /ars/api/messages/<pk>?compress` read only Redis, so it 404'd once the Redis TTL lapsed on a message still readable through every other endpoint | falls back to the durable `ars_message.data` copy and re-warms Redis |
| `GET /ars/api/health` answered a non-GET with `Only POST is permitted!` | `Only GET is permitted!` |
| `GET /ars/api/filter/<pk>` ran `ast.literal_eval` on raw query-string values, so a malformed literal escaped as an unstyled 500 | 400 naming the filter and value |
| `GET /ars/api/latest_pk/<n>` took `n` unbounded as both a day count and a row limit, walking one response entry per day | clamped to 1..365 |
| `GET /ars/api/reports/<inforesid>` interpolated the path segment into a `LIKE` pattern unescaped | metacharacters escaped |

## Not ported (documented drops)

- Django admin, the websocket echo consumer, the HTML status/answers pages
  (`/ars/app/*`, `/ars/answer/<pk>`), and the tr_kp proxy views. The JSON
  APIs those pages consume are all served.
- `ara-explanatory` special-case remains as the callback-injection skip
  only, as upstream.
